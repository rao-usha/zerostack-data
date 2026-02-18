"""
Email Inferrer for People Intelligence Platform.

Infers work email addresses based on company patterns and known email formats.
"""

import re
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum


class EmailPattern(Enum):
    """Common corporate email patterns."""

    FIRST_LAST = "first.last"  # john.smith@company.com
    FIRST_L = "first.l"  # john.s@company.com
    F_LAST = "f.last"  # j.smith@company.com
    FIRST = "first"  # john@company.com
    LAST = "last"  # smith@company.com
    FIRST_LAST_NO_DOT = "firstlast"  # johnsmith@company.com
    LAST_FIRST = "last.first"  # smith.john@company.com
    FIRST_INITIAL = "first_l"  # john_s@company.com
    F_LAST_NO_DOT = "flast"  # jsmith@company.com


@dataclass
class InferredEmail:
    """An inferred email address with confidence."""

    email: str
    pattern: EmailPattern
    confidence: str  # high, medium, low
    notes: Optional[str] = None


class EmailInferrer:
    """Service for inferring work email addresses."""

    # Known company email patterns (domain -> pattern)
    KNOWN_PATTERNS: Dict[str, EmailPattern] = {
        "fastenal.com": EmailPattern.FIRST_LAST,
        "grainger.com": EmailPattern.FIRST_LAST,
        "mscdirect.com": EmailPattern.FIRST_LAST,
        "applied.com": EmailPattern.FIRST_LAST,
        "motion.com": EmailPattern.FIRST_LAST,
        "wesco.com": EmailPattern.FIRST_LAST,
        "rfreston.com": EmailPattern.FIRST_LAST,
        "kaman.com": EmailPattern.FIRST_LAST,
    }

    def __init__(self):
        pass

    def infer_email(
        self,
        first_name: str,
        last_name: str,
        company_domain: str,
        known_pattern: Optional[EmailPattern] = None,
    ) -> List[InferredEmail]:
        """
        Infer possible email addresses for a person.

        Returns list of candidates ordered by likelihood.
        """
        if not first_name or not last_name or not company_domain:
            return []

        # Normalize inputs
        first = self._normalize_name(first_name)
        last = self._normalize_name(last_name)
        domain = company_domain.lower().strip()

        # Remove common domain prefixes if present
        if domain.startswith("www."):
            domain = domain[4:]

        results = []

        # If we know the pattern for this company
        if known_pattern:
            email = self._generate_email(first, last, domain, known_pattern)
            results.append(
                InferredEmail(
                    email=email,
                    pattern=known_pattern,
                    confidence="high",
                    notes="Known company pattern",
                )
            )
        elif domain in self.KNOWN_PATTERNS:
            pattern = self.KNOWN_PATTERNS[domain]
            email = self._generate_email(first, last, domain, pattern)
            results.append(
                InferredEmail(
                    email=email,
                    pattern=pattern,
                    confidence="high",
                    notes="Known company pattern",
                )
            )

        # Generate candidates for common patterns
        patterns_to_try = [
            (EmailPattern.FIRST_LAST, "medium"),  # Most common
            (EmailPattern.F_LAST, "medium"),  # Second most common
            (EmailPattern.FIRST_L, "low"),
            (EmailPattern.FIRST, "low"),
            (EmailPattern.FIRST_LAST_NO_DOT, "low"),
            (EmailPattern.F_LAST_NO_DOT, "low"),
        ]

        for pattern, confidence in patterns_to_try:
            # Skip if already added from known pattern
            if results and results[0].pattern == pattern:
                continue

            email = self._generate_email(first, last, domain, pattern)
            results.append(
                InferredEmail(
                    email=email,
                    pattern=pattern,
                    confidence=confidence,
                )
            )

        return results

    def _normalize_name(self, name: str) -> str:
        """Normalize a name for email generation."""
        if not name:
            return ""
        # Remove accents and special characters
        name = name.lower().strip()
        # Remove common suffixes
        name = re.sub(r"\s+(jr|sr|ii|iii|iv)\.?$", "", name)
        # Keep only alphanumeric
        name = re.sub(r"[^a-z]", "", name)
        return name

    def _generate_email(
        self,
        first: str,
        last: str,
        domain: str,
        pattern: EmailPattern,
    ) -> str:
        """Generate email address from pattern."""
        if pattern == EmailPattern.FIRST_LAST:
            local = f"{first}.{last}"
        elif pattern == EmailPattern.FIRST_L:
            local = f"{first}.{last[0]}" if last else first
        elif pattern == EmailPattern.F_LAST:
            local = f"{first[0]}.{last}" if first else last
        elif pattern == EmailPattern.FIRST:
            local = first
        elif pattern == EmailPattern.LAST:
            local = last
        elif pattern == EmailPattern.FIRST_LAST_NO_DOT:
            local = f"{first}{last}"
        elif pattern == EmailPattern.LAST_FIRST:
            local = f"{last}.{first}"
        elif pattern == EmailPattern.FIRST_INITIAL:
            local = f"{first}_{last[0]}" if last else first
        elif pattern == EmailPattern.F_LAST_NO_DOT:
            local = f"{first[0]}{last}" if first else last
        else:
            local = f"{first}.{last}"

        return f"{local}@{domain}"

    def learn_pattern_from_email(
        self,
        email: str,
        first_name: str,
        last_name: str,
    ) -> Optional[EmailPattern]:
        """
        Learn email pattern from a known email address.

        Returns the pattern that matches the email.
        """
        if not email or "@" not in email:
            return None

        first = self._normalize_name(first_name)
        last = self._normalize_name(last_name)
        local_part = email.split("@")[0].lower()

        # Try to match patterns
        patterns = [
            (EmailPattern.FIRST_LAST, f"{first}.{last}"),
            (EmailPattern.FIRST_L, f"{first}.{last[0]}" if last else None),
            (EmailPattern.F_LAST, f"{first[0]}.{last}" if first else None),
            (EmailPattern.FIRST, first),
            (EmailPattern.LAST, last),
            (EmailPattern.FIRST_LAST_NO_DOT, f"{first}{last}"),
            (EmailPattern.LAST_FIRST, f"{last}.{first}"),
            (EmailPattern.FIRST_INITIAL, f"{first}_{last[0]}" if last else None),
            (EmailPattern.F_LAST_NO_DOT, f"{first[0]}{last}" if first else None),
        ]

        for pattern, expected in patterns:
            if expected and local_part == expected:
                return pattern

        return None

    def validate_email_format(self, email: str) -> bool:
        """
        Validate email format (not deliverability).
        """
        if not email:
            return False

        # Basic email regex
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return bool(re.match(pattern, email))

    def extract_domain_from_website(self, website: str) -> Optional[str]:
        """
        Extract email domain from company website URL.
        """
        if not website:
            return None

        # Remove protocol
        domain = re.sub(r"^https?://", "", website.lower())
        # Remove www
        domain = re.sub(r"^www\.", "", domain)
        # Remove path
        domain = domain.split("/")[0]
        # Remove port
        domain = domain.split(":")[0]

        return domain if "." in domain else None


class CompanyEmailPatternLearner:
    """
    Learn email patterns for a company from known emails.
    """

    def __init__(self):
        self.email_inferrer = EmailInferrer()

    def learn_pattern_from_db(
        self,
        company_id: int,
        session,
    ) -> Optional[EmailPattern]:
        """
        Learn the email pattern for a company from existing database records.

        Queries company_people JOIN people where work_email or email is populated,
        then learns the most common pattern from those known emails.

        Args:
            company_id: The company to learn patterns for.
            session: SQLAlchemy session.

        Returns:
            The most common EmailPattern found, or None.
        """
        try:
            from app.core.people_models import Person, CompanyPerson

            # Find people at this company who have known emails
            rows = (
                session.query(
                    Person.first_name,
                    Person.last_name,
                    Person.email,
                    CompanyPerson.work_email,
                )
                .join(CompanyPerson, CompanyPerson.person_id == Person.id)
                .filter(
                    CompanyPerson.company_id == company_id,
                    CompanyPerson.is_current == True,
                )
                .all()
            )

            emails = []
            for first_name, last_name, person_email, work_email in rows:
                email = work_email or person_email
                if email and first_name and last_name:
                    emails.append((email, first_name, last_name))

            if not emails:
                return None

            return self.learn_from_known_emails(emails)

        except Exception as e:
            import logging

            logging.getLogger(__name__).warning(
                f"Failed to learn email pattern from DB for company {company_id}: {e}"
            )
            return None

    def learn_from_known_emails(
        self,
        emails: List[Tuple[str, str, str]],  # (email, first_name, last_name)
    ) -> Optional[EmailPattern]:
        """
        Learn the most common email pattern from known emails.

        Takes list of (email, first_name, last_name) tuples.
        Returns the most common pattern found.
        """
        if not emails:
            return None

        pattern_counts: Dict[EmailPattern, int] = {}

        for email, first_name, last_name in emails:
            pattern = self.email_inferrer.learn_pattern_from_email(
                email, first_name, last_name
            )
            if pattern:
                pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1

        if not pattern_counts:
            return None

        # Return most common pattern
        return max(pattern_counts, key=pattern_counts.get)

    def infer_company_emails(
        self,
        company_domain: str,
        people: List[Tuple[str, str]],  # (first_name, last_name)
        known_emails: Optional[List[Tuple[str, str, str]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Infer emails for multiple people at a company.

        If known_emails provided, learns pattern first.
        """
        pattern = None
        if known_emails:
            pattern = self.learn_from_known_emails(known_emails)

        results = []
        for first_name, last_name in people:
            candidates = self.email_inferrer.infer_email(
                first_name, last_name, company_domain, pattern
            )
            results.append(
                {
                    "first_name": first_name,
                    "last_name": last_name,
                    "candidates": [
                        {
                            "email": c.email,
                            "pattern": c.pattern.value,
                            "confidence": c.confidence,
                        }
                        for c in candidates[:3]  # Top 3 candidates
                    ],
                }
            )

        return results
