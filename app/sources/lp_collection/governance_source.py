"""
Governance data collector for LP funds.

Collects board members, trustees, and committee information from:
- LP official websites (board/about pages)
- Annual reports
- SEC filings

IMPORTANT: Only collects publicly available information.
"""

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.types import (
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)

logger = logging.getLogger(__name__)


# Patterns to identify governance pages (ordered by specificity)
GOVERNANCE_PAGE_PATTERNS = [
    # Most specific paths first
    "/about-us/our-plan/our-board",
    "/about-us/board-of-trustees",
    "/about-us/leadership",
    "/about-us/our-board",
    "/about/board-of-trustees",
    "/about/leadership",
    "/about/our-board",
    "/about/board",
    "/en-ca/about-us/our-plan/our-board",  # OTPP style
    "/en/about/board",
    # Standard paths
    "/board-of-trustees",
    "/board-of-directors",
    "/investment-committee",
    "/board",
    "/trustees",
    "/leadership",
    "/governance",
    "/our-team",
    "/who-we-are",
    "/executive-team",
    "/management-team",
    # Generic fallbacks
    "/about-us",
    "/about",
]

# Role patterns for extraction (ordered by specificity - most specific first)
GOVERNANCE_ROLE_PATTERNS = {
    "board_chair": [
        r"^chair$",
        r"^chairman$",
        r"^chairwoman$",
        r"^chairperson$",
        r"chair(?:man|woman|person)?\s+of\s+the\s+board",
        r"board\s+chair",
        r"presiding\s+(?:member|officer)",
    ],
    "vice_chair": [
        r"vice[- ]?chair(?:man|woman|person)?",
        r"deputy\s+chair",
    ],
    "trustee": [
        r"^trustee$",
        r"board\s+of\s+trustees",
        r"appointed\s+trustee",
        r"elected\s+trustee",
    ],
    "investment_committee_chair": [
        r"investment\s+committee\s+chair",
        r"chair.*investment\s+committee",
        r"cio",
        r"chief\s+investment",
    ],
    "investment_committee_member": [
        r"investment\s+committee",
        r"investment\s+board",
    ],
    "audit_committee_chair": [
        r"audit\s+committee\s+chair",
        r"chair.*audit\s+committee",
    ],
    "audit_committee_member": [
        r"audit\s+committee",
    ],
    "executive_director": [
        r"executive\s+director",
        r"^ceo$",
        r"chief\s+executive\s+officer",
        r"chief\s+executive",
        r"^president$",
        r"president\s+and\s+ceo",
    ],
    "board_member": [
        r"board\s+member",
        r"^director$",
        r"^member$",
        r"member\s+of\s+the\s+board",
        r"appointed\s+(?:by|member)",
        r"elected\s+(?:by|member)",
    ],
}

# Representation patterns
REPRESENTATION_PATTERNS = [
    (r"state\s+treasurer", "State Treasurer"),
    (r"retiree\s+representative", "Retiree Representative"),
    (r"public\s+member", "Public Member"),
    (r"governor(?:'s)?\s+appoint", "Governor Appointee"),
    (r"legislature|legislative", "Legislative Appointee"),
    (r"employee\s+representative", "Employee Representative"),
    (r"beneficiary", "Beneficiary Representative"),
    (r"ex\s*-?\s*officio", "Ex Officio"),
]


class GovernanceCollector(BaseCollector):
    """
    Collects governance structure data from LP sources.

    Extracts:
    - Board/trustee members
    - Committee memberships
    - Tenure information
    - Meeting schedules and documents
    """

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.WEBSITE  # Uses website crawling

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect governance data for an LP.

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL

        Returns:
            CollectionResult with governance items
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Collecting governance data for {lp_name}")

        if not website_url:
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message="No website URL provided",
                started_at=started_at,
            )

        try:
            # Find and parse governance pages
            governance_items = await self._collect_governance_members(
                website_url, lp_id, lp_name
            )
            items.extend(governance_items)

            # Collect meeting information
            meeting_items = await self._collect_meeting_info(
                website_url, lp_id, lp_name
            )
            items.extend(meeting_items)

            success = len(items) > 0
            if not items:
                warnings.append("No governance information found")

            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error collecting governance for {lp_name}: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                started_at=started_at,
            )

    async def _collect_governance_members(
        self,
        website_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Find and parse governance pages for board/trustee members."""
        items = []

        # Try common governance page patterns
        for pattern in GOVERNANCE_PAGE_PATTERNS:
            page_url = website_url.rstrip("/") + pattern
            response = await self._fetch_url(page_url)

            if response and response.status_code == 200:
                # Extract members from this page
                members = self._extract_governance_members(
                    response.text, page_url, lp_id, lp_name
                )
                items.extend(members)

                if members:
                    logger.info(
                        f"Found {len(members)} governance members at {page_url}"
                    )
                    break  # Found governance page, stop searching

        return items

    def _extract_governance_members(
        self,
        html: str,
        source_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Extract governance member information from HTML."""
        items = []
        seen_names = set()

        # Helper to add member if valid
        def add_member(name: str, title: str, confidence: str = "medium"):
            name = name.strip()
            if self._is_valid_name(name) and name not in seen_names:
                seen_names.add(name)
                item = self._create_governance_item(
                    lp_id, lp_name, name, title.strip(), source_url, confidence
                )
                if item:
                    items.append(item)

        # Pattern 1: "Role: Name" format (e.g., "Chair: John Smith")
        # Handles: <p>Chair: Debbie Stein</p>, Vice-Chair: Name, etc.
        role_name_pattern = re.compile(
            r'(?:^|>)\s*'
            r'(Chair(?:man|woman|person)?|Vice[- ]?Chair(?:man|woman|person)?|'
            r'President|CEO|Executive\s+Director|Trustee|Director|Member)'
            r'\s*:\s*'
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
            r'\s*(?:<|$)',
            re.IGNORECASE | re.MULTILINE
        )
        for match in role_name_pattern.finditer(html):
            title = match.group(1).strip()
            name = match.group(2).strip()
            add_member(name, title, "high")

        # Pattern 2: "Members: Name1, Name2 and Name3" format
        members_list_pattern = re.compile(
            r'(?:Members|Board\s+Members|Trustees|Directors)\s*:\s*'
            r'([A-Z][^<]{10,200}?)(?:<|$)',
            re.IGNORECASE
        )
        for match in members_list_pattern.finditer(html):
            names_text = match.group(1)
            # Split by comma and "and"
            names = re.split(r'\s*,\s*|\s+and\s+', names_text)
            for name in names:
                # Clean up name (remove trailing punctuation)
                name = re.sub(r'[.,;:\s]+$', '', name.strip())
                add_member(name, "Board Member", "medium")

        # Pattern 3: List items with "Name, Organization" format
        # Handles: <li>Marie Moftah, L'Association des enseignantes...</li>
        li_name_org_pattern = re.compile(
            r'<li[^>]*>\s*'
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
            r'\s*,\s*'
            r'([^<]+)'
            r'\s*</li>',
            re.IGNORECASE
        )
        for match in li_name_org_pattern.finditer(html):
            name = match.group(1).strip()
            org = match.group(2).strip()
            # Use org as title/representing info
            add_member(name, f"Board Member ({org[:100]})", "medium")

        # Pattern 4: Header tag followed by title/role (original pattern, improved)
        name_title_pattern = re.compile(
            r'<(?:h[2-5]|strong|b)[^>]*>\s*'
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
            r'\s*</(?:h[2-5]|strong|b)>'
            r'[^<]{0,50}<[^>]*>\s*([^<]+)',
            re.IGNORECASE
        )
        for match in name_title_pattern.finditer(html):
            name = match.group(1).strip()
            title = match.group(2).strip()
            add_member(name, title, "medium")

        # Pattern 5: Card/div with class containing member/trustee/director
        card_pattern = re.compile(
            r'class="[^"]*(?:member|trustee|director|team|board|staff|person|profile)[^"]*"[^>]*>'
            r'[^<]*(?:<[^>]+>[^<]*)*?'
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
            r'[^<]*(?:<[^>]+>[^<]*)*?'
            r'(?:title|role|position|job)[^>]*>([^<]+)',
            re.IGNORECASE | re.DOTALL
        )
        for match in card_pattern.finditer(html):
            name = match.group(1).strip()
            title = match.group(2).strip()
            add_member(name, title, "medium")

        # Pattern 6: Structured data / JSON-LD for board members
        json_person_pattern = re.compile(
            r'"@type"\s*:\s*"Person"[^}]*"name"\s*:\s*"([^"]+)"[^}]*"jobTitle"\s*:\s*"([^"]+)"',
            re.IGNORECASE
        )
        for match in json_person_pattern.finditer(html):
            name = match.group(1).strip()
            title = match.group(2).strip()
            add_member(name, title, "high")

        # Pattern 7: Simple name in governance context (fallback)
        # Look for names near governance keywords
        if len(items) < 3:
            governance_section = self._find_governance_section(html)
            if governance_section:
                simple_name_pattern = re.compile(
                    r'(?:^|[>\s])([A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+)(?:[,<\s]|$)'
                )
                for match in simple_name_pattern.finditer(governance_section):
                    name = match.group(1).strip()
                    add_member(name, "Board Member", "low")

        return items

    def _find_governance_section(self, html: str) -> Optional[str]:
        """Find the section of HTML most likely to contain governance info."""
        # Look for sections with governance-related headers
        section_pattern = re.compile(
            r'(?:<(?:section|div|article)[^>]*>)?'
            r'[^<]*<(?:h[1-4])[^>]*>[^<]*'
            r'(?:Board|Trustees?|Directors?|Leadership|Governance|Committee)'
            r'[^<]*</(?:h[1-4])>'
            r'([\s\S]{0,5000}?)'
            r'(?:</(?:section|div|article)>|<(?:h[1-4])[^>]*>)',
            re.IGNORECASE
        )
        match = section_pattern.search(html)
        if match:
            return match.group(1)
        return None

    def _is_valid_name(self, name: str) -> bool:
        """Check if extracted string looks like a person's name."""
        if not name or len(name) < 5 or len(name) > 60:
            return False

        # Must have at least first and last name
        parts = name.split()
        if len(parts) < 2 or len(parts) > 5:
            return False

        # Each part should start with capital letter and have reasonable pattern
        for part in parts:
            # Allow initials (single letter with optional period)
            if len(part) <= 2:
                continue
            if not part[0].isupper():
                return False
            # Name parts should have mostly lowercase after first letter
            if len(part) > 3 and part[1:].isupper():
                return False  # Likely an acronym like "OPERS"

        # Check for common non-name patterns (organization names, categories, etc.)
        non_names = [
            # Generic web/nav terms
            "board", "committee", "member", "trustee", "director",
            "about", "contact", "home", "login", "menu", "search",
            "privacy", "terms", "copyright", "read more", "click here",
            "learn more", "view all", "see more", "our team", "our board",
            # Organization terms
            "investment committee", "audit committee", "annual report",
            "pension fund", "pension plan", "retirement system",
            "foundation", "association", "corporation", "institute",
            "university", "college", "school", "district",
            # Common non-person phrases
            "ontario teachers", "natural resources", "allowed is",
            "venture growth", "real estate", "capital markets",
            "total fund", "private capital", "news releases",
            "general plans", "safety plans", "retiree healthcare",
            "leadership team", "executive team", "management team",
            "policy", "strategy", "governance", "oversight",
            "stakeholder", "relations", "services", "resources",
            "portfolio", "solutions", "operations", "affairs",
            # URL/tech
            "http", "www", ".com", ".org", ".gov", ".edu",
        ]
        name_lower = name.lower()
        if any(non in name_lower for non in non_names):
            return False

        # Check if any part looks like an acronym (all caps, 2+ chars)
        for part in parts:
            if len(part) >= 2 and part.isupper():
                return False

        # Name parts should be reasonable length (2-15 chars each)
        for part in parts:
            if len(part) > 15:
                return False

        # Should not contain numbers or special characters (except periods, hyphens, apostrophes)
        if re.search(r'[0-9@#$%^&*()+=\[\]{}|\\/<>]', name):
            return False

        # First name should look like a name (not a category word)
        first_name = parts[0].lower()
        category_words = [
            "general", "safety", "public", "private", "corporate",
            "state", "federal", "national", "local", "regional",
            "early", "senior", "junior", "middle", "total",
            "annual", "quarterly", "monthly", "weekly", "daily",
            "our", "your", "their", "the", "this", "that",
            "new", "old", "current", "former", "past", "future",
            # Business/corporate terms
            "responsible", "sustainable", "strategic", "global",
            "business", "corporate", "financial", "investment",
            "join", "discover", "explore", "learn", "read",
            "view", "see", "click", "download", "subscribe",
        ]
        if first_name in category_words:
            return False

        # Last word should also not be a category/nav word
        last_word = parts[-1].lower()
        nav_words = [
            "management", "history", "mission", "vision", "values",
            "overview", "information", "details", "news", "updates",
            "benefits", "plans", "programs", "services", "contact",
            # More business terms
            "investor", "insurer", "employer", "citizen", "business",
            "ethics", "report", "governance", "compliance", "risk",
            "us", "here", "more", "all", "now",
        ]
        if last_word in nav_words:
            return False

        # Reject common two-word non-name phrases
        full_lower = name.lower()
        non_name_phrases = [
            "join us", "contact us", "about us", "follow us",
            "read more", "learn more", "view more", "see more",
            "click here", "sign up", "log in", "sign in",
            "remuneration report", "annual report", "business ethics",
        ]
        if full_lower in non_name_phrases:
            return False

        return True

    def _create_governance_item(
        self,
        lp_id: int,
        lp_name: str,
        name: str,
        title: str,
        source_url: str,
        confidence: str = "medium",
    ) -> Optional[CollectedItem]:
        """Create a governance member CollectedItem."""
        # Determine governance role from title
        role = self._categorize_governance_role(title)

        # Default to board_member if no specific role found but name is valid
        if not role:
            role = "board_member"

        # Determine who they represent
        representing = self._extract_representation(title)

        return CollectedItem(
            item_type="governance_member",
            data={
                "lp_id": lp_id,
                "lp_name": lp_name,
                "full_name": name,
                "title": title[:200] if title else None,
                "governance_role": role,
                "representing": representing,
                "is_current": 1,
                "source_type": "website",
            },
            source_url=source_url,
            confidence=confidence,
        )

    def _categorize_governance_role(self, title: str) -> Optional[str]:
        """Categorize title into governance role."""
        if not title:
            return None

        title_lower = title.lower()

        for role, patterns in GOVERNANCE_ROLE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, title_lower):
                    return role

        # Default to board_member if title contains relevant keywords
        if any(kw in title_lower for kw in ["board", "trustee", "director"]):
            return "board_member"

        return None

    def _extract_representation(self, title: str) -> Optional[str]:
        """Extract who the member represents from their title."""
        if not title:
            return None

        title_lower = title.lower()

        for pattern, representation in REPRESENTATION_PATTERNS:
            if re.search(pattern, title_lower):
                return representation

        return None

    async def _collect_meeting_info(
        self,
        website_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Collect board meeting information."""
        items = []

        # Common meeting page patterns
        meeting_patterns = [
            "/meetings",
            "/board-meetings",
            "/calendar",
            "/agendas",
            "/minutes",
            "/board/meetings",
        ]

        for pattern in meeting_patterns:
            page_url = website_url.rstrip("/") + pattern
            response = await self._fetch_url(page_url)

            if response and response.status_code == 200:
                meetings = self._extract_meetings(
                    response.text, page_url, lp_id, lp_name
                )
                items.extend(meetings)

                if meetings:
                    logger.info(f"Found {len(meetings)} meetings at {page_url}")
                    break

        return items

    def _extract_meetings(
        self,
        html: str,
        source_url: str,
        lp_id: int,
        lp_name: str,
    ) -> List[CollectedItem]:
        """Extract meeting information from HTML."""
        items = []

        # Look for date patterns near meeting-related text
        date_pattern = re.compile(
            r'(\w+\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})'
        )

        # Look for document links (PDFs)
        pdf_pattern = re.compile(
            r'href=["\']([^"\']+\.pdf)["\'][^>]*>([^<]*(?:agenda|minutes|meeting)[^<]*)',
            re.IGNORECASE
        )

        # Extract PDF meeting documents
        for match in pdf_pattern.finditer(html):
            pdf_url = match.group(1)
            link_text = match.group(2).strip()

            # Determine meeting type from link text
            meeting_type = "board_regular"
            link_lower = link_text.lower()
            if "special" in link_lower:
                meeting_type = "board_special"
            elif "investment" in link_lower:
                meeting_type = "investment_committee"
            elif "audit" in link_lower:
                meeting_type = "audit_committee"

            # Try to extract date
            meeting_date = None
            context_start = max(0, match.start() - 100)
            context = html[context_start:match.end() + 100]
            date_match = date_pattern.search(context)
            if date_match:
                meeting_date = self._parse_date(date_match.group(1))

            # Construct full URL if relative
            if not pdf_url.startswith("http"):
                base = source_url.rsplit("/", 1)[0]
                pdf_url = f"{base}/{pdf_url.lstrip('/')}"

            # Determine if agenda or minutes
            is_agenda = "agenda" in link_lower
            is_minutes = "minute" in link_lower

            item = CollectedItem(
                item_type="board_meeting",
                data={
                    "lp_id": lp_id,
                    "lp_name": lp_name,
                    "meeting_date": meeting_date.isoformat() if meeting_date else None,
                    "meeting_type": meeting_type,
                    "meeting_title": link_text[:200],
                    "agenda_url": pdf_url if is_agenda else None,
                    "minutes_url": pdf_url if is_minutes else None,
                    "materials_url": pdf_url if not is_agenda and not is_minutes else None,
                    "source_type": "website",
                },
                source_url=source_url,
                confidence="medium",
            )
            items.append(item)

        return items[:20]  # Limit to recent meetings

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats."""
        formats = [
            "%B %d, %Y",
            "%B %d %Y",
            "%b %d, %Y",
            "%m/%d/%Y",
            "%m/%d/%y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue

        return None
