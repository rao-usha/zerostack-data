"""
Contact validation utilities for LP key contacts.

Validates email, phone, and name formats according to project rules:
- Only professional/institutional emails
- Standardized phone formats
- Proper name formatting
"""

import re
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# EMAIL VALIDATION
# =============================================================================


def validate_email(email: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate email format and content.

    Returns:
        (is_valid, error_message)
    """
    if not email:
        return True, None  # Optional field

    # Basic format validation
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(email_pattern, email):
        return False, f"Invalid email format: {email}"

    # Reject personal email domains
    personal_domains = [
        "gmail.com",
        "yahoo.com",
        "hotmail.com",
        "outlook.com",
        "aol.com",
        "icloud.com",
        "protonmail.com",
    ]
    domain = email.split("@")[1].lower()
    if domain in personal_domains:
        return False, f"Personal email domain not allowed: {domain}"

    # Check for generic prefixes (but allow institutional ones)
    generic_prefixes = [
        "info",
        "contact",
        "admin",
        "webmaster",
        "sales",
        "support",
        "hello",
    ]
    email_prefix = email.split("@")[0].lower()

    if email_prefix in generic_prefixes:
        # Allow if it contains investment-related terms
        allowed_terms = ["investment", "investor", "relations", "ir", "capital", "fund"]
        if not any(term in email.lower() for term in allowed_terms):
            return False, f"Generic email prefix not allowed: {email_prefix}@..."

    return True, None


def is_institutional_email(email: str) -> bool:
    """Check if email appears to be from an institutional domain."""
    if not email:
        return False

    institutional_indicators = [
        ".gov",
        ".edu",
        ".org",  # Government, education, org TLDs
        "pension",
        "invest",
        "capital",
        "treasury",
        "endowment",  # Keywords
        "retirement",
        "fund",
        "asset",
    ]

    email_lower = email.lower()
    return any(indicator in email_lower for indicator in institutional_indicators)


# =============================================================================
# PHONE VALIDATION
# =============================================================================


def validate_phone(phone: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate phone number format.

    Accepts common formats:
    - +1-XXX-XXX-XXXX
    - (XXX) XXX-XXXX
    - XXX-XXX-XXXX
    - +CC-XXX-XXX...

    Returns:
        (is_valid, error_message)
    """
    if not phone:
        return True, None  # Optional field

    # Remove all spaces for validation
    phone_clean = phone.replace(" ", "")

    # Common phone patterns
    patterns = [
        r"^\+?1?[-.]?\(?(\d{3})\)?[-.]?(\d{3})[-.]?(\d{4})$",  # US/Canada
        r"^\+\d{1,3}[-.]?\d{3,4}[-.]?\d{3,4}[-.]?\d{3,4}$",  # International
        r"^\(\d{3}\)\s?\d{3}[-.]?\d{4}$",  # (XXX) XXX-XXXX
    ]

    for pattern in patterns:
        if re.match(pattern, phone_clean):
            return True, None

    return False, f"Invalid phone format: {phone}"


def standardize_phone(phone: str) -> str:
    """
    Standardize phone number format to +1-XXX-XXX-XXXX for US/Canada.

    Returns original if not a recognized format.
    """
    if not phone:
        return phone

    # Extract digits only
    digits = re.sub(r"\D", "", phone)

    # US/Canada format (10 or 11 digits)
    if len(digits) == 10:
        return f"+1-{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    elif len(digits) == 11 and digits[0] == "1":
        return f"+1-{digits[1:4]}-{digits[4:7]}-{digits[7:11]}"

    # Return original if not US/Canada
    return phone


# =============================================================================
# NAME VALIDATION
# =============================================================================


def validate_name(name: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate contact name.

    Rules:
    - Must be at least 2 words (first + last name minimum)
    - No generic titles like "Investment Office" without person name
    - Allow exceptions for generic contacts (e.g., "Media Relations")

    Returns:
        (is_valid, error_message)
    """
    if not name or len(name.strip()) < 2:
        return False, "Name is required and must be at least 2 characters"

    name_clean = name.strip()
    words = name_clean.split()

    # Check for generic department names without person
    generic_terms = [
        "office",
        "department",
        "team",
        "group",
        "division",
        "unit",
        "desk",
        "center",
        "centre",
        "bureau",
    ]

    # Allow specific generic contacts
    allowed_generic = [
        "media relations",
        "investor relations",
        "investment office",
        "contact",
        "communications",
        "public affairs",
    ]

    name_lower = name_clean.lower()

    # Check if it's an allowed generic contact
    if any(allowed in name_lower for allowed in allowed_generic):
        return True, None

    # Check if it's a disallowed generic name
    if len(words) <= 2 and any(term in name_lower for term in generic_terms):
        return False, f"Generic department name not allowed: {name_clean}"

    # Must have at least 2 words for person names
    if len(words) < 2:
        return False, f"Name must include first and last name: {name_clean}"

    return True, None


def is_generic_contact(name: str) -> bool:
    """
    Check if contact name represents a generic/department contact
    rather than a specific person.
    """
    if not name:
        return False

    generic_indicators = [
        "office",
        "relations",
        "media",
        "communications",
        "contact",
        "department",
        "team",
        "inquiry",
        "inquiries",
    ]

    name_lower = name.lower()
    return any(indicator in name_lower for indicator in generic_indicators)


# =============================================================================
# DUPLICATE DETECTION
# =============================================================================


def normalize_name_for_comparison(name: str) -> str:
    """
    Normalize name for duplicate detection.

    - Lowercase
    - Remove middle initials
    - Remove titles (Dr., Mr., Ms., etc.)
    - Strip whitespace
    """
    if not name:
        return ""

    name_clean = name.lower().strip()

    # Remove common titles
    titles = ["dr.", "mr.", "mrs.", "ms.", "prof.", "professor"]
    for title in titles:
        name_clean = name_clean.replace(title, "").strip()

    # Remove middle initials (single letter followed by period)
    name_clean = re.sub(r"\b[a-z]\.\s?", "", name_clean)

    # Collapse multiple spaces
    name_clean = re.sub(r"\s+", " ", name_clean)

    return name_clean


def is_likely_duplicate(
    name1: str, email1: Optional[str], name2: str, email2: Optional[str]
) -> bool:
    """
    Check if two contacts are likely duplicates.

    Duplicates if:
    - Same email (if both provided)
    - Same normalized name
    """
    # If both have emails and they match, it's a duplicate
    if email1 and email2 and email1.lower() == email2.lower():
        return True

    # If normalized names match exactly, likely duplicate
    norm1 = normalize_name_for_comparison(name1)
    norm2 = normalize_name_for_comparison(name2)

    if norm1 == norm2:
        return True

    return False


# =============================================================================
# BULK VALIDATION
# =============================================================================


def validate_contact(
    full_name: str,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    role_category: Optional[str] = None,
) -> Tuple[bool, list[str]]:
    """
    Validate all fields of a contact.

    Returns:
        (is_valid, list_of_errors)
    """
    errors = []

    # Validate name
    name_valid, name_error = validate_name(full_name)
    if not name_valid:
        errors.append(name_error)

    # Validate email
    email_valid, email_error = validate_email(email)
    if not email_valid:
        errors.append(email_error)

    # Validate phone
    phone_valid, phone_error = validate_phone(phone)
    if not phone_valid:
        errors.append(phone_error)

    # Validate role category
    if role_category:
        valid_roles = [
            "CIO",
            "CFO",
            "CEO",
            "Investment Director",
            "Board Member",
            "Managing Director",
            "IR Contact",
            "Other",
        ]
        if role_category not in valid_roles:
            errors.append(f"Invalid role_category: {role_category}")

    return len(errors) == 0, errors


# =============================================================================
# DATA QUALITY SCORING
# =============================================================================


def calculate_confidence_score(
    email: Optional[str], phone: Optional[str], source_type: str, is_verified: bool
) -> str:
    """
    Calculate confidence level based on data completeness and source.

    Returns:
        'high', 'medium', or 'low'
    """
    score = 0

    # Source quality
    source_scores = {
        "sec_adv": 3,  # Highest (regulatory filing)
        "manual": 3,  # High (human verified)
        "website": 2,  # Medium (official site)
        "disclosure_doc": 2,  # Medium (official document)
        "annual_report": 2,  # Medium (official report)
    }
    score += source_scores.get(source_type, 1)

    # Data completeness
    if email and is_institutional_email(email):
        score += 2
    if phone:
        score += 1
    if is_verified:
        score += 2

    # Determine level
    if score >= 6:
        return "high"
    elif score >= 3:
        return "medium"
    else:
        return "low"
