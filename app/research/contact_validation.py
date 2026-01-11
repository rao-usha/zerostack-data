"""
Contact validation utilities for family office contact enrichment.
Enforces strict quality and privacy standards.
"""
import re
from typing import Optional, Tuple
from datetime import date


# Personal email providers that should be rejected for business contacts
PERSONAL_EMAIL_PROVIDERS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'me.com', 'mac.com', 'live.com', 'msn.com',
    'protonmail.com', 'protonmail.ch', 'mail.com', 'yandex.com'
}

# Acceptable generic email prefixes for business emails
ACCEPTABLE_GENERIC_PREFIXES = {
    'info', 'contact', 'hello', 'admin', 'office', 'inquiry',
    'inquiries', 'business', 'invest', 'investment', 'partners'
}

# Family office executive title patterns
EXECUTIVE_TITLE_PATTERNS = [
    r'\b(?:Chief Investment Officer|CIO)\b',
    r'\b(?:Chief Executive Officer|CEO)\b',
    r'\b(?:Chief Financial Officer|CFO)\b',
    r'\b(?:Chief Operating Officer|COO)\b',
    r'\b(?:Managing Director|Managing Member)\b',
    r'\b(?:Principal|Family Principal)\b',
    r'\b(?:Investment Director|Portfolio Manager)\b',
    r'\b(?:Chief Compliance Officer|CCO)\b',
    r'\b(?:Head of Investments?)\b',
    r'\b(?:Head of Portfolio)\b',
    r'\b(?:Director of Investments?)\b',
    r'\b(?:Senior Partner|Partner)\b',
]


def validate_email(email: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate email address for business contact use.
    
    Returns:
        (is_valid, error_message)
    """
    if not email:
        return False, "Email is empty"
    
    # Basic regex validation
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, email):
        return False, "Invalid email format"
    
    # Extract domain
    try:
        domain = email.split('@')[1].lower()
    except IndexError:
        return False, "Cannot extract domain"
    
    # Reject personal email providers
    if domain in PERSONAL_EMAIL_PROVIDERS:
        return False, f"Personal email provider ({domain}) not allowed for business contacts"
    
    # Check if generic email is from a business domain
    prefix = email.split('@')[0].lower()
    if prefix in ACCEPTABLE_GENERIC_PREFIXES:
        # Generic emails are acceptable if from a non-personal domain
        return True, None
    
    # All other business domain emails are acceptable
    return True, None


def validate_phone(phone: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate phone number and check for reasonable business phone format.
    
    Returns:
        (is_valid, error_message)
    """
    if not phone:
        return False, "Phone is empty"
    
    # Remove common formatting characters
    cleaned = re.sub(r'[\s\-\(\)\.]', '', phone)
    
    # Check if it starts with + for international or has 10+ digits
    if cleaned.startswith('+'):
        # International format: + followed by 7-15 digits
        if re.match(r'^\+\d{7,15}$', cleaned):
            return True, None
        else:
            return False, "Invalid international phone format"
    else:
        # US/Canada format: 10 digits
        if re.match(r'^\d{10}$', cleaned):
            return True, None
        # Or 11 digits starting with 1
        elif re.match(r'^1\d{10}$', cleaned):
            return True, None
        else:
            return False, "Invalid phone format (expected 10-digit US/Canada or international +format)"
    
    return False, "Unable to validate phone number"


def standardize_phone(phone: str) -> str:
    """
    Standardize phone number to consistent format.
    US/Canada: (XXX) XXX-XXXX
    International: +X-XXX-XXX-XXXX
    """
    if not phone:
        return ""
    
    # Remove all formatting
    cleaned = re.sub(r'[\s\-\(\)\.]', '', phone)
    
    if cleaned.startswith('+'):
        # International: keep + and add hyphens
        # Simple formatting: +1-234-567-8900
        if len(cleaned) >= 8:
            return f"+{cleaned[1:2]}-{cleaned[2:5]}-{cleaned[5:8]}-{cleaned[8:]}"
        return phone
    else:
        # US/Canada format
        if len(cleaned) == 10:
            return f"({cleaned[0:3]}) {cleaned[3:6]}-{cleaned[6:10]}"
        elif len(cleaned) == 11 and cleaned[0] == '1':
            return f"+1 ({cleaned[1:4]}) {cleaned[4:7]}-{cleaned[7:11]}"
        else:
            return phone


def validate_name(name: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Validate contact name (must be at least 2 words: first + last).
    
    Returns:
        (is_valid, error_message)
    """
    if not name:
        return False, "Name is empty"
    
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    # Must have at least 2 words
    parts = name.split()
    if len(parts) < 2:
        return False, "Name must include first and last name (at least 2 words)"
    
    # Check for reasonable length
    if len(name) < 3:
        return False, "Name too short"
    
    if len(name) > 100:
        return False, "Name too long (max 100 characters)"
    
    # Check for mostly alphabetic characters (allow spaces, hyphens, apostrophes, periods)
    if not re.match(r"^[A-Za-z\s\-'.]+$", name):
        return False, "Name contains invalid characters"
    
    return True, None


def extract_executive_title(text: str) -> Optional[str]:
    """
    Extract executive title from text if present.
    
    Returns:
        Matched title or None
    """
    if not text:
        return None
    
    text = text.strip()
    
    for pattern in EXECUTIVE_TITLE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0)
    
    return None


def is_likely_executive_title(title: Optional[str]) -> bool:
    """
    Check if title indicates an executive/decision-maker role.
    """
    if not title:
        return False
    
    for pattern in EXECUTIVE_TITLE_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return True
    
    return False


def determine_role_from_title(title: Optional[str]) -> Optional[str]:
    """
    Determine standardized role from title.
    
    Returns:
        One of: CIO, CEO, CFO, COO, CCO, Principal, Managing Director, 
                Investment Director, Portfolio Manager, Partner, Other
    """
    if not title:
        return None
    
    title_lower = title.lower()
    
    # Map titles to standardized roles
    if 'chief investment officer' in title_lower or title_lower == 'cio':
        return 'CIO'
    elif 'chief executive officer' in title_lower or title_lower == 'ceo':
        return 'CEO'
    elif 'chief financial officer' in title_lower or title_lower == 'cfo':
        return 'CFO'
    elif 'chief operating officer' in title_lower or title_lower == 'coo':
        return 'COO'
    elif 'chief compliance officer' in title_lower or title_lower == 'cco':
        return 'CCO'
    elif 'principal' in title_lower:
        return 'Principal'
    elif 'managing director' in title_lower or 'managing member' in title_lower:
        return 'Managing Director'
    elif 'investment director' in title_lower or 'head of invest' in title_lower:
        return 'Investment Director'
    elif 'portfolio manager' in title_lower:
        return 'Portfolio Manager'
    elif 'partner' in title_lower:
        return 'Partner'
    elif 'analyst' in title_lower:
        return 'Analyst'
    elif 'associate' in title_lower:
        return 'Associate'
    else:
        return 'Other'


def should_skip_page(html_content: str, url: str) -> Tuple[bool, Optional[str]]:
    """
    Check if page should be skipped due to privacy concerns or paywalls.
    
    Returns:
        (should_skip, reason)
    """
    if not html_content:
        return True, "Empty content"
    
    html_lower = html_content.lower()
    
    # Check for login/authentication requirements
    if any(phrase in html_lower for phrase in [
        'please log in',
        'please sign in',
        'login required',
        'member login',
        'authentication required',
        'subscribe to access',
        'subscription required'
    ]):
        return True, "Authentication/subscription required"
    
    # Check for "do not contact" or privacy language
    if any(phrase in html_lower for phrase in [
        'do not contact',
        'no solicitation',
        'private office',
        'invitation only',
        'invite only',
        'by invitation',
        'confidential'
    ]):
        return True, "Privacy/no contact language detected"
    
    return False, None


def calculate_confidence_level(
    source_type: str,
    has_email: bool,
    has_phone: bool,
    has_title: bool,
    is_executive: bool
) -> str:
    """
    Calculate confidence level for contact quality.
    
    Returns:
        'high', 'medium', or 'low'
    """
    # SEC ADV data is always high confidence
    if source_type == 'sec_adv':
        return 'high'
    
    # Website data with executive + contact info = high
    if source_type == 'website':
        if is_executive and (has_email or has_phone) and has_title:
            return 'high'
        elif has_title and (has_email or has_phone):
            return 'medium'
        else:
            return 'low'
    
    # Manual research is usually high if complete
    if source_type == 'manual':
        if (has_email or has_phone) and has_title:
            return 'high'
        else:
            return 'medium'
    
    # 13F filings usually provide firm-level contacts
    if source_type == '13f':
        return 'medium'
    
    # Default
    return 'low'


def validate_contact_data(
    full_name: str,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    title: Optional[str] = None
) -> Tuple[bool, list[str]]:
    """
    Validate all contact data and return validation results.
    
    Returns:
        (is_valid, list_of_errors)
    """
    errors = []
    
    # Validate name (required)
    name_valid, name_error = validate_name(full_name)
    if not name_valid:
        errors.append(f"Name: {name_error}")
    
    # Validate email (if provided)
    if email:
        email_valid, email_error = validate_email(email)
        if not email_valid:
            errors.append(f"Email: {email_error}")
    
    # Validate phone (if provided)
    if phone:
        phone_valid, phone_error = validate_phone(phone)
        if not phone_valid:
            errors.append(f"Phone: {phone_error}")
    
    # At least one contact method required
    if not email and not phone:
        errors.append("Contact must have at least one of: email or phone")
    
    is_valid = len(errors) == 0
    return is_valid, errors
