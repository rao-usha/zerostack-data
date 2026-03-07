"""
NPPES NPI Registry metadata and schema definitions.

Defines the database schema, taxonomy code constants, and parsing
utilities for NPPES provider data.
"""

import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table and dataset metadata
# ---------------------------------------------------------------------------

TABLE_NAME = "nppes_providers"
DATASET_ID = "nppes_providers"
DISPLAY_NAME = "NPPES NPI Provider Registry"
DESCRIPTION = (
    "National Plan and Provider Enumeration System (NPPES) NPI Registry. "
    "Contains individual and organizational healthcare provider data including "
    "addresses, taxonomy codes, and enumeration dates."
)
SOURCE_URL = "https://npiregistry.cms.hhs.gov/api/"


# ---------------------------------------------------------------------------
# Column definitions
# ---------------------------------------------------------------------------

COLUMNS = {
    "npi": {"type": "TEXT", "description": "10-digit National Provider Identifier"},
    "entity_type": {
        "type": "TEXT",
        "description": "Entity type: 1=Individual, 2=Organization",
    },
    "legal_name": {
        "type": "TEXT",
        "description": "Legal name (org name or first+last for individuals)",
    },
    "first_name": {"type": "TEXT", "description": "Individual provider first name"},
    "last_name": {"type": "TEXT", "description": "Individual provider last name"},
    "credential": {"type": "TEXT", "description": "Provider credential (e.g., MD, DO)"},
    "dba_name": {"type": "TEXT", "description": "Doing Business As name"},
    "gender": {"type": "TEXT", "description": "Provider gender (M/F)"},
    "practice_address_line1": {"type": "TEXT", "description": "Practice address line 1"},
    "practice_address_line2": {"type": "TEXT", "description": "Practice address line 2"},
    "practice_city": {"type": "TEXT", "description": "Practice city"},
    "practice_state": {"type": "TEXT", "description": "Practice state abbreviation"},
    "practice_zip": {"type": "TEXT", "description": "Practice ZIP code"},
    "practice_phone": {"type": "TEXT", "description": "Practice phone number"},
    "practice_fax": {"type": "TEXT", "description": "Practice fax number"},
    "mailing_address_line1": {"type": "TEXT", "description": "Mailing address line 1"},
    "mailing_address_line2": {"type": "TEXT", "description": "Mailing address line 2"},
    "mailing_city": {"type": "TEXT", "description": "Mailing city"},
    "mailing_state": {"type": "TEXT", "description": "Mailing state abbreviation"},
    "mailing_zip": {"type": "TEXT", "description": "Mailing ZIP code"},
    "taxonomy_code": {
        "type": "TEXT",
        "description": "Primary taxonomy code (Healthcare Provider Taxonomy Code Set)",
    },
    "taxonomy_description": {
        "type": "TEXT",
        "description": "Primary taxonomy description",
    },
    "taxonomy_license": {"type": "TEXT", "description": "Taxonomy state license number"},
    "taxonomy_state": {"type": "TEXT", "description": "Taxonomy state"},
    "enumeration_date": {
        "type": "DATE",
        "description": "Date the NPI was assigned",
    },
    "last_updated": {
        "type": "DATE",
        "description": "Date the NPI record was last updated",
    },
    "status": {"type": "TEXT", "description": "NPI status: A=Active, D=Deactivated"},
    "sole_proprietor": {"type": "TEXT", "description": "Sole proprietor flag (Y/N/X)"},
    "organization_subpart": {
        "type": "TEXT",
        "description": "Organization subpart flag (Y/N)",
    },
    "ingestion_timestamp": {
        "type": "TIMESTAMP",
        "description": "When this record was ingested",
    },
}

COLUMN_NAMES = [c for c in COLUMNS.keys() if c != "ingestion_timestamp"]

CONFLICT_COLUMNS = ["npi"]

UPDATE_COLUMNS = [c for c in COLUMN_NAMES if c != "npi"]


# ---------------------------------------------------------------------------
# CREATE TABLE SQL
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    npi TEXT PRIMARY KEY,
    entity_type TEXT,
    legal_name TEXT,
    first_name TEXT,
    last_name TEXT,
    credential TEXT,
    dba_name TEXT,
    gender TEXT,
    practice_address_line1 TEXT,
    practice_address_line2 TEXT,
    practice_city TEXT,
    practice_state TEXT,
    practice_zip TEXT,
    practice_phone TEXT,
    practice_fax TEXT,
    mailing_address_line1 TEXT,
    mailing_address_line2 TEXT,
    mailing_city TEXT,
    mailing_state TEXT,
    mailing_zip TEXT,
    taxonomy_code TEXT,
    taxonomy_description TEXT,
    taxonomy_license TEXT,
    taxonomy_state TEXT,
    enumeration_date DATE,
    last_updated DATE,
    status TEXT,
    sole_proprietor TEXT,
    organization_subpart TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state
    ON {TABLE_NAME}(practice_state);
CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_taxonomy
    ON {TABLE_NAME}(taxonomy_code);
CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_taxonomy_desc
    ON {TABLE_NAME}(taxonomy_description);
CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_entity_type
    ON {TABLE_NAME}(entity_type);
CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_city_state
    ON {TABLE_NAME}(practice_state, practice_city);
CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_zip
    ON {TABLE_NAME}(practice_zip);
CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_legal_name
    ON {TABLE_NAME}(legal_name);
"""


# ---------------------------------------------------------------------------
# MedSpa-relevant taxonomy codes
# ---------------------------------------------------------------------------

MEDSPA_TAXONOMY_CODES = {
    "207N00000X": "Dermatology",
    "261QM0801X": "Ambulatory Health Care Facilities - Clinic/Center (Aesthetic/Cosmetic Medicine)",
    "261Q00000X": "Ambulatory Health Care Facilities - Clinic/Center",
    "207ND0900X": "Dermatology - Dermatopathology",
    "1223P0221X": "Dentist - Prosthodontics",
    "174400000X": "Specialist - Nurse Practitioner",
    "207RC0000X": "Internal Medicine - Cardiovascular Disease",
    "208200000X": "Plastic Surgery",
    "2085R0001X": "Surgery - Reconstructive Surgery",
    "207RX0202X": "Internal Medicine - Medical Oncology",
    "364S00000X": "Clinical Nurse Specialist",
    "363L00000X": "Nurse Practitioner",
    "363A00000X": "Physician Assistant",
    "1223S0112X": "Dentist - Oral and Maxillofacial Surgery",
}

# Search terms for the NPPES API taxonomy_description parameter.
# The API does partial matching, so we use short distinctive terms
# rather than the full hierarchical classification string.
TAXONOMY_SEARCH_TERMS = {
    "207N00000X": "Dermatology",
    "261QM0801X": "Clinic/Center",
    "261Q00000X": "Clinic/Center",
    "207ND0900X": "Dermatopathology",
    "1223P0221X": "Prosthodontics",
    "174400000X": "Nurse Practitioner",
    "207RC0000X": "Cardiovascular Disease",
    "208200000X": "Plastic Surgery",
    "2085R0001X": "Reconstructive Surgery",
    "207RX0202X": "Medical Oncology",
    "364S00000X": "Clinical Nurse Specialist",
    "363L00000X": "Nurse Practitioner",
    "363A00000X": "Physician Assistant",
    "1223S0112X": "Oral and Maxillofacial Surgery",
}

# Taxonomy codes most strongly associated with aesthetic/cosmetic services
AESTHETIC_TAXONOMY_CODES = [
    "207N00000X",   # Dermatology
    "261QM0801X",   # Aesthetic/Cosmetic Medicine Clinic
    "208200000X",   # Plastic Surgery
    "174400000X",   # Nurse Practitioner
    "363L00000X",   # Nurse Practitioner
    "363A00000X",   # Physician Assistant
]


# ---------------------------------------------------------------------------
# Parsing utilities
# ---------------------------------------------------------------------------


def parse_provider_record(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single NPPES API result into a flat row dict for DB insertion.

    The NPPES API returns nested structures for addresses and taxonomies.
    This function flattens them into the DB schema.

    Args:
        result: Single result dict from the NPPES API

    Returns:
        Flat dict matching COLUMN_NAMES
    """
    basic = result.get("basic", {})
    entity_type_code = result.get("enumeration_type", "")

    # Determine entity type code (1 or 2)
    if entity_type_code == "NPI-1":
        entity_type = "1"
    elif entity_type_code == "NPI-2":
        entity_type = "2"
    else:
        entity_type = entity_type_code

    # Build legal name
    first_name = basic.get("first_name", "")
    last_name = basic.get("last_name", "")
    org_name = basic.get("organization_name", "")

    if entity_type == "2":
        legal_name = org_name
    else:
        parts = [p for p in [first_name, last_name] if p]
        legal_name = " ".join(parts) if parts else None

    # Extract practice address (practice_location in API)
    practice_addr = _extract_address(result.get("addresses", []), "LOCATION")
    mailing_addr = _extract_address(result.get("addresses", []), "MAILING")

    # Extract primary taxonomy
    taxonomy = _extract_primary_taxonomy(result.get("taxonomies", []))

    row = {
        "npi": str(result.get("number", "")),
        "entity_type": entity_type,
        "legal_name": legal_name or None,
        "first_name": first_name or None,
        "last_name": last_name or None,
        "credential": basic.get("credential") or None,
        "dba_name": (
            basic.get("name")  # NPI-1 uses "name" for DBA
            or basic.get("authorized_official_organization_name")
            or None
        ),
        "gender": basic.get("gender") or None,
        # Practice address
        "practice_address_line1": practice_addr.get("address_1"),
        "practice_address_line2": practice_addr.get("address_2"),
        "practice_city": practice_addr.get("city"),
        "practice_state": practice_addr.get("state"),
        "practice_zip": practice_addr.get("postal_code"),
        "practice_phone": _clean_phone(practice_addr.get("telephone_number")),
        "practice_fax": _clean_phone(practice_addr.get("fax_number")),
        # Mailing address
        "mailing_address_line1": mailing_addr.get("address_1"),
        "mailing_address_line2": mailing_addr.get("address_2"),
        "mailing_city": mailing_addr.get("city"),
        "mailing_state": mailing_addr.get("state"),
        "mailing_zip": mailing_addr.get("postal_code"),
        # Taxonomy
        "taxonomy_code": taxonomy.get("code"),
        "taxonomy_description": taxonomy.get("desc"),
        "taxonomy_license": taxonomy.get("license"),
        "taxonomy_state": taxonomy.get("state"),
        # Dates
        "enumeration_date": _parse_date(basic.get("enumeration_date")),
        "last_updated": _parse_date(basic.get("last_updated")),
        # Status
        "status": basic.get("status", "A"),
        "sole_proprietor": basic.get("sole_proprietor") or None,
        "organization_subpart": basic.get("organization_subpart") or None,
    }

    return row


def _extract_address(
    addresses: List[Dict[str, Any]], address_purpose: str
) -> Dict[str, Any]:
    """
    Extract an address by purpose from the addresses list.

    The NPPES API returns addresses as a list with "address_purpose"
    being either "LOCATION" (practice) or "MAILING".

    Args:
        addresses: List of address dicts from API
        address_purpose: "LOCATION" or "MAILING"

    Returns:
        Address dict (or empty dict if not found)
    """
    for addr in addresses:
        if addr.get("address_purpose") == address_purpose:
            return addr
    return {}


def _extract_primary_taxonomy(
    taxonomies: List[Dict[str, Any]],
) -> Dict[str, Optional[str]]:
    """
    Extract the primary taxonomy from the taxonomies list.

    The NPPES API returns a list of taxonomy objects. The primary one
    has "primary" set to True. Falls back to first in list.

    Args:
        taxonomies: List of taxonomy dicts from API

    Returns:
        Dict with code, desc, license, state keys
    """
    primary = None

    # Look for the primary taxonomy
    for tax in taxonomies:
        if tax.get("primary") is True:
            primary = tax
            break

    # Fall back to first taxonomy if no primary flag
    if not primary and taxonomies:
        primary = taxonomies[0]

    if not primary:
        return {"code": None, "desc": None, "license": None, "state": None}

    return {
        "code": primary.get("code"),
        "desc": primary.get("desc"),
        "license": primary.get("license"),
        "state": primary.get("state"),
    }


def _clean_phone(phone: Optional[str]) -> Optional[str]:
    """
    Clean phone number string.

    Removes common formatting characters but preserves the digits.

    Args:
        phone: Raw phone number string

    Returns:
        Cleaned phone string or None
    """
    if not phone:
        return None
    # Strip whitespace; keep digits and hyphens
    cleaned = phone.strip()
    return cleaned if cleaned else None


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """
    Parse NPPES date string to ISO format.

    NPPES dates come as MM-DD-YYYY or YYYY-MM-DD.

    Args:
        date_str: Date string from API

    Returns:
        ISO date string (YYYY-MM-DD) or None
    """
    if not date_str:
        return None

    # Handle MM-DD-YYYY format
    parts = date_str.split("-")
    if len(parts) == 3:
        if len(parts[0]) == 2:
            # MM-DD-YYYY -> YYYY-MM-DD
            return f"{parts[2]}-{parts[0]}-{parts[1]}"
        else:
            # Already YYYY-MM-DD
            return date_str

    return date_str
