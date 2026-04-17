"""
CMS Hospital Provider metadata utilities.

Handles:
- Table schema definition
- CREATE TABLE SQL generation
- Data parsing and transformation
- Field mapping from CMS API to database columns
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "cms_hospitals"

# Dataset identifier
DATASET_ID = "cms_hospitals"

# Display name
DISPLAY_NAME = "CMS Hospital Provider Data"

# Description
DESCRIPTION = (
    "CMS Hospital Compare provider data including hospital type, ownership, "
    "emergency services availability, overall quality rating, and domain-specific "
    "ratings for mortality, readmission, patient experience, effectiveness, "
    "timeliness, and medical imaging."
)

# Database columns for INSERT operations
INSERT_COLUMNS = [
    "facility_id",
    "facility_name",
    "address",
    "city",
    "state",
    "zip_code",
    "county",
    "hospital_type",
    "ownership",
    "emergency_services",
    "overall_rating",
    "mortality_rating",
    "readmission_rating",
    "patient_experience_rating",
    "effectiveness_rating",
    "timeliness_rating",
    "imaging_rating",
]

# Columns to update on conflict (upsert)
UPDATE_COLUMNS = [
    "facility_name",
    "address",
    "city",
    "state",
    "zip_code",
    "county",
    "hospital_type",
    "ownership",
    "emergency_services",
    "overall_rating",
    "mortality_rating",
    "readmission_rating",
    "patient_experience_rating",
    "effectiveness_rating",
    "timeliness_rating",
    "imaging_rating",
]

# Conflict column for upsert
CONFLICT_COLUMNS = ["facility_id"]


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for CMS hospital provider data.

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        facility_id TEXT NOT NULL UNIQUE,
        facility_name TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip_code TEXT,
        county TEXT,
        hospital_type TEXT,
        ownership TEXT,
        emergency_services BOOLEAN,
        overall_rating INTEGER,
        mortality_rating TEXT,
        readmission_rating TEXT,
        patient_experience_rating TEXT,
        effectiveness_rating TEXT,
        timeliness_rating TEXT,
        imaging_rating TEXT,
        ingested_at TIMESTAMP DEFAULT NOW()
    );

    -- Index on state for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state
        ON {TABLE_NAME} (state);

    -- Index on hospital_type for filtering
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_type
        ON {TABLE_NAME} (hospital_type);

    -- Index on overall_rating for quality filtering
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_rating
        ON {TABLE_NAME} (overall_rating);

    -- Index on ownership for filtering
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_ownership
        ON {TABLE_NAME} (ownership);

    -- Index on county for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_county
        ON {TABLE_NAME} (county);

    COMMENT ON TABLE {TABLE_NAME} IS 'CMS Hospital Compare provider quality and rating data';
    """


def _safe_str(value: Any, default: Optional[str] = None) -> Optional[str]:
    """Safely convert a value to stripped string."""
    if value is None or str(value).strip() in ("", "None", "null", "Not Available"):
        return default
    return str(value).strip()


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert a value to integer."""
    if value is None:
        return default
    try:
        val_str = str(value).strip()
        if val_str in ("", "Not Available", "None", "null"):
            return default
        return int(val_str)
    except (ValueError, TypeError):
        return default


def _safe_bool(value: Any, default: Optional[bool] = None) -> Optional[bool]:
    """Safely convert a value to boolean."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    val_str = str(value).strip().lower()
    if val_str in ("yes", "true", "1", "y"):
        return True
    if val_str in ("no", "false", "0", "n"):
        return False
    return default


def parse_hospital(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single hospital record from the CMS API response.

    Maps CMS API field names to database column names.

    Args:
        raw: Raw hospital record from API response

    Returns:
        Dict suitable for database insertion
    """
    # CMS uses "facility_id" or "provider_id" or "Facility ID"
    facility_id = _safe_str(
        raw.get("facility_id")
        or raw.get("provider_id")
        or raw.get("Facility ID")
        or raw.get("Provider ID")
        or raw.get("cms_certification_number_ccn")
    )

    if not facility_id:
        return {}

    return {
        "facility_id": facility_id,
        "facility_name": _safe_str(
            raw.get("facility_name")
            or raw.get("hospital_name")
            or raw.get("Hospital Name")
        ),
        "address": _safe_str(
            raw.get("address")
            or raw.get("address_line_1")
            or raw.get("Address")
        ),
        "city": _safe_str(
            raw.get("city")
            or raw.get("city_town")
            or raw.get("City/Town")
        ),
        "state": _safe_str(
            raw.get("state")
            or raw.get("State")
        ),
        "zip_code": _safe_str(
            raw.get("zip_code")
            or raw.get("zip")
            or raw.get("ZIP Code")
        ),
        "county": _safe_str(
            raw.get("county_parish")
            or raw.get("county_name")
            or raw.get("county")
            or raw.get("County/Parish")
        ),
        "hospital_type": _safe_str(
            raw.get("hospital_type")
            or raw.get("Hospital Type")
        ),
        "ownership": _safe_str(
            raw.get("hospital_ownership")
            or raw.get("ownership")
            or raw.get("Hospital Ownership")
        ),
        "emergency_services": _safe_bool(
            raw.get("emergency_services")
            or raw.get("Emergency Services")
        ),
        "overall_rating": _safe_int(
            raw.get("hospital_overall_rating")
            or raw.get("overall_rating")
            or raw.get("Hospital overall rating")
        ),
        "mortality_rating": _safe_str(
            raw.get("mortality_national_comparison")
            or raw.get("Mortality national comparison")
        ),
        "readmission_rating": _safe_str(
            raw.get("readmission_national_comparison")
            or raw.get("Readmission national comparison")
        ),
        "patient_experience_rating": _safe_str(
            raw.get("patient_experience_national_comparison")
            or raw.get("Patient experience national comparison")
        ),
        "effectiveness_rating": _safe_str(
            raw.get("effectiveness_of_care_national_comparison")
            or raw.get("Effectiveness of care national comparison")
        ),
        "timeliness_rating": _safe_str(
            raw.get("timeliness_of_care_national_comparison")
            or raw.get("Timeliness of care national comparison")
        ),
        "imaging_rating": _safe_str(
            raw.get("efficient_use_of_medical_imaging_national_comparison")
            or raw.get("Efficient use of medical imaging national comparison")
        ),
    }


def parse_hospitals(raw_hospitals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse a list of hospital records from the CMS API response.

    Args:
        raw_hospitals: List of raw hospital records from API

    Returns:
        List of dicts suitable for database insertion
    """
    parsed = []
    skipped = 0

    for raw in raw_hospitals:
        record = parse_hospital(raw)
        if record and record.get("facility_id"):
            # Ensure all columns are present for batch insert consistency
            for col in INSERT_COLUMNS:
                record.setdefault(col, None)
            parsed.append(record)
        else:
            skipped += 1

    if skipped:
        logger.warning(f"Skipped {skipped} hospitals with missing facility_id")

    logger.info(f"Parsed {len(parsed)} hospital records")
    return parsed
