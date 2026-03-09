"""
EPA ECHO metadata utilities.

Handles:
- Table schema definition
- CREATE TABLE SQL generation
- Data parsing and transformation
- Field mapping from ECHO API to database columns
"""

import json
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "epa_echo_facilities"

# Dataset identifier
DATASET_ID = "epa_echo_facilities"

# Display name
DISPLAY_NAME = "EPA ECHO Facilities"

# Description
DESCRIPTION = (
    "EPA Enforcement and Compliance History Online (ECHO) facility data. "
    "Includes facility location, compliance status, violations, inspections, "
    "and penalties across environmental media programs (AIR, WATER, RCRA, SDWA)."
)

# Valid media programs
VALID_MEDIA_PROGRAMS = ["AIR", "WATER", "RCRA", "SDWA", "ALL"]

# Database columns for INSERT operations
INSERT_COLUMNS = [
    "facility_id",
    "facility_name",
    "street_address",
    "city",
    "state",
    "zip_code",
    "county",
    "latitude",
    "longitude",
    "naics_codes",
    "sic_codes",
    "media_programs",
    "compliance_status",
    "violation_count",
    "inspection_count",
    "penalty_amount",
    "last_inspection_date",
    "last_penalty_date",
    "dfr_url",
    "epa_region",
    "facility_type",
]

# Columns to update on conflict (upsert)
UPDATE_COLUMNS = [
    "facility_name",
    "street_address",
    "city",
    "state",
    "zip_code",
    "county",
    "latitude",
    "longitude",
    "naics_codes",
    "sic_codes",
    "media_programs",
    "compliance_status",
    "violation_count",
    "inspection_count",
    "penalty_amount",
    "last_inspection_date",
    "last_penalty_date",
    "dfr_url",
    "epa_region",
    "facility_type",
]

# Conflict column for upsert
CONFLICT_COLUMNS = ["facility_id"]


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for EPA ECHO facilities data.

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        facility_id TEXT PRIMARY KEY,
        facility_name TEXT,
        street_address TEXT,
        city TEXT,
        state TEXT,
        zip_code TEXT,
        county TEXT,
        latitude NUMERIC,
        longitude NUMERIC,
        naics_codes JSONB,
        sic_codes JSONB,
        media_programs JSONB,
        compliance_status TEXT,
        violation_count INTEGER DEFAULT 0,
        inspection_count INTEGER DEFAULT 0,
        penalty_amount NUMERIC DEFAULT 0,
        last_inspection_date DATE,
        last_penalty_date DATE,
        dfr_url TEXT,
        epa_region TEXT,
        facility_type TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );

    -- Index on state for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state
        ON {TABLE_NAME} (state);

    -- Index on zip_code for local searches
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_zip_code
        ON {TABLE_NAME} (zip_code);

    -- Index on compliance_status for filtering
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_compliance_status
        ON {TABLE_NAME} (compliance_status);

    -- GIN index on naics_codes for JSONB containment queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_naics_codes
        ON {TABLE_NAME} USING GIN (naics_codes);

    -- GIN index on media_programs for JSONB containment queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_media_programs
        ON {TABLE_NAME} USING GIN (media_programs);

    -- Index on county for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_county
        ON {TABLE_NAME} (county);

    -- Spatial index approximation via lat/lon
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_lat_lon
        ON {TABLE_NAME} (latitude, longitude);

    COMMENT ON TABLE {TABLE_NAME} IS 'EPA ECHO facility compliance and enforcement data';
    """


def _safe_int(value: Any, default: int = 0) -> int:
    """Safely convert a value to integer."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert a value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_date(value: Any) -> Optional[str]:
    """
    Safely parse a date string.

    ECHO API returns dates in various formats. We normalize to YYYY-MM-DD
    or return None if unparseable.
    """
    if not value or value in ("", "None", "null"):
        return None

    value = str(value).strip()

    # Already in YYYY-MM-DD
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return value

    # MM/DD/YYYY format
    if "/" in value:
        parts = value.split("/")
        if len(parts) == 3:
            try:
                month, day, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except (ValueError, IndexError):
                pass

    return None


def _parse_list_field(value: Any) -> List[str]:
    """
    Parse a comma-separated or space-separated string into a list.

    ECHO API returns some fields as comma-separated strings.
    """
    if not value or value in ("", "None", "null"):
        return []

    if isinstance(value, list):
        return [str(v).strip() for v in value if v]

    value = str(value).strip()
    if not value:
        return []

    # Try comma-separated first, then space-separated
    if "," in value:
        return [v.strip() for v in value.split(",") if v.strip()]
    return [v.strip() for v in value.split() if v.strip()]


def parse_facility(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single facility record from the ECHO API response.

    Maps ECHO API field names to database column names.

    Args:
        raw: Raw facility record from API response

    Returns:
        Dict suitable for database insertion
    """
    # Extract the registry ID (primary identifier)
    # ECHO API uses "RegistryID" (capital D) in get_qid responses
    facility_id = (
        raw.get("RegistryID")
        or raw.get("RegistryId")
        or raw.get("FacilityId")
        or raw.get("SourceID")
    )

    if not facility_id:
        return {}

    # Parse NAICS and SIC codes into lists
    # ECHO uses FacNAICSCodes and FacSICCodes in get_qid responses
    naics_raw = raw.get("FacNAICSCodes") or raw.get("NAICSCodes") or ""
    naics_codes = _parse_list_field(naics_raw)

    sic_raw = raw.get("FacSICCodes") or raw.get("SICCodes") or ""
    sic_codes = _parse_list_field(sic_raw)

    # Detect media programs from flag fields
    media_programs = []
    if raw.get("AIRFlag") == "Y":
        media_programs.append("AIR")
    if raw.get("CWAComplianceStatus"):
        media_programs.append("WATER")
    if raw.get("RCRAComplianceStatus"):
        media_programs.append("RCRA")
    if raw.get("SDWAComplianceStatus"):
        media_programs.append("SDWA")
    if raw.get("TRIFlag") == "Y":
        media_programs.append("TRI")

    # Parse penalty amount — strip $ and commas from formatted strings
    penalty_raw = raw.get("TotalPenalties") or raw.get("CAAPenalties") or "0"
    if isinstance(penalty_raw, str):
        penalty_raw = penalty_raw.replace("$", "").replace(",", "").strip()
    penalty_amount = _safe_float(penalty_raw, 0.0)

    # Build DFR URL
    dfr_url = f"https://echo.epa.gov/detailed-facility-report?fid={facility_id}"

    return {
        "facility_id": str(facility_id).strip(),
        "facility_name": (raw.get("FacName") or "").strip(),
        "street_address": (raw.get("FacStreet") or "").strip(),
        "city": (raw.get("FacCity") or "").strip(),
        "state": (raw.get("FacState") or "").strip(),
        "zip_code": (raw.get("FacZip") or "").strip()[:10],
        "county": (raw.get("FacCounty") or "").strip(),
        "latitude": _safe_float(raw.get("FacLat")),
        "longitude": _safe_float(raw.get("FacLong")),
        "naics_codes": json.dumps(naics_codes) if naics_codes else None,
        "sic_codes": json.dumps(sic_codes) if sic_codes else None,
        "media_programs": json.dumps(media_programs) if media_programs else None,
        "compliance_status": (
            raw.get("FacComplianceStatus") or ""
        ).strip(),
        "violation_count": _safe_int(raw.get("FacQtrsWithNC")),
        "inspection_count": _safe_int(raw.get("FacInspectionCount")),
        "penalty_amount": penalty_amount,
        "last_inspection_date": _safe_date(raw.get("FacDateLastInspection")),
        "last_penalty_date": _safe_date(raw.get("FacDateLastPenalty")),
        "dfr_url": dfr_url,
        "epa_region": (raw.get("EPARegion") or "").strip(),
        "facility_type": (raw.get("FacActiveFlag") or "").strip(),
    }


def parse_facilities(raw_facilities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse a list of facility records from the ECHO API response.

    Args:
        raw_facilities: List of raw facility records from API

    Returns:
        List of dicts suitable for database insertion
    """
    parsed = []
    skipped = 0

    for raw in raw_facilities:
        record = parse_facility(raw)
        if record and record.get("facility_id"):
            # Ensure all columns are present for batch insert consistency
            for col in INSERT_COLUMNS:
                record.setdefault(col, None)
            parsed.append(record)
        else:
            skipped += 1

    if skipped:
        logger.warning(f"Skipped {skipped} facilities with missing facility_id")

    logger.info(f"Parsed {len(parsed)} facility records")
    return parsed
