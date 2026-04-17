"""
EPA GHGRP metadata utilities.

Handles:
- Table schema definition
- CREATE TABLE SQL generation
- Data parsing and transformation
- Field mapping from Envirofacts API to database columns
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "epa_ghg_emissions"

# Dataset identifier
DATASET_ID = "epa_ghg_emissions"

# Display name
DISPLAY_NAME = "EPA Greenhouse Gas Emissions"

# Description
DESCRIPTION = (
    "EPA Greenhouse Gas Reporting Program (GHGRP) facility-level emissions data. "
    "Includes facility location, industry type, total reported emissions, "
    "reporting year, and parent company information."
)

# Database columns for INSERT operations
INSERT_COLUMNS = [
    "facility_id",
    "facility_name",
    "state",
    "city",
    "zip_code",
    "latitude",
    "longitude",
    "industry_type",
    "total_reported_emissions",
    "reporting_year",
    "parent_company",
]

# Columns to update on conflict (upsert)
UPDATE_COLUMNS = [
    "facility_name",
    "state",
    "city",
    "zip_code",
    "latitude",
    "longitude",
    "industry_type",
    "total_reported_emissions",
    "parent_company",
]

# Conflict columns for upsert (composite unique key)
CONFLICT_COLUMNS = ["facility_id", "reporting_year"]


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for EPA GHGRP emissions data.

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        facility_id TEXT NOT NULL,
        facility_name TEXT,
        state TEXT,
        city TEXT,
        zip_code TEXT,
        latitude NUMERIC(10,6),
        longitude NUMERIC(10,6),
        industry_type TEXT,
        total_reported_emissions NUMERIC(18,2),
        reporting_year INTEGER,
        parent_company TEXT,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(facility_id, reporting_year)
    );

    -- Index on state for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state
        ON {TABLE_NAME} (state);

    -- Index on reporting_year for temporal queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_year
        ON {TABLE_NAME} (reporting_year);

    -- Index on industry_type for sector filtering
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_industry
        ON {TABLE_NAME} (industry_type);

    -- Spatial index approximation via lat/lon
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_lat_lon
        ON {TABLE_NAME} (latitude, longitude);

    COMMENT ON TABLE {TABLE_NAME} IS 'EPA Greenhouse Gas Reporting Program facility emissions data';
    """


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert a value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert a value to integer."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _safe_str(value: Any, default: Optional[str] = None) -> Optional[str]:
    """Safely convert a value to stripped string."""
    if value is None or str(value).strip() in ("", "None", "null"):
        return default
    return str(value).strip()


def parse_facility(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single facility record from the Envirofacts API response.

    Maps Envirofacts field names to database column names.

    Args:
        raw: Raw facility record from API response

    Returns:
        Dict suitable for database insertion
    """
    facility_id = _safe_str(
        raw.get("FACILITY_ID") or raw.get("facility_id")
    )

    if not facility_id:
        return {}

    return {
        "facility_id": facility_id,
        "facility_name": _safe_str(
            raw.get("FACILITY_NAME") or raw.get("facility_name")
        ),
        "state": _safe_str(
            raw.get("STATE") or raw.get("state")
        ),
        "city": _safe_str(
            raw.get("CITY") or raw.get("city")
        ),
        "zip_code": _safe_str(
            raw.get("ZIP") or raw.get("zip") or raw.get("ZIP_CODE") or raw.get("zip_code")
        ),
        "latitude": _safe_float(
            raw.get("LATITUDE") or raw.get("latitude")
        ),
        "longitude": _safe_float(
            raw.get("LONGITUDE") or raw.get("longitude")
        ),
        "industry_type": _safe_str(
            raw.get("INDUSTRY_TYPE") or raw.get("industry_type")
            or raw.get("PRIMARY_NAICS_CODE") or raw.get("primary_naics_code")
        ),
        "total_reported_emissions": _safe_float(
            raw.get("TOTAL_REPORTED_DIRECT_EMISSIONS")
            or raw.get("total_reported_direct_emissions")
        ),
        "reporting_year": _safe_int(
            raw.get("REPORTING_YEAR") or raw.get("reporting_year")
            or raw.get("YEAR") or raw.get("year")
        ),
        "parent_company": _safe_str(
            raw.get("PARENT_COMPANY") or raw.get("parent_company")
            or raw.get("PARENT_COMPANIES") or raw.get("parent_companies")
        ),
    }


def parse_facilities(raw_facilities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse a list of facility records from the Envirofacts API response.

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
        logger.warning(f"Skipped {skipped} GHGRP facilities with missing facility_id")

    logger.info(f"Parsed {len(parsed)} GHGRP facility records")
    return parsed
