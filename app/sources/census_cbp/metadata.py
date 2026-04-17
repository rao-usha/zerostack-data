"""
Census County Business Patterns (CBP) metadata utilities.

Handles:
- Table schema definition
- CREATE TABLE SQL generation
- Data parsing and transformation
- Field mapping from Census CBP API to database columns
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "census_business_patterns"

# Dataset identifier
DATASET_ID = "census_business_patterns"

# Display name
DISPLAY_NAME = "Census County Business Patterns"

# Description
DESCRIPTION = (
    "Census Bureau County Business Patterns (CBP). "
    "Provides establishment counts, employment, and annual payroll "
    "by state and NAICS industry classification."
)

# Database columns for INSERT operations
INSERT_COLUMNS = [
    "state_fips",
    "state_abbr",
    "naics_code",
    "naics_description",
    "establishments",
    "employees",
    "annual_payroll_thousands",
    "year",
]

# Columns to update on conflict (upsert)
UPDATE_COLUMNS = [
    "state_abbr",
    "naics_description",
    "establishments",
    "employees",
    "annual_payroll_thousands",
]

# Conflict columns for upsert
CONFLICT_COLUMNS = ["state_fips", "naics_code", "year"]

# FIPS code to state abbreviation mapping
FIPS_TO_STATE = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
    "78": "VI",
}


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for Census CBP data.

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        state_fips TEXT NOT NULL,
        state_abbr TEXT,
        naics_code TEXT NOT NULL,
        naics_description TEXT,
        establishments INTEGER,
        employees INTEGER,
        annual_payroll_thousands INTEGER,
        year INTEGER NOT NULL,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(state_fips, naics_code, year)
    );

    -- Index on state_fips for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state_fips
        ON {TABLE_NAME} (state_fips);

    -- Index on naics_code for industry queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_naics_code
        ON {TABLE_NAME} (naics_code);

    -- Index on year for temporal queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_year
        ON {TABLE_NAME} (year);

    -- Composite index for common query pattern
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state_naics
        ON {TABLE_NAME} (state_fips, naics_code);

    COMMENT ON TABLE {TABLE_NAME} IS 'Census Bureau County Business Patterns by state, NAICS industry, and year';
    """


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert a value to integer."""
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_response(raw_data: List[List[str]], year: int = 2022) -> List[Dict[str, Any]]:
    """
    Parse Census CBP API response into database records.

    Census API returns a JSON array where the first row is headers
    and subsequent rows are data.

    Args:
        raw_data: Raw API response (list of lists, first row = headers)
        year: Data year for the records

    Returns:
        List of dicts suitable for database insertion
    """
    if not raw_data or len(raw_data) < 2:
        logger.warning("Census CBP response is empty or has no data rows")
        return []

    headers = [h.strip() for h in raw_data[0]]
    parsed = []
    skipped = 0

    for row in raw_data[1:]:
        try:
            record = dict(zip(headers, row))

            state_fips = record.get("state", "").strip()
            naics_code = record.get("NAICS2017", "").strip()

            if not state_fips or not naics_code:
                skipped += 1
                continue

            parsed_record = {
                "state_fips": state_fips,
                "state_abbr": FIPS_TO_STATE.get(state_fips),
                "naics_code": naics_code,
                "naics_description": (record.get("NAICS2017_LABEL") or "").strip(),
                "establishments": _safe_int(record.get("ESTAB")),
                "employees": _safe_int(record.get("EMP")),
                "annual_payroll_thousands": _safe_int(record.get("PAYANN")),
                "year": year,
            }

            # Ensure all columns are present for batch insert consistency
            for col in INSERT_COLUMNS:
                parsed_record.setdefault(col, None)

            parsed.append(parsed_record)

        except Exception as e:
            logger.warning(f"Failed to parse CBP row: {e}")
            skipped += 1

    if skipped:
        logger.warning(f"Skipped {skipped} CBP rows with missing data")

    logger.info(f"Parsed {len(parsed)} CBP records")
    return parsed
