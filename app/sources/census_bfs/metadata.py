"""
Census Business Formation Statistics (BFS) metadata utilities.

Handles:
- Table schema definition
- CREATE TABLE SQL generation
- Data parsing and transformation
- Field mapping from Census BFS API to database columns
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "census_bfs"

# Dataset identifier
DATASET_ID = "census_bfs"

# Display name
DISPLAY_NAME = "Census Business Formation Statistics"

# Description
DESCRIPTION = (
    "Census Bureau Business Formation Statistics (BFS). "
    "Tracks business applications, high-propensity applications, "
    "applications with planned wages, and applications with first payroll "
    "by state and time period."
)

# Database columns for INSERT operations
INSERT_COLUMNS = [
    "state_fips",
    "time_period",
    "business_applications",
    "high_propensity_applications",
    "with_planned_wages",
    "with_first_payroll",
]

# Columns to update on conflict (upsert)
UPDATE_COLUMNS = [
    "business_applications",
    "high_propensity_applications",
    "with_planned_wages",
    "with_first_payroll",
]

# Conflict columns for upsert
CONFLICT_COLUMNS = ["state_fips", "time_period"]

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
    Generate CREATE TABLE SQL for Census BFS data.

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        state_fips TEXT NOT NULL DEFAULT 'US',
        time_period TEXT NOT NULL,
        business_applications INTEGER,
        high_propensity_applications INTEGER,
        with_planned_wages INTEGER,
        with_first_payroll INTEGER,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(state_fips, time_period)
    );

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_time_period
        ON {TABLE_NAME} (time_period);

    COMMENT ON TABLE {TABLE_NAME} IS 'Census Bureau Business Formation Statistics — monthly national data';
    """


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert a value to integer."""
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_response(raw_data) -> List[Dict[str, Any]]:
    """
    Parse Census BFS API response into database records.

    The client returns a list of dicts with keys:
    time_period, BA_BA, BA_WBA, BA_HBA, BA_CBA

    Args:
        raw_data: List of dicts from CensusBFSClient.fetch_business_formation()

    Returns:
        List of dicts suitable for database insertion
    """
    if not raw_data:
        logger.warning("Census BFS response is empty")
        return []

    parsed = []
    for record in raw_data:
        try:
            time_period = record.get("time_period", "")
            if not time_period:
                continue

            parsed_record = {
                "state_fips": "US",
                "time_period": time_period,
                "business_applications": _safe_int(record.get("BA_BA")),
                "high_propensity_applications": _safe_int(record.get("BA_HBA")),
                "with_planned_wages": _safe_int(record.get("BA_WBA")),
                "with_first_payroll": _safe_int(record.get("BA_CBA")),
            }

            for col in INSERT_COLUMNS:
                parsed_record.setdefault(col, None)

            parsed.append(parsed_record)
        except Exception as e:
            logger.warning(f"Failed to parse BFS row: {e}")

    logger.info(f"Parsed {len(parsed)} BFS records")
    return parsed
