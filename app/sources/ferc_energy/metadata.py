"""
FERC Energy Filings metadata utilities.

Handles:
- Table schema definition
- CREATE TABLE SQL generation
- Data parsing and transformation
- Field mapping from EIA API to database columns
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "ferc_energy_filings"

# Dataset identifier
DATASET_ID = "ferc_energy_filings"

# Display name
DISPLAY_NAME = "FERC Energy Filings"

# Description
DESCRIPTION = (
    "State-level electricity profile data from EIA API. "
    "Includes total consumption, generation, average retail prices, "
    "revenue, and utility counts by state and year."
)

# Database columns for INSERT operations
INSERT_COLUMNS = [
    "state",
    "period",
    "total_consumption_mwh",
    "total_generation_mwh",
    "avg_retail_price_cents",
    "total_revenue_thousands",
    "num_utilities",
]

# Columns to update on conflict (upsert)
UPDATE_COLUMNS = [
    "total_consumption_mwh",
    "total_generation_mwh",
    "avg_retail_price_cents",
    "total_revenue_thousands",
    "num_utilities",
]

# Conflict columns for upsert
CONFLICT_COLUMNS = ["state", "period"]


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for FERC energy filings data.

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        state TEXT NOT NULL,
        period TEXT NOT NULL,
        total_consumption_mwh NUMERIC(18,2),
        total_generation_mwh NUMERIC(18,2),
        avg_retail_price_cents NUMERIC(8,2),
        total_revenue_thousands NUMERIC(18,2),
        num_utilities INTEGER,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(state, period)
    );

    -- Index on state for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state
        ON {TABLE_NAME} (state);

    -- Index on period for temporal queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_period
        ON {TABLE_NAME} (period);

    -- Index on total_generation for ranking
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_generation
        ON {TABLE_NAME} (total_generation_mwh DESC);

    -- Index on avg_retail_price for price queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_price
        ON {TABLE_NAME} (avg_retail_price_cents);

    COMMENT ON TABLE {TABLE_NAME} IS 'State electricity profile data via EIA API (FERC proxy)';
    """


def _safe_numeric(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert a value to float for NUMERIC columns."""
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


def parse_state_profile(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single state electricity profile record from EIA API response.

    EIA v2 API returns records with fields like:
    {
        "period": "2022",
        "stateid": "TX",
        "stateDescription": "Texas",
        "total-consumption": 123456.78,
        "total-generation": 234567.89,
        "average-retail-price": 12.34,
        "total-revenue": 56789.0,
        "total-number-of-utilities": 123
    }

    Args:
        raw: Raw record from API response

    Returns:
        Dict suitable for database insertion
    """
    state = raw.get("stateid") or raw.get("stateId") or ""
    period = raw.get("period") or ""

    if not state or not period:
        return {}

    return {
        "state": str(state).strip().upper(),
        "period": str(period).strip(),
        "total_consumption_mwh": _safe_numeric(
            raw.get("total-consumption")
        ),
        "total_generation_mwh": _safe_numeric(
            raw.get("total-generation")
        ),
        "avg_retail_price_cents": _safe_numeric(
            raw.get("average-retail-price")
        ),
        "total_revenue_thousands": _safe_numeric(
            raw.get("total-revenue")
        ),
        "num_utilities": _safe_int(
            raw.get("total-number-of-utilities")
        ),
    }


def parse_state_profiles(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Parse a list of state electricity profile records.

    Args:
        raw_records: List of raw records from EIA API

    Returns:
        List of dicts suitable for database insertion
    """
    parsed = []
    skipped = 0

    for raw in raw_records:
        record = parse_state_profile(raw)
        if record and record.get("state") and record.get("period"):
            # Ensure all columns are present for batch insert consistency
            for col in INSERT_COLUMNS:
                record.setdefault(col, None)
            parsed.append(record)
        else:
            skipped += 1

    if skipped:
        logger.warning(
            f"Skipped {skipped} energy records with missing state/period"
        )

    logger.info(f"Parsed {len(parsed)} state electricity profile records")
    return parsed
