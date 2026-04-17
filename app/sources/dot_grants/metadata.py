"""
DOT Infrastructure Grants metadata utilities.

Handles:
- Table schema definition
- CREATE TABLE SQL generation
- Data parsing and transformation
- Field mapping from USAspending API to database columns
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "dot_infra_grants"

# Dataset identifier
DATASET_ID = "dot_infra_grants"

# Display name
DISPLAY_NAME = "DOT Infrastructure Grants"

# Description
DESCRIPTION = (
    "Department of Transportation infrastructure grant spending by state, "
    "sourced from USAspending.gov. Includes aggregated grant amounts, "
    "transaction counts, population, and per-capita spending."
)

# Database columns for INSERT operations
INSERT_COLUMNS = [
    "state",
    "agency",
    "aggregated_amount",
    "transaction_count",
    "population",
    "per_capita",
    "fiscal_year",
]

# Columns to update on conflict (upsert)
UPDATE_COLUMNS = [
    "aggregated_amount",
    "transaction_count",
    "population",
    "per_capita",
]

# Conflict columns for upsert (composite unique key)
CONFLICT_COLUMNS = ["state", "agency", "fiscal_year"]


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for DOT infrastructure grants data.

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        state TEXT NOT NULL,
        agency TEXT NOT NULL,
        aggregated_amount NUMERIC(18,2),
        transaction_count INTEGER,
        population INTEGER,
        per_capita NUMERIC(12,2),
        fiscal_year INTEGER,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(state, agency, fiscal_year)
    );

    -- Index on state for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_state
        ON {TABLE_NAME} (state);

    -- Index on fiscal_year for temporal queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_year
        ON {TABLE_NAME} (fiscal_year);

    -- Index on agency for filtering
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_agency
        ON {TABLE_NAME} (agency);

    COMMENT ON TABLE {TABLE_NAME} IS 'DOT infrastructure grant spending by state from USAspending.gov';
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
        return int(float(value))  # handle "123.0" strings
    except (ValueError, TypeError):
        return default


def _safe_str(value: Any, default: Optional[str] = None) -> Optional[str]:
    """Safely convert a value to stripped string."""
    if value is None or str(value).strip() in ("", "None", "null"):
        return default
    return str(value).strip()


def parse_grant_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a single state-level grant record from the USAspending API response.

    Maps USAspending field names to database column names.

    Args:
        raw: Raw state-level spending record from API response

    Returns:
        Dict suitable for database insertion
    """
    # USAspending returns state as "display_name" or "shape_code"
    state = _safe_str(
        raw.get("shape_code")
        or raw.get("display_name")
        or raw.get("state")
    )

    if not state:
        return {}

    aggregated_amount = _safe_float(
        raw.get("aggregated_amount")
        or raw.get("amount")
    )
    population = _safe_int(
        raw.get("population")
    )
    per_capita = _safe_float(
        raw.get("per_capita")
    )

    # Calculate per_capita if we have amount and population but no per_capita
    if per_capita is None and aggregated_amount and population and population > 0:
        per_capita = round(aggregated_amount / population, 2)

    return {
        "state": state,
        "agency": _safe_str(raw.get("agency"), "Department of Transportation"),
        "aggregated_amount": aggregated_amount,
        "transaction_count": _safe_int(
            raw.get("transaction_count")
            or raw.get("award_count")
        ),
        "population": population,
        "per_capita": per_capita,
        "fiscal_year": _safe_int(raw.get("fiscal_year")),
    }


def parse_grant_records(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse a list of state-level grant records from the USAspending API.

    Args:
        raw_records: List of raw state spending records from API

    Returns:
        List of dicts suitable for database insertion
    """
    parsed = []
    skipped = 0

    for raw in raw_records:
        record = parse_grant_record(raw)
        if record and record.get("state"):
            # Ensure all columns are present for batch insert consistency
            for col in INSERT_COLUMNS:
                record.setdefault(col, None)
            parsed.append(record)
        else:
            skipped += 1

    if skipped:
        logger.warning(f"Skipped {skipped} grant records with missing state")

    logger.info(f"Parsed {len(parsed)} DOT grant records")
    return parsed
