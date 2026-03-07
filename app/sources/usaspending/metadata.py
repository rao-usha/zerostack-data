"""
USAspending.gov metadata utilities.

Handles:
- Table name generation
- CREATE TABLE SQL generation
- Data parsing and transformation
- Date validation and defaults
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Table name
TABLE_NAME = "usaspending_awards"

# Dataset identifier
DATASET_ID = "usaspending_awards"

# Display name
DISPLAY_NAME = "USAspending Federal Awards"

# Description
DESCRIPTION = (
    "Federal contract and grant awards from USAspending.gov, "
    "searchable by NAICS code, location, and time period."
)


def generate_table_name() -> str:
    """
    Generate table name for USAspending data.

    Returns:
        Table name: usaspending_awards
    """
    return TABLE_NAME


def generate_create_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL for USAspending awards.

    Args:
        table_name: Name of the table to create

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        award_id TEXT PRIMARY KEY,
        recipient_name TEXT,
        recipient_uei TEXT,
        award_amount NUMERIC,
        total_obligation NUMERIC,
        naics_code TEXT,
        naics_description TEXT,
        awarding_agency TEXT,
        place_of_performance_city TEXT,
        place_of_performance_state TEXT,
        place_of_performance_zip TEXT,
        period_of_performance_start DATE,
        period_of_performance_end DATE,
        award_type TEXT,
        description TEXT,
        ingested_at TIMESTAMP DEFAULT NOW()
    );

    -- Create index on NAICS code for sector queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_naics
        ON {table_name} (naics_code);

    -- Create index on state for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state
        ON {table_name} (place_of_performance_state);

    -- Create index on award amount for value-based queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_amount
        ON {table_name} (award_amount DESC);

    -- Create index on recipient for company lookups
    CREATE INDEX IF NOT EXISTS idx_{table_name}_recipient
        ON {table_name} (recipient_name);

    -- Create index on recipient UEI
    CREATE INDEX IF NOT EXISTS idx_{table_name}_uei
        ON {table_name} (recipient_uei);

    -- Create index on performance dates
    CREATE INDEX IF NOT EXISTS idx_{table_name}_perf_start
        ON {table_name} (period_of_performance_start);

    -- Add comment documenting the table
    COMMENT ON TABLE {table_name} IS 'USAspending.gov - Federal contract and grant award data';
    """


def get_insert_sql(table_name: str) -> str:
    """
    Get INSERT SQL statement with ON CONFLICT upsert.

    Args:
        table_name: Target table name

    Returns:
        INSERT SQL with ON CONFLICT handling
    """
    return f"""
        INSERT INTO {table_name} (
            award_id, recipient_name, recipient_uei,
            award_amount, total_obligation,
            naics_code, naics_description, awarding_agency,
            place_of_performance_city, place_of_performance_state,
            place_of_performance_zip,
            period_of_performance_start, period_of_performance_end,
            award_type, description
        ) VALUES (
            :award_id, :recipient_name, :recipient_uei,
            :award_amount, :total_obligation,
            :naics_code, :naics_description, :awarding_agency,
            :place_of_performance_city, :place_of_performance_state,
            :place_of_performance_zip,
            :period_of_performance_start, :period_of_performance_end,
            :award_type, :description
        )
        ON CONFLICT (award_id)
        DO UPDATE SET
            recipient_name = EXCLUDED.recipient_name,
            recipient_uei = EXCLUDED.recipient_uei,
            award_amount = EXCLUDED.award_amount,
            total_obligation = EXCLUDED.total_obligation,
            naics_code = EXCLUDED.naics_code,
            naics_description = EXCLUDED.naics_description,
            awarding_agency = EXCLUDED.awarding_agency,
            place_of_performance_city = EXCLUDED.place_of_performance_city,
            place_of_performance_state = EXCLUDED.place_of_performance_state,
            place_of_performance_zip = EXCLUDED.place_of_performance_zip,
            period_of_performance_start = EXCLUDED.period_of_performance_start,
            period_of_performance_end = EXCLUDED.period_of_performance_end,
            award_type = EXCLUDED.award_type,
            description = EXCLUDED.description,
            ingested_at = NOW()
    """


# Column names for batch operations
COLUMNS = [
    "award_id",
    "recipient_name",
    "recipient_uei",
    "award_amount",
    "total_obligation",
    "naics_code",
    "naics_description",
    "awarding_agency",
    "place_of_performance_city",
    "place_of_performance_state",
    "place_of_performance_zip",
    "period_of_performance_start",
    "period_of_performance_end",
    "award_type",
    "description",
]


def parse_awards_response(api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse USAspending API response into database rows.

    USAspending search response format:
    {
        "results": [...],
        "page_metadata": {
            "page": 1,
            "hasNext": true,
            "total": 1234,
            ...
        }
    }

    Args:
        api_response: Raw API response dict

    Returns:
        List of dictionaries suitable for database insertion
    """
    results = api_response.get("results", [])

    if not results:
        logger.warning("No results in USAspending API response")
        return []

    parsed_rows = []
    for record in results:
        try:
            parsed = _parse_award_record(record)
            if parsed and parsed.get("award_id"):
                parsed_rows.append(parsed)
        except Exception as e:
            logger.warning(f"Failed to parse USAspending record: {e}")
            continue

    return parsed_rows


def _parse_award_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a single award record from the API response.

    The USAspending API returns field names with spaces and mixed case.
    This function normalizes them to snake_case column names.

    Args:
        record: Single result dict from USAspending API

    Returns:
        Normalized dict for database insertion, or None if invalid
    """
    award_id = record.get("Award ID")
    if not award_id:
        return None

    return {
        "award_id": str(award_id).strip(),
        "recipient_name": _clean_text(record.get("Recipient Name")),
        "recipient_uei": _clean_text(record.get("Recipient UEI")),
        "award_amount": _parse_numeric(record.get("Award Amount")),
        "total_obligation": _parse_numeric(record.get("Total Obligation")),
        "naics_code": _clean_text(record.get("NAICS Code")),
        "naics_description": _clean_text(record.get("NAICS Description")),
        "awarding_agency": _clean_text(record.get("Awarding Agency")),
        "place_of_performance_city": _clean_text(
            record.get("Place of Performance City Code")
        ),
        "place_of_performance_state": _clean_text(
            record.get("Place of Performance State Code")
        ),
        "place_of_performance_zip": _clean_text(
            record.get("Place of Performance Zip5")
        ),
        "period_of_performance_start": _parse_date(
            record.get("Period of Performance Start Date")
        ),
        "period_of_performance_end": _parse_date(
            record.get("Period of Performance Current End Date")
        ),
        "award_type": _clean_text(record.get("Award Type")),
        "description": _clean_text(record.get("Description")),
    }


def _parse_numeric(value: Any) -> Optional[float]:
    """Parse numeric value, handling nulls and empty strings."""
    if value is None or value == "" or value == "null":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_date(value: Any) -> Optional[str]:
    """
    Parse date value from USAspending API.

    USAspending returns dates as YYYY-MM-DD strings.

    Args:
        value: Date string or None

    Returns:
        Date string in YYYY-MM-DD format, or None
    """
    if value is None or value == "" or value == "null":
        return None
    try:
        # Validate it parses as a date
        datetime.strptime(str(value)[:10], "%Y-%m-%d")
        return str(value)[:10]
    except (ValueError, TypeError):
        return None


def _clean_text(value: Any) -> Optional[str]:
    """Clean text value, handling nulls and empty strings."""
    if value is None or value == "" or value == "null":
        return None
    return str(value).strip()


def get_default_date_range() -> tuple:
    """
    Get default date range for USAspending data ingestion.

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format

    Default: Last 3 years (federal contracts can span long periods)
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 3)

    return (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))


def validate_date_format(date_str: str) -> bool:
    """
    Validate date string is in YYYY-MM-DD format.

    Args:
        date_str: Date string to validate

    Returns:
        True if valid, False otherwise
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_response_pagination(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract pagination info from USAspending API response.

    Args:
        api_response: Raw API response

    Returns:
        Dict with page, has_next, total keys
    """
    page_meta = api_response.get("page_metadata", {})

    return {
        "page": page_meta.get("page", 1),
        "has_next": page_meta.get("hasNext", False),
        "total": page_meta.get("total", 0),
    }
