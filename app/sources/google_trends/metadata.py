"""
Google Trends metadata utilities.

Handles:
- Table schema definition
- CREATE TABLE SQL generation
- Data parsing and transformation
- Field mapping from Google Trends API to database columns
"""

import json
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Table name constant
TABLE_NAME = "google_trends"

# Dataset identifier
DATASET_ID = "google_trends"

# Display name
DISPLAY_NAME = "Google Trends"

# Description
DESCRIPTION = (
    "Google Trends search interest data. "
    "Includes trending searches, interest by region, and related queries "
    "for specified keywords and geographic areas."
)

# Database columns for INSERT operations
INSERT_COLUMNS = [
    "keyword",
    "geo",
    "state",
    "date",
    "interest_score",
    "related_queries",
]

# Columns to update on conflict (upsert)
UPDATE_COLUMNS = [
    "state",
    "interest_score",
    "related_queries",
]

# Conflict columns for upsert
CONFLICT_COLUMNS = ["keyword", "geo", "date"]


def generate_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for Google Trends data.

    Returns:
        CREATE TABLE SQL statement
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        keyword TEXT NOT NULL,
        geo TEXT NOT NULL,
        state TEXT,
        date TEXT,
        interest_score INTEGER,
        related_queries JSONB,
        ingested_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(keyword, geo, date)
    );

    -- Index on keyword for search queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_keyword
        ON {TABLE_NAME} (keyword);

    -- Index on geo for geographic queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geo
        ON {TABLE_NAME} (geo);

    -- Index on date for temporal queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_date
        ON {TABLE_NAME} (date);

    -- Composite index for keyword + geo lookups
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_keyword_geo
        ON {TABLE_NAME} (keyword, geo);

    -- GIN index on related_queries for JSONB containment queries
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_related_queries
        ON {TABLE_NAME} USING GIN (related_queries);

    COMMENT ON TABLE {TABLE_NAME} IS 'Google Trends search interest and trending data';
    """


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert a value to integer."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_daily_trends(
    raw_response: Dict[str, Any], geo: str = "US"
) -> List[Dict[str, Any]]:
    """
    Parse daily trending searches from Google Trends API response.

    The daily trends response contains trending search topics with
    traffic volume and related queries.

    Args:
        raw_response: Raw response from dailytrends endpoint
        geo: Geographic region code used for the request

    Returns:
        List of dicts suitable for database insertion
    """
    parsed = []

    # Navigate the nested response structure
    default = raw_response.get("default", {})
    trending_days = default.get("trendingSearchesDays", [])

    for day in trending_days:
        date = day.get("date", "")
        # Convert YYYYMMDD to YYYY-MM-DD
        if len(date) == 8:
            date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"

        searches = day.get("trendingSearches", [])
        for search in searches:
            title = search.get("title", {}).get("query", "")
            if not title:
                continue

            # Extract traffic volume
            traffic = search.get("formattedTraffic", "0")
            traffic_str = str(traffic).replace("+", "").replace("K", "000").replace("M", "000000")
            interest = _safe_int(traffic_str, 0)

            # Extract related queries
            related = []
            for related_item in search.get("relatedQueries", []):
                query = related_item.get("query", "")
                if query:
                    related.append(query)

            record = {
                "keyword": title.strip(),
                "geo": geo,
                "state": None,
                "date": date,
                "interest_score": interest,
                "related_queries": related if related else None,
            }

            # Ensure all columns are present
            for col in INSERT_COLUMNS:
                record.setdefault(col, None)

            parsed.append(record)

    logger.info(f"Parsed {len(parsed)} daily trend records for geo={geo}")
    return parsed


def parse_interest_by_region(
    raw_response: Dict[str, Any],
    keyword: str,
    geo: str = "US",
    date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Parse interest by region data from Google Trends API response.

    Args:
        raw_response: Raw response from interest by region endpoint
        keyword: The keyword that was searched
        geo: Geographic region code
        date: Date string for the record

    Returns:
        List of dicts suitable for database insertion
    """
    parsed = []

    # The response structure varies; handle common formats
    geo_data = raw_response.get("default", {}).get("geoMapData", [])

    for region in geo_data:
        region_name = region.get("geoName", "")
        value = region.get("value", [0])
        score = value[0] if isinstance(value, list) and value else _safe_int(value, 0)

        record = {
            "keyword": keyword.strip(),
            "geo": geo,
            "state": region_name.strip() if region_name else None,
            "date": date,
            "interest_score": _safe_int(score, 0),
            "related_queries": None,
        }

        # Ensure all columns are present
        for col in INSERT_COLUMNS:
            record.setdefault(col, None)

        parsed.append(record)

    logger.info(
        f"Parsed {len(parsed)} regional interest records "
        f"for keyword={keyword}, geo={geo}"
    )
    return parsed
