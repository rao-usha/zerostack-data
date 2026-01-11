"""
FRED metadata utilities.

Handles:
- Series ID definitions and categories
- Table name generation
- CREATE TABLE SQL generation
- Data parsing and transformation
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_table_name(category: str) -> str:
    """
    Generate table name for FRED dataset.
    
    Convention: fred_{category}
    
    Args:
        category: Category name (e.g., "interest_rates", "monetary_aggregates")
        
    Returns:
        Table name (e.g., "fred_interest_rates")
    """
    # Sanitize category name
    sanitized = category.lower().replace("-", "_").replace(" ", "_")
    return f"fred_{sanitized}"


def generate_create_table_sql(table_name: str, series_ids: List[str]) -> str:
    """
    Generate CREATE TABLE SQL for FRED data.
    
    Table schema:
    - series_id TEXT: FRED series ID
    - date DATE: Observation date
    - value NUMERIC: Observation value
    - realtime_start DATE: Real-time period start (when this data was available)
    - realtime_end DATE: Real-time period end
    - ingested_at TIMESTAMP: When the data was ingested
    
    Primary key: (series_id, date)
    
    Args:
        table_name: Name of the table to create
        series_ids: List of series IDs (for documentation)
        
    Returns:
        CREATE TABLE SQL statement
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        series_id TEXT NOT NULL,
        date DATE NOT NULL,
        value NUMERIC,
        realtime_start DATE,
        realtime_end DATE,
        ingested_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (series_id, date)
    );
    
    -- Create index on date for time-series queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (date);
    
    -- Create index on series_id for filtering
    CREATE INDEX IF NOT EXISTS idx_{table_name}_series_id ON {table_name} (series_id);
    
    -- Add comment documenting the table
    COMMENT ON TABLE {table_name} IS 'FRED (Federal Reserve Economic Data) - Contains {len(series_ids)} series';
    """
    
    return sql


def parse_observations(
    api_response: Dict[str, Any],
    series_id: str
) -> List[Dict[str, Any]]:
    """
    Parse FRED API observations response into database rows.
    
    FRED API response format:
    {
        "realtime_start": "2023-01-01",
        "realtime_end": "2023-12-31",
        "observation_start": "1954-07-01",
        "observation_end": "9999-12-31",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 123,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {
                "realtime_start": "2023-01-01",
                "realtime_end": "2023-12-31",
                "date": "1954-07-01",
                "value": "0.80"
            },
            ...
        ]
    }
    
    Args:
        api_response: Raw API response dict
        series_id: FRED series ID
        
    Returns:
        List of dictionaries suitable for database insertion
    """
    observations = api_response.get("observations", [])
    
    parsed_rows = []
    for obs in observations:
        date_str = obs.get("date")
        value_str = obs.get("value")
        
        # Skip if missing required fields
        if not date_str:
            logger.warning(f"Skipping observation with missing date: {obs}")
            continue
        
        # Parse value (may be "." for missing data)
        # Skip rows with missing data
        if not value_str or value_str == ".":
            continue

        try:
            value = float(value_str)
        except ValueError:
            logger.warning(f"Invalid value '{value_str}' for {series_id} on {date_str}")
            continue

        row = {
            "series_id": series_id,
            "date": date_str,
            "value": value,
            "realtime_start": obs.get("realtime_start"),
            "realtime_end": obs.get("realtime_end")
        }
        
        parsed_rows.append(row)
    
    return parsed_rows


def get_series_for_category(category: str) -> List[str]:
    """
    Get list of FRED series IDs for a given category.
    
    Args:
        category: Category name (e.g., "interest_rates", "monetary_aggregates")
        
    Returns:
        List of series IDs
        
    Raises:
        ValueError: If category is not recognized
    """
    from app.sources.fred.client import COMMON_SERIES
    
    category_lower = category.lower()
    
    if category_lower not in COMMON_SERIES:
        available = ", ".join(COMMON_SERIES.keys())
        raise ValueError(
            f"Unknown FRED category: {category}. "
            f"Available categories: {available}"
        )
    
    series_dict = COMMON_SERIES[category_lower]
    return list(series_dict.values())


def get_category_display_name(category: str) -> str:
    """
    Get display name for a FRED category.
    
    Args:
        category: Category name
        
    Returns:
        Human-readable display name
    """
    display_names = {
        "interest_rates": "Interest Rates (H.15)",
        "monetary_aggregates": "Monetary Aggregates (M1, M2)",
        "industrial_production": "Industrial Production Indices",
        "economic_indicators": "Core Economic Indicators"
    }
    
    return display_names.get(category.lower(), category.replace("_", " ").title())


def get_category_description(category: str) -> str:
    """
    Get description for a FRED category.
    
    Args:
        category: Category name
        
    Returns:
        Description text
    """
    descriptions = {
        "interest_rates": (
            "Federal Reserve interest rates including Federal Funds Rate, "
            "Treasury rates, and Prime Rate from the H.15 statistical release"
        ),
        "monetary_aggregates": (
            "Money supply measures including M1, M2, monetary base, "
            "and currency in circulation"
        ),
        "industrial_production": (
            "Industrial production indices covering total production, manufacturing, "
            "mining, utilities, and capacity utilization"
        ),
        "economic_indicators": (
            "Core economic indicators including GDP, unemployment rate, CPI, "
            "personal consumption expenditures, and retail sales"
        )
    }
    
    return descriptions.get(
        category.lower(),
        f"FRED data series for {category.replace('_', ' ')}"
    )


def build_insert_values(
    parsed_data: Dict[str, List[Dict[str, Any]]]
) -> List[Dict[str, Any]]:
    """
    Build list of dictionaries for parameterized INSERT.
    
    Args:
        parsed_data: Dict mapping series_id to list of parsed observations
        
    Returns:
        List of dictionaries for parameterized INSERT
    """
    all_rows = []
    
    for series_id, observations in parsed_data.items():
        all_rows.extend(observations)
    
    logger.info(f"Built {len(all_rows)} rows for insertion")
    return all_rows


def get_default_date_range() -> tuple[str, str]:
    """
    Get default date range for FRED data ingestion.
    
    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
        
    Default: Last 10 years
    """
    from datetime import datetime, timedelta
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 10)  # 10 years
    
    return (
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )


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

