"""
EIA metadata and schema generation utilities.

Handles:
- Table name generation
- CREATE TABLE SQL generation
- Data parsing and transformation
- Date validation
"""
import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def generate_table_name(category: str, subcategory: Optional[str] = None) -> str:
    """
    Generate PostgreSQL table name for EIA data.
    
    Format: eia_{category}_{subcategory} (if subcategory provided)
            eia_{category} (if no subcategory)
    
    Args:
        category: Category name (e.g., "petroleum", "natural_gas", "electricity")
        subcategory: Optional subcategory (e.g., "consumption", "production")
        
    Returns:
        PostgreSQL table name
        
    Examples:
        >>> generate_table_name("petroleum", "consumption")
        'eia_petroleum_consumption'
        >>> generate_table_name("retail_gas_prices")
        'eia_retail_gas_prices'
    """
    # Normalize category name
    category_clean = category.lower().replace("-", "_").replace(" ", "_")
    
    if subcategory:
        subcategory_clean = subcategory.lower().replace("-", "_").replace(" ", "_")
        return f"eia_{category_clean}_{subcategory_clean}"
    else:
        return f"eia_{category_clean}"


def generate_create_table_sql(
    table_name: str,
    category: str
) -> str:
    """
    Generate CREATE TABLE SQL for EIA data.
    
    EIA data typically has these fields:
    - period: date/time period (varies by frequency)
    - value: numeric value
    - units: units of measurement
    - Various facet columns (product, process, area, etc.)
    
    Args:
        table_name: PostgreSQL table name
        category: Data category to determine schema
        
    Returns:
        CREATE TABLE SQL statement
    """
    # Base columns common to all EIA data
    base_columns = """
        id SERIAL PRIMARY KEY,
        period TEXT NOT NULL,
        value NUMERIC,
        units TEXT,
        series_id TEXT,
        product TEXT,
        process TEXT,
        area_code TEXT,
        area_name TEXT,
        state_code TEXT,
        sector TEXT,
        frequency TEXT,
        ingested_at TIMESTAMP DEFAULT NOW()
    """
    
    # Category-specific columns
    category_columns = {
        "petroleum": """
            duoarea TEXT,
            product_name TEXT
        """,
        "natural_gas": """
            duoarea TEXT,
            process_name TEXT
        """,
        "electricity": """
            sectorid TEXT,
            sector_name TEXT,
            stateid TEXT,
            state_name TEXT
        """,
        "retail_gas_prices": """
            grade TEXT,
            formulation TEXT
        """,
        "steo": """
            series_name TEXT,
            series_description TEXT
        """
    }
    
    # Get category-specific columns or empty string
    additional_columns = category_columns.get(category, "")
    
    # Build complete CREATE TABLE statement
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {base_columns}
        {additional_columns if additional_columns else ""}
    );
    
    -- Create index on period for efficient time-based queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_period ON {table_name} (period);
    
    -- Create index on series_id if present
    CREATE INDEX IF NOT EXISTS idx_{table_name}_series_id ON {table_name} (series_id);
    
    -- Create composite index for common queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_composite 
        ON {table_name} (period, area_code, product, sector);
    """
    
    return sql


def parse_eia_response(
    response: Dict[str, Any],
    category: str
) -> List[Dict[str, Any]]:
    """
    Parse EIA API response and extract data records.
    
    Args:
        response: Raw API response from EIA
        category: Data category
        
    Returns:
        List of parsed data records
    """
    # EIA v2 API typically returns data in response.data
    if "response" in response and "data" in response["response"]:
        data_records = response["response"]["data"]
    elif "data" in response:
        data_records = response["data"]
    else:
        logger.warning("No data found in EIA response")
        return []
    
    parsed_records = []
    
    for record in data_records:
        parsed = parse_eia_record(record, category)
        if parsed:
            parsed_records.append(parsed)
    
    logger.info(f"Parsed {len(parsed_records)} records from EIA response")
    return parsed_records


def parse_eia_record(
    record: Dict[str, Any],
    category: str
) -> Optional[Dict[str, Any]]:
    """
    Parse a single EIA data record.
    
    Args:
        record: Raw data record from EIA API
        category: Data category
        
    Returns:
        Parsed record or None if invalid
    """
    try:
        # Extract common fields
        parsed = {
            "period": record.get("period"),
            "value": record.get("value"),
            "units": record.get("units"),
            "series_id": record.get("series-id") or record.get("seriesId"),
        }
        
        # Extract facet fields
        if "product" in record:
            parsed["product"] = record["product"]
        if "product-name" in record:
            parsed["product_name"] = record["product-name"]
        
        if "process" in record:
            parsed["process"] = record["process"]
        if "process-name" in record:
            parsed["process_name"] = record["process-name"]
        
        if "area" in record:
            parsed["area_code"] = record["area"]
        if "area-name" in record:
            parsed["area_name"] = record["area-name"]
        
        if "duoarea" in record:
            parsed["duoarea"] = record["duoarea"]
        
        if "state" in record:
            parsed["state_code"] = record["state"]
        if "stateid" in record:
            parsed["state_code"] = record["stateid"]
        if "state-name" in record:
            parsed["area_name"] = record["state-name"]
        
        if "sector" in record:
            parsed["sector"] = record["sector"]
        if "sectorid" in record:
            parsed["sector"] = record["sectorid"]
        if "sector-name" in record:
            parsed["sector_name"] = record["sector-name"]
        
        if "frequency" in record:
            parsed["frequency"] = record["frequency"]
        
        # Category-specific fields
        if category == "retail_gas_prices":
            if "grade" in record:
                parsed["grade"] = record["grade"]
            if "formulation" in record:
                parsed["formulation"] = record["formulation"]
        
        elif category == "steo":
            if "series-name" in record:
                parsed["series_name"] = record["series-name"]
            if "series-description" in record:
                parsed["series_description"] = record["series-description"]
        
        return parsed
    
    except Exception as e:
        logger.error(f"Failed to parse EIA record: {e}")
        return None


def validate_date_format(date_str: str, frequency: str = "annual") -> bool:
    """
    Validate date format based on frequency.
    
    Args:
        date_str: Date string to validate
        frequency: Data frequency (annual, monthly, weekly, daily)
        
    Returns:
        True if valid, False otherwise
    """
    try:
        if frequency == "annual":
            # Format: YYYY
            return bool(re.match(r'^\d{4}$', date_str))
        elif frequency == "monthly":
            # Format: YYYY-MM
            return bool(re.match(r'^\d{4}-\d{2}$', date_str))
        elif frequency == "weekly":
            # Format: YYYY-MM-DD
            return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', date_str))
        elif frequency == "daily":
            # Format: YYYY-MM-DD
            return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', date_str))
        else:
            return False
    except Exception:
        return False


def get_default_date_range(frequency: str = "annual") -> Tuple[str, str]:
    """
    Get default date range for EIA data.
    
    Args:
        frequency: Data frequency
        
    Returns:
        Tuple of (start_date, end_date)
    """
    end_date = datetime.now()
    
    if frequency == "annual":
        # Last 10 years
        start_date = end_date.replace(year=end_date.year - 10)
        return (str(start_date.year), str(end_date.year))
    elif frequency == "monthly":
        # Last 5 years
        start_date = end_date.replace(year=end_date.year - 5)
        return (
            start_date.strftime("%Y-%m"),
            end_date.strftime("%Y-%m")
        )
    elif frequency == "weekly" or frequency == "daily":
        # Last 2 years
        start_date = end_date - timedelta(days=730)
        return (
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
    else:
        # Default to annual
        start_date = end_date.replace(year=end_date.year - 10)
        return (str(start_date.year), str(end_date.year))


def get_category_display_name(category: str) -> str:
    """
    Get human-readable display name for category.
    
    Args:
        category: Category identifier
        
    Returns:
        Display name
    """
    display_names = {
        "petroleum": "Petroleum Data",
        "petroleum_consumption": "Petroleum Consumption",
        "petroleum_production": "Petroleum Production",
        "natural_gas": "Natural Gas Data",
        "natural_gas_consumption": "Natural Gas Consumption",
        "natural_gas_production": "Natural Gas Production",
        "electricity": "Electricity Data",
        "electricity_generation": "Electricity Generation",
        "electricity_retail_sales": "Electricity Retail Sales",
        "retail_gas_prices": "Retail Gas Prices",
        "steo": "Short-Term Energy Outlook Projections"
    }
    
    return display_names.get(
        category.lower().replace("-", "_"),
        category.replace("_", " ").replace("-", " ").title()
    )


def get_category_description(category: str) -> str:
    """
    Get description for category.
    
    Args:
        category: Category identifier
        
    Returns:
        Description
    """
    descriptions = {
        "petroleum": "Petroleum production, consumption, imports, exports, and stocks data from EIA",
        "petroleum_consumption": "Petroleum consumption data by product and region from EIA",
        "petroleum_production": "Petroleum production data by product and region from EIA",
        "natural_gas": "Natural gas production, consumption, storage, and prices data from EIA",
        "natural_gas_consumption": "Natural gas consumption data by sector and region from EIA",
        "natural_gas_production": "Natural gas production data by region from EIA",
        "electricity": "Electricity generation, sales, revenue, and customer data from EIA",
        "electricity_generation": "Electricity generation data by fuel type and region from EIA",
        "electricity_retail_sales": "Electricity retail sales data by sector and state from EIA",
        "retail_gas_prices": "Retail gasoline prices by grade and region from EIA",
        "steo": "Short-Term Energy Outlook projections for energy supply, demand, and prices from EIA"
    }
    
    return descriptions.get(
        category.lower().replace("-", "_"),
        f"{category.replace('_', ' ').replace('-', ' ').title()} data from EIA"
    )


def build_insert_values(
    parsed_records: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Build parameterized insert values from parsed records.
    
    Args:
        parsed_records: List of parsed data records
        
    Returns:
        List of dicts ready for parameterized insert
    """
    insert_rows = []
    
    for record in parsed_records:
        # Convert None values to NULL-compatible format
        row = {
            "period": record.get("period"),
            "value": record.get("value"),
            "units": record.get("units"),
            "series_id": record.get("series_id"),
            "product": record.get("product"),
            "process": record.get("process"),
            "area_code": record.get("area_code"),
            "area_name": record.get("area_name"),
            "state_code": record.get("state_code"),
            "sector": record.get("sector"),
            "frequency": record.get("frequency"),
            "duoarea": record.get("duoarea"),
            "product_name": record.get("product_name"),
            "process_name": record.get("process_name"),
            "sectorid": record.get("sector"),  # Map to sector column
            "sector_name": record.get("sector_name"),
            "stateid": record.get("state_code"),  # Map to state_code column
            "state_name": record.get("area_name"),  # Map to area_name column
            "grade": record.get("grade"),
            "formulation": record.get("formulation"),
            "series_name": record.get("series_name"),
            "series_description": record.get("series_description"),
        }
        
        insert_rows.append(row)
    
    return insert_rows

