"""
USDA NASS metadata and schema definitions.

Defines table schemas and parsing functions for agricultural data.
"""

from typing import Dict, Any, Optional


# Database schema for USDA crop production
CROP_PRODUCTION_COLUMNS = {
    "id": "SERIAL PRIMARY KEY",
    "source_desc": "VARCHAR(50)",  # SURVEY, CENSUS
    "sector_desc": "VARCHAR(100)",  # CROPS, ANIMALS & PRODUCTS
    "group_desc": "VARCHAR(100)",  # FIELD CROPS, FRUIT & TREE NUTS
    "commodity_desc": "VARCHAR(100) NOT NULL",  # CORN, SOYBEANS
    "class_desc": "VARCHAR(100)",  # ALL CLASSES, GRAIN
    "prodn_practice_desc": "VARCHAR(100)",  # ALL PRODUCTION PRACTICES
    "util_practice_desc": "VARCHAR(100)",  # ALL UTILIZATION PRACTICES
    "statisticcat_desc": "VARCHAR(100)",  # PRODUCTION, YIELD
    "unit_desc": "VARCHAR(50)",  # BU, ACRES, $
    # Location
    "agg_level_desc": "VARCHAR(50)",  # NATIONAL, STATE, COUNTY
    "state_name": "VARCHAR(50)",
    "state_fips_code": "VARCHAR(5)",
    "county_name": "VARCHAR(100)",
    "county_code": "VARCHAR(10)",
    # Time
    "year": "INTEGER NOT NULL",
    "freq_desc": "VARCHAR(50)",  # ANNUAL, MONTHLY, WEEKLY
    "begin_code": "VARCHAR(10)",  # Week/month number
    "end_code": "VARCHAR(10)",
    "reference_period_desc": "VARCHAR(100)",  # YEAR, MAR, WEEK #15
    # Values
    "value": "NUMERIC",  # Main data value
    "value_text": "VARCHAR(50)",  # Original text (may include commas, (D))
    "cv_pct": "NUMERIC(5,2)",  # Coefficient of variation
    # Metadata
    "short_desc": "VARCHAR(500)",  # Full description
    "domain_desc": "VARCHAR(100)",
    "domaincat_desc": "VARCHAR(100)",
    "load_time": "VARCHAR(50)",  # When USDA loaded the data
    "ingested_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}

# Schema for livestock data
LIVESTOCK_COLUMNS = {
    **CROP_PRODUCTION_COLUMNS,
    "inventory_type": "VARCHAR(100)",  # BEGINNING OF YEAR, END OF YEAR
}

# Schema for prices received
PRICES_COLUMNS = {
    **CROP_PRODUCTION_COLUMNS,
    "marketing_year": "VARCHAR(20)",
}


def generate_table_name(data_type: str, commodity: Optional[str] = None) -> str:
    """
    Generate table name for USDA data.

    Args:
        data_type: Type of data (production, prices, livestock)
        commodity: Optional commodity name

    Returns:
        Table name string
    """
    if commodity:
        return f"usda_{data_type}_{commodity.lower().replace(' ', '_')}"
    return f"usda_{data_type}"


def generate_create_table_sql(data_type: str, commodity: Optional[str] = None) -> str:
    """
    Generate CREATE TABLE SQL for USDA data.

    Args:
        data_type: Type of data
        commodity: Optional commodity name

    Returns:
        SQL CREATE TABLE statement
    """
    table_name = generate_table_name(data_type, commodity)

    # Select appropriate columns
    if data_type == "livestock":
        columns = LIVESTOCK_COLUMNS
    elif data_type == "prices":
        columns = PRICES_COLUMNS
    else:
        columns = CROP_PRODUCTION_COLUMNS

    column_defs = ",\n    ".join([f"{col} {dtype}" for col, dtype in columns.items()])

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {column_defs}
    );
    
    -- Create indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_commodity ON {table_name}(commodity_desc);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name}(year);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {table_name}(state_name);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_stat ON {table_name}(statisticcat_desc);
    
    -- Unique constraint to prevent duplicates
    CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_unique 
    ON {table_name}(commodity_desc, year, state_name, statisticcat_desc, reference_period_desc, 
                   COALESCE(class_desc, ''), COALESCE(domain_desc, ''));
    """

    return sql


def parse_usda_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse USDA API record to database format.

    Args:
        record: Raw record from USDA API

    Returns:
        Parsed record ready for database
    """
    # Parse numeric value (USDA returns strings with commas, or "(D)" for withheld)
    value_text = record.get("Value", "")
    value = _parse_usda_value(value_text)

    parsed = {
        "source_desc": record.get("source_desc"),
        "sector_desc": record.get("sector_desc"),
        "group_desc": record.get("group_desc"),
        "commodity_desc": record.get("commodity_desc"),
        "class_desc": record.get("class_desc"),
        "prodn_practice_desc": record.get("prodn_practice_desc"),
        "util_practice_desc": record.get("util_practice_desc"),
        "statisticcat_desc": record.get("statisticcat_desc"),
        "unit_desc": record.get("unit_desc"),
        # Location
        "agg_level_desc": record.get("agg_level_desc"),
        "state_name": record.get("state_name"),
        "state_fips_code": record.get("state_fips_code"),
        "county_name": record.get("county_name"),
        "county_code": record.get("county_code"),
        # Time
        "year": _safe_int(record.get("year")),
        "freq_desc": record.get("freq_desc"),
        "begin_code": record.get("begin_code"),
        "end_code": record.get("end_code"),
        "reference_period_desc": record.get("reference_period_desc"),
        # Values
        "value": value,
        "value_text": value_text,
        "cv_pct": _safe_float(record.get("CV (%)")),
        # Metadata
        "short_desc": record.get("short_desc"),
        "domain_desc": record.get("domain_desc"),
        "domaincat_desc": record.get("domaincat_desc"),
        "load_time": record.get("load_time"),
    }

    return parsed


def _parse_usda_value(value_str: str) -> Optional[float]:
    """
    Parse USDA value string to numeric.

    USDA returns:
    - Numbers with commas: "14,385,000"
    - Withheld data: "(D)"
    - Not available: "(NA)" or "(X)"
    - Zeros: "(Z)" for less than half unit

    Args:
        value_str: Value string from USDA

    Returns:
        Numeric value or None
    """
    if not value_str:
        return None

    value_str = str(value_str).strip()

    # Check for special codes
    if value_str in ("(D)", "(NA)", "(X)", "(Z)", "", "NA"):
        return None

    # Remove commas and convert
    try:
        return float(value_str.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Safely convert to int."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert to float."""
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None


# Major commodities and their categories
COMMODITY_CATEGORIES = {
    "grains": {
        "commodities": [
            "CORN",
            "SOYBEANS",
            "WHEAT",
            "OATS",
            "BARLEY",
            "SORGHUM",
            "RICE",
        ],
        "key_stats": [
            "PRODUCTION",
            "YIELD",
            "AREA PLANTED",
            "AREA HARVESTED",
            "PRICE RECEIVED",
        ],
    },
    "cotton": {
        "commodities": ["COTTON"],
        "key_stats": ["PRODUCTION", "YIELD", "AREA PLANTED", "PRICE RECEIVED"],
    },
    "livestock": {
        "commodities": ["CATTLE", "HOGS", "SHEEP", "CHICKENS"],
        "key_stats": ["INVENTORY", "SLAUGHTER", "PRICE RECEIVED"],
    },
    "dairy": {
        "commodities": ["MILK"],
        "key_stats": ["PRODUCTION", "PRICE RECEIVED"],
    },
}

# State FIPS codes for reference
STATE_FIPS = {
    "ALABAMA": "01",
    "ALASKA": "02",
    "ARIZONA": "04",
    "ARKANSAS": "05",
    "CALIFORNIA": "06",
    "COLORADO": "08",
    "CONNECTICUT": "09",
    "DELAWARE": "10",
    "FLORIDA": "12",
    "GEORGIA": "13",
    "HAWAII": "15",
    "IDAHO": "16",
    "ILLINOIS": "17",
    "INDIANA": "18",
    "IOWA": "19",
    "KANSAS": "20",
    "KENTUCKY": "21",
    "LOUISIANA": "22",
    "MAINE": "23",
    "MARYLAND": "24",
    "MASSACHUSETTS": "25",
    "MICHIGAN": "26",
    "MINNESOTA": "27",
    "MISSISSIPPI": "28",
    "MISSOURI": "29",
    "MONTANA": "30",
    "NEBRASKA": "31",
    "NEVADA": "32",
    "NEW HAMPSHIRE": "33",
    "NEW JERSEY": "34",
    "NEW MEXICO": "35",
    "NEW YORK": "36",
    "NORTH CAROLINA": "37",
    "NORTH DAKOTA": "38",
    "OHIO": "39",
    "OKLAHOMA": "40",
    "OREGON": "41",
    "PENNSYLVANIA": "42",
    "RHODE ISLAND": "44",
    "SOUTH CAROLINA": "45",
    "SOUTH DAKOTA": "46",
    "TENNESSEE": "47",
    "TEXAS": "48",
    "UTAH": "49",
    "VERMONT": "50",
    "VIRGINIA": "51",
    "WASHINGTON": "53",
    "WEST VIRGINIA": "54",
    "WISCONSIN": "55",
    "WYOMING": "56",
}
