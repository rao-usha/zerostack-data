"""
BEA metadata and schema generation utilities.

Handles:
- Table name generation for BEA datasets
- CREATE TABLE SQL generation
- Data parsing and transformation
- Schema definitions for NIPA, Regional, GDP by Industry data
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# ========== Schema Definitions ==========

# NIPA (National Income and Product Accounts) schema
NIPA_COLUMNS = {
    "table_name": ("TEXT", "BEA table identifier"),
    "series_code": ("TEXT", "Time series code"),
    "line_number": ("INTEGER", "Line number in table"),
    "line_description": ("TEXT", "Description of line item"),
    "time_period": ("TEXT", "Time period (YYYY or YYYYQN)"),
    "cl_unit": ("TEXT", "Unit of measure"),
    "unit_mult": ("INTEGER", "Unit multiplier"),
    "data_value": ("NUMERIC(20,4)", "Data value"),
    "notes": ("TEXT", "Additional notes"),
}

# Regional Economic Accounts schema
REGIONAL_COLUMNS = {
    "table_name": ("TEXT", "BEA table identifier"),
    "geo_fips": ("TEXT", "Geographic FIPS code"),
    "geo_name": ("TEXT", "Geographic area name"),
    "line_code": ("TEXT", "Line code"),
    "line_description": ("TEXT", "Description of line item"),
    "time_period": ("TEXT", "Time period (year)"),
    "cl_unit": ("TEXT", "Unit of measure"),
    "unit_mult": ("INTEGER", "Unit multiplier"),
    "data_value": ("NUMERIC(20,4)", "Data value"),
}

# GDP by Industry schema
GDP_INDUSTRY_COLUMNS = {
    "table_id": ("TEXT", "Table identifier"),
    "industry_id": ("TEXT", "Industry code"),
    "industry_description": ("TEXT", "Industry name"),
    "frequency": ("TEXT", "Data frequency (A/Q)"),
    "time_period": ("TEXT", "Time period"),
    "data_value": ("NUMERIC(20,4)", "Data value"),
    "notes": ("TEXT", "Additional notes"),
}

# International Transactions schema
INTERNATIONAL_COLUMNS = {
    "indicator": ("TEXT", "Transaction indicator code"),
    "indicator_description": ("TEXT", "Indicator description"),
    "area_or_country": ("TEXT", "Geographic area or country"),
    "frequency": ("TEXT", "Data frequency"),
    "time_period": ("TEXT", "Time period"),
    "cl_unit": ("TEXT", "Unit of measure"),
    "unit_mult": ("INTEGER", "Unit multiplier"),
    "data_value": ("NUMERIC(20,4)", "Data value"),
}


def generate_table_name(dataset: str, table_id: Optional[str] = None) -> str:
    """
    Generate PostgreSQL table name for BEA data.

    Args:
        dataset: Dataset identifier (nipa, regional, gdp_industry, international)
        table_id: Optional specific table ID

    Returns:
        PostgreSQL table name

    Examples:
        >>> generate_table_name("nipa")
        'bea_nipa'
        >>> generate_table_name("regional", "SAGDP2N")
        'bea_regional_sagdp2n'
    """
    dataset_clean = dataset.lower().replace("-", "_").replace(" ", "_")

    if table_id:
        table_id_clean = table_id.lower().replace("-", "_").replace(" ", "_")
        return f"bea_{dataset_clean}_{table_id_clean}"
    else:
        return f"bea_{dataset_clean}"


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for BEA data.

    Args:
        table_name: PostgreSQL table name
        dataset: Dataset type to determine schema

    Returns:
        CREATE TABLE SQL statement
    """
    # Select appropriate schema
    if dataset == "nipa":
        columns = NIPA_COLUMNS
        unique_constraint = "table_name, series_code, time_period"
        indexes = [
            ("table_name", "table_name"),
            ("time_period", "time_period"),
            ("series_code", "series_code"),
        ]
    elif dataset == "regional":
        columns = REGIONAL_COLUMNS
        unique_constraint = "table_name, geo_fips, line_code, time_period"
        indexes = [
            ("table_name", "table_name"),
            ("geo_fips", "geo_fips"),
            ("time_period", "time_period"),
            ("geo_name", "geo_name"),
        ]
    elif dataset == "gdp_industry":
        columns = GDP_INDUSTRY_COLUMNS
        unique_constraint = "table_id, industry_id, time_period"
        indexes = [
            ("table_id", "table_id"),
            ("industry_id", "industry_id"),
            ("time_period", "time_period"),
        ]
    elif dataset == "international":
        columns = INTERNATIONAL_COLUMNS
        unique_constraint = "indicator, area_or_country, time_period"
        indexes = [
            ("indicator", "indicator"),
            ("area_or_country", "area_or_country"),
            ("time_period", "time_period"),
        ]
    else:
        raise ValueError(f"Unknown BEA dataset: {dataset}")

    # Build column definitions (no inline comments - they break SQL parsing)
    column_defs = []
    column_defs.append("id SERIAL PRIMARY KEY")

    for col_name, (col_type, col_desc) in columns.items():
        column_defs.append(f"{col_name} {col_type}")

    column_defs.append("ingested_at TIMESTAMP DEFAULT NOW()")

    columns_sql = ",\n        ".join(column_defs)

    # Build indexes
    index_sql_parts = []
    for idx_name, idx_cols in indexes:
        index_sql_parts.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{idx_name} "
            f"ON {table_name} ({idx_cols});"
        )

    indexes_sql = "\n    ".join(index_sql_parts)

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql},
        CONSTRAINT {table_name}_unique UNIQUE ({unique_constraint})
    );
    
    {indexes_sql}
    """

    return sql


def parse_nipa_response(
    response: Dict[str, Any], table_name_param: str
) -> List[Dict[str, Any]]:
    """
    Parse NIPA data from BEA API response.

    Args:
        response: Raw API response
        table_name_param: Table name parameter used in request

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    try:
        # Navigate BEA response structure
        results = response.get("BEAAPI", {}).get("Results", {})

        if not results:
            logger.warning("No results in BEA NIPA response")
            return []

        # Get data array
        data = results.get("Data", [])

        if not data:
            logger.warning("No data in BEA NIPA response")
            return []

        for record in data:
            try:
                parsed = {
                    "table_name": record.get("TableName", table_name_param),
                    "series_code": record.get("SeriesCode"),
                    "line_number": _safe_int(record.get("LineNumber")),
                    "line_description": record.get("LineDescription"),
                    "time_period": record.get("TimePeriod"),
                    "cl_unit": record.get("CL_UNIT") or record.get("UNIT_MULT_DESC"),
                    "unit_mult": _safe_int(record.get("UNIT_MULT")),
                    "data_value": _safe_float(record.get("DataValue")),
                    "notes": record.get("NoteRef"),
                }

                # Skip records without required fields
                if parsed["time_period"] and parsed["series_code"]:
                    parsed_records.append(parsed)

            except Exception as e:
                logger.warning(f"Failed to parse NIPA record: {e}")

        logger.info(f"Parsed {len(parsed_records)} NIPA records")
        return parsed_records

    except Exception as e:
        logger.error(f"Failed to parse NIPA response: {e}")
        return []


def parse_regional_response(
    response: Dict[str, Any], table_name_param: str
) -> List[Dict[str, Any]]:
    """
    Parse Regional data from BEA API response.

    Args:
        response: Raw API response
        table_name_param: Table name parameter used in request

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    try:
        results = response.get("BEAAPI", {}).get("Results", {})

        if not results:
            logger.warning("No results in BEA Regional response")
            return []

        # Get metadata
        statistic = results.get("Statistic", "")
        public_table = results.get("PublicTable", "")

        data = results.get("Data", [])

        if not data:
            logger.warning("No data in BEA Regional response")
            return []

        for record in data:
            try:
                # Regional uses 'Code' field for line code (e.g., "SAINC1-1")
                code = record.get("Code", "")

                parsed = {
                    "table_name": table_name_param,
                    "geo_fips": record.get("GeoFips"),
                    "geo_name": record.get("GeoName"),
                    "line_code": code,
                    "line_description": statistic or public_table,
                    "time_period": record.get("TimePeriod"),
                    "cl_unit": record.get("CL_UNIT"),
                    "unit_mult": _safe_int(record.get("UNIT_MULT")),
                    "data_value": _safe_float(record.get("DataValue")),
                }

                if parsed["time_period"] and parsed["geo_fips"]:
                    parsed_records.append(parsed)

            except Exception as e:
                logger.warning(f"Failed to parse Regional record: {e}")

        logger.info(f"Parsed {len(parsed_records)} Regional records")
        return parsed_records

    except Exception as e:
        logger.error(f"Failed to parse Regional response: {e}")
        return []


def parse_gdp_industry_response(
    response: Dict[str, Any], table_id_param: str
) -> List[Dict[str, Any]]:
    """
    Parse GDP by Industry data from BEA API response.

    Args:
        response: Raw API response
        table_id_param: Table ID parameter used in request

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    try:
        beaapi = response.get("BEAAPI", {})
        results = beaapi.get("Results", {})

        if not results:
            logger.warning("No results in BEA GDP by Industry response")
            return []

        # Results can be a list or dict depending on the response
        if isinstance(results, list):
            # If results is a list, it might be the data directly
            data = results
        else:
            data = results.get("Data", [])

        if not data:
            logger.warning("No data in BEA GDP by Industry response")
            return []

        for record in data:
            try:
                # Handle different field names in GDP by Industry API
                parsed = {
                    "table_id": record.get("TableID") or table_id_param,
                    "industry_id": record.get("Industry") or record.get("IndustrYId"),
                    "industry_description": (
                        record.get("IndustrYDescription")
                        or record.get("IndustryDescription")
                        or record.get("Description")
                        or ""
                    ),
                    "frequency": record.get("Frequency") or "A",
                    "time_period": record.get("Year") or record.get("TimePeriod"),
                    "data_value": _safe_float(record.get("DataValue")),
                    "notes": record.get("NoteRef") or record.get("Notes"),
                }

                if parsed["time_period"] and parsed["industry_id"]:
                    parsed_records.append(parsed)

            except Exception as e:
                logger.warning(f"Failed to parse GDP by Industry record: {e}")

        logger.info(f"Parsed {len(parsed_records)} GDP by Industry records")
        return parsed_records

    except Exception as e:
        logger.error(f"Failed to parse GDP by Industry response: {e}")
        return []


def parse_international_response(
    response: Dict[str, Any], indicator_param: str
) -> List[Dict[str, Any]]:
    """
    Parse International Transactions data from BEA API response.

    Args:
        response: Raw API response
        indicator_param: Indicator parameter used in request

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    try:
        results = response.get("BEAAPI", {}).get("Results", {})

        if not results:
            logger.warning("No results in BEA International response")
            return []

        data = results.get("Data", [])

        if not data:
            logger.warning("No data in BEA International response")
            return []

        for record in data:
            try:
                parsed = {
                    "indicator": record.get("Indicator", indicator_param),
                    "indicator_description": record.get("IndicatorDescription")
                    or record.get("Description"),
                    "area_or_country": record.get("AreaOrCountry"),
                    "frequency": record.get("Frequency"),
                    "time_period": record.get("TimePeriod") or record.get("Year"),
                    "cl_unit": record.get("CL_UNIT"),
                    "unit_mult": _safe_int(record.get("UNIT_MULT")),
                    "data_value": _safe_float(record.get("DataValue")),
                }

                if parsed["time_period"] and parsed["area_or_country"]:
                    parsed_records.append(parsed)

            except Exception as e:
                logger.warning(f"Failed to parse International record: {e}")

        logger.info(f"Parsed {len(parsed_records)} International records")
        return parsed_records

    except Exception as e:
        logger.error(f"Failed to parse International response: {e}")
        return []


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float."""
    if value is None or value == "" or value == "(NA)" or value == "...":
        return None
    try:
        # Remove commas from numbers
        if isinstance(value, str):
            value = value.replace(",", "")
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Safely convert value to int."""
    if value is None or value == "" or value == "(NA)":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def get_default_year_range(dataset: str) -> str:
    """
    Get default year range for BEA data.

    Args:
        dataset: Dataset identifier

    Returns:
        Year specification string
    """
    current_year = datetime.now().year

    if dataset in ("nipa", "regional"):
        # Last 10 years
        years = [str(y) for y in range(current_year - 10, current_year + 1)]
        return ",".join(years)
    elif dataset == "gdp_industry":
        # Last 5 years
        years = [str(y) for y in range(current_year - 5, current_year + 1)]
        return ",".join(years)
    else:
        return "ALL"


def get_dataset_display_name(dataset: str) -> str:
    """Get human-readable display name for dataset."""
    display_names = {
        "nipa": "BEA National Income and Product Accounts (NIPA)",
        "regional": "BEA Regional Economic Accounts",
        "gdp_industry": "BEA GDP by Industry",
        "international": "BEA International Transactions",
    }
    return display_names.get(dataset, f"BEA {dataset.replace('_', ' ').title()}")


def get_dataset_description(dataset: str) -> str:
    """Get description for dataset."""
    descriptions = {
        "nipa": (
            "National Income and Product Accounts including GDP, Personal Income, "
            "Personal Consumption Expenditures (PCE), Government spending, "
            "Saving and Investment, and Corporate Profits."
        ),
        "regional": (
            "Regional economic data including GDP by state/county/metro, "
            "Personal Income by region, Employment, and Per Capita Income."
        ),
        "gdp_industry": (
            "GDP by Industry data including Value Added, Gross Output, "
            "Intermediate Inputs, and Price Indexes by industry."
        ),
        "international": (
            "International Transactions data including Trade Balance, "
            "Foreign Direct Investment, and International Investment Position."
        ),
    }
    return descriptions.get(dataset, f"Economic data from Bureau of Economic Analysis")


# Reference data for common geographic codes
GEO_FIPS_CODES = {
    "STATE": "All 50 states + DC",
    "COUNTY": "All US counties",
    "MSA": "Metropolitan Statistical Areas",
    "00000": "United States total",
    "01000": "Alabama",
    "02000": "Alaska",
    "04000": "Arizona",
    "05000": "Arkansas",
    "06000": "California",
    "08000": "Colorado",
    "09000": "Connecticut",
    "10000": "Delaware",
    "11000": "District of Columbia",
    "12000": "Florida",
    # ... (abbreviated for brevity)
}

# Frequency codes
FREQUENCY_CODES = {
    "A": "Annual",
    "Q": "Quarterly",
    "M": "Monthly",
}
