"""
US Trade metadata and schema generation utilities.

Handles:
- Table name generation for trade datasets
- CREATE TABLE SQL generation
- Data parsing and transformation
- Schema definitions for exports, imports, port-level, and state-level data
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


# ========== Schema Definitions ==========

# Exports by HS code schema
EXPORTS_HS_COLUMNS = {
    "year": ("INTEGER", "Data year"),
    "month": ("INTEGER", "Data month (1-12)"),
    "country_code": ("TEXT", "Census country code"),
    "country_name": ("TEXT", "Country name"),
    "hs_code": ("TEXT", "Harmonized System code"),
    "commodity_desc": ("TEXT", "Commodity description"),
    "value_monthly": ("BIGINT", "Export value (USD) for month"),
    "value_ytd": ("BIGINT", "Export value (USD) year-to-date"),
    "quantity_monthly": ("NUMERIC(18,2)", "Quantity for month"),
    "quantity_ytd": ("NUMERIC(18,2)", "Quantity year-to-date"),
    "quantity_unit": ("TEXT", "Unit of quantity"),
}

# Imports by HS code schema
IMPORTS_HS_COLUMNS = {
    "year": ("INTEGER", "Data year"),
    "month": ("INTEGER", "Data month (1-12)"),
    "country_code": ("TEXT", "Census country code"),
    "country_name": ("TEXT", "Country name"),
    "hs_code": ("TEXT", "Harmonized System code"),
    "commodity_desc": ("TEXT", "Commodity description"),
    "general_value_monthly": ("BIGINT", "General import value (USD) for month"),
    "general_value_ytd": ("BIGINT", "General import value (USD) year-to-date"),
    "consumption_value_monthly": ("BIGINT", "Consumption value (USD) for month"),
    "consumption_value_ytd": ("BIGINT", "Consumption value (USD) year-to-date"),
    "dutyfree_value_monthly": ("BIGINT", "Duty-free value (USD) for month"),
    "dutiable_value_monthly": ("BIGINT", "Dutiable value (USD) for month"),
    "quantity_monthly": ("NUMERIC(18,2)", "Quantity for month"),
    "quantity_ytd": ("NUMERIC(18,2)", "Quantity year-to-date"),
    "quantity_unit": ("TEXT", "Unit of quantity"),
}

# Exports by state schema
EXPORTS_STATE_COLUMNS = {
    "year": ("INTEGER", "Data year"),
    "month": ("INTEGER", "Data month (1-12)"),
    "state_code": ("TEXT", "State FIPS code"),
    "state_name": ("TEXT", "State name"),
    "country_code": ("TEXT", "Census country code"),
    "country_name": ("TEXT", "Country name"),
    "hs_code": ("TEXT", "Harmonized System code"),
    "commodity_desc": ("TEXT", "Commodity description"),
    "value_monthly": ("BIGINT", "Export value (USD) for month"),
    "value_ytd": ("BIGINT", "Export value (USD) year-to-date"),
}

# Exports/Imports by port schema
PORT_TRADE_COLUMNS = {
    "year": ("INTEGER", "Data year"),
    "month": ("INTEGER", "Data month (1-12)"),
    "district_code": ("TEXT", "Customs district code"),
    "district_name": ("TEXT", "Customs district name"),
    "country_code": ("TEXT", "Census country code"),
    "country_name": ("TEXT", "Country name"),
    "hs_code": ("TEXT", "Harmonized System code"),
    "commodity_desc": ("TEXT", "Commodity description"),
    "trade_type": ("TEXT", "Export or Import"),
    "value_monthly": ("BIGINT", "Trade value (USD) for month"),
    "value_ytd": ("BIGINT", "Trade value (USD) year-to-date"),
}

# Trade summary by country schema (aggregated)
TRADE_SUMMARY_COLUMNS = {
    "year": ("INTEGER", "Data year"),
    "month": ("INTEGER", "Data month (1-12, NULL for annual)"),
    "country_code": ("TEXT", "Census country code"),
    "country_name": ("TEXT", "Country name"),
    "exports_value": ("BIGINT", "Total exports value (USD)"),
    "imports_value": ("BIGINT", "Total imports value (USD)"),
    "total_trade": ("BIGINT", "Total trade value (exports + imports)"),
    "trade_balance": ("BIGINT", "Trade balance (exports - imports)"),
}


def generate_table_name(dataset: str, year: Optional[int] = None) -> str:
    """
    Generate PostgreSQL table name for US Trade data.

    Args:
        dataset: Dataset identifier
        year: Optional year for year-specific tables

    Returns:
        PostgreSQL table name

    Examples:
        >>> generate_table_name("exports_hs")
        'us_trade_exports_hs'
        >>> generate_table_name("exports_hs", 2024)
        'us_trade_exports_hs_2024'
    """
    dataset_clean = dataset.lower().replace("-", "_").replace(" ", "_")

    if year:
        return f"us_trade_{dataset_clean}_{year}"
    return f"us_trade_{dataset_clean}"


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for US Trade data.

    Args:
        table_name: PostgreSQL table name
        dataset: Dataset type to determine schema

    Returns:
        CREATE TABLE SQL statement
    """
    # Select appropriate schema
    if dataset == "exports_hs":
        columns = EXPORTS_HS_COLUMNS
        unique_constraint = '"year", "month", "country_code", "hs_code"'
        indexes = [
            ("year_month", '"year", "month"'),
            ("country", '"country_code"'),
            ("hs_code", '"hs_code"'),
            ("value", '"value_monthly" DESC'),
        ]
    elif dataset == "imports_hs":
        columns = IMPORTS_HS_COLUMNS
        unique_constraint = '"year", "month", "country_code", "hs_code"'
        indexes = [
            ("year_month", '"year", "month"'),
            ("country", '"country_code"'),
            ("hs_code", '"hs_code"'),
            ("value", '"general_value_monthly" DESC'),
        ]
    elif dataset == "exports_state":
        columns = EXPORTS_STATE_COLUMNS
        unique_constraint = '"year", "month", "state_code", "country_code", "hs_code"'
        indexes = [
            ("year_month", '"year", "month"'),
            ("state", '"state_code"'),
            ("country", '"country_code"'),
            ("hs_code", '"hs_code"'),
        ]
    elif dataset in ("exports_port", "imports_port", "port_trade"):
        columns = PORT_TRADE_COLUMNS
        unique_constraint = (
            '"year", "month", "district_code", "country_code", "hs_code", "trade_type"'
        )
        indexes = [
            ("year_month", '"year", "month"'),
            ("district", '"district_code"'),
            ("country", '"country_code"'),
            ("trade_type", '"trade_type"'),
        ]
    elif dataset == "trade_summary":
        columns = TRADE_SUMMARY_COLUMNS
        unique_constraint = '"year", COALESCE("month", 0), "country_code"'
        indexes = [
            ("year_month", '"year", "month"'),
            ("country", '"country_code"'),
            ("trade_balance", '"trade_balance"'),
            ("total_trade", '"total_trade" DESC'),
        ]
    else:
        raise ValueError(f"Unknown US Trade dataset: {dataset}")

    # Build column definitions
    column_defs = []
    column_defs.append("id SERIAL PRIMARY KEY")

    for col_name, (col_type, col_desc) in columns.items():
        column_defs.append(f'"{col_name}" {col_type}')

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


def parse_exports_hs_response(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse exports by HS code from Census API response.

    Args:
        records: Raw API response records

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    for record in records:
        try:
            # Parse time field (format: "2024-01") into year and month
            time_val = record.get("time", "")
            year, month = None, None
            if time_val and "-" in time_val:
                parts = time_val.split("-")
                year = _safe_int(parts[0])
                month = _safe_int(parts[1]) if len(parts) > 1 else None

            parsed = {
                "year": year,
                "month": month,
                "country_code": record.get("CTY_CODE"),
                "country_name": record.get("CTY_NAME"),
                "hs_code": record.get("E_COMMODITY"),
                "commodity_desc": record.get("E_COMMODITY_LDESC"),
                "value_monthly": _safe_int(record.get("ALL_VAL_MO")),
                "value_ytd": _safe_int(record.get("ALL_VAL_YR")),
                "quantity_monthly": _safe_float(record.get("QTY_1_MO")),
                "quantity_ytd": _safe_float(record.get("QTY_1_YR")),
                "quantity_unit": record.get("UNIT_QY1"),
            }

            # Skip records without required fields
            if parsed["year"] and parsed["country_code"]:
                parsed_records.append(parsed)
            else:
                logger.debug(f"Skipping incomplete export record: {record}")

        except Exception as e:
            logger.warning(f"Failed to parse export record: {e}")

    logger.info(f"Parsed {len(parsed_records)} export records")
    return parsed_records


def parse_imports_hs_response(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse imports by HS code from Census API response.

    Args:
        records: Raw API response records

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    for record in records:
        try:
            # Parse time field (format: "2024-01") into year and month
            time_val = record.get("time", "")
            year, month = None, None
            if time_val and "-" in time_val:
                parts = time_val.split("-")
                year = _safe_int(parts[0])
                month = _safe_int(parts[1]) if len(parts) > 1 else None

            parsed = {
                "year": year,
                "month": month,
                "country_code": record.get("CTY_CODE"),
                "country_name": record.get("CTY_NAME"),
                "hs_code": record.get("I_COMMODITY"),
                "commodity_desc": record.get("I_COMMODITY_LDESC"),
                "general_value_monthly": _safe_int(record.get("GEN_VAL_MO")),
                "general_value_ytd": _safe_int(record.get("GEN_VAL_YR")),
                "consumption_value_monthly": _safe_int(record.get("CON_VAL_MO")),
                "consumption_value_ytd": _safe_int(record.get("CON_VAL_YR")),
                "dutyfree_value_monthly": None,  # Not directly available in API
                "dutiable_value_monthly": _safe_int(record.get("DUT_VAL_MO")),
                "quantity_monthly": _safe_float(record.get("GEN_QY1_MO")),
                "quantity_ytd": _safe_float(record.get("GEN_QY1_YR")),
                "quantity_unit": record.get("UNIT_QY1"),
            }

            if parsed["year"] and parsed["country_code"]:
                parsed_records.append(parsed)
            else:
                logger.debug(f"Skipping incomplete import record: {record}")

        except Exception as e:
            logger.warning(f"Failed to parse import record: {e}")

    logger.info(f"Parsed {len(parsed_records)} import records")
    return parsed_records


def parse_exports_state_response(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse state exports from Census API response.

    Args:
        records: Raw API response records

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    for record in records:
        try:
            # Parse time field (format: "2024-01") into year and month
            time_val = record.get("time", "")
            year, month = None, None
            if time_val and "-" in time_val:
                parts = time_val.split("-")
                year = _safe_int(parts[0])
                month = _safe_int(parts[1]) if len(parts) > 1 else None

            parsed = {
                "year": year,
                "month": month,
                "state_code": record.get("STATE"),
                "state_name": None,  # Not available in API, use state_code
                "country_code": record.get("CTY_CODE"),
                "country_name": record.get("CTY_NAME"),
                "hs_code": record.get("E_COMMODITY"),
                "commodity_desc": record.get("E_COMMODITY_LDESC"),
                "value_monthly": _safe_int(record.get("ALL_VAL_MO")),
                "value_ytd": _safe_int(record.get("ALL_VAL_YR")),
            }

            if parsed["year"] and parsed["state_code"]:
                parsed_records.append(parsed)

        except Exception as e:
            logger.warning(f"Failed to parse state export record: {e}")

    logger.info(f"Parsed {len(parsed_records)} state export records")
    return parsed_records


def parse_port_trade_response(
    records: List[Dict[str, Any]], trade_type: str = "export"
) -> List[Dict[str, Any]]:
    """
    Parse port-level trade data from Census API response.

    Args:
        records: Raw API response records
        trade_type: "export" or "import"

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    for record in records:
        try:
            # Parse time field (format: "2024-01") into year and month
            time_val = record.get("time", "")
            year, month = None, None
            if time_val and "-" in time_val:
                parts = time_val.split("-")
                year = _safe_int(parts[0])
                month = _safe_int(parts[1]) if len(parts) > 1 else None

            # Value and HS code fields differ between exports and imports
            if trade_type == "export":
                value_monthly = _safe_int(record.get("ALL_VAL_MO"))
                value_ytd = _safe_int(record.get("ALL_VAL_YR"))
                hs_code = record.get("E_COMMODITY")
                commodity_desc = record.get("E_COMMODITY_LDESC")
            else:
                value_monthly = _safe_int(record.get("GEN_VAL_MO"))
                value_ytd = _safe_int(record.get("GEN_VAL_YR"))
                hs_code = record.get("I_COMMODITY")
                commodity_desc = record.get("I_COMMODITY_LDESC")

            parsed = {
                "year": year,
                "month": month,
                "district_code": record.get("DISTRICT"),
                "district_name": record.get("DIST_NAME"),
                "country_code": record.get("CTY_CODE"),
                "country_name": record.get("CTY_NAME"),
                "hs_code": hs_code,
                "commodity_desc": commodity_desc,
                "trade_type": trade_type,
                "value_monthly": value_monthly,
                "value_ytd": value_ytd,
            }

            if parsed["year"] and parsed["district_code"]:
                parsed_records.append(parsed)

        except Exception as e:
            logger.warning(f"Failed to parse port trade record: {e}")

    logger.info(f"Parsed {len(parsed_records)} port {trade_type} records")
    return parsed_records


def parse_trade_summary(
    exports: List[Dict[str, Any]],
    imports: List[Dict[str, Any]],
    year: int,
    month: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Create trade summary by country from exports and imports data.

    Args:
        exports: Export records
        imports: Import records
        year: Data year
        month: Data month (None for annual)

    Returns:
        List of trade summary records
    """
    country_data = {}

    # Process exports
    for record in exports:
        cty_code = record.get("CTY_CODE") or record.get("country_code")
        if not cty_code:
            continue

        if cty_code not in country_data:
            country_data[cty_code] = {
                "year": year,
                "month": month,
                "country_code": cty_code,
                "country_name": record.get("CTY_NAME") or record.get("country_name"),
                "exports_value": 0,
                "imports_value": 0,
            }

        val = (
            record.get("ALL_VAL_YR")
            or record.get("ALL_VAL_MO")
            or record.get("value_ytd")
            or record.get("value_monthly")
            or 0
        )
        country_data[cty_code]["exports_value"] += int(val) if val else 0

    # Process imports
    for record in imports:
        cty_code = record.get("CTY_CODE") or record.get("country_code")
        if not cty_code:
            continue

        if cty_code not in country_data:
            country_data[cty_code] = {
                "year": year,
                "month": month,
                "country_code": cty_code,
                "country_name": record.get("CTY_NAME") or record.get("country_name"),
                "exports_value": 0,
                "imports_value": 0,
            }

        val = (
            record.get("GEN_VAL_YR")
            or record.get("GEN_VAL_MO")
            or record.get("general_value_ytd")
            or record.get("general_value_monthly")
            or 0
        )
        country_data[cty_code]["imports_value"] += int(val) if val else 0

    # Calculate totals
    results = []
    for cty in country_data.values():
        cty["total_trade"] = cty["exports_value"] + cty["imports_value"]
        cty["trade_balance"] = cty["exports_value"] - cty["imports_value"]
        results.append(cty)

    logger.info(f"Created trade summary for {len(results)} countries")
    return results


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float."""
    if value is None or value == "" or value == "NA":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Safely convert value to int."""
    if value is None or value == "" or value == "NA":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def get_default_date_range(dataset: str) -> Tuple[int, int, Optional[int]]:
    """
    Get default date range for US Trade data.

    Args:
        dataset: Dataset identifier

    Returns:
        Tuple of (start_year, end_year, month or None for annual)
    """
    current_year = datetime.now().year
    current_month = datetime.now().month

    # Default to last 3 years of annual data
    return (current_year - 3, current_year, None)


def get_dataset_display_name(dataset: str) -> str:
    """Get human-readable display name for dataset."""
    display_names = {
        "exports_hs": "US Exports by HS Code",
        "imports_hs": "US Imports by HS Code",
        "exports_state": "US State Exports",
        "exports_port": "US Exports by Port/District",
        "imports_port": "US Imports by Port/District",
        "trade_summary": "US Trade Summary by Country",
    }
    return display_names.get(dataset, f"US Trade {dataset.replace('_', ' ').title()}")


def get_dataset_description(dataset: str) -> str:
    """Get description for dataset."""
    descriptions = {
        "exports_hs": (
            "US export data classified by Harmonized System (HS) codes. "
            "Includes value, quantity, and trading partner by commodity. "
            "Source: US Census Bureau International Trade API."
        ),
        "imports_hs": (
            "US import data classified by Harmonized System (HS) codes. "
            "Includes general value, consumption value, duty status, and quantities. "
            "Source: US Census Bureau International Trade API."
        ),
        "exports_state": (
            "State-level US export data by HS code and trading partner. "
            "Shows which states export what commodities to which countries. "
            "Source: US Census Bureau International Trade API."
        ),
        "exports_port": (
            "US export data by customs district (port of entry). "
            "Shows trade volumes through each port by commodity and country. "
            "Source: US Census Bureau International Trade API."
        ),
        "imports_port": (
            "US import data by customs district (port of entry). "
            "Shows trade volumes through each port by commodity and country. "
            "Source: US Census Bureau International Trade API."
        ),
        "trade_summary": (
            "Aggregated US trade summary by country showing exports, imports, "
            "total trade, and trade balance. Derived from Census Bureau data."
        ),
    }
    return descriptions.get(
        dataset, "International trade statistics from US Census Bureau"
    )


# ========== HS Code Utilities ==========


def get_hs_chapter(hs_code: str) -> str:
    """Get the 2-digit HS chapter from a longer code."""
    if not hs_code:
        return ""
    return hs_code[:2]


def get_hs_heading(hs_code: str) -> str:
    """Get the 4-digit HS heading from a longer code."""
    if not hs_code:
        return ""
    return hs_code[:4]


def get_hs_subheading(hs_code: str) -> str:
    """Get the 6-digit HS subheading from a longer code."""
    if not hs_code:
        return ""
    return hs_code[:6]


# Major commodity categories for analysis
MAJOR_COMMODITY_CHAPTERS = {
    "agricultural": [
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
    ],
    "fuels": ["27"],
    "chemicals": ["28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38"],
    "plastics_rubber": ["39", "40"],
    "textiles": [
        "50",
        "51",
        "52",
        "53",
        "54",
        "55",
        "56",
        "57",
        "58",
        "59",
        "60",
        "61",
        "62",
        "63",
    ],
    "metals": ["72", "73", "74", "75", "76", "78", "79", "80", "81", "82", "83"],
    "machinery": ["84", "85"],
    "vehicles": ["86", "87", "88", "89"],
    "instruments": ["90", "91", "92"],
}
