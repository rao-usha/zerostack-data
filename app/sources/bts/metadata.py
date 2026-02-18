"""
BTS metadata and schema generation utilities.

Handles:
- Table name generation for BTS datasets
- CREATE TABLE SQL generation
- Data parsing and transformation
- Schema definitions for Border Crossing, FAF5, VMT data
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


# ========== Schema Definitions ==========

# Border Crossing Entry Data schema
BORDER_CROSSING_COLUMNS = {
    "port_name": ("TEXT", "Port of entry name"),
    "state": ("TEXT", "State code"),
    "port_code": ("TEXT", "Port code"),
    "border": ("TEXT", "Border (US-Canada, US-Mexico)"),
    "date": ("TEXT", "Date (YYYY-MM)"),
    "measure": ("TEXT", "Measure type (Trucks, Containers, etc.)"),
    "value": ("BIGINT", "Count/volume"),
    "latitude": ("NUMERIC(10,6)", "Port latitude"),
    "longitude": ("NUMERIC(10,6)", "Port longitude"),
}

# FAF5 Freight Analysis Framework schema
FAF_REGIONAL_COLUMNS = {
    "fr_orig": ("TEXT", "Origin FAF zone code"),
    "dms_orig": ("TEXT", "Domestic origin state/region"),
    "dms_dest": ("TEXT", "Domestic destination state/region"),
    "fr_dest": ("TEXT", "Destination FAF zone code"),
    "fr_inmode": ("TEXT", "Inbound mode code"),
    "dms_mode": ("TEXT", "Mode of transport code"),
    "fr_outmode": ("TEXT", "Outbound mode code"),
    "sctg2": ("TEXT", "Commodity code (SCTG2)"),
    "trade_type": ("INTEGER", "Trade type (1=Domestic, 2=Import, 3=Export, 4=FTZ)"),
    "tons": ("NUMERIC(18,4)", "Tonnage (thousands)"),
    "value": ("NUMERIC(18,4)", "Value (millions USD)"),
    "tmiles": ("NUMERIC(18,4)", "Ton-miles (millions)"),
    "curval": ("NUMERIC(18,4)", "Current value"),
    "year": ("INTEGER", "Data year"),
}

# Vehicle Miles Traveled (VMT) schema
VMT_COLUMNS = {
    "date": ("TEXT", "Date (YYYY-MM)"),
    "state": ("TEXT", "State name"),
    "state_fips": ("TEXT", "State FIPS code"),
    "vmt": ("NUMERIC(18,4)", "Vehicle miles traveled (millions)"),
    "vmt_sa": ("NUMERIC(18,4)", "VMT seasonally adjusted"),
    "percent_change": ("NUMERIC(8,4)", "Percent change from prior year"),
    "functional_system": ("TEXT", "Road functional classification"),
}


def generate_table_name(dataset: str) -> str:
    """
    Generate PostgreSQL table name for BTS data.

    Args:
        dataset: Dataset identifier (border_crossing, faf_regional, vmt)

    Returns:
        PostgreSQL table name

    Examples:
        >>> generate_table_name("border_crossing")
        'bts_border_crossing'
        >>> generate_table_name("faf_regional")
        'bts_faf_regional'
    """
    dataset_clean = dataset.lower().replace("-", "_").replace(" ", "_")
    return f"bts_{dataset_clean}"


def generate_create_table_sql(table_name: str, dataset: str) -> str:
    """
    Generate CREATE TABLE SQL for BTS data.

    Args:
        table_name: PostgreSQL table name
        dataset: Dataset type to determine schema

    Returns:
        CREATE TABLE SQL statement
    """
    # Select appropriate schema
    if dataset == "border_crossing":
        columns = BORDER_CROSSING_COLUMNS
        unique_constraint = '"port_code", "date", "measure"'
        indexes = [
            ("date", '"date"'),
            ("border", '"border"'),
            ("state", '"state"'),
            ("measure", '"measure"'),
        ]
    elif dataset == "faf_regional":
        columns = FAF_REGIONAL_COLUMNS
        unique_constraint = (
            '"year", "fr_orig", "fr_dest", "sctg2", "dms_mode", "trade_type"'
        )
        indexes = [
            ("year", '"year"'),
            ("dms_mode", '"dms_mode"'),
            ("sctg2", '"sctg2"'),
            ("trade_type", '"trade_type"'),
            ("orig_dest", '"fr_orig", "fr_dest"'),
        ]
    elif dataset == "vmt":
        columns = VMT_COLUMNS
        unique_constraint = '"date", "state", COALESCE("functional_system", \'\')'
        indexes = [
            ("date", '"date"'),
            ("state", '"state"'),
        ]
    else:
        raise ValueError(f"Unknown BTS dataset: {dataset}")

    # Build column definitions
    column_defs = []
    column_defs.append("id SERIAL PRIMARY KEY")

    for col_name, (col_type, col_desc) in columns.items():
        # Quote column names to handle reserved words like 'state', 'date', 'value'
        # Don't use SQL comments as they would comment out the comma separators
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


def parse_border_crossing_response(
    records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Parse border crossing data from Socrata API response.

    Args:
        records: Raw API response records

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    for record in records:
        try:
            parsed = {
                "port_name": record.get("port_name"),
                "state": record.get("state"),
                "port_code": record.get("port_code"),
                "border": record.get("border"),
                "date": _normalize_date(record.get("date")),
                "measure": record.get("measure"),
                "value": _safe_int(record.get("value")),
                "latitude": _safe_float(record.get("latitude")),
                "longitude": _safe_float(record.get("longitude")),
            }

            # Skip records without required fields
            if parsed["port_code"] and parsed["date"] and parsed["measure"]:
                parsed_records.append(parsed)
            else:
                logger.debug(f"Skipping incomplete border crossing record: {record}")

        except Exception as e:
            logger.warning(f"Failed to parse border crossing record: {e}")

    logger.info(f"Parsed {len(parsed_records)} border crossing records")
    return parsed_records


def parse_vmt_response(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse VMT data from Socrata API response.

    Args:
        records: Raw API response records

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    for record in records:
        try:
            parsed = {
                "date": _normalize_date(record.get("date") or record.get("month")),
                "state": record.get("state") or record.get("state_name"),
                "state_fips": record.get("state_fips") or record.get("fips"),
                "vmt": _safe_float(record.get("vmt") or record.get("travel")),
                "vmt_sa": _safe_float(record.get("vmt_sa") or record.get("travel_sa")),
                "percent_change": _safe_float(
                    record.get("percent_change") or record.get("pct_change")
                ),
                "functional_system": record.get("functional_system")
                or record.get("f_system"),
            }

            # Skip records without required fields
            if parsed["date"] and parsed["state"]:
                parsed_records.append(parsed)
            else:
                logger.debug(f"Skipping incomplete VMT record: {record}")

        except Exception as e:
            logger.warning(f"Failed to parse VMT record: {e}")

    logger.info(f"Parsed {len(parsed_records)} VMT records")
    return parsed_records


def parse_faf_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse FAF5 freight data records.

    Args:
        records: Records from CSV parsing

    Returns:
        List of parsed records ready for insertion
    """
    parsed_records = []

    for record in records:
        try:
            parsed = {
                "fr_orig": record.get("fr_orig"),
                "dms_orig": record.get("dms_orig"),
                "dms_dest": record.get("dms_dest"),
                "fr_dest": record.get("fr_dest"),
                "fr_inmode": record.get("fr_inmode"),
                "dms_mode": record.get("dms_mode"),
                "fr_outmode": record.get("fr_outmode"),
                "sctg2": record.get("sctg2"),
                "trade_type": record.get("trade_type"),
                "tons": record.get("tons"),
                "value": record.get("value"),
                "tmiles": record.get("tmiles"),
                "curval": record.get("curval"),
                "year": record.get("year"),
            }

            # Skip records without critical fields
            if parsed["fr_orig"] and parsed["fr_dest"]:
                parsed_records.append(parsed)

        except Exception as e:
            logger.warning(f"Failed to parse FAF record: {e}")

    logger.info(f"Parsed {len(parsed_records)} FAF records")
    return parsed_records


def _normalize_date(date_str: Optional[str]) -> Optional[str]:
    """
    Normalize date string to YYYY-MM format.

    Args:
        date_str: Date string in various formats

    Returns:
        Normalized date string (YYYY-MM) or None
    """
    if not date_str:
        return None

    try:
        # Handle ISO format (2024-01-01T00:00:00.000)
        if "T" in date_str:
            date_str = date_str.split("T")[0]

        # Handle YYYY-MM-DD
        if len(date_str) == 10 and date_str[4] == "-":
            return date_str[:7]  # Return YYYY-MM

        # Handle YYYY-MM
        if len(date_str) == 7 and date_str[4] == "-":
            return date_str

        # Handle MM/YYYY
        if "/" in date_str:
            parts = date_str.split("/")
            if len(parts) == 2:
                month, year = parts
                return f"{year}-{month.zfill(2)}"

        return date_str

    except Exception:
        return date_str


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


def get_default_date_range(dataset: str) -> Tuple[str, str]:
    """
    Get default date range for BTS data.

    Args:
        dataset: Dataset identifier

    Returns:
        Tuple of (start_date, end_date)
    """
    end_date = datetime.now()

    if dataset == "border_crossing":
        # Last 5 years of border crossing data
        start_date = end_date.replace(year=end_date.year - 5)
        return (start_date.strftime("%Y-%m"), end_date.strftime("%Y-%m"))
    elif dataset == "vmt":
        # Last 3 years of VMT data
        start_date = end_date.replace(year=end_date.year - 3)
        return (start_date.strftime("%Y-%m"), end_date.strftime("%Y-%m"))
    elif dataset == "faf_regional":
        # FAF5 is 2018-2024
        return ("2018", "2024")
    else:
        # Default to last 5 years
        start_date = end_date.replace(year=end_date.year - 5)
        return (start_date.strftime("%Y-%m"), end_date.strftime("%Y-%m"))


def get_dataset_display_name(dataset: str) -> str:
    """Get human-readable display name for dataset."""
    display_names = {
        "border_crossing": "BTS Border Crossing Entry Data",
        "faf_regional": "BTS Freight Analysis Framework (FAF5) Regional Data",
        "vmt": "BTS Vehicle Miles Traveled (VMT)",
    }
    return display_names.get(dataset, f"BTS {dataset.replace('_', ' ').title()}")


def get_dataset_description(dataset: str) -> str:
    """Get description for dataset."""
    descriptions = {
        "border_crossing": (
            "Monthly statistics on border crossings at US ports of entry, "
            "including trucks, containers, trains, buses, and personal vehicles. "
            "Source: Bureau of Transportation Statistics via Socrata."
        ),
        "faf_regional": (
            "Freight Analysis Framework version 5 (FAF5) regional database with "
            "freight tonnage, value, and ton-miles by origin-destination pair, "
            "commodity type, and transport mode. Source: BTS/FHWA."
        ),
        "vmt": (
            "Vehicle Miles Traveled (VMT) by state, measuring traffic volume "
            "on all public roads. Used as a proxy for consumer activity and "
            "economic health. Source: FHWA Traffic Volume Trends."
        ),
    }
    return descriptions.get(
        dataset,
        f"Transportation statistics data from Bureau of Transportation Statistics",
    )


# Reference data for FAF commodity codes (SCTG2)
SCTG2_COMMODITIES = {
    "01": "Live animals and fish",
    "02": "Cereal grains",
    "03": "Other agricultural products",
    "04": "Animal feed",
    "05": "Meat, seafood, and preparations",
    "06": "Milled grain products and preparations",
    "07": "Other foodstuffs",
    "08": "Alcoholic beverages",
    "09": "Tobacco products",
    "10": "Building stone",
    "11": "Natural sands",
    "12": "Gravel and crushed stone",
    "13": "Nonmetallic minerals",
    "14": "Metallic ores",
    "15": "Coal",
    "16": "Crude petroleum",
    "17": "Gasoline and aviation turbine fuel",
    "18": "Fuel oils",
    "19": "Products of petroleum refining",
    "20": "Basic chemicals",
    "21": "Pharmaceutical products",
    "22": "Fertilizers",
    "23": "Chemical products and preparations",
    "24": "Plastics and rubber",
    "25": "Logs and other wood in the rough",
    "26": "Wood products",
    "27": "Pulp, newsprint, paper, and paperboard",
    "28": "Paper or paperboard articles",
    "29": "Printed products",
    "30": "Textiles, leather, and articles",
    "31": "Nonmetallic mineral products",
    "32": "Base metal in primary or semi-finished forms",
    "33": "Articles of base metal",
    "34": "Machinery",
    "35": "Electronic and other electrical equipment",
    "36": "Motorized and other vehicles",
    "37": "Transportation equipment",
    "38": "Precision instruments and apparatus",
    "39": "Furniture, mattresses, and lighting",
    "40": "Miscellaneous manufactured products",
    "41": "Waste and scrap",
    "43": "Mixed freight",
    "99": "Unknown",
}

# FAF zone code reference (partial - major zones)
FAF_ZONES = {
    "011": "Birmingham-Hoover AL",
    "012": "Rest of Alabama",
    "020": "Alaska",
    "041": "Phoenix-Mesa AZ",
    "042": "Rest of Arizona",
    "051": "Los Angeles-Long Beach CA",
    "061": "Denver-Aurora CO",
    "091": "Miami-Fort Lauderdale FL",
    "111": "Atlanta-Sandy Springs GA",
    "171": "Chicago-Naperville IL",
    "181": "Indianapolis-Carmel IN",
    "221": "New Orleans-Metairie LA",
    "251": "Boston-Cambridge MA",
    "261": "Detroit-Warren MI",
    "271": "Minneapolis-St. Paul MN",
    "291": "St. Louis MO-IL",
    "341": "New York-Newark NY-NJ-CT",
    "361": "Cleveland-Elyria OH",
    "371": "Portland-Vancouver OR-WA",
    "421": "Philadelphia-Camden PA-NJ-DE-MD",
    "451": "Memphis TN-MS-AR",
    "481": "Dallas-Fort Worth TX",
    "482": "Houston-The Woodlands TX",
    "531": "Seattle-Tacoma WA",
}
