"""
Census metadata parsing and schema mapping.

Converts Census variable definitions to Postgres column types.
"""

import logging
import re
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)

# Reserved SQL keywords that need to be quoted or renamed
SQL_RESERVED_KEYWORDS = {
    "user",
    "table",
    "column",
    "select",
    "from",
    "where",
    "order",
    "group",
    "by",
    "having",
    "limit",
    "offset",
    "join",
    "inner",
    "outer",
    "left",
    "right",
    "on",
    "as",
    "and",
    "or",
    "not",
    "null",
    "default",
    "primary",
    "foreign",
    "key",
    "unique",
    "index",
    "constraint",
    "create",
    "drop",
    "alter",
    "truncate",
    "insert",
    "update",
    "delete",
    "grant",
    "revoke",
}


def clean_column_name(name: str) -> str:
    """
    Clean a Census variable name to make it a valid Postgres column name.

    Rules:
    - Replace invalid characters with underscores
    - Ensure starts with letter or underscore
    - Handle reserved keywords
    - Lowercase for consistency

    Args:
        name: Original variable name (e.g., "B01001_001E")

    Returns:
        Clean column name safe for Postgres
    """
    # Convert to lowercase
    clean = name.lower()

    # Replace invalid characters with underscore
    clean = re.sub(r"[^a-z0-9_]", "_", clean)

    # Ensure starts with letter or underscore
    if clean and clean[0].isdigit():
        clean = f"col_{clean}"

    # Handle reserved keywords
    if clean in SQL_RESERVED_KEYWORDS:
        clean = f"{clean}_col"

    return clean


def map_census_type_to_postgres(predicate_type: str) -> str:
    """
    Map Census API data type to Postgres column type.

    Census predicateType values:
    - "int" -> INTEGER
    - "float" -> NUMERIC
    - "string" -> TEXT

    Args:
        predicate_type: Census predicateType value

    Returns:
        Postgres column type
    """
    type_map = {"int": "INTEGER", "float": "NUMERIC", "string": "TEXT"}
    return type_map.get(predicate_type.lower(), "TEXT")


def parse_table_metadata(
    metadata: Dict[str, Any], table_id: str
) -> Dict[str, Dict[str, Any]]:
    """
    Parse Census metadata and extract relevant variables for a table.

    Filters variables to only those belonging to the specified table.

    Args:
        metadata: Full metadata dictionary from Census API
        table_id: Table identifier (e.g., "B01001")

    Returns:
        Dictionary mapping variable names to their metadata:
        {
            "B01001_001E": {
                "label": "Estimate!!Total:",
                "concept": "SEX BY AGE",
                "postgres_type": "INTEGER",
                "column_name": "b01001_001e"
            },
            ...
        }
    """
    variables = metadata.get("variables", {})
    table_vars = {}

    # Filter to variables for this table
    for var_name, var_info in variables.items():
        # Census variables follow pattern: TABLE_XXXE/M/PE (E=estimate, M=margin, PE=percent)
        # We want EXACT table match only (e.g., B01001_xxx, not B01001A_xxx)
        # This avoids pulling in race-specific subtables
        var_upper = var_name.upper()
        table_upper = table_id.upper()

        # Must start with table_id followed by underscore
        if not var_upper.startswith(table_upper + "_"):
            continue

        # Check if this is a subtable (has a letter immediately after table_id)
        # Example: B01001A_001E has 'A' after B01001, so it's a subtable
        # Example: B01001_001E has '_' after B01001, so it's the main table
        prefix_match = var_name[: len(table_id)]
        if prefix_match.upper() == table_upper:
            # Check the character right after the table_id
            next_pos = len(table_id)
            if next_pos < len(var_name):
                next_char = var_name[next_pos]
                # If it's a letter (A-Z, case insensitive), it's a subtable - skip it
                if next_char.isalpha():
                    continue
                # If it's underscore, it's the main table - keep it
                if next_char == "_":
                    pass  # This is what we want
                else:
                    continue  # Some other character, skip

        # Skip annotation variables (end with A, EA, MA, etc.)
        if var_upper.endswith("A"):
            continue

        # Extract metadata
        label = var_info.get("label", "")
        concept = var_info.get("concept", "")
        predicate_type = var_info.get("predicateType", "string")

        # Map to Postgres
        postgres_type = map_census_type_to_postgres(predicate_type)
        column_name = clean_column_name(var_name)

        table_vars[var_name] = {
            "label": label,
            "concept": concept,
            "predicate_type": predicate_type,
            "postgres_type": postgres_type,
            "column_name": column_name,
        }

    logger.info(f"Parsed {len(table_vars)} variables for table {table_id}")
    return table_vars


def generate_create_table_sql(
    table_name: str,
    table_vars: Dict[str, Dict[str, Any]],
    include_geo_columns: bool = True,
) -> str:
    """
    Generate CREATE TABLE SQL statement for a Census table.

    Idempotent - uses CREATE TABLE IF NOT EXISTS.

    Args:
        table_name: Name for the Postgres table
        table_vars: Variable metadata from parse_table_metadata
        include_geo_columns: Whether to add geography columns

    Returns:
        SQL CREATE TABLE statement
    """
    columns = []

    # Add ID column
    columns.append("    id SERIAL PRIMARY KEY")

    # Add geography columns if requested
    if include_geo_columns:
        columns.append("    geo_name TEXT")  # Geographic area name
        columns.append("    geo_id TEXT")  # Geographic identifier
        columns.append("    state_fips TEXT")
        # Add more geo columns as needed based on geo_level

    # Add data columns from Census variables
    for var_name, var_meta in table_vars.items():
        col_name = var_meta["column_name"]
        col_type = var_meta["postgres_type"]
        # Add comment with variable label
        columns.append(f"    {col_name} {col_type}")

    columns_sql = ",\n".join(columns)

    sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{columns_sql}
);"""

    return sql


def build_column_mapping(table_vars: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Build a mapping from Census variable names to Postgres column names.

    Args:
        table_vars: Variable metadata from parse_table_metadata

    Returns:
        Dictionary mapping Census name -> Postgres column name
    """
    return {
        var_name: var_meta["column_name"] for var_name, var_meta in table_vars.items()
    }
