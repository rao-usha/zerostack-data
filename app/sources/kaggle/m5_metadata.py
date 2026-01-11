"""
M5 Forecasting dataset metadata and schema definitions.

The M5 dataset is from the M5 Forecasting competition on Kaggle:
https://www.kaggle.com/competitions/m5-forecasting-accuracy

Dataset structure:
- sales_train_validation.csv: 30,490 items × 1,969 days of daily unit sales
- calendar.csv: Calendar with dates, events, SNAP info
- sell_prices.csv: Store/item/week level prices

Schema design:
- We transform the wide-format sales data to long format for efficient querying
- Proper typed columns (INT, NUMERIC, TEXT) per RULES
- Primary keys and indexes for common query patterns

M5 Hierarchy:
- State (3): CA, TX, WI
- Store (10): CA_1-4, TX_1-3, WI_1-3
- Category (3): HOBBIES, HOUSEHOLD, FOODS
- Department (7): HOBBIES_1, HOBBIES_2, HOUSEHOLD_1, HOUSEHOLD_2, FOODS_1, FOODS_2, FOODS_3
- Item (~3,000): Item-level SKUs
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# TABLE NAME GENERATION
# =============================================================================

def generate_table_name(table_type: str) -> str:
    """
    Generate Postgres table name for M5 dataset components.
    
    Args:
        table_type: One of 'sales', 'calendar', 'prices', 'items'
        
    Returns:
        Table name string (e.g., 'm5_sales', 'm5_calendar')
    """
    valid_types = {'sales', 'calendar', 'prices', 'items'}
    if table_type not in valid_types:
        raise ValueError(f"Invalid table type: {table_type}. Must be one of {valid_types}")
    
    return f"m5_{table_type}"


# =============================================================================
# SCHEMA DEFINITIONS
# =============================================================================

M5_CALENDAR_SCHEMA = {
    "table_name": "m5_calendar",
    "description": "M5 calendar dimension with dates, events, and SNAP indicators",
    "columns": [
        # Primary key
        {"name": "date", "type": "DATE", "nullable": False, "pk": True},
        {"name": "d", "type": "VARCHAR(10)", "nullable": False, "description": "Day identifier (d_1 to d_1969)"},
        
        # Time dimensions
        {"name": "wm_yr_wk", "type": "INTEGER", "nullable": False, "description": "Walmart year-week"},
        {"name": "weekday", "type": "VARCHAR(10)", "nullable": False},
        {"name": "wday", "type": "INTEGER", "nullable": False, "description": "Day of week (1-7)"},
        {"name": "month", "type": "INTEGER", "nullable": False},
        {"name": "year", "type": "INTEGER", "nullable": False},
        
        # Events
        {"name": "event_name_1", "type": "VARCHAR(50)", "nullable": True},
        {"name": "event_type_1", "type": "VARCHAR(30)", "nullable": True},
        {"name": "event_name_2", "type": "VARCHAR(50)", "nullable": True},
        {"name": "event_type_2", "type": "VARCHAR(30)", "nullable": True},
        
        # SNAP (food stamp) indicators by state
        {"name": "snap_ca", "type": "INTEGER", "nullable": False, "description": "SNAP day for California (0/1)"},
        {"name": "snap_tx", "type": "INTEGER", "nullable": False, "description": "SNAP day for Texas (0/1)"},
        {"name": "snap_wi", "type": "INTEGER", "nullable": False, "description": "SNAP day for Wisconsin (0/1)"},
        
        # Metadata
        {"name": "ingested_at", "type": "TIMESTAMP", "nullable": False, "default": "NOW()"},
    ],
    "indexes": [
        {"name": "idx_m5_calendar_d", "columns": ["d"]},
        {"name": "idx_m5_calendar_wm_yr_wk", "columns": ["wm_yr_wk"]},
        {"name": "idx_m5_calendar_year_month", "columns": ["year", "month"]},
    ]
}

M5_PRICES_SCHEMA = {
    "table_name": "m5_prices",
    "description": "M5 price data at store/item/week level",
    "columns": [
        # Composite primary key
        {"name": "store_id", "type": "VARCHAR(10)", "nullable": False},
        {"name": "item_id", "type": "VARCHAR(30)", "nullable": False},
        {"name": "wm_yr_wk", "type": "INTEGER", "nullable": False, "description": "Walmart year-week"},
        
        # Price
        {"name": "sell_price", "type": "NUMERIC(10, 2)", "nullable": False},
        
        # Metadata
        {"name": "ingested_at", "type": "TIMESTAMP", "nullable": False, "default": "NOW()"},
    ],
    "primary_key": ["store_id", "item_id", "wm_yr_wk"],
    "indexes": [
        {"name": "idx_m5_prices_store", "columns": ["store_id"]},
        {"name": "idx_m5_prices_item", "columns": ["item_id"]},
        {"name": "idx_m5_prices_wm_yr_wk", "columns": ["wm_yr_wk"]},
    ]
}

M5_ITEMS_SCHEMA = {
    "table_name": "m5_items",
    "description": "M5 item dimension with hierarchy (category, department, store, state)",
    "columns": [
        # Primary key - unique combination of item and store
        {"name": "id", "type": "VARCHAR(50)", "nullable": False, "pk": True, "description": "Unique ID (item_id + store_id)"},
        
        # Item identification
        {"name": "item_id", "type": "VARCHAR(30)", "nullable": False},
        
        # Hierarchy
        {"name": "dept_id", "type": "VARCHAR(20)", "nullable": False, "description": "Department (e.g., FOODS_1)"},
        {"name": "cat_id", "type": "VARCHAR(20)", "nullable": False, "description": "Category (FOODS, HOBBIES, HOUSEHOLD)"},
        {"name": "store_id", "type": "VARCHAR(10)", "nullable": False, "description": "Store (e.g., CA_1)"},
        {"name": "state_id", "type": "VARCHAR(5)", "nullable": False, "description": "State (CA, TX, WI)"},
        
        # Metadata
        {"name": "ingested_at", "type": "TIMESTAMP", "nullable": False, "default": "NOW()"},
    ],
    "indexes": [
        {"name": "idx_m5_items_item_id", "columns": ["item_id"]},
        {"name": "idx_m5_items_store_id", "columns": ["store_id"]},
        {"name": "idx_m5_items_cat_id", "columns": ["cat_id"]},
        {"name": "idx_m5_items_dept_id", "columns": ["dept_id"]},
        {"name": "idx_m5_items_state_id", "columns": ["state_id"]},
        {"name": "idx_m5_items_hierarchy", "columns": ["state_id", "store_id", "cat_id", "dept_id"]},
    ]
}

M5_SALES_SCHEMA = {
    "table_name": "m5_sales",
    "description": "M5 daily sales data in long format (transformed from wide format)",
    "columns": [
        # Foreign keys / identifiers
        {"name": "item_store_id", "type": "VARCHAR(50)", "nullable": False, "description": "FK to m5_items.id"},
        {"name": "d", "type": "VARCHAR(10)", "nullable": False, "description": "Day identifier (FK to m5_calendar.d)"},
        
        # Denormalized for query performance (avoid joins for common queries)
        {"name": "item_id", "type": "VARCHAR(30)", "nullable": False},
        {"name": "store_id", "type": "VARCHAR(10)", "nullable": False},
        {"name": "date", "type": "DATE", "nullable": True, "description": "Actual date (can be joined from calendar)"},
        
        # Sales value
        {"name": "sales", "type": "INTEGER", "nullable": False, "description": "Unit sales for the day"},
        
        # Metadata
        {"name": "ingested_at", "type": "TIMESTAMP", "nullable": False, "default": "NOW()"},
    ],
    "primary_key": ["item_store_id", "d"],
    "indexes": [
        {"name": "idx_m5_sales_item_store", "columns": ["item_store_id"]},
        {"name": "idx_m5_sales_item_id", "columns": ["item_id"]},
        {"name": "idx_m5_sales_store_id", "columns": ["store_id"]},
        {"name": "idx_m5_sales_d", "columns": ["d"]},
        {"name": "idx_m5_sales_date", "columns": ["date"]},
        {"name": "idx_m5_sales_store_date", "columns": ["store_id", "date"]},
    ]
}


# =============================================================================
# SQL GENERATION
# =============================================================================

def generate_create_table_sql(schema: Dict[str, Any]) -> str:
    """
    Generate CREATE TABLE IF NOT EXISTS SQL from schema definition.
    
    Args:
        schema: Schema dictionary with table_name, columns, indexes, etc.
        
    Returns:
        SQL string for creating the table
    """
    table_name = schema["table_name"]
    columns = schema["columns"]
    
    # Build column definitions
    col_defs = []
    for col in columns:
        parts = [col["name"], col["type"]]
        
        if not col.get("nullable", True):
            parts.append("NOT NULL")
        
        if col.get("default"):
            parts.append(f"DEFAULT {col['default']}")
        
        col_defs.append(" ".join(parts))
    
    # Add primary key constraint
    pk_columns = [col["name"] for col in columns if col.get("pk")]
    if not pk_columns and schema.get("primary_key"):
        pk_columns = schema["primary_key"]
    
    if pk_columns:
        col_defs.append(f"PRIMARY KEY ({', '.join(pk_columns)})")
    
    # Build CREATE TABLE
    sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {','.join(f'    {c}' for c in col_defs)}
);"""
    
    # Add indexes
    indexes = schema.get("indexes", [])
    for idx in indexes:
        idx_name = idx["name"]
        idx_cols = ", ".join(idx["columns"])
        sql += f"\nCREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({idx_cols});"
    
    return sql


def get_all_create_table_sql() -> str:
    """
    Generate CREATE TABLE SQL for all M5 tables.
    
    Returns:
        Combined SQL string for all tables
    """
    schemas = [
        M5_CALENDAR_SCHEMA,
        M5_ITEMS_SCHEMA,
        M5_PRICES_SCHEMA,
        M5_SALES_SCHEMA,
    ]
    
    sql_parts = []
    for schema in schemas:
        sql_parts.append(f"-- {schema['description']}")
        sql_parts.append(generate_create_table_sql(schema))
        sql_parts.append("")  # Empty line between tables
    
    return "\n".join(sql_parts)


# =============================================================================
# DATA PARSING
# =============================================================================

def parse_calendar_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a row from calendar.csv into database format.
    
    Args:
        row: Dictionary with raw calendar data
        
    Returns:
        Dictionary ready for database insertion
    """
    return {
        "date": row["date"],
        "d": row["d"],
        "wm_yr_wk": int(row["wm_yr_wk"]),
        "weekday": row["weekday"],
        "wday": int(row["wday"]),
        "month": int(row["month"]),
        "year": int(row["year"]),
        "event_name_1": row.get("event_name_1") or None,
        "event_type_1": row.get("event_type_1") or None,
        "event_name_2": row.get("event_name_2") or None,
        "event_type_2": row.get("event_type_2") or None,
        "snap_ca": int(row["snap_CA"]),
        "snap_tx": int(row["snap_TX"]),
        "snap_wi": int(row["snap_WI"]),
    }


def parse_price_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a row from sell_prices.csv into database format.
    
    Args:
        row: Dictionary with raw price data
        
    Returns:
        Dictionary ready for database insertion
    """
    return {
        "store_id": row["store_id"],
        "item_id": row["item_id"],
        "wm_yr_wk": int(row["wm_yr_wk"]),
        "sell_price": float(row["sell_price"]),
    }


def parse_sales_row_to_items(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract item dimension data from a sales row.
    
    Args:
        row: Dictionary with raw sales data (has item_id, dept_id, etc.)
        
    Returns:
        Dictionary for m5_items table
    """
    item_id = row["item_id"]
    store_id = row["store_id"]
    
    return {
        "id": f"{item_id}_{store_id}",
        "item_id": item_id,
        "dept_id": row["dept_id"],
        "cat_id": row["cat_id"],
        "store_id": store_id,
        "state_id": row["state_id"],
    }


def parse_sales_row_to_long_format(
    row: Dict[str, Any],
    day_columns: List[str],
    calendar_lookup: Optional[Dict[str, str]] = None
) -> List[Dict[str, Any]]:
    """
    Transform a wide-format sales row to long format.
    
    The sales_train_validation.csv has columns:
    id, item_id, dept_id, cat_id, store_id, state_id, d_1, d_2, ..., d_1969
    
    We transform each d_X column into a separate row.
    
    Args:
        row: Dictionary with raw sales data (wide format)
        day_columns: List of day column names (d_1, d_2, ...)
        calendar_lookup: Optional dict mapping d -> date string
        
    Returns:
        List of dictionaries for m5_sales table (one per day)
    """
    item_id = row["item_id"]
    store_id = row["store_id"]
    item_store_id = f"{item_id}_{store_id}"
    
    sales_rows = []
    
    for d_col in day_columns:
        sales_value = row.get(d_col)
        
        # Skip if no data
        if sales_value is None or sales_value == "":
            continue
        
        # Get date from calendar lookup if available
        date_value = calendar_lookup.get(d_col) if calendar_lookup else None
        
        sales_rows.append({
            "item_store_id": item_store_id,
            "d": d_col,
            "item_id": item_id,
            "store_id": store_id,
            "date": date_value,
            "sales": int(sales_value),
        })
    
    return sales_rows


# =============================================================================
# METADATA HELPERS
# =============================================================================

def get_table_display_name(table_type: str) -> str:
    """Get human-readable display name for a table type."""
    names = {
        "sales": "M5 Forecasting - Daily Sales",
        "calendar": "M5 Forecasting - Calendar",
        "prices": "M5 Forecasting - Prices",
        "items": "M5 Forecasting - Items",
    }
    return names.get(table_type, f"M5 {table_type.title()}")


def get_table_description(table_type: str) -> str:
    """Get description for a table type."""
    descriptions = {
        "sales": "Daily unit sales data for ~30K item-store combinations across 1,969 days",
        "calendar": "Calendar dimension with dates, events, and SNAP indicators (2011-2016)",
        "prices": "Weekly prices by store and item (~6.8M price records)",
        "items": "Item dimension with hierarchical attributes (category, department, store, state)",
    }
    return descriptions.get(table_type, "M5 Forecasting dataset component")


def get_m5_summary() -> Dict[str, Any]:
    """
    Get summary information about the M5 dataset.
    
    Returns:
        Dictionary with dataset summary
    """
    return {
        "name": "M5 Forecasting",
        "source": "Kaggle",
        "competition": "m5-forecasting-accuracy",
        "description": (
            "Walmart-style retail demand forecasting dataset with hierarchical structure. "
            "Includes daily unit sales for 3,049 products across 10 stores in 3 states, "
            "along with calendar, price, and promotional information."
        ),
        "date_range": "2011-01-29 to 2016-06-19",
        "tables": {
            "m5_calendar": "1,969 rows (dates)",
            "m5_items": "~30,490 rows (item-store combinations)",
            "m5_prices": "~6.8M rows (store/item/week prices)",
            "m5_sales": "~60M rows (daily sales in long format)",
        },
        "hierarchy": {
            "states": ["CA", "TX", "WI"],
            "stores": 10,
            "categories": ["FOODS", "HOBBIES", "HOUSEHOLD"],
            "departments": 7,
            "items": "~3,049",
        },
        "use_cases": [
            "Hierarchical demand forecasting",
            "Price elasticity analysis",
            "Promotional impact modeling",
            "Inventory optimization",
            "Evaluation of forecasting models",
        ],
        "license": "Kaggle Competition Data License - check competition rules for usage terms",
    }

