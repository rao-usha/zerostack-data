"""
CFTC COT metadata and schema definitions.

Defines table schemas and parsing functions for COT data.
"""

from typing import Dict, List, Any, Optional


# Database schema for COT tables
COT_LEGACY_COLUMNS = {
    "id": "SERIAL PRIMARY KEY",
    "report_type": "VARCHAR(50) NOT NULL",
    "report_date": "DATE NOT NULL",
    "year": "INTEGER NOT NULL",
    "market_name": "VARCHAR(200)",
    "cftc_contract_code": "VARCHAR(20)",
    "cftc_market_code": "VARCHAR(20)",
    "cftc_commodity_code": "VARCHAR(20)",
    # Open interest
    "open_interest": "BIGINT",
    # Non-commercial (speculators)
    "noncomm_long": "BIGINT",
    "noncomm_short": "BIGINT",
    "noncomm_spread": "BIGINT",
    # Commercial (hedgers)
    "comm_long": "BIGINT",
    "comm_short": "BIGINT",
    # Total reportable
    "total_reportable_long": "BIGINT",
    "total_reportable_short": "BIGINT",
    # Non-reportable (small traders)
    "nonreportable_long": "BIGINT",
    "nonreportable_short": "BIGINT",
    # Weekly changes
    "change_open_interest": "BIGINT",
    "change_noncomm_long": "BIGINT",
    "change_noncomm_short": "BIGINT",
    # Concentration ratios
    "conc_gross_4_long": "NUMERIC(5,2)",
    "conc_gross_4_short": "NUMERIC(5,2)",
    "conc_gross_8_long": "NUMERIC(5,2)",
    "conc_gross_8_short": "NUMERIC(5,2)",
    # Number of traders
    "traders_total": "INTEGER",
    "traders_noncomm_long": "INTEGER",
    "traders_noncomm_short": "INTEGER",
    "traders_comm_long": "INTEGER",
    "traders_comm_short": "INTEGER",
    # Contract info
    "contract_units": "VARCHAR(100)",
    # Calculated fields
    "noncomm_net": "BIGINT",  # long - short
    "comm_net": "BIGINT",  # long - short
    # Metadata
    "ingested_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}

COT_DISAGGREGATED_COLUMNS = {
    **COT_LEGACY_COLUMNS,
    # Producer/Merchant/Processor/User
    "prod_merc_long": "BIGINT",
    "prod_merc_short": "BIGINT",
    # Swap Dealers
    "swap_long": "BIGINT",
    "swap_short": "BIGINT",
    # Managed Money (hedge funds, CTAs)
    "managed_money_long": "BIGINT",
    "managed_money_short": "BIGINT",
    # Other Reportables
    "other_reportable_long": "BIGINT",
    "other_reportable_short": "BIGINT",
}

COT_TFF_COLUMNS = {
    **COT_LEGACY_COLUMNS,
    # Dealer/Intermediary
    "dealer_long": "BIGINT",
    "dealer_short": "BIGINT",
    # Asset Manager/Institutional
    "asset_mgr_long": "BIGINT",
    "asset_mgr_short": "BIGINT",
    # Leveraged Funds
    "lev_money_long": "BIGINT",
    "lev_money_short": "BIGINT",
}


def generate_table_name(report_type: str) -> str:
    """
    Generate table name for COT data.

    Args:
        report_type: Type of COT report

    Returns:
        Table name string
    """
    return f"cftc_cot_{report_type}"


def generate_create_table_sql(report_type: str) -> str:
    """
    Generate CREATE TABLE SQL for COT data.

    Args:
        report_type: Type of COT report

    Returns:
        SQL CREATE TABLE statement
    """
    table_name = generate_table_name(report_type)

    # Select appropriate columns based on report type
    if "disaggregated" in report_type:
        columns = COT_DISAGGREGATED_COLUMNS
    elif "tff" in report_type:
        columns = COT_TFF_COLUMNS
    else:
        columns = COT_LEGACY_COLUMNS

    column_defs = ",\n    ".join([f"{col} {dtype}" for col, dtype in columns.items()])

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {column_defs}
    );
    
    -- Create indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(report_date);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_market ON {table_name}(market_name);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name}(year);
    
    -- Unique constraint to prevent duplicates
    CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_unique 
    ON {table_name}(report_date, market_name, report_type);
    """

    return sql


def parse_cot_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and enhance a COT record.

    Args:
        record: Raw COT record from client

    Returns:
        Enhanced record with calculated fields
    """
    # Calculate net positions
    noncomm_long = record.get("noncomm_long") or 0
    noncomm_short = record.get("noncomm_short") or 0
    comm_long = record.get("comm_long") or 0
    comm_short = record.get("comm_short") or 0

    record["noncomm_net"] = noncomm_long - noncomm_short
    record["comm_net"] = comm_long - comm_short

    return record


# Major commodity groupings for analysis
COMMODITY_GROUPS = {
    "energy": [
        "CRUDE OIL",
        "NATURAL GAS",
        "BRENT CRUDE",
        "HEATING OIL",
        "RBOB GASOLINE",
    ],
    "metals": [
        "GOLD",
        "SILVER",
        "COPPER",
        "PLATINUM",
        "PALLADIUM",
    ],
    "grains": [
        "CORN",
        "SOYBEANS",
        "WHEAT",
        "OATS",
        "SOYBEAN OIL",
        "SOYBEAN MEAL",
    ],
    "softs": [
        "COFFEE",
        "SUGAR",
        "COCOA",
        "COTTON",
        "ORANGE JUICE",
    ],
    "livestock": [
        "LIVE CATTLE",
        "LEAN HOGS",
        "FEEDER CATTLE",
    ],
    "financials": [
        "S&P 500",
        "NASDAQ",
        "DOW",
        "VIX",
        "RUSSELL",
    ],
    "currencies": [
        "EURO FX",
        "JAPANESE YEN",
        "BRITISH POUND",
        "CANADIAN DOLLAR",
        "AUSTRALIAN DOLLAR",
        "SWISS FRANC",
        "MEXICAN PESO",
    ],
    "rates": [
        "TREASURY",
        "FEDERAL FUNDS",
        "EURODOLLAR",
        "SOFR",
    ],
}


def categorize_contract(market_name: str) -> Optional[str]:
    """
    Categorize a contract by commodity group.

    Args:
        market_name: Full market name from COT data

    Returns:
        Commodity group name or None
    """
    if not market_name:
        return None

    market_upper = market_name.upper()

    for group, keywords in COMMODITY_GROUPS.items():
        for keyword in keywords:
            if keyword in market_upper:
                return group

    return "other"
