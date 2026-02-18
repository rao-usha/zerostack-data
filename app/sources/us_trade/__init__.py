"""
US Trade data source module.

Provides access to US Census Bureau International Trade API for:
- Import/export data by HS code (Harmonized System)
- Trade by country/trading partner
- Trade by port of entry (customs district)
- State-level export statistics
- Trade summaries with balance calculations

API: https://api.census.gov/data/timeseries/intltrade.html

No API key required. Optional key available at census.gov/developers.
"""

from app.sources.us_trade.client import (
    USTradeClient,
    HS_CHAPTERS,
    TOP_TRADING_PARTNERS,
)
from app.sources.us_trade.metadata import (
    generate_table_name,
    generate_create_table_sql,
    parse_exports_hs_response,
    parse_imports_hs_response,
    parse_exports_state_response,
    parse_port_trade_response,
    parse_trade_summary,
    get_dataset_display_name,
    get_dataset_description,
    get_default_date_range,
    get_hs_chapter,
    get_hs_heading,
    get_hs_subheading,
    MAJOR_COMMODITY_CHAPTERS,
)
from app.sources.us_trade.ingest import (
    prepare_table_for_trade_data,
    ingest_exports_by_hs,
    ingest_imports_by_hs,
    ingest_exports_by_state,
    ingest_port_trade,
    ingest_trade_summary,
)

__all__ = [
    # Client
    "USTradeClient",
    # Reference data
    "HS_CHAPTERS",
    "TOP_TRADING_PARTNERS",
    "MAJOR_COMMODITY_CHAPTERS",
    # Metadata functions
    "generate_table_name",
    "generate_create_table_sql",
    "parse_exports_hs_response",
    "parse_imports_hs_response",
    "parse_exports_state_response",
    "parse_port_trade_response",
    "parse_trade_summary",
    "get_dataset_display_name",
    "get_dataset_description",
    "get_default_date_range",
    "get_hs_chapter",
    "get_hs_heading",
    "get_hs_subheading",
    # Ingestion functions
    "prepare_table_for_trade_data",
    "ingest_exports_by_hs",
    "ingest_imports_by_hs",
    "ingest_exports_by_state",
    "ingest_port_trade",
    "ingest_trade_summary",
]
