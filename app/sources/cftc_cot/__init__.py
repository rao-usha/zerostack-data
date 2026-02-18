"""
CFTC Commitments of Traders (COT) data source.

Provides weekly positioning data for futures markets:
- Legacy reports (commercial vs non-commercial)
- Disaggregated reports (producer, swap dealer, managed money)
- Traders in Financial Futures (TFF) for financial contracts

No API key required - public data.
"""

from .client import CFTCCOTClient, MAJOR_CONTRACTS
from .metadata import (
    generate_table_name,
    generate_create_table_sql,
    parse_cot_record,
    COMMODITY_GROUPS,
    categorize_contract,
)
from .ingest import (
    ingest_cot_legacy,
    ingest_cot_disaggregated,
    ingest_cot_tff,
    ingest_cot_all_reports,
    prepare_cot_table,
)

__all__ = [
    # Client
    "CFTCCOTClient",
    "MAJOR_CONTRACTS",
    # Metadata
    "generate_table_name",
    "generate_create_table_sql",
    "parse_cot_record",
    "COMMODITY_GROUPS",
    "categorize_contract",
    # Ingestion
    "ingest_cot_legacy",
    "ingest_cot_disaggregated",
    "ingest_cot_tff",
    "ingest_cot_all_reports",
    "prepare_cot_table",
]
