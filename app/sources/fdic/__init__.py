"""
FDIC BankFind Suite adapter.

Provides access to FDIC BankFind API for bank financials, demographics,
failed banks, and summary of deposits data.

Official API: https://banks.data.fdic.gov/docs/
API Key: NOT REQUIRED

Datasets:
- /api/financials - Bank balance sheets, income statements, 1,100+ metrics
- /api/institutions - Bank demographics, locations, FDIC cert info
- /api/failures - Failed banks list (crisis indicator)
- /api/sod - Summary of deposits (branch-level deposit data)
"""
from app.sources.fdic.client import FDICClient
from app.sources.fdic.ingest import (
    ingest_bank_financials,
    ingest_institutions,
    ingest_failed_banks,
    ingest_summary_of_deposits,
    ingest_all_fdic_datasets,
)
from app.sources.fdic.metadata import (
    FINANCIAL_METRICS,
    COMMON_FINANCIAL_METRICS,
    generate_table_name,
    generate_financials_table_sql,
    generate_institutions_table_sql,
    generate_failed_banks_table_sql,
    generate_deposits_table_sql,
)

__all__ = [
    "FDICClient",
    "ingest_bank_financials",
    "ingest_institutions",
    "ingest_failed_banks",
    "ingest_summary_of_deposits",
    "ingest_all_fdic_datasets",
    "FINANCIAL_METRICS",
    "COMMON_FINANCIAL_METRICS",
    "generate_table_name",
    "generate_financials_table_sql",
    "generate_institutions_table_sql",
    "generate_failed_banks_table_sql",
    "generate_deposits_table_sql",
]
