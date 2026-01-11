"""
IRS Statistics of Income (SOI) data source.

Provides access to IRS SOI tax statistics including:
- Individual Income by ZIP Code
- Individual Income by County
- Migration Data (county-to-county flows)
- Business Income by ZIP

Data source: https://www.irs.gov/statistics/soi-tax-stats
API Key: NOT REQUIRED (bulk CSV/Excel downloads)
License: Public domain (US government data)
"""
from app.sources.irs_soi.client import IRSSOIClient
from app.sources.irs_soi.metadata import (
    AGI_BRACKETS,
    ZIP_INCOME_COLUMNS,
    COUNTY_INCOME_COLUMNS,
    MIGRATION_COLUMNS,
    BUSINESS_INCOME_COLUMNS,
    generate_table_name,
    generate_create_table_sql,
)
from app.sources.irs_soi.ingest import (
    ingest_zip_income_data,
    ingest_county_income_data,
    ingest_migration_data,
    ingest_business_income_data,
    ingest_all_soi_data,
)

__all__ = [
    "IRSSOIClient",
    "AGI_BRACKETS",
    "ZIP_INCOME_COLUMNS",
    "COUNTY_INCOME_COLUMNS",
    "MIGRATION_COLUMNS",
    "BUSINESS_INCOME_COLUMNS",
    "generate_table_name",
    "generate_create_table_sql",
    "ingest_zip_income_data",
    "ingest_county_income_data",
    "ingest_migration_data",
    "ingest_business_income_data",
    "ingest_all_soi_data",
]
