"""
BLS (Bureau of Labor Statistics) source module.

Provides access to Bureau of Labor Statistics data including:
- CES (Current Employment Statistics) - Employment, hours, earnings by industry
- CPS (Current Population Survey) - Labor force status, unemployment
- JOLTS (Job Openings and Labor Turnover Survey) - Job openings, hires, quits
- CPI (Consumer Price Index) - Inflation measures
- PPI (Producer Price Index) - Wholesale/producer prices
- OES (Occupational Employment Statistics) - Employment and wages by occupation

API Key: Optional but recommended for higher rate limits
- Without key: 25 queries/day, 10 years per query, 25 series per query
- With key: 500 queries/day, 20 years per query, 50 series per query

Get a free API key at: https://data.bls.gov/registrationEngine/
"""

from app.sources.bls.client import (
    BLSClient,
    COMMON_SERIES,
    CPS_SERIES,
    CES_SERIES,
    JOLTS_SERIES,
    CPI_SERIES,
    PPI_SERIES,
    OES_SERIES,
    get_series_for_dataset,
    get_series_info,
)

from app.sources.bls.metadata import (
    generate_table_name,
    generate_create_table_sql,
    parse_bls_observation,
    parse_bls_series_response,
    build_insert_values,
    get_dataset_display_name,
    get_dataset_description,
    get_default_date_range,
    validate_year_range,
    get_series_reference,
    DATASET_TABLES,
    ALL_SERIES_REFERENCE,
)

from app.sources.bls.ingest import (
    prepare_table_for_dataset,
    ingest_bls_series,
    ingest_bls_dataset,
    ingest_all_bls_datasets,
    ingest_unemployment_data,
    ingest_employment_data,
    ingest_cpi_data,
    ingest_ppi_data,
    ingest_jolts_data,
)

__all__ = [
    # Client
    "BLSClient",
    "COMMON_SERIES",
    "CPS_SERIES",
    "CES_SERIES",
    "JOLTS_SERIES",
    "CPI_SERIES",
    "PPI_SERIES",
    "OES_SERIES",
    "get_series_for_dataset",
    "get_series_info",
    # Metadata
    "generate_table_name",
    "generate_create_table_sql",
    "parse_bls_observation",
    "parse_bls_series_response",
    "build_insert_values",
    "get_dataset_display_name",
    "get_dataset_description",
    "get_default_date_range",
    "validate_year_range",
    "get_series_reference",
    "DATASET_TABLES",
    "ALL_SERIES_REFERENCE",
    # Ingest
    "prepare_table_for_dataset",
    "ingest_bls_series",
    "ingest_bls_dataset",
    "ingest_all_bls_datasets",
    "ingest_unemployment_data",
    "ingest_employment_data",
    "ingest_cpi_data",
    "ingest_ppi_data",
    "ingest_jolts_data",
]
