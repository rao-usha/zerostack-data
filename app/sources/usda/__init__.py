"""
USDA NASS QuickStats data source.

Provides access to agricultural statistics:
- Crop production (corn, soybeans, wheat, etc.)
- Crop yields and area planted/harvested
- Prices received by farmers
- Livestock inventory
- Farm economics

API Key required - register free at: https://quickstats.nass.usda.gov/api
Set USDA_API_KEY environment variable.
"""

from .client import USDAClient, MAJOR_CROP_STATES, DATA_UNITS
from .metadata import (
    generate_table_name,
    generate_create_table_sql,
    parse_usda_record,
    COMMODITY_CATEGORIES,
    STATE_FIPS,
)
from .ingest import (
    ingest_crop_production,
    ingest_crop_all_stats,
    ingest_livestock_inventory,
    ingest_annual_crops,
    ingest_all_major_crops,
    prepare_usda_table,
)

__all__ = [
    # Client
    "USDAClient",
    "MAJOR_CROP_STATES",
    "DATA_UNITS",
    # Metadata
    "generate_table_name",
    "generate_create_table_sql",
    "parse_usda_record",
    "COMMODITY_CATEGORIES",
    "STATE_FIPS",
    # Ingestion
    "ingest_crop_production",
    "ingest_crop_all_stats",
    "ingest_livestock_inventory",
    "ingest_annual_crops",
    "ingest_all_major_crops",
    "prepare_usda_table",
]
