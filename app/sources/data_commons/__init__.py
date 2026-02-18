"""
Google Data Commons data source adapter.

Data Commons provides access to 200+ sources of public data organized into a knowledge graph,
covering demographics, economics, education, environment, energy, health, and housing.

Official API: https://docs.datacommons.org/api/rest/v2
API Key: Optional (higher rate limits with API key)
"""

from app.sources.data_commons.client import DataCommonsClient
from app.sources.data_commons.ingest import (
    ingest_statistical_variable,
    ingest_place_statistics,
    prepare_table_for_data_commons,
)

__all__ = [
    "DataCommonsClient",
    "ingest_statistical_variable",
    "ingest_place_statistics",
    "prepare_table_for_data_commons",
]
