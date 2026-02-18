"""
Kaggle data source adapter.

This module provides functionality for downloading and ingesting datasets
from Kaggle, following the project's plugin pattern.

Currently supported datasets:
- M5 Forecasting (Walmart-style retail demand)

Kaggle API Documentation:
https://github.com/Kaggle/kaggle-api

IMPORTANT: Requires Kaggle API credentials configured via:
- Environment variables: KAGGLE_USERNAME, KAGGLE_KEY
- Or ~/.kaggle/kaggle.json file
- Get credentials at: https://www.kaggle.com/account (API section)

Competition/Dataset terms apply - check license before commercial use.
"""

from app.sources.kaggle.client import KaggleClient
from app.sources.kaggle.ingest import (
    ingest_m5_dataset,
    prepare_m5_tables,
)

__all__ = [
    "KaggleClient",
    "ingest_m5_dataset",
    "prepare_m5_tables",
]
