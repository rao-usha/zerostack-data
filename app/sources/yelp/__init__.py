"""
Yelp Fusion API data source adapter.

Yelp Fusion API provides access to business listings and reviews data:
- Business search by location, category, keyword
- Business details and reviews
- Categories and attributes

Official API: https://docs.developer.yelp.com/docs/fusion-intro
API Key: Required (free tier: 500 calls/day)
"""

from app.sources.yelp.client import YelpClient
from app.sources.yelp.ingest import (
    ingest_businesses_by_location,
    ingest_business_categories,
    prepare_table_for_yelp_data,
)

__all__ = [
    "YelpClient",
    "ingest_businesses_by_location",
    "ingest_business_categories",
    "prepare_table_for_yelp_data",
]
