"""
openFDA Device Registration adapter.

Provides access to the openFDA Device Registration and Listing API
for querying medical device manufacturers, establishments, and products.

Official API: https://open.fda.gov/apis/device/registrationlisting/
API Key: Optional (1,000 req/day without, 120,000/day with free key)

Datasets:
- Device Registrations: Manufacturer/establishment registrations and product listings
"""

from app.sources.fda.client import OpenFDAClient
from app.sources.fda.ingest import ingest_device_registrations
from app.sources.fda.metadata import (
    TABLE_NAME,
    COLUMNS,
    CONFLICT_COLUMNS,
    UPDATE_COLUMNS,
    generate_create_table_sql,
    parse_registration_record,
    AESTHETIC_PRODUCT_CODES,
)

__all__ = [
    "OpenFDAClient",
    "ingest_device_registrations",
    "TABLE_NAME",
    "COLUMNS",
    "CONFLICT_COLUMNS",
    "UPDATE_COLUMNS",
    "generate_create_table_sql",
    "parse_registration_record",
    "AESTHETIC_PRODUCT_CODES",
]
