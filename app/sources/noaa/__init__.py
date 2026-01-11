"""
NOAA (National Oceanic and Atmospheric Administration) data source adapter.

This module provides access to NOAA climate and weather data including:
- Daily/Hourly Weather Observations via NCEI CDO API
- Climate Normals via NCEI CDO API
- Storm Events Database
- NEXRAD Indexes (AWS Open Data)

Official APIs:
- NCEI Climate Data Online (CDO) API: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
- Storm Events Database: https://www.ncdc.noaa.gov/stormevents/
- NEXRAD on AWS: https://registry.opendata.aws/noaa-nexrad/

Data License: Public Domain (U.S. Government Work)
Rate Limits: 5 requests per second, 10,000 requests per day for CDO API
"""

from app.sources.noaa.client import NOAAClient
from app.sources.noaa.metadata import NOAA_DATASETS
from app.sources.noaa.ingest import ingest_noaa_data

__all__ = ["NOAAClient", "NOAA_DATASETS", "ingest_noaa_data"]














