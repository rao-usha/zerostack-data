"""
FBI Crime Data Explorer source adapter.

Provides access to FBI Uniform Crime Reports (UCR) and
National Incident-Based Reporting System (NIBRS) data.

API Documentation: https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi
API Key: Free from https://api.data.gov/signup/
"""
from app.sources.fbi_crime.client import FBICrimeClient
from app.sources.fbi_crime.ingest import (
    ingest_fbi_crime_estimates,
    ingest_fbi_crime_summarized,
    ingest_fbi_crime_nibrs,
    ingest_fbi_hate_crime,
    ingest_fbi_leoka,
    ingest_all_fbi_crime_data,
)

__all__ = [
    "FBICrimeClient",
    "ingest_fbi_crime_estimates",
    "ingest_fbi_crime_summarized",
    "ingest_fbi_crime_nibrs",
    "ingest_fbi_hate_crime",
    "ingest_fbi_leoka",
    "ingest_all_fbi_crime_data",
]
