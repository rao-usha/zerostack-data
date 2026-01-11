"""
OpenFEMA data source adapter.

Provides access to FEMA disaster and emergency management data:
- Disaster Declarations - All federally declared disasters since 1953
- Public Assistance Grants - PA funded projects and grants
- Hazard Mitigation Projects - HMA mitigation projects
- NFIP Flood Insurance - Policies and claims data
- Individual Assistance - Housing assistance data

API Documentation: https://www.fema.gov/about/openfema/api
Data Sets: https://www.fema.gov/about/openfema/data-sets

No API key required - free public API.
Rate Limits: 1000 requests per minute (be respectful)
"""

from app.sources.fema.client import FEMAClient
from app.sources.fema import metadata
from app.sources.fema import ingest

__all__ = ["FEMAClient", "metadata", "ingest"]
