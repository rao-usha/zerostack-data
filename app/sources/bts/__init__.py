"""
Bureau of Transportation Statistics (BTS) data source adapter.

Provides access to:
- Border Crossing Entry Data (via Socrata API)
- Freight Analysis Framework (FAF5) regional data (via CSV download)
- Vehicle Miles Traveled (VMT) monthly data (via Socrata API)

Data sources:
- Socrata API: https://data.transportation.gov
- BTS Downloads: https://www.bts.gov

No API key required for public datasets (optional app token for higher rate limits).
"""

from app.sources.bts.client import BTSClient
from app.sources.bts import metadata
from app.sources.bts import ingest

__all__ = ["BTSClient", "metadata", "ingest"]
