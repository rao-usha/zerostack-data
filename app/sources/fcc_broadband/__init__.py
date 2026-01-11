"""
FCC Broadband & Telecom data source adapter.

Provides access to:
- FCC National Broadband Map data (coverage by geography)
- Provider availability and technology types
- Speed tier classifications
- ISP market data

Data sources:
- FCC Broadband Map API: https://broadbandmap.fcc.gov/api/public
- FCC Open Data: https://opendata.fcc.gov

No API key required for public datasets.

License: Public domain (U.S. government data)
"""

from app.sources.fcc_broadband.client import FCCBroadbandClient
from app.sources.fcc_broadband import metadata
from app.sources.fcc_broadband import ingest

__all__ = ["FCCBroadbandClient", "metadata", "ingest"]
