"""
International Economic Data source adapter.

Provides access to major international economic data sources:
- World Bank Open Data (WDI, Global Economic Monitor)
- International Monetary Fund (IMF) Data
- OECD Data
- Bank for International Settlements (BIS)

All sources are free, no API key required.
"""
from app.sources.international_econ.client import (
    WorldBankClient,
    IMFClient,
    OECDClient,
    BISClient
)
from app.sources.international_econ.ingest import (
    ingest_worldbank_wdi,
    ingest_worldbank_indicators,
    ingest_imf_weo,
    ingest_imf_ifs
)

__all__ = [
    "WorldBankClient",
    "IMFClient", 
    "OECDClient",
    "BISClient",
    "ingest_worldbank_wdi",
    "ingest_worldbank_indicators",
    "ingest_imf_weo",
    "ingest_imf_ifs"
]
