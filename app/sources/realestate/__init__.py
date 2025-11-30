"""
Real Estate / Housing data source module.

Provides ingestion for:
- FHFA House Price Index (Federal Housing Finance Agency)
- HUD Permits & Starts (U.S. Department of Housing and Urban Development)
- Redfin Data Dump (public housing market data)
- OpenStreetMap Building Footprints (via Overpass API)

All sources use official APIs or bulk download endpoints. No web scraping.
"""

__all__ = ["client", "ingest", "metadata"]

