"""
FERC Energy Filings source module.

Provides access to state-level electricity profile data via the EIA API:
- Total consumption (MWh)
- Total generation (MWh)
- Average retail electricity prices
- Revenue and utility counts

Requires EIA_API_KEY environment variable.
"""

__all__ = ["client", "ingest", "metadata"]
