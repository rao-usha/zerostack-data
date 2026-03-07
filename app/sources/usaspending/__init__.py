"""
USAspending.gov source module.

Provides access to federal award spending data including:
- Contract awards
- Grant awards
- Direct payments
- Loans
- Other financial assistance

Award data is searchable by NAICS code, location, time period, and award type.

API Documentation: https://api.usaspending.gov/
API Key: NOT REQUIRED (public API)

All data is public domain from the U.S. Department of the Treasury.
"""

__all__ = ["client", "ingest", "metadata"]
