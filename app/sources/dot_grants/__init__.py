"""
DOT Infrastructure Grants source module (via USAspending).

Provides access to Department of Transportation grant spending data
aggregated by state, sourced from the USAspending.gov API.

Includes aggregated grant amounts, transaction counts, population,
and per-capita spending by state and fiscal year.

All data is publicly available via the USAspending API.
No API key required.
"""

__all__ = ["client", "ingest", "metadata"]
