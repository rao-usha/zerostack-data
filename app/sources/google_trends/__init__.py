"""
Google Trends source module.

Provides access to Google Trends interest data:
- Daily trending searches
- Interest by region for keywords
- Related queries

Note: Google Trends heavily rate-limits automated access.
The pytrends library can be used as an alternative for more reliable access.
No API key required, but requests must be very slow (1 per 5 seconds).
"""

__all__ = ["client", "ingest", "metadata"]
