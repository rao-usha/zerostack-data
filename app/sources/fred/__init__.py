"""
FRED (Federal Reserve Economic Data) source module.

Provides access to Federal Reserve economic data including:
- Core Time Series
- H.15 Interest Rates
- Monetary Aggregates (M1, M2)
- Industrial Production

All data is public domain and does not require an API key.
"""

__all__ = ["client", "ingest", "metadata"]

