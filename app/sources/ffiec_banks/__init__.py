"""
FFIEC Bank Call Reports source module.

Provides access to bank financial data via the FDIC BankFind Suite API:
- Bank financial summaries (assets, deposits, loans, equity, income)
- Quarterly call report data
- Institution-level detail by CERT ID

No API key required. Data sourced from FDIC BankFind Suite.
"""

__all__ = ["client", "ingest", "metadata"]
