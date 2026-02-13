"""
Portfolio company collectors.

Collectors for gathering data about PE-backed portfolio companies:
- SEC 13F/13D for institutional holdings and large ownership filings
- PE firm website portfolio sections
- Crunchbase API for company data
"""

from app.sources.pe_collection.portfolio_collectors.sec_13f_collector import SEC13FCollector

__all__ = [
    "SEC13FCollector",
]
