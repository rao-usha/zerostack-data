"""
PE Portfolio Intelligence Collection System.

This module provides comprehensive data collection for PE/VC intelligence:

Collector Categories:
- firm_collectors/: PE firm data (Form ADV, websites, LinkedIn)
- portfolio_collectors/: Portfolio company data (13D, websites, Crunchbase)
- people_collectors/: Executive and investor profiles (LinkedIn, bios)
- deal_collectors/: M&A transactions (Form D, press releases)
- financial_collectors/: Company financials and valuations
- news_collectors/: News and sentiment analysis
"""

from app.sources.pe_collection.types import (
    PECollectionSource,
    PECollectionConfig,
    PECollectedItem,
    PECollectionResult,
)
from app.sources.pe_collection.base_collector import BasePECollector

__all__ = [
    "PECollectionSource",
    "PECollectionConfig",
    "PECollectedItem",
    "PECollectionResult",
    "BasePECollector",
]
