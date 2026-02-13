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
from app.sources.pe_collection.orchestrator import PECollectionOrchestrator

# Import collectors to trigger registration
from app.sources.pe_collection.firm_collectors.sec_adv_collector import SECADVCollector
from app.sources.pe_collection.firm_collectors.firm_website_collector import FirmWebsiteCollector
from app.sources.pe_collection.deal_collectors.sec_formd_collector import SECFormDCollector

# Register all collectors with the orchestrator
PECollectionOrchestrator.register_collector(PECollectionSource.SEC_ADV, SECADVCollector)
PECollectionOrchestrator.register_collector(PECollectionSource.FIRM_WEBSITE, FirmWebsiteCollector)
PECollectionOrchestrator.register_collector(PECollectionSource.SEC_FORM_D, SECFormDCollector)

__all__ = [
    "PECollectionSource",
    "PECollectionConfig",
    "PECollectedItem",
    "PECollectionResult",
    "BasePECollector",
    "PECollectionOrchestrator",
]
