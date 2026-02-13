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
from app.sources.pe_collection.portfolio_collectors.sec_13f_collector import SEC13FCollector
from app.sources.pe_collection.financial_collectors.public_comps_collector import PublicCompsCollector
from app.sources.pe_collection.deal_collectors.press_release_collector import PressReleaseCollector
from app.sources.pe_collection.people_collectors.bio_extractor import BioExtractor
from app.sources.pe_collection.news_collectors.news_collector import PENewsCollector
from app.sources.pe_collection.financial_collectors.valuation_estimator import ValuationEstimator

# Register all collectors with the orchestrator
PECollectionOrchestrator.register_collector(PECollectionSource.SEC_ADV, SECADVCollector)
PECollectionOrchestrator.register_collector(PECollectionSource.FIRM_WEBSITE, FirmWebsiteCollector)
PECollectionOrchestrator.register_collector(PECollectionSource.SEC_FORM_D, SECFormDCollector)
PECollectionOrchestrator.register_collector(PECollectionSource.SEC_13D, SEC13FCollector)
PECollectionOrchestrator.register_collector(PECollectionSource.PUBLIC_COMPS, PublicCompsCollector)
PECollectionOrchestrator.register_collector(PECollectionSource.PRESS_RELEASE, PressReleaseCollector)
PECollectionOrchestrator.register_collector(PECollectionSource.BIO_EXTRACTOR, BioExtractor)
PECollectionOrchestrator.register_collector(PECollectionSource.NEWS_API, PENewsCollector)
PECollectionOrchestrator.register_collector(PECollectionSource.VALUATION_ESTIMATOR, ValuationEstimator)

__all__ = [
    "PECollectionSource",
    "PECollectionConfig",
    "PECollectedItem",
    "PECollectionResult",
    "BasePECollector",
    "PECollectionOrchestrator",
]
