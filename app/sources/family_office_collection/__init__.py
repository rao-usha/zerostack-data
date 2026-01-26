"""
Family Office Collection System - Continuous data collection for family offices.

This module provides:
- Multi-source data collection (website, SEC filings, news)
- Rate-limited, incremental collection
- Scheduling and orchestration
- Data normalization and deduplication
"""

from app.sources.family_office_collection.types import (
    FoCollectionConfig,
    FoCollectionResult,
    FoCollectedItem,
    FoCollectionSource,
    FoRegistryEntry,
)
from app.sources.family_office_collection.runner import FoCollectionOrchestrator
from app.sources.family_office_collection.base_collector import FoBaseCollector
from app.sources.family_office_collection.website_source import FoWebsiteCollector
from app.sources.family_office_collection.news_source import FoNewsCollector
from app.sources.family_office_collection.normalizer import FoDataNormalizer
from app.sources.family_office_collection.config import (
    get_fo_registry,
    get_fo_by_name,
    get_registry_stats,
)

__all__ = [
    "FoCollectionConfig",
    "FoCollectionResult",
    "FoCollectedItem",
    "FoCollectionSource",
    "FoRegistryEntry",
    "FoCollectionOrchestrator",
    "FoBaseCollector",
    "FoWebsiteCollector",
    "FoNewsCollector",
    "FoDataNormalizer",
    "get_fo_registry",
    "get_fo_by_name",
    "get_registry_stats",
]
