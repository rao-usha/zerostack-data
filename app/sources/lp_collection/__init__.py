"""
LP Collection System - Continuous data collection for institutional investors.

This module provides:
- Multi-source data collection (website, SEC ADV, CAFR, news)
- Rate-limited, incremental collection
- Scheduling and orchestration
- Data normalization and deduplication
"""

from app.sources.lp_collection.types import (
    CollectionConfig,
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)
from app.sources.lp_collection.runner import LpCollectionOrchestrator
from app.sources.lp_collection.base_collector import BaseCollector

__all__ = [
    "CollectionConfig",
    "CollectionResult",
    "CollectedItem",
    "LpCollectionSource",
    "LpCollectionOrchestrator",
    "BaseCollector",
]
