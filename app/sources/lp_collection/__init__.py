"""
LP Collection System - Continuous data collection for institutional investors.

This module provides:
- Multi-source data collection (website, SEC ADV, SEC 13F, CAFR, news)
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
from app.sources.lp_collection.sec_13f_source import Sec13fCollector
from app.sources.lp_collection.form_990_source import Form990Collector
from app.sources.lp_collection.cafr_parser import CafrParser

__all__ = [
    "CollectionConfig",
    "CollectionResult",
    "CollectedItem",
    "LpCollectionSource",
    "LpCollectionOrchestrator",
    "BaseCollector",
    "Sec13fCollector",
    "Form990Collector",
    "CafrParser",
]
