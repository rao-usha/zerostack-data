"""
Site Intelligence Platform - Industrial & Data Center Site Selection.

This module provides comprehensive data collection and analysis for:
- Power Infrastructure (EIA, NREL, HIFLD)
- Telecom/Fiber Infrastructure (FCC, PeeringDB)
- Transportation Infrastructure (BTS, FRA, USACE)
- Labor Market (BLS, Census)
- Risk & Environmental (FEMA, USGS, NOAA, EPA)
- Incentives & Real Estate (CDFI, FTZ Board, EDOs)
- Freight & Logistics (Freightos, USDA AMS)
- Site Scoring Engine

Usage:
    from app.sources.site_intel import SiteIntelOrchestrator

    orchestrator = SiteIntelOrchestrator(db)
    result = await orchestrator.collect(domain='power', source='eia')
"""

from app.sources.site_intel.types import (
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
)
from app.sources.site_intel.base_collector import BaseCollector

__all__ = [
    'SiteIntelDomain',
    'SiteIntelSource',
    'CollectionConfig',
    'CollectionResult',
    'BaseCollector',
]
