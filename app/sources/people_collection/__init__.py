"""
People Collection Module - Agentic data collection for leadership intelligence.

This module provides automated collection of corporate leadership data from:
- Company websites (leadership/team pages)
- SEC filings (DEF 14A, 8-K, 10-K)
- Press releases and news
- LinkedIn (Google-indexed profiles only)
- Corporate structure discovery (Exhibit 21, website, LLM)
- Recursive subsidiary collection

Key Components:
- types.py: Pydantic models for extraction results
- config.py: Rate limits, LLM prompts, settings
- base_collector.py: HTTP client with rate limiting and retries
- llm_extractor.py: LLM-based data extraction (Claude/GPT)
- orchestrator.py: Coordinates collection agents
- page_finder.py: Leadership page discovery
- html_cleaner.py: HTML preprocessing for LLM
- website_agent.py: Leadership page extraction
- sec_agent.py: SEC filing parsing
- news_agent.py: Press release monitoring
- structure_discovery.py: Corporate structure discovery (Exhibit 21 + web + LLM)
- linkedin_discovery.py: LinkedIn people discovery via Google search
- functional_org_mapper.py: Function-specific org chart mapping
- recursive_collector.py: Top-level recursive collection orchestrator

Usage:
    from app.sources.people_collection import PeopleCollectionOrchestrator

    orchestrator = PeopleCollectionOrchestrator()
    result = await orchestrator.collect_company(company_id=1, sources=["website", "sec"])

    # Recursive collection (Fortune 500):
    from app.sources.people_collection import RecursiveCollector
    collector = RecursiveCollector(db_session=session)
    result = await collector.collect(company_id=1)
"""

from app.sources.people_collection.types import (
    ExtractedPerson,
    LeadershipChange,
    CollectionResult,
    BatchCollectionResult,
    ExtractionConfidence,
    ChangeType,
    TitleLevel,
)
from app.sources.people_collection.orchestrator import PeopleCollectionOrchestrator
from app.sources.people_collection.website_agent import WebsiteAgent
from app.sources.people_collection.page_finder import PageFinder
from app.sources.people_collection.html_cleaner import HTMLCleaner
from app.sources.people_collection.sec_agent import SECAgent
from app.sources.people_collection.filing_fetcher import FilingFetcher
from app.sources.people_collection.news_agent import NewsAgent
from app.sources.people_collection.change_detector import ChangeDetector
from app.sources.people_collection.recursive_collector import RecursiveCollector
from app.sources.people_collection.structure_discovery import StructureDiscoveryAgent
from app.sources.people_collection.linkedin_discovery import LinkedInDiscovery
from app.sources.people_collection.functional_org_mapper import FunctionalOrgMapper

__all__ = [
    # Types
    "ExtractedPerson",
    "LeadershipChange",
    "CollectionResult",
    "BatchCollectionResult",
    "ExtractionConfidence",
    "ChangeType",
    "TitleLevel",
    # Agents
    "PeopleCollectionOrchestrator",
    "RecursiveCollector",
    "StructureDiscoveryAgent",
    "LinkedInDiscovery",
    "FunctionalOrgMapper",
    "WebsiteAgent",
    "SECAgent",
    "NewsAgent",
    # Utilities
    "PageFinder",
    "HTMLCleaner",
    "FilingFetcher",
    "ChangeDetector",
]
