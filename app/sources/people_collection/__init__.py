"""
People Collection Module - Agentic data collection for leadership intelligence.

This module provides automated collection of corporate leadership data from:
- Company websites (leadership/team pages)
- SEC filings (DEF 14A, 8-K, 10-K)
- Press releases and news
- LinkedIn (Google-indexed profiles only)

Key Components:
- types.py: Pydantic models for extraction results
- config.py: Rate limits, LLM prompts, settings
- base_collector.py: HTTP client with rate limiting and retries
- llm_extractor.py: LLM-based data extraction (Claude/GPT)
- orchestrator.py: Coordinates collection agents
- page_finder.py: Leadership page discovery
- html_cleaner.py: HTML preprocessing for LLM
- website_agent.py: Leadership page extraction
- sec_agent.py: SEC filing parsing (Phase 4)
- news_agent.py: Press release monitoring (Phase 5)

Usage:
    from app.sources.people_collection import PeopleCollectionOrchestrator

    orchestrator = PeopleCollectionOrchestrator()
    result = await orchestrator.collect_company(company_id=1, sources=["website", "sec"])
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
    "WebsiteAgent",
    "SECAgent",
    "NewsAgent",
    # Utilities
    "PageFinder",
    "HTMLCleaner",
    "FilingFetcher",
    "ChangeDetector",
]
