"""
Agentic Portfolio Research System.

This module provides an intelligent agent that automatically discovers and tracks
portfolio companies, investments, and deal flow for Limited Partners (LPs) and
Family Offices (FOs) by combining data from multiple sources.

Key Components:
- portfolio_agent: Main orchestrator that plans and executes strategies
- synthesizer: Deduplication and merging of findings from multiple sources
- validators: Data quality checks
- strategies/: Collection strategies (SEC 13F, website scraping, etc.)
"""

from app.agentic.portfolio_agent import PortfolioResearchAgent
from app.agentic.synthesizer import DataSynthesizer

__all__ = [
    "PortfolioResearchAgent",
    "DataSynthesizer",
]
