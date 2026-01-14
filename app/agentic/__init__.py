"""
Agentic Portfolio Research System.

This module provides an intelligent agent that automatically discovers and tracks
portfolio companies, investments, and deal flow for Limited Partners (LPs) and
Family Offices (FOs) by combining data from multiple sources.

Key Components:
- portfolio_agent: Main orchestrator that plans and executes strategies
- synthesizer: Deduplication and merging of findings from multiple sources
- llm_client: Unified LLM client for OpenAI/Anthropic
- ticker_resolver: Stock ticker to company name resolution
- strategies/: Collection strategies (SEC 13F, website scraping, etc.)
"""

from app.agentic.portfolio_agent import PortfolioResearchAgent
from app.agentic.synthesizer import DataSynthesizer
from app.agentic.llm_client import LLMClient, get_llm_client, LLMResponse
from app.agentic.ticker_resolver import TickerResolver, resolve_ticker, resolve_tickers_batch

__all__ = [
    "PortfolioResearchAgent",
    "DataSynthesizer",
    "LLMClient",
    "get_llm_client",
    "LLMResponse",
    "TickerResolver",
    "resolve_ticker",
    "resolve_tickers_batch",
]
