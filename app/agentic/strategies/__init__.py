"""
Collection strategies for the agentic portfolio research system.

Each strategy implements a different method for discovering portfolio data:
- SEC 13F: Extract public equity holdings from regulatory filings
- Website: Scrape official portfolio pages
- Annual Report: Parse PDF annual reports and CAFRs
- News: Search and extract from press coverage (LLM-powered)
- Reverse Search: Find mentions on portfolio company websites
"""

from app.agentic.strategies.base import BaseStrategy, StrategyResult, InvestorContext
from app.agentic.strategies.sec_13f_strategy import SEC13FStrategy
from app.agentic.strategies.website_strategy import WebsiteStrategy
from app.agentic.strategies.annual_report_strategy import AnnualReportStrategy
from app.agentic.strategies.news_strategy import NewsStrategy
from app.agentic.strategies.reverse_search_strategy import ReverseSearchStrategy

__all__ = [
    "BaseStrategy",
    "StrategyResult",
    "InvestorContext",
    "SEC13FStrategy",
    "WebsiteStrategy",
    "AnnualReportStrategy",
    "NewsStrategy",
    "ReverseSearchStrategy",
]
