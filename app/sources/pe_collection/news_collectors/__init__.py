"""
News collectors.

Collectors for gathering news and sentiment:
- RSS feeds (Bing News, Google News, Yahoo Finance)
- LLM-powered news classification and sentiment
"""

from app.sources.pe_collection.news_collectors.news_collector import PENewsCollector

__all__ = [
    "PENewsCollector",
]
