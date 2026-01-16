"""
Full-text search engine for Nexdata.

Provides unified search across investors, portfolio companies, and co-investors
with fuzzy matching, faceted filtering, and autocomplete.
"""

from app.search.engine import SearchEngine, SearchResultType

__all__ = ["SearchEngine", "SearchResultType"]
