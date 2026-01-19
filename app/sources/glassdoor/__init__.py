"""
Glassdoor data source module.

Provides company reviews, ratings, and salary data.
"""

from app.sources.glassdoor.client import GlassdoorClient

__all__ = ["GlassdoorClient"]
