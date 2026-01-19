"""
GitHub Repository Analytics Module.

T34: Track developer activity as a proxy for tech company health.
"""

from app.sources.github.client import GitHubClient
from app.sources.github.ingest import GitHubAnalyticsService

__all__ = ["GitHubClient", "GitHubAnalyticsService"]
