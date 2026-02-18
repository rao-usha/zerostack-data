"""
GitHub API Client.

Fetches organization and repository data from GitHub REST API.
"""

import logging
import os
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger(__name__)


class GitHubClient:
    """
    Client for accessing GitHub API.

    Supports authenticated requests with rate limiting.
    """

    BASE_URL = "https://api.github.com"

    # Rate limit: 5000/hour authenticated, 60/hour unauthenticated
    RATE_LIMIT_DELAY = 0.1  # 100ms between requests

    def __init__(self):
        self.token = os.environ.get("GITHUB_TOKEN")
        self._last_request_time = 0
        self._rate_limit_remaining = 5000
        self._rate_limit_reset = datetime.now()

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication."""
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Nexdata-GitHub-Analytics/1.0"
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def _rate_limit(self):
        """Enforce rate limits."""
        now = asyncio.get_running_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            await asyncio.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = asyncio.get_running_loop().time()

    async def _request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make an authenticated request to GitHub API."""
        await self._rate_limit()

        url = f"{self.BASE_URL}{endpoint}"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=30
                )

                # Track rate limits
                self._rate_limit_remaining = int(response.headers.get("X-RateLimit-Remaining", 5000))
                reset_ts = int(response.headers.get("X-RateLimit-Reset", 0))
                if reset_ts:
                    self._rate_limit_reset = datetime.fromtimestamp(reset_ts)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 403 and self._rate_limit_remaining == 0:
                    logger.warning(f"GitHub rate limit exceeded. Resets at {self._rate_limit_reset}")
                    return None
                elif response.status_code == 404:
                    logger.warning(f"GitHub resource not found: {endpoint}")
                    return None
                else:
                    logger.warning(f"GitHub API error {response.status_code}: {response.text[:200]}")
                    return None

            except Exception as e:
                logger.error(f"GitHub API request failed: {e}")
                return None

    async def get_organization(self, org: str) -> Optional[Dict]:
        """
        Get organization details.

        Args:
            org: Organization login name

        Returns:
            Organization data or None
        """
        return await self._request(f"/orgs/{org}")

    async def get_org_repos(
        self,
        org: str,
        page: int = 1,
        per_page: int = 100,
        sort: str = "updated"
    ) -> List[Dict]:
        """
        Get repositories for an organization.

        Args:
            org: Organization login name
            page: Page number
            per_page: Results per page (max 100)
            sort: Sort by (created, updated, pushed, full_name)

        Returns:
            List of repositories
        """
        result = await self._request(
            f"/orgs/{org}/repos",
            params={"page": page, "per_page": per_page, "sort": sort}
        )
        return result if result else []

    async def get_repo_details(self, owner: str, repo: str) -> Optional[Dict]:
        """
        Get detailed information for a specific repository.

        Args:
            owner: Repository owner
            repo: Repository name

        Returns:
            Repository data or None
        """
        return await self._request(f"/repos/{owner}/{repo}")

    async def get_repo_languages(self, owner: str, repo: str) -> Dict[str, int]:
        """
        Get language breakdown for a repository.

        Args:
            owner: Repository owner
            repo: Repository name

        Returns:
            Dict mapping language to bytes of code
        """
        result = await self._request(f"/repos/{owner}/{repo}/languages")
        return result if result else {}

    async def get_repo_contributors(
        self,
        owner: str,
        repo: str,
        page: int = 1,
        per_page: int = 100
    ) -> List[Dict]:
        """
        Get contributors for a repository.

        Args:
            owner: Repository owner
            repo: Repository name
            page: Page number
            per_page: Results per page

        Returns:
            List of contributors with contribution counts
        """
        result = await self._request(
            f"/repos/{owner}/{repo}/contributors",
            params={"page": page, "per_page": per_page}
        )
        return result if result else []

    async def get_commit_activity(self, owner: str, repo: str) -> List[Dict]:
        """
        Get weekly commit activity for the last year.

        Args:
            owner: Repository owner
            repo: Repository name

        Returns:
            List of weekly commit counts (52 weeks)
        """
        result = await self._request(f"/repos/{owner}/{repo}/stats/commit_activity")
        return result if result else []

    async def get_participation_stats(self, owner: str, repo: str) -> Optional[Dict]:
        """
        Get contribution statistics for a repository.

        Args:
            owner: Repository owner
            repo: Repository name

        Returns:
            Weekly commit counts for all contributors and owner
        """
        return await self._request(f"/repos/{owner}/{repo}/stats/participation")

    async def get_repo_releases(
        self,
        owner: str,
        repo: str,
        per_page: int = 30
    ) -> List[Dict]:
        """
        Get releases for a repository.

        Args:
            owner: Repository owner
            repo: Repository name
            per_page: Number of releases to fetch

        Returns:
            List of releases
        """
        result = await self._request(
            f"/repos/{owner}/{repo}/releases",
            params={"per_page": per_page}
        )
        return result if result else []

    async def search_repos(
        self,
        query: str,
        sort: str = "stars",
        order: str = "desc",
        per_page: int = 30
    ) -> Dict[str, Any]:
        """
        Search for repositories.

        Args:
            query: Search query (e.g., "org:openai language:python")
            sort: Sort by (stars, forks, help-wanted-issues, updated)
            order: Sort order (asc, desc)
            per_page: Results per page

        Returns:
            Search results with total count and items
        """
        result = await self._request(
            "/search/repositories",
            params={"q": query, "sort": sort, "order": order, "per_page": per_page}
        )
        return result if result else {"total_count": 0, "items": []}

    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get current rate limit status."""
        return {
            "remaining": self._rate_limit_remaining,
            "reset_at": self._rate_limit_reset.isoformat() if self._rate_limit_reset else None,
            "authenticated": bool(self.token)
        }
