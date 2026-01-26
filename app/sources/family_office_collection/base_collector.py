"""
Base collector class for Family Office data collection.

Provides:
- Rate limiting
- Retry logic with exponential backoff
- Request tracking
- Common HTTP utilities
"""

import asyncio
import logging
import random
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict, Any, List

import httpx

from app.sources.family_office_collection.types import (
    FoCollectionResult,
    FoCollectedItem,
    FoCollectionSource,
)

logger = logging.getLogger(__name__)


class FoBaseCollector(ABC):
    """
    Abstract base class for Family Office data collectors.

    Subclasses must implement:
    - source_type: The collection source type
    - collect(): The main collection logic

    Provides:
    - Rate-limited HTTP client
    - Retry logic with exponential backoff
    - Request/byte tracking
    """

    # Rate limiting settings
    DEFAULT_RATE_LIMIT_DELAY = 2.0  # seconds between requests
    DEFAULT_TIMEOUT = 30.0  # HTTP timeout in seconds
    DEFAULT_MAX_RETRIES = 3

    # User agent for HTTP requests
    USER_AGENT = "Nexdata-FO-Collector/1.0 (Data Research; contact@nexdata.io)"

    def __init__(
        self,
        rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        """
        Initialize the collector.

        Args:
            rate_limit_delay: Delay between requests in seconds
            timeout: HTTP timeout in seconds
            max_retries: Maximum retry attempts
        """
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.max_retries = max_retries

        # Request tracking
        self._requests_made = 0
        self._bytes_downloaded = 0
        self._last_request_time: Optional[datetime] = None

    @property
    @abstractmethod
    def source_type(self) -> FoCollectionSource:
        """The collection source type this collector handles."""
        pass

    @abstractmethod
    async def collect(
        self,
        fo_id: int,
        fo_name: str,
        website_url: Optional[str] = None,
        **kwargs,
    ) -> FoCollectionResult:
        """
        Collect data for a single family office.

        Args:
            fo_id: Family office ID
            fo_name: Family office name
            website_url: FO website URL (if applicable)
            **kwargs: Additional source-specific parameters

        Returns:
            FoCollectionResult with collected items or error
        """
        pass

    async def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        if self._last_request_time is not None:
            elapsed = (datetime.utcnow() - self._last_request_time).total_seconds()
            if elapsed < self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay - elapsed)

    async def _fetch_url(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
    ) -> Optional[httpx.Response]:
        """
        Fetch a URL with rate limiting and retry logic.

        Args:
            url: URL to fetch
            headers: Optional HTTP headers
            params: Optional query parameters
            method: HTTP method (GET, POST, etc.)

        Returns:
            Response object or None if all retries failed
        """
        await self._rate_limit()

        default_headers = {
            "User-Agent": self.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        if headers:
            default_headers.update(headers)

        for attempt in range(self.max_retries):
            try:
                self._last_request_time = datetime.utcnow()

                async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                    if method.upper() == "GET":
                        response = await client.get(url, headers=default_headers, params=params)
                    elif method.upper() == "POST":
                        response = await client.post(url, headers=default_headers, params=params)
                    else:
                        response = await client.request(method, url, headers=default_headers, params=params)

                self._requests_made += 1
                self._bytes_downloaded += len(response.content)

                if response.status_code == 200:
                    return response

                # Rate limited - exponential backoff
                if response.status_code == 429:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Rate limited on {url}, waiting {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                    continue

                # Client error - don't retry
                if 400 <= response.status_code < 500:
                    logger.warning(f"Client error {response.status_code} fetching {url}")
                    return response

                # Server error - retry with backoff
                if response.status_code >= 500:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Server error {response.status_code} on {url}, retry in {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                    continue

                return response

            except httpx.TimeoutException:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Timeout fetching {url}, retry {attempt + 1}/{self.max_retries}")
                await asyncio.sleep(wait_time)

            except httpx.RequestError as e:
                logger.error(f"Request error fetching {url}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return None

        logger.error(f"All {self.max_retries} retries failed for {url}")
        return None

    async def _fetch_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch JSON from a URL.

        Args:
            url: URL to fetch
            headers: Optional HTTP headers
            params: Optional query parameters

        Returns:
            Parsed JSON data or None if failed
        """
        json_headers = {"Accept": "application/json"}
        if headers:
            json_headers.update(headers)

        response = await self._fetch_url(url, headers=json_headers, params=params)
        if response and response.status_code == 200:
            try:
                return response.json()
            except Exception as e:
                logger.error(f"Error parsing JSON from {url}: {e}")
                return None
        return None

    def _create_result(
        self,
        fo_id: int,
        fo_name: str,
        success: bool = False,
        items: Optional[List[FoCollectedItem]] = None,
        error_message: Optional[str] = None,
        warnings: Optional[List[str]] = None,
        started_at: Optional[datetime] = None,
    ) -> FoCollectionResult:
        """
        Create a FoCollectionResult with current tracking data.

        Args:
            fo_id: Family office ID
            fo_name: Family office name
            success: Whether collection succeeded
            items: List of collected items
            error_message: Error message if failed
            warnings: List of warnings
            started_at: When collection started

        Returns:
            FoCollectionResult populated with tracking data
        """
        return FoCollectionResult(
            fo_id=fo_id,
            fo_name=fo_name,
            source=self.source_type,
            success=success,
            items=items or [],
            error_message=error_message,
            warnings=warnings or [],
            requests_made=self._requests_made,
            bytes_downloaded=self._bytes_downloaded,
            started_at=started_at,
            completed_at=datetime.utcnow(),
        )

    def reset_tracking(self) -> None:
        """Reset request tracking for a new collection."""
        self._requests_made = 0
        self._bytes_downloaded = 0
        self._last_request_time = None
