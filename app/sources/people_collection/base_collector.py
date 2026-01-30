"""
Base collector with HTTP client, rate limiting, and retry logic.

Provides the foundation for all collection agents.
"""

import asyncio
import hashlib
import logging
import time
from typing import Optional, Dict, Any
from urllib.parse import urlparse
import aiohttp
from aiohttp import ClientTimeout, ClientError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from app.sources.people_collection.config import RATE_LIMITS, RateLimitConfig

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Per-domain rate limiter using token bucket algorithm.

    Ensures we don't overwhelm any single domain with requests.
    """

    def __init__(self):
        self._last_request: Dict[str, float] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL for rate limiting."""
        parsed = urlparse(url)
        return parsed.netloc.lower()

    def _get_lock(self, domain: str) -> asyncio.Lock:
        """Get or create a lock for a domain."""
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()
        return self._locks[domain]

    async def acquire(self, url: str, source_type: str = "website") -> None:
        """
        Wait for rate limit before making a request.

        Args:
            url: The URL being requested
            source_type: Type of source for rate limit config
        """
        domain = self._get_domain(url)
        config = RATE_LIMITS.get(source_type, RATE_LIMITS["website"])
        min_interval = 1.0 / config.requests_per_second

        lock = self._get_lock(domain)
        async with lock:
            now = time.time()
            last = self._last_request.get(domain, 0)
            elapsed = now - last

            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                logger.debug(f"Rate limiting {domain}: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

            self._last_request[domain] = time.time()


class BaseCollector:
    """
    Base class for all collection agents.

    Provides:
    - HTTP client with proper headers
    - Rate limiting per domain
    - Retry logic for transient failures
    - Response caching
    """

    # Default headers to mimic a real browser
    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # SEC EDGAR specific headers (they require a user-agent with contact info)
    SEC_HEADERS = {
        "User-Agent": "Nexdata Research contact@nexdata.com",
        "Accept": "application/json, text/html, */*",
        "Accept-Encoding": "gzip, deflate",
    }

    def __init__(self, source_type: str = "website"):
        """
        Initialize the collector.

        Args:
            source_type: Type of source for rate limiting (website, sec_edgar, news)
        """
        self.source_type = source_type
        self.rate_limiter = RateLimiter()
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: Dict[str, Any] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            config = RATE_LIMITS.get(self.source_type, RATE_LIMITS["website"])
            timeout = ClientTimeout(total=config.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    def _get_headers(self, url: str) -> Dict[str, str]:
        """Get appropriate headers for the URL."""
        if "sec.gov" in url.lower():
            return self.SEC_HEADERS.copy()
        return self.DEFAULT_HEADERS.copy()

    def _cache_key(self, url: str) -> str:
        """Generate cache key for a URL."""
        return hashlib.md5(url.encode()).hexdigest()

    async def fetch_url(
        self,
        url: str,
        use_cache: bool = True,
        cache_ttl_seconds: int = 3600,
    ) -> Optional[str]:
        """
        Fetch a URL with rate limiting and caching.

        Args:
            url: URL to fetch
            use_cache: Whether to use cached response
            cache_ttl_seconds: Cache TTL in seconds

        Returns:
            Response text or None if failed
        """
        # Check cache
        cache_key = self._cache_key(url)
        if use_cache and cache_key in self._cache:
            cached = self._cache[cache_key]
            if time.time() - cached["time"] < cache_ttl_seconds:
                logger.debug(f"Cache hit for {url}")
                return cached["content"]

        # Rate limit
        await self.rate_limiter.acquire(url, self.source_type)

        # Fetch with retries
        try:
            content = await self._fetch_with_retry(url)
            if content:
                self._cache[cache_key] = {
                    "content": content,
                    "time": time.time(),
                }
            return content
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ClientError, asyncio.TimeoutError)),
    )
    async def _fetch_with_retry(self, url: str) -> Optional[str]:
        """Fetch URL with automatic retry on transient failures."""
        session = await self._get_session()
        headers = self._get_headers(url)

        logger.debug(f"Fetching {url}")

        async with session.get(url, headers=headers, allow_redirects=True) as response:
            if response.status == 200:
                content = await response.text()
                logger.debug(f"Fetched {url}: {len(content)} bytes")
                return content
            elif response.status == 404:
                logger.debug(f"Not found: {url}")
                return None
            elif response.status == 403:
                logger.warning(f"Forbidden: {url}")
                return None
            elif response.status == 429:
                # Rate limited - wait and retry
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited on {url}, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                raise ClientError(f"Rate limited: {url}")
            else:
                logger.warning(f"HTTP {response.status} for {url}")
                return None

    async def fetch_json(self, url: str, use_cache: bool = True) -> Optional[Dict]:
        """Fetch URL and parse as JSON."""
        content = await self.fetch_url(url, use_cache=use_cache)
        if content:
            import json
            try:
                return json.loads(content)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for {url}: {e}")
        return None

    async def check_url_exists(self, url: str) -> bool:
        """Check if a URL exists (returns 200)."""
        await self.rate_limiter.acquire(url, self.source_type)

        try:
            session = await self._get_session()
            headers = self._get_headers(url)

            async with session.head(url, headers=headers, allow_redirects=True) as response:
                return response.status == 200
        except Exception as e:
            logger.debug(f"URL check failed for {url}: {e}")
            return False

    def clear_cache(self) -> None:
        """Clear the response cache."""
        self._cache.clear()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
