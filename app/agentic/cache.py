"""
Caching layer for expensive agentic operations.

Provides:
- In-memory cache with TTL (default, no dependencies)
- Optional Redis backend for distributed caching
- Cache decorator for async functions
- Key generation helpers

Usage:
    @cached(ttl=3600)  # Cache for 1 hour
    async def fetch_portfolio_page(url: str):
        ...
"""
import asyncio
import functools
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Union

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """A single cache entry with value and metadata."""
    value: Any
    created_at: float
    ttl: float  # Time-to-live in seconds
    hits: int = 0

    @property
    def expires_at(self) -> float:
        """Get expiration timestamp."""
        return self.created_at + self.ttl

    @property
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        return time.time() > self.expires_at

    @property
    def time_remaining(self) -> float:
        """Get remaining time before expiration."""
        return max(0, self.expires_at - time.time())


class InMemoryCache:
    """
    Simple in-memory cache with TTL support.

    Thread-safe for async operations.
    Automatically cleans up expired entries.
    """

    def __init__(
        self,
        default_ttl: float = 3600,
        max_size: int = 1000,
        cleanup_interval: float = 300
    ):
        """
        Initialize the cache.

        Args:
            default_ttl: Default time-to-live in seconds
            max_size: Maximum number of entries
            cleanup_interval: How often to clean expired entries (seconds)
        """
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.cleanup_interval = cleanup_interval

        self._cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        self._last_cleanup = time.time()

        # Statistics
        self._stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "evictions": 0,
        }

    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        async with self._lock:
            await self._maybe_cleanup()

            entry = self._cache.get(key)

            if entry is None:
                self._stats["misses"] += 1
                return None

            if entry.is_expired:
                del self._cache[key]
                self._stats["misses"] += 1
                return None

            entry.hits += 1
            self._stats["hits"] += 1
            return entry.value

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None
    ) -> None:
        """
        Set a value in the cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (uses default if None)
        """
        if ttl is None:
            ttl = self.default_ttl

        async with self._lock:
            await self._maybe_cleanup()

            # Evict oldest entry if at max size
            if len(self._cache) >= self.max_size and key not in self._cache:
                await self._evict_oldest()

            self._cache[key] = CacheEntry(
                value=value,
                created_at=time.time(),
                ttl=ttl
            )
            self._stats["sets"] += 1

    async def delete(self, key: str) -> bool:
        """
        Delete a key from the cache.

        Args:
            key: Cache key

        Returns:
            True if key was deleted, False if not found
        """
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    async def clear(self) -> int:
        """
        Clear all entries from the cache.

        Returns:
            Number of entries cleared
        """
        async with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count

    async def exists(self, key: str) -> bool:
        """Check if a key exists and is not expired."""
        value = await self.get(key)
        return value is not None

    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        async with self._lock:
            total_requests = self._stats["hits"] + self._stats["misses"]
            hit_rate = (
                self._stats["hits"] / total_requests
                if total_requests > 0 else 0
            )

            return {
                **self._stats,
                "size": len(self._cache),
                "max_size": self.max_size,
                "hit_rate": f"{hit_rate:.2%}",
            }

    async def _maybe_cleanup(self) -> None:
        """Clean up expired entries if cleanup interval has passed."""
        now = time.time()
        if now - self._last_cleanup < self.cleanup_interval:
            return

        self._last_cleanup = now
        expired_keys = [
            key for key, entry in self._cache.items()
            if entry.is_expired
        ]

        for key in expired_keys:
            del self._cache[key]

        if expired_keys:
            logger.debug(f"Cache cleanup: removed {len(expired_keys)} expired entries")

    async def _evict_oldest(self) -> None:
        """Evict the oldest entry from the cache."""
        if not self._cache:
            return

        oldest_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].created_at
        )
        del self._cache[oldest_key]
        self._stats["evictions"] += 1


# Global cache instance
_cache: Optional[InMemoryCache] = None


def get_cache() -> InMemoryCache:
    """Get or create the global cache instance."""
    global _cache
    if _cache is None:
        _cache = InMemoryCache()
    return _cache


def generate_cache_key(*args, prefix: str = "", **kwargs) -> str:
    """
    Generate a cache key from arguments.

    Args:
        *args: Positional arguments to include in key
        prefix: Optional prefix for the key
        **kwargs: Keyword arguments to include in key

    Returns:
        Hash-based cache key
    """
    # Create a hashable representation
    key_data = {
        "args": [str(a) for a in args],
        "kwargs": {k: str(v) for k, v in sorted(kwargs.items())}
    }

    key_str = json.dumps(key_data, sort_keys=True)
    key_hash = hashlib.md5(key_str.encode()).hexdigest()[:16]

    if prefix:
        return f"{prefix}:{key_hash}"
    return key_hash


def url_to_cache_key(url: str, prefix: str = "url") -> str:
    """
    Generate a cache key from a URL.

    Args:
        url: URL to generate key from
        prefix: Key prefix

    Returns:
        Cache key
    """
    url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
    return f"{prefix}:{url_hash}"


def cached(
    ttl: Optional[float] = None,
    key_prefix: str = "",
    key_builder: Optional[Callable[..., str]] = None
):
    """
    Decorator to cache async function results.

    Args:
        ttl: Time-to-live in seconds (uses cache default if None)
        key_prefix: Prefix for cache keys
        key_builder: Custom function to build cache key from arguments
                    Signature: key_builder(*args, **kwargs) -> str

    Example:
        @cached(ttl=3600, key_prefix="portfolio")
        async def fetch_portfolio(url: str):
            ...

        @cached(key_builder=lambda url, **kw: url_to_cache_key(url))
        async def scrape_page(url: str):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            cache = get_cache()

            # Build cache key
            if key_builder:
                key = key_builder(*args, **kwargs)
            else:
                prefix = key_prefix or func.__name__
                key = generate_cache_key(*args, prefix=prefix, **kwargs)

            # Check cache
            cached_value = await cache.get(key)
            if cached_value is not None:
                logger.debug(f"Cache hit for {key}")
                return cached_value

            # Execute function
            logger.debug(f"Cache miss for {key}")
            result = await func(*args, **kwargs)

            # Store in cache
            await cache.set(key, result, ttl=ttl)

            return result

        # Add cache control methods to wrapper
        wrapper.cache_clear = lambda: get_cache().clear()
        wrapper.cache_stats = lambda: get_cache().get_stats()

        return wrapper
    return decorator


class CacheContext:
    """
    Context manager for scoped cache operations.

    Useful for caching within a specific operation context.
    """

    def __init__(self, prefix: str, ttl: float = 3600):
        """
        Initialize cache context.

        Args:
            prefix: Prefix for all keys in this context
            ttl: Default TTL for entries
        """
        self.prefix = prefix
        self.ttl = ttl
        self._cache = get_cache()
        self._keys: set = set()

    async def get(self, key: str) -> Optional[Any]:
        """Get a value with context prefix."""
        full_key = f"{self.prefix}:{key}"
        return await self._cache.get(full_key)

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set a value with context prefix."""
        full_key = f"{self.prefix}:{key}"
        self._keys.add(full_key)
        await self._cache.set(full_key, value, ttl=ttl or self.ttl)

    async def clear_context(self) -> int:
        """Clear all keys created in this context."""
        count = 0
        for key in self._keys:
            if await self._cache.delete(key):
                count += 1
        self._keys.clear()
        return count

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Optionally clear on exit
        pass
