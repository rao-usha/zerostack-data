"""
Per-source rate limiter service.

Implements token bucket algorithm for rate limiting external API requests.
Each source has configurable limits with burst capacity support.
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Optional, Any
from dataclasses import dataclass
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session

from app.core.models import SourceRateLimit

logger = logging.getLogger(__name__)


# =============================================================================
# Default Rate Limits for Known Sources
# =============================================================================

# These are based on documented API rate limits
DEFAULT_RATE_LIMITS: Dict[str, Dict[str, Any]] = {
    # Federal Reserve Economic Data
    "fred": {
        "requests_per_second": 2.0,  # 120/min with key
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "FRED API: 120 requests/minute with API key"
    },
    # US Census Bureau
    "census": {
        "requests_per_second": 0.8,  # ~50/min recommended
        "burst_capacity": 5,
        "concurrent_limit": 3,
        "description": "Census API: ~50 requests/minute recommended"
    },
    # Energy Information Administration
    "eia": {
        "requests_per_second": 1.0,  # Conservative estimate
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "EIA API: No published limit, conservative 60/min"
    },
    # Bureau of Labor Statistics
    "bls": {
        "requests_per_second": 0.5,  # 25/day without key, 500/day with key
        "burst_capacity": 5,
        "concurrent_limit": 2,
        "description": "BLS API: 500 requests/day with API key"
    },
    # Securities and Exchange Commission
    "sec": {
        "requests_per_second": 0.1,  # 10 requests/second max, be conservative
        "burst_capacity": 5,
        "concurrent_limit": 2,
        "description": "SEC EDGAR: 10 requests/second max, be respectful"
    },
    # Bureau of Economic Analysis
    "bea": {
        "requests_per_second": 1.5,  # 100 requests/minute
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "BEA API: 100 requests/minute"
    },
    # NOAA Climate Data
    "noaa": {
        "requests_per_second": 0.2,  # 5 requests/second max
        "burst_capacity": 5,
        "concurrent_limit": 2,
        "description": "NOAA CDO: 5 requests/second max"
    },
    # Yelp Fusion API
    "yelp": {
        "requests_per_second": 0.1,  # 500/day = ~0.006/sec, but allow bursts
        "burst_capacity": 10,
        "concurrent_limit": 2,
        "description": "Yelp Fusion: 500 calls/day free tier"
    },
    # Google Data Commons
    "data_commons": {
        "requests_per_second": 1.0,
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "Data Commons: No published limit, conservative"
    },
    # Bureau of Transportation Statistics
    "bts": {
        "requests_per_second": 0.5,
        "burst_capacity": 5,
        "concurrent_limit": 3,
        "description": "BTS Socrata: ~1000 requests/hour without token"
    },
    # FEMA OpenFEMA
    "fema": {
        "requests_per_second": 1.0,
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "OpenFEMA: No published limit"
    },
    # FBI Crime Data
    "fbi_crime": {
        "requests_per_second": 0.5,
        "burst_capacity": 5,
        "concurrent_limit": 2,
        "description": "FBI Crime Data Explorer: Rate limited"
    },
    # US Trade (Census)
    "us_trade": {
        "requests_per_second": 0.8,
        "burst_capacity": 5,
        "concurrent_limit": 3,
        "description": "Census Trade: Same as Census API"
    },
    # CFTC Commitments of Traders
    "cftc_cot": {
        "requests_per_second": 1.0,
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "CFTC COT: Public data, be respectful"
    },
    # USDA NASS
    "usda": {
        "requests_per_second": 0.5,
        "burst_capacity": 5,
        "concurrent_limit": 3,
        "description": "USDA NASS QuickStats: Rate limited"
    },
    # FCC Broadband
    "fcc_broadband": {
        "requests_per_second": 1.0,
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "FCC National Broadband Map"
    },
    # Treasury FiscalData
    "treasury": {
        "requests_per_second": 1.0,
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "Treasury FiscalData: Public API"
    },
    # FDIC BankFind
    "fdic": {
        "requests_per_second": 1.0,
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "FDIC BankFind Suite: Public API"
    },
    # IRS Statistics of Income
    "irs_soi": {
        "requests_per_second": 0.5,
        "burst_capacity": 5,
        "concurrent_limit": 3,
        "description": "IRS SOI: Public files, be respectful"
    },
    # International Economic Data
    "international_econ": {
        "requests_per_second": 0.5,
        "burst_capacity": 5,
        "concurrent_limit": 3,
        "description": "World Bank/IMF/OECD APIs"
    },
    # Kaggle
    "kaggle": {
        "requests_per_second": 0.2,
        "burst_capacity": 3,
        "concurrent_limit": 2,
        "description": "Kaggle API: Rate limited"
    },
    # CMS Healthcare
    "cms": {
        "requests_per_second": 1.0,
        "burst_capacity": 10,
        "concurrent_limit": 5,
        "description": "CMS/HHS: Public data APIs"
    },
    # Real Estate sources
    "realestate": {
        "requests_per_second": 0.5,
        "burst_capacity": 5,
        "concurrent_limit": 3,
        "description": "Real estate data sources"
    },
    # Default fallback
    "default": {
        "requests_per_second": 1.0,
        "burst_capacity": 5,
        "concurrent_limit": 3,
        "description": "Default rate limit for unknown sources"
    }
}


# =============================================================================
# Token Bucket Implementation
# =============================================================================

@dataclass
class TokenBucket:
    """
    Token bucket for rate limiting.

    Tokens are added at a fixed rate (requests_per_second) up to a maximum
    (burst_capacity). Each request consumes one token.
    """
    source: str
    requests_per_second: float
    burst_capacity: int
    concurrent_limit: int

    # Current state
    tokens: float = 0.0
    last_refill: float = 0.0
    current_concurrent: int = 0

    # Statistics
    total_requests: int = 0
    total_throttled: int = 0

    def __post_init__(self):
        """Initialize with full bucket."""
        self.tokens = float(self.burst_capacity)
        self.last_refill = time.time()

    def _refill(self) -> None:
        """Add tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill

        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.requests_per_second
        self.tokens = min(self.burst_capacity, self.tokens + tokens_to_add)
        self.last_refill = now

    def try_acquire(self) -> bool:
        """
        Try to acquire a token for making a request.

        Returns:
            True if token acquired, False if rate limited
        """
        self._refill()

        # Check concurrent limit
        if self.current_concurrent >= self.concurrent_limit:
            self.total_throttled += 1
            return False

        # Check token availability
        if self.tokens < 1.0:
            self.total_throttled += 1
            return False

        # Acquire token
        self.tokens -= 1.0
        self.current_concurrent += 1
        self.total_requests += 1
        return True

    def release(self) -> None:
        """Release concurrent slot after request completes."""
        self.current_concurrent = max(0, self.current_concurrent - 1)

    def wait_time(self) -> float:
        """
        Calculate time to wait before a token becomes available.

        Returns:
            Seconds to wait (0 if token available)
        """
        self._refill()

        if self.tokens >= 1.0 and self.current_concurrent < self.concurrent_limit:
            return 0.0

        # Calculate time until next token
        tokens_needed = 1.0 - self.tokens
        if tokens_needed <= 0:
            tokens_needed = 0.01  # Small wait for concurrent slot

        return tokens_needed / self.requests_per_second


# =============================================================================
# Rate Limiter Service
# =============================================================================

class RateLimiterService:
    """
    Per-source rate limiter service.

    Manages token buckets for each data source and provides
    async context managers for rate-limited requests.
    """

    def __init__(self):
        self._buckets: Dict[str, TokenBucket] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _get_bucket(self, source: str) -> TokenBucket:
        """Get or create a token bucket for a source."""
        if source not in self._buckets:
            # Get config for this source, fall back to default
            config = DEFAULT_RATE_LIMITS.get(source, DEFAULT_RATE_LIMITS["default"])

            self._buckets[source] = TokenBucket(
                source=source,
                requests_per_second=config["requests_per_second"],
                burst_capacity=config["burst_capacity"],
                concurrent_limit=config["concurrent_limit"]
            )
            self._locks[source] = asyncio.Lock()

        return self._buckets[source]

    def _get_lock(self, source: str) -> asyncio.Lock:
        """Get lock for a source."""
        if source not in self._locks:
            self._locks[source] = asyncio.Lock()
        return self._locks[source]

    async def acquire(self, source: str, timeout: float = 30.0) -> bool:
        """
        Acquire rate limit permission for a source.

        Args:
            source: Data source name
            timeout: Maximum seconds to wait

        Returns:
            True if acquired, False if timed out
        """
        bucket = self._get_bucket(source)
        lock = self._get_lock(source)

        start_time = time.time()

        while True:
            async with lock:
                if bucket.try_acquire():
                    return True

                wait_time = bucket.wait_time()

            # Check timeout
            elapsed = time.time() - start_time
            if elapsed + wait_time > timeout:
                logger.warning(f"Rate limit timeout for source '{source}' after {elapsed:.1f}s")
                return False

            # Wait before retrying
            await asyncio.sleep(min(wait_time, 0.5))

    def release(self, source: str) -> None:
        """Release rate limit slot after request completes."""
        if source in self._buckets:
            self._buckets[source].release()

    @asynccontextmanager
    async def limit(self, source: str, timeout: float = 30.0):
        """
        Async context manager for rate-limited requests.

        Usage:
            async with rate_limiter.limit("fred"):
                response = await client.get(url)

        Args:
            source: Data source name
            timeout: Maximum seconds to wait for rate limit

        Raises:
            RateLimitExceeded: If timeout waiting for rate limit
        """
        acquired = await self.acquire(source, timeout)

        if not acquired:
            raise RateLimitExceeded(f"Rate limit exceeded for source '{source}'")

        try:
            yield
        finally:
            self.release(source)

    def get_stats(self, source: str) -> Dict[str, Any]:
        """Get rate limit statistics for a source."""
        bucket = self._get_bucket(source)

        return {
            "source": source,
            "requests_per_second": bucket.requests_per_second,
            "burst_capacity": bucket.burst_capacity,
            "concurrent_limit": bucket.concurrent_limit,
            "current_tokens": round(bucket.tokens, 2),
            "current_concurrent": bucket.current_concurrent,
            "total_requests": bucket.total_requests,
            "total_throttled": bucket.total_throttled
        }

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get rate limit statistics for all active sources."""
        return {source: self.get_stats(source) for source in self._buckets}

    def configure_source(
        self,
        source: str,
        requests_per_second: float,
        burst_capacity: int,
        concurrent_limit: int
    ) -> None:
        """
        Configure rate limits for a source.

        Args:
            source: Data source name
            requests_per_second: Tokens added per second
            burst_capacity: Maximum tokens (burst size)
            concurrent_limit: Maximum concurrent requests
        """
        self._buckets[source] = TokenBucket(
            source=source,
            requests_per_second=requests_per_second,
            burst_capacity=burst_capacity,
            concurrent_limit=concurrent_limit
        )

        if source not in self._locks:
            self._locks[source] = asyncio.Lock()

        logger.info(
            f"Configured rate limit for '{source}': "
            f"{requests_per_second} rps, burst={burst_capacity}, concurrent={concurrent_limit}"
        )

    def reset_source(self, source: str) -> None:
        """Reset rate limit state for a source (refill tokens)."""
        if source in self._buckets:
            bucket = self._buckets[source]
            bucket.tokens = float(bucket.burst_capacity)
            bucket.last_refill = time.time()
            bucket.current_concurrent = 0
            logger.info(f"Reset rate limit state for '{source}'")


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded and timeout occurs."""
    pass


# =============================================================================
# Database Integration
# =============================================================================

def load_rate_limits_from_db(db: Session, service: RateLimiterService) -> int:
    """
    Load rate limit configurations from database.

    Args:
        db: Database session
        service: Rate limiter service to configure

    Returns:
        Number of rate limits loaded
    """
    rate_limits = db.query(SourceRateLimit).filter(
        SourceRateLimit.is_enabled == 1
    ).all()

    count = 0
    for rl in rate_limits:
        service.configure_source(
            source=rl.source,
            requests_per_second=float(rl.requests_per_second),
            burst_capacity=rl.burst_capacity,
            concurrent_limit=rl.concurrent_limit
        )
        count += 1

    logger.info(f"Loaded {count} rate limit configurations from database")
    return count


def save_rate_limit_to_db(
    db: Session,
    source: str,
    requests_per_second: float,
    burst_capacity: int,
    concurrent_limit: int,
    description: Optional[str] = None
) -> SourceRateLimit:
    """
    Save rate limit configuration to database.

    Args:
        db: Database session
        source: Data source name
        requests_per_second: Tokens per second
        burst_capacity: Maximum burst size
        concurrent_limit: Maximum concurrent requests
        description: Optional description

    Returns:
        Created or updated SourceRateLimit
    """
    rate_limit = db.query(SourceRateLimit).filter(
        SourceRateLimit.source == source
    ).first()

    if rate_limit:
        # Update existing
        rate_limit.requests_per_second = str(requests_per_second)
        rate_limit.burst_capacity = burst_capacity
        rate_limit.concurrent_limit = concurrent_limit
        if description:
            rate_limit.description = description
    else:
        # Create new
        rate_limit = SourceRateLimit(
            source=source,
            requests_per_second=str(requests_per_second),
            burst_capacity=burst_capacity,
            concurrent_limit=concurrent_limit,
            description=description or DEFAULT_RATE_LIMITS.get(source, {}).get("description")
        )
        db.add(rate_limit)

    db.commit()
    db.refresh(rate_limit)

    logger.info(f"Saved rate limit for '{source}': {requests_per_second} rps")
    return rate_limit


def init_default_rate_limits(db: Session) -> int:
    """
    Initialize database with default rate limits.

    Only creates entries for sources that don't already exist.

    Args:
        db: Database session

    Returns:
        Number of rate limits created
    """
    count = 0

    for source, config in DEFAULT_RATE_LIMITS.items():
        if source == "default":
            continue

        # Check if already exists
        existing = db.query(SourceRateLimit).filter(
            SourceRateLimit.source == source
        ).first()

        if not existing:
            rate_limit = SourceRateLimit(
                source=source,
                requests_per_second=str(config["requests_per_second"]),
                burst_capacity=config["burst_capacity"],
                concurrent_limit=config["concurrent_limit"],
                description=config.get("description")
            )
            db.add(rate_limit)
            count += 1

    if count > 0:
        db.commit()
        logger.info(f"Initialized {count} default rate limits")

    return count


def update_rate_limit_stats(
    db: Session,
    source: str,
    service: RateLimiterService
) -> None:
    """
    Update rate limit statistics in database.

    Args:
        db: Database session
        source: Data source name
        service: Rate limiter service with current stats
    """
    rate_limit = db.query(SourceRateLimit).filter(
        SourceRateLimit.source == source
    ).first()

    if rate_limit and source in service._buckets:
        bucket = service._buckets[source]
        rate_limit.total_requests = bucket.total_requests
        rate_limit.total_throttled = bucket.total_throttled
        rate_limit.last_request_at = datetime.utcnow()
        rate_limit.current_tokens = str(round(bucket.tokens, 2))
        rate_limit.last_refill_at = datetime.utcfromtimestamp(bucket.last_refill)
        db.commit()


# =============================================================================
# Global Rate Limiter Instance
# =============================================================================

# Singleton instance
_rate_limiter: Optional[RateLimiterService] = None


def get_rate_limiter() -> RateLimiterService:
    """Get the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiterService()
    return _rate_limiter


def reset_rate_limiter() -> None:
    """Reset the global rate limiter instance (for testing)."""
    global _rate_limiter
    _rate_limiter = None
