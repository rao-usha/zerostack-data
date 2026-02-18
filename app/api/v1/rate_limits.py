"""
Rate limit management endpoints.

Provides API for viewing and configuring per-source rate limits.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import SourceRateLimit
from app.core.rate_limiter import (
    get_rate_limiter,
    DEFAULT_RATE_LIMITS,
    save_rate_limit_to_db,
    init_default_rate_limits,
    load_rate_limits_from_db,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/rate-limits", tags=["rate-limits"])


# =============================================================================
# Pydantic Schemas
# =============================================================================


class RateLimitConfig(BaseModel):
    """Rate limit configuration for a source."""

    source: str = Field(..., min_length=1, max_length=50)
    requests_per_second: float = Field(
        ..., gt=0, le=100, description="Tokens added per second"
    )
    burst_capacity: int = Field(
        ..., ge=1, le=100, description="Maximum tokens (burst size)"
    )
    concurrent_limit: int = Field(
        ..., ge=1, le=50, description="Maximum concurrent requests"
    )
    description: Optional[str] = None


class RateLimitResponse(BaseModel):
    """Response schema for rate limit information."""

    source: str
    requests_per_second: float
    burst_capacity: int
    concurrent_limit: int
    description: Optional[str]
    is_enabled: bool
    total_requests: int
    total_throttled: int
    created_at: Optional[str]
    updated_at: Optional[str]


class RateLimitStats(BaseModel):
    """Runtime statistics for a rate limit."""

    source: str
    requests_per_second: float
    burst_capacity: int
    concurrent_limit: int
    current_tokens: float
    current_concurrent: int
    total_requests: int
    total_throttled: int


class DefaultRateLimit(BaseModel):
    """Default rate limit configuration."""

    source: str
    requests_per_second: float
    burst_capacity: int
    concurrent_limit: int
    description: Optional[str]


# =============================================================================
# Endpoints
# =============================================================================


@router.get("", response_model=List[RateLimitResponse])
def list_rate_limits(
    source: Optional[str] = Query(default=None, description="Filter by source"),
    enabled_only: bool = Query(default=False, description="Only show enabled limits"),
    db: Session = Depends(get_db),
) -> List[RateLimitResponse]:
    """
    List all configured rate limits.

    Returns rate limit configurations stored in the database.
    """
    query = db.query(SourceRateLimit)

    if source:
        query = query.filter(SourceRateLimit.source == source)

    if enabled_only:
        query = query.filter(SourceRateLimit.is_enabled == 1)

    rate_limits = query.order_by(SourceRateLimit.source).all()

    return [
        RateLimitResponse(
            source=rl.source,
            requests_per_second=float(rl.requests_per_second),
            burst_capacity=rl.burst_capacity,
            concurrent_limit=rl.concurrent_limit,
            description=rl.description,
            is_enabled=bool(rl.is_enabled),
            total_requests=rl.total_requests,
            total_throttled=rl.total_throttled,
            created_at=rl.created_at.isoformat() if rl.created_at else None,
            updated_at=rl.updated_at.isoformat() if rl.updated_at else None,
        )
        for rl in rate_limits
    ]


@router.get("/defaults", response_model=List[DefaultRateLimit])
def list_default_rate_limits():
    """
    List default rate limits for all known sources.

    These are the pre-configured limits based on documented API rate limits.
    """
    return [
        DefaultRateLimit(
            source=source,
            requests_per_second=config["requests_per_second"],
            burst_capacity=config["burst_capacity"],
            concurrent_limit=config["concurrent_limit"],
            description=config.get("description"),
        )
        for source, config in sorted(DEFAULT_RATE_LIMITS.items())
        if source != "default"
    ]


@router.get("/stats")
def get_runtime_stats():
    """
    Get runtime statistics for all active rate limiters.

    Shows current token counts, concurrent requests, and throttle counts.
    """
    limiter = get_rate_limiter()
    return {
        "active_sources": list(limiter._buckets.keys()),
        "stats": limiter.get_all_stats(),
    }


@router.get("/stats/{source}", response_model=RateLimitStats)
def get_source_stats(source: str):
    """
    Get runtime statistics for a specific source.
    """
    limiter = get_rate_limiter()
    stats = limiter.get_stats(source)
    return RateLimitStats(**stats)


@router.get("/{source}", response_model=RateLimitResponse)
def get_rate_limit(source: str, db: Session = Depends(get_db)) -> RateLimitResponse:
    """
    Get rate limit configuration for a specific source.
    """
    rate_limit = (
        db.query(SourceRateLimit).filter(SourceRateLimit.source == source).first()
    )

    if not rate_limit:
        # Check if it's a known default
        if source in DEFAULT_RATE_LIMITS:
            default = DEFAULT_RATE_LIMITS[source]
            return RateLimitResponse(
                source=source,
                requests_per_second=default["requests_per_second"],
                burst_capacity=default["burst_capacity"],
                concurrent_limit=default["concurrent_limit"],
                description=default.get(
                    "description", "Default configuration (not persisted)"
                ),
                is_enabled=True,
                total_requests=0,
                total_throttled=0,
                created_at=None,
                updated_at=None,
            )
        raise HTTPException(
            status_code=404, detail=f"Rate limit not found for source: {source}"
        )

    return RateLimitResponse(
        source=rate_limit.source,
        requests_per_second=float(rate_limit.requests_per_second),
        burst_capacity=rate_limit.burst_capacity,
        concurrent_limit=rate_limit.concurrent_limit,
        description=rate_limit.description,
        is_enabled=bool(rate_limit.is_enabled),
        total_requests=rate_limit.total_requests,
        total_throttled=rate_limit.total_throttled,
        created_at=rate_limit.created_at.isoformat() if rate_limit.created_at else None,
        updated_at=rate_limit.updated_at.isoformat() if rate_limit.updated_at else None,
    )


@router.post("", response_model=RateLimitResponse, status_code=201)
def create_or_update_rate_limit(
    config: RateLimitConfig, db: Session = Depends(get_db)
) -> RateLimitResponse:
    """
    Create or update rate limit configuration for a source.

    If the source already exists, updates the configuration.
    Also applies the new configuration to the runtime rate limiter.
    """
    rate_limit = save_rate_limit_to_db(
        db=db,
        source=config.source,
        requests_per_second=config.requests_per_second,
        burst_capacity=config.burst_capacity,
        concurrent_limit=config.concurrent_limit,
        description=config.description,
    )

    # Apply to runtime limiter
    limiter = get_rate_limiter()
    limiter.configure_source(
        source=config.source,
        requests_per_second=config.requests_per_second,
        burst_capacity=config.burst_capacity,
        concurrent_limit=config.concurrent_limit,
    )

    return RateLimitResponse(
        source=rate_limit.source,
        requests_per_second=float(rate_limit.requests_per_second),
        burst_capacity=rate_limit.burst_capacity,
        concurrent_limit=rate_limit.concurrent_limit,
        description=rate_limit.description,
        is_enabled=bool(rate_limit.is_enabled),
        total_requests=rate_limit.total_requests,
        total_throttled=rate_limit.total_throttled,
        created_at=rate_limit.created_at.isoformat() if rate_limit.created_at else None,
        updated_at=rate_limit.updated_at.isoformat() if rate_limit.updated_at else None,
    )


@router.post("/{source}/enable")
def enable_rate_limit(source: str, db: Session = Depends(get_db)):
    """
    Enable rate limiting for a source.
    """
    rate_limit = (
        db.query(SourceRateLimit).filter(SourceRateLimit.source == source).first()
    )

    if not rate_limit:
        raise HTTPException(
            status_code=404, detail=f"Rate limit not found for source: {source}"
        )

    rate_limit.is_enabled = 1
    db.commit()

    return {"message": f"Rate limiting enabled for '{source}'"}


@router.post("/{source}/disable")
def disable_rate_limit(source: str, db: Session = Depends(get_db)):
    """
    Disable rate limiting for a source.

    Note: This only affects the database record. The runtime limiter will
    continue using default limits. Use this to temporarily bypass custom limits.
    """
    rate_limit = (
        db.query(SourceRateLimit).filter(SourceRateLimit.source == source).first()
    )

    if not rate_limit:
        raise HTTPException(
            status_code=404, detail=f"Rate limit not found for source: {source}"
        )

    rate_limit.is_enabled = 0
    db.commit()

    return {"message": f"Rate limiting disabled for '{source}'"}


@router.post("/{source}/reset")
def reset_rate_limit(source: str):
    """
    Reset rate limit state for a source.

    Refills tokens to full capacity and clears concurrent request count.
    Useful if a source gets stuck in a throttled state.
    """
    limiter = get_rate_limiter()
    limiter.reset_source(source)

    return {"message": f"Rate limit state reset for '{source}'"}


@router.delete("/{source}")
def delete_rate_limit(source: str, db: Session = Depends(get_db)):
    """
    Delete rate limit configuration for a source.

    The source will fall back to default rate limits after deletion.
    """
    rate_limit = (
        db.query(SourceRateLimit).filter(SourceRateLimit.source == source).first()
    )

    if not rate_limit:
        raise HTTPException(
            status_code=404, detail=f"Rate limit not found for source: {source}"
        )

    db.delete(rate_limit)
    db.commit()

    return {"message": f"Rate limit deleted for '{source}'. Will use default limits."}


@router.post("/init-defaults")
def initialize_default_rate_limits(db: Session = Depends(get_db)):
    """
    Initialize database with default rate limits.

    Creates rate limit entries for all known sources that don't already
    have a configuration. Existing configurations are not overwritten.
    """
    count = init_default_rate_limits(db)

    # Reload into runtime limiter
    limiter = get_rate_limiter()
    loaded = load_rate_limits_from_db(db, limiter)

    return {
        "message": f"Initialized {count} new rate limits, {loaded} total loaded",
        "created": count,
        "total_active": loaded,
    }


@router.post("/reload")
def reload_rate_limits(db: Session = Depends(get_db)):
    """
    Reload rate limit configurations from database.

    Updates the runtime rate limiter with current database configurations.
    """
    limiter = get_rate_limiter()
    count = load_rate_limits_from_db(db, limiter)

    return {"message": f"Reloaded {count} rate limit configurations", "loaded": count}
