"""
Per-source configuration service.

Provides CRUD for source-specific timeouts, retry policies, and rate limits.
Missing config rows fall back to global defaults.
"""

import logging
from typing import Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models import SourceConfig

logger = logging.getLogger(__name__)

# Global defaults (match existing constants across the codebase)
GLOBAL_DEFAULTS: Dict[str, Any] = {
    "timeout_seconds": 21600,  # 6 hours (STUCK_JOB_TIMEOUT_HOURS * 3600)
    "max_retries": 3,
    "retry_backoff_base_min": 5,  # BASE_DELAY_MINUTES
    "retry_backoff_max_min": 1440,  # MAX_DELAY_MINUTES (24 hours)
    "retry_backoff_multiplier": 2,  # BACKOFF_MULTIPLIER
    "rate_limit_rps": 5.0,
    "max_concurrent": 4,
}

# Initial seed configs for known sources
SEED_CONFIGS = [
    {
        "source": "census",
        "timeout_seconds": 14400,
        "description": "Census ACS - runs 2-4 hours",
    },
    {
        "source": "fcc",
        "timeout_seconds": 10800,
        "description": "FCC - large downloads, often 403",
    },
    {
        "source": "eia",
        "timeout_seconds": 7200,
        "description": "EIA - rate limited, many endpoints",
    },
    {
        "source": "treasury",
        "timeout_seconds": 300,
        "max_retries": 5,
        "description": "Fast source, retry aggressively",
    },
]


def get_source_config(db: Session, source: str) -> Dict[str, Any]:
    """
    Get configuration for a source, falling back to global defaults.

    Args:
        db: Database session
        source: Source identifier (e.g. "census", "eia")

    Returns:
        Config dict with all fields populated (from DB or defaults)
    """
    row = db.query(SourceConfig).filter(SourceConfig.source == source).first()

    if row is None:
        return {"source": source, **GLOBAL_DEFAULTS, "is_default": True}

    return {
        "source": row.source,
        "timeout_seconds": row.timeout_seconds
        if row.timeout_seconds is not None
        else GLOBAL_DEFAULTS["timeout_seconds"],
        "max_retries": row.max_retries
        if row.max_retries is not None
        else GLOBAL_DEFAULTS["max_retries"],
        "retry_backoff_base_min": row.retry_backoff_base_min
        if row.retry_backoff_base_min is not None
        else GLOBAL_DEFAULTS["retry_backoff_base_min"],
        "retry_backoff_max_min": row.retry_backoff_max_min
        if row.retry_backoff_max_min is not None
        else GLOBAL_DEFAULTS["retry_backoff_max_min"],
        "retry_backoff_multiplier": row.retry_backoff_multiplier
        if row.retry_backoff_multiplier is not None
        else GLOBAL_DEFAULTS["retry_backoff_multiplier"],
        "rate_limit_rps": float(row.rate_limit_rps)
        if row.rate_limit_rps is not None
        else GLOBAL_DEFAULTS["rate_limit_rps"],
        "max_concurrent": row.max_concurrent
        if row.max_concurrent is not None
        else GLOBAL_DEFAULTS["max_concurrent"],
        "description": row.description,
        "is_default": False,
    }


def get_timeout_seconds(db: Session, source: str) -> int:
    """Get timeout in seconds for a source."""
    config = get_source_config(db, source)
    return config["timeout_seconds"]


def get_retry_config(db: Session, source: str) -> Dict[str, Any]:
    """Get retry configuration for a source."""
    config = get_source_config(db, source)
    return {
        "max_retries": config["max_retries"],
        "backoff_base_min": config["retry_backoff_base_min"],
        "backoff_max_min": config["retry_backoff_max_min"],
        "backoff_multiplier": config["retry_backoff_multiplier"],
    }


def list_source_configs(db: Session) -> List[Dict[str, Any]]:
    """List all source configurations."""
    rows = db.query(SourceConfig).order_by(SourceConfig.source).all()
    results = []
    for row in rows:
        results.append(
            {
                "source": row.source,
                "timeout_seconds": row.timeout_seconds,
                "max_retries": row.max_retries,
                "retry_backoff_base_min": row.retry_backoff_base_min,
                "retry_backoff_max_min": row.retry_backoff_max_min,
                "retry_backoff_multiplier": row.retry_backoff_multiplier,
                "rate_limit_rps": float(row.rate_limit_rps)
                if row.rate_limit_rps is not None
                else None,
                "max_concurrent": row.max_concurrent,
                "description": row.description,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
        )
    return results


def upsert_source_config(db: Session, source: str, **kwargs) -> Dict[str, Any]:
    """
    Create or update a source configuration.

    Args:
        db: Database session
        source: Source identifier
        **kwargs: Config fields to set

    Returns:
        Updated config dict
    """
    row = db.query(SourceConfig).filter(SourceConfig.source == source).first()

    if row is None:
        row = SourceConfig(source=source)
        db.add(row)

    for key, value in kwargs.items():
        if hasattr(row, key) and key not in ("id", "source", "created_at"):
            setattr(row, key, value)

    db.commit()
    db.refresh(row)

    logger.info(f"Upserted source config for {source}")
    return get_source_config(db, source)


def delete_source_config(db: Session, source: str) -> bool:
    """
    Delete a source configuration (reverts to global defaults).

    Returns:
        True if deleted, False if not found
    """
    row = db.query(SourceConfig).filter(SourceConfig.source == source).first()
    if row is None:
        return False

    db.delete(row)
    db.commit()
    logger.info(f"Deleted source config for {source}, reverted to defaults")
    return True


def seed_source_configs(db: Session) -> int:
    """
    Seed initial source configurations (idempotent).

    Returns:
        Number of configs created
    """
    created = 0
    for seed in SEED_CONFIGS:
        source = seed["source"]
        existing = db.query(SourceConfig).filter(SourceConfig.source == source).first()
        if existing is None:
            row = SourceConfig(**seed)
            db.add(row)
            created += 1

    if created:
        db.commit()
        logger.info(f"Seeded {created} source configs")
    return created
