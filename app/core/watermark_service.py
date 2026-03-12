"""
Source watermark service.

Tracks the last successful ingestion timestamp per source, independent of
schedules. Enables incremental loading for manual and batch jobs.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.core.models import SourceWatermark

logger = logging.getLogger(__name__)


def get_watermark(db: Session, source: str) -> Optional[datetime]:
    """
    Get the last successful ingestion timestamp for a source.

    Returns None if no successful run has been recorded (first run -> full load).
    """
    row = db.query(SourceWatermark).filter(SourceWatermark.source == source).first()
    return row.last_success_at if row else None


def advance_watermark(
    db: Session,
    source: str,
    completed_at: datetime,
    job_id: int,
) -> None:
    """
    Advance the source watermark if the new timestamp is newer.

    Only advances forward -- never moves the watermark back in time.
    """
    row = db.query(SourceWatermark).filter(SourceWatermark.source == source).first()

    if row:
        if row.last_success_at and completed_at <= row.last_success_at:
            return  # Don't move backward
        row.last_success_at = completed_at
        row.last_job_id = job_id
    else:
        row = SourceWatermark(
            source=source,
            last_success_at=completed_at,
            last_job_id=job_id,
        )
        db.add(row)

    db.commit()
    logger.info(f"Advanced watermark for {source} to {completed_at}")


def get_all_watermarks(db: Session, domain: str = None) -> list:
    """
    List all watermarks, optionally filtered by domain prefix.

    Returns list of dicts with source, last_success_at, last_job_id.
    """
    query = db.query(SourceWatermark)
    if domain:
        query = query.filter(SourceWatermark.source.ilike(f"{domain}%"))
    rows = query.order_by(SourceWatermark.source).all()
    return [
        {
            "source": r.source,
            "last_success_at": r.last_success_at.isoformat() if r.last_success_at else None,
            "last_job_id": r.last_job_id,
        }
        for r in rows
    ]


def clear_watermark(
    db: Session, domain: str, source: str, state: str = None
) -> bool:
    """
    Clear a watermark to force full re-sync on next collection.

    Uses domain/source as a combined key matching the source column.
    """
    key = f"{domain}:{source}" if source else domain
    row = db.query(SourceWatermark).filter(SourceWatermark.source == key).first()
    if not row:
        # Try just source as-is
        row = db.query(SourceWatermark).filter(SourceWatermark.source == source).first()
    if not row:
        return False
    db.delete(row)
    db.commit()
    logger.info(f"Cleared watermark for {key}")
    return True


def inject_incremental_from_watermark(
    config: Dict[str, Any],
    source: str,
    db: Session,
) -> Dict[str, Any]:
    """
    Inject incremental start params using the source watermark.

    Delegates to _inject_incremental_params() from scheduler_service
    with the watermark as the last_run_at value.
    """
    from app.core.scheduler_service import _inject_incremental_params

    watermark = get_watermark(db, source.split(":")[0])
    return _inject_incremental_params(config, source.split(":")[0], watermark)
