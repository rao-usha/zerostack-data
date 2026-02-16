"""
Watermark service for incremental collection tracking.

Tracks last-collected timestamps per domain/source/state so collectors
can perform incremental updates instead of full re-syncs.
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import CollectionWatermark

logger = logging.getLogger(__name__)


def get_watermark(
    db: Session,
    domain: str,
    source: str,
    state: Optional[str] = None,
) -> Optional[datetime]:
    """
    Get the last-collected timestamp for a domain/source/state.

    Args:
        db: Database session
        domain: Site intel domain (e.g. "power")
        source: Data source (e.g. "eia")
        state: US state code or None for national

    Returns:
        Last collected datetime, or None if no watermark exists (= full sync)
    """
    row = db.query(CollectionWatermark).filter(
        CollectionWatermark.domain == domain,
        CollectionWatermark.source == source,
        CollectionWatermark.state == state,
    ).first()

    return row.last_collected_at if row else None


def update_watermark(
    db: Session,
    domain: str,
    source: str,
    last_collected_at: datetime,
    job_id: Optional[int] = None,
    records: Optional[int] = None,
    state: Optional[str] = None,
) -> CollectionWatermark:
    """
    Create or update a watermark for a domain/source/state.

    Args:
        db: Database session
        domain: Site intel domain
        source: Data source
        last_collected_at: Timestamp of last successful collection
        job_id: The collection job ID
        records: Number of records collected
        state: US state code or None for national

    Returns:
        The watermark row
    """
    row = db.query(CollectionWatermark).filter(
        CollectionWatermark.domain == domain,
        CollectionWatermark.source == source,
        CollectionWatermark.state == state,
    ).first()

    if row is None:
        row = CollectionWatermark(
            domain=domain,
            source=source,
            state=state,
            last_collected_at=last_collected_at,
            last_job_id=job_id,
            records_collected=records,
        )
        db.add(row)
    else:
        row.last_collected_at = last_collected_at
        row.last_job_id = job_id
        if records is not None:
            row.records_collected = records
        row.updated_at = datetime.utcnow()

    db.commit()
    db.refresh(row)

    logger.debug(f"Updated watermark {domain}/{source}/{state or 'national'} -> {last_collected_at}")
    return row


def get_all_watermarks(
    db: Session,
    domain: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get all watermarks, optionally filtered by domain.

    Returns:
        List of watermark dicts
    """
    query = db.query(CollectionWatermark)
    if domain:
        query = query.filter(CollectionWatermark.domain == domain)

    rows = query.order_by(
        CollectionWatermark.domain,
        CollectionWatermark.source,
    ).all()

    return [
        {
            "id": row.id,
            "domain": row.domain,
            "source": row.source,
            "state": row.state,
            "last_collected_at": row.last_collected_at.isoformat() if row.last_collected_at else None,
            "last_job_id": row.last_job_id,
            "records_collected": row.records_collected,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
        for row in rows
    ]


def clear_watermark(
    db: Session,
    domain: str,
    source: str,
    state: Optional[str] = None,
) -> bool:
    """
    Clear a watermark to force a full re-sync.

    Returns:
        True if cleared, False if not found
    """
    row = db.query(CollectionWatermark).filter(
        CollectionWatermark.domain == domain,
        CollectionWatermark.source == source,
        CollectionWatermark.state == state,
    ).first()

    if row is None:
        return False

    db.delete(row)
    db.commit()
    logger.info(f"Cleared watermark {domain}/{source}/{state or 'national'} — next run will be full sync")
    return True
