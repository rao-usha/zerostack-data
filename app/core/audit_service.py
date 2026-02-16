"""
Collection audit trail service.

Logs every collection trigger (API, schedule, retry) for observability.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.models import CollectionAuditLog

logger = logging.getLogger(__name__)


def log_collection(
    db: Session,
    trigger_type: str,
    source: str,
    job_id: Optional[int] = None,
    domain: Optional[str] = None,
    job_type: Optional[str] = None,
    trigger_source: Optional[str] = None,
    config_snapshot: Optional[Dict[str, Any]] = None,
) -> CollectionAuditLog:
    """
    Create an audit trail entry for a collection trigger.

    Args:
        db: Database session
        trigger_type: "api", "schedule", or "retry"
        source: Data source identifier
        job_id: Associated job ID
        domain: Site intel domain (if applicable)
        job_type: "ingestion" or "site_intel"
        trigger_source: Endpoint path or schedule_id
        config_snapshot: Collection config at time of trigger

    Returns:
        Created audit log entry
    """
    entry = CollectionAuditLog(
        trigger_type=trigger_type,
        trigger_source=trigger_source,
        domain=domain,
        source=source,
        job_id=job_id,
        job_type=job_type,
        config_snapshot=config_snapshot,
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)

    logger.debug(f"Audit: {trigger_type} trigger for {source} (job_id={job_id})")
    return entry


def get_audit_trail(
    db: Session,
    source: Optional[str] = None,
    domain: Optional[str] = None,
    trigger_type: Optional[str] = None,
    limit: int = 50,
    since: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Query the audit trail with optional filters.

    Args:
        db: Database session
        source: Filter by source
        domain: Filter by domain
        trigger_type: Filter by trigger type
        limit: Maximum results
        since: Only entries after this timestamp

    Returns:
        List of audit log entries
    """
    query = db.query(CollectionAuditLog)

    if source:
        query = query.filter(CollectionAuditLog.source == source)
    if domain:
        query = query.filter(CollectionAuditLog.domain == domain)
    if trigger_type:
        query = query.filter(CollectionAuditLog.trigger_type == trigger_type)
    if since:
        query = query.filter(CollectionAuditLog.created_at >= since)

    rows = query.order_by(CollectionAuditLog.created_at.desc()).limit(limit).all()

    return [
        {
            "id": row.id,
            "trigger_type": row.trigger_type,
            "trigger_source": row.trigger_source,
            "domain": row.domain,
            "source": row.source,
            "job_id": row.job_id,
            "job_type": row.job_type,
            "config_snapshot": row.config_snapshot,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def get_audit_summary(db: Session) -> Dict[str, Any]:
    """
    Get audit trail summary: counts by trigger_type and source for 24h/7d/30d.

    Returns:
        Summary dict with period breakdowns
    """
    now = datetime.utcnow()
    periods = {
        "last_24h": now - timedelta(hours=24),
        "last_7d": now - timedelta(days=7),
        "last_30d": now - timedelta(days=30),
    }

    summary = {}
    for period_name, cutoff in periods.items():
        # Count by trigger type
        by_trigger = dict(
            db.query(
                CollectionAuditLog.trigger_type,
                func.count(CollectionAuditLog.id),
            )
            .filter(CollectionAuditLog.created_at >= cutoff)
            .group_by(CollectionAuditLog.trigger_type)
            .all()
        )

        # Count by source
        by_source = dict(
            db.query(
                CollectionAuditLog.source,
                func.count(CollectionAuditLog.id),
            )
            .filter(CollectionAuditLog.created_at >= cutoff)
            .group_by(CollectionAuditLog.source)
            .all()
        )

        total = sum(by_trigger.values())

        summary[period_name] = {
            "total": total,
            "by_trigger_type": by_trigger,
            "by_source": by_source,
        }

    return summary
