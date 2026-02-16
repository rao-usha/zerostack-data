"""
Collection Audit Trail API.

Query endpoints for the collection audit log.
"""
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core import audit_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/audit-trail", tags=["Audit Trail"])


@router.get("")
async def get_audit_trail(
    source: Optional[str] = Query(None, description="Filter by source"),
    domain: Optional[str] = Query(None, description="Filter by domain"),
    trigger_type: Optional[str] = Query(None, description="Filter by trigger type (api, schedule, retry)"),
    since: Optional[str] = Query(None, description="ISO datetime to filter entries after"),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Query the collection audit trail.

    Returns recent collection triggers with optional filters.
    """
    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            since_dt = None

    entries = audit_service.get_audit_trail(
        db,
        source=source,
        domain=domain,
        trigger_type=trigger_type,
        limit=limit,
        since=since_dt,
    )

    return {
        "entries": entries,
        "total": len(entries),
        "filters": {
            "source": source,
            "domain": domain,
            "trigger_type": trigger_type,
            "since": since,
            "limit": limit,
        },
    }


@router.get("/summary")
async def get_audit_summary(db: Session = Depends(get_db)):
    """
    Get audit trail summary.

    Counts by trigger_type and source for last 24h, 7d, and 30d.
    """
    return audit_service.get_audit_summary(db)
