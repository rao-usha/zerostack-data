"""
Freshness dashboard — shows which data sources are stale vs fresh.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import (
    IngestionJob,
    IngestionSchedule,
    JobStatus,
    ScheduleFrequency,
    SourceFreshnessSLA,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/datasets", tags=["freshness"])

# Grace periods per cadence (hours).
# Slightly wider than the raw frequency so a single delayed run
# doesn't immediately flag stale.
CADENCE_GRACE_HOURS = {
    ScheduleFrequency.HOURLY: 2,
    ScheduleFrequency.DAILY: 36,
    ScheduleFrequency.WEEKLY: 8 * 24,  # 8 days
    ScheduleFrequency.MONTHLY: 32 * 24,  # 32 days
    ScheduleFrequency.QUARTERLY: 95 * 24,  # 95 days
}

DEFAULT_GRACE_HOURS = 48  # fallback for CUSTOM or unknown


@router.get("/freshness")
def get_freshness_dashboard(db: Session = Depends(get_db)):
    """
    Return per-source freshness status.

    For every source that has at least one successful job, reports:
    - last_success_at
    - age_hours
    - expected_cadence_hours (from DB SLA first, then schedule cadence)
    - is_stale / freshness label
    """
    now = datetime.utcnow()

    # 1. Last successful job per source (case-insensitive status match
    #    because some jobs were written with uppercase status values)
    last_success_subq = (
        db.query(
            IngestionJob.source,
            func.max(IngestionJob.completed_at).label("last_success"),
        )
        .filter(func.lower(IngestionJob.status) == "success")
        .group_by(IngestionJob.source)
        .all()
    )

    # 2. Expected cadence from active schedules (take the tightest per source)
    schedule_rows = (
        db.query(
            IngestionSchedule.source,
            IngestionSchedule.frequency,
        )
        .filter(IngestionSchedule.is_active == 1)
        .all()
    )

    cadence_map: dict[str, float] = {}
    for row in schedule_rows:
        grace = CADENCE_GRACE_HOURS.get(row.frequency, DEFAULT_GRACE_HOURS)
        # Keep the tightest (smallest) grace if multiple schedules exist
        if row.source not in cadence_map or grace < cadence_map[row.source]:
            cadence_map[row.source] = grace

    # 3. Load DB SLAs — these override schedule-derived cadence
    sla_map = {r.source: r for r in db.query(SourceFreshnessSLA).all()}

    # 4. Build per-source entries
    sources = []
    stale_count = 0
    for row in last_success_subq:
        source = row.source
        last_at: Optional[datetime] = row.last_success
        if last_at is None:
            continue

        age_hours = (now - last_at).total_seconds() / 3600

        # DB SLA overrides schedule-derived cadence
        sla = sla_map.get(source)
        expected = sla.max_age_hours if sla else cadence_map.get(source)
        sla_source = "db" if sla else ("schedule" if source in cadence_map else None)

        is_stale = (age_hours > expected) if expected else False

        sources.append(
            {
                "source": source,
                "last_success_at": last_at.isoformat(),
                "age_hours": round(age_hours, 1),
                "expected_cadence_hours": expected,
                "sla_source": sla_source,
                "is_stale": is_stale,
                "freshness": "stale" if is_stale else "fresh",
            }
        )

        if is_stale:
            stale_count += 1

    # Sort stale-first, then by age descending
    sources.sort(key=lambda s: (not s["is_stale"], -s["age_hours"]))

    return {
        "total_sources": len(sources),
        "stale_count": stale_count,
        "fresh_count": len(sources) - stale_count,
        "sources": sources,
    }


# =============================================================================
# Freshness SLA CRUD
# =============================================================================


class FreshnessSLARequest(BaseModel):
    max_age_hours: float
    alert_on_violation: bool = True
    description: Optional[str] = None


@router.get("/freshness/sla")
def list_freshness_slas(db: Session = Depends(get_db)):
    """List all configured freshness SLAs."""
    slas = db.query(SourceFreshnessSLA).order_by(SourceFreshnessSLA.source).all()
    return [
        {
            "source": s.source,
            "max_age_hours": s.max_age_hours,
            "alert_on_violation": bool(s.alert_on_violation),
            "description": s.description,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "updated_at": s.updated_at.isoformat() if s.updated_at else None,
        }
        for s in slas
    ]


@router.put("/freshness/sla/{source}")
def upsert_freshness_sla(
    source: str,
    body: FreshnessSLARequest,
    db: Session = Depends(get_db),
):
    """Create or update a freshness SLA for a source."""
    sla = (
        db.query(SourceFreshnessSLA)
        .filter(SourceFreshnessSLA.source == source)
        .first()
    )

    if sla:
        sla.max_age_hours = body.max_age_hours
        sla.alert_on_violation = 1 if body.alert_on_violation else 0
        sla.description = body.description
        sla.updated_at = datetime.utcnow()
    else:
        sla = SourceFreshnessSLA(
            source=source,
            max_age_hours=body.max_age_hours,
            alert_on_violation=1 if body.alert_on_violation else 0,
            description=body.description,
        )
        db.add(sla)

    db.commit()
    db.refresh(sla)

    return {
        "source": sla.source,
        "max_age_hours": sla.max_age_hours,
        "alert_on_violation": bool(sla.alert_on_violation),
        "description": sla.description,
    }


@router.delete("/freshness/sla/{source}")
def delete_freshness_sla(source: str, db: Session = Depends(get_db)):
    """Delete a freshness SLA (source falls back to schedule cadence)."""
    sla = (
        db.query(SourceFreshnessSLA)
        .filter(SourceFreshnessSLA.source == source)
        .first()
    )
    if not sla:
        raise HTTPException(status_code=404, detail=f"No SLA configured for '{source}'")

    db.delete(sla)
    db.commit()
    return {"deleted": source}


# =============================================================================
# Freshness violation checker (for periodic / webhook use)
# =============================================================================


async def check_freshness_violations(db: Session):
    """
    Check all sources with DB SLAs for freshness violations.

    Sends ALERT_DATA_STALENESS webhook for each violating source
    that has alert_on_violation enabled.
    """
    from app.core.webhook_service import trigger_webhooks
    from app.core.models import WebhookEventType

    now = datetime.utcnow()

    slas = db.query(SourceFreshnessSLA).filter(
        SourceFreshnessSLA.alert_on_violation == 1
    ).all()

    if not slas:
        return {"violations": 0}

    # Get last success per source
    last_success_rows = (
        db.query(
            IngestionJob.source,
            func.max(IngestionJob.completed_at).label("last_success"),
        )
        .filter(func.lower(IngestionJob.status) == "success")
        .group_by(IngestionJob.source)
        .all()
    )
    last_success_map = {r.source: r.last_success for r in last_success_rows}

    violations = []
    for sla in slas:
        last_at = last_success_map.get(sla.source)
        if last_at is None:
            continue

        age_hours = (now - last_at).total_seconds() / 3600
        if age_hours > sla.max_age_hours:
            violations.append({
                "source": sla.source,
                "age_hours": round(age_hours, 1),
                "max_age_hours": sla.max_age_hours,
            })

            try:
                await trigger_webhooks(
                    event_type=WebhookEventType.ALERT_DATA_STALENESS,
                    event_data={
                        "source": sla.source,
                        "age_hours": round(age_hours, 1),
                        "sla_max_age_hours": sla.max_age_hours,
                        "message": (
                            f"Source '{sla.source}' is {round(age_hours, 1)}h old "
                            f"(SLA: {sla.max_age_hours}h)"
                        ),
                    },
                    source=sla.source,
                )
            except Exception as e:
                logger.error(f"Freshness violation webhook error for {sla.source}: {e}")

    return {"violations": len(violations), "details": violations}
