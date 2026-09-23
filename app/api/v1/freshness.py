"""
Freshness dashboard — shows which data sources are stale vs fresh.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import (
    IngestionJob,
    IngestionSchedule,
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

    Covers every source that has a successful job, an active schedule, an SLA,
    or any job at all, and reports:
    - last_success_at (successful jobs only; None if it never succeeded)
    - age_hours
    - expected_cadence_hours (from DB SLA first, then schedule cadence)
    - is_stale / freshness label: ``fresh``, ``stale``, ``never_succeeded``,
      or ``unknown`` when there is no SLA and no schedule to judge against
      (SPEC_128: that used to be reported as ``fresh``).
    """
    from app.services.data_watchdog import (
        STALL_FACTOR,
        cron_cadence_hours,
        last_success_by_source,
    )

    now = datetime.utcnow()

    # 1. Last successful run per source: successful ingestion_jobs (any status
    #    case) plus linked job_queue successes for workers that do not write
    #    the IngestionJob back -- the same evidence the watchdog uses.
    last_success_map: dict[str, Optional[datetime]] = last_success_by_source(db)

    # 2. Expected cadence from active schedules (take the tightest per source)
    schedule_rows = (
        db.query(
            IngestionSchedule.source,
            IngestionSchedule.frequency,
            IngestionSchedule.cron_expression,
        )
        .filter(IngestionSchedule.is_active == 1)
        .all()
    )

    cadence_map: dict[str, float] = {}
    for row in schedule_rows:
        grace = None
        if row.frequency == ScheduleFrequency.CUSTOM and row.cron_expression:
            # A monthly cron is not stale after the 48h CUSTOM fallback
            cron_hours = cron_cadence_hours(row.cron_expression)
            if cron_hours:
                grace = cron_hours * STALL_FACTOR
        if grace is None:
            grace = CADENCE_GRACE_HOURS.get(row.frequency, DEFAULT_GRACE_HOURS)
        # Keep the tightest (smallest) grace if multiple schedules exist
        if row.source not in cadence_map or grace < cadence_map[row.source]:
            cadence_map[row.source] = grace

    # 3. Load DB SLAs — these override schedule-derived cadence
    sla_map = {r.source: r for r in db.query(SourceFreshnessSLA).all()}

    # 4. Every source that ever had a job, so never-succeeded ones are listed
    attempted = {r.source for r in db.query(IngestionJob.source).distinct().all()}

    all_sources = set(last_success_map) | set(cadence_map) | set(sla_map) | attempted

    # 5. Build per-source entries
    sources = []
    counts = {"fresh": 0, "stale": 0, "never_succeeded": 0, "unknown": 0}
    for source in all_sources:
        last_at: Optional[datetime] = last_success_map.get(source)

        # DB SLA overrides schedule-derived cadence
        sla = sla_map.get(source)
        expected = sla.max_age_hours if sla else cadence_map.get(source)
        sla_source = "db" if sla else ("schedule" if source in cadence_map else None)

        if last_at is None:
            age_hours = None
            is_stale = True
            freshness = "never_succeeded"
        else:
            age_hours = round((now - last_at).total_seconds() / 3600, 1)
            if expected:
                is_stale = age_hours > expected
                freshness = "stale" if is_stale else "fresh"
            else:
                # Nothing to judge against: say so instead of calling it fresh
                is_stale = False
                freshness = "unknown"

        counts[freshness] += 1
        sources.append(
            {
                "source": source,
                "last_success_at": last_at.isoformat() if last_at else None,
                "age_hours": age_hours,
                "expected_cadence_hours": expected,
                "sla_source": sla_source,
                "is_stale": is_stale,
                "freshness": freshness,
            }
        )

    # Sort stale-first (never-succeeded first of all), then by age descending
    sources.sort(key=lambda s: (
        not s["is_stale"],
        s["age_hours"] is not None,
        -(s["age_hours"] or 0),
        s["source"],
    ))

    return {
        "total_sources": len(sources),
        "stale_count": counts["stale"] + counts["never_succeeded"],
        "never_succeeded_count": counts["never_succeeded"],
        "fresh_count": counts["fresh"],
        "unknown_count": counts["unknown"],
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


# SLA violations are checked every 15 minutes by the data watchdog
# (app/services/data_watchdog.py, rule_sla), with dedupe and resolve notices.
# The old check_freshness_violations here had no caller and was removed.
