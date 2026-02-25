"""
Freshness dashboard — shows which data sources are stale vs fresh.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import (
    IngestionJob,
    IngestionSchedule,
    JobStatus,
    ScheduleFrequency,
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
    - expected_cadence_hours (from schedule, if any)
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

    # 3. Build per-source entries
    sources = []
    stale_count = 0
    for row in last_success_subq:
        source = row.source
        last_at: Optional[datetime] = row.last_success
        if last_at is None:
            continue

        age_hours = (now - last_at).total_seconds() / 3600
        expected = cadence_map.get(source)
        is_stale = (age_hours > expected) if expected else False

        sources.append(
            {
                "source": source,
                "last_success_at": last_at.isoformat(),
                "age_hours": round(age_hours, 1),
                "expected_cadence_hours": expected,
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
