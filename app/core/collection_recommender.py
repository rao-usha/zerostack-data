"""
Collection Recommendations Engine.

Analyzes source health scores, freshness gaps, and failure patterns
to generate prioritized collection recommendations. Enables smart
scheduling and proactive data freshness management.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import func, select, distinct, and_
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, SourceConfig, SourceWatermark
from app.core.source_health import (
    calculate_source_health,
    get_all_source_health,
    _get_expected_hours,
    RELIABILITY_WINDOW_DAYS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ACTION_COLLECT_NOW = "collect_now"
ACTION_SCHEDULE_RETRY = "schedule_retry"
ACTION_INVESTIGATE = "investigate"
ACTION_DISABLE = "disable"

# Thresholds for recommendation triggers
STALE_MULTIPLIER = 1.5       # trigger collect_now when age > 1.5x expected
FAILURE_STREAK_THRESHOLD = 3  # investigate after 3 consecutive failures
FAILURE_RATE_THRESHOLD = 0.5  # investigate if >50% failures in window
DISABLE_FAILURE_STREAK = 10   # suggest disable after 10 consecutive failures


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_recommendations(db: Session) -> List[Dict[str, Any]]:
    """
    Generate prioritized collection recommendations for all sources.

    Returns a list of recommendation dicts sorted by priority (1=highest).
    Each recommendation has: source, priority, action, reason, health_score.
    """
    all_health = get_all_source_health(db)
    recommendations = []

    for health in all_health:
        source_key = health["source"]

        # Fetch config to check enabled status and expected frequency
        config = (
            db.query(SourceConfig)
            .filter(SourceConfig.source == source_key)
            .first()
        )

        # Skip disabled sources (but note it)
        if config and not config.enabled:
            continue

        # Fetch watermark
        watermark = (
            db.query(SourceWatermark)
            .filter(SourceWatermark.source == source_key)
            .first()
        )

        expected_hours = _get_expected_hours(config)

        # Calculate staleness
        if watermark and watermark.last_success_at:
            hours_since = (datetime.utcnow() - watermark.last_success_at).total_seconds() / 3600
        else:
            hours_since = expected_hours * 10  # never succeeded

        # Check recent failure streak
        recent_jobs = (
            db.query(IngestionJob)
            .filter(IngestionJob.source == source_key)
            .order_by(IngestionJob.created_at.desc())
            .limit(DISABLE_FAILURE_STREAK)
            .all()
        )

        consecutive_failures = 0
        for j in recent_jobs:
            if j.status == "failed":
                consecutive_failures += 1
            else:
                break

        # Calculate failure rate in window
        cutoff = datetime.utcnow() - timedelta(days=RELIABILITY_WINDOW_DAYS)
        window_jobs = [j for j in recent_jobs if j.created_at and j.created_at >= cutoff]
        total_in_window = len(window_jobs)
        failures_in_window = sum(1 for j in window_jobs if j.status == "failed")
        failure_rate = failures_in_window / total_in_window if total_in_window > 0 else 0

        # --- Generate recommendations based on signals ---

        # Signal 1: Suggest disable for extremely broken sources
        if consecutive_failures >= DISABLE_FAILURE_STREAK:
            recommendations.append({
                "source": source_key,
                "priority": 2,
                "action": ACTION_DISABLE,
                "reason": (
                    f"{consecutive_failures} consecutive failures — "
                    f"consider disabling until root cause is fixed."
                ),
                "health_score": health["score"],
            })
            continue  # don't add lower-priority recs for same source

        # Signal 2: Investigation needed for persistent failures
        if (consecutive_failures >= FAILURE_STREAK_THRESHOLD
                or failure_rate >= FAILURE_RATE_THRESHOLD):
            recommendations.append({
                "source": source_key,
                "priority": 3,
                "action": ACTION_INVESTIGATE,
                "reason": (
                    f"Failure streak: {consecutive_failures}, "
                    f"window failure rate: {failure_rate:.0%} — "
                    f"check API keys, rate limits, source availability."
                ),
                "health_score": health["score"],
            })
            continue

        # Signal 3: Stale data needs immediate collection
        if hours_since >= expected_hours * STALE_MULTIPLIER:
            priority = 1 if hours_since >= expected_hours * 3 else 4
            recommendations.append({
                "source": source_key,
                "priority": priority,
                "action": ACTION_COLLECT_NOW,
                "reason": (
                    f"Data is {hours_since:.0f}h old "
                    f"(expected every {expected_hours:.0f}h)."
                ),
                "health_score": health["score"],
            })
            continue

        # Signal 4: Approaching staleness — schedule a retry
        if hours_since >= expected_hours * 0.8:
            recommendations.append({
                "source": source_key,
                "priority": 7,
                "action": ACTION_SCHEDULE_RETRY,
                "reason": (
                    f"Data will be stale soon ({hours_since:.0f}h old, "
                    f"threshold {expected_hours:.0f}h)."
                ),
                "health_score": health["score"],
            })

    # Sort by priority (lowest number = highest priority)
    recommendations.sort(key=lambda r: (r["priority"], -r.get("health_score", 0)))
    return recommendations


def get_optimal_collection_plan(
    db: Session,
    max_concurrent: int = 4,
) -> Dict[str, Any]:
    """
    Generate a batched collection plan respecting concurrency limits.

    Groups collect_now recommendations into waves that can run in parallel.
    """
    recs = generate_recommendations(db)

    # Filter to actionable recommendations (collect_now and schedule_retry)
    actionable = [
        r for r in recs
        if r["action"] in (ACTION_COLLECT_NOW, ACTION_SCHEDULE_RETRY)
    ]

    # Build waves
    waves = []
    current_wave = []
    for rec in actionable:
        current_wave.append(rec["source"])
        if len(current_wave) >= max_concurrent:
            waves.append(current_wave)
            current_wave = []
    if current_wave:
        waves.append(current_wave)

    # Separate investigation/disable items
    needs_attention = [
        r for r in recs
        if r["action"] in (ACTION_INVESTIGATE, ACTION_DISABLE)
    ]

    return {
        "waves": waves,
        "total_sources": len(actionable),
        "wave_count": len(waves),
        "max_concurrent": max_concurrent,
        "needs_attention": needs_attention,
    }


def get_collection_history_stats(
    db: Session,
    source_key: str,
    days: int = 30,
) -> Dict[str, Any]:
    """
    Historical collection statistics for a single source.

    Returns success rate, average rows, run count, and timing stats
    over the specified window.
    """
    cutoff = datetime.utcnow() - timedelta(days=days)

    jobs = (
        db.query(IngestionJob)
        .filter(
            IngestionJob.source == source_key,
            IngestionJob.created_at >= cutoff,
        )
        .order_by(IngestionJob.created_at.desc())
        .all()
    )

    if not jobs:
        return {
            "source": source_key,
            "period_days": days,
            "total_runs": 0,
            "success_count": 0,
            "failure_count": 0,
            "success_rate": 0.0,
            "avg_rows_per_run": 0,
            "total_rows_ingested": 0,
            "avg_duration_seconds": None,
            "last_success_at": None,
            "last_failure_at": None,
        }

    successes = [j for j in jobs if j.status == "success"]
    failures = [j for j in jobs if j.status == "failed"]

    # Row stats (from successful jobs only)
    row_counts = [
        j.rows_inserted for j in successes
        if j.rows_inserted is not None and j.rows_inserted > 0
    ]
    total_rows = sum(row_counts)
    avg_rows = round(total_rows / len(row_counts)) if row_counts else 0

    # Duration stats
    durations = []
    for j in successes:
        if j.completed_at and j.created_at:
            delta = (j.completed_at - j.created_at).total_seconds()
            if delta > 0:
                durations.append(delta)
    avg_duration = round(sum(durations) / len(durations), 1) if durations else None

    # Last success/failure timestamps
    last_success = successes[0].completed_at if successes else None
    last_failure = failures[0].created_at if failures else None

    return {
        "source": source_key,
        "period_days": days,
        "total_runs": len(jobs),
        "success_count": len(successes),
        "failure_count": len(failures),
        "success_rate": round(len(successes) / len(jobs) * 100, 1) if jobs else 0.0,
        "avg_rows_per_run": avg_rows,
        "total_rows_ingested": total_rows,
        "avg_duration_seconds": avg_duration,
        "last_success_at": last_success.isoformat() if last_success else None,
        "last_failure_at": last_failure.isoformat() if last_failure else None,
    }
