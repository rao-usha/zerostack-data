"""
Source Health Scoring Service.

Computes a 0-100 composite health score for each data source based on
freshness, reliability, coverage, and consistency. Enables operational
dashboards and collection prioritization.
"""

import logging
import statistics
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select, distinct
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, SourceConfig, SourceWatermark

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FREQUENCY_HOURS = {
    "hourly": 1,
    "daily": 24,
    "weekly": 168,
    "monthly": 720,
    "manual": 720,  # manual sources — generous window
}
DEFAULT_EXPECTED_HOURS = 24  # fallback when no config exists

TIER_THRESHOLDS = [
    (80, "Healthy"),
    (60, "Warning"),
    (40, "Degraded"),
    (0, "Critical"),
]

WEIGHT_FRESHNESS = 0.40
WEIGHT_RELIABILITY = 0.30
WEIGHT_COVERAGE = 0.20
WEIGHT_CONSISTENCY = 0.10

RELIABILITY_WINDOW_DAYS = 7
MAX_RECENT_JOBS = 10


# ---------------------------------------------------------------------------
# Low-level scoring functions (exported for unit testing)
# ---------------------------------------------------------------------------

def _calculate_freshness_score(hours_since: float, expected_hours: float) -> float:
    """
    Freshness score: 100 when just collected, decays linearly to 0
    when hours_since reaches 2x the expected frequency.
    """
    if expected_hours <= 0:
        expected_hours = DEFAULT_EXPECTED_HOURS
    ratio = hours_since / expected_hours
    # At ratio 0 → 100, at ratio 2 → 0
    score = max(0.0, 100.0 - ratio * 50.0)
    return round(score, 1)


def _calculate_reliability_score(success_count: int, total_count: int) -> float:
    """Reliability = success / total over the window. 0 if no jobs."""
    if total_count == 0:
        return 0.0
    return round((success_count / total_count) * 100.0, 1)


def _calculate_coverage_score(row_counts: List[int], baseline: Optional[int] = None) -> float:
    """
    Coverage: how many rows are ingested vs an expected baseline.

    If no baseline is provided, we use the median of historical runs as
    the baseline and score the most recent run against it.
    """
    if not row_counts:
        return 0.0

    valid = [r for r in row_counts if r and r > 0]
    if not valid:
        return 0.0

    if baseline is None:
        if len(valid) < 2:
            return 100.0  # only one run, can't compare
        baseline = int(statistics.median(valid))

    if baseline <= 0:
        return 100.0

    latest = valid[0]  # most recent
    ratio = latest / baseline
    score = min(100.0, ratio * 100.0)
    return round(score, 1)


def _calculate_consistency_score(row_counts: List[int]) -> float:
    """
    Consistency: low coefficient of variation → high score.
    CV = stdev / mean.  Score = max(0, 100 - CV * 100).
    """
    valid = [r for r in row_counts if r is not None and r > 0]
    if len(valid) < 2:
        return 0.0 if len(valid) == 0 else 100.0

    mean = statistics.mean(valid)
    if mean == 0:
        return 0.0

    stdev = statistics.stdev(valid)
    cv = stdev / mean
    score = max(0.0, 100.0 - cv * 100.0)
    return round(score, 1)


def _score_to_tier(score: float) -> str:
    """Map a 0-100 score to a tier label."""
    for threshold, tier in TIER_THRESHOLDS:
        if score >= threshold:
            return tier
    return "Critical"


# ---------------------------------------------------------------------------
# Expected frequency helper
# ---------------------------------------------------------------------------

def _get_expected_hours(config) -> float:
    """Derive expected collection interval from SourceConfig."""
    if config and config.schedule_frequency:
        return FREQUENCY_HOURS.get(config.schedule_frequency, DEFAULT_EXPECTED_HOURS)
    return DEFAULT_EXPECTED_HOURS


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calculate_source_health(db: Session, source_key: str) -> Dict[str, Any]:
    """
    Calculate composite health score for a single source.

    Returns dict with: source, score (0-100), tier, components.
    """
    # Fetch watermark
    watermark = (
        db.query(SourceWatermark)
        .filter(SourceWatermark.source == source_key)
        .first()
    )

    # Fetch config
    config = (
        db.query(SourceConfig)
        .filter(SourceConfig.source == source_key)
        .first()
    )

    # Fetch recent jobs (last 7 days)
    cutoff = datetime.utcnow() - timedelta(days=RELIABILITY_WINDOW_DAYS)
    recent_jobs = (
        db.query(IngestionJob)
        .filter(IngestionJob.source == source_key)
        .filter(IngestionJob.created_at >= cutoff)
        .order_by(IngestionJob.created_at.desc())
        .limit(50)
        .all()
    )

    # No data at all → Critical
    if not watermark and not recent_jobs:
        return {
            "source": source_key,
            "score": 0,
            "tier": "Critical",
            "components": {
                "freshness": 0,
                "reliability": 0,
                "coverage": 0,
                "consistency": 0,
            },
        }

    expected_hours = _get_expected_hours(config)

    # --- Freshness ---
    if watermark and watermark.last_success_at:
        hours_since = (datetime.utcnow() - watermark.last_success_at).total_seconds() / 3600
    else:
        hours_since = expected_hours * 10  # very stale
    freshness = _calculate_freshness_score(hours_since, expected_hours)

    # --- Reliability ---
    total = len(recent_jobs)
    successes = sum(1 for j in recent_jobs if j.status == "success")
    reliability = _calculate_reliability_score(successes, total)

    # --- Coverage ---
    row_counts = [j.rows_inserted for j in recent_jobs if j.rows_inserted is not None]
    coverage = _calculate_coverage_score(row_counts)

    # --- Consistency ---
    consistency = _calculate_consistency_score(row_counts)

    # --- Composite ---
    composite = (
        freshness * WEIGHT_FRESHNESS
        + reliability * WEIGHT_RELIABILITY
        + coverage * WEIGHT_COVERAGE
        + consistency * WEIGHT_CONSISTENCY
    )
    score = round(composite)
    tier = _score_to_tier(score)

    logger.debug(
        "Health score for %s: %d (%s) — F=%.0f R=%.0f C=%.0f S=%.0f",
        source_key, score, tier, freshness, reliability, coverage, consistency,
    )

    return {
        "source": source_key,
        "score": score,
        "tier": tier,
        "components": {
            "freshness": freshness,
            "reliability": reliability,
            "coverage": coverage,
            "consistency": consistency,
        },
    }


def get_source_health_detail(db: Session, source_key: str) -> Dict[str, Any]:
    """
    Full health breakdown for a single source.

    Includes component scores, recent jobs, and recommendations.
    """
    health = calculate_source_health(db, source_key)

    # Fetch watermark for last_success_at
    watermark = (
        db.query(SourceWatermark)
        .filter(SourceWatermark.source == source_key)
        .first()
    )

    # Fetch config
    config = (
        db.query(SourceConfig)
        .filter(SourceConfig.source == source_key)
        .first()
    )

    # Recent jobs (last 10)
    recent_jobs_raw = (
        db.query(IngestionJob)
        .filter(IngestionJob.source == source_key)
        .order_by(IngestionJob.created_at.desc())
        .limit(MAX_RECENT_JOBS)
        .all()
    )

    recent_jobs = [
        {
            "id": j.id,
            "status": j.status,
            "created_at": j.created_at.isoformat() if j.created_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            "rows_inserted": j.rows_inserted,
            "error_message": j.error_message,
        }
        for j in recent_jobs_raw
    ]

    # Build recommendations
    recommendations = _build_recommendations(health, config)

    return {
        **health,
        "last_success_at": (
            watermark.last_success_at.isoformat()
            if watermark and watermark.last_success_at
            else None
        ),
        "expected_frequency": config.schedule_frequency if config else "unknown",
        "enabled": config.enabled if config else True,
        "recent_jobs": recent_jobs,
        "recommendations": recommendations,
    }


def get_all_source_health(db: Session) -> List[Dict[str, Any]]:
    """
    Health scores for all known sources, sorted worst-first.

    Discovers sources from IngestionJob + SourceWatermark + SourceConfig.
    """
    # Get distinct source keys across all tables
    stmt = select(distinct(IngestionJob.source))
    sources = db.execute(stmt).scalars().all()

    results = []
    for source_key in sources:
        try:
            health = calculate_source_health(db, source_key)
            results.append(health)
        except Exception as e:
            logger.warning("Failed to score source %s: %s", source_key, e)
            results.append({
                "source": source_key,
                "score": 0,
                "tier": "Critical",
                "components": {
                    "freshness": 0, "reliability": 0,
                    "coverage": 0, "consistency": 0,
                },
                "error": str(e),
            })

    # Sort worst-first
    results.sort(key=lambda r: r["score"])
    return results


def get_health_summary(db: Session) -> Dict[str, Any]:
    """
    Aggregate platform health summary.

    Returns overall score, tier counts, and list of critical sources.
    """
    all_health = get_all_source_health(db)

    if not all_health:
        return {
            "overall_score": 0,
            "total_sources": 0,
            "by_tier": {"Healthy": 0, "Warning": 0, "Degraded": 0, "Critical": 0},
            "critical_sources": [],
        }

    overall = round(sum(h["score"] for h in all_health) / len(all_health))

    by_tier = {"Healthy": 0, "Warning": 0, "Degraded": 0, "Critical": 0}
    critical = []
    for h in all_health:
        by_tier[h["tier"]] = by_tier.get(h["tier"], 0) + 1
        if h["tier"] == "Critical":
            critical.append(h["source"])

    return {
        "overall_score": overall,
        "total_sources": len(all_health),
        "by_tier": by_tier,
        "critical_sources": critical,
    }


# ---------------------------------------------------------------------------
# Recommendation engine (internal)
# ---------------------------------------------------------------------------

def _build_recommendations(health: Dict, config) -> List[str]:
    """Generate actionable recommendations based on component scores."""
    recs = []
    components = health.get("components", {})

    if components.get("freshness", 100) < 50:
        recs.append("Source data is stale — trigger a manual collection or check schedule.")

    if components.get("reliability", 100) < 60:
        recs.append(
            "High failure rate in recent jobs — check API keys, rate limits, "
            "and source availability."
        )

    if components.get("coverage", 100) < 50:
        recs.append(
            "Recent runs ingested fewer rows than baseline — investigate "
            "whether the source API changed or data was filtered."
        )

    if components.get("consistency", 100) < 40:
        recs.append(
            "Row counts vary significantly between runs — consider adding "
            "data validation or alerting on anomalous ingestion sizes."
        )

    if config and not config.enabled:
        recs.append("Source is disabled — re-enable if collection should resume.")

    if health.get("tier") == "Critical":
        recs.append("CRITICAL: Immediate attention required — source may be broken.")

    return recs
