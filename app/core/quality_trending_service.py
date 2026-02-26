"""
Quality Trending Service.

Tracks quality scores over time, computes daily snapshots, checks SLA
compliance, and detects sustained quality degradation.
"""

import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.models import (
    DataProfileSnapshot,
    DataQualityResult,
    DQAnomalyAlert,
    DQCrossSourceResult,
    DQQualitySnapshot,
    DQSLATarget,
    AnomalyAlertType,
    AnomalyAlertStatus,
    RuleSeverity,
    DatasetRegistry,
    IngestionJob,
    JobStatus,
)

logger = logging.getLogger(__name__)

# Composite quality score weights
WEIGHT_COMPLETENESS = 0.30
WEIGHT_FRESHNESS = 0.20
WEIGHT_VALIDITY = 0.30
WEIGHT_CONSISTENCY = 0.20


# =============================================================================
# Score computation helpers
# =============================================================================

def _compute_completeness_score(db: Session, table_name: str) -> Optional[float]:
    """Get completeness from the latest profile snapshot."""
    snapshot = (
        db.query(DataProfileSnapshot)
        .filter(DataProfileSnapshot.table_name == table_name)
        .order_by(DataProfileSnapshot.profiled_at.desc())
        .first()
    )
    if snapshot and snapshot.overall_completeness_pct is not None:
        return snapshot.overall_completeness_pct
    return None


def _compute_freshness_score(db: Session, source: str) -> Optional[float]:
    """
    Compute freshness score based on most recent successful job.

    100 = updated today, 90 = within 1 day, decays linearly.
    """
    job = (
        db.query(IngestionJob)
        .filter(
            IngestionJob.source == source,
            IngestionJob.status == JobStatus.SUCCESS,
        )
        .order_by(IngestionJob.completed_at.desc())
        .first()
    )
    if not job or not job.completed_at:
        return 0.0

    age_hours = (datetime.utcnow() - job.completed_at).total_seconds() / 3600
    # Decay: 100 at 0h, 90 at 24h, 50 at 72h, 0 at 168h (1 week)
    score = max(0.0, 100.0 - (age_hours / 168.0 * 100.0))
    return round(score, 1)


def _compute_validity_score(db: Session, source: str, table_name: str) -> Optional[float]:
    """
    Compute validity score from recent rule evaluation results.

    Based on pass rate of data quality rules.
    """
    # Get results from last 7 days
    cutoff = datetime.utcnow() - timedelta(days=7)
    results = (
        db.query(DataQualityResult)
        .filter(
            DataQualityResult.source == source,
            DataQualityResult.evaluated_at >= cutoff,
        )
        .all()
    )

    if not results:
        return None

    passed = sum(1 for r in results if r.passed)
    total = len(results)
    return round((passed / total) * 100, 1) if total > 0 else None


def _compute_consistency_score(db: Session, source: str) -> Optional[float]:
    """
    Compute consistency score from cross-source validation results.

    Average match rate of recent cross-source validations involving this source.
    """
    # Find validations involving this source (check config JSON)
    from app.core.models import DQCrossSourceValidation

    validations = db.query(DQCrossSourceValidation).filter(
        DQCrossSourceValidation.is_enabled == 1
    ).all()

    relevant_match_rates = []
    for v in validations:
        config = v.config or {}
        left_source = config.get("left", {}).get("source", "")
        right_source = config.get("right", {}).get("source", "")

        if source in (left_source, right_source):
            # Get latest result
            result = (
                db.query(DQCrossSourceResult)
                .filter(DQCrossSourceResult.validation_id == v.id)
                .order_by(DQCrossSourceResult.evaluated_at.desc())
                .first()
            )
            if result and result.match_rate is not None:
                relevant_match_rates.append(result.match_rate * 100)

    if not relevant_match_rates:
        return None

    return round(sum(relevant_match_rates) / len(relevant_match_rates), 1)


# =============================================================================
# Daily snapshot computation
# =============================================================================

def compute_daily_snapshots(db: Session) -> List[DQQualitySnapshot]:
    """
    Compute daily quality scores for all sources/tables in dataset_registry.

    Composite score = 30% completeness + 20% freshness + 30% validity + 20% consistency.
    """
    today = date.today()
    registries = db.query(DatasetRegistry).all()
    snapshots = []

    for registry in registries:
        try:
            source = registry.source
            table_name = registry.table_name

            completeness = _compute_completeness_score(db, table_name)
            freshness = _compute_freshness_score(db, source)
            validity = _compute_validity_score(db, source, table_name)
            consistency = _compute_consistency_score(db, source)

            # Count open anomalies
            anomaly_count = (
                db.query(func.count(DQAnomalyAlert.id))
                .filter(
                    DQAnomalyAlert.table_name == table_name,
                    DQAnomalyAlert.status == AnomalyAlertStatus.OPEN,
                )
                .scalar()
            ) or 0

            # Rule pass rate
            rule_pass_rate = None
            if validity is not None:
                rule_pass_rate = validity

            # Row count from latest profile
            latest_profile = (
                db.query(DataProfileSnapshot)
                .filter(DataProfileSnapshot.table_name == table_name)
                .order_by(DataProfileSnapshot.profiled_at.desc())
                .first()
            )
            row_count = latest_profile.row_count if latest_profile else None

            # Composite score (use available components)
            components = []
            if completeness is not None:
                components.append(("completeness", completeness, WEIGHT_COMPLETENESS))
            if freshness is not None:
                components.append(("freshness", freshness, WEIGHT_FRESHNESS))
            if validity is not None:
                components.append(("validity", validity, WEIGHT_VALIDITY))
            if consistency is not None:
                components.append(("consistency", consistency, WEIGHT_CONSISTENCY))

            if components:
                # Re-normalize weights based on available components
                total_weight = sum(w for _, _, w in components)
                quality_score = sum(
                    (score * weight / total_weight) for _, score, weight in components
                )
                quality_score = round(quality_score, 1)
            else:
                quality_score = None

            # Upsert snapshot
            existing = (
                db.query(DQQualitySnapshot)
                .filter(
                    DQQualitySnapshot.snapshot_date == today,
                    DQQualitySnapshot.source == source,
                    DQQualitySnapshot.table_name == table_name,
                )
                .first()
            )

            if existing:
                existing.quality_score = quality_score
                existing.completeness_score = completeness
                existing.freshness_score = freshness
                existing.validity_score = validity
                existing.consistency_score = consistency
                existing.row_count = row_count
                existing.rule_pass_rate = rule_pass_rate
                existing.anomaly_count = anomaly_count
                snapshot = existing
            else:
                snapshot = DQQualitySnapshot(
                    snapshot_date=today,
                    source=source,
                    table_name=table_name,
                    domain=getattr(registry, "domain", None),
                    quality_score=quality_score,
                    completeness_score=completeness,
                    freshness_score=freshness,
                    validity_score=validity,
                    consistency_score=consistency,
                    row_count=row_count,
                    rule_pass_rate=rule_pass_rate,
                    anomaly_count=anomaly_count,
                )
                db.add(snapshot)

            snapshots.append(snapshot)

        except Exception as e:
            logger.error(f"Error computing snapshot for {registry.table_name}: {e}")
            continue

    db.commit()
    logger.info(f"Computed {len(snapshots)} daily quality snapshots")
    return snapshots


# =============================================================================
# Trend queries
# =============================================================================

def get_trend(
    db: Session,
    source: Optional[str] = None,
    table_name: Optional[str] = None,
    window_days: int = 30,
) -> List[DQQualitySnapshot]:
    """Get quality score trend data for a source/table over a time window."""
    cutoff = date.today() - timedelta(days=window_days)
    query = db.query(DQQualitySnapshot).filter(
        DQQualitySnapshot.snapshot_date >= cutoff
    )

    if source:
        query = query.filter(DQQualitySnapshot.source == source)
    if table_name:
        query = query.filter(DQQualitySnapshot.table_name == table_name)

    return query.order_by(DQQualitySnapshot.snapshot_date.asc()).all()


def get_weekly_aggregation(
    db: Session,
    source: Optional[str] = None,
    weeks: int = 12,
) -> List[Dict[str, Any]]:
    """Get weekly rollup of quality scores."""
    cutoff = date.today() - timedelta(weeks=weeks)

    query = db.query(
        func.date_trunc("week", DQQualitySnapshot.snapshot_date).label("week"),
        func.avg(DQQualitySnapshot.quality_score).label("avg_quality"),
        func.avg(DQQualitySnapshot.completeness_score).label("avg_completeness"),
        func.avg(DQQualitySnapshot.freshness_score).label("avg_freshness"),
        func.avg(DQQualitySnapshot.validity_score).label("avg_validity"),
        func.avg(DQQualitySnapshot.consistency_score).label("avg_consistency"),
        func.count(DQQualitySnapshot.id).label("snapshot_count"),
    ).filter(
        DQQualitySnapshot.snapshot_date >= cutoff
    ).group_by(
        func.date_trunc("week", DQQualitySnapshot.snapshot_date)
    ).order_by(
        func.date_trunc("week", DQQualitySnapshot.snapshot_date).asc()
    )

    if source:
        query = query.filter(DQQualitySnapshot.source == source)

    rows = query.all()
    return [
        {
            "week": str(r.week.date()) if r.week else None,
            "avg_quality": round(r.avg_quality, 1) if r.avg_quality else None,
            "avg_completeness": round(r.avg_completeness, 1) if r.avg_completeness else None,
            "avg_freshness": round(r.avg_freshness, 1) if r.avg_freshness else None,
            "avg_validity": round(r.avg_validity, 1) if r.avg_validity else None,
            "avg_consistency": round(r.avg_consistency, 1) if r.avg_consistency else None,
            "snapshot_count": r.snapshot_count,
        }
        for r in rows
    ]


# =============================================================================
# SLA compliance
# =============================================================================

def check_sla_compliance(db: Session) -> List[Dict[str, Any]]:
    """
    Compare current quality scores against SLA targets.

    Returns a list of compliance results with pass/fail per dimension.
    """
    targets = (
        db.query(DQSLATarget)
        .filter(DQSLATarget.is_enabled == 1)
        .all()
    )

    results = []
    today = date.today()

    for target in targets:
        # Find matching snapshots
        query = db.query(DQQualitySnapshot).filter(
            DQQualitySnapshot.snapshot_date == today
        )

        if target.source:
            query = query.filter(DQQualitySnapshot.source == target.source)

        if target.table_pattern:
            # Get all and filter by regex
            snapshots = query.all()
            snapshots = [
                s for s in snapshots
                if s.table_name and re.search(target.table_pattern, s.table_name)
            ]
        else:
            snapshots = query.all()

        for snap in snapshots:
            compliance = {
                "source": snap.source,
                "table_name": snap.table_name,
                "sla_target_id": target.id,
                "quality": {
                    "current": snap.quality_score,
                    "target": target.target_quality_score,
                    "met": (snap.quality_score or 0) >= target.target_quality_score,
                },
                "completeness": {
                    "current": snap.completeness_score,
                    "target": target.target_completeness,
                    "met": (snap.completeness_score or 0) >= target.target_completeness,
                },
                "freshness": {
                    "current": snap.freshness_score,
                    "target": target.target_freshness,
                    "met": (snap.freshness_score or 0) >= target.target_freshness,
                },
                "validity": {
                    "current": snap.validity_score,
                    "target": target.target_validity,
                    "met": (snap.validity_score or 0) >= target.target_validity,
                },
            }
            compliance["overall_met"] = all(
                compliance[dim]["met"]
                for dim in ["quality", "completeness", "freshness", "validity"]
            )
            results.append(compliance)

    return results


# =============================================================================
# Degradation detection
# =============================================================================

def check_degradation_alerts(db: Session) -> List[DQAnomalyAlert]:
    """
    Check for sustained quality degradation (N consecutive drops).

    Uses SLA target config for consecutive_drops_threshold.
    Generates AnomalyAlert with type QUALITY_DEGRADATION.
    """
    targets = (
        db.query(DQSLATarget)
        .filter(DQSLATarget.is_enabled == 1)
        .all()
    )

    alerts = []
    today = date.today()

    for target in targets:
        threshold = target.consecutive_drops_threshold or 3

        # Get recent snapshots grouped by source/table
        query = db.query(DQQualitySnapshot).filter(
            DQQualitySnapshot.snapshot_date >= today - timedelta(days=threshold + 1)
        ).order_by(DQQualitySnapshot.snapshot_date.desc())

        if target.source:
            query = query.filter(DQQualitySnapshot.source == target.source)

        snapshots = query.all()

        # Group by (source, table_name)
        groups: Dict[tuple, List[DQQualitySnapshot]] = {}
        for snap in snapshots:
            key = (snap.source, snap.table_name)
            if target.table_pattern and snap.table_name:
                if not re.search(target.table_pattern, snap.table_name):
                    continue
            groups.setdefault(key, []).append(snap)

        for (source, table_name), group_snaps in groups.items():
            if len(group_snaps) < threshold:
                continue

            # Check for N consecutive drops in quality_score
            scores = [
                s.quality_score for s in group_snaps[:threshold + 1]
                if s.quality_score is not None
            ]
            if len(scores) < threshold:
                continue

            consecutive_drops = 0
            for i in range(len(scores) - 1):
                if scores[i] < scores[i + 1]:  # scores are desc by date
                    consecutive_drops += 1
                else:
                    break

            if consecutive_drops >= threshold:
                # Check if we already have a recent alert
                existing = (
                    db.query(DQAnomalyAlert)
                    .filter(
                        DQAnomalyAlert.table_name == table_name,
                        DQAnomalyAlert.alert_type == AnomalyAlertType.QUALITY_DEGRADATION,
                        DQAnomalyAlert.status == AnomalyAlertStatus.OPEN,
                        DQAnomalyAlert.detected_at >= datetime.utcnow() - timedelta(days=1),
                    )
                    .first()
                )
                if existing:
                    continue

                alert = DQAnomalyAlert(
                    table_name=table_name or "",
                    source=source,
                    alert_type=AnomalyAlertType.QUALITY_DEGRADATION,
                    severity=RuleSeverity.ERROR,
                    message=(
                        f"Quality degradation: {consecutive_drops} consecutive drops "
                        f"for {source}/{table_name} "
                        f"(latest: {scores[0]:.1f}, oldest: {scores[-1]:.1f})"
                    ),
                    current_value=f"{scores[0]:.1f}",
                    baseline_value=f"{scores[-1]:.1f}",
                    details={
                        "scores": scores,
                        "consecutive_drops": consecutive_drops,
                        "threshold": threshold,
                        "sla_target_id": target.id,
                    },
                )
                db.add(alert)
                alerts.append(alert)

    if alerts:
        db.commit()
        logger.warning(f"Generated {len(alerts)} quality degradation alerts")

    return alerts


# =============================================================================
# Scheduled runners
# =============================================================================

def scheduled_daily_quality_snapshots():
    """Entry point for scheduled daily snapshot computation."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        snapshots = compute_daily_snapshots(db)
        logger.info(f"Scheduled daily snapshot: {len(snapshots)} snapshots computed")
    except Exception as e:
        logger.error(f"Scheduled daily snapshot computation failed: {e}")
    finally:
        db.close()


def scheduled_degradation_checker():
    """Entry point for scheduled degradation checking."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        alerts = check_degradation_alerts(db)
        logger.info(f"Scheduled degradation check: {len(alerts)} alerts generated")
    except Exception as e:
        logger.error(f"Scheduled degradation check failed: {e}")
    finally:
        db.close()
