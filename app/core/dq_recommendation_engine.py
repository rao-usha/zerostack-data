"""
Data Quality Recommendation Engine.

Analyzes signals from all DQ subsystems (profiling, anomalies, rule violations,
job history, freshness, quality trending) and produces prioritized, actionable
recommendations.
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import Integer as SAInteger, func, text
from sqlalchemy.orm import Session

from app.core.models import (
    DataProfileColumn,
    DataProfileSnapshot,
    DataQualityResult,
    DataQualityRule,
    DatasetRegistry,
    DQAnomalyAlert,
    DQQualitySnapshot,
    DQRecommendation,
    IngestionJob,
    JobStatus,
    AnomalyAlertStatus,
    RecommendationCategory,
    RecommendationPriority,
    RecommendationStatus,
    RuleSeverity,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Recommendation dataclass (transient, before persistence)
# ---------------------------------------------------------------------------

@dataclass
class Recommendation:
    """A single recommendation before persistence."""

    category: str
    priority: str
    title: str
    description: str
    suggested_action: str
    source: Optional[str] = None
    table_name: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
    auto_fixable: bool = False
    fix_action: Optional[str] = None
    fix_params: Optional[Dict[str, Any]] = None


# Priority ordering for sorting
_PRIORITY_ORDER = {
    RecommendationPriority.CRITICAL.value: 0,
    RecommendationPriority.HIGH.value: 1,
    RecommendationPriority.MEDIUM.value: 2,
    RecommendationPriority.LOW.value: 3,
}


# ---------------------------------------------------------------------------
# Individual analyzers
# ---------------------------------------------------------------------------

def _analyze_quality_snapshots(db: Session) -> List[Recommendation]:
    """Check quality score trends for degradation and low scores."""
    recs: List[Recommendation] = []
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)

    # Get latest snapshot per source/table
    subq = (
        db.query(
            DQQualitySnapshot.source,
            DQQualitySnapshot.table_name,
            func.max(DQQualitySnapshot.snapshot_date).label("max_date"),
        )
        .group_by(DQQualitySnapshot.source, DQQualitySnapshot.table_name)
        .subquery()
    )

    latest = (
        db.query(DQQualitySnapshot)
        .join(
            subq,
            (DQQualitySnapshot.source == subq.c.source)
            & (DQQualitySnapshot.table_name == subq.c.table_name)
            & (DQQualitySnapshot.snapshot_date == subq.c.max_date),
        )
        .all()
    )

    for snap in latest:
        # Completeness critically low
        if snap.completeness_score is not None and snap.completeness_score < 50:
            recs.append(Recommendation(
                category=RecommendationCategory.DATA_QUALITY.value,
                priority=RecommendationPriority.CRITICAL.value,
                source=snap.source,
                table_name=snap.table_name,
                title=f"Low completeness on {snap.table_name}",
                description=(
                    f"Completeness score is {snap.completeness_score:.0f}% "
                    f"(below 50% threshold). Table may be missing significant data."
                ),
                suggested_action="Investigate data source and re-ingest missing data.",
                evidence={
                    "completeness_score": snap.completeness_score,
                    "snapshot_date": str(snap.snapshot_date),
                },
                auto_fixable=True,
                fix_action="re_ingest",
                fix_params={"source": snap.source},
            ))

        # Freshness critically low
        if snap.freshness_score is not None and snap.freshness_score < 20:
            recs.append(Recommendation(
                category=RecommendationCategory.DATA_QUALITY.value,
                priority=RecommendationPriority.HIGH.value,
                source=snap.source,
                table_name=snap.table_name,
                title=f"Stale data for {snap.source}",
                description=(
                    f"Freshness score is {snap.freshness_score:.0f} "
                    f"(below 20 threshold). Source hasn't been updated in 5+ days."
                ),
                suggested_action="Trigger re-ingestion for this source.",
                evidence={
                    "freshness_score": snap.freshness_score,
                    "snapshot_date": str(snap.snapshot_date),
                },
                auto_fixable=True,
                fix_action="re_ingest",
                fix_params={"source": snap.source},
            ))

        # Quality score dropped significantly in last 7 days
        week_snap = (
            db.query(DQQualitySnapshot)
            .filter(
                DQQualitySnapshot.source == snap.source,
                DQQualitySnapshot.table_name == snap.table_name,
                DQQualitySnapshot.snapshot_date >= week_ago.date(),
                DQQualitySnapshot.snapshot_date < snap.snapshot_date,
            )
            .order_by(DQQualitySnapshot.snapshot_date.asc())
            .first()
        )
        if (
            week_snap
            and snap.quality_score is not None
            and week_snap.quality_score is not None
            and (week_snap.quality_score - snap.quality_score) > 20
        ):
            recs.append(Recommendation(
                category=RecommendationCategory.DATA_QUALITY.value,
                priority=RecommendationPriority.HIGH.value,
                source=snap.source,
                table_name=snap.table_name,
                title=f"Quality degradation on {snap.table_name}",
                description=(
                    f"Quality score dropped from {week_snap.quality_score:.0f} "
                    f"to {snap.quality_score:.0f} in the last 7 days."
                ),
                suggested_action="Review recent ingestion changes and data pipeline.",
                evidence={
                    "current_score": snap.quality_score,
                    "previous_score": week_snap.quality_score,
                    "drop": week_snap.quality_score - snap.quality_score,
                },
            ))

    return recs


def _analyze_anomalies(db: Session) -> List[Recommendation]:
    """Check for unresolved anomalies clustered on same table."""
    recs: List[Recommendation] = []

    # Count open anomalies per table
    anomaly_counts = (
        db.query(
            DQAnomalyAlert.table_name,
            DQAnomalyAlert.source,
            func.count(DQAnomalyAlert.id).label("cnt"),
        )
        .filter(DQAnomalyAlert.status == AnomalyAlertStatus.OPEN)
        .group_by(DQAnomalyAlert.table_name, DQAnomalyAlert.source)
        .all()
    )

    for table_name, source, cnt in anomaly_counts:
        if cnt >= 3:
            recs.append(Recommendation(
                category=RecommendationCategory.DATA_QUALITY.value,
                priority=RecommendationPriority.MEDIUM.value,
                source=source,
                table_name=table_name,
                title=f"{cnt} unresolved anomalies on {table_name}",
                description=(
                    f"There are {cnt} open anomaly alerts for {table_name}. "
                    f"Multiple unresolved anomalies suggest a systemic issue."
                ),
                suggested_action="Investigate and resolve or dismiss open anomalies.",
                evidence={"open_anomaly_count": cnt},
            ))

    return recs


def _analyze_rule_violations(db: Session) -> List[Recommendation]:
    """Check for tables with many recent rule failures."""
    recs: List[Recommendation] = []
    week_ago = datetime.utcnow() - timedelta(days=7)

    violation_counts = (
        db.query(
            DataQualityResult.dataset_name,
            DataQualityResult.source,
            func.count(DataQualityResult.id).label("cnt"),
        )
        .filter(
            DataQualityResult.passed == 0,
            DataQualityResult.evaluated_at >= week_ago,
        )
        .group_by(DataQualityResult.dataset_name, DataQualityResult.source)
        .all()
    )

    for table_name, source, cnt in violation_counts:
        if cnt >= 5:
            recs.append(Recommendation(
                category=RecommendationCategory.DATA_QUALITY.value,
                priority=RecommendationPriority.MEDIUM.value,
                source=source,
                table_name=table_name,
                title=f"{cnt} rule violations on {table_name}",
                description=(
                    f"{cnt} data quality rule failures in the last 7 days "
                    f"for {table_name}. Review the data pipeline."
                ),
                suggested_action="Review and fix data quality rule violations.",
                evidence={"violation_count": cnt, "window_days": 7},
            ))

    return recs


def _analyze_completeness_gaps(db: Session) -> List[Recommendation]:
    """Find columns with very high null rates."""
    recs: List[Recommendation] = []

    # Get latest profile snapshot IDs per table
    subq = (
        db.query(
            DataProfileSnapshot.table_name,
            func.max(DataProfileSnapshot.id).label("max_id"),
        )
        .group_by(DataProfileSnapshot.table_name)
        .subquery()
    )

    high_null_cols = (
        db.query(DataProfileColumn)
        .join(subq, DataProfileColumn.snapshot_id == subq.c.max_id)
        .filter(DataProfileColumn.null_pct > 80)
        .all()
    )

    for col in high_null_cols:
        snapshot = db.query(DataProfileSnapshot).filter(
            DataProfileSnapshot.id == col.snapshot_id
        ).first()
        if not snapshot:
            continue
        recs.append(Recommendation(
            category=RecommendationCategory.DATA_QUALITY.value,
            priority=RecommendationPriority.MEDIUM.value,
            source=snapshot.source,
            table_name=snapshot.table_name,
            title=f"Column {col.column_name} is {col.null_pct:.0f}% null",
            description=(
                f"Column '{col.column_name}' in {snapshot.table_name} has "
                f"{col.null_pct:.0f}% null values. Consider dropping or enriching."
            ),
            suggested_action="Evaluate whether this column should be dropped or enriched from another source.",
            evidence={
                "column": col.column_name,
                "null_pct": col.null_pct,
                "null_count": col.null_count,
            },
        ))

    return recs


def _analyze_missing_coverage(db: Session) -> List[Recommendation]:
    """Find sources/tables with no DQ rules or no profiles."""
    recs: List[Recommendation] = []

    # Sources in registry
    registry_sources = (
        db.query(DatasetRegistry.source)
        .distinct()
        .all()
    )
    source_names = {r[0] for r in registry_sources}

    # Sources with at least one rule
    rule_sources = {
        r[0]
        for r in db.query(DataQualityRule.source).distinct().all()
        if r[0]
    }

    for src in source_names:
        if src and src not in rule_sources:
            recs.append(Recommendation(
                category=RecommendationCategory.DATA_QUALITY.value,
                priority=RecommendationPriority.LOW.value,
                source=src,
                title=f"No DQ rules for source '{src}'",
                description=f"Source '{src}' has no data quality rules defined.",
                suggested_action="Run the rule seeder or manually create rules.",
                auto_fixable=True,
                fix_action="seed_rules",
                fix_params={"source": src},
            ))

    # Tables never profiled
    registry_tables = (
        db.query(DatasetRegistry.table_name, DatasetRegistry.source).all()
    )
    profiled_tables = {
        r[0]
        for r in db.query(DataProfileSnapshot.table_name).distinct().all()
    }

    for table_name, source in registry_tables:
        if table_name and table_name not in profiled_tables:
            recs.append(Recommendation(
                category=RecommendationCategory.DATA_QUALITY.value,
                priority=RecommendationPriority.LOW.value,
                source=source,
                table_name=table_name,
                title=f"Table '{table_name}' never profiled",
                description=f"Table '{table_name}' has no profiling data.",
                suggested_action="Run data profiling on this table.",
                auto_fixable=True,
                fix_action="profile_table",
                fix_params={"table_name": table_name},
            ))

    return recs


def _analyze_job_history(db: Session) -> List[Recommendation]:
    """Analyze ingestion job patterns for orchestration issues."""
    recs: List[Recommendation] = []
    now = datetime.utcnow()
    month_ago = now - timedelta(days=30)

    # --- Source failure patterns ---
    # Get sources with recent jobs
    source_stats = (
        db.query(
            IngestionJob.source,
            func.count(IngestionJob.id).label("total"),
            func.sum(
                func.cast(IngestionJob.status == JobStatus.FAILED, SAInteger)
            ).label("failed"),
        )
        .filter(IngestionJob.created_at >= month_ago)
        .group_by(IngestionJob.source)
        .all()
    )

    for source, total, failed in source_stats:
        if total < 3:
            continue

        # Check last 5 runs for this source
        last_5 = (
            db.query(IngestionJob.status)
            .filter(IngestionJob.source == source)
            .order_by(IngestionJob.created_at.desc())
            .limit(5)
            .all()
        )
        recent_failures = sum(
            1 for (s,) in last_5 if s == JobStatus.FAILED
        )

        if recent_failures >= 3:
            recs.append(Recommendation(
                category=RecommendationCategory.ORCHESTRATION.value,
                priority=RecommendationPriority.CRITICAL.value,
                source=source,
                title=f"Source '{source}' failing frequently",
                description=(
                    f"Source '{source}' failed {recent_failures} of its last "
                    f"5 runs. Check API key, connectivity, or source availability."
                ),
                suggested_action="Check API credentials, network access, and source documentation.",
                evidence={
                    "recent_failures": recent_failures,
                    "total_last_30d": total,
                    "failed_last_30d": failed or 0,
                },
            ))

        # Source runs but inserts 0 rows
        zero_row_runs = (
            db.query(func.count(IngestionJob.id))
            .filter(
                IngestionJob.source == source,
                IngestionJob.status == JobStatus.SUCCESS,
                IngestionJob.rows_inserted == 0,
                IngestionJob.created_at >= month_ago,
            )
            .scalar()
        )
        success_runs = total - (failed or 0)
        if success_runs > 0 and zero_row_runs and zero_row_runs > success_runs * 0.5:
            recs.append(Recommendation(
                category=RecommendationCategory.ORCHESTRATION.value,
                priority=RecommendationPriority.MEDIUM.value,
                source=source,
                title=f"Source '{source}' succeeds but inserts 0 rows",
                description=(
                    f"Source '{source}' completed {zero_row_runs} of "
                    f"{success_runs} successful runs with 0 rows inserted."
                ),
                suggested_action="Check API parameters, filters, or date ranges.",
                evidence={
                    "zero_row_runs": zero_row_runs,
                    "total_success": success_runs,
                },
            ))

    return recs


def _analyze_freshness(db: Session) -> List[Recommendation]:
    """Check for overdue sources based on last successful ingestion."""
    recs: List[Recommendation] = []
    now = datetime.utcnow()

    # Last successful run per source
    last_success = (
        db.query(
            IngestionJob.source,
            func.max(IngestionJob.completed_at).label("last_run"),
        )
        .filter(IngestionJob.status == JobStatus.SUCCESS)
        .group_by(IngestionJob.source)
        .all()
    )

    for source, last_run in last_success:
        if not last_run:
            continue
        days_stale = (now - last_run).days
        if days_stale > 14:
            recs.append(Recommendation(
                category=RecommendationCategory.ORCHESTRATION.value,
                priority=RecommendationPriority.HIGH.value,
                source=source,
                title=f"Source '{source}' overdue ({days_stale}d since last run)",
                description=(
                    f"Source '{source}' last succeeded {days_stale} days ago. "
                    f"Schedule may be missed or disabled."
                ),
                suggested_action="Check ingestion schedule or trigger manual re-ingestion.",
                evidence={
                    "last_success": last_run.isoformat(),
                    "days_since": days_stale,
                },
                auto_fixable=True,
                fix_action="re_ingest",
                fix_params={"source": source},
            ))

    return recs


def _analyze_tier_performance(db: Session) -> List[Recommendation]:
    """Analyze batch tier health from job history."""
    recs: List[Recommendation] = []
    month_ago = datetime.utcnow() - timedelta(days=30)

    # Only works if tier column exists (added in batch system)
    try:
        tier_stats = (
            db.query(
                IngestionJob.tier,
                func.count(IngestionJob.id).label("total"),
                func.sum(
                    func.cast(IngestionJob.status == JobStatus.FAILED, SAInteger)
                ).label("failed"),
            )
            .filter(
                IngestionJob.created_at >= month_ago,
                IngestionJob.tier.isnot(None),
            )
            .group_by(IngestionJob.tier)
            .all()
        )
    except Exception:
        return recs

    for tier, total, failed in tier_stats:
        if total < 4 or not failed:
            continue
        fail_rate = failed / total
        if fail_rate > 0.5:
            recs.append(Recommendation(
                category=RecommendationCategory.ORCHESTRATION.value,
                priority=RecommendationPriority.HIGH.value,
                title=f"Tier {tier} batch health is poor ({fail_rate:.0%} failure)",
                description=(
                    f"Tier {tier} has a {fail_rate:.0%} failure rate over the "
                    f"last 30 days ({failed}/{total} jobs failed)."
                ),
                suggested_action="Investigate common failure causes in this tier.",
                evidence={
                    "tier": tier,
                    "total_jobs": total,
                    "failed_jobs": failed,
                    "fail_rate": round(fail_rate, 3),
                },
            ))

    return recs




# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def generate_recommendations(db: Session) -> List[DQRecommendation]:
    """
    Run all analyzers, deduplicate, prioritize, and persist recommendations.

    Returns the list of newly created DQRecommendation records.
    """
    logger.info("Generating DQ recommendations...")

    # Collect from all analyzers
    raw: List[Recommendation] = []
    analyzers = [
        ("quality_snapshots", _analyze_quality_snapshots),
        ("anomalies", _analyze_anomalies),
        ("rule_violations", _analyze_rule_violations),
        ("completeness_gaps", _analyze_completeness_gaps),
        ("missing_coverage", _analyze_missing_coverage),
        ("job_history", _analyze_job_history),
        ("freshness", _analyze_freshness),
        ("tier_performance", _analyze_tier_performance),
    ]

    for name, analyzer_fn in analyzers:
        try:
            results = analyzer_fn(db)
            raw.extend(results)
            logger.info(f"  {name}: {len(results)} recommendations")
        except Exception as e:
            logger.warning(f"  {name} analyzer failed: {e}")

    # Deduplicate by (category, source, table_name, title)
    seen = set()
    deduped: List[Recommendation] = []
    for r in raw:
        key = (r.category, r.source, r.table_name, r.title)
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    # Sort by priority
    deduped.sort(key=lambda r: _PRIORITY_ORDER.get(r.priority, 99))

    # Expire old open recommendations (mark dismissed if >7 days old)
    week_ago = datetime.utcnow() - timedelta(days=7)
    db.query(DQRecommendation).filter(
        DQRecommendation.status == RecommendationStatus.OPEN,
        DQRecommendation.created_at < week_ago,
    ).update(
        {
            DQRecommendation.status: RecommendationStatus.DISMISSED,
            DQRecommendation.dismissed_at: datetime.utcnow(),
        },
        synchronize_session="fetch",
    )

    # Persist new recommendations
    persisted: List[DQRecommendation] = []
    for r in deduped:
        rec = DQRecommendation(
            category=RecommendationCategory(r.category),
            priority=RecommendationPriority(r.priority),
            source=r.source,
            table_name=r.table_name,
            title=r.title,
            description=r.description,
            suggested_action=r.suggested_action,
            evidence=r.evidence,
            auto_fixable=r.auto_fixable,
            fix_action=r.fix_action,
            fix_params=r.fix_params,
            status=RecommendationStatus.OPEN,
        )
        db.add(rec)
        persisted.append(rec)

    db.commit()
    for rec in persisted:
        db.refresh(rec)

    logger.info(f"Generated {len(persisted)} recommendations (from {len(raw)} raw signals)")
    return persisted


def get_recommendations(
    db: Session,
    category: Optional[str] = None,
    priority: Optional[str] = None,
    source: Optional[str] = None,
    status: str = "open",
    limit: int = 50,
) -> List[DQRecommendation]:
    """Query persisted recommendations with optional filters."""
    q = db.query(DQRecommendation)

    if category:
        q = q.filter(DQRecommendation.category == RecommendationCategory(category))
    if priority:
        q = q.filter(DQRecommendation.priority == RecommendationPriority(priority))
    if source:
        q = q.filter(DQRecommendation.source == source)
    if status:
        q = q.filter(DQRecommendation.status == RecommendationStatus(status))

    return q.order_by(DQRecommendation.created_at.desc()).limit(limit).all()


def apply_recommendation(db: Session, recommendation_id: int) -> Optional[DQRecommendation]:
    """
    Mark a recommendation as applied.

    For auto-fixable recommendations, the caller (API layer) should
    dispatch the fix_action. This function only updates the status.
    """
    rec = db.query(DQRecommendation).filter(DQRecommendation.id == recommendation_id).first()
    if not rec:
        return None
    rec.status = RecommendationStatus.APPLIED
    rec.applied_at = datetime.utcnow()
    db.commit()
    return rec


def dismiss_recommendation(db: Session, recommendation_id: int) -> Optional[DQRecommendation]:
    """Mark a recommendation as dismissed."""
    rec = db.query(DQRecommendation).filter(DQRecommendation.id == recommendation_id).first()
    if not rec:
        return None
    rec.status = RecommendationStatus.DISMISSED
    rec.dismissed_at = datetime.utcnow()
    db.commit()
    return rec


def get_review_summary(db: Session) -> Dict[str, Any]:
    """
    Quick dashboard summary: quality scores by source, open anomalies,
    top recommendations.
    """
    # Quality scores by source (latest snapshot)
    subq = (
        db.query(
            DQQualitySnapshot.source,
            func.max(DQQualitySnapshot.snapshot_date).label("max_date"),
        )
        .group_by(DQQualitySnapshot.source)
        .subquery()
    )
    latest_scores = (
        db.query(DQQualitySnapshot)
        .join(
            subq,
            (DQQualitySnapshot.source == subq.c.source)
            & (DQQualitySnapshot.snapshot_date == subq.c.max_date),
        )
        .all()
    )

    source_scores = {}
    for snap in latest_scores:
        if snap.source not in source_scores:
            source_scores[snap.source] = {
                "quality_score": snap.quality_score,
                "completeness": snap.completeness_score,
                "freshness": snap.freshness_score,
                "validity": snap.validity_score,
                "consistency": snap.consistency_score,
                "snapshot_date": str(snap.snapshot_date),
            }

    # Open anomaly count
    open_anomalies = (
        db.query(func.count(DQAnomalyAlert.id))
        .filter(DQAnomalyAlert.status == AnomalyAlertStatus.OPEN)
        .scalar()
    ) or 0

    # Open recommendation counts by priority
    rec_counts = (
        db.query(
            DQRecommendation.priority,
            func.count(DQRecommendation.id),
        )
        .filter(DQRecommendation.status == RecommendationStatus.OPEN)
        .group_by(DQRecommendation.priority)
        .all()
    )
    priority_counts = {
        p.value if hasattr(p, "value") else p: c for p, c in rec_counts
    }

    # Top 5 recommendations
    top_recs = (
        db.query(DQRecommendation)
        .filter(DQRecommendation.status == RecommendationStatus.OPEN)
        .order_by(DQRecommendation.created_at.desc())
        .limit(5)
        .all()
    )

    return {
        "source_scores": source_scores,
        "open_anomalies": open_anomalies,
        "recommendation_counts": priority_counts,
        "top_recommendations": [r.to_dict() for r in top_recs],
        "generated_at": datetime.utcnow().isoformat(),
    }
