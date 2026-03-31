"""
Site Intelligence Data Quality Service.

Job-level DQ checks for site intelligence collection: zero inserts,
stale jobs, high failure rates, and partial coverage detection.

Entity = SiteIntelCollectionJob id (latest successful job per domain/source).
Implements BaseQualityProvider (dataset = "site_intel").
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.core.dq_base import (
    BaseQualityProvider,
    QualityIssue,
    QualityReport,
    _penalty,
    compute_quality_score,
)

logger = logging.getLogger(__name__)

_STALE_JOB_DAYS = 60
_HIGH_FAILURE_RATE = 0.30


class SiteIntelDQService(BaseQualityProvider):
    """DQ checks for Site Intelligence collection jobs, scoped per job."""

    dataset = "site_intel"

    # ------------------------------------------------------------------ #
    # BaseQualityProvider implementation                                   #
    # ------------------------------------------------------------------ #

    def run(self, entity_id: int, db: Session) -> QualityReport:
        """Run all checks for one SiteIntelCollectionJob."""
        from app.core.models_site_intel import SiteIntelCollectionJob

        job = db.query(SiteIntelCollectionJob).filter(SiteIntelCollectionJob.id == entity_id).first()
        if not job:
            return QualityReport(
                entity_id=entity_id,
                entity_name="Unknown",
                dataset=self.dataset,
                quality_score=0,
                completeness=0,
                freshness=0,
                validity=0,
                consistency=0,
                issues=[QualityIssue("job_not_found", "ERROR", "SiteIntelCollectionJob not found.", dimension="validity")],
            )

        entity_name = f"{job.domain}/{job.source}"
        checks = [
            self._check_zero_inserted,
            self._check_stale_job,
            self._check_high_failure_rate,
            self._check_partial_coverage,
        ]

        issues: list[QualityIssue] = []
        for fn in checks:
            try:
                result = fn(job)
                if result:
                    issues.append(result)
            except Exception as exc:
                logger.warning("Site Intel DQ check %s failed for job %s: %s", fn.__name__, entity_id, exc)

        completeness = _penalty(issues, "completeness")
        freshness    = _penalty(issues, "freshness")
        validity     = _penalty(issues, "validity")
        consistency  = _penalty(issues, "consistency")

        return QualityReport(
            entity_id=entity_id,
            entity_name=entity_name,
            dataset=self.dataset,
            quality_score=compute_quality_score(completeness, freshness, validity, consistency),
            completeness=completeness,
            freshness=freshness,
            validity=validity,
            consistency=consistency,
            issues=issues,
        )

    def run_all(self, db: Session, limit: int | None = None) -> list[QualityReport]:
        """Run checks for the latest successful job per domain/source, worst-first."""
        from app.core.models_site_intel import SiteIntelCollectionJob
        from sqlalchemy import func

        # One report per unique (domain, source) — use the latest successful job
        subq = (
            db.query(
                SiteIntelCollectionJob.domain,
                SiteIntelCollectionJob.source,
                func.max(SiteIntelCollectionJob.id).label("latest_id"),
            )
            .filter(SiteIntelCollectionJob.status == "success")
            .group_by(SiteIntelCollectionJob.domain, SiteIntelCollectionJob.source)
        )
        if limit:
            subq = subq.limit(limit)

        reports: list[QualityReport] = []
        for row in subq.all():
            try:
                reports.append(self.run(row.latest_id, db))
            except Exception as exc:
                logger.error("Site Intel DQ run failed for job %s: %s", row.latest_id, exc)

        reports.sort(key=lambda r: r.quality_score)
        return reports

    # ------------------------------------------------------------------ #
    # Individual checks                                                    #
    # ------------------------------------------------------------------ #

    def _check_zero_inserted(self, job) -> QualityIssue | None:
        """Successful job that inserted 0 records."""
        if job.status == "success" and (job.inserted_items or 0) == 0:
            return QualityIssue(
                check="zero_inserted",
                severity="ERROR",
                message="Job completed successfully but inserted 0 records — source may be empty or broken.",
                count=0,
                dimension="completeness",
            )
        return None

    def _check_stale_job(self, job) -> QualityIssue | None:
        """Last successful completion was more than 60 days ago."""
        if job.status != "success" or not job.completed_at:
            return None
        age_days = (datetime.utcnow() - job.completed_at).days
        if age_days > _STALE_JOB_DAYS:
            return QualityIssue(
                check="stale_job",
                severity="WARNING",
                message=f"Last successful job was {age_days} days ago (threshold: {_STALE_JOB_DAYS} days).",
                count=age_days,
                dimension="freshness",
            )
        return None

    def _check_high_failure_rate(self, job) -> QualityIssue | None:
        """More than 30% of processed items failed."""
        total = job.total_items or 0
        failed = job.failed_items or 0
        if total > 0 and (failed / total) > _HIGH_FAILURE_RATE:
            pct = round(failed / total * 100, 1)
            return QualityIssue(
                check="high_failure_rate",
                severity="WARNING",
                message=f"{failed}/{total} items failed ({pct}%) — above {_HIGH_FAILURE_RATE:.0%} threshold.",
                count=failed,
                dimension="validity",
            )
        return None

    def _check_partial_coverage(self, job) -> QualityIssue | None:
        """Processed significantly fewer items than declared total."""
        total = job.total_items or 0
        processed = job.processed_items or 0
        if total > 10 and processed < total * 0.70:
            pct = round(processed / total * 100, 1)
            return QualityIssue(
                check="partial_coverage",
                severity="INFO",
                message=f"Only {processed}/{total} items processed ({pct}%) — collection may be incomplete.",
                count=total - processed,
                dimension="consistency",
            )
        return None
