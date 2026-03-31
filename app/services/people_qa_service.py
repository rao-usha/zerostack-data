"""
PeopleQAService — rule-based data quality checks for people / org-chart data.

Runs 9 structural checks per company, computes a 0-100 health score using the
unified 4-dimension formula from dq_base, and surfaces pending merge candidates
for human review.  No LLM calls.

Implements BaseQualityProvider so it can be called uniformly from the
dq_post_ingestion_hook alongside PE, Site Intel, and 3PL providers.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy.orm import Session

from app.core.dq_base import (
    BaseQualityProvider,
    QualityIssue,
    QualityReport,
    _penalty,
    compute_quality_score,
)

logger = logging.getLogger(__name__)

# Thresholds
_STALE_DAYS = 90
_MIN_HEADCOUNT = 5
_MAX_DEPTH = 10
_LOW_CONFIDENCE_PCT = 0.30
_SENIOR_TITLES = ("chief executive", "ceo", "chief financial", "cfo", "president")

# Dimension mapping for each check
_CHECK_DIMENSION: dict[str, str] = {
    "no_org_chart": "completeness",
    "no_ceo": "completeness",
    "low_headcount": "completeness",
    "low_confidence": "completeness",
    "stale_snapshot": "freshness",
    "duplicate_ceo_title": "validity",
    "depth_anomaly": "validity",
    "board_misclassified": "validity",
    "pending_dedup": "consistency",
}


class PeopleQAService(BaseQualityProvider):
    """Run data-quality checks for people / org-chart data.

    All checks are synchronous DB queries — no external I/O.
    Implements BaseQualityProvider (dataset = "people").
    """

    dataset = "people"

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def _run_company_raw(self, company_id: int, db: Session) -> dict:
        """Internal: run all checks and return legacy dict format.

        Returns:
            {company_id, company_name, snapshot_date, total_people,
             issues: [{check, severity, message, count}], health_score,
             _quality_issues: list[QualityIssue]}
        """
        from app.core.people_models import (
            CompanyPerson,
            IndustrialCompany,
            OrgChartSnapshot,
        )

        company = db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
        if not company:
            return {"company_id": company_id, "company_name": "Unknown", "issues": [], "health_score": 0}

        snapshot = (
            db.query(OrgChartSnapshot)
            .filter(OrgChartSnapshot.company_id == company_id)
            .order_by(OrgChartSnapshot.snapshot_date.desc())
            .first()
        )

        company_people = (
            db.query(CompanyPerson)
            .filter(
                CompanyPerson.company_id == company_id,
                CompanyPerson.is_current.is_(True),
            )
            .all()
        )

        checks = [
            self._check_no_org_chart,
            self._check_no_ceo,
            self._check_duplicate_ceo_title,
            self._check_stale_snapshot,
            self._check_low_headcount,
            self._check_board_misclassified,
            self._check_depth_anomaly,
            self._check_pending_dedup,
            self._check_low_confidence,
        ]

        issues = []
        for fn in checks:
            try:
                result = fn(company_id, snapshot, company_people, db)
                if result:
                    issues.append(result)
            except Exception as exc:
                logger.warning("QA check %s failed for company %s: %s", fn.__name__, company_id, exc)

        qi_list = [
            QualityIssue(
                check=i["check"],
                severity=i["severity"],
                message=i["message"],
                count=i.get("count", 0),
                dimension=_CHECK_DIMENSION.get(i["check"], "validity"),
            )
            for i in issues
        ]
        health = self._health_score(issues)

        return {
            "company_id": company_id,
            "company_name": company.name,
            "snapshot_date": snapshot.snapshot_date.isoformat() if snapshot and snapshot.snapshot_date else None,
            "total_people": snapshot.total_people if snapshot else 0,
            "issues": issues,
            "health_score": health,
            # 4-dimension sub-scores (available via run())
            "_quality_issues": qi_list,
        }

    # ------------------------------------------------------------------ #
    # BaseQualityProvider implementation                                   #
    # ------------------------------------------------------------------ #

    def run(self, entity_id: int, db: Session) -> QualityReport:
        """Run all checks for one company and return a QualityReport."""
        raw = self.run_company(entity_id, db)
        qi_list: list[QualityIssue] = raw.pop("_quality_issues", [])

        completeness = _penalty(qi_list, "completeness")
        freshness    = _penalty(qi_list, "freshness")
        validity     = _penalty(qi_list, "validity")
        consistency  = _penalty(qi_list, "consistency")

        return QualityReport(
            entity_id=entity_id,
            entity_name=raw["company_name"],
            dataset=self.dataset,
            quality_score=compute_quality_score(completeness, freshness, validity, consistency),
            completeness=completeness,
            freshness=freshness,
            validity=validity,
            consistency=consistency,
            issues=qi_list,
        )

    def run_all(self, db: Session, limit: int | None = None) -> list[QualityReport]:
        """Run checks for all tracked companies, sorted worst quality_score first."""
        from app.core.people_models import IndustrialCompany

        q = db.query(IndustrialCompany).filter(IndustrialCompany.status == "active")
        if limit:
            q = q.limit(limit)
        companies = q.all()

        reports: list[QualityReport] = []
        for co in companies:
            try:
                reports.append(self.run(co.id, db))
            except Exception as exc:
                logger.error("QA run failed for company %s: %s", co.id, exc)

        reports.sort(key=lambda r: r.quality_score)
        logger.info(
            "QA run complete: %d companies, avg score %.0f",
            len(reports),
            sum(r.quality_score for r in reports) / max(len(reports), 1),
        )
        return reports

    def run_company(self, company_id: int, db: Session) -> dict:
        """Legacy dict-returning interface (used by existing API endpoints).

        Prefer run() for new code — it returns a typed QualityReport.
        """
        return self._run_company_raw(company_id, db)

    # ------------------------------------------------------------------ #
    # Health scoring                                                       #
    # ------------------------------------------------------------------ #

    def _health_score(self, issues: list[dict]) -> int:
        """Legacy penalty-based score kept for API backward compat."""
        score = 100
        for issue in issues:
            sev = issue.get("severity", "INFO")
            if sev == "ERROR":
                score -= 20
            elif sev == "WARNING":
                score -= 10
            else:
                score -= 5
        return max(0, score)

    # ------------------------------------------------------------------ #
    # Individual checks                                                    #
    # ------------------------------------------------------------------ #

    def _check_no_org_chart(self, company_id, snapshot, company_people, db) -> dict | None:
        if snapshot is None:
            return {
                "check": "no_org_chart",
                "severity": "ERROR",
                "message": "No org chart snapshot found for this company.",
                "count": 0,
            }
        return None

    def _check_no_ceo(self, company_id, snapshot, company_people, db) -> dict | None:
        ceos = [
            cp for cp in company_people
            if getattr(cp, "management_level", None) == 1
            and not getattr(cp, "is_board_member", False)
        ]
        if not ceos:
            return {
                "check": "no_ceo",
                "severity": "ERROR",
                "message": "No executive with management_level=1 found. CEO may be missing or miscategorised.",
                "count": 0,
            }
        return None

    def _check_duplicate_ceo_title(self, company_id, snapshot, company_people, db) -> dict | None:
        ceo_title_people = [
            cp for cp in company_people
            if cp.title and any(t in cp.title.lower() for t in ("chief executive", " ceo", "ceo "))
        ]
        if len(ceo_title_people) > 1:
            names = ", ".join(cp.full_name or "Unknown" for cp in ceo_title_people[:3])
            return {
                "check": "duplicate_ceo_title",
                "severity": "WARNING",
                "message": f"{len(ceo_title_people)} people hold a CEO-level title: {names}.",
                "count": len(ceo_title_people),
            }
        return None

    def _check_stale_snapshot(self, company_id, snapshot, company_people, db) -> dict | None:
        if snapshot is None:
            return None  # covered by no_org_chart
        if snapshot.snapshot_date and snapshot.snapshot_date < date.today() - timedelta(days=_STALE_DAYS):
            days_old = (date.today() - snapshot.snapshot_date).days
            return {
                "check": "stale_snapshot",
                "severity": "WARNING",
                "message": f"Org chart snapshot is {days_old} days old (threshold: {_STALE_DAYS} days).",
                "count": days_old,
            }
        return None

    def _check_low_headcount(self, company_id, snapshot, company_people, db) -> dict | None:
        if snapshot is None:
            return None
        total = snapshot.total_people or 0
        if total < _MIN_HEADCOUNT:
            return {
                "check": "low_headcount",
                "severity": "WARNING",
                "message": f"Only {total} people in org chart — likely incomplete collection (threshold: {_MIN_HEADCOUNT}).",
                "count": total,
            }
        return None

    def _check_board_misclassified(self, company_id, snapshot, company_people, db) -> dict | None:
        misclassified = [
            cp for cp in company_people
            if cp.is_board_member
            and cp.title
            and any(t in cp.title.lower() for t in _SENIOR_TITLES)
        ]
        if misclassified:
            names = ", ".join(cp.full_name or "Unknown" for cp in misclassified[:3])
            return {
                "check": "board_misclassified",
                "severity": "WARNING",
                "message": f"{len(misclassified)} exec(s) marked as board member with a senior executive title: {names}.",
                "count": len(misclassified),
            }
        return None

    def _check_depth_anomaly(self, company_id, snapshot, company_people, db) -> dict | None:
        if snapshot is None:
            return None
        depth = snapshot.max_depth or 0
        if depth > _MAX_DEPTH:
            return {
                "check": "depth_anomaly",
                "severity": "WARNING",
                "message": f"Org chart max depth is {depth} (threshold: {_MAX_DEPTH}). May indicate a circular chain or bad data.",
                "count": depth,
            }
        return None

    def _check_pending_dedup(self, company_id, snapshot, company_people, db) -> dict | None:
        """Count pending merge candidates linked to this company via shared_company_ids."""
        from app.core.people_models import PeopleMergeCandidate
        from sqlalchemy import cast
        from sqlalchemy.dialects.postgresql import JSONB

        try:
            # shared_company_ids is a JSON array; check if company_id is in it
            candidates = (
                db.query(PeopleMergeCandidate)
                .filter(
                    PeopleMergeCandidate.status == "pending",
                    PeopleMergeCandidate.shared_company_ids.cast(JSONB).contains(
                        cast([company_id], JSONB)
                    ),
                )
                .count()
            )
        except Exception:
            # Fallback: load all pending and filter in Python (slower but safe)
            all_pending = (
                db.query(PeopleMergeCandidate)
                .filter(PeopleMergeCandidate.status == "pending")
                .all()
            )
            candidates = sum(
                1 for c in all_pending
                if c.shared_company_ids and company_id in (c.shared_company_ids or [])
            )

        if candidates > 0:
            return {
                "check": "pending_dedup",
                "severity": "INFO",
                "message": f"{candidates} merge candidate(s) pending review for this company.",
                "count": candidates,
            }
        return None

    def _check_low_confidence(self, company_id, snapshot, company_people, db) -> dict | None:
        if not company_people:
            return None
        low = [cp for cp in company_people if getattr(cp, "confidence", "medium") == "low"]
        pct = len(low) / len(company_people)
        if pct > _LOW_CONFIDENCE_PCT:
            return {
                "check": "low_confidence",
                "severity": "INFO",
                "message": f"{len(low)} of {len(company_people)} people ({pct:.0%}) have low confidence — consider re-collecting.",
                "count": len(low),
            }
        return None
