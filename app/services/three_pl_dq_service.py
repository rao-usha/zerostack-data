"""
3PL Data Quality Service.

Company-level DQ checks for 3PL directory data: website presence,
headquarters data, employee count, revenue, and data freshness.

Implements BaseQualityProvider (dataset = "three_pl", entity = ThreePLCompany).
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

_STALE_ENRICHMENT_DAYS = 90


class ThreePLDQService(BaseQualityProvider):
    """DQ checks for 3PL company data, scoped per ThreePLCompany."""

    dataset = "three_pl"

    # ------------------------------------------------------------------ #
    # BaseQualityProvider implementation                                   #
    # ------------------------------------------------------------------ #

    def run(self, entity_id: int, db: Session) -> QualityReport:
        """Run all checks for one 3PL company."""
        from app.core.models_site_intel import ThreePLCompany

        company = db.query(ThreePLCompany).filter(ThreePLCompany.id == entity_id).first()
        if not company:
            return QualityReport(
                entity_id=entity_id,
                entity_name="Unknown",
                dataset=self.dataset,
                quality_score=0,
                completeness=0,
                freshness=0,
                validity=0,
                consistency=0,
                issues=[QualityIssue("company_not_found", "ERROR", "3PL company not found.", dimension="validity")],
            )

        checks = [
            self._check_no_website,
            self._check_no_hq,
            self._check_no_employees,
            self._check_no_revenue,
            self._check_stale_enrichment,
            self._check_missing_services,
        ]

        issues: list[QualityIssue] = []
        for fn in checks:
            try:
                result = fn(company)
                if result:
                    issues.append(result)
            except Exception as exc:
                logger.warning("3PL DQ check %s failed for company %s: %s", fn.__name__, entity_id, exc)

        completeness = _penalty(issues, "completeness")
        freshness    = _penalty(issues, "freshness")
        validity     = _penalty(issues, "validity")
        consistency  = _penalty(issues, "consistency")

        return QualityReport(
            entity_id=entity_id,
            entity_name=company.company_name,
            dataset=self.dataset,
            quality_score=compute_quality_score(completeness, freshness, validity, consistency),
            completeness=completeness,
            freshness=freshness,
            validity=validity,
            consistency=consistency,
            issues=issues,
        )

    def run_all(self, db: Session, limit: int | None = None) -> list[QualityReport]:
        """Run checks for all 3PL companies, sorted worst quality_score first."""
        from app.core.models_site_intel import ThreePLCompany

        q = db.query(ThreePLCompany)
        if limit:
            q = q.limit(limit)
        companies = q.all()

        reports: list[QualityReport] = []
        for co in companies:
            try:
                reports.append(self.run(co.id, db))
            except Exception as exc:
                logger.error("3PL DQ run failed for company %s: %s", co.id, exc)

        reports.sort(key=lambda r: r.quality_score)
        logger.info(
            "3PL DQ run complete: %d companies, avg score %.0f",
            len(reports),
            sum(r.quality_score for r in reports) / max(len(reports), 1),
        )
        return reports

    # ------------------------------------------------------------------ #
    # Individual checks                                                    #
    # ------------------------------------------------------------------ #

    def _check_no_website(self, company) -> QualityIssue | None:
        if not company.website:
            return QualityIssue(
                check="no_website",
                severity="WARNING",
                message="Company website is missing — limits enrichment and verification.",
                count=0,
                dimension="completeness",
            )
        return None

    def _check_no_hq(self, company) -> QualityIssue | None:
        if not company.headquarters_city and not company.headquarters_state:
            return QualityIssue(
                check="no_hq",
                severity="WARNING",
                message="Headquarters city and state are both missing.",
                count=0,
                dimension="completeness",
            )
        return None

    def _check_no_employees(self, company) -> QualityIssue | None:
        if not company.employee_count:
            return QualityIssue(
                check="no_employees",
                severity="INFO",
                message="Employee count is missing.",
                count=0,
                dimension="completeness",
            )
        return None

    def _check_no_revenue(self, company) -> QualityIssue | None:
        if not company.annual_revenue_million:
            return QualityIssue(
                check="no_revenue",
                severity="INFO",
                message="Annual revenue is missing.",
                count=0,
                dimension="completeness",
            )
        return None

    def _check_stale_enrichment(self, company) -> QualityIssue | None:
        if not company.collected_at:
            return QualityIssue(
                check="stale_enrichment",
                severity="WARNING",
                message="No collection timestamp — data may never have been enriched.",
                count=0,
                dimension="freshness",
            )
        age_days = (datetime.utcnow() - company.collected_at).days
        if age_days > _STALE_ENRICHMENT_DAYS:
            return QualityIssue(
                check="stale_enrichment",
                severity="WARNING",
                message=f"Enrichment data is {age_days} days old (threshold: {_STALE_ENRICHMENT_DAYS}).",
                count=age_days,
                dimension="freshness",
            )
        return None

    def _check_missing_services(self, company) -> QualityIssue | None:
        services = company.services or []
        if not services:
            return QualityIssue(
                check="missing_services",
                severity="INFO",
                message="No services listed — classification incomplete.",
                count=0,
                dimension="consistency",
            )
        return None
