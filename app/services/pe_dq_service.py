"""
PE Intelligence Data Quality Service.

Firm-level DQ checks for PE/VC data: deal completeness, news freshness,
portfolio coverage, LP data presence, and duplicate detection.

Implements BaseQualityProvider (dataset = "pe", entity = PE firm).
"""
from __future__ import annotations

import logging
import re
from datetime import date, timedelta

from sqlalchemy.orm import Session

from app.core.dq_base import (
    BaseQualityProvider,
    QualityIssue,
    QualityReport,
    _penalty,
    compute_quality_score,
)

logger = logging.getLogger(__name__)

_STALE_NEWS_DAYS = 90
_LOW_PORTFOLIO_THRESHOLD = 3
_LARGE_FIRM_AUM_MILLIONS = 500


class PEDQService(BaseQualityProvider):
    """Data quality checks for PE Intelligence data, scoped per PE firm."""

    dataset = "pe"

    # ------------------------------------------------------------------ #
    # BaseQualityProvider implementation                                   #
    # ------------------------------------------------------------------ #

    def run(self, entity_id: int, db: Session) -> QualityReport:
        """Run all checks for one PE firm."""
        from app.core.pe_models import PEFirm

        firm = db.query(PEFirm).filter(PEFirm.id == entity_id).first()
        if not firm:
            return QualityReport(
                entity_id=entity_id,
                entity_name="Unknown",
                dataset=self.dataset,
                quality_score=0,
                completeness=0,
                freshness=0,
                validity=0,
                consistency=0,
                issues=[QualityIssue("firm_not_found", "ERROR", "PE firm not found.", dimension="validity")],
            )

        checks = [
            self._check_no_deal_date,
            self._check_stale_news,
            self._check_low_portfolio_coverage,
            self._check_missing_lp_data,
            self._check_duplicate_firm_name,
            self._check_no_deal_amount,
        ]

        issues: list[QualityIssue] = []
        for fn in checks:
            try:
                result = fn(firm, db)
                if result:
                    issues.append(result)
            except Exception as exc:
                logger.warning("PE DQ check %s failed for firm %s: %s", fn.__name__, entity_id, exc)

        completeness = _penalty(issues, "completeness")
        freshness    = _penalty(issues, "freshness")
        validity     = _penalty(issues, "validity")
        consistency  = _penalty(issues, "consistency")

        return QualityReport(
            entity_id=entity_id,
            entity_name=firm.name,
            dataset=self.dataset,
            quality_score=compute_quality_score(completeness, freshness, validity, consistency),
            completeness=completeness,
            freshness=freshness,
            validity=validity,
            consistency=consistency,
            issues=issues,
        )

    def run_all(self, db: Session, limit: int | None = None) -> list[QualityReport]:
        """Run checks for all active PE firms, sorted worst-first."""
        from app.core.pe_models import PEFirm

        q = db.query(PEFirm).filter(PEFirm.status == "Active")
        if limit:
            q = q.limit(limit)
        firms = q.all()

        reports: list[QualityReport] = []
        for firm in firms:
            try:
                reports.append(self.run(firm.id, db))
            except Exception as exc:
                logger.error("PE DQ run failed for firm %s: %s", firm.id, exc)

        reports.sort(key=lambda r: r.quality_score)
        logger.info(
            "PE DQ run complete: %d firms, avg score %.0f",
            len(reports),
            sum(r.quality_score for r in reports) / max(len(reports), 1),
        )
        return reports

    # ------------------------------------------------------------------ #
    # Individual checks                                                    #
    # ------------------------------------------------------------------ #

    def _check_no_deal_date(self, firm, db: Session) -> QualityIssue | None:
        """Deals without announced_date or closed_date."""
        from app.core.pe_models import PEDeal, PEPortfolioCompany, PEFundInvestment, PEFund

        undated = (
            db.query(PEDeal)
            .join(PEPortfolioCompany, PEDeal.company_id == PEPortfolioCompany.id)
            .join(PEFundInvestment, PEFundInvestment.company_id == PEPortfolioCompany.id)
            .join(PEFund, PEFundInvestment.fund_id == PEFund.id)
            .filter(
                PEFund.firm_id == firm.id,
                PEDeal.announced_date.is_(None),
                PEDeal.closed_date.is_(None),
            )
            .count()
        )
        if undated > 0:
            return QualityIssue(
                check="no_deal_date",
                severity="WARNING",
                message=f"{undated} deal(s) missing both announced_date and closed_date.",
                count=undated,
                dimension="validity",
            )
        return None

    def _check_no_deal_amount(self, firm, db: Session) -> QualityIssue | None:
        """Deals without enterprise_value_usd (common for private deals, so INFO only)."""
        from app.core.pe_models import PEDeal, PEPortfolioCompany, PEFundInvestment, PEFund

        no_amount = (
            db.query(PEDeal)
            .join(PEPortfolioCompany, PEDeal.company_id == PEPortfolioCompany.id)
            .join(PEFundInvestment, PEFundInvestment.company_id == PEPortfolioCompany.id)
            .join(PEFund, PEFundInvestment.fund_id == PEFund.id)
            .filter(
                PEFund.firm_id == firm.id,
                PEDeal.enterprise_value_usd.is_(None),
            )
            .count()
        )
        if no_amount > 0:
            return QualityIssue(
                check="no_deal_amount",
                severity="INFO",
                message=f"{no_amount} deal(s) missing enterprise_value_usd (common for private transactions).",
                count=no_amount,
                dimension="consistency",
            )
        return None

    def _check_stale_news(self, firm, db: Session) -> QualityIssue | None:
        """No PEFirmNews in last 90 days for an active firm."""
        from app.core.pe_models import PEFirmNews

        cutoff = date.today() - timedelta(days=_STALE_NEWS_DAYS)
        recent = (
            db.query(PEFirmNews)
            .filter(
                PEFirmNews.firm_id == firm.id,
                PEFirmNews.published_date >= cutoff,
            )
            .count()
        )
        if recent == 0:
            return QualityIssue(
                check="stale_news",
                severity="WARNING",
                message=f"No news articles collected in the last {_STALE_NEWS_DAYS} days.",
                count=0,
                dimension="freshness",
            )
        return None

    def _check_low_portfolio_coverage(self, firm, db: Session) -> QualityIssue | None:
        """Fewer than 3 portfolio companies for a firm with >$500M AUM."""
        from app.core.pe_models import PEPortfolioCompany, PEFundInvestment, PEFund

        aum = float(firm.aum_usd_millions or 0)
        if aum < _LARGE_FIRM_AUM_MILLIONS:
            return None

        portfolio_count = (
            db.query(PEPortfolioCompany)
            .join(PEFundInvestment, PEFundInvestment.company_id == PEPortfolioCompany.id)
            .join(PEFund, PEFundInvestment.fund_id == PEFund.id)
            .filter(PEFund.firm_id == firm.id)
            .distinct()
            .count()
        )
        if portfolio_count < _LOW_PORTFOLIO_THRESHOLD:
            return QualityIssue(
                check="low_portfolio_coverage",
                severity="WARNING",
                message=(
                    f"Only {portfolio_count} portfolio companies found for a firm with "
                    f"${aum:,.0f}M AUM — likely incomplete collection."
                ),
                count=portfolio_count,
                dimension="completeness",
            )
        return None

    def _check_missing_lp_data(self, firm, db: Session) -> QualityIssue | None:
        """Funds with no PEFundInvestment rows (no LP commitment data)."""
        from app.core.pe_models import PEFund, PEFundInvestment

        funds_without_lp = (
            db.query(PEFund)
            .outerjoin(PEFundInvestment, PEFundInvestment.fund_id == PEFund.id)
            .filter(PEFund.firm_id == firm.id, PEFundInvestment.id.is_(None))
            .count()
        )
        if funds_without_lp > 0:
            return QualityIssue(
                check="missing_lp_data",
                severity="INFO",
                message=f"{funds_without_lp} fund(s) have no LP commitment records.",
                count=funds_without_lp,
                dimension="completeness",
            )
        return None

    def _check_duplicate_firm_name(self, firm, db: Session) -> QualityIssue | None:
        """Two PEFirm rows with the same normalized name (likely duplicates)."""
        from app.core.pe_models import PEFirm

        normalized = _normalize_firm_name(firm.name)
        all_firms = db.query(PEFirm.id, PEFirm.name).all()
        matches = [f for f in all_firms if _normalize_firm_name(f.name) == normalized and f.id != firm.id]
        if matches:
            names = ", ".join(m.name for m in matches[:3])
            return QualityIssue(
                check="duplicate_firm_name",
                severity="ERROR",
                message=f"Possible duplicate firm records: {names}.",
                count=len(matches),
                dimension="validity",
            )
        return None


def _normalize_firm_name(name: str) -> str:
    """Normalize firm name for duplicate detection."""
    name = name.lower().strip()
    name = re.sub(r"\b(llc|lp|inc|ltd|llp|gp|co|corp|management|capital|partners|group|advisors?|investments?)\b", "", name)
    name = re.sub(r"[^\w\s]", "", name)
    return re.sub(r"\s+", " ", name).strip()
