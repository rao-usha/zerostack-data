"""
PE Portfolio Monitor.

Detects changes in exit readiness, financials, and leadership across
a PE firm's portfolio companies by comparing current state against
stored snapshots. Fires webhook alerts on significant changes.
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import select, and_, func
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PEAlert,
    PECompanyFinancials,
    PECompanyLeadership,
    PEFirm,
    PEFund,
    PEFundInvestment,
    PEPortfolioCompany,
    PEPortfolioSnapshot,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CompanyStatus:
    """Health status for one portfolio company."""
    company_id: int
    company_name: str
    exit_score: Optional[float] = None
    exit_grade: Optional[str] = None
    revenue: Optional[float] = None
    ebitda_margin: Optional[float] = None
    leadership_count: int = 0
    alert_count: int = 0
    trend: str = "stable"  # improving, stable, declining


@dataclass
class PortfolioHealthReport:
    """Full portfolio health check result."""
    firm_id: int
    firm_name: str
    check_date: str
    companies_checked: int = 0
    alerts_generated: int = 0
    company_statuses: List[CompanyStatus] = field(default_factory=list)
    alerts: List[Dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Pure comparison functions (no DB — fully testable)
# ---------------------------------------------------------------------------

def _detect_exit_changes(
    current: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Detect exit readiness grade boundary crossings.

    Args:
        current: Dict with company_id, company_name, exit_score, exit_grade.
        snapshot: Prior snapshot dict with exit_score, exit_grade, or None.

    Returns:
        List of alert dicts (may be empty).
    """
    if snapshot is None:
        return []  # first run — baseline, no alerts

    old_grade = snapshot.get("exit_grade")
    new_grade = current.get("exit_grade")

    if not old_grade or not new_grade or old_grade == new_grade:
        return []

    old_score = snapshot.get("exit_score", 0)
    new_score = current.get("exit_score", 0)
    direction = "improved" if new_score > old_score else "declined"

    severity = "warning" if direction == "declined" else "info"

    return [{
        "company_id": current["company_id"],
        "company_name": current["company_name"],
        "alert_type": "PE_EXIT_READINESS_CHANGE",
        "severity": severity,
        "title": f"{current['company_name']}: Exit readiness {direction} {old_grade} \u2192 {new_grade}",
        "detail": {
            "old_score": old_score,
            "new_score": new_score,
            "old_grade": old_grade,
            "new_grade": new_grade,
            "direction": direction,
        },
    }]


def _detect_financial_changes(
    current: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Detect significant financial changes.

    Flags: >10% revenue swing, >5pp EBITDA margin drop.
    """
    if snapshot is None:
        return []

    alerts = []
    company_id = current.get("company_id")
    company_name = current.get("company_name", "Unknown")

    # Revenue change
    old_rev = snapshot.get("revenue")
    new_rev = current.get("revenue")
    if old_rev and new_rev and old_rev > 0:
        pct_change = ((new_rev - old_rev) / old_rev) * 100
        if pct_change < -10:
            alerts.append({
                "company_id": company_id,
                "company_name": company_name,
                "alert_type": "PE_FINANCIAL_ALERT",
                "severity": "critical" if pct_change < -20 else "warning",
                "title": f"{company_name}: Revenue declined {pct_change:.1f}%",
                "detail": {
                    "metric": "revenue",
                    "old_value": old_rev,
                    "new_value": new_rev,
                    "pct_change": round(pct_change, 1),
                },
            })
        elif pct_change > 20:
            alerts.append({
                "company_id": company_id,
                "company_name": company_name,
                "alert_type": "PE_FINANCIAL_ALERT",
                "severity": "info",
                "title": f"{company_name}: Revenue surged +{pct_change:.1f}%",
                "detail": {
                    "metric": "revenue",
                    "old_value": old_rev,
                    "new_value": new_rev,
                    "pct_change": round(pct_change, 1),
                },
            })

    # EBITDA margin compression
    old_margin = snapshot.get("ebitda_margin")
    new_margin = current.get("ebitda_margin")
    if old_margin is not None and new_margin is not None:
        margin_delta = new_margin - old_margin
        if margin_delta < -5:
            alerts.append({
                "company_id": company_id,
                "company_name": company_name,
                "alert_type": "PE_FINANCIAL_ALERT",
                "severity": "warning",
                "title": f"{company_name}: EBITDA margin compressed {margin_delta:+.1f}pp",
                "detail": {
                    "metric": "ebitda_margin",
                    "old_value": old_margin,
                    "new_value": new_margin,
                    "delta_pp": round(margin_delta, 1),
                },
            })

    return alerts


def _detect_leadership_changes(
    current: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Detect C-suite additions and departures.

    Compares leader names between current and snapshot.
    """
    if snapshot is None:
        return []

    alerts = []
    company_id = current.get("company_id")
    company_name = current.get("company_name", "Unknown")

    current_leaders = {l["name"] for l in current.get("leaders", [])}
    snapshot_leaders = {l["name"] for l in snapshot.get("leaders", [])}

    # Departures
    departed = snapshot_leaders - current_leaders
    if departed:
        # Check if any departed were C-suite
        snapshot_by_name = {l["name"]: l for l in snapshot.get("leaders", [])}
        for name in departed:
            leader = snapshot_by_name.get(name, {})
            role = "CEO" if leader.get("is_ceo") else ("CFO" if leader.get("is_cfo") else "leader")
            alerts.append({
                "company_id": company_id,
                "company_name": company_name,
                "alert_type": "PE_LEADERSHIP_CHANGE",
                "severity": "critical" if leader.get("is_ceo") else "warning",
                "title": f"{company_name}: {role} departure — {name} left",
                "detail": {
                    "change_type": "departure",
                    "person_name": name,
                    "role": role,
                },
            })

    # Additions
    added = current_leaders - snapshot_leaders
    if added:
        current_by_name = {l["name"]: l for l in current.get("leaders", [])}
        for name in added:
            leader = current_by_name.get(name, {})
            role = "CEO" if leader.get("is_ceo") else ("CFO" if leader.get("is_cfo") else "leader")
            alerts.append({
                "company_id": company_id,
                "company_name": company_name,
                "alert_type": "PE_LEADERSHIP_CHANGE",
                "severity": "info",
                "title": f"{company_name}: New {role} addition — {name} joined",
                "detail": {
                    "change_type": "addition",
                    "person_name": name,
                    "role": role,
                },
            })

    return alerts


# ---------------------------------------------------------------------------
# DB-backed monitor methods
# ---------------------------------------------------------------------------

class PortfolioMonitorService:
    """Monitors PE portfolio companies for significant changes."""

    def __init__(self, db: Session):
        self.db = db

    def _get_portfolio_companies(self, firm_id: int) -> List[PEPortfolioCompany]:
        """Get all portfolio companies for a firm via fund investments."""
        fund_ids = self.db.execute(
            select(PEFund.id).where(PEFund.firm_id == firm_id)
        ).scalars().all()
        if not fund_ids:
            return []

        company_ids = self.db.execute(
            select(PEFundInvestment.company_id)
            .where(PEFundInvestment.fund_id.in_(fund_ids))
            .distinct()
        ).scalars().all()
        if not company_ids:
            return []

        return list(self.db.execute(
            select(PEPortfolioCompany)
            .where(PEPortfolioCompany.id.in_(company_ids))
        ).scalars().all())

    def _get_latest_snapshot(self, company_id: int) -> Optional[PEPortfolioSnapshot]:
        """Get most recent snapshot for a company."""
        return self.db.execute(
            select(PEPortfolioSnapshot)
            .where(PEPortfolioSnapshot.company_id == company_id)
            .order_by(PEPortfolioSnapshot.snapshot_date.desc())
            .limit(1)
        ).scalar_one_or_none()

    def _get_current_financials(self, company_id: int) -> Dict[str, Any]:
        """Get latest financial metrics for a company."""
        fin = self.db.execute(
            select(PECompanyFinancials)
            .where(PECompanyFinancials.company_id == company_id)
            .order_by(PECompanyFinancials.fiscal_year.desc())
            .limit(1)
        ).scalar_one_or_none()
        if not fin:
            return {}
        return {
            "revenue": float(fin.revenue_usd) if fin.revenue_usd else None,
            "ebitda_margin": float(fin.ebitda_margin_pct) if fin.ebitda_margin_pct else None,
            "ebitda": float(fin.ebitda_usd) if fin.ebitda_usd else None,
            "revenue_growth_pct": float(fin.revenue_growth_pct) if fin.revenue_growth_pct else None,
        }

    def _get_current_leaders(self, company_id: int) -> List[Dict[str, Any]]:
        """Get current leadership roster for a company."""
        from app.core.pe_models import PEPerson
        leaders = self.db.execute(
            select(PECompanyLeadership, PEPerson)
            .join(PEPerson, PEPerson.id == PECompanyLeadership.person_id)
            .where(
                PECompanyLeadership.company_id == company_id,
                PECompanyLeadership.is_current == True,
            )
        ).all()
        return [
            {
                "name": person.full_name,
                "is_ceo": l.is_ceo or False,
                "is_cfo": l.is_cfo or False,
                "is_board_member": l.is_board_member or False,
                "title": l.title,
            }
            for l, person in leaders
        ]

    def _store_snapshot(
        self, firm_id: int, company_id: int, exit_score: Optional[float],
        exit_grade: Optional[str], financials: Dict, leaders: List[Dict],
    ) -> PEPortfolioSnapshot:
        """Store a new portfolio snapshot."""
        snapshot = PEPortfolioSnapshot(
            company_id=company_id,
            firm_id=firm_id,
            snapshot_date=date.today(),
            exit_score=exit_score,
            exit_grade=exit_grade,
            revenue=financials.get("revenue"),
            ebitda_margin=financials.get("ebitda_margin"),
            ebitda=financials.get("ebitda"),
            revenue_growth_pct=financials.get("revenue_growth_pct"),
            leadership_count=len(leaders),
            has_ceo=any(l.get("is_ceo") for l in leaders),
            has_cfo=any(l.get("is_cfo") for l in leaders),
            data={"leaders": leaders, "financials": financials},
            data_source="portfolio_monitor",
        )
        self.db.add(snapshot)
        return snapshot

    def _store_alert(self, firm_id: int, alert: Dict[str, Any]) -> PEAlert:
        """Persist an alert record."""
        record = PEAlert(
            firm_id=firm_id,
            company_id=alert.get("company_id"),
            alert_type=alert["alert_type"],
            severity=alert.get("severity", "info"),
            title=alert["title"],
            detail=alert.get("detail"),
        )
        self.db.add(record)
        return record

    def monitor_exit_readiness(self, firm_id: int) -> List[Dict[str, Any]]:
        """Check exit readiness scores against snapshots, return alerts."""
        from app.core.pe_exit_scoring import score_exit_readiness

        companies = self._get_portfolio_companies(firm_id)
        all_alerts = []

        for company in companies:
            try:
                er = score_exit_readiness(self.db, company.id)
                if not er:
                    continue

                current = {
                    "company_id": company.id,
                    "company_name": company.name,
                    "exit_score": er.composite_score,
                    "exit_grade": er.grade,
                }

                snapshot = self._get_latest_snapshot(company.id)
                snapshot_data = {
                    "exit_score": float(snapshot.exit_score) if snapshot and snapshot.exit_score else None,
                    "exit_grade": snapshot.exit_grade if snapshot else None,
                } if snapshot else None

                alerts = _detect_exit_changes(current, snapshot_data)
                all_alerts.extend(alerts)
            except Exception as e:
                logger.warning("Exit readiness check failed for company %d: %s", company.id, e)

        return all_alerts

    def monitor_financials(self, firm_id: int) -> List[Dict[str, Any]]:
        """Check financials against snapshots, return alerts."""
        companies = self._get_portfolio_companies(firm_id)
        all_alerts = []

        for company in companies:
            financials = self._get_current_financials(company.id)
            if not financials:
                continue

            snapshot = self._get_latest_snapshot(company.id)
            snapshot_data = {
                "revenue": float(snapshot.revenue) if snapshot and snapshot.revenue else None,
                "ebitda_margin": float(snapshot.ebitda_margin) if snapshot and snapshot.ebitda_margin else None,
            } if snapshot else None

            current = {
                "company_id": company.id,
                "company_name": company.name,
                **financials,
            }

            alerts = _detect_financial_changes(current, snapshot_data)
            all_alerts.extend(alerts)

        return all_alerts

    def monitor_leadership(self, firm_id: int) -> List[Dict[str, Any]]:
        """Check leadership roster against snapshots, return alerts."""
        companies = self._get_portfolio_companies(firm_id)
        all_alerts = []

        for company in companies:
            leaders = self._get_current_leaders(company.id)

            snapshot = self._get_latest_snapshot(company.id)
            snapshot_data = None
            if snapshot and snapshot.data and "leaders" in snapshot.data:
                snapshot_data = {"leaders": snapshot.data["leaders"]}

            current = {
                "company_id": company.id,
                "company_name": company.name,
                "leaders": leaders,
            }

            alerts = _detect_leadership_changes(current, snapshot_data)
            all_alerts.extend(alerts)

        return all_alerts

    def run_full_portfolio_check(self, firm_id: int) -> PortfolioHealthReport:
        """Run all monitors, store snapshots, return health report."""
        from app.core.pe_exit_scoring import score_exit_readiness

        firm = self.db.execute(
            select(PEFirm).where(PEFirm.id == firm_id)
        ).scalar_one_or_none()
        if not firm:
            return PortfolioHealthReport(
                firm_id=firm_id, firm_name="Unknown",
                check_date=date.today().isoformat(),
            )

        companies = self._get_portfolio_companies(firm_id)
        all_alerts = []
        company_statuses = []

        for company in companies:
            # Gather current state
            financials = self._get_current_financials(company.id)
            leaders = self._get_current_leaders(company.id)

            exit_score = None
            exit_grade = None
            try:
                er = score_exit_readiness(self.db, company.id)
                if er:
                    exit_score = er.composite_score
                    exit_grade = er.grade
            except Exception:
                pass

            # Get snapshot for comparison
            snapshot = self._get_latest_snapshot(company.id)

            # Run detectors
            current_exit = {
                "company_id": company.id, "company_name": company.name,
                "exit_score": exit_score, "exit_grade": exit_grade,
            }
            snapshot_exit = {
                "exit_score": float(snapshot.exit_score) if snapshot and snapshot.exit_score else None,
                "exit_grade": snapshot.exit_grade if snapshot else None,
            } if snapshot else None

            current_fin = {"company_id": company.id, "company_name": company.name, **financials}
            snapshot_fin = {
                "revenue": float(snapshot.revenue) if snapshot and snapshot.revenue else None,
                "ebitda_margin": float(snapshot.ebitda_margin) if snapshot and snapshot.ebitda_margin else None,
            } if snapshot else None

            current_lead = {"company_id": company.id, "company_name": company.name, "leaders": leaders}
            snapshot_lead = {"leaders": snapshot.data["leaders"]} if snapshot and snapshot.data and "leaders" in snapshot.data else None

            alerts = []
            alerts.extend(_detect_exit_changes(current_exit, snapshot_exit))
            alerts.extend(_detect_financial_changes(current_fin, snapshot_fin))
            alerts.extend(_detect_leadership_changes(current_lead, snapshot_lead))

            # Store alerts
            for a in alerts:
                self._store_alert(firm_id, a)
            all_alerts.extend(alerts)

            # Store new snapshot
            self._store_snapshot(firm_id, company.id, exit_score, exit_grade, financials, leaders)

            # Determine trend
            trend = "stable"
            if snapshot and snapshot.exit_score and exit_score:
                delta = exit_score - float(snapshot.exit_score)
                if delta > 3:
                    trend = "improving"
                elif delta < -3:
                    trend = "declining"

            company_statuses.append(CompanyStatus(
                company_id=company.id,
                company_name=company.name,
                exit_score=exit_score,
                exit_grade=exit_grade,
                revenue=financials.get("revenue"),
                ebitda_margin=financials.get("ebitda_margin"),
                leadership_count=len(leaders),
                alert_count=len(alerts),
                trend=trend,
            ))

        self.db.commit()

        logger.info(
            "Portfolio check for firm %d: %d companies, %d alerts",
            firm_id, len(companies), len(all_alerts),
        )

        return PortfolioHealthReport(
            firm_id=firm_id,
            firm_name=firm.name,
            check_date=date.today().isoformat(),
            companies_checked=len(companies),
            alerts_generated=len(all_alerts),
            company_statuses=company_statuses,
            alerts=all_alerts,
        )
