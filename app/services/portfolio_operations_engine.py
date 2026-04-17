"""
PE Intelligence Platform — Portfolio Operations Engine (PLAN_060 Phase 2).

Aggregates financials, talent, and operational KPIs per firm and
per portfolio company for the "How are we doing?" command center.
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PECompanyFinancials,
    PECompanyLeadership,
    PEFirm,
    PEFund,
    PEFundInvestment,
    PEFundPerformance,
    PEPortfolioCompany,
    PEPortfolioSnapshot,
)

logger = logging.getLogger(__name__)


class PortfolioOperationsEngine:
    """Portfolio-level KPIs + per-company drill-down."""

    def __init__(self, db: Session):
        self.db = db

    # -------------------------------------------------------------------
    # Portfolio overview
    # -------------------------------------------------------------------

    def get_overview(self, firm_id: int) -> Dict:
        firm = self.db.query(PEFirm).filter_by(id=firm_id).first()
        if not firm:
            return {"error": f"Firm {firm_id} not found"}

        # Get all companies in this firm's funds
        investments = (
            self.db.query(PEFundInvestment, PEPortfolioCompany)
            .join(PEPortfolioCompany, PEPortfolioCompany.id == PEFundInvestment.company_id)
            .join(PEFund, PEFund.id == PEFundInvestment.fund_id)
            .filter(PEFund.firm_id == firm_id)
            .filter(PEFundInvestment.status == "Active")
            .all()
        )

        companies_data = []
        total_ev = Decimal("0")
        exit_scores = []
        growth_rates = []
        margins = []
        talent_ok = 0

        for inv, company in investments:
            latest_fin = self._latest_financials(company.id)
            latest_snap = self._latest_snapshot(company.id)
            leadership = self._leadership_summary(company.id)

            rev = float(latest_fin.get("revenue_usd") or 0)
            growth = float(latest_fin.get("revenue_growth_pct") or 0)
            margin = float(latest_fin.get("ebitda_margin_pct") or 0)
            ev = float(inv.entry_ev_usd or 0)
            total_ev += Decimal(str(ev))

            exit_score = float(latest_snap.get("exit_score") or 50)
            exit_grade = latest_snap.get("exit_grade") or "C"
            exit_scores.append(exit_score)
            if growth != 0:
                growth_rates.append(growth)
            if margin != 0:
                margins.append(margin)
            if leadership.get("has_ceo") and leadership.get("has_cfo"):
                talent_ok += 1

            hold_years = 0
            if inv.investment_date:
                hold_years = round((date.today() - inv.investment_date).days / 365.25, 1)

            status_signal = "green" if exit_score >= 65 else ("yellow" if exit_score >= 45 else "red")

            companies_data.append({
                "company_id": company.id,
                "company_name": company.name,
                "sector": company.sector,
                "investment_date": inv.investment_date.isoformat() if inv.investment_date else None,
                "hold_period_years": hold_years,
                "entry_ev_usd": ev,
                "latest_revenue_usd": rev,
                "revenue_growth_pct": growth,
                "ebitda_margin_pct": margin,
                "exit_readiness_score": exit_score,
                "exit_readiness_grade": exit_grade,
                "leadership": leadership,
                "status_signal": status_signal,
            })

        n = len(companies_data) or 1
        fund_perf = self._fund_performance(firm_id)
        sector_alloc = {}
        for c in companies_data:
            s = c.get("sector") or "Other"
            sector_alloc[s] = sector_alloc.get(s, 0) + 1

        kpis = {
            "total_aum_usd": float(total_ev),
            "portfolio_count": len(companies_data),
            "weighted_exit_readiness": round(sum(exit_scores) / n, 1) if exit_scores else 0,
            "revenue_growth_median": round(sorted(growth_rates)[len(growth_rates) // 2], 1) if growth_rates else 0,
            "ebitda_margin_median": round(sorted(margins)[len(margins) // 2], 1) if margins else 0,
            "talent_stability_index": round(talent_ok / n * 100, 0),
            "at_risk_count": sum(1 for s in exit_scores if s < 45),
        }

        return {
            "firm_id": firm_id,
            "firm_name": firm.name,
            "kpis": kpis,
            "companies": companies_data,
            "fund_performance": fund_perf,
            "sector_allocation": sector_alloc,
        }

    # -------------------------------------------------------------------
    # Per-company drill-down
    # -------------------------------------------------------------------

    def get_company_detail(self, company_id: int) -> Optional[Dict]:
        company = self.db.query(PEPortfolioCompany).filter_by(id=company_id).first()
        if not company:
            return None

        financials = self._financial_timeseries(company_id)
        leadership = self._full_leadership(company_id)
        snapshot = self._latest_snapshot(company_id)
        inv = (
            self.db.query(PEFundInvestment)
            .filter_by(company_id=company_id, status="Active")
            .first()
        )

        return {
            "company_id": company.id,
            "company_name": company.name,
            "sector": company.sector,
            "headquarters_state": company.headquarters_state,
            "employee_count": company.employee_count,
            "founded_year": company.founded_year,
            "ownership_status": company.ownership_status,
            "investment_date": inv.investment_date.isoformat() if inv and inv.investment_date else None,
            "entry_ev_usd": float(inv.entry_ev_usd) if inv and inv.entry_ev_usd else None,
            "entry_multiple": float(inv.entry_ev_ebitda_multiple) if inv and inv.entry_ev_ebitda_multiple else None,
            "financial_trajectory": financials,
            "leadership_team": leadership,
            "exit_readiness": snapshot,
        }

    def get_heatmap(self, firm_id: int) -> Dict:
        overview = self.get_overview(firm_id)
        metrics = ["revenue_growth_pct", "ebitda_margin_pct", "exit_readiness_score", "hold_period_years"]
        rows = []
        for c in overview.get("companies", []):
            row = {"company_id": c["company_id"], "company_name": c["company_name"], "sector": c["sector"]}
            for m in metrics:
                row[m] = c.get(m, 0)
            rows.append(row)
        return {"firm_id": firm_id, "metrics": metrics, "companies": rows}

    def get_financial_timeseries(self, company_id: int) -> List[Dict]:
        return self._financial_timeseries(company_id)

    def get_kpis(self, firm_id: int) -> Dict:
        overview = self.get_overview(firm_id)
        return {"firm_id": firm_id, "firm_name": overview.get("firm_name"), "kpis": overview.get("kpis", {})}

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    def _latest_financials(self, company_id: int) -> Dict:
        row = (
            self.db.query(PECompanyFinancials)
            .filter_by(company_id=company_id)
            .order_by(PECompanyFinancials.fiscal_year.desc())
            .first()
        )
        if not row:
            return {}
        return {
            "revenue_usd": float(row.revenue_usd) if row.revenue_usd else 0,
            "revenue_growth_pct": float(row.revenue_growth_pct) if row.revenue_growth_pct else 0,
            "ebitda_margin_pct": float(row.ebitda_margin_pct) if row.ebitda_margin_pct else 0,
            "ebitda_usd": float(row.ebitda_usd) if row.ebitda_usd else 0,
            "debt_to_ebitda": float(row.debt_to_ebitda) if row.debt_to_ebitda else 0,
            "fiscal_year": row.fiscal_year,
        }

    def _financial_timeseries(self, company_id: int) -> List[Dict]:
        rows = (
            self.db.query(PECompanyFinancials)
            .filter_by(company_id=company_id)
            .order_by(PECompanyFinancials.fiscal_year)
            .all()
        )
        return [
            {
                "fiscal_year": r.fiscal_year,
                "revenue_usd": float(r.revenue_usd) if r.revenue_usd else None,
                "revenue_growth_pct": float(r.revenue_growth_pct) if r.revenue_growth_pct else None,
                "ebitda_usd": float(r.ebitda_usd) if r.ebitda_usd else None,
                "ebitda_margin_pct": float(r.ebitda_margin_pct) if r.ebitda_margin_pct else None,
                "net_income_usd": float(r.net_income_usd) if r.net_income_usd else None,
                "free_cash_flow_usd": float(r.free_cash_flow_usd) if r.free_cash_flow_usd else None,
                "debt_to_ebitda": float(r.debt_to_ebitda) if r.debt_to_ebitda else None,
            }
            for r in rows
        ]

    def _latest_snapshot(self, company_id: int) -> Dict:
        row = (
            self.db.query(PEPortfolioSnapshot)
            .filter_by(company_id=company_id)
            .order_by(PEPortfolioSnapshot.snapshot_date.desc())
            .first()
        )
        if not row:
            return {"exit_score": 50, "exit_grade": "C"}
        return {
            "exit_score": float(row.exit_score) if row.exit_score else 50,
            "exit_grade": row.exit_grade or "C",
            "snapshot_date": row.snapshot_date.isoformat() if row.snapshot_date else None,
        }

    def _leadership_summary(self, company_id: int) -> Dict:
        leaders = (
            self.db.query(PECompanyLeadership)
            .filter_by(company_id=company_id, is_current=True)
            .all()
        )
        titles = [l.title for l in leaders]
        has_ceo = any("CEO" in (t or "").upper() or "CHIEF EXECUTIVE" in (t or "").upper() for t in titles)
        has_cfo = any("CFO" in (t or "").upper() or "CHIEF FINANCIAL" in (t or "").upper() for t in titles)
        return {"has_ceo": has_ceo, "has_cfo": has_cfo, "exec_count": len(leaders)}

    def _full_leadership(self, company_id: int) -> List[Dict]:
        leaders = (
            self.db.query(PECompanyLeadership)
            .filter_by(company_id=company_id, is_current=True)
            .all()
        )
        return [
            {
                "person_name": l.person_name,
                "title": l.title,
                "start_date": l.start_date.isoformat() if l.start_date else None,
                "appointed_by_pe": l.appointed_by_pe,
                "tenure_years": round((date.today() - l.start_date).days / 365.25, 1) if l.start_date else None,
            }
            for l in leaders
        ]

    def _fund_performance(self, firm_id: int) -> List[Dict]:
        funds = self.db.query(PEFund).filter_by(firm_id=firm_id).all()
        result = []
        for fund in funds:
            perf = (
                self.db.query(PEFundPerformance)
                .filter_by(fund_id=fund.id)
                .order_by(PEFundPerformance.as_of_date.desc())
                .first()
            )
            result.append({
                "fund_id": fund.id,
                "fund_name": fund.name,
                "vintage_year": fund.vintage_year,
                "status": fund.status,
                "target_size": float(fund.target_size_usd_millions) if fund.target_size_usd_millions else None,
                "irr": float(perf.net_irr_pct) if perf and perf.net_irr_pct else None,
                "tvpi": float(perf.tvpi) if perf and perf.tvpi else None,
                "dpi": float(perf.dpi) if perf and perf.dpi else None,
            })
        return result
