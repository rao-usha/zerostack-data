"""
PE Intelligence Platform — Exit Decision Engine (PLAN_060 Phase 3).

Composes 6 signals into an exit urgency score with timing, method
recommendation, and likely buyer matching.

Signals:
  - exit_readiness (0.25) — company operational readiness
  - macro_favorability (0.20) — sector momentum + convergence
  - lp_pressure (0.15) — fund age vs life + TVPI performance
  - buyer_appetite (0.15) — sector deal volume + multiples
  - transaction_probability (0.15) — P(transaction in 6-12mo)
  - valuation_trajectory (0.10) — multiple trend
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PEDeal,
    PEFirm,
    PEFund,
    PEFundInvestment,
    PEFundPerformance,
    PEMarketSignal,
    PEPortfolioCompany,
    PEPortfolioSnapshot,
)

logger = logging.getLogger(__name__)

WEIGHTS = {
    "exit_readiness": 0.25,
    "macro_favorability": 0.20,
    "lp_pressure": 0.15,
    "buyer_appetite": 0.15,
    "transaction_probability": 0.15,
    "valuation_trajectory": 0.10,
}

NEUTRAL = 50.0


def _grade(score: float) -> str:
    if score >= 80: return "A"
    if score >= 65: return "B"
    if score >= 50: return "C"
    if score >= 35: return "D"
    return "F"


def _exit_window(score: float) -> str:
    if score >= 80: return "This quarter or next (0-6 months)"
    if score >= 65: return "Next 6-12 months"
    if score >= 50: return "12-18 months"
    return "18+ months — continue value creation"


def _exit_method(revenue: float, buyer_appetite: float, exit_readiness: float) -> Dict:
    if revenue > 500_000_000:
        return {"method": "IPO", "confidence": 0.6, "rationale": "Revenue scale supports public offering"}
    if buyer_appetite > 70:
        return {"method": "Strategic Sale", "confidence": 0.8, "rationale": "High buyer appetite in sector — competitive process likely"}
    if exit_readiness > 75:
        return {"method": "Financial Sponsor", "confidence": 0.7, "rationale": "Strong metrics attract secondary buyout"}
    return {"method": "Strategic Sale", "confidence": 0.5, "rationale": "Default path — explore both strategic and sponsor interest"}


class ExitDecisionEngine:
    """Evaluate exit urgency + timing + method + buyers for portfolio companies."""

    def __init__(self, db: Session):
        self.db = db

    # -------------------------------------------------------------------
    # Single company evaluation
    # -------------------------------------------------------------------

    def evaluate(self, company_id: int) -> Optional[Dict]:
        company = self.db.query(PEPortfolioCompany).filter_by(id=company_id).first()
        if not company:
            return None

        inv = (
            self.db.query(PEFundInvestment)
            .filter_by(company_id=company_id, status="Active")
            .first()
        )

        signals = {}

        # 1. Exit readiness
        er = self._get_exit_readiness(company_id)
        signals["exit_readiness"] = er

        # 2. Macro favorability
        mf = self._get_macro_favorability(company.sector)
        signals["macro_favorability"] = mf

        # 3. LP pressure
        lp = self._get_lp_pressure(inv)
        signals["lp_pressure"] = lp

        # 4. Buyer appetite
        ba = self._get_buyer_appetite(company.sector)
        signals["buyer_appetite"] = ba

        # 5. Transaction probability
        tp = self._get_transaction_probability(company_id)
        signals["transaction_probability"] = tp

        # 6. Valuation trajectory
        vt = self._get_valuation_trajectory(company_id)
        signals["valuation_trajectory"] = vt

        # Composite
        raw = sum(signals[k] * WEIGHTS[k] for k in WEIGHTS)
        above_70 = sum(1 for v in signals.values() if v >= 70)
        urgency = min(100, raw * (1 + above_70 * 0.05))

        hold_years = 0
        fund_remaining = 0
        if inv and inv.investment_date:
            hold_years = round((date.today() - inv.investment_date).days / 365.25, 1)
        if inv:
            fund = self.db.query(PEFund).filter_by(id=inv.fund_id).first()
            if fund and fund.vintage_year and fund.fund_life_years:
                fund_remaining = max(0, (fund.vintage_year + fund.fund_life_years) - date.today().year)

        latest_rev = self._get_latest_revenue(company_id)
        method = _exit_method(latest_rev, ba, er)
        alternatives = self._alternative_methods(method["method"])
        buyers = self._find_likely_buyers(company)

        signal_chain = [
            {"signal": k, "score": round(signals[k], 1), "weight": WEIGHTS[k],
             "contribution": round(signals[k] * WEIGHTS[k], 1)}
            for k in WEIGHTS
        ]

        return {
            "company_id": company.id,
            "company_name": company.name,
            "sector": company.sector,
            "exit_urgency_score": round(urgency, 1),
            "exit_urgency_grade": _grade(urgency),
            "recommended_exit_window": _exit_window(urgency),
            "recommended_exit_method": method,
            "alternative_methods": alternatives,
            "signal_chain": signal_chain,
            "top_likely_buyers": buyers,
            "hold_period_years": hold_years,
            "fund_remaining_life_years": fund_remaining,
            "timing_rationale": self._timing_rationale(urgency, lp, ba, er),
        }

    # -------------------------------------------------------------------
    # Portfolio-level
    # -------------------------------------------------------------------

    def portfolio_exit_ranking(self, firm_id: int) -> List[Dict]:
        from app.core.pe_models import PEFund as Fund
        investments = (
            self.db.query(PEFundInvestment, PEPortfolioCompany)
            .join(PEPortfolioCompany, PEPortfolioCompany.id == PEFundInvestment.company_id)
            .join(Fund, Fund.id == PEFundInvestment.fund_id)
            .filter(Fund.firm_id == firm_id, PEFundInvestment.status == "Active")
            .all()
        )
        results = []
        for inv, company in investments:
            result = self.evaluate(company.id)
            if result:
                results.append(result)
        results.sort(key=lambda x: x["exit_urgency_score"], reverse=True)
        return results

    def timing_matrix(self, firm_id: int) -> Dict:
        portfolio = self.portfolio_exit_ranking(firm_id)
        quarters = ["Q3 2026", "Q4 2026", "Q1 2027", "Q2 2027", "H2 2027", "2028+"]
        matrix = []
        for r in portfolio:
            u = r["exit_urgency_score"]
            row = {"company_id": r["company_id"], "company_name": r["company_name"],
                   "urgency": u, "grade": r["exit_urgency_grade"]}
            # Map urgency to quarter
            if u >= 80:
                row["target_quarter"] = quarters[0]
            elif u >= 70:
                row["target_quarter"] = quarters[1]
            elif u >= 60:
                row["target_quarter"] = quarters[2]
            elif u >= 50:
                row["target_quarter"] = quarters[3]
            elif u >= 40:
                row["target_quarter"] = quarters[4]
            else:
                row["target_quarter"] = quarters[5]
            matrix.append(row)
        return {"firm_id": firm_id, "quarters": quarters, "companies": matrix}

    def find_buyers(self, company_id: int) -> List[Dict]:
        company = self.db.query(PEPortfolioCompany).filter_by(id=company_id).first()
        if not company:
            return []
        return self._find_likely_buyers(company)

    # -------------------------------------------------------------------
    # Signal fetchers
    # -------------------------------------------------------------------

    def _get_exit_readiness(self, company_id: int) -> float:
        snap = (
            self.db.query(PEPortfolioSnapshot)
            .filter_by(company_id=company_id)
            .order_by(PEPortfolioSnapshot.snapshot_date.desc())
            .first()
        )
        return float(snap.exit_score) if snap and snap.exit_score else NEUTRAL

    def _get_macro_favorability(self, sector: Optional[str]) -> float:
        if not sector:
            return NEUTRAL
        sig = self.db.query(PEMarketSignal).filter_by(sector=sector).order_by(
            PEMarketSignal.scanned_at.desc()
        ).first()
        return float(sig.momentum_score) if sig and sig.momentum_score else NEUTRAL

    def _get_lp_pressure(self, inv: Optional[PEFundInvestment]) -> float:
        if not inv:
            return NEUTRAL
        fund = self.db.query(PEFund).filter_by(id=inv.fund_id).first()
        if not fund or not fund.vintage_year or not fund.fund_life_years:
            return NEUTRAL
        age = date.today().year - fund.vintage_year
        life_pct = age / fund.fund_life_years
        # Sigmoid ramps at 70% of fund life
        age_pressure = 100 / (1 + math.exp(-8 * (life_pct - 0.7)))
        # Performance relief from TVPI
        perf = (
            self.db.query(PEFundPerformance)
            .filter_by(fund_id=fund.id)
            .order_by(PEFundPerformance.as_of_date.desc())
            .first()
        )
        tvpi = float(perf.tvpi) if perf and perf.tvpi else 1.0
        performance_relief = min(100, tvpi * 40)
        return max(0, min(100, age_pressure - performance_relief * 0.5))

    def _get_buyer_appetite(self, sector: Optional[str]) -> float:
        if not sector:
            return NEUTRAL
        try:
            row = self.db.execute(text(
                """SELECT COUNT(*) as deal_count,
                          COALESCE(AVG(ev_ebitda_multiple), 0) as avg_multiple
                   FROM pe_deals
                   WHERE deal_type != 'Exit'
                     AND announced_date >= CURRENT_DATE - INTERVAL '24 months'
                     AND company_id IN (SELECT id FROM pe_portfolio_companies WHERE sector = :sector)"""
            ), {"sector": sector}).mappings().first()
            if row:
                vol = min(100, int(row["deal_count"]) * 10)
                mult = min(100, float(row["avg_multiple"]) * 8)
                return vol * 0.6 + mult * 0.4
        except Exception:
            self.db.rollback()
        return NEUTRAL

    def _get_transaction_probability(self, company_id: int) -> float:
        try:
            row = self.db.execute(text(
                "SELECT probability FROM txn_prob_scores WHERE company_id = :cid ORDER BY scored_at DESC LIMIT 1"
            ), {"cid": company_id}).mappings().first()
            if row:
                return min(100, float(row["probability"]) * 100)
        except Exception:
            self.db.rollback()
        return NEUTRAL

    def _get_valuation_trajectory(self, company_id: int) -> float:
        try:
            rows = self.db.execute(text(
                """SELECT ebitda_margin_pct, revenue_growth_pct
                   FROM pe_company_financials
                   WHERE company_id = :cid
                   ORDER BY fiscal_year DESC LIMIT 2"""
            ), {"cid": company_id}).mappings().all()
            if len(rows) >= 2:
                margin_delta = float(rows[0].get("ebitda_margin_pct") or 0) - float(rows[1].get("ebitda_margin_pct") or 0)
                growth = float(rows[0].get("revenue_growth_pct") or 0)
                return min(100, max(0, 50 + margin_delta * 2 + growth * 1.5))
        except Exception:
            self.db.rollback()
        return NEUTRAL

    def _get_latest_revenue(self, company_id: int) -> float:
        try:
            row = self.db.execute(text(
                "SELECT revenue_usd FROM pe_company_financials WHERE company_id = :cid ORDER BY fiscal_year DESC LIMIT 1"
            ), {"cid": company_id}).mappings().first()
            if row and row["revenue_usd"]:
                return float(row["revenue_usd"])
        except Exception:
            self.db.rollback()
        return 0

    # -------------------------------------------------------------------
    # Buyer matching
    # -------------------------------------------------------------------

    def _find_likely_buyers(self, company: PEPortfolioCompany) -> List[Dict]:
        buyers = []
        sector = company.sector
        if not sector:
            return buyers

        firms = self.db.query(PEFirm).filter(PEFirm.status == "Active").all()
        for firm in firms:
            focus = firm.sector_focus or []
            if sector in focus:
                # Check deal history in this sector
                deal_count = 0
                try:
                    row = self.db.execute(text(
                        """SELECT COUNT(*) as c FROM pe_deals
                           WHERE buyer_name = :name
                             AND announced_date >= CURRENT_DATE - INTERVAL '36 months'"""
                    ), {"name": firm.name}).mappings().first()
                    deal_count = int(row["c"]) if row else 0
                except Exception:
                    self.db.rollback()

                relevance = 50 + (deal_count * 15)
                if firm.typical_check_size_min and firm.typical_check_size_max:
                    relevance += 10  # Has defined investment parameters
                buyers.append({
                    "firm_name": firm.name,
                    "firm_type": firm.firm_type,
                    "strategy": firm.primary_strategy,
                    "aum_usd_millions": float(firm.aum_usd_millions) if firm.aum_usd_millions else None,
                    "recent_sector_deals": deal_count,
                    "relevance_score": min(100, relevance),
                })

        buyers.sort(key=lambda x: x["relevance_score"], reverse=True)
        return buyers[:5]

    def _alternative_methods(self, primary: str) -> List[Dict]:
        all_methods = {
            "Strategic Sale": {"confidence": 0.6, "rationale": "Broad buyer universe for most sectors"},
            "Financial Sponsor": {"confidence": 0.5, "rationale": "Secondary buyout if valuation supports it"},
            "IPO": {"confidence": 0.3, "rationale": "Requires scale, growth narrative, and market window"},
            "Hold": {"confidence": 0.4, "rationale": "Continue value creation if signals are not yet aligned"},
        }
        return [
            {"method": m, **v}
            for m, v in all_methods.items()
            if m != primary
        ][:2]

    @staticmethod
    def _timing_rationale(urgency, lp_pressure, buyer_appetite, exit_readiness) -> str:
        parts = []
        if exit_readiness >= 70:
            parts.append("Company is operationally exit-ready")
        if lp_pressure >= 60:
            parts.append("fund age creates LP return pressure")
        if buyer_appetite >= 65:
            parts.append("sector buyer appetite is strong")
        if urgency >= 75:
            parts.append("recommend initiating process immediately")
        elif urgency >= 55:
            parts.append("prepare for exit within the next year")
        else:
            parts.append("continue building value before exiting")
        return ". ".join(parts) + "."
