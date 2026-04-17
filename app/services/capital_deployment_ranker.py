"""
PE Intelligence Platform — Capital Deployment Ranker (PLAN_060 Phase 1).

Composes 5 existing scorers into a unified "where to deploy capital"
ranking with explainable signal chains.

Signals:
  - deal_probability (0.25) — P(transaction in 6-12 months)
  - target_attractiveness (0.25) — acquisition target quality
  - sector_momentum (0.20) — pe_market_signals momentum
  - fund_conviction (0.15) — LP base quality for deploying fund
  - macro_tailwind (0.15) — convergence + sector macro
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.pe_models import PEPortfolioCompany, PEMarketSignal

logger = logging.getLogger(__name__)

WEIGHTS = {
    "deal_probability": 0.25,
    "target_attractiveness": 0.25,
    "sector_momentum": 0.20,
    "fund_conviction": 0.15,
    "macro_tailwind": 0.15,
}

NEUTRAL = 50.0


@dataclass
class DeploymentSignal:
    signal: str
    score: float
    weight: float
    contribution: float
    reading: str


@dataclass
class DeploymentOpportunity:
    company_id: int
    company_name: str
    sector: str
    deployment_score: float
    deployment_grade: str
    signals: List[DeploymentSignal]
    recommended_action: str
    timing_window: str
    confidence: float


def _grade(score: float) -> str:
    if score >= 85: return "A"
    if score >= 70: return "B"
    if score >= 55: return "C"
    if score >= 40: return "D"
    return "F"


def _action(score: float) -> str:
    if score >= 75: return "Strong deploy — fast-track diligence"
    if score >= 60: return "Monitor closely — schedule intro"
    if score >= 45: return "Watchlist — re-score quarterly"
    return "Pass — insufficient signal strength"


def _timing(score: float) -> str:
    if score >= 75: return "Next 3-6 months"
    if score >= 60: return "6-12 months"
    return "12+ months"


class CapitalDeploymentRanker:
    """Rank PE portfolio companies by capital deployment attractiveness."""

    def __init__(self, db: Session):
        self.db = db

    def rank_opportunities(
        self,
        sector: Optional[str] = None,
        min_score: float = 0,
        top_n: int = 20,
    ) -> List[Dict]:
        q = self.db.query(PEPortfolioCompany).filter(
            PEPortfolioCompany.status == "Active"
        )
        if sector:
            q = q.filter(PEPortfolioCompany.sector == sector)

        companies = q.all()
        results = []
        for c in companies:
            opp = self._score_company(c)
            if opp.deployment_score >= min_score:
                results.append(opp)

        results.sort(key=lambda x: x.deployment_score, reverse=True)
        return [self._to_dict(r) for r in results[:top_n]]

    def score_single(self, company_id: int) -> Optional[Dict]:
        c = self.db.query(PEPortfolioCompany).filter_by(id=company_id).first()
        if not c:
            return None
        return self._to_dict(self._score_company(c))

    def scan_all(self) -> Dict:
        companies = self.db.query(PEPortfolioCompany).filter_by(status="Active").all()
        scored = 0
        for c in companies:
            self._score_company(c)
            scored += 1
        return {"total": len(companies), "scored": scored}

    def get_sector_summary(self) -> List[Dict]:
        companies = self.db.query(PEPortfolioCompany).filter_by(status="Active").all()
        sector_scores: Dict[str, List[float]] = {}
        for c in companies:
            opp = self._score_company(c)
            sector_scores.setdefault(c.sector or "Other", []).append(opp.deployment_score)
        return [
            {
                "sector": s,
                "company_count": len(scores),
                "avg_score": round(sum(scores) / len(scores), 1),
                "max_score": round(max(scores), 1),
                "grade": _grade(sum(scores) / len(scores)),
            }
            for s, scores in sorted(sector_scores.items(), key=lambda x: -sum(x[1]) / len(x[1]))
        ]

    def _score_company(self, company: PEPortfolioCompany) -> DeploymentOpportunity:
        signals = []

        # 1. Deal probability
        dp_score = self._get_deal_probability(company)
        signals.append(DeploymentSignal("deal_probability", dp_score, WEIGHTS["deal_probability"],
                                         dp_score * WEIGHTS["deal_probability"], self._reading(dp_score, "transaction likelihood")))

        # 2. Target attractiveness
        ta_score = self._get_target_attractiveness(company)
        signals.append(DeploymentSignal("target_attractiveness", ta_score, WEIGHTS["target_attractiveness"],
                                         ta_score * WEIGHTS["target_attractiveness"], self._reading(ta_score, "target quality")))

        # 3. Sector momentum
        sm_score = self._get_sector_momentum(company.sector)
        signals.append(DeploymentSignal("sector_momentum", sm_score, WEIGHTS["sector_momentum"],
                                         sm_score * WEIGHTS["sector_momentum"], self._reading(sm_score, "sector heat")))

        # 4. Fund conviction
        fc_score = self._get_fund_conviction(company)
        signals.append(DeploymentSignal("fund_conviction", fc_score, WEIGHTS["fund_conviction"],
                                         fc_score * WEIGHTS["fund_conviction"], self._reading(fc_score, "LP confidence")))

        # 5. Macro tailwind
        mt_score = self._get_macro_tailwind(company)
        signals.append(DeploymentSignal("macro_tailwind", mt_score, WEIGHTS["macro_tailwind"],
                                         mt_score * WEIGHTS["macro_tailwind"], self._reading(mt_score, "macro environment")))

        raw = sum(s.contribution for s in signals)
        above_70 = sum(1 for s in signals if s.score >= 70)
        composite = min(100, raw * (1 + above_70 * 0.05))
        confidence = sum(1 for s in signals if s.score != NEUTRAL) / len(signals)

        return DeploymentOpportunity(
            company_id=company.id,
            company_name=company.name,
            sector=company.sector or "Other",
            deployment_score=round(composite, 1),
            deployment_grade=_grade(composite),
            signals=signals,
            recommended_action=_action(composite),
            timing_window=_timing(composite),
            confidence=round(confidence, 2),
        )

    # --- Signal fetchers (graceful defaults) ---

    def _get_deal_probability(self, company) -> float:
        try:
            row = self.db.execute(text(
                "SELECT probability FROM txn_prob_scores WHERE company_id = :cid ORDER BY scored_at DESC LIMIT 1"
            ), {"cid": company.id}).mappings().first()
            if row:
                return min(100, float(row["probability"]) * 100)
        except Exception:
            self.db.rollback()
        return NEUTRAL

    def _get_target_attractiveness(self, company) -> float:
        try:
            row = self.db.execute(text(
                "SELECT overall_score FROM acquisition_target_scores WHERE company_id = :cid ORDER BY scored_at DESC LIMIT 1"
            ), {"cid": company.id}).mappings().first()
            if row:
                return float(row["overall_score"])
        except Exception:
            self.db.rollback()
        # Derive from financials if no direct score
        try:
            fin = self.db.execute(text(
                "SELECT revenue_growth_pct, ebitda_margin_pct FROM pe_company_financials WHERE company_id = :cid ORDER BY fiscal_year DESC LIMIT 1"
            ), {"cid": company.id}).mappings().first()
            if fin:
                growth = float(fin.get("revenue_growth_pct") or 0)
                margin = float(fin.get("ebitda_margin_pct") or 0)
                return min(100, max(0, 30 + growth * 1.5 + margin * 1.2))
        except Exception:
            self.db.rollback()
        return NEUTRAL

    def _get_sector_momentum(self, sector: Optional[str]) -> float:
        if not sector:
            return NEUTRAL
        try:
            row = self.db.query(PEMarketSignal).filter_by(sector=sector).order_by(
                PEMarketSignal.scanned_at.desc()
            ).first()
            if row:
                return float(row.momentum_score or NEUTRAL)
        except Exception:
            self.db.rollback()
        return NEUTRAL

    def _get_fund_conviction(self, company) -> float:
        try:
            row = self.db.execute(text(
                """SELECT cs.composite_score FROM pe_fund_conviction_scores cs
                   JOIN pe_fund_investments fi ON cs.fund_id = fi.fund_id
                   WHERE fi.company_id = :cid AND fi.status = 'Active'
                   ORDER BY cs.scored_at DESC LIMIT 1"""
            ), {"cid": company.id}).mappings().first()
            if row:
                return float(row["composite_score"])
        except Exception:
            self.db.rollback()
        return NEUTRAL

    def _get_macro_tailwind(self, company) -> float:
        try:
            if company.headquarters_state:
                row = self.db.execute(text(
                    """SELECT convergence_score FROM convergence_regions
                       WHERE :state = ANY(SELECT jsonb_array_elements_text(states::jsonb))
                       ORDER BY scored_at DESC LIMIT 1"""
                ), {"state": company.headquarters_state}).mappings().first()
                if row:
                    return float(row["convergence_score"])
        except Exception:
            self.db.rollback()
        return NEUTRAL

    @staticmethod
    def _reading(score: float, label: str) -> str:
        if score >= 75: return f"Strong {label}"
        if score >= 55: return f"Moderate {label}"
        if score >= 35: return f"Weak {label}"
        return f"Very weak {label}"

    @staticmethod
    def _to_dict(opp: DeploymentOpportunity) -> Dict:
        return {
            "company_id": opp.company_id,
            "company_name": opp.company_name,
            "sector": opp.sector,
            "deployment_score": opp.deployment_score,
            "deployment_grade": opp.deployment_grade,
            "recommended_action": opp.recommended_action,
            "timing_window": opp.timing_window,
            "confidence": opp.confidence,
            "signals": [
                {"signal": s.signal, "score": round(s.score, 1), "weight": s.weight,
                 "contribution": round(s.contribution, 1), "reading": s.reading}
                for s in opp.signals
            ],
        }
