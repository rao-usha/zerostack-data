"""PE Intelligence — Exit Strategy API (PLAN_060 Phase 3)."""

from typing import Dict, List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.exit_decision_engine import ExitDecisionEngine, WEIGHTS

router = APIRouter(prefix="/pe/exit-strategy", tags=["PE Exit Strategy"])


@router.get("/evaluate/{company_id}", summary="Full exit decision for a company")
def evaluate_exit(company_id: int, db: Session = Depends(get_db)) -> Dict:
    result = ExitDecisionEngine(db).evaluate(company_id)
    if not result:
        raise HTTPException(404, f"Company {company_id} not found")
    return result


@router.get("/portfolio/{firm_id}", summary="Portfolio ranked by exit urgency")
def portfolio_exit_ranking(firm_id: int, db: Session = Depends(get_db)) -> List[Dict]:
    return ExitDecisionEngine(db).portfolio_exit_ranking(firm_id)


@router.get("/timing-matrix/{firm_id}", summary="Exit timing matrix")
def timing_matrix(firm_id: int, db: Session = Depends(get_db)) -> Dict:
    return ExitDecisionEngine(db).timing_matrix(firm_id)


@router.get("/buyers/{company_id}", summary="Likely buyers for a company")
def find_buyers(company_id: int, db: Session = Depends(get_db)) -> List[Dict]:
    return ExitDecisionEngine(db).find_buyers(company_id)


@router.get("/methodology", summary="Exit decision methodology")
def get_methodology() -> Dict:
    return {
        "model": "Exit Decision Engine v1.0",
        "formula": "min(100, weighted_sum * (1 + signals_above_70 * 0.05))",
        "signals": [{"signal": k, "weight": v} for k, v in WEIGHTS.items()],
        "exit_windows": {
            "A (>=80)": "This quarter or next",
            "B (65-79)": "Next 6-12 months",
            "C (50-64)": "12-18 months",
            "D/F (<50)": "18+ months",
        },
        "exit_methods": ["Strategic Sale", "Financial Sponsor", "IPO", "Hold"],
        "buyer_matching": "Sector focus overlap + deal history + check size fit",
    }
