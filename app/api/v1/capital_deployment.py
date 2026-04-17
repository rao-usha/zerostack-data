"""PE Intelligence — Capital Deployment API (PLAN_060 Phase 1)."""

from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.capital_deployment_ranker import CapitalDeploymentRanker

router = APIRouter(prefix="/pe/capital-deployment", tags=["PE Capital Deployment"])


@router.get("/rankings", summary="Top deployment opportunities")
def get_rankings(
    sector: Optional[str] = Query(None),
    min_score: float = Query(0, ge=0, le=100),
    top_n: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
) -> List[Dict]:
    return CapitalDeploymentRanker(db).rank_opportunities(sector=sector, min_score=min_score, top_n=top_n)


@router.get("/company/{company_id}", summary="Deep deployment score for a company")
def get_company_score(company_id: int, db: Session = Depends(get_db)) -> Dict:
    result = CapitalDeploymentRanker(db).score_single(company_id)
    if not result:
        from fastapi import HTTPException
        raise HTTPException(404, f"Company {company_id} not found")
    return result


@router.post("/scan", summary="Batch score all active companies")
def scan_all(db: Session = Depends(get_db)) -> Dict:
    return CapitalDeploymentRanker(db).scan_all()


@router.get("/sectors", summary="Sector-level deployment attractiveness")
def get_sectors(db: Session = Depends(get_db)) -> List[Dict]:
    return CapitalDeploymentRanker(db).get_sector_summary()


@router.get("/methodology", summary="Methodology documentation")
def get_methodology() -> Dict:
    from app.services.capital_deployment_ranker import WEIGHTS
    return {
        "model": "Capital Deployment Ranker v1.0",
        "formula": "min(100, weighted_sum * (1 + signals_above_70 * 0.05))",
        "signals": [{"signal": k, "weight": v} for k, v in WEIGHTS.items()],
        "grade_thresholds": {"A": 85, "B": 70, "C": 55, "D": 40, "F": 0},
        "actions": {"Strong deploy": "score >= 75", "Monitor": "60-74", "Watchlist": "45-59", "Pass": "<45"},
    }
