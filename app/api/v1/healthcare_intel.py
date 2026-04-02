"""
Healthcare Vertical Intelligence API — Chain 7 of PLAN_052.

Unified practice profiles for PE roll-up targeting.
"""
from fastapi import APIRouter, Depends, Query
from typing import Optional
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.healthcare_practice_scorer import HealthcarePracticeScorer

router = APIRouter(prefix="/healthcare", tags=["Healthcare Intelligence (Chain 7)"])


@router.get("/profiles")
def screen_practices(
    state: Optional[str] = Query(None, description="Filter by state (2-letter code)"),
    min_score: int = Query(0, ge=0, le=100),
    limit: int = Query(30, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Screen healthcare practices for acquisition. Returns ranked profiles
    with 5-factor breakdown: Market, Clinical, Competitive, Revenue, Multi-Unit.
    """
    scorer = HealthcarePracticeScorer(db)
    results = scorer.screen(state=state, min_score=min_score, limit=limit)
    return {
        "status": "ok",
        "total": len(results),
        "results": [
            {
                "prospect_id": r.prospect_id,
                "name": r.name,
                "city": r.city,
                "state": r.state,
                "zip_code": r.zip_code,
                "acquisition_score": r.acquisition_score,
                "grade": r.grade,
                "factors": [
                    {"factor": f.factor, "score": f.score, "reading": f.reading, "impact": f.impact}
                    for f in r.factors
                ],
                "details": r.details,
            }
            for r in results
        ],
    }


@router.get("/profiles/{prospect_id}")
def get_practice_profile(prospect_id: int, db: Session = Depends(get_db)):
    """Detailed practice profile with full 5-factor acquisition scoring."""
    scorer = HealthcarePracticeScorer(db)
    result = scorer.score_prospect(prospect_id)
    if not result:
        return {"status": "error", "detail": f"Prospect {prospect_id} not found"}
    return {
        "status": "ok",
        "prospect_id": result.prospect_id,
        "name": result.name,
        "city": result.city,
        "state": result.state,
        "zip_code": result.zip_code,
        "acquisition_score": result.acquisition_score,
        "grade": result.grade,
        "factors": [
            {"factor": f.factor, "score": f.score, "weight": f.weight, "reading": f.reading, "impact": f.impact}
            for f in result.factors
        ],
        "details": result.details,
    }
