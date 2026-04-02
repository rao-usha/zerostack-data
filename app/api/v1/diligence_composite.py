"""
Company Diligence Composite API — Chain 2 of PLAN_052.

POST /diligence/score       — Score a company by name + optional filters
GET  /diligence/score/{name} — Quick score by name
"""
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import Optional
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.company_diligence_scorer import CompanyDiligenceScorer

router = APIRouter(prefix="/diligence", tags=["Diligence Composite"])


class DiligenceScoreRequest(BaseModel):
    company_name: str = Field(..., min_length=2, description="Company name to score")
    state: Optional[str] = Field(None, description="Two-letter state code (e.g. 'VA')")
    naics: Optional[str] = Field(None, description="NAICS code filter")


@router.post("/score")
def score_company(request: DiligenceScoreRequest, db: Session = Depends(get_db)):
    """
    Score a company across 6 diligence factors using 8 public data sources.

    Returns a 0-100 composite score with per-factor breakdown, red flags,
    and confidence based on how many sources matched.
    """
    scorer = CompanyDiligenceScorer(db)
    result = scorer.score_company(
        company_name=request.company_name,
        state=request.state,
        naics=request.naics,
    )
    return {
        "status": "ok",
        "company_name": result.company_name,
        "score": result.score,
        "grade": result.grade,
        "signal": result.signal,
        "recommendation": result.recommendation,
        "confidence": result.confidence,
        "sources_matched": result.sources_matched,
        "sources_empty": result.sources_empty,
        "red_flags": result.red_flags,
        "factors": [
            {
                "factor": f.factor,
                "score": f.score,
                "weight": f.weight,
                "reading": f.reading,
                "impact": f.impact,
                "data_source": f.data_source,
                "details": f.details,
            }
            for f in result.factors
        ],
    }


@router.get("/score/{name}")
def score_company_by_name(name: str, state: Optional[str] = None, db: Session = Depends(get_db)):
    """Quick score a company by name (URL path parameter)."""
    scorer = CompanyDiligenceScorer(db)
    result = scorer.score_company(company_name=name, state=state)
    return {
        "status": "ok",
        "company_name": result.company_name,
        "score": result.score,
        "grade": result.grade,
        "signal": result.signal,
        "confidence": result.confidence,
        "sources_matched": result.sources_matched,
        "red_flags": result.red_flags,
        "factors": [
            {
                "factor": f.factor,
                "score": f.score,
                "reading": f.reading,
                "impact": f.impact,
            }
            for f in result.factors
        ],
    }
