"""
Company Scoring API endpoints.

Provides access to ML-based company health scores.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.ml.company_scorer import CompanyScorer

router = APIRouter(prefix="/scores", tags=["Company Scores"])


@router.get("/company/{name}")
def get_company_score(
    name: str,
    refresh: bool = Query(False, description="Force recalculate score"),
    db: Session = Depends(get_db)
):
    """
    Get health score for a company.

    Calculates a composite score (0-100) based on:
    - Growth signals (employee growth, funding recency)
    - Stability signals (revenue, profitability, status)
    - Market position (web traffic, GitHub stars)
    - Tech velocity (GitHub activity, contributors)

    Returns score breakdown with explanation and confidence level.
    """
    scorer = CompanyScorer(db)
    result = scorer.score_company(name, force_refresh=refresh)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return result


@router.get("/portfolio/{investor_id}")
def get_portfolio_scores(
    investor_id: int,
    min_score: Optional[float] = Query(None, ge=0, le=100, description="Filter by minimum score"),
    tier: Optional[str] = Query(None, regex="^[A-F]$", description="Filter by tier (A-F)"),
    db: Session = Depends(get_db)
):
    """
    Get scores for all companies in an investor's portfolio.

    Returns:
    - Portfolio summary (total companies, average score, tier distribution)
    - Individual company scores sorted by score descending

    Use filters to focus on specific score ranges or tiers.
    """
    scorer = CompanyScorer(db)
    result = scorer.score_portfolio(investor_id, min_score=min_score, tier=tier)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return result


@router.get("/rankings")
def get_score_rankings(
    order: str = Query("top", regex="^(top|bottom)$", description="Ranking order"),
    limit: int = Query(20, ge=1, le=100, description="Number of results"),
    sector: Optional[str] = Query(None, description="Filter by sector"),
    min_confidence: float = Query(0.0, ge=0, le=1, description="Minimum confidence threshold"),
    db: Session = Depends(get_db)
):
    """
    Get top or bottom scored companies.

    Rankings are based on composite scores across all scored companies.
    Use filters to focus on specific sectors or confidence levels.
    """
    scorer = CompanyScorer(db)
    result = scorer.get_rankings(
        order=order,
        limit=limit,
        sector=sector,
        min_confidence=min_confidence
    )

    return result


@router.get("/methodology")
def get_scoring_methodology():
    """
    Get detailed scoring methodology documentation.

    Explains:
    - Category weights and signals
    - Tier definitions and thresholds
    - Confidence calculation
    - Caching policy
    """
    return CompanyScorer.get_methodology()


@router.post("/batch")
def batch_score_companies(
    company_names: list[str],
    db: Session = Depends(get_db)
):
    """
    Score multiple companies in a single request.

    Accepts a list of company names and returns scores for each.
    Companies without sufficient data will have lower confidence scores.
    """
    if len(company_names) > 50:
        raise HTTPException(
            status_code=400,
            detail="Maximum 50 companies per batch request"
        )

    scorer = CompanyScorer(db)
    results = []

    for name in company_names:
        try:
            score = scorer.score_company(name)
            results.append(score)
        except Exception as e:
            results.append({
                "company_name": name,
                "error": str(e),
            })

    # Summary stats
    valid_scores = [r["composite_score"] for r in results if "composite_score" in r]
    avg_score = sum(valid_scores) / len(valid_scores) if valid_scores else 0

    return {
        "total_requested": len(company_names),
        "scored": len(valid_scores),
        "failed": len(company_names) - len(valid_scores),
        "average_score": round(avg_score, 1),
        "results": results,
    }
