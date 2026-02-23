"""
Exit Readiness Score API endpoints.

Provides exit readiness scoring, rankings, and methodology docs
for PE portfolio companies.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.ml.exit_readiness_scorer import ExitReadinessScorer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/exit-readiness", tags=["exit_readiness"])


# ---------------------------------------------------------------------------
# GET /exit-readiness/methodology — static docs (MUST be before /{company_id})
# ---------------------------------------------------------------------------

@router.get(
    "/methodology",
    summary="Exit readiness methodology documentation",
    response_description="Weights, thresholds, and data source descriptions",
)
def get_methodology():
    """Return the Exit Readiness Score methodology, weights, and thresholds."""
    return ExitReadinessScorer.get_methodology()


# ---------------------------------------------------------------------------
# GET /exit-readiness/rankings — all companies ranked
# ---------------------------------------------------------------------------

@router.get(
    "/rankings",
    summary="Rank all PE companies by exit readiness",
    response_description="Ordered list of companies with scores",
)
def get_exit_readiness_rankings(
    grade: Optional[str] = Query(None, description="Filter by grade (A-F)"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Return all PE portfolio companies ranked by their latest Exit Readiness
    Score, with optional filters for grade and minimum confidence.
    """
    scorer = ExitReadinessScorer(db)  # ensure tables exist

    where_clauses = [
        "ers.model_version = :ver",
        "ers.confidence >= :min_conf",
    ]
    params: Dict[str, Any] = {
        "ver": "v1.0",
        "min_conf": min_confidence,
        "lim": limit,
        "off": offset,
    }

    if grade:
        where_clauses.append("ers.grade = :grade")
        params["grade"] = grade.upper()

    where_sql = " AND ".join(where_clauses)

    ranked_query = text(f"""
        SELECT * FROM (
            SELECT DISTINCT ON (ers.company_id)
                   ers.company_id,
                   pc.name AS company_name,
                   pc.industry,
                   pc.current_pe_owner,
                   ers.score_date,
                   ers.overall_score,
                   ers.grade,
                   ers.confidence,
                   ers.financial_health_score,
                   ers.financial_trajectory_score,
                   ers.leadership_stability_score,
                   ers.valuation_momentum_score,
                   ers.market_position_score,
                   ers.hold_period_score,
                   ers.hiring_signal_score,
                   ers.latest_revenue_usd,
                   ers.hold_years,
                   ers.strengths,
                   ers.risks
            FROM exit_readiness_scores ers
            LEFT JOIN pe_portfolio_companies pc ON pc.id = ers.company_id
            WHERE {where_sql}
            ORDER BY ers.company_id, ers.score_date DESC
        ) ranked
        ORDER BY ranked.overall_score DESC
        LIMIT :lim OFFSET :off
    """)

    try:
        rows = db.execute(ranked_query, params).mappings().fetchall()
    except Exception as e:
        logger.error(f"Exit readiness rankings query error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch rankings")

    rankings = []
    for i, r in enumerate(rows, start=offset + 1):
        rankings.append({
            "rank": i,
            "company_id": r["company_id"],
            "company_name": r["company_name"],
            "industry": r["industry"],
            "pe_owner": r["current_pe_owner"],
            "score_date": str(r["score_date"]),
            "overall_score": float(r["overall_score"]),
            "grade": r["grade"],
            "confidence": float(r["confidence"]) if r["confidence"] else 0,
            "sub_scores": {
                "financial_health": float(r["financial_health_score"]) if r["financial_health_score"] else None,
                "financial_trajectory": float(r["financial_trajectory_score"]) if r["financial_trajectory_score"] else None,
                "leadership_stability": float(r["leadership_stability_score"]) if r["leadership_stability_score"] else None,
                "valuation_momentum": float(r["valuation_momentum_score"]) if r["valuation_momentum_score"] else None,
                "market_position": float(r["market_position_score"]) if r["market_position_score"] else None,
                "hold_period": float(r["hold_period_score"]) if r["hold_period_score"] else None,
                "hiring_signal": float(r["hiring_signal_score"]) if r["hiring_signal_score"] else None,
            },
            "hold_years": float(r["hold_years"]) if r["hold_years"] else None,
            "strengths": r["strengths"] if r["strengths"] else [],
            "risks": r["risks"] if r["risks"] else [],
        })

    return {
        "total_returned": len(rankings),
        "filters": {"grade": grade, "min_confidence": min_confidence},
        "rankings": rankings,
    }


# ---------------------------------------------------------------------------
# POST /exit-readiness/compute — trigger scoring
# ---------------------------------------------------------------------------

@router.post(
    "/compute",
    summary="Trigger exit readiness score computation",
    response_description="Scoring result or batch job status",
)
def compute_exit_readiness(
    company_id: Optional[int] = Query(None, description="Score a single company (omit for all)"),
    force: bool = Query(False, description="Force recompute even if cached"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Trigger Exit Readiness Score computation.

    - Provide `company_id` to score a single company synchronously.
    - Omit `company_id` to batch-score all PE portfolio companies in the background.
    """
    scorer = ExitReadinessScorer(db)

    if company_id is not None:
        result = scorer.score_company(company_id, force=force)
        return {"mode": "single", "company_id": company_id, "result": result}

    def _batch_score():
        from app.core.database import get_db as _get_db
        gen = _get_db()
        session = next(gen)
        try:
            batch_scorer = ExitReadinessScorer(session)
            summary = batch_scorer.score_all_companies(force=force)
            logger.info(f"Batch exit readiness scoring complete: {summary}")
        finally:
            try:
                next(gen)
            except StopIteration:
                pass

    background_tasks.add_task(_batch_score)
    return {
        "mode": "batch",
        "status": "started",
        "message": "Batch exit readiness scoring launched in background. Check rankings for results.",
    }


# ---------------------------------------------------------------------------
# GET /exit-readiness/{company_id} — latest score + history (LAST — catch-all)
# ---------------------------------------------------------------------------

@router.get(
    "/{company_id}",
    summary="Get exit readiness score for a company",
    response_description="Latest score plus optional history",
)
def get_exit_readiness_score(
    company_id: int,
    days: int = Query(90, ge=1, le=365, description="History window in days"),
    db: Session = Depends(get_db),
):
    """
    Return the latest Exit Readiness Score and historical scores
    for the given PE portfolio company.
    """
    scorer = ExitReadinessScorer(db)

    latest = scorer.score_company(company_id)

    history_query = text("""
        SELECT score_date, overall_score, grade, confidence,
               financial_health_score, financial_trajectory_score,
               leadership_stability_score, valuation_momentum_score,
               market_position_score, hold_period_score, hiring_signal_score
        FROM exit_readiness_scores
        WHERE company_id = :cid
          AND score_date >= CURRENT_DATE - :days
        ORDER BY score_date DESC
    """)
    try:
        rows = db.execute(
            history_query, {"cid": company_id, "days": days}
        ).mappings().fetchall()
        history = [
            {
                "score_date": str(r["score_date"]),
                "overall_score": float(r["overall_score"]) if r["overall_score"] else None,
                "grade": r["grade"],
                "confidence": float(r["confidence"]) if r["confidence"] else None,
            }
            for r in rows
        ]
    except Exception:
        history = []

    return {
        "company_id": company_id,
        "latest": latest,
        "history": history,
        "history_days": days,
    }
