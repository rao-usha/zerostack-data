"""
Hiring Velocity Score API endpoints.

Provides scoring, rankings, and methodology docs for the
job-posting-based hiring velocity signal.
"""

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.sources.job_postings.velocity_scorer import HiringVelocityScorer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/job-postings/velocity", tags=["hiring_velocity"])


# ---------------------------------------------------------------------------
# GET /velocity/methodology — static docs (MUST be before /{company_id})
# ---------------------------------------------------------------------------

@router.get(
    "/methodology",
    summary="Scoring methodology documentation",
    response_description="Weights, thresholds, and data source descriptions",
)
def get_methodology():
    """Return the Hiring Velocity Score methodology, weights, and thresholds."""
    return HiringVelocityScorer.get_methodology()


# ---------------------------------------------------------------------------
# GET /velocity/rankings — all companies ranked
# ---------------------------------------------------------------------------

@router.get(
    "/rankings",
    summary="Rank all scored companies by velocity",
    response_description="Ordered list of companies with scores",
)
def get_velocity_rankings(
    grade: Optional[str] = Query(None, description="Filter by grade (A-F)"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Return all companies ranked by their latest Hiring Velocity Score,
    with optional filters for grade and minimum confidence.
    """
    scorer = HiringVelocityScorer(db)  # ensure table exists

    where_clauses = [
        "hvs.model_version = :ver",
        "hvs.confidence >= :min_conf",
    ]
    params: Dict[str, Any] = {
        "ver": "v1.0",
        "min_conf": min_confidence,
        "lim": limit,
        "off": offset,
    }

    if grade:
        where_clauses.append("hvs.grade = :grade")
        params["grade"] = grade.upper()

    where_sql = " AND ".join(where_clauses)

    ranked_query = text(f"""
        SELECT * FROM (
            SELECT DISTINCT ON (hvs.company_id)
                   hvs.company_id,
                   ic.name AS company_name,
                   hvs.score_date,
                   hvs.overall_score,
                   hvs.grade,
                   hvs.confidence,
                   hvs.total_open_postings,
                   hvs.posting_growth_score,
                   hvs.industry_relative_score,
                   hvs.momentum_score
            FROM hiring_velocity_scores hvs
            LEFT JOIN industrial_companies ic ON ic.id = hvs.company_id
            WHERE {where_sql}
            ORDER BY hvs.company_id, hvs.score_date DESC
        ) ranked
        ORDER BY ranked.overall_score DESC
        LIMIT :lim OFFSET :off
    """)

    try:
        rows = db.execute(ranked_query, params).mappings().fetchall()
    except Exception as e:
        logger.error(f"Rankings query error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch rankings")

    rankings = []
    for i, r in enumerate(rows, start=offset + 1):
        rankings.append({
            "rank": i,
            "company_id": r["company_id"],
            "company_name": r["company_name"],
            "score_date": str(r["score_date"]),
            "overall_score": float(r["overall_score"]),
            "grade": r["grade"],
            "confidence": float(r["confidence"]) if r["confidence"] else 0,
            "total_open_postings": r["total_open_postings"],
            "sub_scores": {
                "posting_growth": float(r["posting_growth_score"]) if r["posting_growth_score"] else None,
                "industry_relative": float(r["industry_relative_score"]) if r["industry_relative_score"] else None,
                "momentum": float(r["momentum_score"]) if r["momentum_score"] else None,
            },
        })

    return {
        "total_returned": len(rankings),
        "filters": {"grade": grade, "min_confidence": min_confidence},
        "rankings": rankings,
    }


# ---------------------------------------------------------------------------
# POST /velocity/compute — trigger scoring
# ---------------------------------------------------------------------------

@router.post(
    "/compute",
    summary="Trigger velocity scoring",
    response_description="Scoring job status",
)
def compute_velocity_scores(
    company_id: Optional[int] = Query(None, description="Score a single company (omit for all)"),
    force: bool = Query(False, description="Force recompute even if cached"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Trigger Hiring Velocity Score computation.

    - Provide `company_id` to score a single company synchronously.
    - Omit `company_id` to batch-score all eligible companies in the background.
    """
    scorer = HiringVelocityScorer(db)

    if company_id is not None:
        result = scorer.score_company(company_id, force=force)
        return {"mode": "single", "company_id": company_id, "result": result}

    # Batch mode — run in background
    def _batch_score():
        from app.core.database import get_db as _get_db
        gen = _get_db()
        session = next(gen)
        try:
            batch_scorer = HiringVelocityScorer(session)
            summary = batch_scorer.score_all_companies(force=force)
            logger.info(f"Batch velocity scoring complete: {summary}")
        finally:
            try:
                next(gen)
            except StopIteration:
                pass

    background_tasks.add_task(_batch_score)
    return {
        "mode": "batch",
        "status": "started",
        "message": "Batch scoring launched in background. Check rankings for results.",
    }


# ---------------------------------------------------------------------------
# GET /velocity/{company_id} — latest score + history (LAST — catch-all path)
# ---------------------------------------------------------------------------

@router.get(
    "/{company_id}",
    summary="Get hiring velocity score for a company",
    response_description="Latest score plus optional history",
)
def get_velocity_score(
    company_id: int,
    days: int = Query(90, ge=1, le=365, description="History window in days"),
    db: Session = Depends(get_db),
):
    """
    Return the latest Hiring Velocity Score and historical scores
    for the given company.
    """
    scorer = HiringVelocityScorer(db)

    # Latest score (compute if not cached)
    latest = scorer.score_company(company_id)

    # Fetch history
    history_query = text("""
        SELECT score_date, overall_score, grade, confidence,
               posting_growth_score, industry_relative_score,
               momentum_score, seniority_signal_score, dept_diversity_score,
               total_open_postings
        FROM hiring_velocity_scores
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
                "total_open_postings": r["total_open_postings"],
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
