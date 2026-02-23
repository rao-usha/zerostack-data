"""
Private Company Health Score API endpoints.

Provides multi-signal health scoring, rankings, and methodology docs
for companies without public financials.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.ml.health_scorer import PrivateCompanyHealthScorer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/health-scores", tags=["company_health"])


# ---------------------------------------------------------------------------
# GET /health-scores/methodology — static docs (MUST be before /{company_id})
# ---------------------------------------------------------------------------

@router.get(
    "/methodology",
    summary="Health score methodology documentation",
    response_description="Weights, thresholds, and data source descriptions",
)
def get_methodology():
    """Return the Private Company Health Score methodology, weights, and thresholds."""
    return PrivateCompanyHealthScorer.get_methodology()


# ---------------------------------------------------------------------------
# GET /health-scores/rankings — all companies ranked
# ---------------------------------------------------------------------------

@router.get(
    "/rankings",
    summary="Rank all scored companies by health score",
    response_description="Ordered list of companies with scores",
)
def get_health_rankings(
    grade: Optional[str] = Query(None, description="Filter by grade (A-F)"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Return all companies ranked by their latest Private Company Health Score,
    with optional filters for grade and minimum confidence.
    """
    scorer = PrivateCompanyHealthScorer(db)  # ensure tables exist

    where_clauses = [
        "chs.model_version = :ver",
        "chs.confidence >= :min_conf",
    ]
    params: Dict[str, Any] = {
        "ver": "v1.0",
        "min_conf": min_confidence,
        "lim": limit,
        "off": offset,
    }

    if grade:
        where_clauses.append("chs.grade = :grade")
        params["grade"] = grade.upper()

    where_sql = " AND ".join(where_clauses)

    ranked_query = text(f"""
        SELECT * FROM (
            SELECT DISTINCT ON (chs.company_id)
                   chs.company_id,
                   ic.name AS company_name,
                   chs.score_date,
                   chs.overall_score,
                   chs.grade,
                   chs.confidence,
                   chs.hiring_momentum_score,
                   chs.web_presence_score,
                   chs.employee_sentiment_score,
                   chs.foot_traffic_score,
                   chs.tranco_rank,
                   chs.glassdoor_rating,
                   chs.signals_available
            FROM company_health_scores chs
            LEFT JOIN industrial_companies ic ON ic.id = chs.company_id
            WHERE {where_sql}
            ORDER BY chs.company_id, chs.score_date DESC
        ) ranked
        ORDER BY ranked.overall_score DESC
        LIMIT :lim OFFSET :off
    """)

    try:
        rows = db.execute(ranked_query, params).mappings().fetchall()
    except Exception as e:
        logger.error(f"Health rankings query error: {e}")
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
            "sub_scores": {
                "hiring_momentum": float(r["hiring_momentum_score"]) if r["hiring_momentum_score"] else None,
                "web_presence": float(r["web_presence_score"]) if r["web_presence_score"] else None,
                "employee_sentiment": float(r["employee_sentiment_score"]) if r["employee_sentiment_score"] else None,
                "foot_traffic": float(r["foot_traffic_score"]) if r["foot_traffic_score"] else None,
            },
            "tranco_rank": r["tranco_rank"],
            "glassdoor_rating": float(r["glassdoor_rating"]) if r["glassdoor_rating"] else None,
        })

    return {
        "total_returned": len(rankings),
        "filters": {"grade": grade, "min_confidence": min_confidence},
        "rankings": rankings,
    }


# ---------------------------------------------------------------------------
# POST /health-scores/compute — trigger scoring
# ---------------------------------------------------------------------------

@router.post(
    "/compute",
    summary="Trigger health score computation",
    response_description="Scoring result or batch job status",
)
def compute_health_scores(
    company_id: Optional[int] = Query(None, description="Score a single company (omit for all)"),
    force: bool = Query(False, description="Force recompute even if cached"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Trigger Private Company Health Score computation.

    - Provide `company_id` to score a single company synchronously.
    - Omit `company_id` to batch-score all companies in the background.
    """
    scorer = PrivateCompanyHealthScorer(db)

    if company_id is not None:
        result = scorer.score_company(company_id, force=force)
        return {"mode": "single", "company_id": company_id, "result": result}

    # Batch mode — run in background
    def _batch_score():
        from app.core.database import get_db as _get_db
        gen = _get_db()
        session = next(gen)
        try:
            batch_scorer = PrivateCompanyHealthScorer(session)
            summary = batch_scorer.score_all_companies(force=force)
            logger.info(f"Batch health scoring complete: {summary}")
        finally:
            try:
                next(gen)
            except StopIteration:
                pass

    background_tasks.add_task(_batch_score)
    return {
        "mode": "batch",
        "status": "started",
        "message": "Batch health scoring launched in background. Check rankings for results.",
    }


# ---------------------------------------------------------------------------
# GET /health-scores/{company_id} — latest score + history (LAST — catch-all)
# ---------------------------------------------------------------------------

@router.get(
    "/{company_id}",
    summary="Get health score for a company",
    response_description="Latest score plus optional history",
)
def get_health_score(
    company_id: int,
    days: int = Query(90, ge=1, le=365, description="History window in days"),
    db: Session = Depends(get_db),
):
    """
    Return the latest Private Company Health Score and historical scores
    for the given company.
    """
    scorer = PrivateCompanyHealthScorer(db)

    # Latest score (compute if not cached)
    latest = scorer.score_company(company_id)

    # Fetch history
    history_query = text("""
        SELECT score_date, overall_score, grade, confidence,
               hiring_momentum_score, web_presence_score,
               employee_sentiment_score, foot_traffic_score,
               signals_available
        FROM company_health_scores
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
