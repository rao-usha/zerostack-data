"""
Acquisition Target Score API endpoints.

Provides acquisition target scoring, rankings, and methodology docs
for PE portfolio companies.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.ml.acquisition_target_scorer import AcquisitionTargetScorer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/acquisition-targets", tags=["acquisition_targets"])


# ---------------------------------------------------------------------------
# GET /acquisition-targets/methodology — static docs (MUST be before /{company_id})
# ---------------------------------------------------------------------------

@router.get(
    "/methodology",
    summary="Acquisition target methodology documentation",
    response_description="Weights, thresholds, and data source descriptions",
)
def get_methodology():
    """Return the Acquisition Target Score methodology, weights, and thresholds."""
    return AcquisitionTargetScorer.get_methodology()


# ---------------------------------------------------------------------------
# GET /acquisition-targets/rankings — all companies ranked
# ---------------------------------------------------------------------------

@router.get(
    "/rankings",
    summary="Rank all PE companies by acquisition attractiveness",
    response_description="Ordered list of companies with scores",
)
def get_acquisition_target_rankings(
    grade: Optional[str] = Query(None, description="Filter by grade (A-F)"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Return all PE portfolio companies ranked by their latest Acquisition
    Target Score, with optional filters for grade and minimum confidence.
    """
    scorer = AcquisitionTargetScorer(db)  # ensure tables exist

    where_clauses = [
        "ats.model_version = :ver",
        "ats.confidence >= :min_conf",
    ]
    params: Dict[str, Any] = {
        "ver": "v1.0",
        "min_conf": min_confidence,
        "lim": limit,
        "off": offset,
    }

    if grade:
        where_clauses.append("ats.grade = :grade")
        params["grade"] = grade.upper()

    where_sql = " AND ".join(where_clauses)

    ranked_query = text(f"""
        SELECT * FROM (
            SELECT DISTINCT ON (ats.company_id)
                   ats.company_id,
                   pc.name AS company_name,
                   pc.industry,
                   pc.sector,
                   pc.current_pe_owner,
                   ats.score_date,
                   ats.overall_score,
                   ats.grade,
                   ats.confidence,
                   ats.growth_signal_score,
                   ats.market_attractiveness_score,
                   ats.management_gap_score,
                   ats.deal_activity_score,
                   ats.sector_momentum_score,
                   ats.revenue_growth_pct,
                   ats.employee_count,
                   ats.leadership_count,
                   ats.sector_pe_deal_count,
                   ats.strengths,
                   ats.risks
            FROM acquisition_target_scores ats
            LEFT JOIN pe_portfolio_companies pc ON pc.id = ats.company_id
            WHERE {where_sql}
            ORDER BY ats.company_id, ats.score_date DESC
        ) ranked
        ORDER BY ranked.overall_score DESC
        LIMIT :lim OFFSET :off
    """)

    try:
        rows = db.execute(ranked_query, params).mappings().fetchall()
    except Exception as e:
        logger.error(f"Acquisition target rankings query error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch rankings")

    rankings = []
    for i, r in enumerate(rows, start=offset + 1):
        rankings.append({
            "rank": i,
            "company_id": r["company_id"],
            "company_name": r["company_name"],
            "industry": r["industry"],
            "sector": r["sector"],
            "pe_owner": r["current_pe_owner"],
            "score_date": str(r["score_date"]),
            "overall_score": float(r["overall_score"]),
            "grade": r["grade"],
            "confidence": float(r["confidence"]) if r["confidence"] else 0,
            "sub_scores": {
                "growth_signal": float(r["growth_signal_score"]) if r["growth_signal_score"] else None,
                "market_attractiveness": float(r["market_attractiveness_score"]) if r["market_attractiveness_score"] else None,
                "management_gap": float(r["management_gap_score"]) if r["management_gap_score"] else None,
                "deal_activity": float(r["deal_activity_score"]) if r["deal_activity_score"] else None,
                "sector_momentum": float(r["sector_momentum_score"]) if r["sector_momentum_score"] else None,
            },
            "revenue_growth_pct": float(r["revenue_growth_pct"]) if r["revenue_growth_pct"] else None,
            "strengths": r["strengths"] if r["strengths"] else [],
            "risks": r["risks"] if r["risks"] else [],
        })

    return {
        "total_returned": len(rankings),
        "filters": {"grade": grade, "min_confidence": min_confidence},
        "rankings": rankings,
    }


# ---------------------------------------------------------------------------
# POST /acquisition-targets/compute — trigger scoring
# ---------------------------------------------------------------------------

@router.post(
    "/compute",
    summary="Trigger acquisition target score computation",
    response_description="Scoring result or batch job status",
)
def compute_acquisition_targets(
    company_id: Optional[int] = Query(None, description="Score a single company (omit for all)"),
    force: bool = Query(False, description="Force recompute even if cached"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Trigger Acquisition Target Score computation.

    - Provide `company_id` to score a single company synchronously.
    - Omit `company_id` to batch-score all PE portfolio companies in the background.
    """
    scorer = AcquisitionTargetScorer(db)

    if company_id is not None:
        result = scorer.score_company(company_id, force=force)
        return {"mode": "single", "company_id": company_id, "result": result}

    def _batch_score():
        from app.core.database import get_db as _get_db
        gen = _get_db()
        session = next(gen)
        try:
            batch_scorer = AcquisitionTargetScorer(session)
            summary = batch_scorer.score_all_companies(force=force)
            logger.info(f"Batch acquisition target scoring complete: {summary}")
        finally:
            try:
                next(gen)
            except StopIteration:
                pass

    background_tasks.add_task(_batch_score)
    return {
        "mode": "batch",
        "status": "started",
        "message": "Batch acquisition target scoring launched in background. Check rankings for results.",
    }


# ---------------------------------------------------------------------------
# GET /acquisition-targets/{company_id} — latest score + history (LAST — catch-all)
# ---------------------------------------------------------------------------

@router.get(
    "/{company_id}",
    summary="Get acquisition target score for a company",
    response_description="Latest score plus optional history",
)
def get_acquisition_target_score(
    company_id: int,
    days: int = Query(90, ge=1, le=365, description="History window in days"),
    db: Session = Depends(get_db),
):
    """
    Return the latest Acquisition Target Score and historical scores
    for the given PE portfolio company.
    """
    scorer = AcquisitionTargetScorer(db)

    latest = scorer.score_company(company_id)

    history_query = text("""
        SELECT score_date, overall_score, grade, confidence,
               growth_signal_score, market_attractiveness_score,
               management_gap_score, deal_activity_score,
               sector_momentum_score
        FROM acquisition_target_scores
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
