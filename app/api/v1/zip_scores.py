"""
ZIP Med-Spa Revenue Potential Score API endpoints.

Provides scoring, rankings, top-market summaries, and methodology docs
for US ZIP codes ranked by med-spa revenue potential.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.ml.zip_medspa_scorer import ZipMedSpaScorer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/zip-scores", tags=["ZIP Intelligence"])


# ---------------------------------------------------------------------------
# GET /zip-scores/methodology  (MUST be before /{zip_code})
# ---------------------------------------------------------------------------

@router.get(
    "/methodology",
    summary="ZIP med-spa scoring methodology",
    response_description="Weights, grade thresholds, and data source descriptions",
)
def get_methodology():
    """Return the ZIP Med-Spa Score methodology, weights, and thresholds."""
    return ZipMedSpaScorer.get_methodology()


# ---------------------------------------------------------------------------
# GET /zip-scores/rankings
# ---------------------------------------------------------------------------

@router.get(
    "/rankings",
    summary="Rank ZIPs by med-spa revenue potential",
    response_description="Ordered list of ZIPs with scores",
)
def get_zip_rankings(
    state: Optional[str] = Query(None, description="Filter by state abbreviation (e.g. CA)"),
    grade: Optional[str] = Query(None, description="Filter by grade (A-F)"),
    min_score: float = Query(0.0, ge=0.0, le=100.0, description="Minimum overall score"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Return ZIP codes ranked by their latest med-spa revenue potential score,
    with optional filters for state, grade, and minimum score.
    """
    scorer = ZipMedSpaScorer(db)  # ensure tables exist

    where_clauses = [
        "zms.model_version = :ver",
        "zms.overall_score >= :min_score",
    ]
    params: Dict[str, Any] = {
        "ver": "v1.0",
        "min_score": min_score,
        "lim": limit,
        "off": offset,
    }

    if state:
        where_clauses.append("zms.state_abbr = :state")
        params["state"] = state.upper()
    if grade:
        where_clauses.append("zms.grade = :grade")
        params["grade"] = grade.upper()

    where_sql = " AND ".join(where_clauses)

    ranked_query = text(f"""
        SELECT * FROM (
            SELECT DISTINCT ON (zms.zip_code)
                   zms.zip_code,
                   zms.state_abbr,
                   zms.score_date,
                   zms.overall_score,
                   zms.grade,
                   zms.confidence,
                   zms.affluence_density_score,
                   zms.discretionary_wealth_score,
                   zms.market_size_score,
                   zms.professional_density_score,
                   zms.wealth_concentration_score,
                   zms.pct_returns_100k_plus,
                   zms.pct_returns_200k_plus,
                   zms.avg_agi,
                   zms.total_returns,
                   zms.cap_gains_per_return,
                   zms.dividends_per_return,
                   zms.total_market_income,
                   zms.partnership_density,
                   zms.self_employment_density,
                   zms.joint_pct_top_bracket,
                   zms.amt_per_return
            FROM zip_medspa_scores zms
            WHERE {where_sql}
            ORDER BY zms.zip_code, zms.score_date DESC
        ) ranked
        ORDER BY ranked.overall_score DESC
        LIMIT :lim OFFSET :off
    """)

    try:
        rows = db.execute(ranked_query, params).mappings().fetchall()
    except Exception as e:
        logger.error(f"ZIP rankings query error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch rankings")

    rankings = []
    for i, r in enumerate(rows, start=offset + 1):
        rankings.append({
            "rank": i,
            "zip_code": r["zip_code"],
            "state_abbr": r["state_abbr"],
            "score_date": str(r["score_date"]),
            "overall_score": float(r["overall_score"]),
            "grade": r["grade"],
            "confidence": float(r["confidence"]) if r["confidence"] else 0,
            "sub_scores": {
                "affluence_density": float(r["affluence_density_score"]) if r["affluence_density_score"] else None,
                "discretionary_wealth": float(r["discretionary_wealth_score"]) if r["discretionary_wealth_score"] else None,
                "market_size": float(r["market_size_score"]) if r["market_size_score"] else None,
                "professional_density": float(r["professional_density_score"]) if r["professional_density_score"] else None,
                "wealth_concentration": float(r["wealth_concentration_score"]) if r["wealth_concentration_score"] else None,
            },
            "raw_metrics": {
                "pct_returns_100k_plus": float(r["pct_returns_100k_plus"]) if r["pct_returns_100k_plus"] else None,
                "pct_returns_200k_plus": float(r["pct_returns_200k_plus"]) if r["pct_returns_200k_plus"] else None,
                "avg_agi": float(r["avg_agi"]) if r["avg_agi"] else None,
                "total_returns": r["total_returns"],
                "cap_gains_per_return": float(r["cap_gains_per_return"]) if r["cap_gains_per_return"] else None,
                "dividends_per_return": float(r["dividends_per_return"]) if r["dividends_per_return"] else None,
            },
        })

    return {
        "total_returned": len(rankings),
        "filters": {"state": state, "grade": grade, "min_score": min_score},
        "rankings": rankings,
    }


# ---------------------------------------------------------------------------
# GET /zip-scores/top-markets
# ---------------------------------------------------------------------------

@router.get(
    "/top-markets",
    summary="Top med-spa markets grouped by state",
    response_description="States ranked by average score with top ZIPs per state",
)
def get_top_markets(
    top_states: int = Query(15, ge=1, le=52, description="Number of top states"),
    zips_per_state: int = Query(5, ge=1, le=20, description="Top ZIPs per state"),
    db: Session = Depends(get_db),
):
    """
    Return top states ranked by average med-spa score, with top ZIPs per state.
    Answers: "Where should I look for med-spa acquisitions?"
    """
    scorer = ZipMedSpaScorer(db)  # ensure tables exist

    # Get state averages + A-grade counts
    state_query = text("""
        SELECT state_abbr,
               ROUND(AVG(overall_score)::numeric, 2) AS avg_score,
               COUNT(*) FILTER (WHERE grade = 'A') AS a_grade_count,
               COUNT(*) AS total_zips
        FROM (
            SELECT DISTINCT ON (zip_code) zip_code, state_abbr, overall_score, grade
            FROM zip_medspa_scores
            WHERE model_version = :ver
            ORDER BY zip_code, score_date DESC
        ) latest
        WHERE state_abbr IS NOT NULL
        GROUP BY state_abbr
        ORDER BY avg_score DESC
        LIMIT :top_states
    """)

    try:
        state_rows = db.execute(
            state_query, {"ver": "v1.0", "top_states": top_states}
        ).mappings().fetchall()
    except Exception as e:
        logger.error(f"Top markets state query error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch top markets")

    top_state_list = []
    for sr in state_rows:
        st = sr["state_abbr"]

        # Get top ZIPs for this state
        zip_query = text("""
            SELECT * FROM (
                SELECT DISTINCT ON (zip_code)
                       zip_code, overall_score, grade, affluence_density_score,
                       discretionary_wealth_score, market_size_score,
                       total_returns, avg_agi
                FROM zip_medspa_scores
                WHERE model_version = :ver AND state_abbr = :state
                ORDER BY zip_code, score_date DESC
            ) latest
            ORDER BY overall_score DESC
            LIMIT :lim
        """)
        try:
            zip_rows = db.execute(
                zip_query, {"ver": "v1.0", "state": st, "lim": zips_per_state}
            ).mappings().fetchall()
        except Exception:
            zip_rows = []

        top_state_list.append({
            "state": st,
            "avg_score": float(sr["avg_score"]),
            "a_grade_count": sr["a_grade_count"],
            "total_zips": sr["total_zips"],
            "top_zips": [
                {
                    "zip_code": zr["zip_code"],
                    "overall_score": float(zr["overall_score"]),
                    "grade": zr["grade"],
                    "affluence_density": float(zr["affluence_density_score"]) if zr["affluence_density_score"] else None,
                    "discretionary_wealth": float(zr["discretionary_wealth_score"]) if zr["discretionary_wealth_score"] else None,
                    "market_size": float(zr["market_size_score"]) if zr["market_size_score"] else None,
                    "total_returns": zr["total_returns"],
                    "avg_agi": float(zr["avg_agi"]) if zr["avg_agi"] else None,
                }
                for zr in zip_rows
            ],
        })

    return {"top_states": top_state_list}


# ---------------------------------------------------------------------------
# POST /zip-scores/compute
# ---------------------------------------------------------------------------

@router.post(
    "/compute",
    summary="Trigger ZIP med-spa score computation",
    response_description="Scoring result or batch job status",
)
def compute_zip_scores(
    state: Optional[str] = Query(None, description="Score only ZIPs in this state"),
    force: bool = Query(False, description="Force recompute even if cached"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Trigger ZIP Med-Spa Score computation.

    - Provide `state` to score only that state's ZIPs synchronously.
    - Omit `state` to batch-score all ~27,604 ZIPs in the background.
    """
    if state:
        scorer = ZipMedSpaScorer(db)
        result = scorer.score_all_zips(force=force, state=state.upper())
        return {"mode": "single_state", "state": state.upper(), "result": result}

    def _batch_score():
        from app.core.database import get_db as _get_db
        gen = _get_db()
        session = next(gen)
        try:
            batch_scorer = ZipMedSpaScorer(session)
            summary = batch_scorer.score_all_zips(force=force)
            logger.info(f"Batch ZIP med-spa scoring complete: {summary}")
        finally:
            try:
                next(gen)
            except StopIteration:
                pass

    background_tasks.add_task(_batch_score)
    return {
        "mode": "batch",
        "status": "started",
        "message": (
            "Batch ZIP med-spa scoring launched in background for all ZIPs. "
            "Check /rankings for results."
        ),
    }


# ---------------------------------------------------------------------------
# GET /zip-scores/{zip_code}  (LAST — catch-all path param)
# ---------------------------------------------------------------------------

@router.get(
    "/{zip_code}",
    summary="Get med-spa score for a single ZIP code",
    response_description="Full score detail with all raw metrics and sub-scores",
)
def get_zip_score(
    zip_code: str,
    db: Session = Depends(get_db),
):
    """
    Return the latest med-spa revenue potential score for a single ZIP code,
    including all sub-scores and raw metrics.
    """
    scorer = ZipMedSpaScorer(db)

    result = scorer.score_zip(zip_code)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return {
        "zip_code": zip_code,
        "score": result,
    }
