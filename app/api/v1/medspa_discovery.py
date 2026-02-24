"""
Med-Spa Discovery API endpoints.

Provides med-spa prospect discovery (via Yelp + ZIP scores), ranked prospect
listings, market map summaries, and methodology/budget docs.
"""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import get_db
from app.sources.medspa_discovery.collector import MedSpaDiscoveryCollector

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/medspa-discovery", tags=["Med-Spa Discovery"])


# ---------------------------------------------------------------------------
# GET /methodology — static docs (MUST be before /{yelp_id})
# ---------------------------------------------------------------------------

@router.get(
    "/methodology",
    summary="Med-spa prospect scoring methodology",
    response_description="Weights, grade thresholds, and data sources",
)
def get_methodology():
    """Return the acquisition prospect scoring methodology."""
    return MedSpaDiscoveryCollector.get_methodology()


# ---------------------------------------------------------------------------
# GET /api-budget — Yelp daily limit + estimates
# ---------------------------------------------------------------------------

@router.get(
    "/api-budget",
    summary="Yelp API budget and call estimates",
    response_description="Daily limit, calls per ZIP, and estimates",
)
def get_api_budget():
    """Return Yelp API budget info and per-ZIP call estimates."""
    return MedSpaDiscoveryCollector.get_api_budget()


# ---------------------------------------------------------------------------
# GET /prospects — ranked prospect list with filters
# ---------------------------------------------------------------------------

@router.get(
    "/prospects",
    summary="Ranked med-spa acquisition prospects",
    response_description="Filtered, paginated prospect list ordered by score",
)
def get_prospects(
    state: Optional[str] = Query(None, description="Filter by state (e.g. CA)"),
    grade: Optional[str] = Query(None, description="Filter by acquisition grade (A-F)"),
    min_score: float = Query(0.0, ge=0.0, le=100.0, description="Minimum acquisition score"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List med-spa prospects ranked by acquisition score with optional filters."""
    collector = MedSpaDiscoveryCollector(db)  # noqa: F841 — ensures table exists

    where_clauses = ["acquisition_score >= :min_score"]
    params = {"min_score": min_score, "lim": limit, "off": offset}

    if state:
        where_clauses.append("state = :state")
        params["state"] = state.upper()
    if grade:
        where_clauses.append("acquisition_grade = :grade")
        params["grade"] = grade.upper()

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        SELECT
            yelp_id, name, alias, rating, review_count, price, phone,
            url, image_url, latitude, longitude, address, city, state,
            zip_code, categories, is_closed,
            zip_overall_score, zip_grade, zip_affluence_density,
            zip_total_returns, zip_avg_agi,
            acquisition_score, acquisition_grade,
            zip_affluence_sub, yelp_rating_sub, review_volume_sub,
            low_competition_sub, price_tier_sub,
            competitor_count_in_zip, batch_id, model_version, discovered_at
        FROM medspa_prospects
        WHERE {where_sql}
        ORDER BY acquisition_score DESC
        LIMIT :lim OFFSET :off
    """)

    count_query = text(f"""
        SELECT COUNT(*) FROM medspa_prospects WHERE {where_sql}
    """)

    try:
        rows = db.execute(query, params).mappings().fetchall()
        total_row = db.execute(
            count_query, {k: v for k, v in params.items() if k not in ("lim", "off")}
        ).scalar()
    except Exception as e:
        logger.error(f"Error querying prospects: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    prospects = []
    for r in rows:
        rec = dict(r)
        # Convert categories from PG array to list if needed
        if rec.get("categories") and isinstance(rec["categories"], str):
            rec["categories"] = [rec["categories"]]
        prospects.append(rec)

    return {
        "total_matching": total_row or 0,
        "returned": len(prospects),
        "filters": {
            "state": state,
            "grade": grade,
            "min_score": min_score,
        },
        "prospects": prospects,
    }


# ---------------------------------------------------------------------------
# GET /market-map — by-state summary
# ---------------------------------------------------------------------------

@router.get(
    "/market-map",
    summary="Market map of med-spa prospects by state",
    response_description="State-level aggregation with counts and top prospects",
)
def get_market_map(
    limit_per_state: int = Query(3, ge=1, le=10, description="Top prospects per state"),
    db: Session = Depends(get_db),
):
    """Aggregate med-spa prospects by state: count, avg score, A-grade count, top N."""
    collector = MedSpaDiscoveryCollector(db)  # noqa: F841

    summary_query = text("""
        SELECT
            state,
            COUNT(*) as prospect_count,
            ROUND(AVG(acquisition_score)::numeric, 2) as avg_score,
            COUNT(*) FILTER (WHERE acquisition_grade = 'A') as a_count,
            COUNT(*) FILTER (WHERE acquisition_grade = 'B') as b_count
        FROM medspa_prospects
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY avg_score DESC
    """)

    try:
        state_rows = db.execute(summary_query).mappings().fetchall()
    except Exception as e:
        logger.error(f"Error building market map: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    if not state_rows:
        return {"total_states": 0, "states": []}

    states = []
    for sr in state_rows:
        st = sr["state"]

        # Top N prospects in this state
        top_query = text("""
            SELECT yelp_id, name, city, zip_code,
                   acquisition_score, acquisition_grade, rating, review_count
            FROM medspa_prospects
            WHERE state = :state
            ORDER BY acquisition_score DESC
            LIMIT :lim
        """)
        top_rows = db.execute(top_query, {"state": st, "lim": limit_per_state}).mappings().fetchall()

        states.append({
            "state": st,
            "prospect_count": sr["prospect_count"],
            "avg_score": float(sr["avg_score"]) if sr["avg_score"] else 0,
            "a_grade_count": sr["a_count"],
            "b_grade_count": sr["b_count"],
            "top_prospects": [dict(r) for r in top_rows],
        })

    return {
        "total_states": len(states),
        "total_prospects": sum(s["prospect_count"] for s in states),
        "states": states,
    }


# ---------------------------------------------------------------------------
# POST /discover — trigger Yelp search across top ZIPs (background)
# ---------------------------------------------------------------------------

@router.post(
    "/discover",
    summary="Trigger med-spa discovery across top ZIP codes",
    response_description="Batch job status with estimate",
)
def trigger_discovery(
    limit: int = Query(100, ge=1, le=500, description="Top N ZIPs to search"),
    states: Optional[str] = Query(None, description="Comma-separated state codes (e.g. CA,NY,TX)"),
    min_grade: str = Query("B", description="Minimum ZIP grade (A-F)"),
    search_terms: Optional[str] = Query(None, description="Comma-separated custom search terms"),
    max_api_calls: int = Query(400, ge=1, le=500, description="Safety cap on API calls"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Launch med-spa discovery in the background.

    Searches Yelp for med-spa businesses in the top-scoring ZIP codes,
    scores each as an acquisition prospect, and persists results.
    Requires YELP_API_KEY env var.
    """
    settings = get_settings()
    try:
        api_key = settings.require_yelp_api_key()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    state_list = [s.strip().upper() for s in states.split(",")] if states else None
    term_list = [t.strip() for t in search_terms.split(",")] if search_terms else None
    calls_per_zip = len(term_list) if term_list else 2
    estimated_calls = min(limit, max_api_calls // calls_per_zip) * calls_per_zip

    import threading

    def _run_in_thread():
        import traceback as _tb
        from app.core.database import get_db as _get_db

        gen = _get_db()
        session = next(gen)
        try:
            print("[medspa-discovery] Starting discovery...", flush=True)
            collector = MedSpaDiscoveryCollector(session)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    collector.discover(
                        api_key=api_key,
                        limit=limit,
                        states=state_list,
                        min_grade=min_grade,
                        search_terms=term_list,
                        max_api_calls=max_api_calls,
                    )
                )
            finally:
                loop.close()
            print(
                f"[medspa-discovery] Complete: "
                f"{result.get('unique_businesses', 0)} unique businesses, "
                f"{result.get('zips_searched', 0)} ZIPs searched",
                flush=True,
            )
        except Exception as exc:
            print(f"[medspa-discovery] FAILED: {exc}", flush=True)
            _tb.print_exc()
        finally:
            try:
                next(gen)
            except StopIteration:
                pass

    threading.Thread(target=_run_in_thread, daemon=True).start()

    return {
        "status": "started",
        "message": "Med-spa discovery launched in background. Check /prospects when complete.",
        "params": {
            "zip_limit": limit,
            "states": state_list,
            "min_grade": min_grade,
            "search_terms": term_list or ["med spa", "medical spa"],
            "max_api_calls": max_api_calls,
        },
        "estimated_api_calls": estimated_calls,
    }


# ---------------------------------------------------------------------------
# GET /prospects/{yelp_id} — single prospect detail (LAST — catch-all path)
# ---------------------------------------------------------------------------

@router.get(
    "/prospects/{yelp_id}",
    summary="Get a single med-spa prospect by Yelp ID",
    response_description="Full prospect detail with all scores",
)
def get_prospect_detail(
    yelp_id: str,
    db: Session = Depends(get_db),
):
    """Return full detail for a single med-spa prospect."""
    collector = MedSpaDiscoveryCollector(db)  # noqa: F841

    query = text("""
        SELECT
            yelp_id, name, alias, rating, review_count, price, phone,
            url, image_url, latitude, longitude, address, city, state,
            zip_code, categories, is_closed,
            zip_overall_score, zip_grade, zip_affluence_density,
            zip_total_returns, zip_avg_agi,
            acquisition_score, acquisition_grade,
            zip_affluence_sub, yelp_rating_sub, review_volume_sub,
            low_competition_sub, price_tier_sub,
            competitor_count_in_zip, search_term, batch_id,
            model_version, discovered_at, updated_at
        FROM medspa_prospects
        WHERE yelp_id = :yelp_id
    """)

    try:
        row = db.execute(query, {"yelp_id": yelp_id}).mappings().fetchone()
    except Exception as e:
        logger.error(f"Error fetching prospect {yelp_id}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(status_code=404, detail=f"Prospect not found: {yelp_id}")

    return dict(row)
