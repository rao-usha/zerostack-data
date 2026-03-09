"""
Multi-Vertical Discovery Engine — REST API.

Single router covering all verticals via {slug} path parameter.
"""

import asyncio
import threading
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.sources.vertical_discovery.configs import (
    VERTICAL_REGISTRY,
    GRADE_THRESHOLDS,
)

router = APIRouter(prefix="/vertical-discovery", tags=["Vertical Discovery"])


def _get_config(slug: str):
    """Resolve vertical config or raise 404-like dict."""
    config = VERTICAL_REGISTRY.get(slug)
    if not config:
        return None
    return config


# ------------------------------------------------------------------
# List verticals
# ------------------------------------------------------------------

@router.get(
    "/verticals",
    summary="List all supported discovery verticals",
    response_description="Available verticals with configuration details",
)
def list_verticals():
    return {
        "total_verticals": len(VERTICAL_REGISTRY),
        "verticals": [
            {
                "slug": c.slug,
                "display_name": c.display_name,
                "table_name": c.table_name,
                "search_terms": c.search_terms,
                "has_nppes_enrichment": c.has_nppes_enrichment,
                "prospect_weights": c.prospect_weights,
            }
            for c in VERTICAL_REGISTRY.values()
        ],
    }


# ------------------------------------------------------------------
# Methodology
# ------------------------------------------------------------------

@router.get(
    "/{slug}/methodology",
    summary="Scoring methodology for a vertical",
    response_description="Weights, grade thresholds, revenue benchmarks",
)
def get_methodology(slug: str):
    config = _get_config(slug)
    if not config:
        return {"error": f"Unknown vertical: {slug}", "available": list(VERTICAL_REGISTRY.keys())}

    return {
        "vertical": config.slug,
        "display_name": config.display_name,
        "description": (
            f"Discover and score {config.display_name.lower()} as PE acquisition "
            f"prospects using Yelp business data + IRS SOI ZIP-level income data."
        ),
        "search_terms": config.search_terms,
        "yelp_categories": config.yelp_categories,
        "scoring_weights": config.prospect_weights,
        "revenue_benchmarks": {
            k or "unknown": v for k, v in config.revenue_benchmarks.items()
        },
        "has_nppes_enrichment": config.has_nppes_enrichment,
        "nppes_taxonomy_codes": config.nppes_taxonomy_codes,
        "grade_thresholds": {
            grade: f">={threshold}" for threshold, grade in GRADE_THRESHOLDS
        },
    }


# ------------------------------------------------------------------
# Prospects (query)
# ------------------------------------------------------------------

@router.get(
    "/{slug}/prospects",
    summary="Ranked prospects for a vertical",
    response_description="Filtered, paginated prospect list",
)
def get_prospects(
    slug: str,
    state: Optional[str] = Query(None),
    grade: Optional[str] = Query(None),
    ownership_type: Optional[str] = Query(None),
    min_score: float = Query(0.0, ge=0.0, le=100.0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    config = _get_config(slug)
    if not config:
        return {"error": f"Unknown vertical: {slug}"}

    t = config.table_name
    where_clauses = ["acquisition_score >= :min_score"]
    params = {"min_score": min_score, "lim": limit, "off": offset}

    if state:
        where_clauses.append("state = :state")
        params["state"] = state.upper()
    if grade:
        where_clauses.append("acquisition_grade = :grade")
        params["grade"] = grade.upper()
    if ownership_type:
        where_clauses.append("ownership_type = :ownership_type")
        params["ownership_type"] = ownership_type

    where_sql = " AND ".join(where_clauses)

    try:
        query = text(f"""
            SELECT * FROM {t}
            WHERE {where_sql}
            ORDER BY acquisition_score DESC
            LIMIT :lim OFFSET :off
        """)
        count_query = text(f"""
            SELECT COUNT(*) FROM {t} WHERE {where_sql}
        """)

        rows = db.execute(query, params).mappings().fetchall()
        count_params = {k: v for k, v in params.items() if k not in ("lim", "off")}
        total = db.execute(count_query, count_params).scalar() or 0
    except Exception as e:
        return {
            "error": f"Table {t} may not exist yet. Run discovery first.",
            "detail": str(e),
        }

    return {
        "vertical": slug,
        "total_matching": total,
        "returned": len(rows),
        "filters": {"state": state, "grade": grade, "ownership_type": ownership_type},
        "prospects": [dict(r) for r in rows],
    }


# ------------------------------------------------------------------
# Discover (trigger)
# ------------------------------------------------------------------

@router.post(
    "/{slug}/discover",
    summary="Trigger prospect discovery for a vertical",
    response_description="Launches background Yelp search + scoring",
)
def trigger_discovery(
    slug: str,
    limit: int = Query(100, description="Max ZIPs to search"),
    states: Optional[str] = Query(None, description="Comma-separated state abbreviations"),
    db: Session = Depends(get_db),
):
    config = _get_config(slug)
    if not config:
        return {"error": f"Unknown vertical: {slug}"}

    parsed_states = (
        [s.strip().upper() for s in states.split(",") if s.strip()]
        if states else None
    )

    def _run_in_thread():
        from app.core.database import get_db as _get_db
        from app.core.config import get_settings

        gen = _get_db()
        session = next(gen)
        try:
            settings = get_settings()
            api_key = settings.yelp_api_key
            if not api_key:
                logger_thread = __import__("logging").getLogger(__name__)
                logger_thread.error("YELP_API_KEY not configured")
                return

            from app.sources.vertical_discovery.collector import VerticalDiscoveryCollector
            collector = VerticalDiscoveryCollector(session, config)

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(
                    collector.discover(
                        api_key=api_key,
                        limit=limit,
                        states=parsed_states,
                    )
                )
            finally:
                loop.close()
        finally:
            try:
                next(gen)
            except StopIteration:
                pass

    threading.Thread(target=_run_in_thread, daemon=True).start()

    return {
        "status": "started",
        "vertical": slug,
        "message": f"Discovery started for {config.display_name}",
        "limit": limit,
        "states": parsed_states,
    }


# ------------------------------------------------------------------
# Enrich
# ------------------------------------------------------------------

@router.post(
    "/{slug}/enrich/all",
    summary="Run full enrichment pipeline for a vertical",
    response_description="NPPES + density + revenue enrichment results",
)
def enrich_all(
    slug: str,
    force: bool = Query(False),
    db: Session = Depends(get_db),
):
    config = _get_config(slug)
    if not config:
        return {"error": f"Unknown vertical: {slug}"}

    from app.sources.vertical_discovery.enrichment import VerticalEnrichmentPipeline
    pipeline = VerticalEnrichmentPipeline(db, config)
    return pipeline.enrich_all(force=force)


# ------------------------------------------------------------------
# Classify ownership
# ------------------------------------------------------------------

@router.post(
    "/{slug}/classify",
    summary="Classify prospect ownership (Independent/Multi-Site/PE/Public)",
    response_description="Classification pipeline results",
)
def classify_ownership(
    slug: str,
    force: bool = Query(False),
    db: Session = Depends(get_db),
):
    config = _get_config(slug)
    if not config:
        return {"error": f"Unknown vertical: {slug}"}

    from app.sources.vertical_discovery.ownership_classifier import VerticalOwnershipClassifier
    classifier = VerticalOwnershipClassifier(db, config)
    return classifier.classify_all(force=force)
