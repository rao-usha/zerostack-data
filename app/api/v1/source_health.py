"""
Source Health API.

Endpoints for monitoring source-level health scores, freshness,
reliability, and collection recommendations.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core import source_health as health_service
from app.core import collection_recommender as recommender_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/source-health", tags=["Source Health"])


@router.get("")
async def list_source_health(db: Session = Depends(get_db)):
    """
    Get health scores for all sources, sorted worst-first.

    Returns composite score (0-100), tier, and component breakdown
    for every known source.
    """
    results = health_service.get_all_source_health(db)
    return {
        "sources": results,
        "total": len(results),
    }


@router.get("/summary")
async def health_summary(db: Session = Depends(get_db)):
    """
    Aggregate platform health summary.

    Returns overall score, source counts per tier, and list of
    critical sources needing attention.
    """
    return health_service.get_health_summary(db)


@router.get("/{source}")
async def source_detail(source: str, db: Session = Depends(get_db)):
    """
    Detailed health breakdown for a single source.

    Includes component scores, recent jobs, and recommendations.
    """
    detail = health_service.get_source_health_detail(db, source)
    if detail["score"] == 0 and not detail.get("recent_jobs"):
        raise HTTPException(status_code=404, detail=f"No data found for source: {source}")
    return detail


@router.post("/{source}/refresh")
async def refresh_source_health(source: str, db: Session = Depends(get_db)):
    """
    Re-calculate health score for a source.

    Forces a fresh calculation and returns the updated result.
    """
    detail = health_service.get_source_health_detail(db, source)
    return {
        "refreshed": True,
        **detail,
    }


@router.get("/recommendations/all")
async def collection_recommendations(db: Session = Depends(get_db)):
    """
    Get prioritized collection recommendations for all sources.

    Analyzes health scores, freshness gaps, and failure patterns to
    suggest which sources need collection, investigation, or disabling.
    """
    recs = recommender_service.generate_recommendations(db)
    return {
        "recommendations": recs,
        "total": len(recs),
    }


@router.get("/recommendations/plan")
async def collection_plan(
    max_concurrent: int = 4,
    db: Session = Depends(get_db),
):
    """
    Get an optimal batched collection plan.

    Groups sources needing collection into waves that respect
    the max_concurrent limit.
    """
    return recommender_service.get_optimal_collection_plan(db, max_concurrent)


@router.get("/history/{source}")
async def collection_history(
    source: str,
    days: int = 30,
    db: Session = Depends(get_db),
):
    """
    Historical collection statistics for a source.

    Returns success rate, average rows, timing stats over the
    specified window (default 30 days).
    """
    return recommender_service.get_collection_history_stats(db, source, days)
