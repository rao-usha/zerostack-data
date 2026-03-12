"""
DQ Review & Recommendation endpoints.

Provides a unified review workflow that ties together profiling, anomaly
detection, rule evaluation, and quality trending into actionable recommendations.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import (
    DQRecommendation,
    RecommendationCategory,
    RecommendationPriority,
    RecommendationStatus,
)
from app.core import dq_recommendation_engine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dq-review", tags=["dq-review"])


# =============================================================================
# Pydantic Schemas
# =============================================================================


class ReviewRunResponse(BaseModel):
    """Response from running a full DQ review."""

    recommendations_generated: int = Field(description="Number of new recommendations")
    by_priority: Dict[str, int] = Field(description="Counts by priority level")
    by_category: Dict[str, int] = Field(description="Counts by category")
    recommendations: List[Dict[str, Any]] = Field(description="Recommendation details")


class RecommendationResponse(BaseModel):
    """Single recommendation."""

    id: int
    category: Optional[str] = None
    priority: Optional[str] = None
    source: Optional[str] = None
    table_name: Optional[str] = None
    title: str
    description: str
    suggested_action: str
    evidence: Optional[Dict[str, Any]] = None
    auto_fixable: bool = False
    fix_action: Optional[str] = None
    fix_params: Optional[Dict[str, Any]] = None
    status: Optional[str] = None
    created_at: Optional[str] = None
    applied_at: Optional[str] = None
    dismissed_at: Optional[str] = None


class SummaryResponse(BaseModel):
    """Quick dashboard summary."""

    source_scores: Dict[str, Any] = Field(description="Quality scores by source")
    open_anomalies: int = Field(description="Number of open anomaly alerts")
    recommendation_counts: Dict[str, int] = Field(description="Open recommendations by priority")
    top_recommendations: List[Dict[str, Any]] = Field(description="Top 5 recommendations")
    generated_at: str


class ApplyResponse(BaseModel):
    """Response from applying a recommendation."""

    id: int
    status: str
    fix_action: Optional[str] = None
    fix_params: Optional[Dict[str, Any]] = None
    message: str


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/run", response_model=ReviewRunResponse)
def run_dq_review(db: Session = Depends(get_db)):
    """
    Run a full DQ review.

    Analyzes all DQ signals (quality snapshots, anomalies, rule violations,
    job history, freshness, completeness gaps) and generates prioritized
    recommendations.
    """
    try:
        recs = dq_recommendation_engine.generate_recommendations(db)

        by_priority: Dict[str, int] = {}
        by_category: Dict[str, int] = {}
        for r in recs:
            p = r.priority.value if r.priority else "unknown"
            c = r.category.value if r.category else "unknown"
            by_priority[p] = by_priority.get(p, 0) + 1
            by_category[c] = by_category.get(c, 0) + 1

        return ReviewRunResponse(
            recommendations_generated=len(recs),
            by_priority=by_priority,
            by_category=by_category,
            recommendations=[r.to_dict() for r in recs],
        )
    except Exception as e:
        logger.error(f"DQ review failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"DQ review failed: {str(e)}")


@router.get("/recommendations", response_model=List[RecommendationResponse])
def get_recommendations(
    category: Optional[str] = Query(None, description="Filter by category"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    source: Optional[str] = Query(None, description="Filter by source"),
    status: Optional[str] = Query("open", description="Filter by status"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Get recommendations with optional filters."""
    try:
        recs = dq_recommendation_engine.get_recommendations(
            db,
            category=category,
            priority=priority,
            source=source,
            status=status,
            limit=limit,
        )
        return [
            RecommendationResponse(**r.to_dict())
            for r in recs
        ]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/summary", response_model=SummaryResponse)
def get_review_summary(db: Session = Depends(get_db)):
    """
    Quick dashboard: quality scores by source, open anomalies,
    top recommendations.
    """
    summary = dq_recommendation_engine.get_review_summary(db)
    return SummaryResponse(**summary)


@router.post("/apply/{recommendation_id}", response_model=ApplyResponse)
def apply_recommendation(
    recommendation_id: int,
    db: Session = Depends(get_db),
):
    """
    Apply an auto-fixable recommendation.

    Marks the recommendation as applied. For recommendations with
    fix_action set, returns the action details so the caller can
    dispatch it (e.g., trigger re-ingestion, run rule seeder).
    """
    rec = dq_recommendation_engine.apply_recommendation(db, recommendation_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recommendation not found")

    return ApplyResponse(
        id=rec.id,
        status=rec.status.value if rec.status else "applied",
        fix_action=rec.fix_action,
        fix_params=rec.fix_params,
        message=(
            f"Recommendation applied. "
            f"{'Action: ' + rec.fix_action if rec.fix_action else 'No auto-fix action.'}"
        ),
    )


@router.post("/dismiss/{recommendation_id}", response_model=ApplyResponse)
def dismiss_recommendation(
    recommendation_id: int,
    db: Session = Depends(get_db),
):
    """Dismiss a recommendation."""
    rec = dq_recommendation_engine.dismiss_recommendation(db, recommendation_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recommendation not found")

    return ApplyResponse(
        id=rec.id,
        status=rec.status.value if rec.status else "dismissed",
        fix_action=None,
        fix_params=None,
        message="Recommendation dismissed.",
    )
