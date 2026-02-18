"""
LLM Cost Tracking API endpoints.

Provides endpoints for querying LLM usage and costs:
- Summary totals (by model, by source)
- Per-job cost breakdown
- Daily cost trend
- Current session totals
"""

from typing import Optional, List
from datetime import date, datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Date
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import LLMUsage


router = APIRouter(prefix="/llm-costs", tags=["LLM Costs"])


# =============================================================================
# Response Models
# =============================================================================


class ModelCostBreakdown(BaseModel):
    model: str
    provider: Optional[str] = None
    total_calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0


class SourceCostBreakdown(BaseModel):
    source: str
    total_calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0


class CostSummaryResponse(BaseModel):
    total_calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0
    by_model: List[ModelCostBreakdown] = []
    by_source: List[SourceCostBreakdown] = []
    session_totals: Optional[dict] = None


class JobCostResponse(BaseModel):
    job_id: int
    total_calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0
    by_source: List[SourceCostBreakdown] = []


class DailyCostEntry(BaseModel):
    date: str
    total_calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0


class DailyCostResponse(BaseModel):
    days: List[DailyCostEntry] = []
    total_cost_usd: float = 0.0


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/summary", response_model=CostSummaryResponse)
def get_cost_summary(
    since: Optional[date] = Query(None, description="Filter costs since this date"),
    db: Session = Depends(get_db),
):
    """
    Get total LLM cost summary with breakdowns by model and source.

    Includes current in-memory session totals.
    """
    query = db.query(LLMUsage)
    if since:
        query = query.filter(
            LLMUsage.created_at >= datetime.combine(since, datetime.min.time())
        )

    # Overall totals
    totals = db.query(
        func.count(LLMUsage.id).label("total_calls"),
        func.coalesce(func.sum(LLMUsage.input_tokens), 0).label("total_input_tokens"),
        func.coalesce(func.sum(LLMUsage.output_tokens), 0).label("total_output_tokens"),
        func.coalesce(func.sum(LLMUsage.cost_usd), 0).label("total_cost_usd"),
    )
    if since:
        totals = totals.filter(
            LLMUsage.created_at >= datetime.combine(since, datetime.min.time())
        )
    totals = totals.first()

    # By model
    model_query = db.query(
        LLMUsage.model,
        LLMUsage.provider,
        func.count(LLMUsage.id).label("total_calls"),
        func.coalesce(func.sum(LLMUsage.input_tokens), 0).label("total_input_tokens"),
        func.coalesce(func.sum(LLMUsage.output_tokens), 0).label("total_output_tokens"),
        func.coalesce(func.sum(LLMUsage.cost_usd), 0).label("total_cost_usd"),
    ).group_by(LLMUsage.model, LLMUsage.provider)
    if since:
        model_query = model_query.filter(
            LLMUsage.created_at >= datetime.combine(since, datetime.min.time())
        )
    model_rows = model_query.order_by(func.sum(LLMUsage.cost_usd).desc()).all()

    # By source
    source_query = db.query(
        LLMUsage.source,
        func.count(LLMUsage.id).label("total_calls"),
        func.coalesce(func.sum(LLMUsage.input_tokens), 0).label("total_input_tokens"),
        func.coalesce(func.sum(LLMUsage.output_tokens), 0).label("total_output_tokens"),
        func.coalesce(func.sum(LLMUsage.cost_usd), 0).label("total_cost_usd"),
    ).group_by(LLMUsage.source)
    if since:
        source_query = source_query.filter(
            LLMUsage.created_at >= datetime.combine(since, datetime.min.time())
        )
    source_rows = source_query.order_by(func.sum(LLMUsage.cost_usd).desc()).all()

    # Session totals
    try:
        from app.core.llm_cost_tracker import get_cost_tracker

        session_totals = get_cost_tracker().get_session_totals()
    except Exception:
        session_totals = None

    return CostSummaryResponse(
        total_calls=totals.total_calls or 0,
        total_input_tokens=int(totals.total_input_tokens or 0),
        total_output_tokens=int(totals.total_output_tokens or 0),
        total_cost_usd=float(totals.total_cost_usd or 0),
        by_model=[
            ModelCostBreakdown(
                model=row.model,
                provider=row.provider,
                total_calls=row.total_calls,
                total_input_tokens=int(row.total_input_tokens),
                total_output_tokens=int(row.total_output_tokens),
                total_cost_usd=float(row.total_cost_usd),
            )
            for row in model_rows
        ],
        by_source=[
            SourceCostBreakdown(
                source=row.source or "unknown",
                total_calls=row.total_calls,
                total_input_tokens=int(row.total_input_tokens),
                total_output_tokens=int(row.total_output_tokens),
                total_cost_usd=float(row.total_cost_usd),
            )
            for row in source_rows
        ],
        session_totals=session_totals,
    )


@router.get("/by-job/{job_id}", response_model=JobCostResponse)
def get_job_costs(
    job_id: int,
    db: Session = Depends(get_db),
):
    """Get LLM costs for a specific collection job."""
    rows = db.query(LLMUsage).filter(LLMUsage.job_id == job_id).all()

    total_input = sum(r.input_tokens for r in rows)
    total_output = sum(r.output_tokens for r in rows)
    total_cost = sum(float(r.cost_usd) for r in rows)

    # By source
    source_map = {}
    for r in rows:
        src = r.source or "unknown"
        if src not in source_map:
            source_map[src] = {"calls": 0, "input": 0, "output": 0, "cost": 0.0}
        source_map[src]["calls"] += 1
        source_map[src]["input"] += r.input_tokens
        source_map[src]["output"] += r.output_tokens
        source_map[src]["cost"] += float(r.cost_usd)

    return JobCostResponse(
        job_id=job_id,
        total_calls=len(rows),
        total_input_tokens=total_input,
        total_output_tokens=total_output,
        total_cost_usd=total_cost,
        by_source=[
            SourceCostBreakdown(
                source=src,
                total_calls=data["calls"],
                total_input_tokens=data["input"],
                total_output_tokens=data["output"],
                total_cost_usd=data["cost"],
            )
            for src, data in source_map.items()
        ],
    )


@router.get("/daily", response_model=DailyCostResponse)
def get_daily_costs(
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
    db: Session = Depends(get_db),
):
    """Get daily LLM cost breakdown."""
    from datetime import timedelta

    cutoff = datetime.utcnow() - timedelta(days=days)

    daily_rows = (
        db.query(
            cast(LLMUsage.created_at, Date).label("day"),
            func.count(LLMUsage.id).label("total_calls"),
            func.coalesce(func.sum(LLMUsage.input_tokens), 0).label(
                "total_input_tokens"
            ),
            func.coalesce(func.sum(LLMUsage.output_tokens), 0).label(
                "total_output_tokens"
            ),
            func.coalesce(func.sum(LLMUsage.cost_usd), 0).label("total_cost_usd"),
        )
        .filter(LLMUsage.created_at >= cutoff)
        .group_by(cast(LLMUsage.created_at, Date))
        .order_by(cast(LLMUsage.created_at, Date).desc())
        .all()
    )

    entries = [
        DailyCostEntry(
            date=str(row.day),
            total_calls=row.total_calls,
            total_input_tokens=int(row.total_input_tokens),
            total_output_tokens=int(row.total_output_tokens),
            total_cost_usd=float(row.total_cost_usd),
        )
        for row in daily_rows
    ]

    total = sum(e.total_cost_usd for e in entries)

    return DailyCostResponse(days=entries, total_cost_usd=total)
