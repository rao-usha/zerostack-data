"""
PE Benchmarking & Exit Readiness API endpoints.

Endpoints for:
- Financial benchmarking (single company + portfolio heatmap)
- Exit readiness scoring
- Demo data seeding
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pe", tags=["PE Intelligence - Benchmarks"])


# =============================================================================
# Response Models
# =============================================================================


class MetricBenchmarkResponse(BaseModel):
    metric: str
    label: str
    value: Optional[float] = None
    industry_median: Optional[float] = None
    portfolio_avg: Optional[float] = None
    top_quartile: Optional[float] = None
    bottom_quartile: Optional[float] = None
    percentile: Optional[int] = None
    trend: Optional[str] = None
    peer_count: int = 0


class CompanyBenchmarkResponse(BaseModel):
    company_id: int
    company_name: str
    industry: Optional[str] = None
    fiscal_year: int
    metrics: List[MetricBenchmarkResponse] = []
    overall_percentile: Optional[int] = None
    data_quality: str = "high"


class HeatmapCellResponse(BaseModel):
    company_id: int
    company_name: str
    industry: Optional[str] = None
    status: str
    metrics: Dict[str, Optional[int]] = {}


class SubScoreResponse(BaseModel):
    dimension: str
    label: str
    weight: float
    raw_score: float
    weighted_score: float
    grade: str
    explanation: str
    recommendations: List[str] = []


class ExitReadinessResponse(BaseModel):
    company_id: int
    company_name: str
    composite_score: float
    grade: str
    sub_scores: List[SubScoreResponse] = []
    recommendations: List[str] = []
    confidence: str = "high"
    data_gaps: List[str] = []


class SeedDemoResponse(BaseModel):
    status: str
    tables: Dict[str, int] = {}
    total_rows: int = 0


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/seed-demo", response_model=SeedDemoResponse)
async def seed_demo_data(db: Session = Depends(get_db)):
    """
    Seed PE demo data (3 firms, 6 funds, 24 companies, financials, people, deals).
    Idempotent — safe to run multiple times.
    """
    from app.sources.pe.demo_seeder import seed_pe_demo_data

    try:
        counts = await seed_pe_demo_data(db)
        return SeedDemoResponse(
            status="success",
            tables=counts,
            total_rows=sum(counts.values()),
        )
    except Exception as e:
        logger.exception("Demo seeder failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Seeder failed: {str(e)}")


@router.get("/benchmarks/{company_id}", response_model=CompanyBenchmarkResponse)
async def get_company_benchmarks(
    company_id: int,
    fiscal_year: Optional[int] = Query(None, description="Fiscal year (defaults to most recent)"),
    db: Session = Depends(get_db),
):
    """
    Benchmark a portfolio company against industry peers.

    Returns percentile ranks for revenue growth, EBITDA margin,
    revenue per employee, debt/EBITDA, and more.
    """
    from app.core.pe_benchmarking import benchmark_company

    result = benchmark_company(db, company_id, fiscal_year)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    return CompanyBenchmarkResponse(
        company_id=result.company_id,
        company_name=result.company_name,
        industry=result.industry,
        fiscal_year=result.fiscal_year,
        metrics=[
            MetricBenchmarkResponse(
                metric=m.metric,
                label=m.label,
                value=m.value,
                industry_median=m.industry_median,
                portfolio_avg=m.portfolio_avg,
                top_quartile=m.top_quartile,
                bottom_quartile=m.bottom_quartile,
                percentile=m.percentile,
                trend=m.trend,
                peer_count=m.peer_count,
            )
            for m in result.metrics
        ],
        overall_percentile=result.overall_percentile,
        data_quality=result.data_quality,
    )


@router.get("/benchmarks/portfolio/{firm_id}", response_model=List[HeatmapCellResponse])
async def get_portfolio_heatmap(
    firm_id: int,
    fiscal_year: Optional[int] = Query(None, description="Fiscal year (defaults to most recent)"),
    db: Session = Depends(get_db),
):
    """
    Portfolio heatmap — percentile rank per metric for each company in a firm's portfolio.
    """
    from app.core.pe_benchmarking import benchmark_portfolio

    rows = benchmark_portfolio(db, firm_id, fiscal_year)
    return [
        HeatmapCellResponse(
            company_id=r.company_id,
            company_name=r.company_name,
            industry=r.industry,
            status=r.status,
            metrics=r.metrics,
        )
        for r in rows
    ]


@router.get("/exit-readiness/{company_id}", response_model=ExitReadinessResponse)
async def get_exit_readiness(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Exit readiness score for a portfolio company.

    Returns composite 0-100 score with 6 weighted sub-scores:
    Financial Health (30%), Market Position (20%), Management Quality (15%),
    Data Room Readiness (15%), Market Timing (10%), Regulatory Risk (10%).
    """
    from app.core.pe_exit_scoring import score_exit_readiness

    result = score_exit_readiness(db, company_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    return ExitReadinessResponse(
        company_id=result.company_id,
        company_name=result.company_name,
        composite_score=result.composite_score,
        grade=result.grade,
        sub_scores=[
            SubScoreResponse(
                dimension=s.dimension,
                label=s.label,
                weight=s.weight,
                raw_score=s.raw_score,
                weighted_score=s.weighted_score,
                grade=s.grade,
                explanation=s.explanation,
                recommendations=s.recommendations,
            )
            for s in result.sub_scores
        ],
        recommendations=result.recommendations,
        confidence=result.confidence,
        data_gaps=result.data_gaps,
    )
