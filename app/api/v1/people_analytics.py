"""
People Analytics API endpoints.

Provides endpoints for leadership analytics, trends, and aggregations:
- Industry stats and metrics for leadership data
- Talent flow analysis
- Change trends over time
- Hot roles and hiring patterns
- Company benchmark scores
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.core.database import get_db
from app.services.analytics_service import AnalyticsService


router = APIRouter(prefix="/people-analytics", tags=["People Analytics"])


# =============================================================================
# Response Models
# =============================================================================


class InstabilityFlag(BaseModel):
    """Company with high leadership turnover."""

    company_id: int
    company_name: str
    c_suite_changes: int
    flag: str


class IndustryStatsResponse(BaseModel):
    """Industry-wide statistics for leadership data."""

    industry: str
    period_days: int
    total_companies: int
    total_executives: int
    c_suite_count: int
    board_members: int
    changes_in_period: int
    changes_by_type: dict
    c_suite_changes: int
    board_changes: int
    avg_ceo_tenure_months: Optional[float] = None
    avg_cfo_tenure_months: Optional[float] = None
    avg_c_suite_tenure_months: Optional[float] = None
    instability_flags: List[InstabilityFlag] = []


class TalentFlowEntry(BaseModel):
    """Single company's talent flow."""

    company_id: int
    company_name: str
    hires: int
    departures: int
    net: int


class TalentFlowResponse(BaseModel):
    """Talent flow analysis response."""

    industry: str
    period_days: int
    net_importers: List[TalentFlowEntry]
    net_exporters: List[TalentFlowEntry]
    stable: List[TalentFlowEntry]


class TrendDataPoint(BaseModel):
    """Single month's trend data."""

    month: str
    total: int
    hires: int = 0
    departures: int = 0
    promotions: int = 0
    retirements: int = 0


class TrendsResponse(BaseModel):
    """Change trends over time."""

    industry: str
    months: int
    trends: List[TrendDataPoint]


class HotRoleEntry(BaseModel):
    """Hot role with hiring count."""

    role: str
    hires: int


class BenchmarkComponents(BaseModel):
    """Benchmark score components."""

    completeness: float
    depth: float
    tenure: float
    board: float


class BenchmarkDetails(BaseModel):
    """Benchmark detail metrics."""

    has_ceo: bool
    has_cfo: bool
    has_coo: bool
    c_suite_count: int
    vp_count: int
    board_count: int
    avg_c_suite_tenure_months: Optional[float] = None


class BenchmarkScoreResponse(BaseModel):
    """Company benchmark score."""

    company_id: int
    company_name: str
    team_score: float
    components: BenchmarkComponents
    details: BenchmarkDetails


class PortfolioAnalyticsResponse(BaseModel):
    """Portfolio analytics response."""

    portfolio_id: int
    portfolio_name: str
    total_companies: int
    total_executives: int
    c_suite_count: int = 0
    changes_in_period: int = 0
    changes_by_type: dict = {}
    period_days: int


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/industries", response_model=List[str])
async def list_industries(
    db: Session = Depends(get_db),
):
    """
    List all industries with leadership data.

    Returns distinct industry segments from the companies table.
    """
    from app.core.people_models import IndustrialCompany

    industries = (
        db.query(IndustrialCompany.industry_segment)
        .distinct()
        .filter(IndustrialCompany.industry_segment.isnot(None))
        .all()
    )

    return [i[0] for i in industries if i[0]]


@router.get("/industries/{industry}/stats", response_model=IndustryStatsResponse)
async def get_industry_stats(
    industry: str,
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get comprehensive leadership statistics for an industry.

    Includes executive counts, change stats, tenure averages, and instability flags.
    """
    service = AnalyticsService(db)
    stats = service.get_industry_stats(industry=industry, days=days)

    # Convert instability_flags to proper model
    flags = [InstabilityFlag(**f) for f in stats.get("instability_flags", [])]
    stats["instability_flags"] = flags

    return IndustryStatsResponse(**stats)


@router.get("/stats", response_model=IndustryStatsResponse)
async def get_overall_stats(
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get comprehensive leadership statistics across all industries.

    Useful for dashboard overview.
    """
    service = AnalyticsService(db)
    stats = service.get_industry_stats(industry=None, days=days)

    # Convert instability_flags to proper model
    flags = [InstabilityFlag(**f) for f in stats.get("instability_flags", [])]
    stats["instability_flags"] = flags

    return IndustryStatsResponse(**stats)


@router.get("/industries/{industry}/talent-flow", response_model=TalentFlowResponse)
async def get_talent_flow(
    industry: str,
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Analyze talent flow for an industry.

    Shows which companies are net importers vs exporters of executive talent.
    """
    service = AnalyticsService(db)
    flow = service.get_talent_flow(industry=industry, days=days)

    return TalentFlowResponse(
        industry=flow["industry"],
        period_days=flow["period_days"],
        net_importers=[TalentFlowEntry(**e) for e in flow["net_importers"]],
        net_exporters=[TalentFlowEntry(**e) for e in flow["net_exporters"]],
        stable=[TalentFlowEntry(**e) for e in flow["stable"]],
    )


@router.get("/talent-flow", response_model=TalentFlowResponse)
async def get_overall_talent_flow(
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Analyze talent flow across all industries.
    """
    service = AnalyticsService(db)
    flow = service.get_talent_flow(industry=None, days=days)

    return TalentFlowResponse(
        industry=flow["industry"],
        period_days=flow["period_days"],
        net_importers=[TalentFlowEntry(**e) for e in flow["net_importers"]],
        net_exporters=[TalentFlowEntry(**e) for e in flow["net_exporters"]],
        stable=[TalentFlowEntry(**e) for e in flow["stable"]],
    )


@router.get("/industries/{industry}/trends", response_model=TrendsResponse)
async def get_change_trends(
    industry: str,
    months: int = Query(12, ge=1, le=24, description="Months of history"),
    db: Session = Depends(get_db),
):
    """
    Get monthly leadership change trends for an industry.

    Returns time series of leadership changes by type.
    """
    service = AnalyticsService(db)
    trends = service.get_change_trends(industry=industry, months=months)

    return TrendsResponse(
        industry=trends["industry"],
        months=trends["months"],
        trends=[TrendDataPoint(**t) for t in trends["trends"]],
    )


@router.get("/trends", response_model=TrendsResponse)
async def get_overall_trends(
    months: int = Query(12, ge=1, le=24, description="Months of history"),
    db: Session = Depends(get_db),
):
    """
    Get monthly leadership change trends across all industries.
    """
    service = AnalyticsService(db)
    trends = service.get_change_trends(industry=None, months=months)

    return TrendsResponse(
        industry=trends["industry"],
        months=trends["months"],
        trends=[TrendDataPoint(**t) for t in trends["trends"]],
    )


@router.get("/industries/{industry}/hot-roles", response_model=List[HotRoleEntry])
async def get_hot_roles(
    industry: str,
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get most frequently hired leadership roles in an industry.

    Identifies roles with highest hiring activity.
    """
    service = AnalyticsService(db)
    roles = service.get_hot_roles(industry=industry, days=days)
    return [HotRoleEntry(**r) for r in roles]


@router.get("/hot-roles", response_model=List[HotRoleEntry])
async def get_overall_hot_roles(
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get most frequently hired leadership roles across all industries.
    """
    service = AnalyticsService(db)
    roles = service.get_hot_roles(industry=None, days=days)
    return [HotRoleEntry(**r) for r in roles]


@router.get("/companies/{company_id}/benchmark", response_model=BenchmarkScoreResponse)
async def get_company_benchmark(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Get team strength benchmark score for a company.

    Returns 0-100 score based on team completeness, tenure, and stability.
    Components:
    - Completeness (25 pts): Key roles filled (CEO, CFO, COO)
    - Depth (25 pts): Total C-suite and VP count
    - Tenure (25 pts): Average C-suite tenure
    - Board (25 pts): Board size and strength
    """
    service = AnalyticsService(db)
    result = service.get_company_benchmark_score(company_id)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return BenchmarkScoreResponse(
        company_id=result["company_id"],
        company_name=result["company_name"],
        team_score=result["team_score"],
        components=BenchmarkComponents(**result["components"]),
        details=BenchmarkDetails(**result["details"]),
    )


@router.get("/portfolios/{portfolio_id}", response_model=PortfolioAnalyticsResponse)
async def get_portfolio_analytics(
    portfolio_id: int,
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get leadership analytics for a specific portfolio.

    Aggregates leadership stats across all portfolio companies.
    """
    service = AnalyticsService(db)
    result = service.get_portfolio_analytics(portfolio_id, days=days)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return PortfolioAnalyticsResponse(**result)
