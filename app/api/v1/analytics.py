"""
Dashboard Analytics API (T13).

Endpoints providing pre-computed analytics for frontend dashboards.
"""

import logging
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.analytics.dashboard import get_dashboard_analytics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["Dashboard Analytics"])


# =============================================================================
# Response Models
# =============================================================================


class CollectionStats(BaseModel):
    """Collection job statistics."""

    jobs_last_24h: int
    jobs_last_7d: int
    jobs_last_30d: int
    success_rate_7d: float
    avg_companies_per_job: float
    total_companies_collected_7d: int


class AlertStats(BaseModel):
    """Alert statistics."""

    pending_alerts: int
    alerts_triggered_today: int
    alerts_triggered_7d: int
    active_subscriptions: int


class SystemOverviewResponse(BaseModel):
    """System-wide statistics for main dashboard."""

    total_lps: int
    total_family_offices: int
    lps_with_portfolio_data: int
    fos_with_portfolio_data: int
    coverage_percentage: float

    total_portfolio_companies: int
    unique_companies: int
    total_market_value_usd: Optional[float]

    companies_by_source: Dict[str, int]
    collection_stats: CollectionStats
    alert_stats: AlertStats

    last_collection_at: Optional[str]
    avg_data_age_days: float


class PortfolioSummary(BaseModel):
    """Portfolio summary for an investor."""

    total_companies: int
    total_market_value_usd: Optional[float]
    sources_used: List[str]
    last_updated: Optional[str]
    data_age_days: int


class IndustryBreakdown(BaseModel):
    """Industry breakdown item."""

    industry: str
    company_count: int
    percentage: float
    total_value_usd: Optional[float]


class HoldingSummary(BaseModel):
    """Top holding summary."""

    company_name: str
    industry: Optional[str]
    market_value_usd: Optional[float]
    shares_held: Optional[int]
    source_type: Optional[str]
    confidence_level: Optional[float]


class DataQualityScore(BaseModel):
    """Data quality score breakdown."""

    overall_score: int
    completeness: int
    freshness: int
    source_diversity: int
    confidence_avg: float
    issues: List[str]


class CollectionEvent(BaseModel):
    """Collection history event."""

    job_id: int
    date: Optional[str]
    status: Optional[str]
    companies_found: int
    strategies_used: List[str]


class InvestorAnalyticsResponse(BaseModel):
    """Detailed analytics for a single investor."""

    investor_id: int
    investor_type: str
    investor_name: str
    portfolio_summary: PortfolioSummary
    industry_distribution: List[IndustryBreakdown]
    top_holdings: List[HoldingSummary]
    data_quality: DataQualityScore
    collection_history: List[CollectionEvent]


class TrendDataPoint(BaseModel):
    """Single data point in a trend."""

    date: Optional[str]
    value: int
    details: Optional[Dict[str, Any]]


class TrendSummary(BaseModel):
    """Summary statistics for a trend."""

    total: int
    average: float
    min: int
    max: int
    trend_direction: str
    change_percentage: float


class TrendsResponse(BaseModel):
    """Time-series trend data response."""

    period: str
    metric: str
    data_points: List[TrendDataPoint]
    summary: TrendSummary


class PortfolioMover(BaseModel):
    """Recent portfolio change/mover."""

    investor_id: int
    investor_type: str
    investor_name: Optional[str]
    change_type: str
    company_name: str
    details: Dict[str, Any]
    detected_at: Optional[str]


class TopMoversResponse(BaseModel):
    """Recent portfolio changes response."""

    movers: List[PortfolioMover]
    generated_at: str


class IndustryStats(BaseModel):
    """Industry statistics."""

    industry: str
    company_count: int
    percentage: float
    investor_count: int
    total_value_usd: Optional[float]
    top_companies: List[str]


class IndustryBreakdownResponse(BaseModel):
    """Aggregate industry breakdown response."""

    total_companies: int
    industries: List[IndustryStats]
    other_count: int


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/overview", response_model=SystemOverviewResponse)
async def get_system_overview(db: Session = Depends(get_db)):
    """
    📊 Get system-wide analytics overview.

    Returns comprehensive statistics for the main dashboard including:
    - Investor coverage (LPs and Family Offices with portfolio data)
    - Portfolio totals (companies, unique names, market value)
    - Data source breakdown
    - Collection activity (jobs in last 24h, 7d, 30d)
    - Alert statistics
    - Data freshness metrics

    This endpoint is designed to power dashboard summary cards and header stats.
    """
    try:
        analytics = get_dashboard_analytics(db)
        result = await analytics.get_system_overview()

        return SystemOverviewResponse(
            total_lps=result["total_lps"],
            total_family_offices=result["total_family_offices"],
            lps_with_portfolio_data=result["lps_with_portfolio_data"],
            fos_with_portfolio_data=result["fos_with_portfolio_data"],
            coverage_percentage=result["coverage_percentage"],
            total_portfolio_companies=result["total_portfolio_companies"],
            unique_companies=result["unique_companies"],
            total_market_value_usd=result["total_market_value_usd"],
            companies_by_source=result["companies_by_source"],
            collection_stats=CollectionStats(**result["collection_stats"]),
            alert_stats=AlertStats(**result["alert_stats"]),
            last_collection_at=result["last_collection_at"],
            avg_data_age_days=result["avg_data_age_days"],
        )

    except Exception as e:
        logger.error(f"Error getting system overview: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/investor/{investor_id}", response_model=InvestorAnalyticsResponse)
async def get_investor_analytics(
    investor_id: int,
    investor_type: str = Query(..., description="'lp' or 'family_office'"),
    db: Session = Depends(get_db),
):
    """
    📈 Get detailed analytics for a single investor.

    Returns comprehensive analytics including:
    - **Portfolio Summary**: Total companies, market value, data sources, freshness
    - **Industry Distribution**: Breakdown by industry (for pie charts)
    - **Top Holdings**: Top 10 companies by market value
    - **Data Quality Score**: 0-100 score with breakdown (completeness, freshness, diversity)
    - **Collection History**: Recent data collection jobs for this investor

    **Data Quality Scoring:**
    - Completeness (40 pts): % of fields populated (industry, value, confidence)
    - Freshness (25 pts): Based on data age (<7d = 25, <30d = 15, <90d = 5)
    - Source Diversity (20 pts): Multiple sources = higher (3+ = 20, 2 = 12, 1 = 5)
    - Confidence (15 pts): Average confidence level * 15
    """
    try:
        if investor_type not in ("lp", "family_office"):
            raise HTTPException(
                status_code=400, detail="investor_type must be 'lp' or 'family_office'"
            )

        analytics = get_dashboard_analytics(db)
        result = await analytics.get_investor_analytics(investor_id, investor_type)

        return InvestorAnalyticsResponse(
            investor_id=result["investor_id"],
            investor_type=result["investor_type"],
            investor_name=result["investor_name"],
            portfolio_summary=PortfolioSummary(**result["portfolio_summary"]),
            industry_distribution=[
                IndustryBreakdown(**ind) for ind in result["industry_distribution"]
            ],
            top_holdings=[HoldingSummary(**h) for h in result["top_holdings"]],
            data_quality=DataQualityScore(**result["data_quality"]),
            collection_history=[
                CollectionEvent(**e) for e in result["collection_history"]
            ],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting investor analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends", response_model=TrendsResponse)
async def get_trends(
    period: str = Query("30d", description="Time period: 7d, 30d, or 90d"),
    metric: str = Query(
        "collections", description="Metric: collections, companies, or alerts"
    ),
    db: Session = Depends(get_db),
):
    """
    📉 Get time-series trend data for charts.

    Returns daily data points for the specified metric and period, plus summary statistics.

    **Metrics:**
    - `collections`: Collection jobs per day (with success count and companies found)
    - `companies`: New portfolio companies added per day
    - `alerts`: Alerts triggered per day

    **Periods:**
    - `7d`: Last 7 days
    - `30d`: Last 30 days (default)
    - `90d`: Last 90 days

    **Summary includes:**
    - Total, average, min, max values
    - Trend direction (up/down/stable)
    - Change percentage vs previous period
    """
    try:
        if period not in ("7d", "30d", "90d"):
            raise HTTPException(
                status_code=400, detail="period must be '7d', '30d', or '90d'"
            )

        if metric not in ("collections", "companies", "alerts"):
            raise HTTPException(
                status_code=400,
                detail="metric must be 'collections', 'companies', or 'alerts'",
            )

        analytics = get_dashboard_analytics(db)
        result = await analytics.get_trends(period, metric)

        return TrendsResponse(
            period=result["period"],
            metric=result["metric"],
            data_points=[TrendDataPoint(**dp) for dp in result["data_points"]],
            summary=TrendSummary(**result["summary"]),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting trends: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-movers", response_model=TopMoversResponse)
async def get_top_movers(
    limit: int = Query(20, ge=1, le=100, description="Number of movers to return"),
    change_type: Optional[str] = Query(
        None,
        description="Filter by change type: new_holding, removed_holding, value_change, shares_change",
    ),
    db: Session = Depends(get_db),
):
    """
    🔥 Get recent significant portfolio changes.

    Returns recent portfolio changes suitable for an activity feed or alerts widget.
    Changes are sourced from portfolio alerts or recent portfolio additions.

    **Change Types:**
    - `new_holding`: Investor added a new company
    - `removed_holding`: Investor exited a position
    - `value_change`: Position value changed significantly
    - `shares_change`: Share count changed significantly

    **Response includes:**
    - Investor info (id, type, name)
    - Change details (type, company, specifics)
    - Detection timestamp
    """
    try:
        valid_types = {
            "new_holding",
            "removed_holding",
            "value_change",
            "shares_change",
        }
        if change_type and change_type not in valid_types:
            raise HTTPException(
                status_code=400, detail=f"change_type must be one of: {valid_types}"
            )

        analytics = get_dashboard_analytics(db)
        result = await analytics.get_top_movers(limit, change_type)

        return TopMoversResponse(
            movers=[PortfolioMover(**m) for m in result["movers"]],
            generated_at=result["generated_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting top movers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/industry-breakdown", response_model=IndustryBreakdownResponse)
async def get_industry_breakdown(
    investor_type: Optional[str] = Query(
        None, description="Filter by investor type: 'lp' or 'family_office'"
    ),
    limit: int = Query(20, ge=1, le=100, description="Top N industries to return"),
    db: Session = Depends(get_db),
):
    """
    🏭 Get aggregate industry distribution across all portfolios.

    Returns a breakdown of portfolio companies by industry, useful for pie charts
    and sector analysis.

    **Response includes:**
    - Industry name
    - Company count and percentage
    - Number of investors holding companies in this industry
    - Total market value
    - Top 3 company names per industry

    **Filtering:**
    - Use `investor_type` to see breakdown for just LPs or Family Offices
    - Use `limit` to control how many industries are returned (rest go to "other")
    """
    try:
        if investor_type and investor_type not in ("lp", "family_office"):
            raise HTTPException(
                status_code=400, detail="investor_type must be 'lp' or 'family_office'"
            )

        analytics = get_dashboard_analytics(db)
        result = await analytics.get_industry_breakdown(investor_type, limit)

        return IndustryBreakdownResponse(
            total_companies=result["total_companies"],
            industries=[IndustryStats(**ind) for ind in result["industries"]],
            other_count=result["other_count"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting industry breakdown: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
