"""
Investment Trend Analysis API Endpoints.

T23: Surfaces investment trends across LP portfolios including
sector rotation, emerging themes, and geographic shifts.
"""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.analytics.trends import TrendAnalysisService

router = APIRouter(prefix="/trends", tags=["Trends"])


@router.get("/sectors")
def get_sector_trends(
    period: str = Query("quarter", description="Aggregation period: month, quarter, year"),
    periods: int = Query(4, ge=1, le=12, description="Number of periods to return"),
    lp_type: Optional[str] = Query(None, description="Filter by LP type"),
    min_holdings: int = Query(5, ge=1, description="Minimum holdings for sector inclusion"),
    db: Session = Depends(get_db),
):
    """
    Get sector allocation trends over time.

    Returns time series data showing how sector allocations have changed
    across investment portfolios.
    """
    service = TrendAnalysisService(db)
    return service.get_sector_trends(
        period=period,
        periods=periods,
        lp_type=lp_type,
        min_holdings=min_holdings,
    )


@router.get("/emerging")
def get_emerging_sectors(
    limit: int = Query(10, ge=1, le=50, description="Number of sectors to return"),
    lp_type: Optional[str] = Query(None, description="Filter by LP type"),
    db: Session = Depends(get_db),
):
    """
    Get sectors with positive momentum (accelerating investment).

    Returns sectors ranked by momentum score, showing which industries
    are seeing increased investment activity.
    """
    service = TrendAnalysisService(db)
    return service.get_emerging_sectors(limit=limit, lp_type=lp_type)


@router.get("/geographic")
def get_geographic_trends(
    lp_type: Optional[str] = Query(None, description="Filter by LP type"),
    db: Session = Depends(get_db),
):
    """
    Get geographic distribution of investments.

    Returns investment counts by region with top sectors per region.
    """
    service = TrendAnalysisService(db)
    return service.get_geographic_trends(lp_type=lp_type)


@router.get("/stages")
def get_stage_trends(
    lp_type: Optional[str] = Query(None, description="Filter by LP type"),
    db: Session = Depends(get_db),
):
    """
    Get investment stage distribution and trends.

    Returns breakdown of investments by company stage (Seed, Series A, etc.).
    """
    service = TrendAnalysisService(db)
    return service.get_stage_trends(lp_type=lp_type)


@router.get("/by-lp-type")
def get_trends_by_lp_type(
    db: Session = Depends(get_db),
):
    """
    Compare sector allocations by LP type.

    Returns top sectors for each LP type (pension, endowment, etc.)
    to show differences in allocation strategies.
    """
    service = TrendAnalysisService(db)
    return service.get_trends_by_lp_type()


@router.get("/snapshot")
def get_allocation_snapshot(
    db: Session = Depends(get_db),
):
    """
    Get current allocation snapshot across all dimensions.

    Returns a comprehensive view of current portfolio allocations
    by sector, LP type, stage, and region.
    """
    service = TrendAnalysisService(db)
    return service.get_allocation_snapshot()
