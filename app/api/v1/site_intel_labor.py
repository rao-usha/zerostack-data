"""
Site Intelligence Platform - Labor Market API.

Endpoints for wages, employment, commuting patterns, and education data.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel

from app.core.database import get_db
from app.core.models_site_intel import (
    LaborMarketArea,
    OccupationalWage,
    IndustryEmployment,
    CommuteFlow,
    EducationalAttainment,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/site-intel/labor", tags=["Site Intel - Labor"])


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get("/areas")
async def list_labor_market_areas(
    state: Optional[str] = Query(None),
    area_type: Optional[str] = Query(None, description="Type: metro, county, state"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """List labor market areas with key statistics."""
    query = db.query(LaborMarketArea)

    if state:
        query = query.filter(LaborMarketArea.state == state.upper())
    if area_type:
        query = query.filter(LaborMarketArea.area_type == area_type)

    areas = (
        query.order_by(LaborMarketArea.labor_force.desc().nullslast())
        .limit(limit)
        .all()
    )

    return [
        {
            "id": a.id,
            "area_code": a.area_code,
            "area_name": a.area_name,
            "area_type": a.area_type,
            "state": a.state,
            "population": a.population,
            "labor_force": a.labor_force,
            "unemployment_rate": float(a.unemployment_rate)
            if a.unemployment_rate
            else None,
        }
        for a in areas
    ]


@router.get("/wages")
async def search_occupational_wages(
    area_code: Optional[str] = Query(None, description="Area code (FIPS or CBSA)"),
    occupation_code: Optional[str] = Query(None, description="SOC occupation code"),
    occupation_search: Optional[str] = Query(
        None, description="Search occupation title"
    ),
    year: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Query occupational wages.

    Returns wage data by occupation and area from BLS OES.
    """
    query = db.query(OccupationalWage)

    if area_code:
        query = query.filter(OccupationalWage.area_code == area_code)
    if occupation_code:
        query = query.filter(OccupationalWage.occupation_code == occupation_code)
    if occupation_search:
        query = query.filter(
            OccupationalWage.occupation_title.ilike(f"%{occupation_search}%")
        )
    if year:
        query = query.filter(OccupationalWage.period_year == year)

    query = query.order_by(OccupationalWage.median_annual_wage.desc().nullslast())
    wages = query.limit(limit).all()

    return [
        {
            "area_code": w.area_code,
            "area_name": w.area_name,
            "occupation_code": w.occupation_code,
            "occupation_title": w.occupation_title,
            "employment": w.employment,
            "mean_hourly_wage": float(w.mean_hourly_wage)
            if w.mean_hourly_wage
            else None,
            "median_hourly_wage": float(w.median_hourly_wage)
            if w.median_hourly_wage
            else None,
            "median_annual_wage": float(w.median_annual_wage)
            if w.median_annual_wage
            else None,
            "year": w.period_year,
        }
        for w in wages
    ]


@router.get("/wages/comparison")
async def compare_wages_across_areas(
    occupation_code: str = Query(..., description="SOC occupation code to compare"),
    area_codes: str = Query(..., description="Comma-separated area codes"),
    db: Session = Depends(get_db),
):
    """
    Compare wages for an occupation across multiple areas.

    Useful for site selection labor cost analysis.
    """
    codes = [c.strip() for c in area_codes.split(",")]

    wages = (
        db.query(OccupationalWage)
        .filter(
            OccupationalWage.occupation_code == occupation_code,
            OccupationalWage.area_code.in_(codes),
        )
        .order_by(OccupationalWage.period_year.desc())
        .all()
    )

    # Get latest for each area
    latest_by_area = {}
    for w in wages:
        if w.area_code not in latest_by_area:
            latest_by_area[w.area_code] = {
                "area_code": w.area_code,
                "area_name": w.area_name,
                "median_hourly_wage": float(w.median_hourly_wage)
                if w.median_hourly_wage
                else None,
                "median_annual_wage": float(w.median_annual_wage)
                if w.median_annual_wage
                else None,
                "employment": w.employment,
                "year": w.period_year,
            }

    return {
        "occupation_code": occupation_code,
        "comparison": sorted(
            latest_by_area.values(), key=lambda x: x["median_hourly_wage"] or 999
        ),
    }


@router.get("/employment")
async def search_industry_employment(
    area_fips: Optional[str] = Query(None, description="Area FIPS code"),
    industry_code: Optional[str] = Query(None, description="NAICS code"),
    year: Optional[int] = Query(None),
    quarter: Optional[int] = Query(None, ge=1, le=4),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Query industry employment data from QCEW.

    Returns establishment counts, employment, and wages by industry.
    """
    query = db.query(IndustryEmployment)

    if area_fips:
        query = query.filter(IndustryEmployment.area_fips == area_fips)
    if industry_code:
        query = query.filter(IndustryEmployment.industry_code == industry_code)
    if year:
        query = query.filter(IndustryEmployment.period_year == year)
    if quarter:
        query = query.filter(IndustryEmployment.period_quarter == quarter)

    query = query.order_by(IndustryEmployment.avg_monthly_employment.desc().nullslast())
    employment = query.limit(limit).all()

    return [
        {
            "area_fips": e.area_fips,
            "area_name": e.area_name,
            "industry_code": e.industry_code,
            "industry_title": e.industry_title,
            "establishments": e.establishments,
            "avg_monthly_employment": e.avg_monthly_employment,
            "avg_weekly_wage": float(e.avg_weekly_wage) if e.avg_weekly_wage else None,
            "year": e.period_year,
            "quarter": e.period_quarter,
        }
        for e in employment
    ]


@router.get("/commute-shed")
async def get_commute_shed(
    work_county_fips: str = Query(..., description="Work location county FIPS"),
    min_workers: int = Query(100, description="Minimum worker count to include"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Get commute shed for a work location.

    Shows where workers commute from - useful for labor availability analysis.
    """
    flows = (
        db.query(CommuteFlow)
        .filter(
            CommuteFlow.work_county_fips == work_county_fips,
            CommuteFlow.worker_count >= min_workers,
        )
        .order_by(CommuteFlow.worker_count.desc())
        .limit(limit)
        .all()
    )

    total_workers = sum(f.worker_count for f in flows)

    return {
        "work_county_fips": work_county_fips,
        "total_commuters": total_workers,
        "origin_counties": [
            {
                "home_county_fips": f.home_county_fips,
                "home_county_name": f.home_county_name,
                "home_state": f.home_state,
                "worker_count": f.worker_count,
                "pct_of_total": round(f.worker_count / total_workers * 100, 1)
                if total_workers > 0
                else 0,
                "avg_earnings": float(f.avg_earnings) if f.avg_earnings else None,
            }
            for f in flows
        ],
    }


@router.get("/education")
async def get_educational_attainment(
    area_fips: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Get educational attainment by area.

    Shows percentage of population with various education levels.
    """
    query = db.query(EducationalAttainment)

    if area_fips:
        query = query.filter(EducationalAttainment.area_fips == area_fips)
    if state:
        query = query.filter(EducationalAttainment.area_fips.like(f"{state}%"))

    query = query.order_by(EducationalAttainment.pct_bachelors.desc().nullslast())
    education = query.limit(limit).all()

    return [
        {
            "area_fips": e.area_fips,
            "area_name": e.area_name,
            "population_25_plus": e.population_25_plus,
            "pct_high_school": float(e.pct_high_school) if e.pct_high_school else None,
            "pct_bachelors": float(e.pct_bachelors) if e.pct_bachelors else None,
            "pct_graduate": float(e.pct_graduate) if e.pct_graduate else None,
            "year": e.period_year,
        }
        for e in education
    ]


@router.get("/workforce-score")
async def get_workforce_score(
    area_code: str = Query(..., description="Labor market area code"),
    db: Session = Depends(get_db),
):
    """
    Get composite workforce score for an area.

    Factors in labor force size, education, wages, and unemployment.
    """
    area = (
        db.query(LaborMarketArea).filter(LaborMarketArea.area_code == area_code).first()
    )
    education = (
        db.query(EducationalAttainment)
        .filter(EducationalAttainment.area_fips == area_code)
        .first()
    )

    if not area:
        return {"error": "Area not found", "area_code": area_code}

    # Simple scoring algorithm
    labor_score = min((area.labor_force or 0) / 10000, 30)  # Max 30
    education_score = (
        (float(education.pct_bachelors or 0) / 100) * 30 if education else 0
    )  # Max 30
    unemployment_score = max(0, 20 - (float(area.unemployment_rate or 5) * 2))  # Max 20

    total = labor_score + education_score + unemployment_score

    return {
        "area_code": area_code,
        "area_name": area.area_name,
        "workforce_score": round(min(total, 100), 1),
        "factors": {
            "labor_force_size": {
                "value": area.labor_force,
                "score": round(labor_score, 1),
            },
            "education": {
                "pct_bachelors": float(education.pct_bachelors) if education else None,
                "score": round(education_score, 1),
            },
            "unemployment": {
                "rate": float(area.unemployment_rate)
                if area.unemployment_rate
                else None,
                "score": round(unemployment_score, 1),
            },
        },
    }


@router.get("/summary")
async def get_labor_summary(db: Session = Depends(get_db)):
    """Get summary statistics for labor market data."""
    return {
        "domain": "labor",
        "record_counts": {
            "labor_market_areas": db.query(func.count(LaborMarketArea.id)).scalar(),
            "occupational_wages": db.query(func.count(OccupationalWage.id)).scalar(),
            "industry_employment": db.query(func.count(IndustryEmployment.id)).scalar(),
            "commute_flows": db.query(func.count(CommuteFlow.id)).scalar(),
            "education_records": db.query(
                func.count(EducationalAttainment.id)
            ).scalar(),
        },
        "available_endpoints": [
            "/site-intel/labor/areas",
            "/site-intel/labor/wages",
            "/site-intel/labor/wages/comparison",
            "/site-intel/labor/employment",
            "/site-intel/labor/commute-shed",
            "/site-intel/labor/education",
            "/site-intel/labor/workforce-score",
        ],
    }
