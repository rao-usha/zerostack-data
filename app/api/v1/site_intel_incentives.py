"""
Site Intelligence Platform - Incentives & Real Estate API.

Endpoints for Opportunity Zones, FTZ, incentive programs, and industrial sites.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel

from app.core.database import get_db
from app.core.models_site_intel import (
    OpportunityZone,
    ForeignTradeZone,
    IncentiveProgram,
    IncentiveDeal,
    IndustrialSite,
    ZoningDistrict,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/site-intel/incentives", tags=["Site Intel - Incentives"])


# =============================================================================
# OPPORTUNITY ZONE ENDPOINTS
# =============================================================================


@router.get("/opportunity-zones")
async def search_opportunity_zones(
    state: Optional[str] = Query(None),
    county: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search Opportunity Zone census tracts."""
    query = db.query(OpportunityZone)

    if state:
        query = query.filter(OpportunityZone.state == state.upper())
    if county:
        query = query.filter(OpportunityZone.county.ilike(f"%{county}%"))

    zones = query.limit(limit).all()

    return [
        {
            "id": z.id,
            "tract_geoid": z.tract_geoid,
            "state": z.state,
            "county": z.county,
            "is_low_income": z.is_low_income,
            "designation_date": z.designation_date.isoformat()
            if z.designation_date
            else None,
        }
        for z in zones
    ]


@router.get("/opportunity-zones/at-location")
async def check_opportunity_zone_at_location(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Check if a location is in an Opportunity Zone.

    OZ provides capital gains tax benefits for qualified investments.
    """
    # Full implementation requires PostGIS ST_Contains
    return {
        "location": {"latitude": lat, "longitude": lng},
        "in_opportunity_zone": None,
        "tract_geoid": None,
        "note": "Full OZ lookup requires PostGIS. Use Census Geocoder to get tract ID.",
        "benefits": [
            "Temporary tax deferral on prior capital gains",
            "Step-up in basis for gains held 5+ years",
            "Permanent exclusion of gains on OZ investments held 10+ years",
        ],
    }


# =============================================================================
# FOREIGN TRADE ZONE ENDPOINTS
# =============================================================================


@router.get("/ftz")
async def search_foreign_trade_zones(
    state: Optional[str] = Query(None),
    status: Optional[str] = Query("active", description="Status: active, pending"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search Foreign Trade Zones."""
    query = db.query(ForeignTradeZone)

    if state:
        query = query.filter(ForeignTradeZone.state == state.upper())
    if status:
        query = query.filter(ForeignTradeZone.status == status)

    ftzs = query.order_by(ForeignTradeZone.ftz_number).limit(limit).all()

    return [
        {
            "id": f.id,
            "ftz_number": f.ftz_number,
            "zone_name": f.zone_name,
            "grantee": f.grantee,
            "state": f.state,
            "city": f.city,
            "latitude": float(f.latitude) if f.latitude else None,
            "longitude": float(f.longitude) if f.longitude else None,
            "acreage": float(f.acreage) if f.acreage else None,
            "status": f.status,
        }
        for f in ftzs
    ]


@router.get("/ftz/nearby")
async def find_nearby_ftz(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(50, gt=0, le=200),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Find Foreign Trade Zones within radius.

    FTZ provides duty deferral, reduction, and elimination benefits.
    """
    distance_expr = (
        3959
        * func.acos(
            func.cos(func.radians(lat))
            * func.cos(func.radians(ForeignTradeZone.latitude))
            * func.cos(func.radians(ForeignTradeZone.longitude) - func.radians(lng))
            + func.sin(func.radians(lat))
            * func.sin(func.radians(ForeignTradeZone.latitude))
        )
    ).label("distance_miles")

    query = db.query(ForeignTradeZone, distance_expr).filter(
        ForeignTradeZone.latitude.isnot(None),
    )

    lat_range = radius_miles / 69.0
    query = query.filter(
        ForeignTradeZone.latitude.between(lat - lat_range, lat + lat_range),
    )

    results = query.order_by("distance_miles").limit(limit * 2).all()

    ftzs = []
    for ftz, distance in results:
        if distance and distance <= radius_miles:
            ftzs.append(
                {
                    "ftz_number": ftz.ftz_number,
                    "zone_name": ftz.zone_name,
                    "grantee": ftz.grantee,
                    "city": ftz.city,
                    "state": ftz.state,
                    "distance_miles": round(distance, 2),
                }
            )
            if len(ftzs) >= limit:
                break

    return {
        "location": {"latitude": lat, "longitude": lng},
        "ftz_nearby": ftzs,
        "benefits": [
            "Defer, reduce, or eliminate customs duties",
            "No duties on re-exported goods",
            "Inverted tariff benefits",
            "Weekly customs entry vs per-shipment",
        ],
    }


# =============================================================================
# INCENTIVE PROGRAM ENDPOINTS
# =============================================================================


@router.get("/programs")
async def search_incentive_programs(
    state: Optional[str] = Query(None),
    program_type: Optional[str] = Query(
        None, description="Type: tax_credit, grant, abatement, financing"
    ),
    target: Optional[str] = Query(
        None, description="Target: data_center, manufacturing, warehouse"
    ),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search state and local incentive programs."""
    query = db.query(IncentiveProgram)

    if state:
        query = query.filter(IncentiveProgram.state == state.upper())
    if program_type:
        query = query.filter(IncentiveProgram.program_type == program_type)
    if target:
        # JSON array contains check
        query = query.filter(IncentiveProgram.target_investments.contains([target]))

    programs = query.limit(limit).all()

    return [
        {
            "id": p.id,
            "program_name": p.program_name,
            "program_type": p.program_type,
            "state": p.state,
            "geography_name": p.geography_name,
            "target_industries": p.target_industries,
            "target_investments": p.target_investments,
            "min_investment": p.min_investment,
            "min_jobs": p.min_jobs,
            "max_benefit": p.max_benefit,
            "description": p.description[:200] + "..."
            if p.description and len(p.description) > 200
            else p.description,
        }
        for p in programs
    ]


@router.get("/programs/by-state/{state}")
async def get_state_incentives(
    state: str,
    db: Session = Depends(get_db),
):
    """Get all incentive programs for a state."""
    programs = (
        db.query(IncentiveProgram).filter(IncentiveProgram.state == state.upper()).all()
    )

    by_type = {}
    for p in programs:
        ptype = p.program_type or "other"
        if ptype not in by_type:
            by_type[ptype] = []
        by_type[ptype].append(
            {
                "program_name": p.program_name,
                "max_benefit": p.max_benefit,
                "min_investment": p.min_investment,
                "min_jobs": p.min_jobs,
            }
        )

    return {
        "state": state.upper(),
        "program_count": len(programs),
        "by_type": by_type,
    }


# =============================================================================
# INCENTIVE DEALS ENDPOINTS
# =============================================================================


@router.get("/deals")
async def search_incentive_deals(
    company: Optional[str] = Query(None, description="Search company name"),
    state: Optional[str] = Query(None),
    industry: Optional[str] = Query(None),
    min_value: Optional[int] = Query(None, description="Minimum subsidy value"),
    year: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Search disclosed incentive deals.

    Data from Good Jobs First Subsidy Tracker.
    """
    query = db.query(IncentiveDeal)

    if company:
        query = query.filter(IncentiveDeal.company_name.ilike(f"%{company}%"))
    if state:
        query = query.filter(IncentiveDeal.state == state.upper())
    if industry:
        query = query.filter(IncentiveDeal.industry.ilike(f"%{industry}%"))
    if min_value:
        query = query.filter(IncentiveDeal.subsidy_value >= min_value)
    if year:
        query = query.filter(IncentiveDeal.year == year)

    query = query.order_by(IncentiveDeal.subsidy_value.desc().nullslast())
    deals = query.limit(limit).all()

    return [
        {
            "id": d.id,
            "company": d.company_name,
            "parent_company": d.parent_company,
            "state": d.state,
            "city": d.city,
            "year": d.year,
            "subsidy_type": d.subsidy_type,
            "subsidy_value": d.subsidy_value,
            "jobs_announced": d.jobs_announced,
            "investment_announced": d.investment_announced,
            "industry": d.industry,
        }
        for d in deals
    ]


@router.get("/deals/benchmark")
async def benchmark_incentive_deals(
    industry: str = Query(..., description="Industry to benchmark"),
    db: Session = Depends(get_db),
):
    """
    Benchmark incentive deals by industry.

    Shows typical deal sizes and terms for negotiation context.
    """
    deals = (
        db.query(IncentiveDeal)
        .filter(
            IncentiveDeal.industry.ilike(f"%{industry}%"),
            IncentiveDeal.subsidy_value.isnot(None),
        )
        .all()
    )

    if not deals:
        return {"industry": industry, "deals_found": 0}

    values = [d.subsidy_value for d in deals if d.subsidy_value]
    jobs = [d.jobs_announced for d in deals if d.jobs_announced]
    investments = [d.investment_announced for d in deals if d.investment_announced]

    return {
        "industry": industry,
        "deals_analyzed": len(deals),
        "subsidy_value": {
            "median": sorted(values)[len(values) // 2] if values else None,
            "average": sum(values) / len(values) if values else None,
            "max": max(values) if values else None,
        },
        "jobs_announced": {
            "median": sorted(jobs)[len(jobs) // 2] if jobs else None,
            "average": sum(jobs) / len(jobs) if jobs else None,
        },
        "investment_announced": {
            "median": sorted(investments)[len(investments) // 2] if investments else None,
            "average": sum(investments) / len(investments) if investments else None,
        },
        "subsidy_per_job": {
            "average": sum(values) / sum(jobs)
            if values and jobs and sum(jobs) > 0
            else None,
        },
        "top_states": list(set(d.state for d in deals[:20])),
    }


# =============================================================================
# INDUSTRIAL SITE ENDPOINTS
# =============================================================================


@router.get("/sites")
async def search_industrial_sites(
    state: Optional[str] = Query(None),
    site_type: Optional[str] = Query(
        None, description="Type: greenfield, building, spec_building"
    ),
    min_acreage: Optional[float] = Query(None),
    rail_served: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Search available industrial sites."""
    query = db.query(IndustrialSite)

    if state:
        query = query.filter(IndustrialSite.state == state.upper())
    if site_type:
        query = query.filter(IndustrialSite.site_type == site_type)
    if min_acreage:
        query = query.filter(IndustrialSite.acreage >= min_acreage)
    if rail_served is not None:
        query = query.filter(IndustrialSite.rail_served == rail_served)

    sites = query.order_by(IndustrialSite.acreage.desc().nullslast()).limit(limit).all()

    return [
        {
            "id": s.id,
            "site_name": s.site_name,
            "site_type": s.site_type,
            "city": s.city,
            "state": s.state,
            "county": s.county,
            "acreage": float(s.acreage) if s.acreage else None,
            "building_sqft": s.building_sqft,
            "available_sqft": s.available_sqft,
            "rail_served": s.rail_served,
            "utilities": s.utilities_available,
            "contact": s.edo_name,
        }
        for s in sites
    ]


@router.get("/sites/nearby")
async def find_nearby_industrial_sites(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(50, gt=0, le=200),
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Find industrial sites within radius."""
    distance_expr = (
        3959
        * func.acos(
            func.cos(func.radians(lat))
            * func.cos(func.radians(IndustrialSite.latitude))
            * func.cos(func.radians(IndustrialSite.longitude) - func.radians(lng))
            + func.sin(func.radians(lat))
            * func.sin(func.radians(IndustrialSite.latitude))
        )
    ).label("distance_miles")

    query = db.query(IndustrialSite, distance_expr).filter(
        IndustrialSite.latitude.isnot(None),
    )

    lat_range = radius_miles / 69.0
    query = query.filter(
        IndustrialSite.latitude.between(lat - lat_range, lat + lat_range),
    )

    results = query.order_by("distance_miles").limit(limit * 2).all()

    sites = []
    for site, distance in results:
        if distance and distance <= radius_miles:
            sites.append(
                {
                    "id": site.id,
                    "site_name": site.site_name,
                    "site_type": site.site_type,
                    "city": site.city,
                    "state": site.state,
                    "acreage": float(site.acreage) if site.acreage else None,
                    "distance_miles": round(distance, 2),
                }
            )
            if len(sites) >= limit:
                break

    return sites


# =============================================================================
# ZONING ENDPOINTS
# =============================================================================


@router.get("/zoning/at-location")
async def get_zoning_at_location(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Get zoning information for a location.

    Note: Full implementation requires PostGIS for polygon intersection.
    """
    return {
        "location": {"latitude": lat, "longitude": lng},
        "zoning": None,
        "note": "Full zoning lookup requires PostGIS or local jurisdiction API",
        "recommendation": "Check with local planning department for official zoning determination",
    }


@router.get("/summary")
async def get_incentives_summary(db: Session = Depends(get_db)):
    """Get summary statistics for incentives data."""
    return {
        "domain": "incentives",
        "record_counts": {
            "opportunity_zones": db.query(func.count(OpportunityZone.id)).scalar(),
            "foreign_trade_zones": db.query(func.count(ForeignTradeZone.id)).scalar(),
            "incentive_programs": db.query(func.count(IncentiveProgram.id)).scalar(),
            "incentive_deals": db.query(func.count(IncentiveDeal.id)).scalar(),
            "industrial_sites": db.query(func.count(IndustrialSite.id)).scalar(),
            "zoning_districts": db.query(func.count(ZoningDistrict.id)).scalar(),
        },
        "available_endpoints": [
            "/site-intel/incentives/opportunity-zones",
            "/site-intel/incentives/ftz",
            "/site-intel/incentives/ftz/nearby",
            "/site-intel/incentives/programs",
            "/site-intel/incentives/programs/by-state/{state}",
            "/site-intel/incentives/deals",
            "/site-intel/incentives/deals/benchmark",
            "/site-intel/incentives/sites",
            "/site-intel/incentives/sites/nearby",
        ],
    }
