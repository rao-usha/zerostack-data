"""
Site Intelligence Platform - Risk & Environmental API.

Endpoints for flood zones, seismic hazard, climate, and environmental data.
"""
import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel

from app.core.database import get_db
from app.core.models_site_intel import (
    FloodZone, SeismicHazard, FaultLine, ClimateData,
    EnvironmentalFacility, Wetland, NationalRiskIndex,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/site-intel/risk", tags=["Site Intel - Risk"])


# =============================================================================
# FLOOD ENDPOINTS
# =============================================================================

@router.get("/flood")
async def search_flood_zones(
    state: Optional[str] = Query(None),
    zone_code: Optional[str] = Query(None, description="Zone code (A, AE, V, VE, X)"),
    high_risk_only: bool = Query(False, description="Only return high-risk zones"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search flood zones from FEMA NFHL."""
    query = db.query(FloodZone)

    if state:
        query = query.filter(FloodZone.state == state.upper())
    if zone_code:
        query = query.filter(FloodZone.zone_code == zone_code.upper())
    if high_risk_only:
        query = query.filter(FloodZone.is_high_risk == True)

    zones = query.limit(limit).all()

    return [
        {
            "id": z.id,
            "zone_code": z.zone_code,
            "zone_description": z.zone_description,
            "is_high_risk": z.is_high_risk,
            "is_coastal": z.is_coastal,
            "state": z.state,
            "county": z.county,
        }
        for z in zones
    ]


@router.get("/flood/at-location")
async def get_flood_risk_at_location(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Get flood risk at a specific location.

    Note: Full implementation requires PostGIS for polygon intersection.
    """
    # This is a simplified lookup - full version uses PostGIS ST_Contains
    # For now, return nearby zone information
    lat_range = 0.05  # ~3 miles

    nearby_zones = db.query(FloodZone).filter(
        FloodZone.state.isnot(None),  # Placeholder filter
    ).limit(5).all()

    return {
        "location": {"latitude": lat, "longitude": lng},
        "flood_risk": {
            "zone": "Unknown - requires PostGIS spatial lookup",
            "is_high_risk": None,
            "recommendation": "Use FEMA's Flood Map Service Center for official determination"
        },
        "note": "Full flood zone lookup requires PostGIS extension"
    }


# =============================================================================
# NATIONAL RISK INDEX ENDPOINTS
# =============================================================================

@router.get("/nri")
async def search_nri_counties(
    state: Optional[str] = Query(None),
    min_risk_score: Optional[float] = Query(None, description="Minimum risk score"),
    max_risk_score: Optional[float] = Query(None, description="Maximum risk score"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search FEMA National Risk Index by county."""
    query = db.query(NationalRiskIndex)

    if state:
        query = query.filter(NationalRiskIndex.state == state.upper())
    if min_risk_score is not None:
        query = query.filter(NationalRiskIndex.risk_score >= min_risk_score)
    if max_risk_score is not None:
        query = query.filter(NationalRiskIndex.risk_score <= max_risk_score)

    query = query.order_by(NationalRiskIndex.risk_score.desc().nullslast())
    counties = query.limit(limit).all()

    return [
        {
            "county_fips": c.county_fips,
            "county_name": c.county_name,
            "state": c.state,
            "risk_score": float(c.risk_score) if c.risk_score else None,
            "risk_rating": c.risk_rating,
            "earthquake_score": float(c.earthquake_score) if c.earthquake_score else None,
            "flood_score": float(c.flood_score) if c.flood_score else None,
            "tornado_score": float(c.tornado_score) if c.tornado_score else None,
            "hurricane_score": float(c.hurricane_score) if c.hurricane_score else None,
            "wildfire_score": float(c.wildfire_score) if c.wildfire_score else None,
        }
        for c in counties
    ]


@router.get("/nri/county/{county_fips}")
async def get_county_nri(
    county_fips: str,
    db: Session = Depends(get_db),
):
    """Get detailed NRI data for a specific county."""
    county = db.query(NationalRiskIndex).filter(
        NationalRiskIndex.county_fips == county_fips
    ).first()

    if not county:
        return {"error": "County not found", "county_fips": county_fips}

    return {
        "county_fips": county.county_fips,
        "county_name": county.county_name,
        "state": county.state,
        "risk_score": float(county.risk_score) if county.risk_score else None,
        "risk_rating": county.risk_rating,
        "hazard_scores": county.hazard_scores,
        "individual_scores": {
            "earthquake": float(county.earthquake_score) if county.earthquake_score else None,
            "flood": float(county.flood_score) if county.flood_score else None,
            "tornado": float(county.tornado_score) if county.tornado_score else None,
            "hurricane": float(county.hurricane_score) if county.hurricane_score else None,
            "wildfire": float(county.wildfire_score) if county.wildfire_score else None,
        },
        "resilience": {
            "social_vulnerability": float(county.social_vulnerability) if county.social_vulnerability else None,
            "community_resilience": float(county.community_resilience) if county.community_resilience else None,
            "expected_annual_loss": float(county.expected_annual_loss) if county.expected_annual_loss else None,
        },
    }


@router.get("/nri/by-state/{state}")
async def get_state_nri_summary(
    state: str,
    db: Session = Depends(get_db),
):
    """Get NRI summary for all counties in a state."""
    counties = db.query(NationalRiskIndex).filter(
        NationalRiskIndex.state == state.upper()
    ).order_by(NationalRiskIndex.risk_score.desc().nullslast()).all()

    if not counties:
        return {"state": state.upper(), "county_count": 0, "counties": []}

    scores = [float(c.risk_score) for c in counties if c.risk_score]

    return {
        "state": state.upper(),
        "county_count": len(counties),
        "risk_summary": {
            "avg_risk_score": sum(scores) / len(scores) if scores else None,
            "max_risk_score": max(scores) if scores else None,
            "min_risk_score": min(scores) if scores else None,
        },
        "highest_risk_counties": [
            {
                "county_name": c.county_name,
                "county_fips": c.county_fips,
                "risk_score": float(c.risk_score) if c.risk_score else None,
                "risk_rating": c.risk_rating,
            }
            for c in counties[:5]
        ],
        "lowest_risk_counties": [
            {
                "county_name": c.county_name,
                "county_fips": c.county_fips,
                "risk_score": float(c.risk_score) if c.risk_score else None,
                "risk_rating": c.risk_rating,
            }
            for c in counties[-5:] if c.risk_score
        ],
    }


# =============================================================================
# SEISMIC ENDPOINTS
# =============================================================================

@router.get("/seismic/at-location")
async def get_seismic_risk_at_location(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Get seismic hazard data for a location.

    Returns Peak Ground Acceleration and design category from USGS.
    """
    # Find nearest seismic data point
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(SeismicHazard.latitude)) *
            func.cos(func.radians(SeismicHazard.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(SeismicHazard.latitude))
        )
    ).label('distance_miles')

    result = db.query(SeismicHazard, distance_expr).filter(
        SeismicHazard.latitude.isnot(None),
    ).order_by('distance_miles').first()

    if not result:
        return {
            "location": {"latitude": lat, "longitude": lng},
            "seismic_data": None,
            "message": "No seismic data available"
        }

    hazard, distance = result

    # Determine risk level based on PGA
    pga = float(hazard.pga_2pct_50yr or 0)
    risk_level = (
        "Very High" if pga >= 0.4 else
        "High" if pga >= 0.2 else
        "Moderate" if pga >= 0.1 else
        "Low"
    )

    return {
        "location": {"latitude": lat, "longitude": lng},
        "seismic_data": {
            "pga_2pct_50yr": pga,
            "seismic_design_category": hazard.seismic_design_category,
            "site_class": hazard.site_class,
            "data_distance_miles": round(distance, 2),
        },
        "risk_level": risk_level,
        "building_code_implications": f"Seismic Design Category {hazard.seismic_design_category or 'Unknown'}"
    }


@router.get("/faults/nearby")
async def find_nearby_faults(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(50, gt=0, le=200),
    db: Session = Depends(get_db),
):
    """Find active faults within radius."""
    # Simplified - full version uses PostGIS ST_DWithin on geometry
    faults = db.query(FaultLine).limit(20).all()

    return {
        "location": {"latitude": lat, "longitude": lng},
        "radius_miles": radius_miles,
        "faults": [
            {
                "id": f.id,
                "fault_name": f.fault_name,
                "fault_type": f.fault_type,
                "slip_rate_mm_yr": float(f.slip_rate_mm_yr) if f.slip_rate_mm_yr else None,
            }
            for f in faults
        ],
        "note": "Full fault proximity calculation requires PostGIS"
    }


# =============================================================================
# CLIMATE ENDPOINTS
# =============================================================================

@router.get("/climate/at-location")
async def get_climate_at_location(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Get climate data for a location.

    Returns temperature, precipitation, and degree days from nearest station.
    """
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(ClimateData.latitude)) *
            func.cos(func.radians(ClimateData.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(ClimateData.latitude))
        )
    ).label('distance_miles')

    result = db.query(ClimateData, distance_expr).filter(
        ClimateData.latitude.isnot(None),
    ).order_by('distance_miles').first()

    if not result:
        return {
            "location": {"latitude": lat, "longitude": lng},
            "climate_data": None
        }

    climate, distance = result

    return {
        "location": {"latitude": lat, "longitude": lng},
        "station": {
            "name": climate.station_name,
            "distance_miles": round(distance, 2),
        },
        "temperature": {
            "avg_annual_f": float(climate.avg_temp_annual) if climate.avg_temp_annual else None,
            "avg_january_f": float(climate.avg_temp_jan) if climate.avg_temp_jan else None,
            "avg_july_f": float(climate.avg_temp_jul) if climate.avg_temp_jul else None,
            "days_above_90": climate.days_above_90,
            "days_below_32": climate.days_below_32,
        },
        "precipitation": {
            "annual_inches": float(climate.precip_annual_inches) if climate.precip_annual_inches else None,
            "snowfall_inches": float(climate.snowfall_annual_inches) if climate.snowfall_annual_inches else None,
        },
        "degree_days": {
            "cooling": climate.cooling_degree_days,
            "heating": climate.heating_degree_days,
        },
        "extreme_weather": {
            "tornado_risk_score": climate.tornado_risk_score,
            "hurricane_risk_score": climate.hurricane_risk_score,
        }
    }


# =============================================================================
# ENVIRONMENTAL ENDPOINTS
# =============================================================================

@router.get("/environmental/nearby")
async def find_nearby_environmental_facilities(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(5, gt=0, le=50),
    superfund_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Find EPA-regulated facilities near a location.

    Important for due diligence - identifies potential contamination sources.
    """
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(EnvironmentalFacility.latitude)) *
            func.cos(func.radians(EnvironmentalFacility.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(EnvironmentalFacility.latitude))
        )
    ).label('distance_miles')

    query = db.query(EnvironmentalFacility, distance_expr).filter(
        EnvironmentalFacility.latitude.isnot(None),
    )

    if superfund_only:
        query = query.filter(EnvironmentalFacility.is_superfund == True)

    lat_range = radius_miles / 69.0
    query = query.filter(
        EnvironmentalFacility.latitude.between(lat - lat_range, lat + lat_range),
    )

    results = query.order_by('distance_miles').limit(limit * 2).all()

    facilities = []
    for facility, distance in results:
        if distance and distance <= radius_miles:
            facilities.append({
                "id": facility.id,
                "epa_id": facility.epa_id,
                "name": facility.facility_name,
                "address": facility.address,
                "city": facility.city,
                "distance_miles": round(distance, 2),
                "is_superfund": facility.is_superfund,
                "is_brownfield": facility.is_brownfield,
                "violations_5yr": facility.violations_5yr,
            })
            if len(facilities) >= limit:
                break

    return {
        "location": {"latitude": lat, "longitude": lng},
        "radius_miles": radius_miles,
        "facilities": facilities,
        "summary": {
            "total_found": len(facilities),
            "superfund_sites": sum(1 for f in facilities if f["is_superfund"]),
            "with_violations": sum(1 for f in facilities if f["violations_5yr"] and f["violations_5yr"] > 0),
        }
    }


@router.get("/wetlands/at-location")
async def check_wetlands_at_location(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Check for wetlands at a location.

    Wetlands require permits and can significantly delay development.
    """
    # Full implementation uses PostGIS ST_Contains
    return {
        "location": {"latitude": lat, "longitude": lng},
        "wetland_status": "Unknown - requires PostGIS spatial lookup",
        "recommendation": "Use USFWS Wetlands Mapper for official determination",
        "note": "Presence of wetlands may require Section 404 permit from Army Corps of Engineers"
    }


# =============================================================================
# COMPOSITE RISK SCORE
# =============================================================================

@router.get("/score")
async def get_risk_score(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Get composite risk score for a location.

    Lower scores indicate lower risk (better for development).
    """
    # Get seismic risk
    seismic = db.query(SeismicHazard).filter(
        SeismicHazard.latitude.between(lat - 0.5, lat + 0.5),
    ).first()

    pga = float(seismic.pga_2pct_50yr or 0) if seismic else 0.1
    seismic_score = min(pga * 100, 30)  # Max 30 points

    # Get climate extremes
    climate = db.query(ClimateData).filter(
        ClimateData.latitude.between(lat - 1, lat + 1),
    ).first()

    tornado_risk = (climate.tornado_risk_score or 3) if climate else 3
    hurricane_risk = (climate.hurricane_risk_score or 2) if climate else 2
    climate_score = (tornado_risk + hurricane_risk) * 2  # Max ~20 points

    # Check for nearby environmental issues
    env_count = db.query(func.count(EnvironmentalFacility.id)).filter(
        EnvironmentalFacility.latitude.between(lat - 0.1, lat + 0.1),
        EnvironmentalFacility.is_superfund == True,
    ).scalar() or 0

    env_score = min(env_count * 15, 20)  # Max 20 points

    # Flood risk placeholder (requires PostGIS)
    flood_score = 15  # Default moderate

    total_risk = seismic_score + climate_score + env_score + flood_score

    return {
        "location": {"latitude": lat, "longitude": lng},
        "risk_score": round(min(total_risk, 100), 1),
        "risk_level": (
            "High" if total_risk >= 60 else
            "Moderate" if total_risk >= 35 else
            "Low"
        ),
        "factors": {
            "seismic": {"pga": pga, "score": round(seismic_score, 1)},
            "climate": {"tornado": tornado_risk, "hurricane": hurricane_risk, "score": round(climate_score, 1)},
            "environmental": {"superfund_nearby": env_count, "score": round(env_score, 1)},
            "flood": {"note": "Requires PostGIS", "score": flood_score},
        }
    }


@router.get("/summary")
async def get_risk_summary(db: Session = Depends(get_db)):
    """Get summary statistics for risk data."""
    return {
        "domain": "risk",
        "record_counts": {
            "nri_counties": db.query(func.count(NationalRiskIndex.id)).scalar(),
            "flood_zones": db.query(func.count(FloodZone.id)).scalar(),
            "seismic_points": db.query(func.count(SeismicHazard.id)).scalar(),
            "fault_lines": db.query(func.count(FaultLine.id)).scalar(),
            "climate_stations": db.query(func.count(ClimateData.id)).scalar(),
            "environmental_facilities": db.query(func.count(EnvironmentalFacility.id)).scalar(),
            "wetlands": db.query(func.count(Wetland.id)).scalar(),
        },
        "available_endpoints": [
            "/site-intel/risk/nri",
            "/site-intel/risk/nri/county/{county_fips}",
            "/site-intel/risk/nri/by-state/{state}",
            "/site-intel/risk/flood",
            "/site-intel/risk/flood/at-location",
            "/site-intel/risk/seismic/at-location",
            "/site-intel/risk/faults/nearby",
            "/site-intel/risk/climate/at-location",
            "/site-intel/risk/environmental/nearby",
            "/site-intel/risk/wetlands/at-location",
            "/site-intel/risk/score",
        ]
    }
