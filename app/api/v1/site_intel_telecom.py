"""
Site Intelligence Platform - Telecom/Fiber Infrastructure API.

Endpoints for broadband availability, internet exchanges, and data centers.
"""
import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel

from app.core.database import get_db
from app.core.models_site_intel import (
    BroadbandAvailability, InternetExchange, DataCenterFacility,
    SubmarineCableLanding, NetworkLatency,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/site-intel/telecom", tags=["Site Intel - Telecom"])


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class InternetExchangeResponse(BaseModel):
    id: int
    peeringdb_id: Optional[int]
    name: str
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    network_count: Optional[int]
    speed_gbps: Optional[int]
    distance_miles: Optional[float] = None

    class Config:
        from_attributes = True


class DataCenterResponse(BaseModel):
    id: int
    peeringdb_id: Optional[int]
    name: str
    operator: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    network_count: Optional[int]
    ix_count: Optional[int]
    power_mw: Optional[float]
    tier_certification: Optional[str]
    distance_miles: Optional[float] = None

    class Config:
        from_attributes = True


# =============================================================================
# BROADBAND ENDPOINTS
# =============================================================================

@router.get("/broadband")
async def search_broadband_availability(
    state: Optional[str] = Query(None, description="Filter by state"),
    technology: Optional[str] = Query(None, description="Technology: fiber, cable, fixed_wireless, dsl"),
    min_download_mbps: Optional[int] = Query(None, description="Minimum download speed"),
    is_business: Optional[bool] = Query(None, description="Filter business services only"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Search broadband availability records.

    Data center sites require high-speed fiber connectivity (1Gbps+).
    """
    query = db.query(BroadbandAvailability)

    if state:
        query = query.filter(BroadbandAvailability.state == state.upper())
    if technology:
        query = query.filter(BroadbandAvailability.technology == technology)
    if min_download_mbps:
        query = query.filter(BroadbandAvailability.max_download_mbps >= min_download_mbps)
    if is_business is not None:
        query = query.filter(BroadbandAvailability.is_business_service == is_business)

    query = query.order_by(BroadbandAvailability.max_download_mbps.desc().nullslast())
    records = query.limit(limit).all()

    return [
        {
            "id": r.id,
            "state": r.state,
            "county": r.county,
            "provider": r.provider_name,
            "technology": r.technology,
            "download_mbps": r.max_download_mbps,
            "upload_mbps": r.max_upload_mbps,
            "is_business": r.is_business_service,
        }
        for r in records
    ]


@router.get("/broadband/at-location")
async def get_broadband_at_location(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Get broadband providers serving a location.

    Returns available ISPs and their service capabilities.
    """
    # Find nearby broadband records
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(BroadbandAvailability.latitude)) *
            func.cos(func.radians(BroadbandAvailability.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(BroadbandAvailability.latitude))
        )
    ).label('distance_miles')

    query = db.query(BroadbandAvailability, distance_expr).filter(
        BroadbandAvailability.latitude.isnot(None),
    )

    # Small radius for location-specific lookup
    lat_range = 0.1  # ~7 miles
    query = query.filter(
        BroadbandAvailability.latitude.between(lat - lat_range, lat + lat_range),
        BroadbandAvailability.longitude.between(lng - lat_range, lng + lat_range),
    )

    results = query.order_by('distance_miles').limit(20).all()

    providers = {}
    for record, distance in results:
        if distance > 5:  # Only within 5 miles
            continue
        provider = record.provider_name
        if provider not in providers or record.max_download_mbps > providers[provider]["download_mbps"]:
            providers[provider] = {
                "provider": provider,
                "technology": record.technology,
                "download_mbps": record.max_download_mbps,
                "upload_mbps": record.max_upload_mbps,
                "is_business": record.is_business_service,
            }

    return {
        "location": {"latitude": lat, "longitude": lng},
        "providers": list(providers.values()),
        "has_fiber": any(p["technology"] == "fiber" for p in providers.values()),
        "max_download_mbps": max((p["download_mbps"] or 0) for p in providers.values()) if providers else 0,
    }


# =============================================================================
# INTERNET EXCHANGE ENDPOINTS
# =============================================================================

@router.get("/ix", response_model=List[InternetExchangeResponse])
async def search_internet_exchanges(
    country: Optional[str] = Query("US", description="Country code (default: US)"),
    state: Optional[str] = Query(None, description="Filter by state"),
    min_networks: Optional[int] = Query(None, description="Minimum connected networks"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Search Internet Exchange Points.

    IX proximity is critical for data centers - low latency peering.
    """
    query = db.query(InternetExchange)

    if country:
        query = query.filter(InternetExchange.country == country.upper())
    if state:
        query = query.filter(InternetExchange.state == state)
    if min_networks:
        query = query.filter(InternetExchange.network_count >= min_networks)

    query = query.order_by(InternetExchange.network_count.desc().nullslast())
    exchanges = query.limit(limit).all()

    return [InternetExchangeResponse.model_validate(ix) for ix in exchanges]


@router.get("/ix/nearby", response_model=List[InternetExchangeResponse])
async def find_nearby_internet_exchanges(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(100, gt=0, le=500),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Find Internet Exchanges within radius.

    Data centers within 50 miles of major IX points have latency advantages.
    """
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(InternetExchange.latitude)) *
            func.cos(func.radians(InternetExchange.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(InternetExchange.latitude))
        )
    ).label('distance_miles')

    query = db.query(InternetExchange, distance_expr).filter(
        InternetExchange.latitude.isnot(None),
    )

    lat_range = radius_miles / 69.0
    query = query.filter(
        InternetExchange.latitude.between(lat - lat_range, lat + lat_range),
    )

    results = query.order_by('distance_miles').limit(limit * 2).all()

    exchanges = []
    for ix, distance in results:
        if distance and distance <= radius_miles:
            response = InternetExchangeResponse.model_validate(ix)
            response.distance_miles = round(distance, 2)
            exchanges.append(response)
            if len(exchanges) >= limit:
                break

    return exchanges


# =============================================================================
# DATA CENTER ENDPOINTS
# =============================================================================

@router.get("/data-centers", response_model=List[DataCenterResponse])
async def search_data_centers(
    country: Optional[str] = Query("US", description="Country code"),
    state: Optional[str] = Query(None, description="Filter by state"),
    operator: Optional[str] = Query(None, description="Filter by operator name"),
    min_networks: Optional[int] = Query(None, description="Minimum connected networks"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Search data center facilities.

    Facility data from PeeringDB includes network presence and IX connectivity.
    """
    query = db.query(DataCenterFacility)

    if country:
        query = query.filter(DataCenterFacility.country == country.upper())
    if state:
        query = query.filter(DataCenterFacility.state == state)
    if operator:
        query = query.filter(DataCenterFacility.operator.ilike(f"%{operator}%"))
    if min_networks:
        query = query.filter(DataCenterFacility.network_count >= min_networks)

    query = query.order_by(DataCenterFacility.network_count.desc().nullslast())
    facilities = query.limit(limit).all()

    return [DataCenterResponse.model_validate(dc) for dc in facilities]


@router.get("/data-centers/nearby", response_model=List[DataCenterResponse])
async def find_nearby_data_centers(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(50, gt=0, le=500),
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Find data centers within radius.

    Proximity to existing data centers indicates good infrastructure.
    """
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(DataCenterFacility.latitude)) *
            func.cos(func.radians(DataCenterFacility.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(DataCenterFacility.latitude))
        )
    ).label('distance_miles')

    query = db.query(DataCenterFacility, distance_expr).filter(
        DataCenterFacility.latitude.isnot(None),
    )

    lat_range = radius_miles / 69.0
    query = query.filter(
        DataCenterFacility.latitude.between(lat - lat_range, lat + lat_range),
    )

    results = query.order_by('distance_miles').limit(limit * 2).all()

    facilities = []
    for dc, distance in results:
        if distance and distance <= radius_miles:
            response = DataCenterResponse.model_validate(dc)
            response.distance_miles = round(distance, 2)
            facilities.append(response)
            if len(facilities) >= limit:
                break

    return facilities


# =============================================================================
# SUBMARINE CABLE ENDPOINTS
# =============================================================================

@router.get("/submarine-cables")
async def list_submarine_cable_landings(
    country: Optional[str] = Query("US", description="Country code"),
    state: Optional[str] = Query(None, description="Filter by state"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    List submarine cable landing points.

    Cable landings are critical for international connectivity.
    """
    query = db.query(SubmarineCableLanding)

    if country:
        query = query.filter(SubmarineCableLanding.country == country.upper())
    if state:
        query = query.filter(SubmarineCableLanding.state == state)

    cables = query.order_by(SubmarineCableLanding.capacity_tbps.desc().nullslast()).limit(limit).all()

    return [
        {
            "id": c.id,
            "cable_name": c.cable_name,
            "landing_point": c.landing_point_name,
            "city": c.city,
            "state": c.state,
            "country": c.country,
            "latitude": float(c.latitude) if c.latitude else None,
            "longitude": float(c.longitude) if c.longitude else None,
            "capacity_tbps": float(c.capacity_tbps) if c.capacity_tbps else None,
            "rfs_date": c.rfs_date.isoformat() if c.rfs_date else None,
        }
        for c in cables
    ]


# =============================================================================
# CONNECTIVITY SCORE ENDPOINT
# =============================================================================

@router.get("/connectivity-score")
async def get_connectivity_score(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Get composite connectivity score for a location.

    Factors in IX proximity, data center density, and fiber availability.
    """
    # Count nearby IXs (within 100 miles)
    ix_count = db.query(func.count(InternetExchange.id)).filter(
        InternetExchange.latitude.between(lat - 1.5, lat + 1.5),
        InternetExchange.longitude.between(lng - 1.5, lng + 1.5),
    ).scalar() or 0

    # Count nearby data centers (within 50 miles)
    dc_count = db.query(func.count(DataCenterFacility.id)).filter(
        DataCenterFacility.latitude.between(lat - 0.75, lat + 0.75),
        DataCenterFacility.longitude.between(lng - 0.75, lng + 0.75),
    ).scalar() or 0

    # Simple scoring algorithm
    ix_score = min(ix_count * 15, 40)  # Max 40 points
    dc_score = min(dc_count * 3, 40)   # Max 40 points
    # Fiber availability would add up to 20 points

    total_score = ix_score + dc_score

    return {
        "location": {"latitude": lat, "longitude": lng},
        "connectivity_score": min(total_score, 100),
        "factors": {
            "ix_proximity": {"count_within_100mi": ix_count, "score": ix_score},
            "dc_density": {"count_within_50mi": dc_count, "score": dc_score},
        },
        "assessment": (
            "Excellent" if total_score >= 70 else
            "Good" if total_score >= 50 else
            "Moderate" if total_score >= 30 else
            "Limited"
        )
    }


# =============================================================================
# SUMMARY ENDPOINT
# =============================================================================

@router.get("/summary")
async def get_telecom_summary(db: Session = Depends(get_db)):
    """Get summary statistics for telecom infrastructure data."""
    return {
        "domain": "telecom",
        "record_counts": {
            "broadband_records": db.query(func.count(BroadbandAvailability.id)).scalar(),
            "internet_exchanges": db.query(func.count(InternetExchange.id)).scalar(),
            "data_centers": db.query(func.count(DataCenterFacility.id)).scalar(),
            "submarine_cables": db.query(func.count(SubmarineCableLanding.id)).scalar(),
        },
        "available_endpoints": [
            "/site-intel/telecom/broadband",
            "/site-intel/telecom/broadband/at-location",
            "/site-intel/telecom/ix",
            "/site-intel/telecom/ix/nearby",
            "/site-intel/telecom/data-centers",
            "/site-intel/telecom/data-centers/nearby",
            "/site-intel/telecom/submarine-cables",
            "/site-intel/telecom/connectivity-score",
        ]
    }
