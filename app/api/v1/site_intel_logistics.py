"""
Site Intelligence Platform - Freight & Logistics API.

Endpoints for freight rates, trucking lanes, and warehouse facilities.
"""
import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel

from app.core.database import get_db
from app.core.models_site_intel import (
    FreightRateIndex, TruckingLaneRate, WarehouseFacility,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/site-intel/logistics", tags=["Site Intel - Logistics"])


# =============================================================================
# FREIGHT RATE ENDPOINTS
# =============================================================================

@router.get("/freight-rates")
async def search_freight_rates(
    mode: Optional[str] = Query(None, description="Mode: ocean, trucking, rail, air"),
    index_code: Optional[str] = Query(None),
    route_origin: Optional[str] = Query(None),
    route_destination: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search freight rate indices."""
    query = db.query(FreightRateIndex)

    if mode:
        query = query.filter(FreightRateIndex.mode == mode)
    if index_code:
        query = query.filter(FreightRateIndex.index_code == index_code)
    if route_origin:
        query = query.filter(FreightRateIndex.route_origin.ilike(f"%{route_origin}%"))
    if route_destination:
        query = query.filter(FreightRateIndex.route_destination.ilike(f"%{route_destination}%"))

    query = query.order_by(FreightRateIndex.rate_date.desc())
    rates = query.limit(limit).all()

    return [
        {
            "id": r.id,
            "index_name": r.index_name,
            "index_code": r.index_code,
            "mode": r.mode,
            "route_origin": r.route_origin,
            "route_destination": r.route_destination,
            "rate_date": r.rate_date.isoformat() if r.rate_date else None,
            "rate_value": float(r.rate_value) if r.rate_value else None,
            "rate_unit": r.rate_unit,
            "change_pct_wow": float(r.change_pct_wow) if r.change_pct_wow else None,
            "change_pct_yoy": float(r.change_pct_yoy) if r.change_pct_yoy else None,
        }
        for r in rates
    ]


@router.get("/freight-rates/latest")
async def get_latest_freight_rates(
    mode: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Get latest freight rates by route."""
    query = db.query(FreightRateIndex)

    if mode:
        query = query.filter(FreightRateIndex.mode == mode)

    # Get distinct routes with latest rate
    rates = query.order_by(
        FreightRateIndex.route_origin,
        FreightRateIndex.route_destination,
        FreightRateIndex.rate_date.desc()
    ).limit(100).all()

    # Deduplicate by route
    seen_routes = set()
    latest = []
    for r in rates:
        route_key = f"{r.route_origin}-{r.route_destination}"
        if route_key not in seen_routes:
            seen_routes.add(route_key)
            latest.append({
                "index_name": r.index_name,
                "mode": r.mode,
                "route": f"{r.route_origin} → {r.route_destination}",
                "rate_date": r.rate_date.isoformat() if r.rate_date else None,
                "rate_value": float(r.rate_value) if r.rate_value else None,
                "rate_unit": r.rate_unit,
                "change_pct_yoy": float(r.change_pct_yoy) if r.change_pct_yoy else None,
            })

    return latest


# =============================================================================
# TRUCKING RATE ENDPOINTS
# =============================================================================

@router.get("/trucking-rates")
async def search_trucking_rates(
    origin: Optional[str] = Query(None, description="Origin market"),
    destination: Optional[str] = Query(None, description="Destination market"),
    equipment_type: Optional[str] = Query(None, description="Type: van, reefer, flatbed"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search trucking lane rates."""
    query = db.query(TruckingLaneRate)

    if origin:
        query = query.filter(TruckingLaneRate.origin_market.ilike(f"%{origin}%"))
    if destination:
        query = query.filter(TruckingLaneRate.destination_market.ilike(f"%{destination}%"))
    if equipment_type:
        query = query.filter(TruckingLaneRate.equipment_type == equipment_type)

    query = query.order_by(TruckingLaneRate.rate_date.desc())
    rates = query.limit(limit).all()

    return [
        {
            "id": r.id,
            "origin_market": r.origin_market,
            "origin_state": r.origin_state,
            "destination_market": r.destination_market,
            "destination_state": r.destination_state,
            "equipment_type": r.equipment_type,
            "rate_date": r.rate_date.isoformat() if r.rate_date else None,
            "rate_per_mile": float(r.rate_per_mile) if r.rate_per_mile else None,
            "total_rate_per_mile": float(r.total_rate_per_mile) if r.total_rate_per_mile else None,
            "load_count": r.load_count,
        }
        for r in rates
    ]


@router.get("/trucking-rates/lane")
async def get_lane_rate_history(
    origin: str = Query(..., description="Origin market"),
    destination: str = Query(..., description="Destination market"),
    equipment_type: str = Query("van", description="Equipment type"),
    db: Session = Depends(get_db),
):
    """Get rate history for a specific lane."""
    rates = db.query(TruckingLaneRate).filter(
        TruckingLaneRate.origin_market.ilike(f"%{origin}%"),
        TruckingLaneRate.destination_market.ilike(f"%{destination}%"),
        TruckingLaneRate.equipment_type == equipment_type,
    ).order_by(TruckingLaneRate.rate_date.desc()).limit(52).all()

    return {
        "lane": {
            "origin": origin,
            "destination": destination,
            "equipment_type": equipment_type,
        },
        "history": [
            {
                "date": r.rate_date.isoformat() if r.rate_date else None,
                "rate_per_mile": float(r.total_rate_per_mile) if r.total_rate_per_mile else None,
                "load_count": r.load_count,
            }
            for r in rates
        ]
    }


# =============================================================================
# WAREHOUSE ENDPOINTS
# =============================================================================

@router.get("/warehouses")
async def search_warehouses(
    state: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    facility_type: Optional[str] = Query(None, description="Type: distribution, fulfillment, cold_storage, cross_dock"),
    min_sqft: Optional[int] = Query(None),
    has_cold: Optional[bool] = Query(None, description="Has cold storage"),
    has_rail: Optional[bool] = Query(None, description="Has rail access"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Search warehouse/3PL facilities."""
    query = db.query(WarehouseFacility)

    if state:
        query = query.filter(WarehouseFacility.state == state.upper())
    if city:
        query = query.filter(WarehouseFacility.city.ilike(f"%{city}%"))
    if facility_type:
        query = query.filter(WarehouseFacility.facility_type == facility_type)
    if min_sqft:
        query = query.filter(WarehouseFacility.sqft_total >= min_sqft)
    if has_cold is not None:
        query = query.filter(WarehouseFacility.has_cold_storage == has_cold)
    if has_rail is not None:
        query = query.filter(WarehouseFacility.has_rail == has_rail)

    query = query.order_by(WarehouseFacility.sqft_total.desc().nullslast())
    warehouses = query.limit(limit).all()

    return [
        {
            "id": w.id,
            "facility_name": w.facility_name,
            "operator": w.operator_name,
            "facility_type": w.facility_type,
            "city": w.city,
            "state": w.state,
            "sqft_total": w.sqft_total,
            "sqft_available": w.sqft_available,
            "clear_height_ft": w.clear_height_ft,
            "dock_doors": w.dock_doors,
            "has_cold_storage": w.has_cold_storage,
            "has_rail": w.has_rail,
            "asking_rent_psf": float(w.asking_rent_psf) if w.asking_rent_psf else None,
        }
        for w in warehouses
    ]


@router.get("/warehouses/nearby")
async def find_nearby_warehouses(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(25, gt=0, le=100),
    facility_type: Optional[str] = Query(None),
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Find warehouses within radius."""
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(WarehouseFacility.latitude)) *
            func.cos(func.radians(WarehouseFacility.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(WarehouseFacility.latitude))
        )
    ).label('distance_miles')

    query = db.query(WarehouseFacility, distance_expr).filter(
        WarehouseFacility.latitude.isnot(None),
    )

    if facility_type:
        query = query.filter(WarehouseFacility.facility_type == facility_type)

    lat_range = radius_miles / 69.0
    query = query.filter(
        WarehouseFacility.latitude.between(lat - lat_range, lat + lat_range),
    )

    results = query.order_by('distance_miles').limit(limit * 2).all()

    warehouses = []
    for wh, distance in results:
        if distance and distance <= radius_miles:
            warehouses.append({
                "id": wh.id,
                "facility_name": wh.facility_name,
                "operator": wh.operator_name,
                "facility_type": wh.facility_type,
                "city": wh.city,
                "state": wh.state,
                "sqft_total": wh.sqft_total,
                "distance_miles": round(distance, 2),
            })
            if len(warehouses) >= limit:
                break

    return warehouses


@router.get("/summary")
async def get_logistics_summary(db: Session = Depends(get_db)):
    """Get summary statistics for logistics data."""
    return {
        "domain": "logistics",
        "record_counts": {
            "freight_rate_indices": db.query(func.count(FreightRateIndex.id)).scalar(),
            "trucking_lane_rates": db.query(func.count(TruckingLaneRate.id)).scalar(),
            "warehouses": db.query(func.count(WarehouseFacility.id)).scalar(),
        },
        "available_endpoints": [
            "/site-intel/logistics/freight-rates",
            "/site-intel/logistics/freight-rates/latest",
            "/site-intel/logistics/trucking-rates",
            "/site-intel/logistics/trucking-rates/lane",
            "/site-intel/logistics/warehouses",
            "/site-intel/logistics/warehouses/nearby",
        ]
    }
