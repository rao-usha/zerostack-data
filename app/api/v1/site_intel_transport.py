"""
Site Intelligence Platform - Transportation Infrastructure API.

Endpoints for intermodal terminals, ports, airports, rail, and freight corridors.
"""
import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel

from app.core.database import get_db
from app.core.models_site_intel import (
    IntermodalTerminal, RailLine, Port, PortThroughput,
    Airport, FreightCorridor, HeavyHaulRoute,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/site-intel/transport", tags=["Site Intel - Transport"])


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class IntermodalTerminalResponse(BaseModel):
    id: int
    name: str
    railroad: Optional[str]
    city: Optional[str]
    state: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    annual_lifts: Optional[int]
    has_on_dock_rail: Optional[bool]
    distance_miles: Optional[float] = None

    class Config:
        from_attributes = True


class PortResponse(BaseModel):
    id: int
    port_code: str
    name: str
    state: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    has_container_terminal: Optional[bool]
    has_bulk_terminal: Optional[bool]
    channel_depth_ft: Optional[int]
    distance_miles: Optional[float] = None

    class Config:
        from_attributes = True


class AirportResponse(BaseModel):
    id: int
    faa_code: Optional[str]
    icao_code: Optional[str]
    name: str
    city: Optional[str]
    state: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    airport_type: Optional[str]
    has_cargo_facility: Optional[bool]
    longest_runway_ft: Optional[int]
    distance_miles: Optional[float] = None

    class Config:
        from_attributes = True


# =============================================================================
# INTERMODAL TERMINAL ENDPOINTS
# =============================================================================

@router.get("/intermodal", response_model=List[IntermodalTerminalResponse])
async def search_intermodal_terminals(
    state: Optional[str] = Query(None, description="Filter by state"),
    railroad: Optional[str] = Query(None, description="Filter by railroad (BNSF, UP, CSX, NS)"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search intermodal terminals (rail/truck transfer points)."""
    query = db.query(IntermodalTerminal)

    if state:
        query = query.filter(IntermodalTerminal.state == state.upper())
    if railroad:
        query = query.filter(IntermodalTerminal.railroad.ilike(f"%{railroad}%"))

    query = query.order_by(IntermodalTerminal.annual_lifts.desc().nullslast())
    terminals = query.limit(limit).all()

    return [IntermodalTerminalResponse.model_validate(t) for t in terminals]


@router.get("/intermodal/nearby", response_model=List[IntermodalTerminalResponse])
async def find_nearby_intermodal_terminals(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(50, gt=0, le=500),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Find intermodal terminals within radius."""
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(IntermodalTerminal.latitude)) *
            func.cos(func.radians(IntermodalTerminal.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(IntermodalTerminal.latitude))
        )
    ).label('distance_miles')

    query = db.query(IntermodalTerminal, distance_expr).filter(
        IntermodalTerminal.latitude.isnot(None),
    )

    lat_range = radius_miles / 69.0
    query = query.filter(
        IntermodalTerminal.latitude.between(lat - lat_range, lat + lat_range),
    )

    results = query.order_by('distance_miles').limit(limit * 2).all()

    terminals = []
    for term, distance in results:
        if distance and distance <= radius_miles:
            response = IntermodalTerminalResponse.model_validate(term)
            response.distance_miles = round(distance, 2)
            terminals.append(response)
            if len(terminals) >= limit:
                break

    return terminals


# =============================================================================
# RAIL ENDPOINTS
# =============================================================================

@router.get("/rail/access")
async def check_rail_access(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(10, gt=0, le=100),
    db: Session = Depends(get_db),
):
    """
    Check rail access for a location.

    Returns nearby rail lines and their characteristics.
    """
    # This is a simplified version - full implementation would use PostGIS
    lat_range = radius_miles / 69.0

    lines = db.query(RailLine).filter(
        RailLine.state.isnot(None),
    ).limit(10).all()

    # Count nearby intermodal terminals as proxy for rail access
    terminal_count = db.query(func.count(IntermodalTerminal.id)).filter(
        IntermodalTerminal.latitude.between(lat - lat_range, lat + lat_range),
        IntermodalTerminal.longitude.between(lng - lat_range*1.5, lng + lat_range*1.5),
    ).scalar() or 0

    return {
        "location": {"latitude": lat, "longitude": lng},
        "radius_miles": radius_miles,
        "has_rail_access": terminal_count > 0,
        "nearby_terminals": terminal_count,
        "assessment": (
            "Excellent" if terminal_count >= 3 else
            "Good" if terminal_count >= 1 else
            "Limited - no intermodal terminals within radius"
        )
    }


# =============================================================================
# PORT ENDPOINTS
# =============================================================================

@router.get("/ports", response_model=List[PortResponse])
async def search_ports(
    state: Optional[str] = Query(None, description="Filter by state"),
    port_type: Optional[str] = Query(None, description="Type: seaport, river, great_lakes"),
    has_container: Optional[bool] = Query(None, description="Has container terminal"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search ports."""
    query = db.query(Port)

    if state:
        query = query.filter(Port.state == state.upper())
    if port_type:
        query = query.filter(Port.port_type == port_type)
    if has_container is not None:
        query = query.filter(Port.has_container_terminal == has_container)

    ports = query.limit(limit).all()
    return [PortResponse.model_validate(p) for p in ports]


@router.get("/ports/nearby", response_model=List[PortResponse])
async def find_nearby_ports(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(100, gt=0, le=500),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Find ports within radius."""
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(Port.latitude)) *
            func.cos(func.radians(Port.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(Port.latitude))
        )
    ).label('distance_miles')

    query = db.query(Port, distance_expr).filter(Port.latitude.isnot(None))

    lat_range = radius_miles / 69.0
    query = query.filter(Port.latitude.between(lat - lat_range, lat + lat_range))

    results = query.order_by('distance_miles').limit(limit * 2).all()

    ports = []
    for port, distance in results:
        if distance and distance <= radius_miles:
            response = PortResponse.model_validate(port)
            response.distance_miles = round(distance, 2)
            ports.append(response)
            if len(ports) >= limit:
                break

    return ports


@router.get("/ports/{port_code}/throughput")
async def get_port_throughput(
    port_code: str,
    start_year: Optional[int] = Query(None),
    end_year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """Get port throughput history."""
    port = db.query(Port).filter(Port.port_code == port_code.upper()).first()
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")

    query = db.query(PortThroughput).filter(PortThroughput.port_id == port.id)

    if start_year:
        query = query.filter(PortThroughput.period_year >= start_year)
    if end_year:
        query = query.filter(PortThroughput.period_year <= end_year)

    throughput = query.order_by(
        PortThroughput.period_year.desc(),
        PortThroughput.period_month.desc().nullslast()
    ).limit(60).all()

    return {
        "port": {"code": port.port_code, "name": port.name},
        "throughput": [
            {
                "year": t.period_year,
                "month": t.period_month,
                "teu_total": t.teu_total,
                "teu_import": t.teu_import,
                "teu_export": t.teu_export,
                "tonnage_total_thousand": float(t.tonnage_total_thousand) if t.tonnage_total_thousand else None,
            }
            for t in throughput
        ]
    }


# =============================================================================
# AIRPORT ENDPOINTS
# =============================================================================

@router.get("/airports", response_model=List[AirportResponse])
async def search_airports(
    state: Optional[str] = Query(None, description="Filter by state"),
    has_cargo: Optional[bool] = Query(None, description="Has cargo facility"),
    airport_type: Optional[str] = Query(None, description="Type: large_hub, medium_hub, small_hub, cargo"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search airports."""
    query = db.query(Airport)

    if state:
        query = query.filter(Airport.state == state.upper())
    if has_cargo is not None:
        query = query.filter(Airport.has_cargo_facility == has_cargo)
    if airport_type:
        query = query.filter(Airport.airport_type == airport_type)

    query = query.order_by(Airport.cargo_tonnage_annual.desc().nullslast())
    airports = query.limit(limit).all()

    return [AirportResponse.model_validate(a) for a in airports]


@router.get("/airports/nearby", response_model=List[AirportResponse])
async def find_nearby_airports(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(50, gt=0, le=500),
    has_cargo: Optional[bool] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Find airports within radius."""
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(Airport.latitude)) *
            func.cos(func.radians(Airport.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(Airport.latitude))
        )
    ).label('distance_miles')

    query = db.query(Airport, distance_expr).filter(Airport.latitude.isnot(None))

    if has_cargo is not None:
        query = query.filter(Airport.has_cargo_facility == has_cargo)

    lat_range = radius_miles / 69.0
    query = query.filter(Airport.latitude.between(lat - lat_range, lat + lat_range))

    results = query.order_by('distance_miles').limit(limit * 2).all()

    airports = []
    for airport, distance in results:
        if distance and distance <= radius_miles:
            response = AirportResponse.model_validate(airport)
            response.distance_miles = round(distance, 2)
            airports.append(response)
            if len(airports) >= limit:
                break

    return airports


# =============================================================================
# HEAVY HAUL ENDPOINTS
# =============================================================================

@router.get("/heavy-haul")
async def search_heavy_haul_routes(
    state: Optional[str] = Query(None, description="Filter by state"),
    min_weight_lbs: Optional[int] = Query(None, description="Minimum weight capacity"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Search heavy haul routes for oversize loads.

    Critical for data center construction (transformers can be 500+ tons).
    """
    query = db.query(HeavyHaulRoute)

    if state:
        query = query.filter(HeavyHaulRoute.state == state.upper())
    if min_weight_lbs:
        query = query.filter(HeavyHaulRoute.max_weight_lbs >= min_weight_lbs)

    routes = query.order_by(HeavyHaulRoute.max_weight_lbs.desc().nullslast()).limit(limit).all()

    return [
        {
            "id": r.id,
            "route_name": r.route_name,
            "state": r.state,
            "max_weight_lbs": r.max_weight_lbs,
            "max_height_ft": float(r.max_height_ft) if r.max_height_ft else None,
            "max_width_ft": float(r.max_width_ft) if r.max_width_ft else None,
            "permit_required": r.permit_required,
            "restrictions": r.restrictions,
        }
        for r in routes
    ]


# =============================================================================
# SUMMARY ENDPOINT
# =============================================================================

@router.get("/summary")
async def get_transport_summary(db: Session = Depends(get_db)):
    """Get summary statistics for transportation infrastructure data."""
    return {
        "domain": "transport",
        "record_counts": {
            "intermodal_terminals": db.query(func.count(IntermodalTerminal.id)).scalar(),
            "rail_lines": db.query(func.count(RailLine.id)).scalar(),
            "ports": db.query(func.count(Port.id)).scalar(),
            "airports": db.query(func.count(Airport.id)).scalar(),
            "freight_corridors": db.query(func.count(FreightCorridor.id)).scalar(),
            "heavy_haul_routes": db.query(func.count(HeavyHaulRoute.id)).scalar(),
        },
        "available_endpoints": [
            "/site-intel/transport/intermodal",
            "/site-intel/transport/intermodal/nearby",
            "/site-intel/transport/rail/access",
            "/site-intel/transport/ports",
            "/site-intel/transport/ports/nearby",
            "/site-intel/transport/ports/{code}/throughput",
            "/site-intel/transport/airports",
            "/site-intel/transport/airports/nearby",
            "/site-intel/transport/heavy-haul",
        ]
    }
