"""
Site Intelligence Platform - Power Infrastructure API.

Endpoints for power plants, substations, utilities, and grid data.
"""
import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models_site_intel import (
    PowerPlant, Substation, UtilityTerritory,
    InterconnectionQueue, ElectricityPrice, RenewableResource,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/site-intel/power", tags=["Site Intel - Power"])


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class PowerPlantResponse(BaseModel):
    id: int
    eia_plant_id: Optional[str]
    name: str
    state: Optional[str]
    county: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    primary_fuel: Optional[str]
    nameplate_capacity_mw: Optional[float]
    grid_region: Optional[str]
    distance_miles: Optional[float] = None

    class Config:
        from_attributes = True


class SubstationResponse(BaseModel):
    id: int
    hifld_id: Optional[str]
    name: Optional[str]
    state: Optional[str]
    city: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    max_voltage_kv: Optional[float]
    owner: Optional[str]
    distance_miles: Optional[float] = None

    class Config:
        from_attributes = True


class UtilityResponse(BaseModel):
    id: int
    eia_utility_id: Optional[int]
    utility_name: str
    utility_type: Optional[str]
    state: Optional[str]
    avg_rate_residential: Optional[float]
    avg_rate_commercial: Optional[float]
    avg_rate_industrial: Optional[float]
    customers_industrial: Optional[int]

    class Config:
        from_attributes = True


class InterconnectionQueueResponse(BaseModel):
    id: int
    iso_region: str
    queue_id: Optional[str]
    project_name: Optional[str]
    fuel_type: Optional[str]
    capacity_mw: Optional[float]
    state: Optional[str]
    status: Optional[str]
    queue_date: Optional[str]
    target_cod: Optional[str]

    class Config:
        from_attributes = True


# =============================================================================
# POWER PLANT ENDPOINTS
# =============================================================================

@router.get("/plants", response_model=List[PowerPlantResponse])
async def search_power_plants(
    state: Optional[str] = Query(None, description="Filter by state code (e.g., 'TX')"),
    fuel: Optional[str] = Query(None, description="Filter by primary fuel (natural_gas, coal, solar, wind, nuclear)"),
    min_capacity_mw: Optional[float] = Query(None, description="Minimum nameplate capacity in MW"),
    grid_region: Optional[str] = Query(None, description="Filter by grid region (PJM, ERCOT, CAISO, etc.)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Search power plants with filters.

    Returns power plants matching the specified criteria, ordered by capacity.
    """
    query = db.query(PowerPlant)

    if state:
        query = query.filter(PowerPlant.state == state.upper())
    if fuel:
        query = query.filter(PowerPlant.primary_fuel == fuel)
    if min_capacity_mw:
        query = query.filter(PowerPlant.nameplate_capacity_mw >= min_capacity_mw)
    if grid_region:
        query = query.filter(PowerPlant.grid_region == grid_region)

    query = query.order_by(PowerPlant.nameplate_capacity_mw.desc().nullslast())
    plants = query.offset(offset).limit(limit).all()

    return [PowerPlantResponse.model_validate(p) for p in plants]


@router.get("/plants/nearby", response_model=List[PowerPlantResponse])
async def find_nearby_power_plants(
    lat: float = Query(..., description="Latitude", ge=-90, le=90),
    lng: float = Query(..., description="Longitude", ge=-180, le=180),
    radius_miles: float = Query(25, description="Search radius in miles", gt=0, le=500),
    fuel: Optional[str] = Query(None, description="Filter by primary fuel"),
    min_capacity_mw: Optional[float] = Query(None, description="Minimum capacity in MW"),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Find power plants within a radius of coordinates.

    Uses Haversine formula to calculate distances. Results ordered by distance.
    """
    # Haversine distance calculation in SQL (approximate for performance)
    # 3959 = Earth's radius in miles
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(PowerPlant.latitude)) *
            func.cos(func.radians(PowerPlant.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(PowerPlant.latitude))
        )
    ).label('distance_miles')

    query = db.query(PowerPlant, distance_expr).filter(
        PowerPlant.latitude.isnot(None),
        PowerPlant.longitude.isnot(None),
    )

    if fuel:
        query = query.filter(PowerPlant.primary_fuel == fuel)
    if min_capacity_mw:
        query = query.filter(PowerPlant.nameplate_capacity_mw >= min_capacity_mw)

    # Filter by approximate bounding box first for performance
    lat_range = radius_miles / 69.0  # ~69 miles per degree latitude
    lng_range = radius_miles / (69.0 * func.cos(func.radians(lat)))

    query = query.filter(
        PowerPlant.latitude.between(lat - lat_range, lat + lat_range),
        PowerPlant.longitude.between(lng - float(radius_miles/50), lng + float(radius_miles/50)),
    )

    results = query.order_by('distance_miles').limit(limit * 2).all()

    # Filter by exact distance and limit
    plants = []
    for plant, distance in results:
        if distance and distance <= radius_miles:
            response = PowerPlantResponse.model_validate(plant)
            response.distance_miles = round(distance, 2)
            plants.append(response)
            if len(plants) >= limit:
                break

    return plants


@router.get("/plants/{plant_id}", response_model=PowerPlantResponse)
async def get_power_plant(
    plant_id: int,
    db: Session = Depends(get_db),
):
    """Get details for a specific power plant."""
    plant = db.query(PowerPlant).filter(PowerPlant.id == plant_id).first()
    if not plant:
        raise HTTPException(status_code=404, detail="Power plant not found")
    return PowerPlantResponse.model_validate(plant)


# =============================================================================
# SUBSTATION ENDPOINTS
# =============================================================================

@router.get("/substations", response_model=List[SubstationResponse])
async def search_substations(
    state: Optional[str] = Query(None, description="Filter by state code"),
    min_voltage_kv: Optional[float] = Query(None, description="Minimum voltage in kV"),
    substation_type: Optional[str] = Query(None, description="Type: transmission or distribution"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Search electrical substations with filters.

    High voltage substations (115kV+) are most relevant for industrial/data center sites.
    """
    query = db.query(Substation)

    if state:
        query = query.filter(Substation.state == state.upper())
    if min_voltage_kv:
        query = query.filter(Substation.max_voltage_kv >= min_voltage_kv)
    if substation_type:
        query = query.filter(Substation.substation_type == substation_type)

    query = query.order_by(Substation.max_voltage_kv.desc().nullslast())
    substations = query.offset(offset).limit(limit).all()

    return [SubstationResponse.model_validate(s) for s in substations]


@router.get("/substations/nearby", response_model=List[SubstationResponse])
async def find_nearby_substations(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_miles: float = Query(25, gt=0, le=500),
    min_voltage_kv: float = Query(None, description="Minimum voltage in kV (115+ for large loads)"),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Find substations within a radius of coordinates.

    For data centers, look for 115kV+ substations within 10-25 miles.
    """
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(Substation.latitude)) *
            func.cos(func.radians(Substation.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(Substation.latitude))
        )
    ).label('distance_miles')

    query = db.query(Substation, distance_expr).filter(
        Substation.latitude.isnot(None),
        Substation.longitude.isnot(None),
    )

    if min_voltage_kv:
        query = query.filter(Substation.max_voltage_kv >= min_voltage_kv)

    # Bounding box filter
    lat_range = radius_miles / 69.0
    query = query.filter(
        Substation.latitude.between(lat - lat_range, lat + lat_range),
        Substation.longitude.between(lng - radius_miles/50, lng + radius_miles/50),
    )

    results = query.order_by('distance_miles').limit(limit * 2).all()

    substations = []
    for sub, distance in results:
        if distance and distance <= radius_miles:
            response = SubstationResponse.model_validate(sub)
            response.distance_miles = round(distance, 2)
            substations.append(response)
            if len(substations) >= limit:
                break

    return substations


# =============================================================================
# UTILITY ENDPOINTS
# =============================================================================

@router.get("/utilities", response_model=List[UtilityResponse])
async def search_utilities(
    state: Optional[str] = Query(None, description="Filter by state"),
    utility_type: Optional[str] = Query(None, description="Type: investor_owned, municipal, coop"),
    max_industrial_rate: Optional[float] = Query(None, description="Maximum industrial rate ($/kWh)"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Search utility service territories.

    Industrial electricity rates are key for data center site selection.
    """
    query = db.query(UtilityTerritory)

    if state:
        query = query.filter(UtilityTerritory.state == state.upper())
    if utility_type:
        query = query.filter(UtilityTerritory.utility_type == utility_type)
    if max_industrial_rate:
        query = query.filter(UtilityTerritory.avg_rate_industrial <= max_industrial_rate)

    query = query.order_by(UtilityTerritory.avg_rate_industrial.asc().nullslast())
    utilities = query.limit(limit).all()

    return [UtilityResponse.model_validate(u) for u in utilities]


@router.get("/utilities/at-location")
async def get_utility_at_location(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Get the utility serving a specific location.

    Note: Requires PostGIS for full spatial lookup. Falls back to nearest utility.
    """
    # TODO: Implement proper spatial lookup with PostGIS
    # For now, return utilities in the same state as a placeholder
    return {
        "message": "Spatial lookup requires PostGIS. Use /utilities endpoint with state filter.",
        "location": {"latitude": lat, "longitude": lng}
    }


# =============================================================================
# ELECTRICITY PRICE ENDPOINTS
# =============================================================================

@router.get("/prices")
async def get_electricity_prices(
    state: Optional[str] = Query(None, description="Filter by state"),
    sector: str = Query("industrial", description="Sector: residential, commercial, industrial"),
    year: Optional[int] = Query(None, description="Filter by year"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Query electricity prices by geography.

    Returns average prices in cents per kWh.
    """
    query = db.query(ElectricityPrice).filter(
        ElectricityPrice.sector == sector
    )

    if state:
        query = query.filter(ElectricityPrice.geography_id == state.upper())
    if year:
        query = query.filter(ElectricityPrice.period_year == year)

    query = query.order_by(
        ElectricityPrice.period_year.desc(),
        ElectricityPrice.avg_price_cents_kwh.asc()
    )
    prices = query.limit(limit).all()

    return [
        {
            "geography": p.geography_name,
            "geography_type": p.geography_type,
            "year": p.period_year,
            "month": p.period_month,
            "sector": p.sector,
            "price_cents_kwh": float(p.avg_price_cents_kwh) if p.avg_price_cents_kwh else None,
        }
        for p in prices
    ]


@router.get("/prices/comparison")
async def compare_electricity_prices(
    states: str = Query(..., description="Comma-separated state codes (e.g., 'TX,VA,OH')"),
    sector: str = Query("industrial", description="Sector to compare"),
    db: Session = Depends(get_db),
):
    """
    Compare electricity prices across states.

    Useful for site selection cost analysis.
    """
    state_list = [s.strip().upper() for s in states.split(",")]

    prices = db.query(ElectricityPrice).filter(
        ElectricityPrice.geography_type == "state",
        ElectricityPrice.geography_id.in_(state_list),
        ElectricityPrice.sector == sector,
    ).order_by(
        ElectricityPrice.period_year.desc(),
        ElectricityPrice.period_month.desc().nullslast(),
    ).all()

    # Group by state, get latest
    latest_by_state = {}
    for p in prices:
        if p.geography_id not in latest_by_state:
            latest_by_state[p.geography_id] = {
                "state": p.geography_id,
                "price_cents_kwh": float(p.avg_price_cents_kwh) if p.avg_price_cents_kwh else None,
                "year": p.period_year,
                "month": p.period_month,
            }

    return {
        "sector": sector,
        "comparison": sorted(
            latest_by_state.values(),
            key=lambda x: x["price_cents_kwh"] or 999
        )
    }


# =============================================================================
# INTERCONNECTION QUEUE ENDPOINTS
# =============================================================================

@router.get("/interconnection-queue", response_model=List[InterconnectionQueueResponse])
async def search_interconnection_queue(
    iso_region: Optional[str] = Query(None, description="ISO/RTO region (PJM, CAISO, ERCOT, etc.)"),
    state: Optional[str] = Query(None, description="Filter by state"),
    fuel_type: Optional[str] = Query(None, description="Filter by fuel type"),
    status: Optional[str] = Query(None, description="Filter by status (active, withdrawn, completed)"),
    min_capacity_mw: Optional[float] = Query(None, description="Minimum capacity"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Search grid interconnection queue entries.

    Queue wait times indicate grid congestion and new capacity being added.
    """
    query = db.query(InterconnectionQueue)

    if iso_region:
        query = query.filter(InterconnectionQueue.iso_region == iso_region.upper())
    if state:
        query = query.filter(InterconnectionQueue.state == state.upper())
    if fuel_type:
        query = query.filter(InterconnectionQueue.fuel_type == fuel_type)
    if status:
        query = query.filter(InterconnectionQueue.status == status)
    if min_capacity_mw:
        query = query.filter(InterconnectionQueue.capacity_mw >= min_capacity_mw)

    query = query.order_by(InterconnectionQueue.queue_date.desc().nullslast())
    entries = query.limit(limit).all()

    return [
        InterconnectionQueueResponse(
            id=e.id,
            iso_region=e.iso_region,
            queue_id=e.queue_id,
            project_name=e.project_name,
            fuel_type=e.fuel_type,
            capacity_mw=float(e.capacity_mw) if e.capacity_mw else None,
            state=e.state,
            status=e.status,
            queue_date=e.queue_date.isoformat() if e.queue_date else None,
            target_cod=e.target_cod.isoformat() if e.target_cod else None,
        )
        for e in entries
    ]


# =============================================================================
# RENEWABLE RESOURCE ENDPOINTS
# =============================================================================

@router.get("/renewable-potential")
async def get_renewable_potential(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Get solar and wind resource potential at a location.

    Returns nearby resource data from NREL.
    """
    # Find nearest resource data points
    distance_expr = (
        3959 * func.acos(
            func.cos(func.radians(lat)) *
            func.cos(func.radians(RenewableResource.latitude)) *
            func.cos(func.radians(RenewableResource.longitude) - func.radians(lng)) +
            func.sin(func.radians(lat)) *
            func.sin(func.radians(RenewableResource.latitude))
        )
    ).label('distance_miles')

    solar = db.query(RenewableResource, distance_expr).filter(
        RenewableResource.resource_type == 'solar',
        RenewableResource.latitude.isnot(None),
    ).order_by('distance_miles').first()

    wind = db.query(RenewableResource, distance_expr).filter(
        RenewableResource.resource_type == 'wind',
        RenewableResource.latitude.isnot(None),
    ).order_by('distance_miles').first()

    return {
        "location": {"latitude": lat, "longitude": lng},
        "solar": {
            "ghi_kwh_m2_day": float(solar[0].ghi_kwh_m2_day) if solar and solar[0].ghi_kwh_m2_day else None,
            "dni_kwh_m2_day": float(solar[0].dni_kwh_m2_day) if solar and solar[0].dni_kwh_m2_day else None,
            "distance_miles": round(solar[1], 2) if solar else None,
        } if solar else None,
        "wind": {
            "wind_speed_100m_ms": float(wind[0].wind_speed_100m_ms) if wind and wind[0].wind_speed_100m_ms else None,
            "capacity_factor_pct": float(wind[0].capacity_factor_pct) if wind and wind[0].capacity_factor_pct else None,
            "distance_miles": round(wind[1], 2) if wind else None,
        } if wind else None,
    }


# =============================================================================
# SUMMARY ENDPOINT
# =============================================================================

@router.get("/summary")
async def get_power_summary(db: Session = Depends(get_db)):
    """
    Get summary statistics for power infrastructure data.
    """
    plant_count = db.query(func.count(PowerPlant.id)).scalar()
    substation_count = db.query(func.count(Substation.id)).scalar()
    utility_count = db.query(func.count(UtilityTerritory.id)).scalar()
    queue_count = db.query(func.count(InterconnectionQueue.id)).scalar()

    return {
        "domain": "power",
        "record_counts": {
            "power_plants": plant_count,
            "substations": substation_count,
            "utilities": utility_count,
            "interconnection_queue": queue_count,
        },
        "available_endpoints": [
            "/site-intel/power/plants",
            "/site-intel/power/plants/nearby",
            "/site-intel/power/substations",
            "/site-intel/power/substations/nearby",
            "/site-intel/power/utilities",
            "/site-intel/power/prices",
            "/site-intel/power/interconnection-queue",
            "/site-intel/power/renewable-potential",
        ]
    }
