"""
Site Intelligence Platform - Water & Utilities API.

Endpoints for:
- USGS water monitoring sites (streamflow, groundwater)
- EPA public water systems and violations
- Natural gas pipelines and storage
- Utility electricity rates
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc, and_
from math import radians, sin, cos, sqrt, atan2

from app.core.database import get_db
from app.core.models_site_intel import (
    WaterMonitoringSite,
    PublicWaterSystem,
    WaterSystemViolation,
    NaturalGasPipeline,
    NaturalGasStorage,
    UtilityRate,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/site-intel/water-utilities", tags=["Site Intel - Water & Utilities"]
)


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in miles using Haversine formula."""
    R = 3959  # Earth's radius in miles

    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)

    a = (
        sin(delta_lat / 2) ** 2
        + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon / 2) ** 2
    )
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


# =============================================================================
# WATER MONITORING SITE ENDPOINTS (USGS)
# =============================================================================


@router.get("/monitoring-sites")
async def search_monitoring_sites(
    state: Optional[str] = Query(None, description="Filter by state code (e.g., 'TX')"),
    site_type: Optional[str] = Query(
        None, description="Filter by site type: stream, well, spring, lake"
    ),
    has_streamflow: Optional[bool] = Query(
        None, description="Filter to sites with streamflow data"
    ),
    has_groundwater: Optional[bool] = Query(
        None, description="Filter to sites with groundwater data"
    ),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Search USGS water monitoring sites.

    Returns monitoring stations with location and latest readings.
    Use for identifying water availability near potential sites.
    """
    query = db.query(WaterMonitoringSite)

    if state:
        query = query.filter(WaterMonitoringSite.state == state.upper())
    if site_type:
        query = query.filter(WaterMonitoringSite.site_type == site_type.lower())
    if has_streamflow is not None:
        query = query.filter(WaterMonitoringSite.has_streamflow == has_streamflow)
    if has_groundwater is not None:
        query = query.filter(WaterMonitoringSite.has_groundwater == has_groundwater)

    query = query.order_by(WaterMonitoringSite.site_name)
    sites = query.limit(limit).all()

    return [
        {
            "id": s.id,
            "site_number": s.site_number,
            "site_name": s.site_name,
            "site_type": s.site_type,
            "state": s.state,
            "county": s.county,
            "latitude": float(s.latitude) if s.latitude else None,
            "longitude": float(s.longitude) if s.longitude else None,
            "drainage_area_sq_mi": float(s.drainage_area_sq_mi)
            if s.drainage_area_sq_mi
            else None,
            "aquifer_name": s.aquifer_name,
            "well_depth_ft": float(s.well_depth_ft) if s.well_depth_ft else None,
            "latest_streamflow_cfs": float(s.latest_streamflow_cfs)
            if s.latest_streamflow_cfs
            else None,
            "latest_gage_height_ft": float(s.latest_gage_height_ft)
            if s.latest_gage_height_ft
            else None,
            "measurement_date": s.measurement_date.isoformat()
            if s.measurement_date
            else None,
            "has_streamflow": s.has_streamflow,
            "has_groundwater": s.has_groundwater,
        }
        for s in sites
    ]


@router.get("/monitoring-sites/near")
async def find_monitoring_sites_near(
    latitude: float = Query(
        ..., ge=-90, le=90, description="Latitude of target location"
    ),
    longitude: float = Query(
        ..., ge=-180, le=180, description="Longitude of target location"
    ),
    radius_miles: float = Query(50, ge=1, le=200, description="Search radius in miles"),
    site_type: Optional[str] = Query(
        None, description="Filter by site type: stream, well"
    ),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Find water monitoring sites near a location.

    Returns sites sorted by distance, useful for assessing water
    availability at potential industrial sites.
    """
    # Rough bounding box filter for performance
    lat_range = radius_miles / 69.0  # ~69 miles per degree latitude
    lon_range = radius_miles / (69.0 * cos(radians(latitude)))

    query = db.query(WaterMonitoringSite).filter(
        and_(
            WaterMonitoringSite.latitude.between(
                latitude - lat_range, latitude + lat_range
            ),
            WaterMonitoringSite.longitude.between(
                longitude - lon_range, longitude + lon_range
            ),
        )
    )

    if site_type:
        query = query.filter(WaterMonitoringSite.site_type == site_type.lower())

    sites = query.all()

    # Calculate distances and filter
    results = []
    for s in sites:
        if s.latitude and s.longitude:
            distance = haversine_distance(
                latitude, longitude, float(s.latitude), float(s.longitude)
            )
            if distance <= radius_miles:
                results.append(
                    {
                        "id": s.id,
                        "site_number": s.site_number,
                        "site_name": s.site_name,
                        "site_type": s.site_type,
                        "state": s.state,
                        "county": s.county,
                        "latitude": float(s.latitude),
                        "longitude": float(s.longitude),
                        "distance_miles": round(distance, 2),
                        "drainage_area_sq_mi": float(s.drainage_area_sq_mi)
                        if s.drainage_area_sq_mi
                        else None,
                        "aquifer_name": s.aquifer_name,
                        "latest_streamflow_cfs": float(s.latest_streamflow_cfs)
                        if s.latest_streamflow_cfs
                        else None,
                        "has_streamflow": s.has_streamflow,
                        "has_groundwater": s.has_groundwater,
                    }
                )

    # Sort by distance and limit
    results.sort(key=lambda x: x["distance_miles"])
    return results[:limit]


@router.get("/monitoring-sites/{site_number}")
async def get_monitoring_site(
    site_number: str,
    db: Session = Depends(get_db),
):
    """
    Get detailed information for a specific USGS monitoring site.

    Returns complete site details including latest readings.
    """
    site = (
        db.query(WaterMonitoringSite)
        .filter(WaterMonitoringSite.site_number == site_number)
        .first()
    )

    if not site:
        raise HTTPException(
            status_code=404, detail=f"Monitoring site {site_number} not found"
        )

    return {
        "id": site.id,
        "site_number": site.site_number,
        "site_name": site.site_name,
        "site_type": site.site_type,
        "state": site.state,
        "county": site.county,
        "latitude": float(site.latitude) if site.latitude else None,
        "longitude": float(site.longitude) if site.longitude else None,
        "drainage_area_sq_mi": float(site.drainage_area_sq_mi)
        if site.drainage_area_sq_mi
        else None,
        "aquifer_code": site.aquifer_code,
        "aquifer_name": site.aquifer_name,
        "well_depth_ft": float(site.well_depth_ft) if site.well_depth_ft else None,
        "latest_readings": {
            "streamflow_cfs": float(site.latest_streamflow_cfs)
            if site.latest_streamflow_cfs
            else None,
            "gage_height_ft": float(site.latest_gage_height_ft)
            if site.latest_gage_height_ft
            else None,
            "water_temp_c": float(site.latest_water_temp_c)
            if site.latest_water_temp_c
            else None,
            "dissolved_oxygen_mgl": float(site.latest_dissolved_oxygen)
            if site.latest_dissolved_oxygen
            else None,
            "ph": float(site.latest_ph) if site.latest_ph else None,
            "turbidity_ntu": float(site.latest_turbidity)
            if site.latest_turbidity
            else None,
            "measurement_date": site.measurement_date.isoformat()
            if site.measurement_date
            else None,
        },
        "data_availability": {
            "has_streamflow": site.has_streamflow,
            "has_groundwater": site.has_groundwater,
            "has_quality": site.has_quality,
        },
        "source": site.source,
        "collected_at": site.collected_at.isoformat() if site.collected_at else None,
    }


# =============================================================================
# PUBLIC WATER SYSTEM ENDPOINTS (EPA SDWIS)
# =============================================================================


@router.get("/water-systems")
async def search_water_systems(
    state: Optional[str] = Query(None, description="Filter by state code"),
    pws_type: Optional[str] = Query(
        None, description="Type: community, transient, non_transient"
    ),
    min_population: Optional[int] = Query(
        None, description="Minimum population served"
    ),
    primary_source: Optional[str] = Query(
        None, description="Source: groundwater, surface_water"
    ),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Search EPA public water systems.

    Returns water utility information including capacity and compliance.
    Use for identifying municipal water availability.
    """
    query = db.query(PublicWaterSystem)

    if state:
        query = query.filter(PublicWaterSystem.state == state.upper())
    if pws_type:
        query = query.filter(PublicWaterSystem.pws_type == pws_type.lower())
    if min_population:
        query = query.filter(PublicWaterSystem.population_served >= min_population)
    if primary_source:
        query = query.filter(
            PublicWaterSystem.primary_source_name.ilike(f"%{primary_source}%")
        )
    if is_active is not None:
        query = query.filter(PublicWaterSystem.is_active == is_active)

    query = query.order_by(desc(PublicWaterSystem.population_served))
    systems = query.limit(limit).all()

    return [
        {
            "id": s.id,
            "pwsid": s.pwsid,
            "pws_name": s.pws_name,
            "pws_type": s.pws_type,
            "state": s.state,
            "county": s.county,
            "city": s.city,
            "population_served": s.population_served,
            "service_connections": s.service_connections,
            "primary_source": s.primary_source_name,
            "is_active": s.is_active,
            "compliance_status": s.compliance_status,
        }
        for s in systems
    ]


@router.get("/water-systems/{pwsid}")
async def get_water_system(
    pwsid: str,
    db: Session = Depends(get_db),
):
    """
    Get detailed information for a specific public water system.

    Returns complete water system details including infrastructure.
    """
    system = (
        db.query(PublicWaterSystem)
        .filter(PublicWaterSystem.pwsid == pwsid.upper())
        .first()
    )

    if not system:
        raise HTTPException(status_code=404, detail=f"Water system {pwsid} not found")

    return {
        "id": system.id,
        "pwsid": system.pwsid,
        "pws_name": system.pws_name,
        "pws_type": system.pws_type,
        "state": system.state,
        "county": system.county,
        "city": system.city,
        "zip_code": system.zip_code,
        "service_area": {
            "population_served": system.population_served,
            "service_connections": system.service_connections,
            "service_area_type": system.service_area_type,
        },
        "water_source": {
            "primary_source_code": system.primary_source_code,
            "primary_source_name": system.primary_source_name,
            "source_water_protection": system.source_water_protection,
        },
        "infrastructure": {
            "treatment_plant_count": system.treatment_plant_count,
            "storage_capacity_mg": float(system.storage_capacity_mg)
            if system.storage_capacity_mg
            else None,
            "distribution_miles": float(system.distribution_miles)
            if system.distribution_miles
            else None,
        },
        "status": {
            "is_active": system.is_active,
            "compliance_status": system.compliance_status,
            "last_compliance_date": system.last_compliance_date.isoformat()
            if system.last_compliance_date
            else None,
        },
        "contact": {
            "admin_contact_name": system.admin_contact_name,
            "admin_contact_phone": system.admin_contact_phone,
        },
        "source": system.source,
        "collected_at": system.collected_at.isoformat()
        if system.collected_at
        else None,
    }


@router.get("/water-systems/{pwsid}/violations")
async def get_water_system_violations(
    pwsid: str,
    violation_type: Optional[str] = Query(None, description="Filter by violation type"),
    is_health_based: Optional[bool] = Query(
        None, description="Filter to health-based violations"
    ),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Get violation history for a specific water system.

    Returns water quality violations and enforcement actions.
    """
    # Verify system exists
    system = (
        db.query(PublicWaterSystem)
        .filter(PublicWaterSystem.pwsid == pwsid.upper())
        .first()
    )

    if not system:
        raise HTTPException(status_code=404, detail=f"Water system {pwsid} not found")

    query = db.query(WaterSystemViolation).filter(
        WaterSystemViolation.pwsid == pwsid.upper()
    )

    if violation_type:
        query = query.filter(
            WaterSystemViolation.violation_type.ilike(f"%{violation_type}%")
        )
    if is_health_based is not None:
        query = query.filter(WaterSystemViolation.is_health_based == is_health_based)

    query = query.order_by(desc(WaterSystemViolation.violation_date))
    violations = query.limit(limit).all()

    return {
        "pwsid": pwsid.upper(),
        "pws_name": system.pws_name,
        "total_violations": len(violations),
        "violations": [
            {
                "violation_id": v.violation_id,
                "violation_type": v.violation_type,
                "contaminant_code": v.contaminant_code,
                "contaminant_name": v.contaminant_name,
                "contaminant_group": v.contaminant_group,
                "violation_date": v.violation_date.isoformat()
                if v.violation_date
                else None,
                "compliance_period": v.compliance_period,
                "is_health_based": v.is_health_based,
                "severity_level": v.severity_level,
                "enforcement_action": v.enforcement_action,
                "returned_to_compliance": v.returned_to_compliance,
                "returned_to_compliance_date": v.returned_to_compliance_date.isoformat()
                if v.returned_to_compliance_date
                else None,
            }
            for v in violations
        ],
    }


# =============================================================================
# NATURAL GAS PIPELINE ENDPOINTS (EIA)
# =============================================================================


@router.get("/gas-pipelines")
async def search_gas_pipelines(
    state: Optional[str] = Query(
        None, description="Filter by origin or destination state"
    ),
    pipeline_type: Optional[str] = Query(
        None, description="Type: interstate, intrastate"
    ),
    operator: Optional[str] = Query(None, description="Filter by operator name"),
    min_capacity_mmcfd: Optional[float] = Query(
        None, description="Minimum capacity in MMcf/d"
    ),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Search natural gas pipelines.

    Returns pipeline infrastructure for assessing gas availability.
    """
    query = db.query(NaturalGasPipeline)

    if state:
        state_upper = state.upper()
        query = query.filter(
            (NaturalGasPipeline.origin_state == state_upper)
            | (NaturalGasPipeline.destination_state == state_upper)
            | (NaturalGasPipeline.states_crossed.contains([state_upper]))
        )
    if pipeline_type:
        query = query.filter(NaturalGasPipeline.pipeline_type == pipeline_type.lower())
    if operator:
        query = query.filter(NaturalGasPipeline.operator_name.ilike(f"%{operator}%"))
    if min_capacity_mmcfd:
        query = query.filter(NaturalGasPipeline.capacity_mmcfd >= min_capacity_mmcfd)

    query = query.order_by(desc(NaturalGasPipeline.capacity_mmcfd))
    pipelines = query.limit(limit).all()

    return [
        {
            "id": p.id,
            "pipeline_id": p.pipeline_id,
            "pipeline_name": p.pipeline_name,
            "operator_name": p.operator_name,
            "origin_state": p.origin_state,
            "destination_state": p.destination_state,
            "states_crossed": p.states_crossed,
            "capacity_mmcfd": float(p.capacity_mmcfd) if p.capacity_mmcfd else None,
            "pipeline_type": p.pipeline_type,
            "is_bidirectional": p.is_bidirectional,
            "status": p.status,
        }
        for p in pipelines
    ]


# =============================================================================
# NATURAL GAS STORAGE ENDPOINTS (EIA)
# =============================================================================


@router.get("/gas-storage")
async def search_gas_storage(
    state: Optional[str] = Query(None, description="Filter by state code"),
    storage_type: Optional[str] = Query(
        None, description="Type: depleted_field, salt_cavern, aquifer"
    ),
    min_capacity_bcf: Optional[float] = Query(
        None, description="Minimum total capacity in Bcf"
    ),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Search natural gas storage facilities.

    Returns underground storage facilities for energy reliability assessment.
    """
    query = db.query(NaturalGasStorage)

    if state:
        query = query.filter(NaturalGasStorage.state == state.upper())
    if storage_type:
        query = query.filter(NaturalGasStorage.storage_type == storage_type.lower())
    if min_capacity_bcf:
        query = query.filter(NaturalGasStorage.total_capacity_bcf >= min_capacity_bcf)

    query = query.order_by(desc(NaturalGasStorage.total_capacity_bcf))
    facilities = query.limit(limit).all()

    return [
        {
            "id": f.id,
            "facility_id": f.facility_id,
            "facility_name": f.facility_name,
            "operator_name": f.operator_name,
            "state": f.state,
            "county": f.county,
            "latitude": float(f.latitude) if f.latitude else None,
            "longitude": float(f.longitude) if f.longitude else None,
            "storage_type": f.storage_type,
            "total_capacity_bcf": float(f.total_capacity_bcf)
            if f.total_capacity_bcf
            else None,
            "working_gas_bcf": float(f.working_gas_bcf) if f.working_gas_bcf else None,
            "deliverability_mmcfd": float(f.deliverability_mmcfd)
            if f.deliverability_mmcfd
            else None,
            "utilization_pct": float(f.utilization_pct) if f.utilization_pct else None,
            "status": f.status,
        }
        for f in facilities
    ]


@router.get("/gas-storage/near")
async def find_gas_storage_near(
    latitude: float = Query(
        ..., ge=-90, le=90, description="Latitude of target location"
    ),
    longitude: float = Query(
        ..., ge=-180, le=180, description="Longitude of target location"
    ),
    radius_miles: float = Query(
        100, ge=1, le=300, description="Search radius in miles"
    ),
    storage_type: Optional[str] = Query(None, description="Filter by storage type"),
    limit: int = Query(20, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """
    Find natural gas storage facilities near a location.

    Returns facilities sorted by distance for energy reliability assessment.
    """
    # Rough bounding box filter
    lat_range = radius_miles / 69.0
    lon_range = radius_miles / (69.0 * cos(radians(latitude)))

    query = db.query(NaturalGasStorage).filter(
        and_(
            NaturalGasStorage.latitude.between(
                latitude - lat_range, latitude + lat_range
            ),
            NaturalGasStorage.longitude.between(
                longitude - lon_range, longitude + lon_range
            ),
        )
    )

    if storage_type:
        query = query.filter(NaturalGasStorage.storage_type == storage_type.lower())

    facilities = query.all()

    # Calculate distances and filter
    results = []
    for f in facilities:
        if f.latitude and f.longitude:
            distance = haversine_distance(
                latitude, longitude, float(f.latitude), float(f.longitude)
            )
            if distance <= radius_miles:
                results.append(
                    {
                        "id": f.id,
                        "facility_id": f.facility_id,
                        "facility_name": f.facility_name,
                        "operator_name": f.operator_name,
                        "state": f.state,
                        "county": f.county,
                        "latitude": float(f.latitude),
                        "longitude": float(f.longitude),
                        "distance_miles": round(distance, 2),
                        "storage_type": f.storage_type,
                        "total_capacity_bcf": float(f.total_capacity_bcf)
                        if f.total_capacity_bcf
                        else None,
                        "deliverability_mmcfd": float(f.deliverability_mmcfd)
                        if f.deliverability_mmcfd
                        else None,
                        "status": f.status,
                    }
                )

    # Sort by distance and limit
    results.sort(key=lambda x: x["distance_miles"])
    return results[:limit]


# =============================================================================
# UTILITY RATE ENDPOINTS (OpenEI/EIA)
# =============================================================================


@router.get("/utility-rates")
async def search_utility_rates(
    state: Optional[str] = Query(None, description="Filter by state code"),
    customer_class: Optional[str] = Query(
        None, description="Class: residential, commercial, industrial"
    ),
    utility_name: Optional[str] = Query(None, description="Filter by utility name"),
    has_time_of_use: Optional[bool] = Query(None, description="Filter to TOU rates"),
    has_demand_charges: Optional[bool] = Query(
        None, description="Filter to rates with demand charges"
    ),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Search utility electricity rates.

    Returns rate schedules for energy cost analysis at potential sites.
    """
    query = db.query(UtilityRate)

    if state:
        query = query.filter(UtilityRate.state == state.upper())
    if customer_class:
        query = query.filter(UtilityRate.customer_class == customer_class.lower())
    if utility_name:
        query = query.filter(UtilityRate.utility_name.ilike(f"%{utility_name}%"))
    if has_time_of_use is not None:
        query = query.filter(UtilityRate.has_time_of_use == has_time_of_use)
    if has_demand_charges is not None:
        query = query.filter(UtilityRate.has_demand_charges == has_demand_charges)

    query = query.order_by(UtilityRate.energy_rate_kwh)
    rates = query.limit(limit).all()

    return [
        {
            "id": r.id,
            "rate_schedule_id": r.rate_schedule_id,
            "utility_id": r.utility_id,
            "utility_name": r.utility_name,
            "state": r.state,
            "rate_schedule_name": r.rate_schedule_name,
            "customer_class": r.customer_class,
            "energy_rate_kwh": float(r.energy_rate_kwh) if r.energy_rate_kwh else None,
            "demand_charge_kw": float(r.demand_charge_kw)
            if r.demand_charge_kw
            else None,
            "fixed_monthly_charge": float(r.fixed_monthly_charge)
            if r.fixed_monthly_charge
            else None,
            "has_time_of_use": r.has_time_of_use,
            "has_demand_charges": r.has_demand_charges,
            "effective_date": r.effective_date.isoformat()
            if r.effective_date
            else None,
            "source": r.source,
        }
        for r in rates
    ]


@router.get("/utility-rates/compare")
async def compare_utility_rates(
    states: str = Query(
        ..., description="Comma-separated state codes (e.g., 'TX,CA,OH')"
    ),
    customer_class: str = Query("industrial", description="Customer class to compare"),
    db: Session = Depends(get_db),
):
    """
    Compare utility rates across multiple states.

    Returns average rates by state for cost comparison analysis.
    """
    state_list = [s.strip().upper() for s in states.split(",")]

    results = {}
    for state in state_list:
        rates = (
            db.query(UtilityRate)
            .filter(
                and_(
                    UtilityRate.state == state,
                    UtilityRate.customer_class == customer_class.lower(),
                )
            )
            .all()
        )

        if rates:
            energy_rates = [
                float(r.energy_rate_kwh) for r in rates if r.energy_rate_kwh
            ]
            demand_charges = [
                float(r.demand_charge_kw) for r in rates if r.demand_charge_kw
            ]

            results[state] = {
                "state": state,
                "customer_class": customer_class.lower(),
                "rate_count": len(rates),
                "avg_energy_rate_kwh": round(sum(energy_rates) / len(energy_rates), 6)
                if energy_rates
                else None,
                "min_energy_rate_kwh": round(min(energy_rates), 6)
                if energy_rates
                else None,
                "max_energy_rate_kwh": round(max(energy_rates), 6)
                if energy_rates
                else None,
                "avg_demand_charge_kw": round(
                    sum(demand_charges) / len(demand_charges), 2
                )
                if demand_charges
                else None,
                "utilities": list(set(r.utility_name for r in rates if r.utility_name)),
            }
        else:
            results[state] = {
                "state": state,
                "customer_class": customer_class.lower(),
                "rate_count": 0,
                "avg_energy_rate_kwh": None,
                "utilities": [],
            }

    # Sort by average energy rate
    sorted_results = sorted(
        results.values(),
        key=lambda x: x.get("avg_energy_rate_kwh") or float("inf"),
    )

    return {
        "comparison": sorted_results,
        "lowest_cost_state": sorted_results[0]["state"]
        if sorted_results and sorted_results[0].get("avg_energy_rate_kwh")
        else None,
    }


@router.get("/utility-rates/{utility_id}")
async def get_utility_rates(
    utility_id: str,
    customer_class: Optional[str] = Query(None, description="Filter by customer class"),
    db: Session = Depends(get_db),
):
    """
    Get all rate schedules for a specific utility.

    Returns available rate schedules and pricing details.
    """
    query = db.query(UtilityRate).filter(UtilityRate.utility_id == utility_id)

    if customer_class:
        query = query.filter(UtilityRate.customer_class == customer_class.lower())

    rates = query.order_by(
        UtilityRate.customer_class, UtilityRate.rate_schedule_name
    ).all()

    if not rates:
        raise HTTPException(
            status_code=404, detail=f"No rates found for utility {utility_id}"
        )

    utility_name = rates[0].utility_name if rates else None
    state = rates[0].state if rates else None

    return {
        "utility_id": utility_id,
        "utility_name": utility_name,
        "state": state,
        "rate_schedules": [
            {
                "rate_schedule_id": r.rate_schedule_id,
                "rate_schedule_name": r.rate_schedule_name,
                "customer_class": r.customer_class,
                "sector": r.sector,
                "pricing": {
                    "energy_rate_kwh": float(r.energy_rate_kwh)
                    if r.energy_rate_kwh
                    else None,
                    "demand_charge_kw": float(r.demand_charge_kw)
                    if r.demand_charge_kw
                    else None,
                    "fixed_monthly_charge": float(r.fixed_monthly_charge)
                    if r.fixed_monthly_charge
                    else None,
                    "minimum_charge": float(r.minimum_charge)
                    if r.minimum_charge
                    else None,
                },
                "features": {
                    "has_time_of_use": r.has_time_of_use,
                    "has_demand_charges": r.has_demand_charges,
                    "has_net_metering": r.has_net_metering,
                },
                "effective_date": r.effective_date.isoformat()
                if r.effective_date
                else None,
                "description": r.description,
                "source": r.source,
                "source_url": r.source_url,
            }
            for r in rates
        ],
    }
