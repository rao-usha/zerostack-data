"""
Site Intelligence Platform - Freight & Logistics API.

Endpoints for:
- Container freight rates (Freightos, Drewry, SCFI)
- Trucking rates (USDA AMS, spot rates)
- Motor carriers and safety (FMCSA)
- Port throughput metrics
- Air cargo statistics
- Trade gateway data
- 3PL company directory
- Warehouse listings
"""
import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from app.core.database import get_db
from app.core.models_site_intel import (
    FreightRateIndex, TruckingLaneRate, WarehouseFacility,
    ContainerFreightIndex, UsdaTruckRate, MotorCarrier, CarrierSafety,
    PortThroughputMonthly, AirCargoStats, TradeGatewayStats,
    ThreePLCompany, WarehouseListing,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/site-intel/logistics", tags=["Site Intel - Logistics"])


# =============================================================================
# CONTAINER FREIGHT RATE ENDPOINTS
# =============================================================================

@router.get("/container-rates")
async def search_container_rates(
    provider: Optional[str] = Query(None, description="Provider: freightos, drewry, scfi"),
    index_code: Optional[str] = Query(None),
    origin_region: Optional[str] = Query(None),
    destination_region: Optional[str] = Query(None),
    container_type: Optional[str] = Query(None, description="Type: 20ft, 40ft, 40hc"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search container freight rate indices."""
    query = db.query(ContainerFreightIndex)

    if provider:
        query = query.filter(ContainerFreightIndex.provider == provider)
    if index_code:
        query = query.filter(ContainerFreightIndex.index_code == index_code)
    if origin_region:
        query = query.filter(ContainerFreightIndex.route_origin_region.ilike(f"%{origin_region}%"))
    if destination_region:
        query = query.filter(ContainerFreightIndex.route_destination_region.ilike(f"%{destination_region}%"))
    if container_type:
        query = query.filter(ContainerFreightIndex.container_type == container_type)

    query = query.order_by(ContainerFreightIndex.rate_date.desc())
    rates = query.limit(limit).all()

    return [
        {
            "id": r.id,
            "index_code": r.index_code,
            "provider": r.provider,
            "route": f"{r.route_origin_port or r.route_origin_region} → {r.route_destination_port or r.route_destination_region}",
            "container_type": r.container_type,
            "rate_date": r.rate_date.isoformat() if r.rate_date else None,
            "rate_value": float(r.rate_value) if r.rate_value else None,
            "change_pct_wow": float(r.change_pct_wow) if r.change_pct_wow else None,
            "change_pct_mom": float(r.change_pct_mom) if r.change_pct_mom else None,
            "change_pct_yoy": float(r.change_pct_yoy) if r.change_pct_yoy else None,
        }
        for r in rates
    ]


@router.get("/container-rates/routes")
async def list_container_routes(
    provider: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """List available container freight trade lanes."""
    query = db.query(
        ContainerFreightIndex.index_code,
        ContainerFreightIndex.provider,
        ContainerFreightIndex.route_origin_region,
        ContainerFreightIndex.route_origin_port,
        ContainerFreightIndex.route_destination_region,
        ContainerFreightIndex.route_destination_port,
    ).distinct()

    if provider:
        query = query.filter(ContainerFreightIndex.provider == provider)

    routes = query.all()

    return [
        {
            "index_code": r.index_code,
            "provider": r.provider,
            "origin_region": r.route_origin_region,
            "origin_port": r.route_origin_port,
            "destination_region": r.route_destination_region,
            "destination_port": r.route_destination_port,
        }
        for r in routes
    ]


@router.get("/container-rates/history/{index_code}")
async def get_container_rate_history(
    index_code: str,
    weeks: int = Query(52, ge=1, le=156),
    db: Session = Depends(get_db),
):
    """Get historical rates for a specific container route."""
    rates = db.query(ContainerFreightIndex).filter(
        ContainerFreightIndex.index_code == index_code
    ).order_by(ContainerFreightIndex.rate_date.desc()).limit(weeks).all()

    if not rates:
        raise HTTPException(status_code=404, detail=f"No rates found for index {index_code}")

    return {
        "index_code": index_code,
        "provider": rates[0].provider if rates else None,
        "route": f"{rates[0].route_origin_port or rates[0].route_origin_region} → {rates[0].route_destination_port or rates[0].route_destination_region}" if rates else None,
        "history": [
            {
                "date": r.rate_date.isoformat() if r.rate_date else None,
                "rate": float(r.rate_value) if r.rate_value else None,
                "change_pct_wow": float(r.change_pct_wow) if r.change_pct_wow else None,
            }
            for r in rates
        ]
    }


# =============================================================================
# TRUCKING RATE ENDPOINTS (USDA Agricultural)
# =============================================================================

@router.get("/truck-rates/agricultural")
async def search_agricultural_truck_rates(
    origin_state: Optional[str] = Query(None),
    destination_state: Optional[str] = Query(None),
    commodity: Optional[str] = Query(None),
    mileage_band: Optional[str] = Query(None, description="local, short, medium, long"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search USDA agricultural truck rates."""
    query = db.query(UsdaTruckRate)

    if origin_state:
        query = query.filter(UsdaTruckRate.origin_state == origin_state.upper())
    if destination_state:
        query = query.filter(UsdaTruckRate.destination_state == destination_state.upper())
    if commodity:
        query = query.filter(UsdaTruckRate.commodity.ilike(f"%{commodity}%"))
    if mileage_band:
        query = query.filter(UsdaTruckRate.mileage_band == mileage_band)

    query = query.order_by(UsdaTruckRate.report_date.desc())
    rates = query.limit(limit).all()

    return [
        {
            "id": r.id,
            "origin_region": r.origin_region,
            "origin_state": r.origin_state,
            "destination_city": r.destination_city,
            "destination_state": r.destination_state,
            "commodity": r.commodity,
            "mileage_band": r.mileage_band,
            "rate_per_mile": float(r.rate_per_mile) if r.rate_per_mile else None,
            "rate_per_truckload": float(r.rate_per_truckload) if r.rate_per_truckload else None,
            "fuel_price": float(r.fuel_price) if r.fuel_price else None,
            "report_date": r.report_date.isoformat() if r.report_date else None,
        }
        for r in rates
    ]


@router.get("/truck-rates/commodities")
async def list_truck_rate_commodities(db: Session = Depends(get_db)):
    """List available commodities with truck rate data."""
    commodities = db.query(
        UsdaTruckRate.commodity,
        func.count(UsdaTruckRate.id).label("record_count"),
        func.avg(UsdaTruckRate.rate_per_mile).label("avg_rate_per_mile"),
    ).group_by(UsdaTruckRate.commodity).all()

    return [
        {
            "commodity": c.commodity,
            "record_count": c.record_count,
            "avg_rate_per_mile": round(float(c.avg_rate_per_mile), 2) if c.avg_rate_per_mile else None,
        }
        for c in commodities
    ]


# =============================================================================
# MOTOR CARRIER ENDPOINTS (FMCSA)
# =============================================================================

@router.get("/carriers")
async def search_carriers(
    state: Optional[str] = Query(None),
    min_power_units: Optional[int] = Query(None),
    carrier_operation: Optional[str] = Query(None),
    name: Optional[str] = Query(None, description="Search by company name"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Search motor carriers."""
    query = db.query(MotorCarrier)

    if state:
        query = query.filter(MotorCarrier.physical_state == state.upper())
    if min_power_units:
        query = query.filter(MotorCarrier.power_units >= min_power_units)
    if carrier_operation:
        query = query.filter(MotorCarrier.carrier_operation == carrier_operation)
    if name:
        query = query.filter(
            (MotorCarrier.legal_name.ilike(f"%{name}%")) |
            (MotorCarrier.dba_name.ilike(f"%{name}%"))
        )

    query = query.filter(MotorCarrier.is_active == True)
    query = query.order_by(MotorCarrier.power_units.desc().nullslast())
    carriers = query.limit(limit).all()

    return [
        {
            "id": c.id,
            "dot_number": c.dot_number,
            "mc_number": c.mc_number,
            "legal_name": c.legal_name,
            "dba_name": c.dba_name,
            "city": c.physical_city,
            "state": c.physical_state,
            "power_units": c.power_units,
            "drivers": c.drivers,
            "carrier_operation": c.carrier_operation,
        }
        for c in carriers
    ]


@router.get("/carriers/{dot_number}")
async def get_carrier_details(dot_number: str, db: Session = Depends(get_db)):
    """Get detailed carrier information."""
    carrier = db.query(MotorCarrier).filter(
        MotorCarrier.dot_number == dot_number
    ).first()

    if not carrier:
        raise HTTPException(status_code=404, detail=f"Carrier DOT {dot_number} not found")

    return {
        "dot_number": carrier.dot_number,
        "mc_number": carrier.mc_number,
        "legal_name": carrier.legal_name,
        "dba_name": carrier.dba_name,
        "physical_address": carrier.physical_address,
        "physical_city": carrier.physical_city,
        "physical_state": carrier.physical_state,
        "physical_zip": carrier.physical_zip,
        "telephone": carrier.telephone,
        "email": carrier.email,
        "power_units": carrier.power_units,
        "drivers": carrier.drivers,
        "mcs150_date": carrier.mcs150_date.isoformat() if carrier.mcs150_date else None,
        "mcs150_mileage": carrier.mcs150_mileage,
        "carrier_operation": carrier.carrier_operation,
        "cargo_carried": carrier.cargo_carried,
        "operation_classification": carrier.operation_classification,
        "is_active": carrier.is_active,
    }


@router.get("/carriers/{dot_number}/safety")
async def get_carrier_safety(dot_number: str, db: Session = Depends(get_db)):
    """Get carrier safety record and BASIC scores."""
    # Get latest safety record
    safety = db.query(CarrierSafety).filter(
        CarrierSafety.dot_number == dot_number
    ).order_by(CarrierSafety.inspection_date.desc()).first()

    if not safety:
        raise HTTPException(status_code=404, detail=f"No safety data for DOT {dot_number}")

    return {
        "dot_number": safety.dot_number,
        "safety_rating": safety.safety_rating,
        "rating_date": safety.rating_date.isoformat() if safety.rating_date else None,
        "basic_scores": {
            "unsafe_driving": float(safety.unsafe_driving_score) if safety.unsafe_driving_score else None,
            "hours_of_service": float(safety.hours_of_service_score) if safety.hours_of_service_score else None,
            "driver_fitness": float(safety.driver_fitness_score) if safety.driver_fitness_score else None,
            "controlled_substances": float(safety.controlled_substances_score) if safety.controlled_substances_score else None,
            "vehicle_maintenance": float(safety.vehicle_maintenance_score) if safety.vehicle_maintenance_score else None,
            "hazmat_compliance": float(safety.hazmat_compliance_score) if safety.hazmat_compliance_score else None,
            "crash_indicator": float(safety.crash_indicator_score) if safety.crash_indicator_score else None,
        },
        "inspection_stats": {
            "vehicle_oos_rate": float(safety.vehicle_oos_rate) if safety.vehicle_oos_rate else None,
            "driver_oos_rate": float(safety.driver_oos_rate) if safety.driver_oos_rate else None,
            "total_inspections": safety.total_inspections,
            "total_violations": safety.total_violations,
        },
        "crash_stats": {
            "total_crashes": safety.total_crashes,
            "fatal_crashes": safety.fatal_crashes,
            "injury_crashes": safety.injury_crashes,
            "tow_crashes": safety.tow_crashes,
        },
        "inspection_date": safety.inspection_date.isoformat() if safety.inspection_date else None,
    }


# =============================================================================
# PORT THROUGHPUT ENDPOINTS
# =============================================================================

@router.get("/port-throughput")
async def search_port_throughput(
    port_code: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None, ge=1, le=12),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search port throughput statistics."""
    query = db.query(PortThroughputMonthly)

    if port_code:
        query = query.filter(PortThroughputMonthly.port_code == port_code.upper())
    if year:
        query = query.filter(PortThroughputMonthly.period_year == year)
    if month:
        query = query.filter(PortThroughputMonthly.period_month == month)

    query = query.order_by(
        PortThroughputMonthly.period_year.desc(),
        PortThroughputMonthly.period_month.desc()
    )
    stats = query.limit(limit).all()

    return [
        {
            "port_code": s.port_code,
            "port_name": s.port_name,
            "period": f"{s.period_year}-{s.period_month:02d}",
            "teu_total": s.teu_total,
            "teu_loaded_import": s.teu_loaded_import,
            "teu_loaded_export": s.teu_loaded_export,
            "container_vessel_calls": s.container_vessel_calls,
            "avg_berthing_hours": float(s.avg_berthing_hours) if s.avg_berthing_hours else None,
            "tonnage_total": s.tonnage_total,
        }
        for s in stats
    ]


@router.get("/port-throughput/{port_code}/history")
async def get_port_history(
    port_code: str,
    months: int = Query(24, ge=1, le=60),
    db: Session = Depends(get_db),
):
    """Get historical throughput for a specific port."""
    stats = db.query(PortThroughputMonthly).filter(
        PortThroughputMonthly.port_code == port_code.upper()
    ).order_by(
        PortThroughputMonthly.period_year.desc(),
        PortThroughputMonthly.period_month.desc()
    ).limit(months).all()

    if not stats:
        raise HTTPException(status_code=404, detail=f"No data for port {port_code}")

    return {
        "port_code": port_code.upper(),
        "port_name": stats[0].port_name if stats else None,
        "history": [
            {
                "period": f"{s.period_year}-{s.period_month:02d}",
                "teu_total": s.teu_total,
                "vessel_calls": s.container_vessel_calls,
                "tonnage_total": s.tonnage_total,
            }
            for s in stats
        ]
    }


# =============================================================================
# AIR CARGO ENDPOINTS
# =============================================================================

@router.get("/air-cargo")
async def search_air_cargo(
    airport_code: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None, ge=1, le=12),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search air cargo statistics by airport."""
    query = db.query(AirCargoStats)

    if airport_code:
        query = query.filter(AirCargoStats.airport_code == airport_code.upper())
    if year:
        query = query.filter(AirCargoStats.period_year == year)
    if month:
        query = query.filter(AirCargoStats.period_month == month)

    query = query.order_by(
        AirCargoStats.period_year.desc(),
        AirCargoStats.period_month.desc()
    )
    stats = query.limit(limit).all()

    return [
        {
            "airport_code": s.airport_code,
            "airport_name": s.airport_name,
            "period": f"{s.period_year}-{s.period_month:02d}",
            "freight_tons_total": float(s.freight_tons_total) if s.freight_tons_total else None,
            "freight_domestic": float(s.freight_domestic) if s.freight_domestic else None,
            "freight_international": float(s.freight_international) if s.freight_international else None,
            "mail_tons": float(s.mail_tons) if s.mail_tons else None,
        }
        for s in stats
    ]


@router.get("/air-cargo/top-airports")
async def get_top_cargo_airports(
    year: Optional[int] = Query(None),
    limit: int = Query(20, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Get top cargo airports by annual volume."""
    # Aggregate by airport
    query = db.query(
        AirCargoStats.airport_code,
        AirCargoStats.airport_name,
        func.sum(AirCargoStats.freight_tons_total).label("annual_tons"),
        func.sum(AirCargoStats.freight_domestic).label("domestic_tons"),
        func.sum(AirCargoStats.freight_international).label("international_tons"),
    ).group_by(
        AirCargoStats.airport_code,
        AirCargoStats.airport_name
    )

    if year:
        query = query.filter(AirCargoStats.period_year == year)

    query = query.order_by(desc("annual_tons"))
    airports = query.limit(limit).all()

    return [
        {
            "rank": i + 1,
            "airport_code": a.airport_code,
            "airport_name": a.airport_name,
            "annual_tons": float(a.annual_tons) if a.annual_tons else None,
            "domestic_tons": float(a.domestic_tons) if a.domestic_tons else None,
            "international_tons": float(a.international_tons) if a.international_tons else None,
        }
        for i, a in enumerate(airports)
    ]


# =============================================================================
# TRADE GATEWAY ENDPOINTS
# =============================================================================

@router.get("/trade-gateways")
async def search_trade_gateways(
    customs_district: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None, ge=1, le=12),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Search trade gateway import/export statistics."""
    query = db.query(TradeGatewayStats)

    if customs_district:
        query = query.filter(TradeGatewayStats.customs_district.ilike(f"%{customs_district}%"))
    if year:
        query = query.filter(TradeGatewayStats.period_year == year)
    if month:
        query = query.filter(TradeGatewayStats.period_month == month)

    query = query.order_by(
        TradeGatewayStats.period_year.desc(),
        TradeGatewayStats.period_month.desc()
    )
    stats = query.limit(limit).all()

    return [
        {
            "customs_district": s.customs_district,
            "port_code": s.port_code,
            "period": f"{s.period_year}-{s.period_month:02d}",
            "import_value_million": float(s.import_value_million) if s.import_value_million else None,
            "export_value_million": float(s.export_value_million) if s.export_value_million else None,
            "trade_balance_million": float(s.trade_balance_million) if s.trade_balance_million else None,
            "mode_breakdown": {
                "vessel_pct": float(s.vessel_pct) if s.vessel_pct else None,
                "air_pct": float(s.air_pct) if s.air_pct else None,
                "truck_pct": float(s.truck_pct) if s.truck_pct else None,
                "rail_pct": float(s.rail_pct) if s.rail_pct else None,
            },
        }
        for s in stats
    ]


@router.get("/trade-gateways/trends")
async def get_trade_trends(
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """Get trade volume trends by district."""
    query = db.query(
        TradeGatewayStats.customs_district,
        func.sum(TradeGatewayStats.import_value_million).label("total_imports"),
        func.sum(TradeGatewayStats.export_value_million).label("total_exports"),
    ).group_by(TradeGatewayStats.customs_district)

    if year:
        query = query.filter(TradeGatewayStats.period_year == year)

    query = query.order_by(desc("total_imports"))
    districts = query.limit(25).all()

    return [
        {
            "customs_district": d.customs_district,
            "total_imports_million": float(d.total_imports) if d.total_imports else None,
            "total_exports_million": float(d.total_exports) if d.total_exports else None,
            "trade_balance_million": round(float(d.total_exports or 0) - float(d.total_imports or 0), 2),
        }
        for d in districts
    ]


# =============================================================================
# 3PL COMPANY ENDPOINTS
# =============================================================================

@router.get("/3pl-companies")
async def search_3pl_companies(
    state: Optional[str] = Query(None),
    service: Optional[str] = Query(None, description="Service type (warehousing, transportation, etc.)"),
    has_cold_chain: Optional[bool] = Query(None),
    min_revenue: Optional[float] = Query(None, description="Minimum annual revenue in millions"),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Search 3PL company directory."""
    query = db.query(ThreePLCompany)

    if state:
        query = query.filter(ThreePLCompany.headquarters_state == state.upper())
    if has_cold_chain is not None:
        query = query.filter(ThreePLCompany.has_cold_chain == has_cold_chain)
    if min_revenue:
        query = query.filter(ThreePLCompany.annual_revenue_million >= min_revenue)

    query = query.order_by(ThreePLCompany.annual_revenue_million.desc().nullslast())
    companies = query.limit(limit).all()

    return [
        {
            "id": c.id,
            "company_name": c.company_name,
            "parent_company": c.parent_company,
            "headquarters": f"{c.headquarters_city}, {c.headquarters_state}" if c.headquarters_city else c.headquarters_state,
            "annual_revenue_million": float(c.annual_revenue_million) if c.annual_revenue_million else None,
            "employee_count": c.employee_count,
            "facility_count": c.facility_count,
            "transport_topics_rank": c.transport_topics_rank,
            "services": c.services,
            "has_cold_chain": c.has_cold_chain,
            "is_asset_based": c.is_asset_based,
        }
        for c in companies
    ]


@router.get("/3pl-companies/{company_id}/coverage")
async def get_3pl_coverage(company_id: int, db: Session = Depends(get_db)):
    """Get 3PL company geographic coverage."""
    company = db.query(ThreePLCompany).filter(ThreePLCompany.id == company_id).first()

    if not company:
        raise HTTPException(status_code=404, detail=f"Company ID {company_id} not found")

    return {
        "company_name": company.company_name,
        "regions_served": company.regions_served,
        "states_coverage": company.states_coverage,
        "countries_coverage": company.countries_coverage,
        "facility_count": company.facility_count,
        "industries_served": company.industries_served,
    }


# =============================================================================
# WAREHOUSE LISTING ENDPOINTS
# =============================================================================

@router.get("/warehouse-listings")
async def search_warehouse_listings(
    state: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    listing_type: Optional[str] = Query(None, description="for_lease, for_sale"),
    min_sqft: Optional[int] = Query(None),
    has_cold: Optional[bool] = Query(None),
    has_rail: Optional[bool] = Query(None),
    max_rent: Optional[float] = Query(None, description="Max rent $/SF/year"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Search active warehouse listings."""
    query = db.query(WarehouseListing).filter(WarehouseListing.is_active == True)

    if state:
        query = query.filter(WarehouseListing.state == state.upper())
    if city:
        query = query.filter(WarehouseListing.city.ilike(f"%{city}%"))
    if listing_type:
        query = query.filter(WarehouseListing.listing_type == listing_type)
    if min_sqft:
        query = query.filter(WarehouseListing.total_sqft >= min_sqft)
    if has_cold is not None:
        query = query.filter(WarehouseListing.has_cold_storage == has_cold)
    if has_rail is not None:
        query = query.filter(WarehouseListing.has_rail_spur == has_rail)
    if max_rent:
        query = query.filter(WarehouseListing.asking_rent_psf <= max_rent)

    query = query.order_by(WarehouseListing.total_sqft.desc().nullslast())
    listings = query.limit(limit).all()

    return [
        {
            "id": l.id,
            "listing_id": l.listing_id,
            "property_name": l.property_name,
            "listing_type": l.listing_type,
            "property_type": l.property_type,
            "city": l.city,
            "state": l.state,
            "total_sqft": l.total_sqft,
            "available_sqft": l.available_sqft,
            "clear_height_ft": l.clear_height_ft,
            "dock_doors": l.dock_doors,
            "has_cold_storage": l.has_cold_storage,
            "has_rail_spur": l.has_rail_spur,
            "asking_rent_psf": float(l.asking_rent_psf) if l.asking_rent_psf else None,
            "asking_price": l.asking_price,
            "broker_company": l.broker_company,
        }
        for l in listings
    ]


@router.get("/warehouse-listings/market-summary")
async def get_warehouse_market_summary(
    state: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Get warehouse market summary with vacancy and rent statistics."""
    query = db.query(
        WarehouseListing.state,
        func.count(WarehouseListing.id).label("listing_count"),
        func.sum(WarehouseListing.total_sqft).label("total_sqft"),
        func.sum(WarehouseListing.available_sqft).label("available_sqft"),
        func.avg(WarehouseListing.asking_rent_psf).label("avg_rent_psf"),
        func.avg(WarehouseListing.clear_height_ft).label("avg_clear_height"),
    ).filter(
        WarehouseListing.is_active == True,
        WarehouseListing.listing_type == "for_lease"
    ).group_by(WarehouseListing.state)

    if state:
        query = query.filter(WarehouseListing.state == state.upper())

    query = query.order_by(desc("total_sqft"))
    markets = query.all()

    return [
        {
            "state": m.state,
            "listing_count": m.listing_count,
            "total_sqft": m.total_sqft,
            "available_sqft": m.available_sqft,
            "avg_rent_psf": round(float(m.avg_rent_psf), 2) if m.avg_rent_psf else None,
            "avg_clear_height": round(float(m.avg_clear_height), 1) if m.avg_clear_height else None,
        }
        for m in markets
    ]


# =============================================================================
# LEGACY ENDPOINTS (maintained for backwards compatibility)
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
    """Search freight rate indices (legacy endpoint)."""
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


@router.get("/trucking-rates")
async def search_trucking_rates(
    origin: Optional[str] = Query(None, description="Origin market"),
    destination: Optional[str] = Query(None, description="Destination market"),
    equipment_type: Optional[str] = Query(None, description="Type: van, reefer, flatbed"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Search trucking lane rates (legacy endpoint)."""
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


@router.get("/warehouses")
async def search_warehouses(
    state: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    facility_type: Optional[str] = Query(None),
    min_sqft: Optional[int] = Query(None),
    has_cold: Optional[bool] = Query(None),
    has_rail: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Search warehouse/3PL facilities (legacy endpoint)."""
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


# =============================================================================
# SUMMARY ENDPOINT
# =============================================================================

@router.get("/summary")
async def get_logistics_summary(db: Session = Depends(get_db)):
    """Get summary statistics for logistics data."""
    return {
        "domain": "logistics",
        "record_counts": {
            "container_freight_indices": db.query(func.count(ContainerFreightIndex.id)).scalar() or 0,
            "usda_truck_rates": db.query(func.count(UsdaTruckRate.id)).scalar() or 0,
            "motor_carriers": db.query(func.count(MotorCarrier.id)).scalar() or 0,
            "carrier_safety_records": db.query(func.count(CarrierSafety.id)).scalar() or 0,
            "port_throughput_records": db.query(func.count(PortThroughputMonthly.id)).scalar() or 0,
            "air_cargo_records": db.query(func.count(AirCargoStats.id)).scalar() or 0,
            "trade_gateway_records": db.query(func.count(TradeGatewayStats.id)).scalar() or 0,
            "three_pl_companies": db.query(func.count(ThreePLCompany.id)).scalar() or 0,
            "warehouse_listings": db.query(func.count(WarehouseListing.id)).scalar() or 0,
            # Legacy tables
            "freight_rate_indices": db.query(func.count(FreightRateIndex.id)).scalar() or 0,
            "trucking_lane_rates": db.query(func.count(TruckingLaneRate.id)).scalar() or 0,
            "warehouse_facilities": db.query(func.count(WarehouseFacility.id)).scalar() or 0,
        },
        "available_endpoints": [
            # Container rates
            "/site-intel/logistics/container-rates",
            "/site-intel/logistics/container-rates/routes",
            "/site-intel/logistics/container-rates/history/{index_code}",
            # USDA truck rates
            "/site-intel/logistics/truck-rates/agricultural",
            "/site-intel/logistics/truck-rates/commodities",
            # Motor carriers
            "/site-intel/logistics/carriers",
            "/site-intel/logistics/carriers/{dot_number}",
            "/site-intel/logistics/carriers/{dot_number}/safety",
            # Port throughput
            "/site-intel/logistics/port-throughput",
            "/site-intel/logistics/port-throughput/{port_code}/history",
            # Air cargo
            "/site-intel/logistics/air-cargo",
            "/site-intel/logistics/air-cargo/top-airports",
            # Trade gateways
            "/site-intel/logistics/trade-gateways",
            "/site-intel/logistics/trade-gateways/trends",
            # 3PL companies
            "/site-intel/logistics/3pl-companies",
            "/site-intel/logistics/3pl-companies/{company_id}/coverage",
            # Warehouse listings
            "/site-intel/logistics/warehouse-listings",
            "/site-intel/logistics/warehouse-listings/market-summary",
            # Legacy
            "/site-intel/logistics/freight-rates",
            "/site-intel/logistics/trucking-rates",
            "/site-intel/logistics/warehouses",
        ]
    }
