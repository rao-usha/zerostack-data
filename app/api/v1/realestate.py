"""
Real Estate / Housing API routes.

Provides HTTP endpoints for ingesting real estate data from:
- FHFA House Price Index
- HUD Building Permits & Housing Starts
- Redfin Housing Market Data
- OpenStreetMap Building Footprints

Also provides query endpoints for:
- Zoning districts (NZA + county-level data)
- Land use / land cover (NJDEP statewide data)
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job
from app.core.models_site_intel import ZoningDistrict, LandUseParcel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/realestate", tags=["Real Estate / Housing"])


# Request models
class FHFAIngestRequest(BaseModel):
    """Request model for FHFA House Price Index ingestion."""

    geography_type: Optional[str] = Field(
        None, description="Geography type filter: National, State, MSA, ZIP3"
    )
    start_date: Optional[str] = Field(
        None, description="Start date in YYYY-MM-DD format"
    )
    end_date: Optional[str] = Field(None, description="End date in YYYY-MM-DD format")


class HUDIngestRequest(BaseModel):
    """Request model for HUD Permits & Starts ingestion."""

    geography_type: str = Field(
        "National", description="Geography type: National, State, MSA, County"
    )
    geography_id: Optional[str] = Field(
        None, description="Geography identifier (state FIPS, MSA code, etc.)"
    )
    start_date: Optional[str] = Field(
        None, description="Start date in YYYY-MM-DD format"
    )
    end_date: Optional[str] = Field(None, description="End date in YYYY-MM-DD format")


class RedfinIngestRequest(BaseModel):
    """Request model for Redfin housing data ingestion."""

    region_type: str = Field(
        "zip", description="Region type: zip, city, neighborhood, metro"
    )
    property_type: str = Field("All Residential", description="Property type filter")


class OSMIngestRequest(BaseModel):
    """Request model for OpenStreetMap buildings ingestion."""

    bounding_box: List[float] = Field(
        ...,
        description="Bounding box as [south, west, north, east]",
        min_length=4,
        max_length=4,
    )
    building_type: Optional[str] = Field(
        None, description="Building type filter: residential, commercial, etc."
    )
    limit: int = Field(
        10000, description="Maximum number of buildings to fetch", ge=1, le=50000
    )


# FHFA endpoints
@router.post("/fhfa/ingest")
async def ingest_fhfa_hpi(
    request: FHFAIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest FHFA House Price Index data.

    The FHFA House Price Index tracks changes in single-family home values
    across the United States. Data is available at multiple geographic levels.

    **Data Source:** Federal Housing Finance Agency
    **Update Frequency:** Quarterly
    **Geographic Levels:** National, State, MSA, ZIP3
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="realestate",
        config={
            "geography_type": request.geography_type,
            "start_date": request.start_date,
            "end_date": request.end_date,
        },
        message="FHFA HPI ingestion started",
    )


# HUD endpoints
@router.post("/hud/ingest")
async def ingest_hud_permits(
    request: HUDIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest HUD Building Permits and Housing Starts data.

    **Data Source:** U.S. Department of Housing and Urban Development
    **Update Frequency:** Monthly
    **Geographic Levels:** National, State, MSA, County
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="realestate",
        config={
            "dataset": "hud_permits",
            "geography_type": request.geography_type,
            "geography_id": request.geography_id,
            "start_date": request.start_date,
            "end_date": request.end_date,
        },
        message="HUD permits ingestion started",
    )


# Redfin endpoints
@router.post("/redfin/ingest")
async def ingest_redfin_data(
    request: RedfinIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest Redfin housing market data.

    **Data Source:** Redfin Data Center
    **Update Frequency:** Weekly
    **Geographic Levels:** ZIP, City, Neighborhood, Metro
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="realestate",
        config={
            "dataset": "redfin",
            "region_type": request.region_type,
            "property_type": request.property_type,
        },
        message="Redfin data ingestion started",
    )


# OpenStreetMap endpoints
@router.post("/osm/ingest")
async def ingest_osm_buildings(
    request: OSMIngestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest OpenStreetMap building footprints.

    **Data Source:** OpenStreetMap via Overpass API
    **Update Frequency:** Real-time
    **Geographic Scope:** Global (query by bounding box)

    **Important:** Keep bounding boxes small to avoid timeouts.
    """
    if len(request.bounding_box) != 4:
        raise HTTPException(
            status_code=400,
            detail="Bounding box must have exactly 4 coordinates [south, west, north, east]",
        )

    south, west, north, east = request.bounding_box

    if not (-90 <= south <= 90 and -90 <= north <= 90):
        raise HTTPException(status_code=400, detail="Invalid latitude values")
    if not (-180 <= west <= 180 and -180 <= east <= 180):
        raise HTTPException(status_code=400, detail="Invalid longitude values")
    if south >= north:
        raise HTTPException(status_code=400, detail="South must be less than north")
    if west >= east:
        raise HTTPException(status_code=400, detail="West must be less than east")

    return create_and_dispatch_job(
        db,
        background_tasks,
        source="realestate",
        config={
            "dataset": "osm_buildings",
            "bounding_box": request.bounding_box,
            "building_type": request.building_type,
            "limit": request.limit,
        },
        message="OSM buildings ingestion started",
    )


# General info endpoint
@router.get("/info")
async def get_realestate_info():
    """
    Get information about available real estate data sources.
    """
    return {
        "sources": [
            {
                "id": "fhfa_hpi",
                "name": "FHFA House Price Index",
                "description": "Quarterly house price indices tracking single-family home values",
                "provider": "Federal Housing Finance Agency",
                "update_frequency": "Quarterly",
                "geographic_levels": ["National", "State", "MSA", "ZIP3"],
                "api_endpoint": "/api/v1/realestate/fhfa/ingest",
                "documentation": "https://www.fhfa.gov/DataTools/Downloads/Pages/House-Price-Index-Datasets.aspx",
            },
            {
                "id": "hud_permits",
                "name": "HUD Building Permits & Housing Starts",
                "description": "Monthly data on building permits, housing starts, and completions",
                "provider": "U.S. Department of Housing and Urban Development",
                "update_frequency": "Monthly",
                "geographic_levels": ["National", "State", "MSA", "County"],
                "api_endpoint": "/api/v1/realestate/hud/ingest",
                "documentation": "https://www.huduser.gov/portal/datasets/socds.html",
            },
            {
                "id": "redfin",
                "name": "Redfin Housing Market Data",
                "description": "Weekly housing market metrics including prices, inventory, and days on market",
                "provider": "Redfin",
                "update_frequency": "Weekly",
                "geographic_levels": ["ZIP", "City", "Neighborhood", "Metro"],
                "api_endpoint": "/api/v1/realestate/redfin/ingest",
                "documentation": "https://www.redfin.com/news/data-center/",
            },
            {
                "id": "osm_buildings",
                "name": "OpenStreetMap Building Footprints",
                "description": "Building footprints with location, type, and characteristics",
                "provider": "OpenStreetMap",
                "update_frequency": "Real-time",
                "geographic_scope": "Global (query by bounding box)",
                "api_endpoint": "/api/v1/realestate/osm/ingest",
                "documentation": "https://wiki.openstreetmap.org/wiki/Overpass_API",
            },
            {
                "id": "zoning_districts",
                "name": "Zoning Districts",
                "description": "Municipal zoning districts with allowed uses and development constraints",
                "provider": "National Zoning Atlas, NJ County GIS",
                "update_frequency": "Annual",
                "geographic_scope": "NJ + 6 NZA states",
                "api_endpoint": "/api/v1/realestate/zoning/districts",
                "documentation": "https://www.zoningatlas.org/",
            },
            {
                "id": "land_use",
                "name": "Land Use / Land Cover",
                "description": "County-level land use aggregation showing industrial, commercial, residential acreage",
                "provider": "NJ DEP",
                "update_frequency": "Decennial",
                "geographic_scope": "New Jersey (21 counties)",
                "api_endpoint": "/api/v1/realestate/land-use/summary",
                "documentation": "https://gisdata-njdep.opendata.arcgis.com/",
            },
        ]
    }


# =============================================================================
# ZONING DISTRICT ENDPOINTS
# =============================================================================


@router.get("/zoning/districts")
async def get_zoning_districts(
    state: Optional[str] = Query(None, description="State filter (e.g., NJ)"),
    jurisdiction: Optional[str] = Query(None, description="Jurisdiction filter"),
    category: Optional[str] = Query(
        None,
        description="Zone category: industrial, commercial, residential, mixed, agricultural, other",
    ),
    allows_data_center: Optional[bool] = Query(None, description="Filter to DC-eligible zones"),
    allows_manufacturing: Optional[bool] = Query(None, description="Filter to manufacturing-eligible zones"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Query zoning districts with filters.

    Returns zoning districts with allowed uses and development constraints.
    Sources: National Zoning Atlas, NJ county GIS data.
    """
    query = db.query(ZoningDistrict)

    if state:
        query = query.filter(ZoningDistrict.state == state.upper())
    if jurisdiction:
        query = query.filter(ZoningDistrict.jurisdiction.ilike(f"%{jurisdiction}%"))
    if category:
        query = query.filter(ZoningDistrict.zone_category == category.lower())
    if allows_data_center is not None:
        query = query.filter(ZoningDistrict.allows_data_center == allows_data_center)
    if allows_manufacturing is not None:
        query = query.filter(ZoningDistrict.allows_manufacturing == allows_manufacturing)

    total = query.count()
    districts = query.order_by(ZoningDistrict.state, ZoningDistrict.jurisdiction).offset(offset).limit(limit).all()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "districts": [
            {
                "id": d.id,
                "state": d.state,
                "jurisdiction": d.jurisdiction,
                "zone_code": d.zone_code,
                "zone_name": d.zone_name,
                "zone_category": d.zone_category,
                "allows_manufacturing": d.allows_manufacturing,
                "allows_warehouse": d.allows_warehouse,
                "allows_data_center": d.allows_data_center,
                "max_height_ft": d.max_height_ft,
                "max_far": float(d.max_far) if d.max_far else None,
                "min_lot_sqft": d.min_lot_sqft,
                "source": d.source,
                "collected_at": d.collected_at.isoformat() if d.collected_at else None,
            }
            for d in districts
        ],
    }


@router.get("/zoning/summary")
async def get_zoning_summary(
    state: Optional[str] = Query(None, description="State filter"),
    db: Session = Depends(get_db),
):
    """
    Get zoning summary statistics.

    Returns district counts by state, category, and allowed uses.
    """
    base = db.query(ZoningDistrict)
    if state:
        base = base.filter(ZoningDistrict.state == state.upper())

    total = base.count()

    # By state
    by_state = (
        base.with_entities(
            ZoningDistrict.state,
            func.count().label("count"),
            func.count().filter(ZoningDistrict.allows_data_center.is_(True)).label("dc_eligible"),
            func.count().filter(ZoningDistrict.allows_manufacturing.is_(True)).label("mfg_eligible"),
        )
        .group_by(ZoningDistrict.state)
        .order_by(func.count().desc())
        .all()
    )

    # By category
    by_category = (
        base.with_entities(
            ZoningDistrict.zone_category,
            func.count().label("count"),
        )
        .group_by(ZoningDistrict.zone_category)
        .order_by(func.count().desc())
        .all()
    )

    return {
        "total_districts": total,
        "by_state": [
            {
                "state": s.state,
                "districts": s.count,
                "dc_eligible": s.dc_eligible,
                "mfg_eligible": s.mfg_eligible,
            }
            for s in by_state
        ],
        "by_category": [
            {"category": c.zone_category or "unknown", "count": c.count}
            for c in by_category
        ],
    }


@router.get("/zoning/dc-eligible")
async def get_dc_eligible_zones(
    state: Optional[str] = Query(None, description="State filter"),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Get zoning districts that allow data center development.

    Filters to zones classified as industrial or mixed-use where
    data centers are permitted.
    """
    query = db.query(ZoningDistrict).filter(
        ZoningDistrict.allows_data_center.is_(True)
    )

    if state:
        query = query.filter(ZoningDistrict.state == state.upper())

    total = query.count()
    districts = query.order_by(ZoningDistrict.state, ZoningDistrict.jurisdiction).limit(limit).all()

    return {
        "total_dc_eligible": total,
        "districts": [
            {
                "id": d.id,
                "state": d.state,
                "jurisdiction": d.jurisdiction,
                "zone_code": d.zone_code,
                "zone_name": d.zone_name,
                "zone_category": d.zone_category,
                "max_height_ft": d.max_height_ft,
                "max_far": float(d.max_far) if d.max_far else None,
                "source": d.source,
            }
            for d in districts
        ],
    }


# =============================================================================
# LAND USE ENDPOINTS
# =============================================================================


@router.get("/land-use/summary")
async def get_land_use_summary(
    state: Optional[str] = Query(None, description="State filter (e.g., NJ)"),
    county: Optional[str] = Query(None, description="County name filter"),
    county_fips: Optional[str] = Query(None, description="County FIPS code filter"),
    db: Session = Depends(get_db),
):
    """
    Get land use summary by county.

    Returns acres per land use category aggregated at county level.
    Useful for identifying counties with available industrial/commercial land.
    """
    base = db.query(LandUseParcel)
    if state:
        base = base.filter(LandUseParcel.state == state.upper())
    if county:
        base = base.filter(LandUseParcel.county.ilike(f"%{county}%"))
    if county_fips:
        base = base.filter(LandUseParcel.county_fips == county_fips)

    # Aggregate by county + category
    results = (
        base.with_entities(
            LandUseParcel.state,
            LandUseParcel.county,
            LandUseParcel.county_fips,
            LandUseParcel.land_use_category,
            func.sum(LandUseParcel.acres).label("total_acres"),
            func.sum(LandUseParcel.polygon_count).label("total_polygons"),
            func.sum(LandUseParcel.impervious_acres).label("total_impervious_acres"),
        )
        .group_by(
            LandUseParcel.state,
            LandUseParcel.county,
            LandUseParcel.county_fips,
            LandUseParcel.land_use_category,
        )
        .order_by(LandUseParcel.county, LandUseParcel.land_use_category)
        .all()
    )

    # Group by county
    counties = {}
    for r in results:
        key = f"{r.state}_{r.county}"
        if key not in counties:
            counties[key] = {
                "state": r.state,
                "county": r.county,
                "county_fips": r.county_fips,
                "categories": {},
                "total_acres": 0,
            }
        acres = float(r.total_acres) if r.total_acres else 0
        counties[key]["categories"][r.land_use_category] = {
            "acres": round(acres, 1),
            "polygons": int(r.total_polygons) if r.total_polygons else 0,
            "impervious_acres": round(float(r.total_impervious_acres), 1) if r.total_impervious_acres else 0,
        }
        counties[key]["total_acres"] += acres

    # Sort by industrial acres descending for site selection
    county_list = sorted(
        counties.values(),
        key=lambda c: c["categories"].get("industrial", {}).get("acres", 0),
        reverse=True,
    )

    # Add industrial % to each county
    for c in county_list:
        c["total_acres"] = round(c["total_acres"], 1)
        ind_acres = c["categories"].get("industrial", {}).get("acres", 0)
        c["industrial_pct"] = round(ind_acres / c["total_acres"] * 100, 1) if c["total_acres"] > 0 else 0

    return {
        "total_counties": len(county_list),
        "counties": county_list,
    }


@router.get("/land-use/industrial-ranking")
async def get_industrial_land_ranking(
    state: Optional[str] = Query(None, description="State filter"),
    limit: int = Query(25, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Rank counties by available industrial land.

    Returns counties sorted by industrial acreage, useful for
    identifying areas with existing industrial infrastructure.
    """
    base = db.query(LandUseParcel).filter(
        LandUseParcel.land_use_category == "industrial"
    )
    if state:
        base = base.filter(LandUseParcel.state == state.upper())

    results = (
        base.with_entities(
            LandUseParcel.state,
            LandUseParcel.county,
            LandUseParcel.county_fips,
            func.sum(LandUseParcel.acres).label("industrial_acres"),
            func.sum(LandUseParcel.polygon_count).label("industrial_sites"),
        )
        .group_by(
            LandUseParcel.state,
            LandUseParcel.county,
            LandUseParcel.county_fips,
        )
        .order_by(func.sum(LandUseParcel.acres).desc())
        .limit(limit)
        .all()
    )

    return {
        "ranking": [
            {
                "rank": i + 1,
                "state": r.state,
                "county": r.county,
                "county_fips": r.county_fips,
                "industrial_acres": round(float(r.industrial_acres), 1) if r.industrial_acres else 0,
                "industrial_sites": int(r.industrial_sites) if r.industrial_sites else 0,
            }
            for i, r in enumerate(results)
        ],
    }


@router.post("/land-use/collect-nj")
async def collect_nj_land_use(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Trigger NJ DEP Land Use / Land Cover data collection.

    Collects land use data for all 21 NJ counties from the NJDEP
    ArcGIS REST API using server-side aggregation.
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="site_intel",
        config={
            "sources": ["njdep_lulc"],
        },
        message="NJ DEP land use collection started",
    )


@router.post("/zoning/collect-nj")
async def collect_nj_zoning(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Trigger NJ county zoning district collection.

    Collects zoning district boundaries from NJ county ArcGIS endpoints
    (currently Sussex County, 2,478 districts).
    """
    async def _run_nj_zoning():
        from app.core.database import get_db as _get_db
        from app.sources.site_intel.incentives.nj_county_zoning_collector import (
            NJCountyZoningCollector,
        )
        from app.sources.site_intel.types import CollectionConfig, SiteIntelDomain, SiteIntelSource

        db_gen = _get_db()
        session = next(db_gen)
        try:
            collector = NJCountyZoningCollector(session)
            config = CollectionConfig(
                domain=SiteIntelDomain.INCENTIVES,
                source=SiteIntelSource.NJDEP_LULC,
            )
            result = await collector.collect(config)
            logger.info(f"NJ zoning collection result: {result.status}, {result.inserted_items} inserted")
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    background_tasks.add_task(_run_nj_zoning)
    return {"status": "started", "message": "NJ county zoning collection started"}
