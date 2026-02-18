"""
Real Estate / Housing API routes.

Provides HTTP endpoints for ingesting real estate data from:
- FHFA House Price Index
- HUD Building Permits & Housing Starts
- Redfin Housing Market Data
- OpenStreetMap Building Footprints
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job

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
        ]
    }
