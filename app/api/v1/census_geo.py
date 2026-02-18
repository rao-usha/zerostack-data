"""
Census-specific geographic endpoints.

Provides separate endpoints for each geographic level with GeoJSON support.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.job_helpers import create_and_dispatch_job

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/census", tags=["census-geography"])


class CensusStateRequest(BaseModel):
    """Request to ingest Census data at state level."""

    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    year: int = Field(..., description="Survey year", ge=2010, le=2023)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    include_geojson: bool = Field(
        default=True, description="Include GeoJSON boundaries"
    )


class CensusCountyRequest(BaseModel):
    """Request to ingest Census data at county level."""

    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    year: int = Field(..., description="Survey year", ge=2010, le=2023)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    state_fips: Optional[str] = Field(
        None, description="Filter to specific state FIPS code"
    )
    include_geojson: bool = Field(
        default=True, description="Include GeoJSON boundaries"
    )


class CensusTractRequest(BaseModel):
    """Request to ingest Census data at tract level."""

    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    year: int = Field(..., description="Survey year", ge=2010, le=2023)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    state_fips: str = Field(
        ..., description="Required: State FIPS code (e.g., '06' for California)"
    )
    county_fips: Optional[str] = Field(None, description="Optional: County FIPS code")
    include_geojson: bool = Field(
        default=True, description="Include GeoJSON boundaries"
    )


class CensusZipRequest(BaseModel):
    """Request to ingest Census data at ZCTA (ZIP) level."""

    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    year: int = Field(..., description="Survey year", ge=2010, le=2023)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    state_fips: Optional[str] = Field(
        None, description="Filter to specific state FIPS code"
    )
    include_geojson: bool = Field(
        default=True, description="Include GeoJSON boundaries"
    )


@router.post("/state", status_code=201)
async def ingest_state_data(
    request: CensusStateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest Census data at STATE level.

    Returns data for all 50 states + DC + Puerto Rico.
    Optionally includes GeoJSON state boundaries.
    """
    return create_and_dispatch_job(
        db,
        background_tasks,
        source="census",
        config={
            "survey": request.survey,
            "year": request.year,
            "table_id": request.table_id,
            "geo_level": "state",
            "include_geojson": request.include_geojson,
        },
        message=f"Census STATE-level ingestion job created for {request.table_id}",
    )


@router.post("/county", status_code=201)
async def ingest_county_data(
    request: CensusCountyRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest Census data at COUNTY level.

    Returns data for all counties in US or filtered by state.
    Optionally includes GeoJSON county boundaries.
    """
    config = {
        "survey": request.survey,
        "year": request.year,
        "table_id": request.table_id,
        "geo_level": "county",
        "include_geojson": request.include_geojson,
    }
    if request.state_fips:
        config["geo_filter"] = {"state": request.state_fips}

    return create_and_dispatch_job(
        db,
        background_tasks,
        source="census",
        config=config,
        message=f"Census COUNTY-level ingestion job created for {request.table_id}",
    )


@router.post("/tract", status_code=201)
async def ingest_tract_data(
    request: CensusTractRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest Census data at TRACT level.

    Requires state_fips. Optionally filter by county.
    Tracts are small areas (~4,000 people) used for detailed analysis.
    Optionally includes GeoJSON tract boundaries.
    """
    geo_filter = {"state": request.state_fips}
    if request.county_fips:
        geo_filter["county"] = request.county_fips

    return create_and_dispatch_job(
        db,
        background_tasks,
        source="census",
        config={
            "survey": request.survey,
            "year": request.year,
            "table_id": request.table_id,
            "geo_level": "tract",
            "geo_filter": geo_filter,
            "include_geojson": request.include_geojson,
        },
        message=f"Census TRACT-level ingestion job created for {request.table_id}",
    )


@router.post("/zip", status_code=201)
async def ingest_zip_data(
    request: CensusZipRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Ingest Census data at ZCTA (ZIP Code Tabulation Area) level.

    ZCTAs are Census approximations of ZIP codes.
    Optionally filter by state.
    Optionally includes GeoJSON ZCTA boundaries.
    """
    config = {
        "survey": request.survey,
        "year": request.year,
        "table_id": request.table_id,
        "geo_level": "zip code tabulation area",
        "include_geojson": request.include_geojson,
    }
    if request.state_fips:
        config["geo_filter"] = {"state": request.state_fips}

    return create_and_dispatch_job(
        db,
        background_tasks,
        source="census",
        config=config,
        message=f"Census ZCTA-level ingestion job created for {request.table_id}",
    )
