"""
Census-specific geographic endpoints.

Provides separate endpoints for each geographic level with GeoJSON support.
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.core.schemas import JobResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/census", tags=["census-geography"])


class CensusStateRequest(BaseModel):
    """Request to ingest Census data at state level."""
    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    year: int = Field(..., description="Survey year", ge=2010, le=2023)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    include_geojson: bool = Field(default=True, description="Include GeoJSON boundaries")


class CensusCountyRequest(BaseModel):
    """Request to ingest Census data at county level."""
    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    year: int = Field(..., description="Survey year", ge=2010, le=2023)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    state_fips: Optional[str] = Field(None, description="Filter to specific state FIPS code")
    include_geojson: bool = Field(default=True, description="Include GeoJSON boundaries")


class CensusTractRequest(BaseModel):
    """Request to ingest Census data at tract level."""
    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    year: int = Field(..., description="Survey year", ge=2010, le=2023)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    state_fips: str = Field(..., description="Required: State FIPS code (e.g., '06' for California)")
    county_fips: Optional[str] = Field(None, description="Optional: County FIPS code")
    include_geojson: bool = Field(default=True, description="Include GeoJSON boundaries")


class CensusZipRequest(BaseModel):
    """Request to ingest Census data at ZCTA (ZIP) level."""
    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    year: int = Field(..., description="Survey year", ge=2010, le=2023)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    state_fips: Optional[str] = Field(None, description="Filter to specific state FIPS code")
    include_geojson: bool = Field(default=True, description="Include GeoJSON boundaries")


@router.post("/state", response_model=JobResponse, status_code=201)
async def ingest_state_data(
    request: CensusStateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> JobResponse:
    """
    Ingest Census data at STATE level.
    
    Returns data for all 50 states + DC + Puerto Rico.
    Optionally includes GeoJSON state boundaries.
    """
    from app.api.v1.jobs import run_ingestion_job
    
    config = {
        "survey": request.survey,
        "year": request.year,
        "table_id": request.table_id,
        "geo_level": "state",
        "include_geojson": request.include_geojson
    }
    
    job = IngestionJob(
        source="census",
        status=JobStatus.PENDING,
        config=config
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    logger.info(f"Created STATE-level job {job.id} for {request.table_id}")
    
    background_tasks.add_task(run_ingestion_job, job.id, "census", config)
    
    return JobResponse.model_validate(job)


@router.post("/county", response_model=JobResponse, status_code=201)
async def ingest_county_data(
    request: CensusCountyRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> JobResponse:
    """
    Ingest Census data at COUNTY level.
    
    Returns data for all counties in US or filtered by state.
    Optionally includes GeoJSON county boundaries.
    """
    from app.api.v1.jobs import run_ingestion_job
    
    config = {
        "survey": request.survey,
        "year": request.year,
        "table_id": request.table_id,
        "geo_level": "county",
        "include_geojson": request.include_geojson
    }
    
    if request.state_fips:
        config["geo_filter"] = {"state": request.state_fips}
    
    job = IngestionJob(
        source="census",
        status=JobStatus.PENDING,
        config=config
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    logger.info(f"Created COUNTY-level job {job.id} for {request.table_id}")
    
    background_tasks.add_task(run_ingestion_job, job.id, "census", config)
    
    return JobResponse.model_validate(job)


@router.post("/tract", response_model=JobResponse, status_code=201)
async def ingest_tract_data(
    request: CensusTractRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> JobResponse:
    """
    Ingest Census data at TRACT level.
    
    Requires state_fips. Optionally filter by county.
    Tracts are small areas (~4,000 people) used for detailed analysis.
    Optionally includes GeoJSON tract boundaries.
    """
    from app.api.v1.jobs import run_ingestion_job
    
    geo_filter = {"state": request.state_fips}
    if request.county_fips:
        geo_filter["county"] = request.county_fips
    
    config = {
        "survey": request.survey,
        "year": request.year,
        "table_id": request.table_id,
        "geo_level": "tract",
        "geo_filter": geo_filter,
        "include_geojson": request.include_geojson
    }
    
    job = IngestionJob(
        source="census",
        status=JobStatus.PENDING,
        config=config
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    logger.info(f"Created TRACT-level job {job.id} for {request.table_id}")
    
    background_tasks.add_task(run_ingestion_job, job.id, "census", config)
    
    return JobResponse.model_validate(job)


@router.post("/zip", response_model=JobResponse, status_code=201)
async def ingest_zip_data(
    request: CensusZipRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> JobResponse:
    """
    Ingest Census data at ZCTA (ZIP Code Tabulation Area) level.
    
    ZCTAs are Census approximations of ZIP codes.
    Optionally filter by state.
    Optionally includes GeoJSON ZCTA boundaries.
    """
    from app.api.v1.jobs import run_ingestion_job
    
    config = {
        "survey": request.survey,
        "year": request.year,
        "table_id": request.table_id,
        "geo_level": "zip code tabulation area",
        "include_geojson": request.include_geojson
    }
    
    if request.state_fips:
        config["geo_filter"] = {"state": request.state_fips}
    
    job = IngestionJob(
        source="census",
        status=JobStatus.PENDING,
        config=config
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    logger.info(f"Created ZCTA-level job {job.id} for {request.table_id}")
    
    background_tasks.add_task(run_ingestion_job, job.id, "census", config)
    
    return JobResponse.model_validate(job)




