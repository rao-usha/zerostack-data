"""
Batch Census ingestion endpoints.

Ingest multiple years or tables at once.
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.core.schemas import JobResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/census/batch", tags=["census-batch"])


class BatchStateRequest(BaseModel):
    """Request to ingest Census data at state level for multiple years."""
    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    years: List[int] = Field(..., description="List of years to ingest", min_items=1, max_items=10)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    include_geojson: bool = Field(default=False, description="Include GeoJSON boundaries")


class BatchCountyRequest(BaseModel):
    """Request to ingest Census data at county level for multiple years."""
    survey: str = Field(default="acs5", description="Survey type (acs5, acs1)")
    years: List[int] = Field(..., description="List of years to ingest", min_items=1, max_items=10)
    table_id: str = Field(..., description="Census table ID (e.g., B01001)")
    state_fips: Optional[str] = Field(None, description="Filter to specific state FIPS code")
    include_geojson: bool = Field(default=False, description="Include GeoJSON boundaries")


class BatchJobResponse(BaseModel):
    """Response for batch job creation."""
    job_ids: List[int] = Field(..., description="List of created job IDs")
    total_jobs: int = Field(..., description="Total number of jobs created")
    message: str = Field(..., description="Status message")


@router.post("/state", response_model=BatchJobResponse, status_code=201)
async def batch_ingest_state_data(
    request: BatchStateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> BatchJobResponse:
    """
    Batch ingest Census data at STATE level for multiple years.
    
    Creates separate jobs for each year. Jobs run in parallel.
    """
    from app.api.v1.jobs import run_ingestion_job
    
    job_ids = []
    
    for year in request.years:
        config = {
            "survey": request.survey,
            "year": year,
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
        
        job_ids.append(job.id)
        
        # Start background ingestion
        background_tasks.add_task(run_ingestion_job, job.id, "census", config)
        
        logger.info(f"Created STATE-level batch job {job.id} for {year}/{request.table_id}")
    
    return BatchJobResponse(
        job_ids=job_ids,
        total_jobs=len(job_ids),
        message=f"Created {len(job_ids)} ingestion jobs for years {min(request.years)}-{max(request.years)}"
    )


@router.post("/county", response_model=BatchJobResponse, status_code=201)
async def batch_ingest_county_data(
    request: BatchCountyRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> BatchJobResponse:
    """
    Batch ingest Census data at COUNTY level for multiple years.
    
    Creates separate jobs for each year. Jobs run in parallel.
    """
    from app.api.v1.jobs import run_ingestion_job
    
    job_ids = []
    
    for year in request.years:
        config = {
            "survey": request.survey,
            "year": year,
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
        
        job_ids.append(job.id)
        
        # Start background ingestion
        background_tasks.add_task(run_ingestion_job, job.id, "census", config)
        
        logger.info(f"Created COUNTY-level batch job {job.id} for {year}/{request.table_id}")
    
    return BatchJobResponse(
        job_ids=job_ids,
        total_jobs=len(job_ids),
        message=f"Created {len(job_ids)} ingestion jobs for years {min(request.years)}-{max(request.years)}"
    )


@router.get("/status", response_model=dict)
async def batch_job_status(
    job_ids: str,
    db: Session = Depends(get_db)
) -> dict:
    """
    Check status of multiple jobs.
    
    Args:
        job_ids: Comma-separated list of job IDs (e.g., "1,2,3,4,5")
    
    Returns:
        Summary of job statuses
    """
    # Parse job IDs
    try:
        job_id_list = [int(x.strip()) for x in job_ids.split(",")]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job_ids format. Use comma-separated integers.")
    
    # Fetch jobs
    jobs = db.query(IngestionJob).filter(IngestionJob.id.in_(job_id_list)).all()
    
    if not jobs:
        raise HTTPException(status_code=404, detail="No jobs found")
    
    # Count by status
    status_counts = {}
    total_rows = 0
    
    for job in jobs:
        status = job.status.value
        status_counts[status] = status_counts.get(status, 0) + 1
        if job.rows_inserted:
            total_rows += job.rows_inserted
    
    return {
        "total_jobs": len(jobs),
        "status_counts": status_counts,
        "total_rows_inserted": total_rows,
        "jobs": [
            {
                "id": job.id,
                "status": job.status.value,
                "year": job.config.get("year"),
                "rows_inserted": job.rows_inserted,
                "error_message": job.error_message
            }
            for job in jobs
        ]
    }




