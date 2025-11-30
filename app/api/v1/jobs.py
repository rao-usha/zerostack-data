"""
Job management endpoints.
"""
import logging
from typing import List
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.core.schemas import JobCreate, JobResponse
from app.core.config import get_settings, MissingCensusAPIKeyError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])


async def run_ingestion_job(job_id: int, source: str, config: dict):
    """
    Background task to run ingestion job.
    """
    from datetime import datetime
    from app.core.database import get_session_factory
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    
    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if not job:
            logger.error(f"Job {job_id} not found")
            return
        
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        db.commit()
        
        # Route to appropriate source adapter
        if source == "census":
            # Validate Census API key is present
            settings = get_settings()
            try:
                settings.require_census_api_key()
            except MissingCensusAPIKeyError as e:
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.completed_at = datetime.utcnow()
                db.commit()
                return
            
            # Call Census ingestion
            from app.sources.census.ingest import ingest_acs_table
            
            # Extract config parameters
            survey = config.get("survey", "acs5")
            year = config.get("year")
            table_id = config.get("table_id")
            geo_level = config.get("geo_level", "state")
            geo_filter = config.get("geo_filter")
            
            # Validate required parameters
            if not year or not table_id:
                job.status = JobStatus.FAILED
                job.error_message = "Missing required config: 'year' and 'table_id' are required"
                job.completed_at = datetime.utcnow()
                db.commit()
                return
            
            # Run ingestion
            try:
                include_geojson = config.get("include_geojson", False)
                result = await ingest_acs_table(
                    db=db,
                    job_id=job_id,
                    survey=survey,
                    year=year,
                    table_id=table_id,
                    geo_level=geo_level,
                    geo_filter=geo_filter,
                    include_geojson=include_geojson
                )
                
                # Update job with success
                job.status = JobStatus.SUCCESS
                job.rows_inserted = result.get("rows_inserted", 0)
                job.completed_at = datetime.utcnow()
                db.commit()
                
                logger.info(f"Job {job_id} completed successfully: {result}")
            
            except Exception as e:
                logger.exception(f"Error during Census ingestion for job {job_id}")
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.error_details = {"error_type": type(e).__name__}
                job.completed_at = datetime.utcnow()
                db.commit()
        
        elif source == "public_lp_strategies":
            # Call public_lp_strategies ingestion
            from app.sources.public_lp_strategies.ingest import ingest_lp_strategy_document
            from app.sources.public_lp_strategies.types import (
                LpDocumentInput,
                DocumentTextSectionInput,
                StrategySnapshotInput,
                AssetClassAllocationInput,
                AssetClassProjectionInput,
                ThematicTagInput,
            )
            
            # Extract config parameters
            lp_name = config.get("lp_name")
            program = config.get("program")
            fiscal_year = config.get("fiscal_year")
            fiscal_quarter = config.get("fiscal_quarter")
            document_metadata = config.get("document_metadata", {})
            parsed_sections = config.get("parsed_sections", [])
            extracted_strategy = config.get("extracted_strategy", {})
            
            # Validate required parameters
            if not all([lp_name, program, fiscal_year, fiscal_quarter]):
                job.status = JobStatus.FAILED
                job.error_message = "Missing required config: 'lp_name', 'program', 'fiscal_year', 'fiscal_quarter'"
                job.completed_at = datetime.utcnow()
                db.commit()
                return
            
            try:
                # Parse inputs
                document_input = LpDocumentInput(
                    lp_id=0,  # Will be set by ingest function
                    **document_metadata
                )
                
                text_sections = [
                    DocumentTextSectionInput(**section)
                    for section in parsed_sections
                ]
                
                strategy_input = StrategySnapshotInput(
                    lp_id=0,  # Will be set by ingest function
                    program=program,
                    fiscal_year=fiscal_year,
                    fiscal_quarter=fiscal_quarter,
                    **extracted_strategy.get("strategy", {})
                )
                
                allocations = [
                    AssetClassAllocationInput(**alloc)
                    for alloc in extracted_strategy.get("allocations", [])
                ]
                
                projections = [
                    AssetClassProjectionInput(**proj)
                    for proj in extracted_strategy.get("projections", [])
                ]
                
                thematic_tags = [
                    ThematicTagInput(**tag)
                    for tag in extracted_strategy.get("thematic_tags", [])
                ]
                
                # Run ingestion
                result = ingest_lp_strategy_document(
                    db=db,
                    lp_name=lp_name,
                    document_input=document_input,
                    text_sections=text_sections,
                    strategy_input=strategy_input,
                    allocations=allocations,
                    projections=projections,
                    thematic_tags=thematic_tags,
                )
                
                # Update job with success
                job.status = JobStatus.SUCCESS
                job.rows_inserted = result.get("sections_count", 0) + result.get("allocations_count", 0)
                job.completed_at = datetime.utcnow()
                db.commit()
                
                logger.info(f"Job {job_id} completed successfully: {result}")
            
            except Exception as e:
                logger.exception(f"Error during public_lp_strategies ingestion for job {job_id}")
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.error_details = {"error_type": type(e).__name__}
                job.completed_at = datetime.utcnow()
                db.commit()
        
        else:
            job.status = JobStatus.FAILED
            job.error_message = f"Unknown source: {source}"
            job.completed_at = datetime.utcnow()
            db.commit()
    
    except Exception as e:
        logger.exception(f"Error running job {job_id}")
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.error_details = {"error_type": type(e).__name__}
            job.completed_at = datetime.utcnow()
            db.commit()
    
    finally:
        db.close()


@router.post("", response_model=JobResponse, status_code=201)
async def create_job(
    job_request: JobCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> JobResponse:
    """
    Create a new ingestion job.
    
    The job will run asynchronously in the background.
    """
    # Validate source
    valid_sources = ["census", "public_lp_strategies"]  # Add more as they're implemented
    if job_request.source not in valid_sources:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid source. Must be one of: {valid_sources}"
        )
    
    # Create job record
    job = IngestionJob(
        source=job_request.source,
        status=JobStatus.PENDING,
        config=job_request.config
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    
    logger.info(f"Created job {job.id} for source {job.source}")
    
    # Start background ingestion
    background_tasks.add_task(
        run_ingestion_job,
        job.id,
        job.source,
        job.config
    )
    
    return JobResponse.model_validate(job)


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: int, db: Session = Depends(get_db)) -> JobResponse:
    """
    Get status and details of a specific job.
    """
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JobResponse.model_validate(job)


@router.get("", response_model=List[JobResponse])
def list_jobs(
    source: str = None,
    status: JobStatus = None,
    limit: int = 100,
    db: Session = Depends(get_db)
) -> List[JobResponse]:
    """
    List ingestion jobs with optional filtering.
    """
    query = db.query(IngestionJob)
    
    if source:
        query = query.filter(IngestionJob.source == source)
    if status:
        query = query.filter(IngestionJob.status == status)
    
    query = query.order_by(IngestionJob.created_at.desc()).limit(limit)
    
    jobs = query.all()
    return [JobResponse.model_validate(job) for job in jobs]

