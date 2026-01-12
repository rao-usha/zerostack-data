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


# =============================================================================
# Retry Endpoints
# =============================================================================

@router.get("/failed/summary")
def get_failed_jobs_summary(db: Session = Depends(get_db)):
    """
    Get summary of failed jobs by source.

    Returns counts of retryable vs exhausted jobs.
    """
    from app.core.retry_service import get_failed_jobs_summary as get_summary
    return get_summary(db)


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_job(
    job_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> JobResponse:
    """
    Retry a failed job.

    The job must be in FAILED status and have retries remaining.
    """
    from app.core.retry_service import mark_job_for_immediate_retry

    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.FAILED:
        raise HTTPException(
            status_code=400,
            detail=f"Job is not in failed status (current: {job.status.value})"
        )

    if not job.can_retry:
        raise HTTPException(
            status_code=400,
            detail=f"Job has exhausted all retries ({job.retry_count}/{job.max_retries})"
        )

    # Mark job for retry
    updated_job = mark_job_for_immediate_retry(db, job_id)
    if not updated_job:
        raise HTTPException(status_code=500, detail="Failed to mark job for retry")

    # Start background ingestion
    background_tasks.add_task(
        run_ingestion_job,
        updated_job.id,
        updated_job.source,
        updated_job.config
    )

    logger.info(f"Retrying job {job_id} (attempt {updated_job.retry_count}/{updated_job.max_retries})")

    return JobResponse.model_validate(updated_job)


@router.post("/retry/all")
async def retry_all_failed_jobs(
    background_tasks: BackgroundTasks,
    source: str = None,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """
    Retry all eligible failed jobs.

    Args:
        source: Optional filter by source (e.g., "fred", "sec")
        limit: Maximum number of jobs to retry (default 10)

    Returns summary of retry operations.
    """
    from app.core.retry_service import retry_all_eligible_jobs

    results = retry_all_eligible_jobs(db, source=source, limit=limit)

    # Schedule background tasks for retried jobs
    for job_info in results["retried"]:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_info["job_id"]).first()
        if job:
            background_tasks.add_task(
                run_ingestion_job,
                job.id,
                job.source,
                job.config
            )

    return {
        "message": f"Scheduled {len(results['retried'])} jobs for retry",
        **results
    }


# =============================================================================
# Data Quality Validation Endpoints
# =============================================================================

@router.get("/{job_id}/validate")
def validate_job_data(
    job_id: int,
    table_name: str,
    expected_min_rows: int = 1,
    db: Session = Depends(get_db)
):
    """
    Validate data quality for a completed ingestion job.

    Performs checks including:
    - Row count validation
    - Null value detection
    - Duplicate detection
    - Range validation for numeric fields

    Args:
        job_id: The ingestion job ID
        table_name: The table that was populated
        expected_min_rows: Minimum expected row count (default 1)

    Returns:
        Validation results with pass/fail status for each check
    """
    from app.core.data_quality import validate_ingestion_job, get_default_validation_config

    # Get job to determine source
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.SUCCESS:
        raise HTTPException(
            status_code=400,
            detail=f"Job is not in success status (current: {job.status.value})"
        )

    # Get default validation config for source
    dataset = job.config.get("dataset") if job.config else None
    default_config = get_default_validation_config(job.source, dataset)

    # Override with provided parameters
    validation_config = {
        **default_config,
        "expected_min_rows": expected_min_rows
    }

    try:
        results = validate_ingestion_job(
            db=db,
            job_id=job_id,
            table_name=table_name,
            validation_config=validation_config
        )
        return results
    except Exception as e:
        logger.error(f"Data validation failed for job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


# =============================================================================
# Monitoring Endpoints
# =============================================================================

@router.get("/monitoring/metrics")
def get_job_metrics(
    hours: int = 24,
    source: str = None,
    db: Session = Depends(get_db)
):
    """
    Get job metrics for monitoring.

    Returns success/failure rates, durations, and recent failures.

    Args:
        hours: Time window in hours (default 24)
        source: Optional filter by source
    """
    from app.core.monitoring import JobMonitor

    monitor = JobMonitor(db)
    return monitor.get_job_metrics(hours=hours, source=source)


@router.get("/monitoring/health")
def get_source_health(db: Session = Depends(get_db)):
    """
    Get health status for each data source.

    Returns health scores based on recent job success rates.
    Sources are classified as:
    - healthy: 100% success rate
    - warning: Some failures but majority success
    - degraded: >50% failure rate
    - critical: 0% success rate
    """
    from app.core.monitoring import JobMonitor

    monitor = JobMonitor(db)
    return monitor.get_source_health()


@router.get("/monitoring/alerts")
def get_active_alerts(
    failure_threshold: int = 3,
    time_window_hours: int = 1,
    db: Session = Depends(get_db)
):
    """
    Get active alerts for job failures.

    Alert types:
    - high_failure_rate: Multiple failures for a source
    - stuck_job: Job running longer than 2 hours
    - data_staleness: No jobs for 24+ hours

    Args:
        failure_threshold: Number of failures to trigger alert (default 3)
        time_window_hours: Time window for failure count (default 1)
    """
    from app.core.monitoring import JobMonitor

    monitor = JobMonitor(db)
    return monitor.check_alerts(
        failure_threshold=failure_threshold,
        time_window_hours=time_window_hours
    )


@router.get("/monitoring/dashboard")
def get_monitoring_dashboard(db: Session = Depends(get_db)):
    """
    Get comprehensive monitoring dashboard.

    Returns all metrics, health status, and alerts in one call.
    Includes:
    - 24h and 1h metrics
    - Source health status
    - Active alerts
    """
    from app.core.monitoring import get_monitoring_dashboard as get_dashboard

    return get_dashboard(db)
