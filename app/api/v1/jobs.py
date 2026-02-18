"""
Job management endpoints.
"""
import importlib
import logging
from typing import List, Dict, Tuple, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.core.schemas import JobCreate, JobResponse
from app.core.config import get_settings, MissingCensusAPIKeyError

logger = logging.getLogger(__name__)

# =============================================================================
# Universal Source Dispatch Table
# =============================================================================
# Maps source name -> (module_path, function_name, [config_keys])
# Config keys are extracted from the job config dict and passed as kwargs.
# Sources with complex config parsing (census, public_lp_strategies) are
# handled as special cases below.

SOURCE_DISPATCH: Dict[str, Tuple[str, str, List[str]]] = {
    # --- Daily ---
    "treasury": (
        "app.sources.treasury.ingest",
        "ingest_treasury_daily_balance",
        ["start_date", "end_date"],
    ),
    "fred": (
        "app.sources.fred.ingest",
        "ingest_fred_category",
        ["category", "series_ids", "observation_start", "observation_end", "api_key"],
    ),
    "prediction_markets": (
        "app.sources.prediction_markets.ingest",
        "create_job",
        ["job_type", "target_platforms", "target_categories"],
    ),
    # --- Weekly ---
    "eia": (
        "app.sources.eia.ingest",
        "ingest_eia_petroleum_data",
        ["subcategory", "route", "frequency", "start", "end", "facets", "api_key"],
    ),
    "noaa": (
        "app.sources.noaa.ingest",
        "create_noaa_table",
        ["dataset_key"],
    ),
    # --- Monthly ---
    "bls": (
        "app.sources.bls.ingest",
        "ingest_bls_dataset",
        ["dataset", "start_year", "end_year", "series_ids"],
    ),
    "bea": (
        "app.sources.bea.ingest",
        "ingest_nipa_data",
        ["table_name", "frequency", "year", "api_key"],
    ),
    "fema": (
        "app.sources.fema.ingest",
        "ingest_disaster_declarations",
        ["state", "year", "disaster_type", "max_records"],
    ),
    "fdic": (
        "app.sources.fdic.ingest",
        "ingest_bank_financials",
        ["cert", "report_date", "year", "limit"],
    ),
    "cms": (
        "app.sources.cms.ingest",
        "ingest_medicare_utilization",
        ["year", "state", "limit"],
    ),
    "fbi_crime": (
        "app.sources.fbi_crime.ingest",
        "ingest_fbi_crime_estimates",
        ["scope", "offenses", "states", "api_key"],
    ),
    "irs_soi": (
        "app.sources.irs_soi.ingest",
        "ingest_zip_income_data",
        ["year", "use_cache"],
    ),
    "data_commons": (
        "app.sources.data_commons.ingest",
        "ingest_statistical_variable",
        ["variable_dcid", "places", "api_key"],
    ),
    "fcc_broadband": (
        "app.sources.fcc_broadband.ingest",
        "ingest_state_coverage",
        ["state_code", "include_summary"],
    ),
    "yelp": (
        "app.sources.yelp.ingest",
        "ingest_businesses_by_location",
        ["location", "term", "categories", "limit", "api_key"],
    ),
    # --- Quarterly ---
    "us_trade": (
        "app.sources.us_trade.ingest",
        "ingest_exports_by_hs",
        ["year", "month", "hs_code", "country", "api_key"],
    ),
    "bts": (
        "app.sources.bts.ingest",
        "ingest_border_crossing_data",
        ["start_date", "end_date", "state", "border", "measure", "app_token"],
    ),
    "international_econ": (
        "app.sources.international_econ.ingest",
        "ingest_worldbank_wdi",
        ["indicator", "countries", "start_year", "end_year"],
    ),
    "realestate": (
        "app.sources.realestate.ingest",
        "ingest_fhfa_hpi",
        ["geography_type", "start_date", "end_date"],
    ),
    "uspto": (
        "app.sources.uspto.ingest",
        "ingest_patents",
        ["query", "start_date", "end_date", "limit"],
    ),
    "foot_traffic": (
        "app.sources.foot_traffic.ingest",
        "discover_brand_locations",
        ["brand_name", "city", "state", "latitude", "longitude", "limit"],
    ),
}

router = APIRouter(prefix="/jobs", tags=["jobs"])


async def _run_quality_gate(db, job: IngestionJob):
    """
    Run data quality rules against a successfully completed job.

    Advisory only — logs results but never changes job status.
    Errors in the quality gate itself are swallowed so they never
    cause the ingestion job to appear failed.
    """
    try:
        from app.core.data_quality_service import evaluate_rules_for_job
        from app.core.models import DatasetRegistry

        # Find the most recent dataset registry entry for this source
        registry = (
            db.query(DatasetRegistry)
            .filter(DatasetRegistry.source == job.source)
            .order_by(DatasetRegistry.last_updated_at.desc())
            .first()
        )
        if not registry:
            logger.debug(f"Quality gate: no dataset registry entry for {job.source}, skipping")
            return

        report = evaluate_rules_for_job(db, job, registry.table_name)
        if report.overall_status == "passed":
            logger.info(f"Quality gate passed for job {job.id} ({job.source})")
        else:
            logger.warning(
                f"Quality gate {report.overall_status} for job {job.id} ({job.source}): "
                f"{report.errors_count} errors, {report.warnings_count} warnings"
            )
    except Exception as e:
        logger.warning(f"Quality gate error for job {job.id}: {e}")


async def _handle_job_completion(db, job: IngestionJob):
    """
    Handle job completion: unblock dependent jobs, update chain status.

    Called after a job succeeds or fails (after retries exhausted).

    Args:
        db: Database session
        job: The completed job
    """
    from app.core import dependency_service

    # Check and unblock any dependent jobs
    unblocked_jobs = dependency_service.check_and_unblock_dependent_jobs(db, job.id)

    if unblocked_jobs:
        logger.info(f"Job {job.id} completion unblocked {len(unblocked_jobs)} dependent jobs: {unblocked_jobs}")
        # Start the unblocked jobs
        await dependency_service.process_unblocked_jobs(db, unblocked_jobs)

    # Update chain execution status if this job is part of a chain
    execution = dependency_service.get_execution_for_job(db, job.id)
    if execution:
        dependency_service.update_chain_execution_status(db, execution.id)
        logger.info(f"Updated chain execution {execution.id} status")


async def _handle_job_failure(
    db,
    job: IngestionJob,
    error_message: str,
    error_type: str = None
):
    """
    Handle job failure: set status, schedule retry, send notifications.

    Args:
        db: Database session
        job: The failed job
        error_message: Error description
        error_type: Type of error (exception class name)
    """
    from datetime import datetime
    from app.core.retry_service import auto_schedule_retry
    from app.core import monitoring

    job.status = JobStatus.FAILED
    job.error_message = error_message
    if error_type:
        job.error_details = {"error_type": error_type}
    job.completed_at = datetime.utcnow()
    db.commit()

    # Try to schedule automatic retry
    retry_scheduled = auto_schedule_retry(db, job)

    if retry_scheduled:
        logger.info(f"Job {job.id} failed, retry scheduled (attempt {job.retry_count + 1}/{job.max_retries})")
    else:
        # No more retries - send webhook notification and handle completion
        logger.warning(f"Job {job.id} failed permanently (exhausted {job.retry_count}/{job.max_retries} retries)")
        try:
            await monitoring.notify_job_completion(
                job_id=job.id,
                source=job.source,
                status=JobStatus.FAILED,
                error_message=error_message,
                config=job.config
            )
        except Exception as e:
            logger.error(f"Failed to send failure notification for job {job.id}: {e}")

        # Handle job completion (unblock dependent jobs, etc.)
        await _handle_job_completion(db, job)


async def _run_dispatched_job(db, job, job_id, source, config, monitoring):
    """Run a job via the SOURCE_DISPATCH registry."""
    from datetime import datetime

    module_path, func_name, config_keys = SOURCE_DISPATCH[source]

    try:
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
    except (ImportError, AttributeError) as e:
        await _handle_job_failure(
            db, job,
            f"Failed to load ingest function for {source}: {e}",
            type(e).__name__,
        )
        return

    # Build kwargs from config, only passing keys that have non-None values
    kwargs = {}
    for key in config_keys:
        val = config.get(key)
        if val is not None:
            kwargs[key] = val

    try:
        result = await func(db=db, job_id=job_id, **kwargs)

        # Update job with success
        job.status = JobStatus.SUCCESS
        rows_inserted = 0
        if isinstance(result, dict):
            rows_inserted = result.get("rows_inserted", 0) or result.get("total_records", 0) or 0
        job.rows_inserted = rows_inserted
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Job {job_id} ({source}) completed successfully: {result}")

        # Run advisory quality gate
        await _run_quality_gate(db, job)

        # Send success notification
        try:
            await monitoring.notify_job_completion(
                job_id=job.id,
                source=job.source,
                status=JobStatus.SUCCESS,
                rows_inserted=rows_inserted,
                config=job.config,
            )
        except Exception as e:
            logger.error(f"Failed to send success notification for job {job_id}: {e}")

        await _handle_job_completion(db, job)

    except Exception as e:
        logger.exception(f"Error during {source} ingestion for job {job_id}")
        await _handle_job_failure(db, job, str(e), type(e).__name__)


async def _run_census_job(db, job, job_id, config, monitoring):
    """Handle census ingestion (complex config parsing)."""
    from datetime import datetime

    settings = get_settings()
    try:
        settings.require_census_api_key()
    except MissingCensusAPIKeyError as e:
        await _handle_job_failure(db, job, str(e), "MissingCensusAPIKeyError")
        return

    from app.sources.census.ingest import ingest_acs_table

    survey = config.get("survey", "acs5")
    year = config.get("year")
    table_id = config.get("table_id")
    geo_level = config.get("geo_level", "state")
    geo_filter = config.get("geo_filter")

    if not year or not table_id:
        await _handle_job_failure(
            db, job,
            "Missing required config: 'year' and 'table_id' are required",
            "ValidationError",
        )
        return

    try:
        include_geojson = config.get("include_geojson", False)
        result = await ingest_acs_table(
            db=db, job_id=job_id, survey=survey, year=year,
            table_id=table_id, geo_level=geo_level,
            geo_filter=geo_filter, include_geojson=include_geojson,
        )

        job.status = JobStatus.SUCCESS
        rows_inserted = result.get("rows_inserted", 0)
        job.rows_inserted = rows_inserted
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Job {job_id} completed successfully: {result}")

        # Run advisory quality gate
        await _run_quality_gate(db, job)

        try:
            await monitoring.notify_job_completion(
                job_id=job.id, source=job.source,
                status=JobStatus.SUCCESS, rows_inserted=rows_inserted,
                config=job.config,
            )
        except Exception as e:
            logger.error(f"Failed to send success notification for job {job_id}: {e}")

        await _handle_job_completion(db, job)

    except Exception as e:
        logger.exception(f"Error during Census ingestion for job {job_id}")
        await _handle_job_failure(db, job, str(e), type(e).__name__)


async def _run_public_lp_strategies_job(db, job, job_id, config, monitoring):
    """Handle public_lp_strategies ingestion (complex config parsing)."""
    from datetime import datetime
    from app.sources.public_lp_strategies.ingest import ingest_lp_strategy_document
    from app.sources.public_lp_strategies.types import (
        LpDocumentInput, DocumentTextSectionInput,
        StrategySnapshotInput, AssetClassAllocationInput,
        AssetClassProjectionInput, ThematicTagInput,
    )

    lp_name = config.get("lp_name")
    program = config.get("program")
    fiscal_year = config.get("fiscal_year")
    fiscal_quarter = config.get("fiscal_quarter")
    document_metadata = config.get("document_metadata", {})
    parsed_sections = config.get("parsed_sections", [])
    extracted_strategy = config.get("extracted_strategy", {})

    if not all([lp_name, program, fiscal_year, fiscal_quarter]):
        await _handle_job_failure(
            db, job,
            "Missing required config: 'lp_name', 'program', 'fiscal_year', 'fiscal_quarter'",
            "ValidationError",
        )
        return

    try:
        document_input = LpDocumentInput(lp_id=0, **document_metadata)
        text_sections = [DocumentTextSectionInput(**s) for s in parsed_sections]
        strategy_input = StrategySnapshotInput(
            lp_id=0, program=program,
            fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter,
            **extracted_strategy.get("strategy", {}),
        )
        allocations = [AssetClassAllocationInput(**a) for a in extracted_strategy.get("allocations", [])]
        projections = [AssetClassProjectionInput(**p) for p in extracted_strategy.get("projections", [])]
        thematic_tags = [ThematicTagInput(**t) for t in extracted_strategy.get("thematic_tags", [])]

        result = ingest_lp_strategy_document(
            db=db, lp_name=lp_name, document_input=document_input,
            text_sections=text_sections, strategy_input=strategy_input,
            allocations=allocations, projections=projections,
            thematic_tags=thematic_tags,
        )

        job.status = JobStatus.SUCCESS
        rows_inserted = result.get("sections_count", 0) + result.get("allocations_count", 0)
        job.rows_inserted = rows_inserted
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Job {job_id} completed successfully: {result}")

        # Run advisory quality gate
        await _run_quality_gate(db, job)

        try:
            await monitoring.notify_job_completion(
                job_id=job.id, source=job.source,
                status=JobStatus.SUCCESS, rows_inserted=rows_inserted,
                config=job.config,
            )
        except Exception as e:
            logger.error(f"Failed to send success notification for job {job_id}: {e}")

        await _handle_job_completion(db, job)

    except Exception as e:
        logger.exception(f"Error during public_lp_strategies ingestion for job {job_id}")
        await _handle_job_failure(db, job, str(e), type(e).__name__)


async def run_ingestion_job(job_id: int, source: str, config: dict):
    """
    Background task to run ingestion job.

    On failure, automatically schedules retry if retries remain.
    Sends webhook notification on final failure (no retries left).
    """
    from datetime import datetime
    from app.core.database import get_session_factory
    from app.core.retry_service import auto_schedule_retry
    from app.core import monitoring

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
            await _run_census_job(db, job, job_id, config, monitoring)

        elif source == "public_lp_strategies":
            await _run_public_lp_strategies_job(db, job, job_id, config, monitoring)

        elif source in SOURCE_DISPATCH:
            await _run_dispatched_job(db, job, job_id, source, config, monitoring)

        else:
            await _handle_job_failure(db, job, f"Unknown source: {source}", "UnknownSourceError")
    
    except Exception as e:
        logger.exception(f"Error running job {job_id}")
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            await _handle_job_failure(db, job, str(e), type(e).__name__)

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
    valid_sources = ["census", "public_lp_strategies"] + list(SOURCE_DISPATCH.keys())
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

    # Audit trail
    try:
        from app.core import audit_service
        audit_service.log_collection(
            db,
            trigger_type="api",
            source=job.source,
            job_id=job.id,
            job_type="ingestion",
            trigger_source="/jobs",
            config_snapshot=job.config,
        )
    except Exception as e:
        logger.debug("Audit trail logging failed: %s", e)

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
