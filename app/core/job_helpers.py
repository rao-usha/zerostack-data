"""
Shared helpers for creating and dispatching ingestion jobs.

Eliminates boilerplate across API routers by providing a single function
to create an IngestionJob, schedule it as a background task via the
centralized SOURCE_DISPATCH registry, and return a standard response.
"""
import logging
from typing import Optional

from fastapi import BackgroundTasks, HTTPException
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus

logger = logging.getLogger(__name__)


def create_and_dispatch_job(
    db: Session,
    background_tasks: BackgroundTasks,
    source: str,
    config: dict,
    message: Optional[str] = None,
) -> dict:
    """
    Create an IngestionJob record and schedule it via the centralized
    run_ingestion_job() background task.

    Args:
        db: Database session
        background_tasks: FastAPI BackgroundTasks instance
        source: Source identifier (e.g. "fema", "eia", "fred")
        config: Job configuration dict (passed to the ingest function).
                Include a "dataset" key for multi-dataset sources
                (e.g. {"dataset": "pa_projects", ...}).
        message: Custom status message (auto-generated if None)

    Returns:
        Standard pending-job response dict with job_id and status URL.

    Raises:
        HTTPException: If job creation fails.
    """
    try:
        job = IngestionJob(
            source=source,
            status=JobStatus.PENDING,
            config=config,
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        from app.api.v1.jobs import run_ingestion_job
        background_tasks.add_task(run_ingestion_job, job.id, source, config)

        return {
            "job_id": job.id,
            "status": "pending",
            "message": message or f"{source} ingestion job created",
            "check_status": f"/api/v1/jobs/{job.id}",
        }

    except Exception as e:
        logger.error("Failed to create %s job: %s", source, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
