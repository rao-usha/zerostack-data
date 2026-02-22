"""Standard data source ingestion executor for the worker queue."""

import logging

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue
from app.core.pg_notify import send_job_event

logger = logging.getLogger(__name__)


async def execute(job: JobQueue, db: Session):
    """
    Execute a standard ingestion job.

    The payload should contain:
        - source: str — data source key (e.g. "fred", "treasury")
        - config: dict — source-specific config
        - ingestion_job_id: int — FK to ingestion_jobs table

    Delegates to run_ingestion_job() which handles all source routing,
    retries, quality gates, and dependency chain processing.
    """

    payload = job.payload or {}
    source = payload.get("source", "unknown")
    config = payload.get("config", {})
    ingestion_job_id = payload.get("ingestion_job_id")

    if not ingestion_job_id:
        raise ValueError(
            f"Ingestion executor requires 'ingestion_job_id' in payload, got: {list(payload.keys())}"
        )

    job.progress_pct = 5.0
    job.progress_message = f"Starting ingestion for source={source}"
    db.commit()

    send_job_event(
        db,
        "job_progress",
        {
            "job_id": job.id,
            "job_type": "ingestion",
            "progress_pct": 5.0,
            "progress_message": f"Ingesting {source}",
            "ingestion_job_id": ingestion_job_id,
        },
    )
    db.commit()

    # Delegate to the existing ingestion pipeline
    from app.api.v1.jobs import run_ingestion_job

    logger.info(
        f"Ingestion executor dispatching job {job.id}: "
        f"source={source}, ingestion_job_id={ingestion_job_id}, "
        f"config keys={list(config.keys())}"
    )

    await run_ingestion_job(ingestion_job_id, source, config)

    # Sync final status from ingestion_jobs back to job_queue
    from app.core.models import IngestionJob, JobStatus

    ing_job = db.get(IngestionJob, ingestion_job_id)
    if ing_job:
        if ing_job.status == JobStatus.FAILED:
            raise RuntimeError(
                f"Ingestion job {ingestion_job_id} failed: {ing_job.error_message or 'unknown error'}"
            )

        job.progress_pct = 100.0
        job.progress_message = (
            f"Ingestion for {source} completed — "
            f"{ing_job.rows_inserted or 0} rows inserted"
        )
        db.commit()
    else:
        job.progress_pct = 100.0
        job.progress_message = f"Ingestion for {source} completed"
        db.commit()
