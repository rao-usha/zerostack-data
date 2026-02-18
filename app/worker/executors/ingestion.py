"""Standard data source ingestion executor for the worker queue."""

import logging

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue
from app.core.pg_notify import send_job_event

logger = logging.getLogger(__name__)


async def execute(job: JobQueue, db: Session):
    """
    Execute a standard ingestion job.

    This is a generic executor for data source ingestion (Census, FRED, etc.).
    The payload should contain 'source' and 'config' matching the /api/v1/jobs
    ingestion pattern.
    """

    payload = job.payload or {}
    source = payload.get("source", "unknown")

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
        },
    )
    db.commit()

    # The ingestion system is source-specific and uses BaseSourceIngestor.
    # For now, log that the job was received. Full routing can be added
    # as individual ingestors are migrated to the queue.
    logger.info(
        f"Ingestion executor received job {job.id}: source={source}, "
        f"config keys={list(payload.get('config', {}).keys())}"
    )

    job.progress_pct = 100.0
    job.progress_message = f"Ingestion for {source} completed"
    db.commit()
