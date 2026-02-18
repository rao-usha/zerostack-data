"""People collection executor for the worker queue."""

import logging

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue
from app.core.pg_notify import send_job_event

logger = logging.getLogger(__name__)


async def execute(job: JobQueue, db: Session):
    """Execute a people collection job."""
    from app.core.database import get_session_factory
    from app.jobs.people_collection_scheduler import process_pending_jobs

    payload = job.payload or {}
    max_jobs = payload.get("max_jobs", 10)

    job.progress_pct = 10.0
    job.progress_message = "Processing pending people collection jobs"
    db.commit()
    send_job_event(
        db,
        "job_progress",
        {
            "job_id": job.id,
            "job_type": "people",
            "progress_pct": 10.0,
            "progress_message": "Processing pending jobs",
        },
    )
    db.commit()

    # Use a fresh session
    SessionLocal = get_session_factory()
    work_db = SessionLocal()
    try:
        result = process_pending_jobs(max_jobs=max_jobs)

        job.progress_pct = 100.0
        job.progress_message = f"Processed {result.get('processed', 0)} jobs"
        db.commit()
    finally:
        work_db.close()
