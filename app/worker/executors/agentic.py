"""Agentic research executor for the worker queue."""

import logging

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue
from app.core.pg_notify import send_job_event

logger = logging.getLogger(__name__)


async def execute(job: JobQueue, db: Session):
    """Execute an agentic portfolio research job."""
    payload = job.payload or {}

    investor_id = payload["investor_id"]
    investor_type = payload["investor_type"]
    strategies = payload.get("strategies")
    domain_job_id = payload.get("job_id")
    db_url = payload.get("db_url")

    if not db_url:
        from app.core.config import get_settings

        db_url = get_settings().database_url

    job.progress_pct = 5.0
    job.progress_message = "Starting agentic portfolio research"
    db.commit()

    send_job_event(
        db,
        "job_progress",
        {
            "job_id": job.id,
            "job_type": "agentic",
            "progress_pct": 5.0,
            "progress_message": "Starting agentic research",
        },
    )
    db.commit()

    # Delegate to the existing background task function
    from app.api.v1.agentic_research import run_portfolio_collection

    await run_portfolio_collection(
        investor_id=investor_id,
        investor_type=investor_type,
        strategies=strategies,
        job_id=domain_job_id,
        db_url=db_url,
    )

    job.progress_pct = 100.0
    job.progress_message = "Agentic research completed"
    db.commit()
