"""LP collection executor for the worker queue."""
import asyncio
import logging

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue
from app.core.pg_notify import send_job_event

logger = logging.getLogger(__name__)


async def execute(job: JobQueue, db: Session):
    """Execute an LP collection job."""
    from app.core.database import get_session_factory
    from app.sources.lp_collection.runner import LpCollectionOrchestrator
    from app.sources.lp_collection.types import CollectionConfig, LpCollectionSource, CollectionMode

    payload = job.payload or {}

    job.progress_pct = 5.0
    job.progress_message = "Building LP collection config"
    db.commit()

    # Rebuild config from payload
    sources = [LpCollectionSource(s) for s in payload.get("sources", ["website"])]
    config = CollectionConfig(
        lp_types=payload.get("lp_types"),
        regions=payload.get("regions"),
        sources=sources,
        mode=CollectionMode(payload.get("mode", "incremental")),
        max_age_days=payload.get("max_age_days", 90),
        max_concurrent_lps=payload.get("max_concurrent_lps", 5),
    )

    # Use a fresh session
    SessionLocal = get_session_factory()
    work_db = SessionLocal()

    try:
        orchestrator = LpCollectionOrchestrator(work_db, config)

        send_job_event(db, "job_progress", {
            "job_id": job.id,
            "job_type": "lp",
            "progress_pct": 10.0,
            "progress_message": "Running LP collection",
        })
        db.commit()

        await orchestrator.run_collection_job()

        job.progress_pct = 100.0
        job.progress_message = "LP collection completed"
        db.commit()
    finally:
        work_db.close()
