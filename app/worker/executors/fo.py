"""Family Office collection executor for the worker queue."""
import logging

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue
from app.core.pg_notify import send_job_event

logger = logging.getLogger(__name__)


async def execute(job: JobQueue, db: Session):
    """Execute a FO collection job."""
    from app.core.database import get_session_factory
    from app.sources.family_office_collection.runner import FoCollectionOrchestrator
    from app.sources.family_office_collection.types import (
        FoCollectionConfig,
        FoCollectionSource,
    )

    payload = job.payload or {}

    job.progress_pct = 5.0
    job.progress_message = "Building FO collection config"
    db.commit()

    sources = [FoCollectionSource(s) for s in payload.get("sources", ["website"])]
    config = FoCollectionConfig(
        fo_types=payload.get("fo_types"),
        regions=payload.get("regions"),
        sources=sources,
        max_concurrent_fos=payload.get("max_concurrent_fos", 5),
        rate_limit_delay=payload.get("rate_limit_delay", 2.0),
    )

    # Use a fresh session
    SessionLocal = get_session_factory()
    work_db = SessionLocal()

    try:
        orchestrator = FoCollectionOrchestrator(config=config, db=work_db)

        send_job_event(db, "job_progress", {
            "job_id": job.id,
            "job_type": "fo",
            "progress_pct": 10.0,
            "progress_message": "Running FO collection",
        })
        db.commit()

        result = await orchestrator.run_collection()

        msg = (
            f"FO collection: {result.get('successful_fos', 0)} successful, "
            f"{result.get('total_items', 0)} items"
        )
        job.progress_pct = 100.0
        job.progress_message = msg
        db.commit()
    finally:
        work_db.close()
