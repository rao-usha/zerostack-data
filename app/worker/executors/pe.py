"""PE collection executor for the worker queue."""
import logging

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue
from app.core.pg_notify import send_job_event

logger = logging.getLogger(__name__)


async def execute(job: JobQueue, db: Session):
    """Execute a PE collection job."""
    import app.sources.pe_collection  # noqa: F401
    from app.core.database import get_session_factory
    from app.sources.pe_collection.orchestrator import PECollectionOrchestrator
    from app.sources.pe_collection.persister import PEPersister
    from app.sources.pe_collection.types import PECollectionConfig

    payload = job.payload or {}

    job.progress_pct = 5.0
    job.progress_message = "Building PE collection config"
    db.commit()

    config = PECollectionConfig.from_dict(payload)

    # Use a fresh session
    SessionLocal = get_session_factory()
    work_db = SessionLocal()

    try:
        orchestrator = PECollectionOrchestrator(db_session=work_db)

        send_job_event(db, "job_progress", {
            "job_id": job.id,
            "job_type": "pe",
            "progress_pct": 10.0,
            "progress_message": "Running PE collection",
        })
        db.commit()

        results = await orchestrator.run_collection(config)

        # Persist results
        persister = PEPersister(work_db)
        persist_stats = persister.persist_results(results)

        total_items = sum(r.items_found for r in results)
        msg = (
            f"PE collection complete: {len(results)} results, "
            f"{total_items} items, persisted={persist_stats['persisted']}"
        )
        logger.info(msg)

        job.progress_pct = 100.0
        job.progress_message = msg
        db.commit()
    finally:
        work_db.close()
