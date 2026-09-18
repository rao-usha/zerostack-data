"""Bulk file ingestion executor (SPEC_107).

Payload:
    bulk_source: str          registered BulkSource name (e.g. "sec_form_d")
    since: "YYYY-MM-DD"       optional lower bound for discovery
    max_releases: int         optional cap per run
    release_keys: [str]       optional explicit releases

The blocking download/COPY work runs in a thread so the worker's heartbeat
coroutine keeps running.
"""

import asyncio
import logging

from sqlalchemy.orm import Session

from app.core.database import get_session_factory
from app.core.models_queue import JobQueue
from app.ingest.bulk.base import run_source
from app.ingest.bulk.registry import get_source

logger = logging.getLogger(__name__)


def _progress_writer(job_id: int):
    SessionLocal = get_session_factory()

    def write(message: str, pct: float) -> None:
        db = SessionLocal()
        try:
            job = db.get(JobQueue, job_id)
            if job is not None:
                job.progress_message = message[:500]
                job.progress_pct = round(pct, 1)
                db.commit()
        except Exception as e:  # progress is best-effort
            db.rollback()
            logger.debug(f"bulk progress update failed: {e}")
        finally:
            db.close()

    return write


def _start_ingestion_job(ingestion_job_id) -> None:
    """Mark the linked ingestion_jobs row RUNNING (scheduled runs)."""
    if not ingestion_job_id:
        return
    from datetime import datetime

    from app.core.models import IngestionJob, JobStatus

    db = get_session_factory()()
    try:
        ing = db.get(IngestionJob, ingestion_job_id)
        if ing is not None:
            ing.status = JobStatus.RUNNING
            ing.started_at = datetime.utcnow()
            db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Could not start ingestion job {ingestion_job_id}: {e}")
    finally:
        db.close()


def _finish_ingestion_job(ingestion_job_id, summary, error: str = None) -> None:
    """Mirror the outcome onto the linked ingestion_jobs row (scheduled runs)."""
    if not ingestion_job_id:
        return
    from datetime import datetime

    from app.core.models import IngestionJob, JobStatus

    db = get_session_factory()()
    try:
        ing = db.get(IngestionJob, ingestion_job_id)
        if ing is None:
            return
        ing.status = JobStatus.FAILED if error else JobStatus.SUCCESS
        ing.rows_inserted = summary.get("rows", 0)
        ing.error_message = error
        ing.completed_at = datetime.utcnow()
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Could not update ingestion job {ingestion_job_id}: {e}")
    finally:
        db.close()


async def execute(job: JobQueue, db: Session):
    payload = job.payload or {}
    name = payload.get("bulk_source")
    if not name:
        raise ValueError("bulk_ingest payload requires 'bulk_source'")
    source = get_source(name)
    _start_ingestion_job(payload.get("ingestion_job_id"))

    job.progress_pct = 1.0
    job.progress_message = f"Discovering {name} releases"
    db.commit()

    summary = await asyncio.to_thread(
        run_source,
        source,
        since=payload.get("since"),
        max_releases=payload.get("max_releases"),
        release_keys=payload.get("release_keys"),
        progress=_progress_writer(job.id),
    )
    logger.info(f"bulk_ingest {name} summary: { {k: v for k, v in summary.items() if k != 'releases'} }")

    attempted = summary["loaded"] + summary["failed"]
    ingestion_job_id = payload.get("ingestion_job_id")
    if attempted and summary["loaded"] == 0:
        message = f"All {summary['failed']} {name} release(s) failed: {summary['errors'][:3]}"
        _finish_ingestion_job(ingestion_job_id, summary, error=message)
        raise RuntimeError(message)
    _finish_ingestion_job(ingestion_job_id, summary)

    if summary["rows"] == 0:
        job.progress_message = (
            f"warning: 0 rows loaded ({summary['skipped']} already loaded, {summary['failed']} failed)"
        )
    else:
        job.progress_message = (
            f"{summary['rows']} rows from {summary['loaded']} release(s); "
            f"{summary['failed']} failed, {summary['skipped']} skipped"
        )
    db.commit()
