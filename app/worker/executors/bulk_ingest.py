"""Bulk file ingestion executor (SPEC_107).

Payload:
    bulk_source: str          registered BulkSource name (e.g. "sec_form_d")
    since: "YYYY-MM-DD"       optional lower bound for discovery
    max_releases: int         optional cap per run
    release_keys: [str]       optional explicit releases
    publish_guard_override: [str] | true
                              optional; accept a publish-guard trip (SPEC_129)
                              for these tables, for this job only

Outcome (SPEC_126a): every attempted release failed -> job failed; some
failed and some loaded -> job success with a ``PARTIAL:`` error_message on the
queue row and the IngestionJob (``summary["status"] == "partial"``).

The blocking download/COPY work runs in a thread so the worker's heartbeat
coroutine keeps running.
"""

import asyncio
import logging

from sqlalchemy.orm import Session

from app.core.copy_loader import publish_guard_override
from app.core.database import get_session_factory
from app.core.ingestion_job_sync import PARTIAL_PREFIX
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


def _finish_ingestion_job(ingestion_job_id, summary, error: str = None,
                          partial: str = None) -> None:
    """Mirror the outcome onto the linked ingestion_jobs row (scheduled runs).

    ``partial`` (SPEC_126a): success with some releases failed -- the row is
    SUCCESS (rows landed; the schedule watermark advances) and carries the
    ``PARTIAL:`` message in error_message.
    """
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
        ing.error_message = error or partial
        ing.completed_at = datetime.utcnow()
        if not error and ing.schedule_id:
            # This executor finishes its own IngestionJob, so SPEC_121's
            # write-back (mirror_queue_outcome) no-ops on the terminal row and
            # never advanced the watermark: do it here, the same way.
            from sqlalchemy import text

            db.execute(text(
                "UPDATE ingestion_schedules SET last_run_at = :c "
                "WHERE id = :sid AND (last_run_at IS NULL OR last_run_at < :c)"
            ), {"c": ing.completed_at, "sid": ing.schedule_id})
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
    ingestion_job_id = payload.get("ingestion_job_id")
    _start_ingestion_job(ingestion_job_id)

    # The IngestionJob is finished in `finally` (SPEC_121): if discovery or a
    # load raises, or the task is cancelled, a RUNNING row would otherwise
    # block its schedule until the stuck-job timeout.
    summary: dict = {}
    error = None
    partial = None
    finished = False
    try:
        job.progress_pct = 1.0
        job.progress_message = f"Discovering {name} releases"
        db.commit()

        # to_thread copies the context, so the override reaches this job's loads only
        with publish_guard_override(payload.get("publish_guard_override")):
            summary = await asyncio.to_thread(
                run_source,
                source,
                since=payload.get("since"),
                max_releases=payload.get("max_releases"),
                release_keys=payload.get("release_keys"),
                progress=_progress_writer(job.id),
                force=bool(payload.get("force")),
            )
        logger.info(f"bulk_ingest {name} summary: { {k: v for k, v in summary.items() if k != 'releases'} }")

        attempted = summary["loaded"] + summary["failed"]
        if attempted and summary["loaded"] == 0:
            error = f"All {summary['failed']} {name} release(s) failed: {summary['errors'][:3]}"
            raise RuntimeError(error)
        if summary["failed"]:
            # SPEC_126a tri-state: some loaded, some failed -> partial, not success
            partial = (
                f"{PARTIAL_PREFIX} {summary['failed']} of {attempted} {name} release(s) "
                f"failed: {summary['errors'][:3]}"
            )[:2000]
            summary["status"] = "partial"
        else:
            summary["status"] = "success"
        finished = True
    except Exception as e:
        if error is None:
            error = f"{type(e).__name__}: {e}"
        raise
    finally:
        if not finished and error is None:
            error = f"bulk_ingest {name} interrupted before completion"
        _finish_ingestion_job(ingestion_job_id, summary, error=error, partial=partial)

    if partial:
        # the queue row stays 'success' (status enum unchanged); the worker keeps
        # this visible as "Completed with partial failures"
        job.error_message = partial
    if summary.get("locked"):
        job.progress_message = f"skipped: another {name} run is in progress"
    elif summary["rows"] == 0 and summary.get("unchanged"):
        job.progress_message = (
            f"unchanged upstream: {summary['unchanged']} snapshot(s) not modified "
            f"({summary['failed']} failed, {summary['skipped']} already loaded)"
        )
    elif summary["rows"] == 0:
        job.progress_message = (
            f"warning: 0 rows loaded ({summary['skipped']} already loaded, {summary['failed']} failed)"
        )
    else:
        job.progress_message = (
            f"{summary['rows']} rows from {summary['loaded']} release(s); "
            f"{summary['failed']} failed, {summary['skipped']} skipped"
        )
    # SPEC_122: bytes downloaded vs download avoided (conditional GET)
    if summary.get("bytes_downloaded") or summary.get("bytes_saved"):
        job.progress_message += (
            f"; {summary.get('bytes_downloaded', 0) / 1e6:.0f} MB downloaded, "
            f"{summary.get('bytes_saved', 0) / 1e6:.0f} MB saved"
        )
    db.commit()
