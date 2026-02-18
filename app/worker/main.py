"""
Worker process entrypoint.

Polls job_queue for pending jobs, claims them via SELECT FOR UPDATE SKIP LOCKED,
and routes to the appropriate executor. Sends progress via pg_notify.

Usage:
    python -m app.worker.main

Env vars:
    DATABASE_URL        — Required
    WORKER_POLL_INTERVAL — Seconds between polls (default 2.0)
"""
import asyncio
import logging
import os
import signal
import socket
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_session_factory
from app.core.models_queue import JobQueue, QueueJobStatus, QueueJobType
from app.core.pg_notify import send_job_event

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("worker")

POLL_INTERVAL = float(os.getenv("WORKER_POLL_INTERVAL", "2.0"))
HEARTBEAT_INTERVAL = 30  # seconds
WORKER_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

# Graceful shutdown flag
_shutdown = asyncio.Event()


def _handle_signal(signum, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    _shutdown.set()


# Register signal handlers
signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ---------------------------------------------------------------------------
# Executor registry
# ---------------------------------------------------------------------------

EXECUTORS = {}


def _load_executors():
    """Import executor modules to populate EXECUTORS dict."""
    from app.worker.executors.site_intel import execute as site_intel_exec
    from app.worker.executors.people import execute as people_exec
    from app.worker.executors.lp import execute as lp_exec
    from app.worker.executors.pe import execute as pe_exec
    from app.worker.executors.fo import execute as fo_exec
    from app.worker.executors.agentic import execute as agentic_exec
    from app.worker.executors.foot_traffic import execute as foot_traffic_exec
    from app.worker.executors.ingestion import execute as ingestion_exec

    EXECUTORS.update({
        QueueJobType.SITE_INTEL: site_intel_exec,
        QueueJobType.PEOPLE: people_exec,
        QueueJobType.LP: lp_exec,
        QueueJobType.PE: pe_exec,
        QueueJobType.FO: fo_exec,
        QueueJobType.AGENTIC: agentic_exec,
        QueueJobType.FOOT_TRAFFIC: foot_traffic_exec,
        QueueJobType.INGESTION: ingestion_exec,
    })


# ---------------------------------------------------------------------------
# Core loop
# ---------------------------------------------------------------------------


def claim_job(db: Session) -> Optional[JobQueue]:
    """
    Claim a pending job using SELECT FOR UPDATE SKIP LOCKED.

    Returns the claimed job or None if no jobs available.
    """
    result = db.execute(
        text("""
            UPDATE job_queue
            SET status = :claimed,
                worker_id = :worker_id,
                claimed_at = NOW(),
                heartbeat_at = NOW()
            WHERE id = (
                SELECT id FROM job_queue
                WHERE status = :pending
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, job_type, payload, job_table_id
        """),
        {
            "claimed": QueueJobStatus.CLAIMED.value,
            "pending": QueueJobStatus.PENDING.value,
            "worker_id": WORKER_ID,
        },
    )
    row = result.fetchone()
    if row is None:
        db.commit()
        return None

    db.commit()

    # Load as ORM object for the executor
    job = db.get(JobQueue, row[0])
    return job


async def _heartbeat_loop(db_factory, job_id: int):
    """Periodically update heartbeat_at while a job is executing."""
    SessionLocal = db_factory
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        session = SessionLocal()
        try:
            session.execute(
                text("UPDATE job_queue SET heartbeat_at = NOW() WHERE id = :id"),
                {"id": job_id},
            )
            session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()


async def execute_job(job: JobQueue, db: Session):
    """Execute a claimed job using the appropriate executor."""
    job_type_enum = QueueJobType(job.job_type) if isinstance(job.job_type, str) else job.job_type
    executor = EXECUTORS.get(job_type_enum)

    if executor is None:
        error = f"No executor registered for job_type={job.job_type}"
        logger.error(error)
        job.status = QueueJobStatus.FAILED
        job.error_message = error
        job.completed_at = datetime.utcnow()
        db.commit()
        return

    # Mark as running
    job.status = QueueJobStatus.RUNNING
    job.started_at = datetime.utcnow()
    job.heartbeat_at = datetime.utcnow()
    db.commit()

    # Send started event
    send_job_event(db, "job_started", {
        "job_id": job.id,
        "job_type": job.job_type if isinstance(job.job_type, str) else job.job_type.value,
        "worker_id": WORKER_ID,
    })
    db.commit()

    # Start heartbeat
    SessionLocal = get_session_factory()
    heartbeat_task = asyncio.create_task(_heartbeat_loop(SessionLocal, job.id))

    try:
        await executor(job, db)

        # Mark success
        job.status = QueueJobStatus.SUCCESS
        job.completed_at = datetime.utcnow()
        job.progress_pct = 100.0
        job.progress_message = "Completed"
        db.commit()

        send_job_event(db, "job_completed", {
            "job_id": job.id,
            "job_type": job.job_type if isinstance(job.job_type, str) else job.job_type.value,
            "worker_id": WORKER_ID,
        })
        db.commit()

        logger.info(f"Job {job.id} ({job.job_type}) completed successfully")

    except Exception as e:
        logger.error(f"Job {job.id} ({job.job_type}) failed: {e}", exc_info=True)

        # Refresh session state in case executor left it dirty
        try:
            db.rollback()
        except Exception:
            pass

        job = db.get(JobQueue, job.id)
        job.status = QueueJobStatus.FAILED
        job.error_message = str(e)[:2000]
        job.completed_at = datetime.utcnow()
        db.commit()

        send_job_event(db, "job_failed", {
            "job_id": job.id,
            "job_type": job.job_type if isinstance(job.job_type, str) else job.job_type.value,
            "worker_id": WORKER_ID,
            "error_message": str(e)[:500],
        })
        db.commit()

    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


async def poll_loop():
    """Main poll loop: claim → execute → repeat."""
    _load_executors()

    SessionLocal = get_session_factory()
    logger.info(f"Worker {WORKER_ID} starting poll loop (interval={POLL_INTERVAL}s)")

    while not _shutdown.is_set():
        db = SessionLocal()
        try:
            job = claim_job(db)
            if job:
                logger.info(
                    f"Claimed job {job.id} (type={job.job_type}, priority={job.priority})"
                )
                await execute_job(job, db)
            else:
                # No jobs available — wait before polling again
                try:
                    await asyncio.wait_for(_shutdown.wait(), timeout=POLL_INTERVAL)
                except asyncio.TimeoutError:
                    pass
        except Exception as e:
            logger.error(f"Poll loop error: {e}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)
        finally:
            db.close()

    logger.info(f"Worker {WORKER_ID} shut down cleanly")


def main():
    """Entrypoint for python -m app.worker.main."""
    # Ensure tables exist (worker might start before API)
    from app.core.database import create_tables
    create_tables()

    asyncio.run(poll_loop())


if __name__ == "__main__":
    main()
