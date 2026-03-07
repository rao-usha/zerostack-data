"""
Worker process entrypoint.

Polls job_queue for pending jobs, claims them via SELECT FOR UPDATE SKIP LOCKED,
and routes to the appropriate executor. Sends progress via pg_notify.

Each worker runs up to WORKER_MAX_CONCURRENT jobs simultaneously using an
asyncio.Semaphore to bound concurrency.

Usage:
    python -m app.worker.main

Env vars:
    DATABASE_URL        — Required
    WORKER_POLL_INTERVAL — Seconds between polls (default 2.0)
    WORKER_MAX_CONCURRENT — Max concurrent jobs per worker (default 4)
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
MAX_CONCURRENT = int(os.getenv("WORKER_MAX_CONCURRENT", "4"))
DRAIN_TIMEOUT = float(os.getenv("WORKER_DRAIN_TIMEOUT", "30.0"))
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

    EXECUTORS.update(
        {
            QueueJobType.SITE_INTEL: site_intel_exec,
            QueueJobType.PEOPLE: people_exec,
            QueueJobType.LP: lp_exec,
            QueueJobType.PE: pe_exec,
            QueueJobType.FO: fo_exec,
            QueueJobType.AGENTIC: agentic_exec,
            QueueJobType.FOOT_TRAFFIC: foot_traffic_exec,
            QueueJobType.INGESTION: ingestion_exec,
        }
    )


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


class JobCancelledError(Exception):
    """Raised when a job is detected as cancelled mid-execution."""
    pass


async def _heartbeat_loop(db_factory, job_id: int):
    """
    Periodically update heartbeat_at while a job is executing.

    Also checks if the job has been cancelled (status set to FAILED with
    'Cancelled by user' error). If so, raises JobCancelledError to stop
    the executor.
    """
    SessionLocal = db_factory
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        session = SessionLocal()
        try:
            # Check if job was cancelled
            result = session.execute(
                text("SELECT status, error_message FROM job_queue WHERE id = :id"),
                {"id": job_id},
            )
            row = result.fetchone()
            if row and row[0] == QueueJobStatus.FAILED.value and row[1] and "Cancelled" in row[1]:
                logger.info(f"Job {job_id} was cancelled — stopping execution")
                session.close()
                raise JobCancelledError(f"Job {job_id} cancelled by user")

            session.execute(
                text("UPDATE job_queue SET heartbeat_at = NOW() WHERE id = :id"),
                {"id": job_id},
            )
            session.commit()
        except JobCancelledError:
            raise
        except Exception:
            session.rollback()
        finally:
            try:
                session.close()
            except Exception:
                pass


async def execute_job(job: JobQueue, db: Session):
    """Execute a claimed job using the appropriate executor."""
    job_type_enum = (
        QueueJobType(job.job_type) if isinstance(job.job_type, str) else job.job_type
    )
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
    send_job_event(
        db,
        "job_started",
        {
            "job_id": job.id,
            "job_type": job.job_type
            if isinstance(job.job_type, str)
            else job.job_type.value,
            "worker_id": WORKER_ID,
        },
    )
    db.commit()

    # Acquire rate limit for this source before executing
    payload = job.payload or {}
    source_name = payload.get("source")
    rate_limiter = None
    if source_name:
        try:
            from app.core.rate_limiter import get_rate_limiter
            rate_limiter = get_rate_limiter()
            acquired = await rate_limiter.acquire(source_name, timeout=60.0)
            if not acquired:
                logger.warning(f"Job {job.id}: rate limit timeout for source '{source_name}', proceeding anyway")
                rate_limiter = None  # Don't release what we didn't acquire
        except Exception as e:
            logger.warning(f"Job {job.id}: rate limiter error: {e}")
            rate_limiter = None

    # Look up per-source timeout from SourceConfig
    source_name = payload.get("source")
    job_timeout_secs = None
    if source_name:
        try:
            from app.core import source_config_service
            timeout_db = get_session_factory()()
            try:
                job_timeout_secs = source_config_service.get_timeout_seconds(
                    timeout_db, source_name.split(":")[0]
                )
            finally:
                timeout_db.close()
        except Exception:
            pass

    # Start heartbeat (also monitors for cancellation)
    SessionLocal = get_session_factory()
    heartbeat_task = asyncio.create_task(_heartbeat_loop(SessionLocal, job.id))
    executor_task = asyncio.create_task(executor(job, db))

    try:
        # Wait for either the executor to finish or the heartbeat to detect cancellation
        done, pending = await asyncio.wait(
            {executor_task, heartbeat_task},
            return_when=asyncio.FIRST_COMPLETED,
            timeout=job_timeout_secs,  # None = no timeout (default if no config)
        )

        # If timed out, done is empty
        if not done:
            executor_task.cancel()
            heartbeat_task.cancel()
            try:
                await executor_task
            except (asyncio.CancelledError, Exception):
                pass
            try:
                await heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            raise TimeoutError(
                f"Job execution timed out after {job_timeout_secs}s (source={source_name})"
            )

        # Check if heartbeat detected cancellation
        if heartbeat_task in done:
            heartbeat_exc = heartbeat_task.exception()
            if isinstance(heartbeat_exc, JobCancelledError):
                # Cancel the executor
                executor_task.cancel()
                try:
                    await executor_task
                except (asyncio.CancelledError, Exception):
                    pass
                raise heartbeat_exc

        # Executor finished — get its result (may raise)
        executor_task.result()

        # Mark success
        job.status = QueueJobStatus.SUCCESS
        job.completed_at = datetime.utcnow()
        job.progress_pct = 100.0
        job.progress_message = "Completed"
        db.commit()

        send_job_event(
            db,
            "job_completed",
            {
                "job_id": job.id,
                "job_type": job.job_type
                if isinstance(job.job_type, str)
                else job.job_type.value,
                "worker_id": WORKER_ID,
            },
        )
        db.commit()

        logger.info(f"Job {job.id} ({job.job_type}) completed successfully")

    except JobCancelledError:
        logger.info(f"Job {job.id} ({job.job_type}) cancelled by user")

        try:
            db.rollback()
        except Exception:
            pass

        job = db.get(JobQueue, job.id)
        if job.status != QueueJobStatus.FAILED:
            job.status = QueueJobStatus.FAILED
            job.error_message = "Cancelled by user"
            job.completed_at = datetime.utcnow()
        db.commit()

        send_job_event(
            db,
            "job_cancelled",
            {
                "job_id": job.id,
                "job_type": job.job_type
                if isinstance(job.job_type, str)
                else job.job_type.value,
                "worker_id": WORKER_ID,
            },
        )
        db.commit()

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

        send_job_event(
            db,
            "job_failed",
            {
                "job_id": job.id,
                "job_type": job.job_type
                if isinstance(job.job_type, str)
                else job.job_type.value,
                "worker_id": WORKER_ID,
                "error_message": str(e)[:500],
            },
        )
        db.commit()

    finally:
        # Release rate limit slot
        if rate_limiter and source_name:
            rate_limiter.release(source_name)

        # Promote blocked jobs in the same batch (tier 2+ waiting for lower tiers)
        batch_id = (job.payload or {}).get("batch_id")
        if batch_id and job.status in (QueueJobStatus.SUCCESS, QueueJobStatus.FAILED):
            try:
                from app.core.job_queue_service import promote_blocked_jobs
                promote_db = get_session_factory()()
                try:
                    promote_blocked_jobs(promote_db, batch_id)
                finally:
                    promote_db.close()
            except Exception as e:
                logger.error(f"Failed to promote blocked jobs for batch {batch_id}: {e}")

        # Clean up whichever task is still running
        for t in (heartbeat_task, executor_task):
            if not t.done():
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass


async def _run_slot(semaphore: asyncio.Semaphore, job: JobQueue, db: Session):
    """Execute a job in a semaphore-bounded slot, then release."""
    try:
        await execute_job(job, db)
    finally:
        db.close()
        semaphore.release()


async def poll_loop():
    """
    Main poll loop with concurrent execution.

    Uses a semaphore to bound the number of concurrent jobs per worker.
    Each claimed job runs in its own asyncio task within a slot.
    """
    _load_executors()

    SessionLocal = get_session_factory()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    active_tasks: set = set()

    logger.info(
        f"Worker {WORKER_ID} starting poll loop "
        f"(interval={POLL_INTERVAL}s, max_concurrent={MAX_CONCURRENT})"
    )

    while not _shutdown.is_set():
        # If all slots are full, wait briefly before checking again
        if semaphore._value == 0:
            await asyncio.sleep(0.5)
            continue

        db = SessionLocal()
        try:
            job = claim_job(db)
            if job:
                logger.info(
                    f"Claimed job {job.id} (type={job.job_type}, priority={job.priority})"
                )
                await semaphore.acquire()
                task = asyncio.create_task(_run_slot(semaphore, job, db))
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)
            else:
                db.close()
                # No jobs available — wait before polling again
                try:
                    await asyncio.wait_for(_shutdown.wait(), timeout=POLL_INTERVAL)
                except asyncio.TimeoutError:
                    pass
        except Exception as e:
            logger.error(f"Poll loop error: {e}", exc_info=True)
            db.close()
            await asyncio.sleep(POLL_INTERVAL)

    # Graceful shutdown: drain active tasks with timeout
    if active_tasks:
        logger.info(f"Draining {len(active_tasks)} task(s) (timeout={DRAIN_TIMEOUT}s)...")
        try:
            await asyncio.wait_for(
                asyncio.gather(*active_tasks, return_exceptions=True),
                timeout=DRAIN_TIMEOUT,
            )
            logger.info("All tasks completed within drain timeout")
        except asyncio.TimeoutError:
            logger.warning(f"Drain timeout exceeded — cancelling {len(active_tasks)} task(s)")
            for task in active_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*active_tasks, return_exceptions=True)

            # Mark interrupted jobs as failed in DB
            try:
                cleanup_db = get_session_factory()()
                cleanup_db.execute(text("""
                    UPDATE job_queue
                    SET status = 'FAILED',
                        error_message = 'Worker drain timeout — forced shutdown',
                        completed_at = NOW()
                    WHERE worker_id = :wid AND status IN ('RUNNING', 'CLAIMED')
                """), {"wid": WORKER_ID})
                cleanup_db.commit()
                cleanup_db.close()
            except Exception as e:
                logger.error(f"Drain cleanup failed: {e}")

    logger.info(f"Worker {WORKER_ID} shut down cleanly")


def main():
    """Entrypoint for python -m app.worker.main."""
    # Ensure tables exist (worker might start before API)
    from app.core.database import create_tables

    create_tables()

    asyncio.run(poll_loop())


if __name__ == "__main__":
    main()
