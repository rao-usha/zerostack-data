"""
Job queue submission service.

Provides submit_job() — the single entry point for all collection endpoints.

When WORKER_MODE is enabled:
    Inserts a row into job_queue with status='pending'.
    A separate worker process claims and executes it.

When WORKER_MODE is disabled (default):
    Falls back to FastAPI BackgroundTasks (existing behavior).
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional, Tuple

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue, QueueJobStatus

logger = logging.getLogger(__name__)

WORKER_MODE = os.getenv("WORKER_MODE", "") in ("1", "true", "True")


def submit_job(
    db: Session,
    job_type: str,
    payload: Dict[str, Any],
    priority: int = 0,
    job_table_id: Optional[int] = None,
    status: Optional[QueueJobStatus] = None,
    background_tasks=None,
    background_func: Optional[Callable] = None,
    background_args: Tuple = (),
) -> Dict[str, Any]:
    """
    Submit a job for execution.

    In worker mode:  inserts into job_queue (worker picks it up).
    In legacy mode:  uses FastAPI BackgroundTasks (runs in-process).

    Args:
        db: SQLAlchemy session
        job_type: One of QueueJobType values (e.g. "site_intel")
        payload: JSON-serializable config dict for the executor
        priority: Higher = picked first (default 0)
        job_table_id: Optional FK to domain-specific job table
        status: Optional initial status (default PENDING). Use BLOCKED for
                tier 2+ batch jobs that should wait for lower tiers.
        background_tasks: FastAPI BackgroundTasks instance (legacy mode)
        background_func: Callable to run in background (legacy mode)
        background_args: Positional args for background_func (legacy mode)

    Returns:
        {"mode": "queued"|"background", "job_queue_id": int|None}
    """
    if WORKER_MODE:
        # Insert into the job queue — a worker will claim it
        job = JobQueue(
            job_type=job_type,
            job_table_id=job_table_id,
            status=status or QueueJobStatus.PENDING,
            priority=priority,
            payload=payload,
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        logger.info(f"Job queued: id={job.id} type={job_type} priority={priority}")
        return {"mode": "queued", "job_queue_id": job.id}

    else:
        # Legacy fallback — run via FastAPI BackgroundTasks
        if background_tasks is None or background_func is None:
            raise ValueError(
                "background_tasks and background_func required when WORKER_MODE is off"
            )
        background_tasks.add_task(background_func, *background_args)

        logger.info(f"Job submitted via BackgroundTasks: type={job_type}")
        return {"mode": "background", "job_queue_id": None}


def reset_stale_jobs(max_age_minutes: int = 2) -> int:
    """
    Reset claimed/running jobs with stale heartbeats back to pending.

    Called periodically by the scheduler. This allows another worker
    to pick up jobs whose worker has died.

    Returns:
        Number of jobs reset
    """
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        cutoff = datetime.utcnow() - timedelta(minutes=max_age_minutes)
        stale = (
            db.query(JobQueue)
            .filter(
                JobQueue.status.in_(
                    [
                        QueueJobStatus.CLAIMED,
                        QueueJobStatus.RUNNING,
                    ]
                ),
                JobQueue.heartbeat_at < cutoff,
            )
            .all()
        )

        count = 0
        for job in stale:
            logger.warning(
                f"Resetting stale job {job.id} (worker={job.worker_id}, "
                f"last heartbeat={job.heartbeat_at})"
            )
            job.status = QueueJobStatus.PENDING
            job.worker_id = None
            job.claimed_at = None
            job.heartbeat_at = None
            count += 1

        if count:
            db.commit()
            logger.info(f"Reset {count} stale job(s) back to pending")

        return count
    except Exception as e:
        logger.error(f"Error resetting stale jobs: {e}")
        db.rollback()
        return 0
    finally:
        db.close()


def promote_blocked_jobs(db: Session, batch_id: str) -> int:
    """
    Promote BLOCKED → PENDING for the next eligible tier in a batch.

    For each tier (ascending), if all lower-tier jobs are terminal
    (SUCCESS/FAILED), promote BLOCKED jobs up to tier_max_concurrent.
    Stops at the first tier with incomplete lower dependencies.

    Also promotes the corresponding IngestionJob BLOCKED → PENDING,
    and sends pg_notify to wake workers.

    Returns:
        Number of jobs promoted.
    """
    from sqlalchemy import text
    from app.core.models import IngestionJob, JobStatus

    TERMINAL = {QueueJobStatus.SUCCESS, QueueJobStatus.FAILED}
    TERMINAL_ING = {JobStatus.SUCCESS, JobStatus.FAILED}

    # Load all queue jobs in this batch
    queue_jobs = (
        db.query(JobQueue)
        .filter(JobQueue.payload["batch_id"].as_string() == batch_id)
        .all()
    )
    if not queue_jobs:
        return 0

    # Group by tier
    tiers: Dict[int, list] = {}
    for qj in queue_jobs:
        tier_level = (qj.payload or {}).get("tier", 0)
        tiers.setdefault(tier_level, []).append(qj)

    promoted = 0

    for tier_level in sorted(tiers.keys()):
        # Check all lower tiers are terminal
        lower_complete = True
        for lower_level in sorted(tiers.keys()):
            if lower_level >= tier_level:
                break
            for qj in tiers[lower_level]:
                if QueueJobStatus(qj.status) not in TERMINAL:
                    lower_complete = False
                    break
            if not lower_complete:
                break

        if not lower_complete:
            break  # Stop — can't promote this or higher tiers

        # Count how many non-blocked (active) jobs exist in this tier
        tier_jobs = tiers[tier_level]
        blocked = [qj for qj in tier_jobs if QueueJobStatus(qj.status) == QueueJobStatus.BLOCKED]
        if not blocked:
            continue

        # Determine max_concurrent from payload
        max_concurrent = (tier_jobs[0].payload or {}).get("tier_max_concurrent", 2)
        active = sum(
            1 for qj in tier_jobs
            if QueueJobStatus(qj.status) not in TERMINAL
            and QueueJobStatus(qj.status) != QueueJobStatus.BLOCKED
        )
        slots = max(0, max_concurrent - active)

        for qj in blocked[:slots]:
            qj.status = QueueJobStatus.PENDING
            promoted += 1

            # Also promote the IngestionJob
            if qj.job_table_id:
                ing_job = db.query(IngestionJob).filter(
                    IngestionJob.id == qj.job_table_id
                ).first()
                if ing_job and ing_job.status == JobStatus.BLOCKED:
                    ing_job.status = JobStatus.PENDING

    if promoted:
        db.commit()
        # Wake workers via pg_notify
        try:
            db.execute(text("SELECT pg_notify('jobs_promoted', :batch_id)"),
                       {"batch_id": batch_id})
            db.commit()
        except Exception:
            pass  # pg_notify is best-effort
        logger.info(f"Promoted {promoted} blocked jobs in batch {batch_id}")

    return promoted
