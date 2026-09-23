"""
Keep ``ingestion_jobs`` in step with the ``job_queue`` rows that run them (SPEC_121).

Many worker job types are submitted with an ``IngestionJob`` as their record
(scheduled ``job:``/``bulk:`` runs, batch runs, manual ingestion), but only some
executors write the outcome back. A row left PENDING or RUNNING blocks its
schedule for good, because ``run_scheduled_job`` skips any schedule with an
active job. This module holds the two halves of the fix:

- ``mirror_queue_outcome`` — the worker's write-back when a queue job finishes.
- ``reconcile_orphaned_ingestion_jobs`` — the periodic sweep for rows the
  write-back missed (older workers, crashes, executors that raised mid-write).

``job_queue.job_table_id`` alone does not say which table it points at
(agentic and LP jobs point at their own tables), so the link is the payload's
``ingestion_job_id`` — see ``linked_ingestion_job_id``.

Statuses are compared lower-case: both tables store lower-case enum values,
but raw SQL elsewhere has written upper case.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

TERMINAL_QUEUE_STATUSES = ("success", "failed", "cancelled")
ACTIVE_INGESTION_STATUSES = ("pending", "running")
# Rows the worker write-back may still move to a terminal state.
WRITABLE_INGESTION_STATUSES = ("pending", "running", "blocked")

SWEEP_GRACE_MINUTES = 10
ORPHAN_HOURS = 24

# SPEC_126a: a run where some units failed and some landed (bulk_ingest with
# mixed release outcomes). Both job_queue and ingestion_jobs stay 'success' --
# data did land, the schedule watermark must advance, and a new status value
# would break every status comparison -- and carry this prefix in
# error_message. The failed raw.source_release rows raise the watchdog's
# failed_release alert.
PARTIAL_PREFIX = "PARTIAL:"


def is_partial(error_message: Optional[str]) -> bool:
    """True when a successful job's error_message marks a partial failure."""
    return bool(error_message) and str(error_message).startswith(PARTIAL_PREFIX)


def linked_ingestion_job_id(job_table_id: Any, payload: Any) -> Optional[int]:
    """The ``ingestion_jobs.id`` a queue job reports to, or None.

    Every submitter that links an IngestionJob puts its id in the payload as
    ``ingestion_job_id`` (and in ``job_table_id``). Agentic and LP jobs reuse
    ``job_table_id`` for their own tables and never set it, so it is not
    trusted on its own.
    """
    if not isinstance(payload, dict):
        return None
    raw = payload.get("ingestion_job_id")
    if raw is None or isinstance(raw, bool):
        return None
    try:
        ing_id = int(raw)
    except (TypeError, ValueError):
        return None
    if job_table_id is not None:
        try:
            if int(job_table_id) != ing_id:
                return None
        except (TypeError, ValueError):
            return None
    return ing_id


def mirror_queue_outcome(
    db: Session,
    ingestion_job_id: int,
    succeeded: bool,
    error: Optional[str] = None,
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
) -> bool:
    """Copy a finished queue job's outcome onto its IngestionJob.

    A no-op when the row is already terminal (an executor wrote its own
    outcome), so it is safe to call after every queue job. On success the
    schedule watermark advances, as ``jobs._advance_schedule_watermark`` does.
    Does not commit. Returns True when the row changed.
    """
    completed = completed_at or datetime.utcnow()
    new_status = "success" if succeeded else "failed"
    row = db.execute(
        text(
            """
            UPDATE ingestion_jobs
            SET status = :status,
                completed_at = :completed,
                started_at = COALESCE(started_at, :started),
                error_message = CASE WHEN :status = 'failed'
                                     THEN COALESCE(:error, error_message)
                                     ELSE error_message END
            WHERE id = :id
              AND lower(status) IN ('pending', 'running', 'blocked')
            RETURNING schedule_id
            """
        ),
        {
            "status": new_status,
            "completed": completed,
            "started": started_at,
            "error": (error or None) and error[:2000],
            "id": ingestion_job_id,
        },
    ).fetchone()
    if row is None:
        return False

    schedule_id = row[0]
    if succeeded and schedule_id:
        db.execute(
            text(
                """
                UPDATE ingestion_schedules SET last_run_at = :completed
                WHERE id = :sid AND (last_run_at IS NULL OR last_run_at < :completed)
                """
            ),
            {"completed": completed, "sid": schedule_id},
        )
    return True


def decide(
    *,
    ing_status: str,
    ing_started_at: Optional[datetime],
    ing_created_at: Optional[datetime],
    q_status: Optional[str],
    q_completed_at: Optional[datetime],
    q_created_at: Optional[datetime],
    now: datetime,
    grace: timedelta,
    orphan_after: timedelta,
) -> Optional[Tuple[str, str]]:
    """What an active IngestionJob should become, given its latest queue row.

    Returns ``(new_status, reason)`` or None to leave it alone.
    """
    ing_status = (ing_status or "").lower()
    if ing_status not in ACTIVE_INGESTION_STATUSES:
        return None

    if q_status is None:
        # No queue row: an in-process run, or a process that died before
        # queueing. A started RUNNING row is cleanup_stuck_jobs' to time out
        # (it knows the per-source timeout); anything else is orphaned.
        if ing_status == "running" and ing_started_at is not None:
            return None
        ref = ing_created_at or now
        if now - ref <= orphan_after:
            return None
        return (
            "failed",
            f"orphaned: {ing_status} for {_hours(now - ref)}h with no job_queue row",
        )

    q_status = q_status.lower()
    if q_status not in TERMINAL_QUEUE_STATUSES:
        return None  # the worker still owns it

    done_at = q_completed_at or q_created_at or now
    if now - done_at < grace:
        return None  # a retry may be about to submit a new queue row
    if ing_started_at is not None and q_completed_at is not None and ing_started_at > q_completed_at:
        return None  # re-run in-process after that queue row finished

    new_status = "success" if q_status == "success" else "failed"
    return new_status, f"reconciled from job_queue ({q_status})"


_ACTIVE_WITH_LATEST_QUEUE = """
    SELECT ij.id, lower(ij.status), ij.started_at, ij.created_at,
           q.id, lower(q.status), q.completed_at, q.created_at, q.started_at, q.error_message
    FROM ingestion_jobs ij
    LEFT JOIN LATERAL (
        SELECT jq.id, jq.status, jq.completed_at, jq.created_at, jq.started_at, jq.error_message
        FROM job_queue jq
        WHERE (jq.payload ->> 'ingestion_job_id') = ij.id::text
          -- same rule as linked_ingestion_job_id: a NULL job_table_id links
          -- too (e.g. /batch/{id}/unstick resubmits used to leave it unset)
          AND (jq.job_table_id = ij.id OR jq.job_table_id IS NULL)
        ORDER BY jq.id DESC
        LIMIT 1
    ) q ON TRUE
    WHERE lower(ij.status) IN ('pending', 'running')
"""


def _apply(db: Session, row, now: datetime, grace: timedelta, orphan_after: timedelta) -> Optional[str]:
    ing_id, ing_status, ing_started, ing_created, q_id, q_status, q_done, q_created, q_started, q_error = row
    verdict = decide(
        ing_status=ing_status,
        ing_started_at=ing_started,
        ing_created_at=ing_created,
        q_status=q_status,
        q_completed_at=q_done,
        q_created_at=q_created,
        now=now,
        grace=grace,
        orphan_after=orphan_after,
    )
    if verdict is None:
        return None
    new_status, reason = verdict
    error = None
    if new_status == "failed":
        error = f"{reason}: {q_error}" if q_error else reason
    changed = mirror_queue_outcome(
        db,
        ing_id,
        succeeded=new_status == "success",
        error=error,
        started_at=q_started,
        completed_at=q_done or now,
    )
    if not changed:
        return None
    logger.warning(
        f"Reconciled ingestion job {ing_id}: {ing_status} -> {new_status} "
        f"({reason}; queue job {q_id})"
    )
    return new_status


def reconcile_orphaned_ingestion_jobs(
    db: Session,
    grace_minutes: int = SWEEP_GRACE_MINUTES,
    orphan_hours: int = ORPHAN_HOURS,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Settle PENDING/RUNNING ingestion jobs whose worker job has finished.

    Commits. Returns ``{"reconciled": n, "jobs": [{"id", "status"}, ...]}``.
    """
    now = now or datetime.utcnow()
    grace = timedelta(minutes=grace_minutes)
    orphan_after = timedelta(hours=orphan_hours)

    rows = db.execute(text(_ACTIVE_WITH_LATEST_QUEUE)).fetchall()
    jobs = []
    for row in rows:
        new_status = _apply(db, row, now, grace, orphan_after)
        if new_status:
            jobs.append({"id": row[0], "status": new_status})
    db.commit()
    if jobs:
        logger.warning(f"Orphan sweep reconciled {len(jobs)} ingestion job(s): {jobs}")
    return {"reconciled": len(jobs), "jobs": jobs}


def reconcile_ingestion_job(db: Session, ingestion_job_id: int, now: Optional[datetime] = None) -> Optional[str]:
    """Settle one active ingestion job now (no grace, no orphan window).

    Used when a schedule finds its previous job still active long past its
    cadence. Commits. Returns the new status, or None if it was left alone
    (its queue row is still live, or it is an in-process run).
    """
    now = now or datetime.utcnow()
    row = db.execute(
        text(_ACTIVE_WITH_LATEST_QUEUE + " AND ij.id = :id"), {"id": ingestion_job_id}
    ).fetchone()
    if row is None:
        return None
    new_status = _apply(db, row, now, timedelta(0), timedelta(0))
    db.commit()
    return new_status


LIVE_QUEUE_STATUSES = ("pending", "blocked", "claimed", "running")


def cancel_blocking_queue_job(
    db: Session, ingestion_job_id: int, reason: str, now: Optional[datetime] = None
) -> Optional[int]:
    """Cancel the live queue job behind an over-age active ingestion job.

    For a blocker whose worker job is hung (job:/bulk payloads carry no
    ``source``, so the worker applies no execution timeout, and the heartbeat
    keeps a hung executor looking alive) or queued with no worker to claim it.
    The queue row is marked failed with a "Cancelled" message, which the
    worker's heartbeat loop turns into JobCancelledError, and the ingestion job
    is failed with the same reason. Commits. Returns the cancelled queue id,
    or None when there is no live linked queue row (an in-process run is
    cleanup_stuck_jobs' to time out).
    """
    now = now or datetime.utcnow()
    row = db.execute(
        text(_ACTIVE_WITH_LATEST_QUEUE + " AND ij.id = :id"), {"id": ingestion_job_id}
    ).fetchone()
    if row is None or row[4] is None or (row[5] or "") not in LIVE_QUEUE_STATUSES:
        return None
    q_id = row[4]
    message = f"Cancelled by scheduler: {reason}"[:2000]
    cancelled = db.execute(
        text(
            """
            UPDATE job_queue
            SET status = 'failed', error_message = :msg, completed_at = :now
            WHERE id = :qid AND lower(status) IN ('pending', 'blocked', 'claimed', 'running')
            RETURNING id
            """
        ),
        {"msg": message, "now": now, "qid": q_id},
    ).fetchone()
    if cancelled is None:
        db.rollback()
        return None
    mirror_queue_outcome(db, ingestion_job_id, succeeded=False, error=message, completed_at=now)
    db.commit()
    logger.warning(f"Cancelled queue job {q_id} blocking ingestion job {ingestion_job_id}: {reason}")
    return q_id


def _hours(delta: timedelta) -> str:
    return f"{delta.total_seconds() / 3600:.1f}"
