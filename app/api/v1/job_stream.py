"""
Job queue streaming and monitoring endpoints.

GET /api/v1/jobs/stream         — SSE stream for all job events
GET /api/v1/jobs/stream/{id}    — SSE stream for a specific job
GET /api/v1/jobs/active         — JSON list of currently running/claimed jobs
GET /api/v1/jobs/history        — Unified paginated history from both tables
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.event_bus import EventBus
from app.core.models import IngestionJob
from app.core.models_queue import JobEvent, JobQueue, QueueJobStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/job-queue", tags=["Job Queue"])


@router.get("/stream")
async def stream_all_jobs():
    """
    SSE stream of all job events (started, progress, completed, failed).

    Usage:
        const es = new EventSource('/api/v1/jobs/stream');
        es.addEventListener('job_started', e => { ... });
        es.addEventListener('job_progress', e => { ... });
        es.addEventListener('job_completed', e => { ... });
        es.addEventListener('job_failed', e => { ... });
    """
    return StreamingResponse(
        EventBus.subscribe_stream("jobs_all"),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/stream/{job_id}")
async def stream_job(job_id: int):
    """SSE stream for a specific job's events."""
    return StreamingResponse(
        EventBus.subscribe_stream(f"job_{job_id}"),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/active")
async def get_active_jobs(
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    db: Session = Depends(get_db),
):
    """
    Get currently active (claimed or running) jobs from the queue.

    Returns a JSON list suitable for the frontend Active Jobs panel.
    """
    query = db.query(JobQueue).filter(
        JobQueue.status.in_(
            [
                QueueJobStatus.CLAIMED,
                QueueJobStatus.RUNNING,
            ]
        )
    )

    if job_type:
        query = query.filter(JobQueue.job_type == job_type)

    jobs = query.order_by(JobQueue.created_at.desc()).all()

    return [
        {
            "id": j.id,
            "job_type": j.job_type if isinstance(j.job_type, str) else j.job_type.value,
            "status": j.status if isinstance(j.status, str) else j.status.value,
            "worker_id": j.worker_id,
            "priority": j.priority,
            "progress_pct": j.progress_pct,
            "progress_message": j.progress_message,
            "created_at": j.created_at.isoformat() if j.created_at else None,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "claimed_at": j.claimed_at.isoformat() if j.claimed_at else None,
            "heartbeat_at": j.heartbeat_at.isoformat() if j.heartbeat_at else None,
        }
        for j in jobs
    ]


@router.get("/queue")
async def get_queue_status(
    limit: int = Query(50, ge=1, le=200),
    status: Optional[str] = Query(None, description="Filter by status"),
    db: Session = Depends(get_db),
):
    """Get job queue status with optional filters."""
    query = db.query(JobQueue)

    if status:
        query = query.filter(JobQueue.status == status)

    jobs = query.order_by(JobQueue.created_at.desc()).limit(limit).all()

    # Count by status
    status_counts = dict(
        db.query(JobQueue.status, func.count(JobQueue.id))
        .group_by(JobQueue.status)
        .all()
    )

    return {
        "total": sum(status_counts.values()),
        "by_status": {
            (k if isinstance(k, str) else k.value): v for k, v in status_counts.items()
        },
        "jobs": [
            {
                "id": j.id,
                "job_type": j.job_type
                if isinstance(j.job_type, str)
                else j.job_type.value,
                "status": j.status if isinstance(j.status, str) else j.status.value,
                "worker_id": j.worker_id,
                "priority": j.priority,
                "progress_pct": j.progress_pct,
                "progress_message": j.progress_message,
                "error_message": j.error_message,
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            }
            for j in jobs
        ],
    }


@router.get("/{job_id}/events")
async def get_job_events(
    job_id: int,
    limit: int = Query(200, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """
    Return the persisted event timeline for a job.

    Events are ordered chronologically (oldest first) so the frontend
    can render them as a top-to-bottom timeline.
    """
    rows = (
        db.query(JobEvent)
        .filter(JobEvent.job_id == job_id)
        .order_by(JobEvent.created_at.asc())
        .limit(limit)
        .all()
    )

    return {
        "job_id": job_id,
        "event_count": len(rows),
        "events": [
            {
                "event_type": r.event_type,
                "message": r.message,
                "data": r.data,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
    }


def _enum_val(v):
    """Extract .value from an enum, or return the string as-is."""
    return v if isinstance(v, str) else v.value


def _duration(started, completed):
    """Compute duration in seconds between two datetimes, or None."""
    if started and completed:
        return round((completed - started).total_seconds())
    return None


@router.get("/history")
async def get_job_history(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(
        None, description="Filter by status (pending, running, success, failed)"
    ),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    db: Session = Depends(get_db),
):
    """
    Unified paginated job history from both job_queue and ingestion_jobs tables.

    Returns jobs sorted by created_at desc with computed duration_seconds
    and summary counts by status.
    """
    # --- Build queue jobs list ---
    q_query = db.query(JobQueue)
    if status:
        q_query = q_query.filter(JobQueue.status == status)
    if job_type:
        q_query = q_query.filter(JobQueue.job_type == job_type)
    queue_jobs = q_query.order_by(JobQueue.created_at.desc()).all()

    queue_dicts = [
        {
            "id": j.id,
            "table": "job_queue",
            "job_type": _enum_val(j.job_type),
            "status": _enum_val(j.status),
            "worker_id": j.worker_id,
            "progress_pct": j.progress_pct,
            "progress_message": j.progress_message,
            "payload": j.payload or {},
            "rows_inserted": None,
            "error_message": j.error_message,
            "created_at": j.created_at.isoformat() if j.created_at else None,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            "duration_seconds": _duration(j.started_at, j.completed_at),
            "_sort_key": j.created_at,
        }
        for j in queue_jobs
    ]

    # --- Build ingestion jobs list ---
    # Map ingestion statuses to the same filter values
    i_query = db.query(IngestionJob)
    if status:
        i_query = i_query.filter(IngestionJob.status == status)
    if job_type:
        # job_type filter: for ingestion jobs, match against source column
        i_query = i_query.filter(IngestionJob.source == job_type)
    ingest_jobs = i_query.order_by(IngestionJob.created_at.desc()).all()

    ingest_dicts = [
        {
            "id": j.id,
            "table": "ingestion_jobs",
            "job_type": j.source,
            "status": _enum_val(j.status),
            "worker_id": None,
            "progress_pct": 100.0 if _enum_val(j.status) == "success" else None,
            "progress_message": None,
            "payload": j.config or {},
            "rows_inserted": j.rows_inserted,
            "error_message": j.error_message,
            "created_at": j.created_at.isoformat() if j.created_at else None,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            "duration_seconds": _duration(j.started_at, j.completed_at),
            "_sort_key": j.created_at,
        }
        for j in ingest_jobs
    ]

    # --- Merge and sort ---
    all_jobs = queue_dicts + ingest_dicts
    from datetime import datetime as _dt

    _epoch = _dt(1970, 1, 1)
    all_jobs.sort(key=lambda x: x["_sort_key"] or _epoch, reverse=True)

    # Summary counts
    summary = {"running": 0, "pending": 0, "success": 0, "failed": 0}
    for j in all_jobs:
        s = j["status"]
        if s in ("running", "claimed"):
            summary["running"] += 1
        elif s == "pending":
            summary["pending"] += 1
        elif s == "success":
            summary["success"] += 1
        elif s == "failed":
            summary["failed"] += 1

    total = len(all_jobs)

    # Apply pagination
    page = all_jobs[offset : offset + limit]

    # Remove internal sort key
    for j in page:
        j.pop("_sort_key", None)

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "summary": summary,
        "jobs": page,
    }
