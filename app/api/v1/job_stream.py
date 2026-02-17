"""
Job queue streaming and monitoring endpoints.

GET /api/v1/jobs/stream         — SSE stream for all job events
GET /api/v1/jobs/stream/{id}    — SSE stream for a specific job
GET /api/v1/jobs/active         — JSON list of currently running/claimed jobs
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.event_bus import EventBus
from app.core.models_queue import JobQueue, QueueJobStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["Job Queue"])


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
        JobQueue.status.in_([
            QueueJobStatus.CLAIMED,
            QueueJobStatus.RUNNING,
        ])
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
    from sqlalchemy import func
    status_counts = dict(
        db.query(JobQueue.status, func.count(JobQueue.id))
        .group_by(JobQueue.status)
        .all()
    )

    return {
        "total": sum(status_counts.values()),
        "by_status": {
            (k if isinstance(k, str) else k.value): v
            for k, v in status_counts.items()
        },
        "jobs": [
            {
                "id": j.id,
                "job_type": j.job_type if isinstance(j.job_type, str) else j.job_type.value,
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
