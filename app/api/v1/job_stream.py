"""
Job queue streaming and monitoring endpoints.

GET /api/v1/jobs/stream         — SSE stream for all job events
GET /api/v1/jobs/stream/{id}    — SSE stream for a specific job
GET /api/v1/jobs/active         — JSON list of currently running/claimed jobs
GET /api/v1/jobs/history        — Unified paginated history from both tables
GET /api/v1/jobs/summary        — Lightweight counts for dashboard headers
"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import String, func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.event_bus import EventBus
from app.core.models import IngestionJob
from app.core.models_queue import JobEvent, JobQueue, QueueJobStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/job-queue", tags=["Job Queue"])


# ── Domain table mapping for enrichment ─────────────────────────────────

_DOMAIN_TABLE_MAP = {
    "people": {
        "model_path": "app.core.people_models",
        "model_name": "PeopleCollectionJob",
        "fields": ["people_found", "people_created", "people_updated", "company_id"],
    },
    "agentic": {
        "model_path": "app.core.models",
        "model_name": "AgenticCollectionJob",
        "fields": ["target_investor_name", "companies_found", "new_companies", "sources_checked"],
    },
    "foot_traffic": {
        "model_path": "app.core.models",
        "model_name": "FootTrafficCollectionJob",
        "fields": ["target_brand", "locations_found", "locations_enriched"],
    },
    "lp": {
        "model_path": "app.core.models",
        "model_name": "LpCollectionJob",
        "fields": ["total_lps", "completed_lps", "successful_lps", "failed_lps"],
    },
}


def _enrich_domain_detail(standalone_jobs: list, queue_jobs_raw: list, db: Session):
    """
    Batch-query domain tables and attach a `domain_detail` sub-dict
    to standalone queue jobs that have a linked job_table_id.
    """
    # Group queue jobs by type for batch lookup
    type_to_ids: dict[str, dict[int, int]] = defaultdict(dict)  # {type: {job_table_id: queue_job_idx}}
    idx_map: dict[int, int] = {}  # queue_job.id -> index in standalone_jobs

    for idx, sj in enumerate(standalone_jobs):
        idx_map[sj["id"]] = idx

    for qj in queue_jobs_raw:
        jt = _enum_val(qj.job_type)
        if jt in _DOMAIN_TABLE_MAP and qj.job_table_id:
            if qj.id in idx_map:
                type_to_ids[jt][qj.job_table_id] = idx_map[qj.id]

    for jtype, id_to_idx in type_to_ids.items():
        if not id_to_idx:
            continue
        cfg = _DOMAIN_TABLE_MAP[jtype]
        try:
            import importlib
            mod = importlib.import_module(cfg["model_path"])
            model_cls = getattr(mod, cfg["model_name"])
            rows = db.query(model_cls).filter(model_cls.id.in_(list(id_to_idx.keys()))).all()
            for row in rows:
                sidx = id_to_idx.get(row.id)
                if sidx is not None:
                    detail = {}
                    for f in cfg["fields"]:
                        detail[f] = getattr(row, f, None)
                    standalone_jobs[sidx]["domain_detail"] = detail
        except Exception as e:
            logger.debug("Domain enrichment for %s failed: %s", jtype, e)


# ── Helpers ─────────────────────────────────────────────────────────────


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


def _parse_datetime(s: Optional[str]) -> Optional[datetime]:
    """Parse an ISO datetime string, returning None on failure."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _get_active_workers(db: Session) -> list[str]:
    """Return distinct worker_ids from running/claimed jobs."""
    rows = (
        db.query(JobQueue.worker_id)
        .filter(
            JobQueue.status.in_([QueueJobStatus.CLAIMED, QueueJobStatus.RUNNING]),
            JobQueue.worker_id.isnot(None),
        )
        .distinct()
        .all()
    )
    return [r[0] for r in rows]


@router.get("/summary")
async def get_job_summary(db: Session = Depends(get_db)):
    """
    Lightweight summary for dashboard headers.

    Returns only counts — no job list, no enrichment.
    Quick-poll friendly for real-time dashboards.
    """
    # by_status from job_queue
    q_status = dict(
        db.query(JobQueue.status, func.count(JobQueue.id))
        .group_by(JobQueue.status)
        .all()
    )
    by_status = {_enum_val(k): v for k, v in q_status.items()}

    # Add ingestion_jobs counts
    i_status = dict(
        db.query(IngestionJob.status, func.count(IngestionJob.id))
        .group_by(IngestionJob.status)
        .all()
    )
    for k, v in i_status.items():
        sk = _enum_val(k) if not isinstance(k, str) else k
        by_status[sk] = by_status.get(sk, 0) + v

    # by_type from job_queue
    q_types = dict(
        db.query(JobQueue.job_type, func.count(JobQueue.id))
        .group_by(JobQueue.job_type)
        .all()
    )
    by_type = {_enum_val(k): v for k, v in q_types.items()}

    # Count ingestion jobs as "ingestion" type
    i_total = db.query(func.count(IngestionJob.id)).scalar() or 0
    by_type["ingestion"] = by_type.get("ingestion", 0) + i_total

    workers = _get_active_workers(db)

    return {
        "by_status": by_status,
        "by_type": by_type,
        "active_workers": workers,
        "worker_count": len(workers),
    }


@router.get("/history")
async def get_job_history(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(
        None, description="Filter by status (pending, running, success, failed)"
    ),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    trigger: Optional[str] = Query(
        None, description="Filter by trigger (batch, manual, scheduled)"
    ),
    created_after: Optional[str] = Query(
        None, description="ISO datetime — only jobs created after this time"
    ),
    created_before: Optional[str] = Query(
        None, description="ISO datetime — only jobs created before this time"
    ),
    search: Optional[str] = Query(
        None, description="Search job_type and progress_message"
    ),
    db: Session = Depends(get_db),
):
    """
    Unified paginated job history.

    Deduplicates ingestion-type queue jobs that have a linked ingestion_jobs
    record — the ingestion_jobs row is the canonical record and gets enriched
    with worker/progress info from the queue row.  Non-ingestion queue jobs
    (site_intel, people, pe, etc.) appear as their own rows with domain_detail
    enrichment from their domain-specific tables.
    """
    # Parse time range filters
    after_dt = _parse_datetime(created_after)
    before_dt = _parse_datetime(created_before)

    # --- Load queue jobs ---
    q_query = db.query(JobQueue)
    if status:
        q_query = q_query.filter(JobQueue.status == status)
    if job_type:
        q_query = q_query.filter(JobQueue.job_type == job_type)
    if trigger:
        q_query = q_query.filter(
            JobQueue.payload["trigger"].astext == trigger
        )
    if after_dt:
        q_query = q_query.filter(JobQueue.created_at >= after_dt)
    if before_dt:
        q_query = q_query.filter(JobQueue.created_at <= before_dt)
    if search:
        search_like = f"%{search}%"
        q_query = q_query.filter(
            (func.cast(JobQueue.job_type, String).ilike(search_like))
            | (JobQueue.progress_message.ilike(search_like))
        )
    queue_jobs = q_query.order_by(JobQueue.created_at.desc()).all()

    # Build lookup: ingestion_job_id → queue job (for merging)
    queue_by_ingest_id = {}
    standalone_queue = []
    standalone_queue_raw = []  # Keep raw ORM objects for enrichment
    for j in queue_jobs:
        jt = _enum_val(j.job_type)
        linked_id = j.job_table_id
        if jt == "ingestion" and linked_id:
            # This queue row wraps an ingestion_jobs row — merge later
            queue_by_ingest_id[linked_id] = j
        else:
            # Non-ingestion queue job (site_intel, people, etc.)
            source_name = (j.payload or {}).get("source", jt)
            standalone_queue_raw.append(j)
            standalone_queue.append({
                "id": j.id,
                "table": "job_queue",
                "job_type": source_name,
                "queue_job_type": jt,
                "status": _enum_val(j.status),
                "worker_id": j.worker_id,
                "progress_pct": j.progress_pct,
                "progress_message": j.progress_message,
                "payload": j.payload or {},
                "rows_inserted": None,
                "error_message": j.error_message,
                "domain_detail": None,
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "completed_at": j.completed_at.isoformat() if j.completed_at else None,
                "duration_seconds": _duration(j.started_at, j.completed_at),
                "can_retry": _enum_val(j.status) == "failed",
                "can_restart": _enum_val(j.status) in ("failed", "success"),
                "can_cancel": _enum_val(j.status) in ("running", "pending", "claimed"),
                "batch_run_id": (j.payload or {}).get("batch_id"),
                "trigger": (j.payload or {}).get("trigger"),
                "_sort_key": j.created_at,
            })

    # Domain table enrichment for non-ingestion jobs
    try:
        _enrich_domain_detail(standalone_queue, standalone_queue_raw, db)
    except Exception as e:
        logger.debug("Domain enrichment failed: %s", e)

    # --- Load ingestion jobs ---
    i_query = db.query(IngestionJob)
    if status:
        i_query = i_query.filter(IngestionJob.status == status)
    if job_type:
        i_query = i_query.filter(IngestionJob.source == job_type)
    if trigger:
        i_query = i_query.filter(IngestionJob.trigger == trigger)
    if after_dt:
        i_query = i_query.filter(IngestionJob.created_at >= after_dt)
    if before_dt:
        i_query = i_query.filter(IngestionJob.created_at <= before_dt)
    if search:
        search_like = f"%{search}%"
        i_query = i_query.filter(
            (IngestionJob.source.ilike(search_like))
            | (IngestionJob.error_message.ilike(search_like))
        )
    ingest_jobs = i_query.order_by(IngestionJob.created_at.desc()).all()

    ingest_dicts = []
    for j in ingest_jobs:
        # Merge queue info if available
        qj = queue_by_ingest_id.get(j.id)
        ing_status = _enum_val(j.status)
        ingest_dicts.append({
            "id": j.id,
            "table": "ingestion_jobs",
            "job_type": j.source,
            "queue_job_type": "ingestion",
            "status": ing_status,
            "worker_id": qj.worker_id if qj else None,
            "progress_pct": (qj.progress_pct if qj else None)
                or (100.0 if ing_status == "success" else None),
            "progress_message": qj.progress_message if qj else None,
            "payload": j.config or {},
            "rows_inserted": j.rows_inserted,
            "error_message": j.error_message,
            "domain_detail": None,
            "created_at": j.created_at.isoformat() if j.created_at else None,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            "duration_seconds": _duration(j.started_at, j.completed_at),
            "retry_count": j.retry_count or 0,
            "max_retries": j.max_retries or 3,
            "can_retry": ing_status == "failed" and (j.retry_count or 0) < (j.max_retries or 3),
            "can_restart": ing_status in ("failed", "success"),
            "can_cancel": ing_status in ("running", "pending"),
            "batch_run_id": j.batch_run_id,
            "trigger": j.trigger,
            "_sort_key": j.created_at,
        })

    # --- Merge and sort ---
    all_jobs = standalone_queue + ingest_dicts
    _epoch = datetime(1970, 1, 1)
    all_jobs.sort(key=lambda x: x["_sort_key"] or _epoch, reverse=True)

    # Summary counts
    summary: dict = {"running": 0, "pending": 0, "success": 0, "failed": 0}
    by_type: dict[str, int] = defaultdict(int)
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
        by_type[j.get("queue_job_type") or j["job_type"]] += 1

    summary["by_type"] = dict(by_type)

    # Active workers
    workers = _get_active_workers(db)
    summary["active_workers"] = workers
    summary["worker_count"] = len(workers)

    total = len(all_jobs)

    # Apply pagination
    page = all_jobs[offset : offset + limit]

    # Remove internal keys
    for j in page:
        j.pop("_sort_key", None)
        j.pop("queue_job_type", None)

    # Enrich with LLM cost data (batch query for this page)
    try:
        from app.core.models import LLMUsage
        page_job_ids = [j["id"] for j in page if j.get("table") == "ingestion_jobs"]
        if page_job_ids:
            cost_rows = (
                db.query(
                    LLMUsage.job_id,
                    func.count().label("llm_calls"),
                    func.sum(LLMUsage.input_tokens).label("input_tokens"),
                    func.sum(LLMUsage.output_tokens).label("output_tokens"),
                    func.sum(LLMUsage.cost_usd).label("cost_usd"),
                )
                .filter(LLMUsage.job_id.in_(page_job_ids))
                .group_by(LLMUsage.job_id)
                .all()
            )
            cost_map = {r.job_id: {
                "llm_calls": r.llm_calls,
                "llm_tokens": (r.input_tokens or 0) + (r.output_tokens or 0),
                "llm_cost_usd": float(r.cost_usd or 0),
            } for r in cost_rows}
            for j in page:
                if j.get("table") == "ingestion_jobs":
                    j["llm_cost"] = cost_map.get(j["id"])
    except Exception:
        pass  # LLM cost enrichment is optional

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "summary": summary,
        "jobs": page,
    }
