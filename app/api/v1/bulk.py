"""
Bulk file ingestion endpoints (SPEC_107).

Loads publisher bulk files (SEC data sets, ...) through the worker queue.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.job_queue_service import submit_job
from app.core.scheduler_service import install_default_bulk_schedules
from app.ingest.bulk.registry import list_sources

router = APIRouter(prefix="/bulk", tags=["Bulk Ingestion"])


@router.get("/sources", summary="List registered bulk sources")
def get_bulk_sources():
    return {"sources": list_sources()}


@router.post("/{source}/run", summary="Queue a bulk load for one source")
def run_bulk_source(
    source: str,
    since: Optional[str] = Query(None, description="Only releases on/after YYYY-MM-DD"),
    max_releases: Optional[int] = Query(None, ge=1, description="Cap releases processed this run"),
    db: Session = Depends(get_db),
):
    if source not in list_sources():
        raise HTTPException(status_code=404, detail=f"Unknown bulk source: {source}")
    payload = {"bulk_source": source, "since": since, "max_releases": max_releases}
    result = submit_job(db=db, job_type="bulk_ingest", payload=payload)
    return {"source": source, **result}


@router.post("/schedules/install", summary="Create the default bulk schedules")
def install_bulk_schedules(db: Session = Depends(get_db)):
    """Idempotent: creates missing schedules, leaves existing ones untouched."""
    return install_default_bulk_schedules(db)


@router.get("/schedules", summary="List bulk ingestion schedules")
def list_bulk_schedules(db: Session = Depends(get_db)):
    from app.core.models import IngestionSchedule

    rows = (
        db.query(IngestionSchedule)
        .filter(IngestionSchedule.source.like("bulk:%"))
        .order_by(IngestionSchedule.name)
        .all()
    )
    return {
        "schedules": [
            {
                "id": s.id,
                "name": s.name,
                "source": s.source,
                "cron_expression": s.cron_expression,
                "is_active": bool(s.is_active),
                "last_run_at": s.last_run_at,
                "next_run_at": s.next_run_at,
                "last_job_id": s.last_job_id,
            }
            for s in rows
        ]
    }


@router.get("/releases", summary="Release manifest (raw.source_release)")
def get_releases(
    source: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(200, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            """
            SELECT source, release_key, status, bytes, rows_loaded, error,
                   fetched_at, loaded_at, updated_at
            FROM raw.source_release
            WHERE (CAST(:source AS TEXT) IS NULL OR source = :source)
              AND (CAST(:status AS TEXT) IS NULL OR status = :status)
            ORDER BY source, release_key DESC
            LIMIT :limit
            """
        ),
        {"source": source, "status": status, "limit": limit},
    ).mappings().all()
    return {"releases": [dict(r) for r in rows]}
