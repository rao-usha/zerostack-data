"""
Dataset status endpoints (SPEC_124).

    GET  /api/v1/datasets/status        every catalog dataset: three clocks,
                                        one status, schedule, run verdict
    POST /api/v1/datasets/{key}/run     admin: enqueue the dataset's producer,
                                        or 409 with the verdict that refused it

Reads are user-level (the router is mounted with ``require_admin_for_writes``);
the run route also depends on ``require_admin`` directly, so it stays
admin-only however the router is mounted.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.catalog.spec import KINDS, STATUS_PUBLIC
from app.core.authz import require_admin
from app.core.database import get_db
from app.services import dataset_status as ds

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/datasets", tags=["dataset-status"])


# =============================================================================
# Response models
# =============================================================================


class Reason(BaseModel):
    code: str
    message: str


class TableStat(BaseModel):
    name: str
    exists: bool
    rows: Optional[int] = None
    rows_exact: bool = False
    bytes: Optional[int] = None


class Clocks(BaseModel):
    last_run_at: Optional[str] = None
    last_run_status: Optional[str] = None
    last_run_store: Optional[str] = None
    last_run_duration_s: Optional[float] = None
    last_success_at: Optional[str] = None
    last_publish_at: Optional[str] = None
    coverage_through: Optional[str] = None
    expected_through: Optional[str] = None
    expectation_basis: Optional[str] = None  # coverage | last_success | None
    lag_days: Optional[int] = None


class Releases(BaseModel):
    loaded: int
    unloaded: int
    failed: int
    superseded: int
    latest_release_key: Optional[str] = None
    last_bytes: Optional[int] = None


class Schedule(BaseModel):
    kind: str  # schedule | batch
    schedule_id: Optional[int] = None
    name: Optional[str] = None
    cron: Optional[str] = None
    active: bool
    cadence_hours: Optional[float] = None
    next_run_at: Optional[str] = None
    next_run_source: Optional[str] = None  # apscheduler | db | None
    last_success_at: Optional[str] = None
    missed_runs_30d: Optional[int] = None


class CanRun(BaseModel):
    allowed: bool
    requires: str
    policy: str
    producer: str
    run_path: Optional[str] = None
    blockers: List[Reason]
    warnings: List[Reason]
    estimated_duration_s: Optional[float] = None
    estimated_bytes: Optional[int] = None


class DatasetStatus(BaseModel):
    key: str
    display_name: str
    source: str
    kind: str
    grain: str
    cadence: str
    status_public: str
    producer: str
    status: str
    status_reason: str
    tables: List[TableStat]
    rows_total: Optional[int] = None
    rows_exact: bool
    clocks: Clocks
    releases: Optional[Releases] = None
    schedule: Optional[Schedule] = None
    blockers: List[Reason]
    can_run: CanRun


class WorkerState(BaseModel):
    worker_mode: bool
    live_workers: int
    last_heartbeat_at: Optional[str] = None


class DatasetStatusResponse(BaseModel):
    generated_at: str
    summary: Dict[str, int]
    count: int
    total: int
    worker: WorkerState
    datasets: List[DatasetStatus]


class RunResponse(BaseModel):
    dataset_key: str
    producer: str
    run_path: Optional[str] = None
    mode: Optional[str] = None
    ingestion_job_id: Optional[int] = None
    job_queue_id: Optional[int] = None
    audit_id: Optional[int] = None
    can_run: CanRun


# =============================================================================
# Routes
# =============================================================================


def _check(name: str, value: Optional[str], allowed) -> None:
    if value is not None and value not in allowed:
        raise HTTPException(status_code=422, detail=f"{name} must be one of {list(allowed)}")


@router.get("/status", response_model=DatasetStatusResponse)
def get_dataset_status(
    status_public: Optional[str] = Query(None, description=f"one of {', '.join(STATUS_PUBLIC)}"),
    kind: Optional[str] = Query(None, description=f"one of {', '.join(KINDS)}"),
    source: Optional[str] = Query(None, description="source family, e.g. sec, fred, site_intel"),
    status: Optional[str] = Query(None, description=f"one of {', '.join(ds.STATUSES)}"),
    db: Session = Depends(get_db),
):
    """Every catalog dataset with its run, publish and coverage clocks and one status.

    Statuses: current, awaiting_upstream, behind, stalled, failing, partial,
    blocked, dormant, never_run, unknown. There is no ``current`` without an
    expectation (an SLO): that is ``unknown``. ``summary`` counts the datasets
    left after the kind/source/status_public filters.
    """
    _check("status_public", status_public, STATUS_PUBLIC)
    _check("kind", kind, KINDS)
    _check("status", status, ds.STATUSES)
    return ds.build_status(db, kind=kind, source=source, status_public=status_public,
                           status=status)


@router.post("/{key}/run", status_code=202, response_model=RunResponse,
             responses={409: {"description": "refused; body carries the can_run verdict"}})
def run_dataset(
    key: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    principal: Dict[str, Any] = Depends(require_admin),
):
    """Enqueue the dataset's producer (admin only).

    Refuses with 409 and the same ``can_run`` verdict ``GET /datasets/status``
    shows when a blocker applies (missing API key, no worker, no run path,
    already running). The actor is written to ``collection_audit_log``.
    """
    specs = ds.catalog_specs()
    spec = next((s for s in specs if s.key == key), None)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"unknown dataset {key!r}")

    report = ds.build_status(db, specs=specs, keys=[key])
    verdict = report["datasets"][0]["can_run"]
    if not verdict["allowed"]:
        codes = ", ".join(b["code"] for b in verdict["blockers"])
        return JSONResponse(status_code=409, content={
            "detail": f"{key} cannot run now: {codes}",
            "can_run": verdict,
        })

    actor = principal.get("email") or principal.get("name")
    out = ds.enqueue_run(db, spec, actor=actor, background_tasks=background_tasks, specs=specs)
    logger.info(f"[dataset_status] {actor} queued {key} via {spec.producer}: {out}")
    return {**out, "can_run": verdict}
