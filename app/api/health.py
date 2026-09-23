"""
Liveness, readiness and health probes (SPEC_128).

- ``/livez``  -- the process is up. Never touches the database.
- ``/readyz`` -- the database answers ``SELECT 1``; 503 otherwise.
- ``/health`` -- backward-compatible summary (``status``, ``service``,
  ``database``, ``worker``) plus worker liveness from ``worker_heartbeats``
  and queue depth; 503 when the database is unreachable.

The old ``/health`` closed its connection and then reused it, swallowed the
error, and matched uppercase statuses the queue never stores -- so it always
said ``worker="unknown"``, ``status="healthy"``. Error text from the driver
(hosts, users) is logged, never returned.
"""

import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.core.database import get_engine

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Root"])

HEARTBEAT_WINDOW = "5 minutes"


@router.get("/livez")
def livez():
    """Process liveness. 200 while the app can serve a request at all."""
    return {"status": "alive"}


@router.get("/readyz")
def readyz():
    """Readiness: 200 when the database is reachable, else 503."""
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        logger.warning(f"Readiness check: database unreachable ({type(e).__name__}: {e})")
        return JSONResponse(status_code=503, content={"status": "not_ready", "database": "unreachable"})
    return {"status": "ready", "database": "connected"}


def _worker_state(conn) -> dict:
    """Worker liveness from worker_heartbeats, queue depth from job_queue."""
    from app.core import job_queue_service

    alive = conn.execute(text(
        f"SELECT COUNT(*) FROM worker_heartbeats "
        f"WHERE last_seen_at >= NOW() - INTERVAL '{HEARTBEAT_WINDOW}'"
    )).scalar() or 0
    rows = conn.execute(text(
        "SELECT LOWER(status), COUNT(*) FROM job_queue "
        "WHERE LOWER(status) IN ('pending', 'blocked', 'claimed', 'running') "
        "GROUP BY LOWER(status)"
    )).all()
    counts = {str(r[0]): int(r[1]) for r in rows}
    queue = {
        "pending": counts.get("pending", 0),
        "blocked": counts.get("blocked", 0),
        "running": counts.get("claimed", 0) + counts.get("running", 0),
    }
    if alive:
        worker = "active" if queue["running"] else "idle"
    else:
        worker = "unavailable"
    return {
        "worker": worker,
        "workers_alive": int(alive),
        "queue": queue,
        "worker_mode": bool(job_queue_service.WORKER_MODE),
    }


@router.get("/health")
def health_check():
    """
    Health check endpoint.

    Returns status of the service, database connectivity, worker liveness
    (from ``worker_heartbeats``) and job queue depth. 503 when the database
    is unreachable; ``degraded`` when jobs go to the queue and no worker is alive.
    """
    health_status = {"status": "healthy", "service": "running", "database": "unknown", "worker": "unknown"}

    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
            health_status["database"] = "connected"
            try:
                health_status.update(_worker_state(conn))
            except Exception as e:
                logger.warning(f"Health check: worker state unavailable ({type(e).__name__}: {e})")
                health_status["worker"] = "unknown"
                health_status["status"] = "degraded"
    except Exception as e:
        logger.warning(f"Database health check failed ({type(e).__name__}: {e})")
        health_status["status"] = "unhealthy"
        health_status["database"] = "unreachable"
        return JSONResponse(status_code=503, content=health_status)

    if health_status["worker"] == "unavailable" and health_status.get("worker_mode"):
        health_status["status"] = "degraded"

    return health_status
