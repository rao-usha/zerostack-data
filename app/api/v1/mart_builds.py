"""
Mart build ledger (SPEC_126a): what each PE-mart / entity-master build consumed
and how it ended.

A separate module from `pe_marts` on purpose: that router is admin for every
method (it queues builds), this one is a read any signed-in user may make
(`_auth` in main.py).
"""

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.marts.build_ledger import recent_builds

router = APIRouter(prefix="/pe/marts", tags=["PE Intelligence - Marts"])

STATUSES = ("running", "success", "failed", "refused")


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


@router.get("/builds", summary="Recent mart builds: inputs consumed, gates, outcome")
def list_builds(
    mart: Optional[str] = Query(None, description="pe_marts | entity_resolve"),
    status: Optional[str] = Query(None, description="running | success | failed | refused"),
    limit: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
):
    mart = mart if isinstance(mart, str) and mart else None
    status = status if isinstance(status, str) and status in STATUSES else None
    if db.execute(text("SELECT to_regclass('core.mart_build')")).scalar() is None:
        return {"count": 0, "builds": [], "note": "core.mart_build not created yet (alembic 0013)"}
    rows = recent_builds(db, mart=mart, status=status, limit=limit)
    builds = [{k: _iso(v) for k, v in r.items()} for r in rows]
    return {"count": len(builds), "builds": builds}
