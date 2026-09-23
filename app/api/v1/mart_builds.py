"""
Mart build ledger (SPEC_126a): what each PE-mart / entity-master build consumed
and how it ended.

A separate module from `pe_marts` on purpose: that router is admin for every
method (it queues builds), this one is a read any signed-in user may make
(`_auth` in main.py). Because of that the free text a build row carries is
treated like SPEC_124 treats run errors (SPEC_125): ``error`` goes to admins
only, and every free-text field (error, refusal_reason, input problems) is
passed through ``redact`` first.
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.authz import ROLE_ADMIN, current_principal
from app.core.database import get_db
from app.marts.build_ledger import recent_builds
from app.services.dataset_status import redact

router = APIRouter(prefix="/pe/marts", tags=["PE Intelligence - Marts"])

STATUSES = ("running", "success", "failed", "refused")


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def _redact_inputs(inputs: Any) -> Any:
    if not isinstance(inputs, list):
        return inputs
    out = []
    for i in inputs:
        if isinstance(i, dict) and isinstance(i.get("problem"), str):
            i = {**i, "problem": redact(i["problem"])}
        out.append(i)
    return out


def present_build(row: Dict[str, Any], include_errors: bool) -> Dict[str, Any]:
    """One core.mart_build row as the API shows it: ISO timestamps, free text
    redacted, and the raw ``error`` only when ``include_errors`` (admins)."""
    b = {k: _iso(v) for k, v in row.items()}
    if include_errors:
        b["error"] = redact(b.get("error")) if b.get("error") else b.get("error")
    else:
        b["error_hidden"] = bool(b.get("error"))
        b["error"] = None
    if isinstance(b.get("refusal_reason"), str):
        b["refusal_reason"] = redact(b["refusal_reason"])
    if "inputs" in b:
        b["inputs"] = _redact_inputs(b["inputs"])
    return b


@router.get("/builds", summary="Recent mart builds: inputs consumed, gates, outcome")
def list_builds(
    mart: Optional[str] = Query(None, description="pe_marts | entity_resolve"),
    status: Optional[str] = Query(None, description="running | success | failed | refused"),
    limit: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
    principal: Dict[str, Any] = Depends(current_principal),
):
    """Error text (redacted) is shown to admins only, as in GET /datasets/status."""
    mart = mart if isinstance(mart, str) and mart else None
    status = status if isinstance(status, str) and status in STATUSES else None
    if db.execute(text("SELECT to_regclass('core.mart_build')")).scalar() is None:
        return {"count": 0, "builds": [], "note": "core.mart_build not created yet (alembic 0013)"}
    rows = recent_builds(db, mart=mart, status=status, limit=limit)
    include_errors = isinstance(principal, dict) and principal.get("role") == ROLE_ADMIN
    builds = [present_build(r, include_errors) for r in rows]
    return {"count": len(builds), "builds": builds}
