"""
Dataset catalog endpoints (SPEC_123).

    GET /api/v1/catalog          the declared datasets, filterable
    GET /api/v1/catalog/{key}    one dataset + live row counts and coverage

The list is static (no database). The detail counts rows on the declared
tables under a statement timeout and caches the result for a minute; only an
admin may bypass the cache (``refresh=true``), since a refresh re-runs the
counts on the largest tables.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.catalog import filter_specs, get_catalog, get_spec
from app.catalog.live import dataset_live
from app.catalog.spec import KINDS, REDISTRIBUTION, STATUS_PUBLIC
from app.core.authz import ROLE_ADMIN, current_principal
from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/catalog", tags=["catalog"])


def _check(name: str, value: Optional[str], allowed) -> None:
    if value is not None and value not in allowed:
        raise HTTPException(status_code=422, detail=f"{name} must be one of {list(allowed)}")


@router.get("")
def list_catalog(
    kind: Optional[str] = Query(None, description=f"one of {', '.join(KINDS)}"),
    source: Optional[str] = Query(None, description="source family, e.g. sec, fred, site_intel"),
    status_public: Optional[str] = Query(None, description=f"one of {', '.join(STATUS_PUBLIC)}"),
    redistribution: Optional[str] = Query(None, description=f"one of {', '.join(REDISTRIBUTION)}"),
    q: Optional[str] = Query(None, description="substring of key, name or description"),
):
    """The declared datasets: what each is, how it is produced, and its rights."""
    _check("kind", kind, KINDS)
    _check("status_public", status_public, STATUS_PUBLIC)
    _check("redistribution", redistribution, REDISTRIBUTION)
    specs = filter_specs(kind=kind, source=source, status_public=status_public,
                         redistribution=redistribution, q=q)
    return {
        "count": len(specs),
        "total": len(get_catalog()),
        "datasets": [s.to_dict() for s in specs],
    }


@router.get("/{key}")
def get_catalog_entry(
    key: str,
    refresh: bool = Query(False, description="bypass the 60 s cache (admin only)"),
    db: Session = Depends(get_db),
    principal: Dict[str, Any] = Depends(current_principal),
):
    """One dataset with live row counts per table and the coverage clock."""
    spec = get_spec(key)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"unknown dataset {key!r}")
    if refresh and principal.get("role") != ROLE_ADMIN:
        raise HTTPException(status_code=403, detail="refresh=true requires the admin role")
    body = spec.to_dict()
    try:
        body["live"] = dataset_live(db.get_bind(), spec, refresh=refresh)
    except Exception as e:
        logger.warning(f"[catalog] live stats for {key} failed: {type(e).__name__}")
        body["live"] = None
    return body
