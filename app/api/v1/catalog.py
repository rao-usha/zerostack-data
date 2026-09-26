"""
Dataset catalog endpoints (SPEC_123, SPEC_144).

    GET  /api/v1/catalog                  the declared datasets, filterable
    GET  /api/v1/catalog/row-trends       daily row counts per dataset (status page sparkline)
    GET  /api/v1/catalog/usage            consumers of every dataset + undeclared view inputs
    POST /api/v1/catalog/admin/backfill-dataset-key   ingestion_jobs.dataset_key backfill (admin)
    GET  /api/v1/catalog/{key}            one dataset + live row counts and coverage,
                                          quality block and consumers

The list is static (no database). The detail counts rows on the declared
tables under a statement timeout and caches the result for a minute; only an
admin may bypass the cache (``refresh=true``), since a refresh re-runs the
counts on the largest tables. The quality block and consumers are best-effort:
if they cannot be computed they are ``null``, never a 500. A plain GET's
quality block scans no table (it reuses the live counts and the profiler's
stored seed counts); ``refresh=true`` re-scans. Every list entry carries the
static ``quality_flags`` (fabricated, seeded, sample_mixed ... from the
verified state), so flagged data is visible without a detail call.
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

ROW_TRENDS_MAX_DAYS = 90


def _check(name: str, value: Optional[str], allowed) -> None:
    if value is not None and value not in allowed:
        raise HTTPException(status_code=422, detail=f"{name} must be one of {list(allowed)}")


def _require_admin(principal: Dict[str, Any]) -> None:
    if principal.get("role") != ROLE_ADMIN:
        raise HTTPException(status_code=403, detail="requires the admin role")


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
    from app.catalog.quality import static_flags

    return {
        "count": len(specs),
        "total": len(get_catalog()),
        "datasets": [dict(s.to_dict(), quality_flags=static_flags(s)) for s in specs],
    }


@router.get("/row-trends")
def catalog_row_trends(
    days: int = Query(30, ge=2, le=ROW_TRENDS_MAX_DAYS, description="window in days"),
    db: Session = Depends(get_db),
):
    """Daily row counts per dataset (summed over its tables) from the daily
    quality snapshots, oldest first. Datasets with no snapshot are left out."""
    from app.catalog.quality import row_trends

    try:
        trends = row_trends(db.get_bind(), get_catalog(), days=days)
    except Exception as e:
        logger.warning(f"[catalog] row trends failed: {type(e).__name__}")
        trends = {}
    return {"days": days, "datasets": trends}


@router.get("/usage")
def catalog_usage(db: Session = Depends(get_db)):
    """Who reads each dataset (static code scan + view dependencies), the
    datasets nothing reads, and tables views read that no spec declares."""
    from app.catalog.quality import base_tables, relations
    from app.catalog.usage_build import consumers_for, undeclared_view_inputs, view_dependencies

    engine = db.get_bind()
    deps = view_dependencies(engine)
    try:
        base = base_tables(relations(engine, cached=True))
    except Exception:
        base = set()
    specs = get_catalog()
    datasets = {s.key: consumers_for(s, deps=deps, existing=base) for s in specs}
    return {
        "datasets": {k: {"count": v["count"], "views": v["views"],
                         "code": sorted({e["path"] for e in v["code"]})}
                     for k, v in datasets.items()},
        "unread": sorted(k for k, v in datasets.items() if v["count"] == 0),
        "undeclared_view_inputs": undeclared_view_inputs(specs, deps, base),
        "view_dependencies_available": bool(deps),
    }


@router.post("/admin/backfill-dataset-key")
def backfill_dataset_key(
    apply: bool = Query(False, description="write; false = dry run (the count report only)"),
    db: Session = Depends(get_db),
    principal: Dict[str, Any] = Depends(current_principal),
):
    """Fill ``ingestion_jobs.dataset_key`` on historical rows from the catalog
    (``job_keys.dataset_key_for_job``). Idempotent: only NULL rows, never an
    overwrite; ambiguous and unresolved rows stay NULL and are counted."""
    _require_admin(principal)
    from app.catalog.backfill_dataset_key import backfill

    return backfill(db.get_bind(), apply=apply)


@router.get("/{key}")
def get_catalog_entry(
    key: str,
    refresh: bool = Query(False, description="bypass the 60 s cache (admin only)"),
    db: Session = Depends(get_db),
    principal: Dict[str, Any] = Depends(current_principal),
):
    """One dataset with live row counts per table and the coverage clock,
    its quality block (SPEC_144) and the code and views that read it."""
    spec = get_spec(key)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"unknown dataset {key!r}")
    if refresh and principal.get("role") != ROLE_ADMIN:
        raise HTTPException(status_code=403, detail="refresh=true requires the admin role")
    from app.catalog.quality import static_flags

    body = spec.to_dict()
    body["quality_flags"] = static_flags(spec)
    engine = db.get_bind()
    try:
        body["live"] = dataset_live(engine, spec, refresh=refresh)
    except Exception as e:
        logger.warning(f"[catalog] live stats for {key} failed: {type(e).__name__}")
        body["live"] = None
    try:
        from app.catalog.quality import quality_block

        # a plain GET reuses the counts above and the profiler's seed counts;
        # only an admin refresh re-scans the tables (SPEC_144 review)
        body["quality"] = quality_block(engine, spec, refresh=refresh, live=body["live"])
    except Exception as e:
        logger.warning(f"[catalog] quality block for {key} failed: {type(e).__name__}")
        body["quality"] = None
    try:
        from app.catalog.usage_build import consumers_for, view_dependencies

        body["consumers"] = consumers_for(spec, deps=view_dependencies(engine))
    except Exception as e:
        logger.warning(f"[catalog] consumers for {key} failed: {type(e).__name__}")
        body["consumers"] = None
    return body
