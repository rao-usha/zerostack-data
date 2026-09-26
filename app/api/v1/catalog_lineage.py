"""
Catalog lineage endpoints (SPEC_143).

    GET /api/v1/catalog/lineage          the whole graph: nodes, edges, drift
    GET /api/v1/catalog/{key}/lineage    upstream / downstream of one dataset

Read-only and user-level (mounted with ``require_admin_for_writes``). The graph
is computed on read from the catalog, the producers' SQL, the mart stage maps,
``pg_depend`` and ``core.mart_build`` (``app/catalog/lineage.py``), cached 60 s.
Included **before** the ``catalog`` router in ``app/main.py``: ``/catalog/lineage``
would otherwise match ``GET /catalog/{key}``.

These replace the legacy ``/lineage`` router (``lineage_service``), which
nothing wrote to; its tables are left in the database, unserved.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.catalog import get_spec
from app.catalog import lineage as lineage_mod
from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/catalog", tags=["catalog"])


def _graph(db: Session, live: bool, consumers: bool = False):
    engine = None
    if live:
        try:
            engine = db.get_bind()
        except Exception:  # no bind: serve the static graph
            engine = None
    return lineage_mod.full_graph(engine, live=live, consumers=consumers)


@router.get("/lineage")
def catalog_lineage_graph(
    live: bool = Query(True, description="add pg_depend view edges and the mart ledger"),
    consumers: bool = Query(False, description="add code readers from usage.json"),
    db: Session = Depends(get_db),
):
    """Every dataset with its producers, tables, views and inputs, plus the
    drift between declared, SQL, stage-map and observed inputs."""
    return _graph(db, live, consumers)


@router.get("/{key}/lineage")
def catalog_dataset_lineage(
    key: str,
    direction: str = Query("both", description="up | down | both"),
    depth: int = Query(lineage_mod.DEFAULT_DEPTH, ge=1, le=lineage_mod.MAX_DEPTH),
    live: bool = Query(True, description="add pg_depend view edges and the mart ledger"),
    status: bool = Query(False, description="join the SPEC_124 dataset status verdicts "
                                            "(impact analysis)"),
    db: Session = Depends(get_db),
):
    """Upstream and downstream datasets of one dataset, to ``depth`` hops.

    With ``status=true`` every reached dataset carries its ``GET /datasets/status``
    verdict, and ``impact`` lists the upstream problems and the downstream
    datasets they put at risk."""
    if get_spec(key) is None:
        raise HTTPException(status_code=404, detail=f"unknown dataset {key!r}")
    if direction not in lineage_mod.DIRECTIONS:
        raise HTTPException(status_code=422,
                            detail=f"direction must be one of {list(lineage_mod.DIRECTIONS)}")
    w = lineage_mod.walk(_graph(db, live), key, direction=direction, depth=depth)
    if status:
        w = lineage_mod.impact(w, _statuses(db, [key] + [r["key"] for r in
                                                         w["upstream"] + w["downstream"]]))
    return w


def _statuses(db: Session, keys):
    """key -> verdict for ``keys``; None when the status cannot be computed."""
    try:
        from app.services.dataset_status import build_status

        body = build_status(db, scheduler=None, include_errors=False, keys=keys)
    except Exception as e:
        logger.warning(f"[catalog_lineage] dataset status unavailable: {type(e).__name__}")
        try:
            db.rollback()
        except Exception:
            pass
        return None
    return {d["key"]: {"status": d["status"], "status_reason": d["status_reason"]}
            for d in body.get("datasets", [])}
