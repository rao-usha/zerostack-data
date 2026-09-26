"""
Column dictionary endpoints (SPEC_137).

    GET  /api/v1/catalog/columns                 search columns across datasets
    GET  /api/v1/catalog/columns/coverage        description coverage per dataset
    GET  /api/v1/catalog/joins                   join-key index by semantic type
    GET  /api/v1/catalog/{key}/schema            columns, types, keys, PII, profile stats
    GET  /api/v1/catalog/{key}/sample            <= 20 rows, PII-masked for non-admins
    GET  /api/v1/catalog/{key}/joins             datasets sharing an identifier, with join SQL
    POST /api/v1/catalog/columns/comments/sync   COMMENT ON COLUMN sync (admin)

Mounted with ``require_admin_for_writes`` and included **before** the
``catalog`` router in ``app/main.py``: ``/catalog/columns`` and
``/catalog/joins`` would otherwise match ``GET /catalog/{key}``.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from app.catalog import get_spec
from app.catalog.identifiers import SEMANTIC_TYPES, join_types
from app.catalog.spec import PII_CLASSES
from app.core.authz import ROLE_ADMIN, current_principal, require_admin
from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/catalog", tags=["catalog"])


def _spec_or_404(key: str):
    spec = get_spec(key)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"unknown dataset {key!r}")
    return spec


def _is_admin(principal: Dict[str, Any]) -> bool:
    return principal.get("role") == ROLE_ADMIN


def _resolved(engine, spec):
    from app.catalog.live import existing_tables, resolve_tables
    from app.catalog.tables import split

    existing = existing_tables(engine, {split(t)[0] for t in spec.tables})
    return resolve_tables(spec, existing), existing


# ---------------------------------------------------------------------------
# Static (dictionary only)
# ---------------------------------------------------------------------------


@router.get("/columns")
def search_catalog_columns(
    q: Optional[str] = Query(None, description="substring of column name, description or table"),
    semantic_type: Optional[str] = Query(None, description=f"one of {', '.join(SEMANTIC_TYPES)}"),
    pii: Optional[str] = Query(None, description=f"one of {', '.join(PII_CLASSES)}"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Columns across all catalog tables (static dictionary; tables the code declares)."""
    from app.catalog.dictionary import dictionary_hash, search_columns

    if semantic_type is not None and semantic_type not in SEMANTIC_TYPES:
        raise HTTPException(status_code=422, detail=f"semantic_type must be one of {list(SEMANTIC_TYPES)}")
    if pii is not None and pii not in PII_CLASSES:
        raise HTTPException(status_code=422, detail=f"pii must be one of {list(PII_CLASSES)}")
    total, hits = search_columns(q=q, semantic_type=semantic_type, pii=pii, limit=limit)
    return {"count": len(hits), "total": total, "dictionary_hash": dictionary_hash(), "columns": hits}


@router.get("/columns/coverage")
def catalog_column_coverage():
    """Share of columns with a description, per dataset, for the PE/entity pack and overall."""
    from app.catalog.dictionary import coverage_report, dictionary_hash

    return {"dictionary_hash": dictionary_hash(), **coverage_report()}


@router.get("/joins")
def catalog_join_keys(
    semantic_type: Optional[str] = Query(None, description="restrict to one identifier type"),
):
    """Identifier columns by semantic type, with the expression that normalises each one."""
    from app.catalog.dictionary import join_index

    jt = join_types()
    if semantic_type is not None and semantic_type not in jt:
        raise HTTPException(status_code=422, detail=f"semantic_type must be one of {sorted(jt)}")
    idx = join_index(semantic_type)
    return {
        "types": {k: {"label": v.label, "canonical_format": v.canonical_format,
                      "normalize_sql": v.normalize_sql, "specificity": v.specificity}
                  for k, v in jt.items() if not semantic_type or k == semantic_type},
        "join_keys": idx,
    }


@router.get("/{key}/joins")
def catalog_dataset_joins(key: str, limit_per_type: int = Query(25, ge=1, le=200)):
    """Other datasets sharing an identifier with ``key``, most specific first, with join SQL."""
    from app.catalog.dictionary import related_datasets

    _spec_or_404(key)
    related = related_datasets(key, limit_per_type=limit_per_type)
    return {"dataset": key, "count": len(related), "related": related}


# ---------------------------------------------------------------------------
# Live
# ---------------------------------------------------------------------------


@router.get("/{key}/schema")
def catalog_dataset_schema(
    key: str,
    response: Response,
    table: Optional[str] = Query(None, description="one of the dataset's tables"),
    db: Session = Depends(get_db),
    principal: Dict[str, Any] = Depends(current_principal),
):
    """Columns of the dataset's tables: type, nullability, description, unit, semantic type,
    PII, source, profile null % / distinct count, and the table's keys. Examples drawn from
    profiled row values are withheld from non-admins of restricted or export-denied tables."""
    from app.catalog.schema_live import MAX_SCHEMA_TABLES, dataset_schema

    spec = _spec_or_404(key)
    engine = db.get_bind()
    if spec.table_patterns:
        tables, existing = _resolved(engine, spec)
    else:
        # no pattern to expand: skip the inspector round trips (PLAN_088: < 300 ms); the
        # facts query itself reports a declared table that does not exist (exists=False)
        tables, existing = list(spec.tables), set(spec.tables)
    if table is not None:
        if table not in tables:
            raise HTTPException(status_code=404, detail=f"{table!r} is not a table of {key!r}")
        chosen = [table]
    else:
        chosen = tables[:MAX_SCHEMA_TABLES]
    admin = _is_admin(principal)
    body = dataset_schema(engine, spec, chosen, existing, len(tables) if table is None else 1, admin=admin)
    response.headers["ETag"] = f'W/"{body["dictionary_hash"]}{"-a" if admin else ""}"'
    return body


@router.get("/{key}/sample")
def catalog_dataset_sample(
    key: str,
    response: Response,
    table: Optional[str] = Query(None, description="one of the dataset's tables (default: the first that exists)"),
    limit: int = Query(10, ge=1, le=20),
    db: Session = Depends(get_db),
    principal: Dict[str, Any] = Depends(current_principal),
):
    """Up to 20 rows of documented columns. PII is masked for non-admin callers; guessed
    emails are always NULL; restricted datasets are admin-only."""
    from app.catalog.schema_live import SampleForbidden, TableNotFound, attribution_of, build_sample

    spec = _spec_or_404(key)
    engine = db.get_bind()
    tables, existing = _resolved(engine, spec)
    if table is None:
        table = next((t for t in tables if t in existing), None)
        if table is None:
            raise HTTPException(status_code=404, detail=f"no table of {key!r} exists")
    elif table not in tables:
        raise HTTPException(status_code=404, detail=f"{table!r} is not a table of {key!r}")
    try:
        body = build_sample(engine, spec, table, limit, _is_admin(principal))
    except TableNotFound:
        raise HTTPException(status_code=404, detail=f"table {table!r} does not exist")
    except SampleForbidden as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:  # statement timeout etc.; the error text can name internals
        logger.warning(f"[catalog] sample of {key}/{table} failed: {type(e).__name__}")
        raise HTTPException(status_code=503, detail="sample unavailable")
    response.headers["X-Dataset-Attribution"] = attribution_of(spec)
    response.headers["X-Dataset-Origin"] = ",".join(body["rights"]["origins"])
    return body


@router.post("/columns/comments/sync", dependencies=[Depends(require_admin)])
def sync_catalog_column_comments(
    dry_run: bool = Query(True, description="report what would be written without writing"),
    db: Session = Depends(get_db),
):
    """Write dictionary descriptions to COMMENT ON COLUMN where a column has none (admin)."""
    from app.catalog.mirror import sync_column_comments

    return sync_column_comments(db.get_bind(), dry_run=dry_run)
