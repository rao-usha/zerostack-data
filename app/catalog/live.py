"""
Live facts for a catalog dataset (SPEC_123): row counts and coverage.

Row counts are ``count(*)`` on the declared tables — never ``rows_inserted``
from job history. Each count runs in its own short transaction under
``SET LOCAL statement_timeout``; a count that times out falls back to the
planner estimate (``pg_class.reltuples``) and says so with
``rows_exact: false``. Results are cached in-process for ``CACHE_TTL_S``.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Set

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from app.catalog.spec import DatasetSpec
from app.catalog.tables import expand, normalize, split
from app.core.safe_sql import qi

logger = logging.getLogger(__name__)

CACHE_TTL_S = 60
COUNT_TIMEOUT_MS = 5000
COVERAGE_TIMEOUT_MS = 5000

_cache: Dict[str, tuple] = {}
_lock = threading.Lock()


def _is_pg(engine: Engine) -> bool:
    return engine.dialect.name == "postgresql"


def existing_tables(engine: Engine, schemas: Iterable[str] = ("public",)) -> Set[str]:
    """Normalised names of tables and views that exist now."""
    insp = inspect(engine)
    out: Set[str] = set()
    for schema in set(schemas) | {"public"}:
        if not _is_pg(engine):
            if schema != "public":
                continue
            names = insp.get_table_names() + insp.get_view_names()
        else:
            try:
                names = insp.get_table_names(schema=schema) + insp.get_view_names(schema=schema)
            except Exception:
                names = []
        out |= {normalize(schema, n) for n in names}
    return out


def resolve_tables(spec: DatasetSpec, existing: Set[str]) -> List[str]:
    """Declared tables, then pattern-expanded ones that exist, without repeats."""
    out = list(spec.tables)
    out += [t for t in expand(spec.table_patterns, existing) if t not in out]
    return out


def _set_timeout(conn, engine: Engine, timeout_ms: int) -> None:
    if _is_pg(engine):
        conn.execute(text(f"SET LOCAL statement_timeout = {int(timeout_ms)}"))


def count_rows(engine: Engine, table: str, exists: bool, timeout_ms: int = COUNT_TIMEOUT_MS) -> Dict[str, Any]:
    stat: Dict[str, Any] = {"table": table, "exists": exists, "rows": None, "rows_exact": False}
    if not exists:
        return stat
    schema, name = split(table)
    fq = f"{qi(schema)}.{qi(name)}" if _is_pg(engine) else qi(name)
    try:
        with engine.connect() as conn:
            with conn.begin():
                _set_timeout(conn, engine, timeout_ms)
                stat["rows"] = int(conn.execute(text(f"SELECT count(*) FROM {fq}")).scalar() or 0)
                stat["rows_exact"] = True
        return stat
    except Exception as e:  # statement timeout, permissions, dropped mid-flight
        logger.info(f"[catalog] exact count of {table} failed ({type(e).__name__}); using estimate")
    if _is_pg(engine):
        try:
            with engine.connect() as conn:
                est = conn.execute(
                    text("SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass(:t)"),
                    {"t": f"{schema}.{name}"},
                ).scalar()
            # reltuples is -1 for a never-analysed table
            stat["rows"] = int(est) if est is not None and est >= 0 else None
        except Exception:
            pass
    return stat


def coverage_through(engine: Engine, spec: DatasetSpec, timeout_ms: int = COVERAGE_TIMEOUT_MS) -> Dict[str, Any]:
    if not spec.coverage_sql:
        return {"coverage_through": None, "coverage_error": None}
    try:
        with engine.connect() as conn:
            with conn.begin():
                _set_timeout(conn, engine, timeout_ms)
                value = conn.execute(text(spec.coverage_sql)).scalar()
    except Exception as e:
        # generic: the SQL error text can name internals
        logger.info(f"[catalog] coverage for {spec.key} failed: {type(e).__name__}")
        return {"coverage_through": None, "coverage_error": "unavailable"}
    if isinstance(value, (date, datetime)):
        value = value.isoformat()
    return {"coverage_through": value, "coverage_error": None}


def dataset_live(engine: Engine, spec: DatasetSpec, refresh: bool = False,
                 ttl_s: int = CACHE_TTL_S) -> Dict[str, Any]:
    now = time.monotonic()
    with _lock:
        hit = _cache.get(spec.key)
        if hit and not refresh and now - hit[0] < ttl_s:
            return hit[1]

    schemas = {split(t)[0] for t in spec.tables}
    existing = existing_tables(engine, schemas)
    tables = [count_rows(engine, t, t in existing) for t in resolve_tables(spec, existing)]
    live = {
        "tables": tables,
        "rows_total": sum(t["rows"] or 0 for t in tables),
        "rows_exact": all(t["rows_exact"] for t in tables if t["exists"]),
        **coverage_through(engine, spec),
        "measured_at": datetime.utcnow().isoformat() + "Z",
    }
    with _lock:
        _cache[spec.key] = (time.monotonic(), live)
    return live


def clear_cache() -> None:
    with _lock:
        _cache.clear()
