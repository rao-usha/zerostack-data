"""
Live facts for a catalog dataset (SPEC_123): row counts and coverage.

Row counts are ``count(*)`` on the declared tables — never ``rows_inserted``
from job history. Each count runs in its own short transaction under
``SET LOCAL statement_timeout``; a count that times out falls back to the
planner estimate (``pg_class.reltuples``) and says so with
``rows_exact: false``. Results are cached in-process for ``CACHE_TTL_S``.

One request is bounded: at most ``MAX_EXACT_COUNTS`` exact counts (declared
tables first, then pattern-expanded ones) and an overall ``LIVE_DEADLINE_S``;
every table past either limit gets the planner estimate. Only one
computation per dataset runs at a time — concurrent requests wait for it and
share the result.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Set

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from app.catalog.spec import DatasetSpec
from app.catalog.tables import expand, normalize, split
from app.core.safe_sql import qi

logger = logging.getLogger(__name__)

CACHE_TTL_S = 60
COUNT_TIMEOUT_MS = 5000
COVERAGE_TIMEOUT_MS = 5000
MAX_EXACT_COUNTS = 12
LIVE_DEADLINE_S = 20.0

_cache: Dict[str, tuple] = {}
_lock = threading.Lock()
_key_locks: Dict[str, threading.Lock] = {}
_generation: Dict[str, int] = {}  # completed computations per dataset


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


def claimed_tables(specs: Iterable[DatasetSpec]) -> Dict[str, Set[str]]:
    """table -> keys of the specs that declare it concretely."""
    out: Dict[str, Set[str]] = {}
    for s in specs:
        for t in s.tables:
            out.setdefault(t, set()).add(s.key)
    return out


def resolve_tables(spec: DatasetSpec, existing: Set[str],
                   claimed: Optional[Dict[str, Set[str]]] = None) -> List[str]:
    """Declared tables, then pattern-expanded ones that exist, without repeats.

    A pattern never pulls in a table another dataset declares concretely
    (``sec_8k*`` must not count the bulk ``sec_8k_index``). ``claimed``
    defaults to the whole catalog.
    """
    if claimed is None:
        from app.catalog.registry import get_catalog

        claimed = claimed_tables(get_catalog())
    out = list(spec.tables)
    for t in expand(spec.table_patterns, existing):
        if t in out or (claimed.get(t, set()) - {spec.key}):
            continue
        out.append(t)
    return out


def _set_timeout(conn, engine: Engine, timeout_ms: int) -> None:
    if _is_pg(engine):
        conn.execute(text(f"SET LOCAL statement_timeout = {int(timeout_ms)}"))


def estimate_rows(engine: Engine, table: str) -> Optional[int]:
    """Planner estimate (PG only); None when unknown or never analysed."""
    if not _is_pg(engine):
        return None
    schema, name = split(table)
    try:
        with engine.connect() as conn:
            est = conn.execute(
                text("SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass(:t)"),
                {"t": f"{schema}.{name}"},
            ).scalar()
    except Exception:
        return None
    # reltuples is -1 for a never-analysed table
    return int(est) if est is not None and est >= 0 else None


def count_rows(engine: Engine, table: str, exists: bool, timeout_ms: int = COUNT_TIMEOUT_MS,
               exact: bool = True) -> Dict[str, Any]:
    stat: Dict[str, Any] = {"table": table, "exists": exists, "rows": None, "rows_exact": False}
    if not exists:
        return stat
    if exact:
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
    stat["rows"] = estimate_rows(engine, table)
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


def _cached(key: str, ttl_s: int) -> Optional[Dict[str, Any]]:
    with _lock:
        hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < ttl_s:
        return hit[1]
    return None


def dataset_live(engine: Engine, spec: DatasetSpec, refresh: bool = False,
                 ttl_s: int = CACHE_TTL_S, max_exact: Optional[int] = None,
                 deadline_s: Optional[float] = None) -> Dict[str, Any]:
    max_exact = MAX_EXACT_COUNTS if max_exact is None else max_exact
    deadline_s = LIVE_DEADLINE_S if deadline_s is None else deadline_s
    if not refresh:
        hit = _cached(spec.key, ttl_s)
        if hit is not None:
            return hit

    with _lock:
        key_lock = _key_locks.setdefault(spec.key, threading.Lock())
        gen_before = _generation.get(spec.key, 0)
    started = time.monotonic()
    with key_lock:
        # a concurrent request finished a computation while we waited: share it
        with _lock:
            hit = _cache.get(spec.key)
            fresh = _generation.get(spec.key, 0) != gen_before
        if hit and fresh:
            return hit[1]

        schemas = {split(t)[0] for t in spec.tables}
        existing = existing_tables(engine, schemas)
        tables: List[Dict[str, Any]] = []
        exact_done = 0
        for t in resolve_tables(spec, existing):
            exact = exact_done < max_exact and time.monotonic() - started < deadline_s
            stat = count_rows(engine, t, t in existing, exact=exact)
            if exact and stat["exists"]:
                exact_done += 1
            tables.append(stat)
        live = {
            "tables": tables,
            "rows_total": sum(t["rows"] or 0 for t in tables),
            "rows_exact": all(t["rows_exact"] for t in tables if t["exists"]),
            **coverage_through(engine, spec),
            "measured_at": datetime.utcnow().isoformat() + "Z",
        }
        with _lock:
            _cache[spec.key] = (time.monotonic(), live)
            _generation[spec.key] = _generation.get(spec.key, 0) + 1
        return live


def clear_cache() -> None:
    with _lock:
        _cache.clear()
