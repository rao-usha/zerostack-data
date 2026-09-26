"""
Live half of the column dictionary (SPEC_137): table schema and the masked sample.

Everything here reads the catalog tables under short statement timeouts and
never counts rows. PostgreSQL gives the full answer (types, comments, unique
keys, ``reltuples``, profile stats, census labels); other engines (the
SQLite unit fixtures) get column names and types only.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from app.catalog.columns import is_jsonish, mask_value
from app.catalog.dictionary import coverage, dictionary_hash, merge_columns, packages_for
from app.catalog.spec import DatasetSpec
from app.catalog.tables import pattern_matches, split
from app.core.export_policy import is_exportable, is_sensitive_column
from app.core.safe_sql import qi

logger = logging.getLogger(__name__)

SCHEMA_TIMEOUT_MS = 5000
SAMPLE_TIMEOUT_MS = 3000
SAMPLE_MAX = 20
SAMPLE_CACHE_TTL_S = 600
TABLESAMPLE_ABOVE = 1_000_000
MAX_SCHEMA_TABLES = 25
MAX_TEXT = 300
# emails whose confidence is one of these are guesses: NULL for everyone, admins included
GUESSED_EMAIL = ("inferred", "guessed")

_sample_cache: Dict[tuple, Tuple[float, Dict[str, Any]]] = {}
_sample_lock = threading.Lock()


class SampleForbidden(Exception):
    pass


class TableNotFound(Exception):
    pass


def _is_pg(engine: Engine) -> bool:
    return engine.dialect.name == "postgresql"


def _fq(table: str, engine: Engine) -> str:
    schema, name = split(table)
    return f"{qi(schema)}.{qi(name)}" if _is_pg(engine) else qi(name)


# ---------------------------------------------------------------------------
# Live facts
# ---------------------------------------------------------------------------


def _pg_facts(engine: Engine, tables: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    """table -> {oid, reltuples, columns:[{name,pg_type,nullable}], comments, unique_keys}."""
    out: Dict[str, Dict[str, Any]] = {}
    if not tables:
        return out
    regs = [f"{split(t)[0]}.{split(t)[1]}" for t in tables]
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(f"SET LOCAL statement_timeout = {int(SCHEMA_TIMEOUT_MS)}"))
            rows = conn.execute(text(
                "SELECT r.t, c.oid::bigint, c.reltuples::bigint FROM unnest(CAST(:regs AS text[])) AS r(t) "
                "JOIN pg_class c ON c.oid = to_regclass(r.t)"), {"regs": regs}).fetchall()
            by_reg = {r[0]: (r[1], r[2]) for r in rows}
            oids = [v[0] for v in by_reg.values()]
            for t, reg in zip(tables, regs):
                if reg in by_reg:
                    oid, rt = by_reg[reg]
                    out[t] = {"oid": oid, "reltuples": int(rt) if rt is not None and rt >= 0 else None,
                              "columns": [], "comments": {}, "unique_keys": []}
            if not oids:
                return out
            by_oid = {v["oid"]: k for k, v in out.items()}
            for oid, name, typ, notnull, comment in conn.execute(text(
                    "SELECT a.attrelid::bigint, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, "
                    "col_description(a.attrelid, a.attnum) FROM pg_attribute a "
                    "WHERE a.attrelid = ANY(CAST(:oids AS oid[])) AND a.attnum > 0 AND NOT a.attisdropped "
                    "ORDER BY a.attrelid, a.attnum"), {"oids": oids}):
                t = out[by_oid[oid]]
                t["columns"].append({"name": name, "pg_type": typ, "nullable": not notnull})
                if comment:
                    t["comments"][name] = comment
            for oid, is_pk, cols in conn.execute(text(
                    "SELECT i.indrelid::bigint, i.indisprimary, "
                    "array_agg(a.attname::text ORDER BY k.ord) FROM pg_index i "
                    "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) "
                    "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
                    "WHERE i.indrelid = ANY(CAST(:oids AS oid[])) AND i.indisunique "
                    "AND i.indpred IS NULL AND i.indexprs IS NULL "
                    "GROUP BY i.indexrelid, i.indrelid, i.indisprimary "
                    "ORDER BY i.indrelid, i.indisprimary DESC, i.indexrelid"), {"oids": oids}):
                out[by_oid[oid]]["unique_keys"].append({"columns": list(cols), "primary": bool(is_pk)})
    return out


def _generic_facts(engine: Engine, tables: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    insp = inspect(engine)
    names = set(insp.get_table_names())
    out = {}
    for t in tables:
        if "." in t or t not in names:
            continue
        cols = [{"name": c["name"], "pg_type": str(c["type"]).lower(), "nullable": bool(c.get("nullable", True))}
                for c in insp.get_columns(t)]
        pk = insp.get_pk_constraint(t).get("constrained_columns") or []
        out[t] = {"oid": None, "reltuples": None, "columns": cols, "comments": {},
                  "unique_keys": [{"columns": pk, "primary": True}] if pk else []}
    return out


def table_facts(engine: Engine, tables: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    try:
        return _pg_facts(engine, tables) if _is_pg(engine) else _generic_facts(engine, tables)
    except Exception as e:
        logger.info(f"[catalog] schema facts failed: {type(e).__name__}")
        return {}


def _optional_table(conn, name: str) -> bool:
    return conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": name}).scalar()


def census_labels(engine: Engine, tables: Sequence[str]) -> Dict[str, Dict[str, str]]:
    if not _is_pg(engine) or not tables:
        return {}
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"SET LOCAL statement_timeout = {int(SCHEMA_TIMEOUT_MS)}"))
                if not _optional_table(conn, "public.census_variable_metadata"):
                    return {}
                rows = conn.execute(text(
                    "SELECT dataset_id, lower(column_name), label, concept FROM census_variable_metadata "
                    "WHERE dataset_id = ANY(CAST(:t AS text[]))"), {"t": list(tables)}).fetchall()
    except Exception as e:
        logger.info(f"[catalog] census labels failed: {type(e).__name__}")
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for ds, col, label, concept in rows:
        lab = " - ".join(p for p in (label or "").replace(":", "").split("!!") if p)
        out.setdefault(ds, {})[col] = f"{concept.strip().capitalize()}: {lab}" if concept else lab
    return out


def profile_stats(engine: Engine, tables: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    """table -> {profiled_at, columns: {name: {null_pct, distinct_count, top_values}}} (latest snapshot)."""
    if not _is_pg(engine) or not tables:
        return {}
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"SET LOCAL statement_timeout = {int(SCHEMA_TIMEOUT_MS)}"))
                if not (_optional_table(conn, "public.data_profile_snapshots")
                        and _optional_table(conn, "public.data_profile_columns")):
                    return {}
                rows = conn.execute(text(
                    "WITH latest AS (SELECT DISTINCT ON (table_name) id, table_name, profiled_at "
                    "FROM data_profile_snapshots WHERE table_name = ANY(CAST(:t AS text[])) "
                    "ORDER BY table_name, profiled_at DESC) "
                    "SELECT l.table_name, l.profiled_at, c.column_name, c.null_pct, c.distinct_count, c.stats "
                    "FROM latest l JOIN data_profile_columns c ON c.snapshot_id = l.id"),
                    {"t": list(tables)}).fetchall()
    except Exception as e:
        logger.info(f"[catalog] profile stats failed: {type(e).__name__}")
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for table, at, col, null_pct, distinct, stats in rows:
        t = out.setdefault(table, {"profiled_at": at.isoformat() if at else None, "columns": {}})
        top = (stats or {}).get("top_values") if isinstance(stats, dict) else None
        t["columns"][col] = {"null_pct": null_pct, "distinct_count": distinct, "top_values": top}
    return out


# ---------------------------------------------------------------------------
# Writers / rights of a table
# ---------------------------------------------------------------------------


def table_writers(table: str, spec: DatasetSpec) -> List[DatasetSpec]:
    """Every catalog spec that writes ``table`` (shared tables: the strictest rights win)."""
    from app.catalog.registry import get_catalog

    out = [s for s in get_catalog() if table in s.tables
           or any(pattern_matches(p, table) for p in s.table_patterns)]
    return out if spec in out else [spec] + out


def effective_pii(col: Dict[str, Any], writers: Iterable[DatasetSpec]) -> str:
    """A PII column of a table any writer classes as 'personal' is personal."""
    pii = col.get("pii") or "none"
    if pii != "none" and any(w.pii_class == "personal" for w in writers):
        return "personal"
    return pii


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def dataset_schema(engine: Engine, spec: DatasetSpec, tables: Sequence[str], existing: set,
                   tables_total: int) -> Dict[str, Any]:
    facts = table_facts(engine, [t for t in tables if t in existing])
    census = census_labels(engine, [t for t in tables if t in facts])
    profiles = profile_stats(engine, [t for t in tables if t in facts])
    pkgs = packages_for(spec)
    out_tables = []
    all_cols: List[Dict[str, Any]] = []
    for i, t in enumerate(tables):
        f = facts.get(t)
        cols = merge_columns(t, f["columns"] if f else None, pkgs, census.get(t),
                             f["comments"] if f else None)
        prof = profiles.get(t, {})
        writers = table_writers(t, spec)
        for c in cols:
            p = prof.get("columns", {}).get(c["name"], {})
            c["null_pct"] = p.get("null_pct")
            c["distinct_count"] = p.get("distinct_count")
            c["pii"] = effective_pii(c, writers)
            if not c.get("example") and c["pii"] == "none" and p.get("top_values"):
                top = p["top_values"][0]
                val = top.get("value") if isinstance(top, dict) else top
                c["example"] = None if val is None else str(val)[:100]
            elif c["pii"] != "none":
                c["example"] = None
        declared_pk = list(spec.primary_key) if (spec.tables and t == spec.tables[0]) else []
        out_tables.append({
            "table": t,
            "exists": t in existing and f is not None,
            "row_estimate": f["reltuples"] if f else None,
            "primary_key": declared_pk,
            "unique_keys": f["unique_keys"] if f else [],
            "profiled_at": prof.get("profiled_at"),
            "coverage": {**coverage(cols), "without_glossary": coverage(cols, False)["pct"]},
            "columns": cols,
        })
        all_cols += cols
    return {
        "dataset": spec.key,
        "dictionary_hash": dictionary_hash(),
        "pii_class": spec.pii_class,
        "tables_total": tables_total,
        "tables_truncated": tables_total > len(tables),
        "coverage": {**coverage(all_cols), "without_glossary": coverage(all_cols, False)["pct"]},
        "tables": out_tables,
    }


# ---------------------------------------------------------------------------
# Sample
# ---------------------------------------------------------------------------


def _natural_key(spec: DatasetSpec, table: str, facts: Dict[str, Any]) -> List[str]:
    names = {c["name"] for c in facts["columns"]}
    if spec.tables and table == spec.tables[0] and spec.primary_key and set(spec.primary_key) <= names:
        return list(spec.primary_key)
    for uk in facts.get("unique_keys", []):
        if uk["columns"] and set(uk["columns"]) <= names:
            return list(uk["columns"])
    return []


def _jsonable(value: Any) -> Any:
    from fastapi.encoders import jsonable_encoder

    if isinstance(value, (bytes, bytearray, memoryview)):
        return None
    v = jsonable_encoder(value)
    if isinstance(v, str) and len(v) > MAX_TEXT:
        return v[:MAX_TEXT] + "..."
    return v


def attribution_of(spec: DatasetSpec) -> str:
    raw = spec.attribution or f"License: {spec.license}"
    return " ".join(raw.split()).encode("latin-1", "replace").decode("latin-1")[:500]


def build_sample(engine: Engine, spec: DatasetSpec, table: str, limit: int, admin: bool,
                 use_cache: bool = True) -> Dict[str, Any]:
    limit = max(1, min(int(limit), SAMPLE_MAX))
    key = (spec.key, table, limit, admin)
    if use_cache:
        with _sample_lock:
            hit = _sample_cache.get(key)
        if hit and time.monotonic() - hit[0] < SAMPLE_CACHE_TTL_S:
            return hit[1]

    facts = table_facts(engine, [table]).get(table)
    if facts is None:
        raise TableNotFound(table)
    live_names = [c["name"] for c in facts["columns"]]
    if not is_exportable(split(table)[1], live_names):
        raise SampleForbidden("this table is excluded by the export policy")
    writers = table_writers(table, spec)
    if not admin and any(w.redistribution == "restricted" or getattr(w, "storage", None) == "forbidden"
                         for w in writers):
        raise SampleForbidden("restricted dataset: samples are admin-only")

    cols = merge_columns(table, facts["columns"], packages_for(spec), None, facts["comments"])
    shown = [c for c in cols if c.get("description") and not is_sensitive_column(c["name"])]
    hidden = len(cols) - len(shown)
    if not shown:
        body = {"dataset": spec.key, "table": table, "limit": limit, "masked": not admin, "sampled": False,
                "order_by": [], "columns": [], "hidden_columns": hidden, "rows": [],
                "attribution": attribution_of(spec)}
        return body

    guessed_email = "email_confidence" in live_names
    select = []
    for c in shown:
        q = qi(c["name"])
        if guessed_email and c["name"] in ("email", "work_email"):
            select.append(f"CASE WHEN {qi('email_confidence')} IN ({', '.join(repr(v) for v in GUESSED_EMAIL)}) "
                          f"THEN NULL ELSE {q} END AS {q}")
        else:
            select.append(q)
    order = _natural_key(spec, table, facts)
    order_sql = f" ORDER BY {', '.join(qi(c) for c in order)}" if order else ""
    reltuples = facts.get("reltuples") or 0
    sampled = _is_pg(engine) and reltuples > TABLESAMPLE_ABOVE
    base = f"SELECT {', '.join(select)} FROM {_fq(table, engine)}"

    def _run(tablesample: bool) -> List[Dict[str, Any]]:
        sql = base
        if tablesample:
            pct = min(100.0, max(0.001, 100.0 * limit * 50 / reltuples))
            sql += f" TABLESAMPLE SYSTEM ({pct:.4f})"
        sql += order_sql + " LIMIT :n"
        with engine.connect() as conn:
            with conn.begin():
                if _is_pg(engine):
                    conn.execute(text("SET TRANSACTION READ ONLY"))
                    conn.execute(text(f"SET LOCAL statement_timeout = {int(SAMPLE_TIMEOUT_MS)}"))
                return [dict(r) for r in conn.execute(text(sql), {"n": limit}).mappings()]

    rows = _run(sampled)
    if sampled and len(rows) < limit:
        rows, sampled = _run(False), False

    col_meta = []
    for c in shown:
        pii = effective_pii(c, writers)
        opaque = is_jsonish(c.get("pg_type")) and c.get("source") not in ("curated", "upstream")
        masked = not admin and (pii != "none" or opaque)
        col_meta.append({"name": c["name"], "pg_type": c.get("pg_type"), "pii": pii, "masked": masked,
                         "_opaque": opaque})
    out_rows = []
    for r in rows:
        row = {}
        for m in col_meta:
            v = r.get(m["name"])
            if not admin:
                v = None if (m["_opaque"] and v is not None) else mask_value(v, m["name"], m["pii"])
            row[m["name"]] = _jsonable(v)
        out_rows.append(row)
    for m in col_meta:
        m.pop("_opaque")
    body = {
        "dataset": spec.key, "table": table, "limit": limit, "masked": not admin, "sampled": sampled,
        "order_by": order, "columns": col_meta, "hidden_columns": hidden, "rows": out_rows,
        "attribution": attribution_of(spec),
    }
    with _sample_lock:
        _sample_cache[key] = (time.monotonic(), body)
    return body


def clear_sample_cache() -> None:
    with _sample_lock:
        _sample_cache.clear()


__all__ = [
    "SampleForbidden", "TableNotFound", "build_sample", "census_labels", "clear_sample_cache",
    "dataset_schema", "effective_pii", "profile_stats", "table_facts",
]
