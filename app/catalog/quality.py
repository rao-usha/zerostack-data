"""
Per-dataset quality signals (SPEC_144).

The DQ framework used to pick its tables from ``DatasetRegistry.ingested()``,
which leaves out every table no per-source ingestor registered: all SEC bulk
tables, the PE marts and ``core.*``. Here the catalog decides:

- ``dq_targets``   the base tables catalog specs resolve to (never views), plus
                   the ingested registry rows the catalog does not cover.
- ``quality_block`` the quality section of ``GET /catalog/{key}``: data state,
                   latest DQ score, rule results, profile age, key-column nulls,
                   row trend and a metadata completeness score.
- ``row_trends``   daily row counts per dataset from ``dq_quality_snapshots``.
- ``post_load``    the advisory hook the bulk runner and the mart builds call.

Everything that reads user tables runs under a short ``statement_timeout`` and
uses the planner estimate above a size limit. Nothing here deletes or moves
data: seeded or fabricated rows are flagged (``seed_contaminated``), never
touched.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from sqlalchemy import bindparam, inspect, text
from sqlalchemy.engine import Engine

from app.catalog.spec import DatasetSpec
from app.catalog.tables import normalize, split
from app.core.safe_sql import qi

logger = logging.getLogger(__name__)

DATA_STATES = ("phantom", "empty", "populated", "defective", "seed_contaminated")

CACHE_TTL_S = 60
QUERY_TIMEOUT_MS = 3000
EXACT_COUNT_MAX_ROWS = 1_000_000   # at or below this estimate, count(*) decides "empty"
SEED_SCAN_MAX_ROWS = 5_000_000     # the seed-row scan is a full scan: skipped above this
POST_LOAD_MAX_ROWS = 2_000_000     # the post-load hook leaves larger tables to the scheduler
TREND_DAYS = 30
RULE_WINDOW = timedelta(days=7)
STALE_PROFILE_FACTOR = 2           # a profile older than 2x the cadence is stale
DEFECTIVE_ALL_NULL_SHARE = 0.9     # >= 90% of the data columns entirely NULL

# Hours per cadence; the same periods as app/services/dataset_status.CADENCE_HOURS,
# plus a month for ad-hoc datasets so they are still re-profiled now and then.
CADENCE_HOURS = {
    "daily": 24,
    "weekly": 7 * 24,
    "monthly": 31 * 24,
    "quarterly": 92 * 24,
    "annual": 366 * 24,
}
AD_HOC_HOURS = 30 * 24

# SPEC_124 status -> freshness component (0-100). The verdict already folds in
# the cadence and slo_lag_hours; ``unknown`` has no expectation, so no score.
FRESHNESS_BY_STATUS = {
    "current": 100.0,
    "awaiting_upstream": 100.0,
    "partial": 60.0,
    "behind": 40.0,
    "stalled": 20.0,
    "failing": 20.0,
    "blocked": 0.0,
    "dormant": 0.0,
    "never_run": 0.0,
    "unknown": None,
}

# Columns a dataset is useless without, beyond its primary key (PLAN_088 §1.3:
# populated, but these are NULL on every row).
KEY_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "usaspending_awards": ("naics_code", "award_type", "period_of_performance_start"),
    "treasury_debt_outstanding": ("debt_held_public_amt", "intragov_hold_amt", "tot_pub_debt_out_amt"),
    "fema_pa_projects": ("state", "damage_category", "project_title", "obligation_date"),
    "cms_hospital_cost_reports": ("prvdr_num", "tot_charges", "tot_costs"),
}

# Bookkeeping columns, left out of the "all data columns NULL" share.
BOOKKEEPING_COLUMNS = frozenset({
    "id", "created_at", "updated_at", "ingested_at", "ingestion_timestamp", "ingestion_job_id",
    "job_id", "source", "collected_at", "last_updated", "last_updated_at", "loaded_at",
})

# A `source` value that marks a seeded, sample or reference row (PLAN_088 §1.4).
SEED_SOURCE_PATTERNS = ("%sample%", "%\\_seed", "nrel_reference")

_cache: Dict[str, Tuple[float, Any]] = {}
_lock = threading.Lock()


def clear_cache() -> None:
    with _lock:
        _cache.clear()


def _cached(key: str, ttl_s: float, compute):
    with _lock:
        hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < ttl_s:
        return hit[1]
    value = compute()
    with _lock:
        _cache[key] = (time.monotonic(), value)
    return value


def _is_pg(engine) -> bool:
    return getattr(getattr(engine, "dialect", None), "name", "") == "postgresql"


def engine_of(bind) -> Engine:
    """A Session's bind may be an Engine or a Connection."""
    return getattr(bind, "engine", bind)


def cadence_hours(cadence: Optional[str]) -> int:
    return CADENCE_HOURS.get(cadence or "", AD_HOC_HOURS)


# =============================================================================
# Relations
# =============================================================================


_RELATIONS_SQL = """
    SELECT n.nspname, c.relname, c.relkind, c.reltuples::bigint, s.n_live_tup
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
    WHERE c.relkind IN ('r', 'p', 'v', 'm')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname NOT LIKE 'pg\\_toast%'
      AND n.nspname NOT LIKE 'pg\\_temp%'
"""


def relations(engine: Engine, cached: bool = False) -> Dict[str, Dict[str, Any]]:
    """Normalised name -> {kind: table|view, reltuples, n_live_tup} for every
    user relation. One query on PostgreSQL; the inspector elsewhere."""
    engine = engine_of(engine)

    def compute() -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not _is_pg(engine):
            insp = inspect(engine)
            for n in insp.get_table_names():
                out[normalize(None, n)] = {"kind": "table", "reltuples": None, "n_live_tup": None}
            for n in insp.get_view_names():
                out[normalize(None, n)] = {"kind": "view", "reltuples": None, "n_live_tup": None}
            return out
        with engine.connect() as conn:
            for schema, name, relkind, reltuples, live in conn.execute(text(_RELATIONS_SQL)):
                out[normalize(schema, name)] = {
                    "kind": "table" if relkind in ("r", "p") else "view",
                    # reltuples is -1 for a never-analysed table (0 for a partitioned parent)
                    "reltuples": int(reltuples) if reltuples is not None and reltuples >= 0 else None,
                    "n_live_tup": int(live) if live is not None else None,
                }
        return out

    if not cached:
        return compute()
    return _cached(f"relations:{id(engine)}", CACHE_TTL_S, compute)


def base_tables(rels: Mapping[str, Mapping[str, Any]]) -> Set[str]:
    return {n for n, r in rels.items() if r["kind"] == "table"}


def row_estimate(rel: Optional[Mapping[str, Any]]) -> Optional[int]:
    """Live-tuple count from the stats collector, else the planner estimate."""
    if not rel:
        return None
    if rel.get("n_live_tup") is not None and (rel["n_live_tup"] > 0 or not rel.get("reltuples")):
        return rel["n_live_tup"]
    return rel.get("reltuples")


def spec_tables(spec: DatasetSpec, base: Set[str],
                claimed: Optional[Dict[str, Set[str]]] = None) -> List[str]:
    """Declared tables then pattern matches, expanded over base tables only
    (a pattern never pulls in a view such as ``fred_observations``)."""
    from app.catalog.live import resolve_tables

    return resolve_tables(spec, base, claimed)


# =============================================================================
# DQ selector
# =============================================================================


def dq_targets(db, specs: Optional[Iterable[DatasetSpec]] = None,
               include_registry: bool = True) -> List[Dict[str, Any]]:
    """Tables the DQ framework covers, in catalog order.

    Each is ``{table, dataset_key, source, domain, rows_estimate}``. Catalog
    tables are included when they exist as base tables; ingested registry rows
    are added for tables no spec covers, and give a covered table its
    registry ``source`` (so snapshot keys stay continuous with the old rows).
    """
    from app.catalog.live import claimed_tables
    from app.catalog.registry import get_catalog

    specs = tuple(specs) if specs is not None else get_catalog()
    engine = engine_of(db.get_bind())
    rels = relations(engine)
    base = base_tables(rels)
    claimed = claimed_tables(get_catalog())
    out: Dict[str, Dict[str, Any]] = {}
    for spec in specs:
        for t in spec_tables(spec, base, claimed):
            if t in base and t not in out:
                out[t] = {"table": t, "dataset_key": spec.key, "source": spec.source,
                          "domain": None, "rows_estimate": row_estimate(rels.get(t))}
    if include_registry:
        from app.core.models import DatasetRegistry

        try:
            registry = db.query(DatasetRegistry).filter(DatasetRegistry.ingested()).all()
        except Exception as e:
            logger.warning(f"[catalog.quality] registry read failed: {type(e).__name__}")
            db.rollback()
            registry = []
        for reg in registry:
            t = reg.table_name
            if not t:
                continue
            if t in out:
                out[t]["source"] = reg.source or out[t]["source"]
                continue
            out[t] = {"table": t, "dataset_key": None, "source": reg.source,
                      "domain": getattr(reg, "domain", None),
                      "rows_estimate": row_estimate(rels.get(t))}
    return list(out.values())


def dataset_tables_for_job(db, job) -> List[str]:
    """Existing base tables of the dataset an ingestion job produced."""
    from app.catalog.job_keys import dataset_key_for_job
    from app.catalog.registry import get_spec

    key = getattr(job, "dataset_key", None)
    if not key:
        key = dataset_key_for_job(getattr(job, "source", None), getattr(job, "config", None))
    spec = get_spec(key) if key else None
    if spec is None:
        return []
    base = base_tables(relations(engine_of(db.get_bind())))
    return [t for t in spec_tables(spec, base) if t in base]


# =============================================================================
# Data state
# =============================================================================


def classify_data_state(tables: Sequence[Mapping[str, Any]]) -> Tuple[str, List[str]]:
    """``(state, reasons)`` from per-table facts.

    Each table: ``{table, exists, rows, key_null_columns, all_null_share,
    seed_rows}``. First match wins: phantom (no declared table exists) >
    empty (none has rows) > defective > seed_contaminated > populated.
    """
    existing = [t for t in tables if t.get("exists")]
    missing = [t["table"] for t in tables if not t.get("exists")]
    if not existing:
        return "phantom", [f"no table exists ({', '.join(missing) or 'none declared'})"]
    with_rows = [t for t in existing if (t.get("rows") or 0) > 0]
    if not with_rows:
        return "empty", [f"{len(existing)} table(s) exist with 0 rows"]
    reasons: List[str] = []
    for t in with_rows:
        cols = t.get("key_null_columns") or []
        if cols:
            reasons.append(f"{t['table']}: key column(s) all NULL: {', '.join(cols)}")
        share = t.get("all_null_share")
        if share is not None and share >= DEFECTIVE_ALL_NULL_SHARE:
            reasons.append(f"{t['table']}: {round(share * 100)}% of data columns all NULL")
    if reasons:
        return "defective", reasons
    seeded = [t for t in with_rows if (t.get("seed_rows") or 0) > 0]
    if seeded:
        return "seed_contaminated", [f"{t['table']}: {t['seed_rows']} seed/sample row(s)" for t in seeded]
    extra = [f"missing table(s): {', '.join(missing)}"] if missing else []
    return "populated", extra


def key_columns(spec: DatasetSpec) -> Tuple[str, ...]:
    return tuple(dict.fromkeys(tuple(spec.primary_key) + KEY_COLUMNS.get(spec.key, ())))


def profile_null_facts(columns: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> Dict[str, Any]:
    """From one profile's columns: key-column null %, all-NULL key columns,
    all-NULL data columns and their share."""
    by_name = {c["column_name"]: c.get("null_pct") for c in columns}
    key_null = {k: by_name[k] for k in keys if k in by_name and by_name[k] is not None}
    data_cols = [c for c in by_name if c not in BOOKKEEPING_COLUMNS]
    all_null = sorted(c for c in data_cols if (by_name[c] or 0) >= 100)
    return {
        "key_null_pct": key_null,
        "key_null_columns": sorted(k for k, v in key_null.items() if v >= 100),
        "all_null_columns": all_null,
        "all_null_share": (len(all_null) / len(data_cols)) if data_cols else None,
    }


# =============================================================================
# Metadata completeness
# =============================================================================


COMPLETENESS_CHECKS = (
    "description", "subtitle", "keywords", "coverage_sql", "coverage_from", "primary_key",
    "upstream_url", "slo", "attribution", "license_url", "column_docs", "named_owner",
)


def metadata_completeness(spec: DatasetSpec, column_doc_pct: Optional[float] = None) -> Dict[str, Any]:
    """Share of the PLAN_088 §2 metadata a spec carries. Deterministic: fields
    later specs add (subtitle, keywords, license_url) count once they exist;
    ``column_doc_pct`` (0-1) comes from the column dictionary when it has one."""
    checks = {
        "description": len((spec.description or "").strip()) >= 50,
        "subtitle": bool(getattr(spec, "subtitle", None)),
        "keywords": bool(getattr(spec, "keywords", None)),
        "coverage_sql": bool(spec.coverage_sql),
        "coverage_from": bool(spec.coverage_from),
        "primary_key": bool(spec.primary_key),
        "upstream_url": bool(spec.upstream_url),
        "slo": spec.slo_lag_hours is not None,
        "attribution": bool(spec.attribution),
        "license_url": bool(getattr(spec, "license_url", None)),
        "column_docs": column_doc_pct is not None and column_doc_pct >= 0.8,
        "named_owner": bool(spec.owner) and spec.owner != "data-platform",
    }
    return {"score": round(sum(checks.values()) / len(checks), 2), "checks": checks}


# =============================================================================
# Quality block
# =============================================================================


def _run(engine, sql: str, params: Optional[Mapping[str, Any]] = None,
         timeout_ms: int = QUERY_TIMEOUT_MS, expanding: Sequence[str] = ()) -> List[Any]:
    """One statement in its own short transaction under a timeout."""
    stmt = text(sql)
    if expanding:
        stmt = stmt.bindparams(*(bindparam(n, expanding=True) for n in expanding))
    with engine.connect() as conn:
        with conn.begin():
            if _is_pg(engine):
                conn.execute(text(f"SET LOCAL statement_timeout = {int(timeout_ms)}"))
            return list(conn.execute(stmt, dict(params or {})))


def _safe(label: str, fn, default):
    try:
        return fn()
    except Exception as e:  # a missing DQ table, a timeout: the block degrades, never 500s
        logger.info(f"[catalog.quality] {label} unavailable: {type(e).__name__}")
        return default


def _fq(table: str, engine) -> str:
    schema, name = split(table)
    return f"{qi(schema)}.{qi(name)}" if _is_pg(engine) else qi(name)


def _count(engine, table: str, estimate: Optional[int]) -> Tuple[Optional[int], bool]:
    if estimate is None or estimate <= EXACT_COUNT_MAX_ROWS:
        try:
            return int(_run(engine, f"SELECT count(*) FROM {_fq(table, engine)}")[0][0]), True
        except Exception:
            pass
    return estimate, False


def _has_column(engine, table: str, column: str) -> bool:
    schema, name = split(table)
    if not _is_pg(engine):
        return any(c["name"] == column for c in inspect(engine).get_columns(name))
    rows = _run(engine, "SELECT 1 FROM information_schema.columns WHERE table_schema = :s "
                        "AND table_name = :t AND column_name = :c", {"s": schema, "t": name, "c": column})
    return bool(rows)


def seed_rows(engine, table: str, estimate: Optional[int]) -> Optional[int]:
    """Rows whose ``source`` marks them as seeded/sample; None when unknown."""
    if estimate is not None and estimate > SEED_SCAN_MAX_ROWS:
        return None
    if not _has_column(engine, table, "source"):
        return 0
    p = SEED_SOURCE_PATTERNS
    rows = _run(engine, f"SELECT count(*) FROM {_fq(table, engine)} "
                        f"WHERE source::text ILIKE :p0 OR source::text LIKE :p1 OR source::text = :p2",
                {"p0": p[0], "p1": p[1], "p2": p[2]})
    return int(rows[0][0])


def _latest_profiles(engine, tables: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    rows = _run(engine, """
        SELECT id, table_name, row_count, profiled_at, overall_completeness_pct
        FROM data_profile_snapshots s
        WHERE table_name IN :ts
          AND profiled_at = (SELECT max(profiled_at) FROM data_profile_snapshots x
                             WHERE x.table_name = s.table_name)
    """, {"ts": list(tables)}, expanding=("ts",))
    out = {r[1]: {"id": r[0], "row_count": r[2], "profiled_at": r[3], "completeness": r[4]} for r in rows}
    if out:
        cols = _run(engine, "SELECT snapshot_id, column_name, null_pct FROM data_profile_columns "
                            "WHERE snapshot_id IN :ids",
                    {"ids": [p["id"] for p in out.values()]}, expanding=("ids",))
        by_id: Dict[int, List[Dict[str, Any]]] = {}
        for sid, name, pct in cols:
            by_id.setdefault(sid, []).append({"column_name": name, "null_pct": pct})
        for p in out.values():
            p["columns"] = by_id.get(p["id"], [])
    return out


def _snapshots(engine, tables: Sequence[str], since: date) -> List[Any]:
    return _run(engine, """
        SELECT snapshot_date, table_name, quality_score, completeness_score, freshness_score,
               validity_score, consistency_score, row_count
        FROM dq_quality_snapshots
        WHERE table_name IN :ts AND snapshot_date >= :since
        ORDER BY snapshot_date
    """, {"ts": list(tables), "since": since}, expanding=("ts",))


def _rule_results(engine, tables: Sequence[str], since: datetime) -> List[Any]:
    """Latest result per (rule, table) in the window."""
    return _run(engine, """
        SELECT x.dataset_name, x.passed, r.name
        FROM data_quality_results x
        LEFT JOIN data_quality_rules r ON r.id = x.rule_id
        WHERE x.dataset_name IN :ts AND x.evaluated_at >= :since
          AND x.evaluated_at = (SELECT max(y.evaluated_at) FROM data_quality_results y
                                WHERE y.rule_id = x.rule_id AND y.dataset_name = x.dataset_name)
    """, {"ts": list(tables), "since": since}, expanding=("ts",))


def _open_anomalies(engine, tables: Sequence[str]) -> Dict[str, int]:
    rows = _run(engine, "SELECT table_name, count(*) FROM dq_anomaly_alerts "
                        "WHERE table_name IN :ts AND status = 'open' GROUP BY table_name",
                {"ts": list(tables)}, expanding=("ts",))
    return {r[0]: int(r[1]) for r in rows}


def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    return round(sum(vals) / len(vals), 1) if vals else None


def _trend(points: Iterable[Tuple[Any, str, Optional[int]]]) -> List[Dict[str, Any]]:
    """(date, table, rows) -> [{date, rows}] summed over tables per date
    (one value per table and date: the largest, if a table has several rows)."""
    per: Dict[Tuple[date, str], int] = {}
    for d, t, rows in points:
        if rows is None:
            continue
        d = d.date() if isinstance(d, datetime) else d
        per[(d, t)] = max(per.get((d, t), 0), int(rows))
    by_date: Dict[date, int] = {}
    for (d, _t), rows in per.items():
        by_date[d] = by_date.get(d, 0) + rows
    return [{"date": d.isoformat(), "rows": n} for d, n in sorted(by_date.items())]


def quality_block(engine, spec: DatasetSpec, now: Optional[datetime] = None,
                  refresh: bool = False, column_doc_pct: Optional[float] = None) -> Dict[str, Any]:
    """The quality section of a catalog entry (cached ``CACHE_TTL_S``)."""
    engine = engine_of(engine)
    if not refresh:
        with _lock:
            hit = _cache.get(f"block:{spec.key}")
        if hit and time.monotonic() - hit[0] < CACHE_TTL_S:
            return hit[1]
    block = _compute_block(engine, spec, now or datetime.utcnow(), column_doc_pct)
    with _lock:
        _cache[f"block:{spec.key}"] = (time.monotonic(), block)
    return block


def _compute_block(engine, spec: DatasetSpec, now: datetime,
                   column_doc_pct: Optional[float]) -> Dict[str, Any]:
    rels = relations(engine, cached=True)
    base = base_tables(rels)
    tables = spec_tables(spec, base)
    existing = [t for t in tables if t in base]
    profiles = _safe("profiles", lambda: _latest_profiles(engine, existing), {}) if existing else {}
    since = (now - timedelta(days=TREND_DAYS)).date()
    snaps = _safe("snapshots", lambda: _snapshots(engine, existing, since), []) if existing else []
    rules = _safe("rules", lambda: _rule_results(engine, existing, now - RULE_WINDOW), []) \
        if existing else []
    anomalies = _safe("anomalies", lambda: _open_anomalies(engine, existing), {}) if existing else {}

    keys = key_columns(spec)
    stale_after = cadence_hours(spec.cadence) * STALE_PROFILE_FACTOR
    per_table: List[Dict[str, Any]] = []
    for t in tables:
        entry: Dict[str, Any] = {"table": t, "exists": t in base}
        if not entry["exists"]:
            per_table.append(entry)
            continue
        est = row_estimate(rels.get(t))
        rows, exact = _safe("count", lambda: _count(engine, t, est), (est, False))
        entry.update(rows=rows, rows_exact=exact)
        prof = profiles.get(t)
        if prof:
            age_h = (now - prof["profiled_at"]).total_seconds() / 3600 if prof["profiled_at"] else None
            entry.update(
                profiled_at=prof["profiled_at"].isoformat() if prof["profiled_at"] else None,
                profile_age_hours=round(age_h, 1) if age_h is not None else None,
                profile_stale=age_h is None or age_h > stale_after,
                profile_rows=prof["row_count"],
                **profile_null_facts(prof.get("columns") or [], keys),
            )
        else:
            entry.update(profiled_at=None, profile_stale=True)
        if rows:
            entry["seed_rows"] = _safe("seed", lambda: seed_rows(engine, t, est), None)
        latest = [s for s in snaps if s[1] == t]
        entry["quality_score"] = latest[-1][2] if latest else None
        entry["open_anomalies"] = anomalies.get(t, 0)
        mine = [r for r in rules if r[0] == t]
        entry["rules"] = {"passed": sum(1 for r in mine if r[1]), "failed": sum(1 for r in mine if not r[1])}
        per_table.append(entry)

    state, reasons = classify_data_state(per_table)
    latest_date = max((s[0] for s in snaps), default=None)
    latest_snaps = [s for s in snaps if s[0] == latest_date]
    failed_rules = sorted({r[2] or "unnamed rule" for r in rules if not r[1]})
    profiled = [p["profiled_at"] for p in profiles.values() if p.get("profiled_at")]
    oldest = min(profiled) if profiled else None
    trend = _trend((s[0], s[1], s[7]) for s in snaps)
    return {
        "data_state": state,
        "data_state_reasons": reasons,
        "score": _mean(s[2] for s in latest_snaps),
        "components": {
            "completeness": _mean(s[3] for s in latest_snaps),
            "freshness": _mean(s[4] for s in latest_snaps),
            "validity": _mean(s[5] for s in latest_snaps),
            "consistency": _mean(s[6] for s in latest_snaps),
        },
        "scored_on": latest_date.isoformat() if latest_date else None,
        "rules": {
            "passed": sum(1 for r in rules if r[1]),
            "failed": len([r for r in rules if not r[1]]),
            "failing": failed_rules[:10],
            "window_days": RULE_WINDOW.days,
        },
        "open_anomalies": sum(anomalies.values()),
        "profile": {
            "oldest_profiled_at": oldest.isoformat() if oldest else None,
            "unprofiled_tables": [t for t in existing if t not in profiles],
            "stale": any(e.get("profile_stale") for e in per_table if e["exists"]),
            "stale_after_hours": stale_after,
        },
        "key_columns": list(keys),
        "row_trend": trend,
        "tables": per_table,
        "metadata_completeness": metadata_completeness(spec, column_doc_pct),
        "measured_at": now.isoformat() + "Z",
    }


# =============================================================================
# Row trends (status page sparkline)
# =============================================================================


def row_trends(engine, specs: Sequence[DatasetSpec], days: int = TREND_DAYS,
               today: Optional[date] = None) -> Dict[str, List[Dict[str, Any]]]:
    """Dataset key -> daily row counts (summed over its tables) from the
    daily quality snapshots. One query; datasets with no points are left out."""
    engine = engine_of(engine)
    since = (today or date.today()) - timedelta(days=days)
    rows = _run(engine, """
        SELECT snapshot_date, table_name, max(row_count)
        FROM dq_quality_snapshots
        WHERE snapshot_date >= :since AND row_count IS NOT NULL AND table_name IS NOT NULL
        GROUP BY snapshot_date, table_name
    """, {"since": since})
    seen = {r[1] for r in rows}
    by_table: Dict[str, List[Tuple[Any, str, int]]] = {}
    for r in rows:
        by_table.setdefault(r[1], []).append((r[0], r[1], r[2]))
    out: Dict[str, List[Dict[str, Any]]] = {}
    for spec in specs:
        pts = [p for t in spec_tables(spec, seen) if t in seen for p in by_table[t]]
        if pts:
            out[spec.key] = _trend(pts)
    return out


# =============================================================================
# Freshness from the SPEC_124 verdict
# =============================================================================


def freshness_by_dataset(db) -> Dict[str, Optional[float]]:
    """Dataset key -> freshness score from its status. Empty when the status
    cannot be computed (the caller falls back)."""
    try:
        from app.services.dataset_status import build_status

        body = build_status(db, scheduler=None, include_errors=False)
    except Exception as e:
        logger.warning(f"[catalog.quality] dataset status unavailable for freshness: {type(e).__name__}")
        try:
            db.rollback()
        except Exception:
            pass
        return {}
    return {d["key"]: FRESHNESS_BY_STATUS.get(d["status"]) for d in body.get("datasets", [])}


# =============================================================================
# Post-load hook
# =============================================================================


def post_load_enabled() -> bool:
    return os.getenv("CATALOG_POST_LOAD_PROFILE", "1").strip().lower() not in ("0", "false", "no", "off")


def post_load(engine, producer: Optional[str], job_id: Optional[int] = None) -> Dict[str, Any]:
    """Advisory: profile the tables of the datasets ``producer`` writes.

    Called after a bulk run that loaded releases and after a successful mart
    build. Tables above ``POST_LOAD_MAX_ROWS`` are left to the scheduled
    profiler. Never raises.
    """
    summary: Dict[str, Any] = {"producer": producer, "profiled": [], "skipped_large": [], "errors": 0}
    if not producer or not post_load_enabled():
        summary["skipped"] = "disabled" if producer else "no producer"
        return summary
    try:
        from sqlalchemy.orm import sessionmaker

        from app.catalog.job_keys import default_map
        from app.catalog.registry import get_spec
        from app.core.data_profiling_service import profile_table

        engine = engine_of(engine)
        keys = default_map().datasets_for(producer)
        if not keys:
            return summary
        rels = relations(engine)
        base = base_tables(rels)
        db = sessionmaker(bind=engine)()
        try:
            for key in keys:
                spec = get_spec(key)
                for t in spec_tables(spec, base) if spec else []:
                    if t not in base:
                        continue
                    est = row_estimate(rels.get(t))
                    if est is not None and est > POST_LOAD_MAX_ROWS:
                        summary["skipped_large"].append(t)
                        continue
                    try:
                        if profile_table(db, t, job_id=job_id, source=spec.source):
                            summary["profiled"].append(t)
                    except Exception as e:
                        summary["errors"] += 1
                        logger.warning(f"[catalog.quality] post-load profile of {t} failed: {type(e).__name__}")
                        db.rollback()
        finally:
            db.close()
    except Exception as e:
        summary["errors"] += 1
        logger.warning(f"[catalog.quality] post-load hook for {producer} failed: {type(e).__name__}: {e}")
    return summary
