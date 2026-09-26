"""
Per-dataset quality signals (SPEC_144).

The DQ framework used to pick its tables from ``DatasetRegistry.ingested()``,
which leaves out every table no per-source ingestor registered: all SEC bulk
tables, the PE marts and ``core.*``. Here the catalog decides:

- ``dq_targets``   the base tables catalog specs resolve to (never views), plus
                   the ingested registry rows the catalog does not cover.
- ``quality_block`` the quality section of ``GET /catalog/{key}``: live state
                   and flags, latest DQ score, rule results, profile age,
                   key-column nulls, row trend and a metadata completeness score.
                   A plain GET reuses the detail's live counts and the seed
                   counts the profiler stored; only an admin ``refresh`` scans.
- ``row_trends``   daily row counts per dataset from ``dq_quality_snapshots``.
- ``post_load``    the advisory hook the bulk runner and the mart builds call.

Everything that reads user tables runs under a short ``statement_timeout`` and
uses the planner estimate above a size limit. Nothing here deletes or moves
data: seeded or fabricated rows are flagged (``seed_contaminated`` plus
``flags``), never touched.

Vocabulary: ``live_state`` is what this module measures now (``LIVE_STATES``).
The verified state is SPEC_141's ``DatasetSpec.data_state`` (``fabricated``,
``seeded`` ...); until that field exists the same verdicts come from
``CURATED_DATA_STATES`` (PLAN_088 §1.3/§1.4). The verified state feeds the live
one (a fabricated dataset is never ``populated``) and both land in ``flags``.
Shared tables: SPEC_141's ``DatasetSpec.row_filters`` scope a dataset's rows
in a table other datasets also declare; a filtered table gets filtered counts
and no whole-table score, profile facts or row trend.
"""

from __future__ import annotations

import json
import logging
import os
import re
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

LIVE_STATES = ("phantom", "empty", "populated", "defective", "seed_contaminated")

# SPEC_141 DATA_STATES that mean "these rows are not real data" (flagged, never removed)
FLAG_STATES = ("fabricated", "seeded", "sample_mixed", "placeholder", "demo")
# Verified states until SPEC_141's DatasetSpec.data_state lands: the values
# SPEC_141 sets (the spec field wins once it exists). PLAN_088 §1.3 and §1.4.
CURATED_DATA_STATES: Dict[str, str] = {
    "treasury_debt_outstanding": "key_columns_null",
    "usaspending_awards": "key_columns_null",
    "fema_pa_projects": "key_columns_null",
    "cms_hospital_cost_reports": "key_columns_null",
    "public_lp_strategies": "fabricated",
    "si_seismic_hazard": "fabricated",
    "si_incentive_deals": "fabricated",
    "si_natural_gas_infra": "seeded",
    "si_foreign_trade_zones": "seeded",
    "si_incentive_programs": "seeded",
    "si_certified_sites": "seeded",
    "si_power_plants": "sample_mixed",
    "si_public_water_systems": "sample_mixed",
    "si_grid_infrastructure": "sample_mixed",
    "si_renewable_resources": "sample_mixed",
    "si_utility_rates": "sample_mixed",
    "si_water_monitoring": "sample_mixed",
    "si_intermodal_terminals": "placeholder",
    "si_opportunity_zones": "placeholder",
    "glassdoor": "demo",
    "app_rankings": "demo",
    "web_traffic": "demo",
}

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

# Provenance columns whose value can mark a seeded, sample, demo or hand-typed
# row, and those values (PLAN_088 §1.4): ILIKE patterns, a LIKE suffix, exact values.
SEED_MARKER_COLUMNS = ("source", "data_source", "origin")
SEED_SOURCE_ILIKE = "%sample%"
SEED_SOURCE_LIKE = "%\\_seed"
SEED_SOURCE_VALUES = ("nrel_reference", "demo_seeder", "gjf_expanded")
# pii_class / redistribution that keep raw column values out of stored profiles
RAW_VALUE_SAFE_PII = ("none",)

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
# Static verdicts, shared tables, raw-value policy
# =============================================================================


def verified_state(spec: DatasetSpec) -> Optional[str]:
    """SPEC_141's ``data_state`` when the spec carries one, else the curated
    PLAN_088 verdict (None when neither says anything)."""
    return getattr(spec, "data_state", None) or CURATED_DATA_STATES.get(spec.key)


def static_flags(spec: DatasetSpec) -> List[str]:
    """Flags known without touching the database (list view, status page)."""
    state = verified_state(spec)
    return [state] if state in FLAG_STATES + ("key_columns_null",) else []


def row_filters(spec: DatasetSpec) -> Dict[str, str]:
    """table -> read-only predicate (SPEC_141 ``row_filters``; {} before it)."""
    return dict(getattr(spec, "row_filters", ()) or ())


def shared_tables(specs: Iterable[DatasetSpec]) -> Dict[str, List[str]]:
    """Declared table -> the dataset keys declaring it, for tables >1 declare."""
    by: Dict[str, List[str]] = {}
    for s in specs:
        for t in s.tables:
            by.setdefault(t, []).append(s.key)
    return {t: ks for t, ks in by.items() if len(ks) > 1}


def _sensitive(spec: DatasetSpec) -> bool:
    return spec.pii_class not in RAW_VALUE_SAFE_PII or spec.redistribution == "restricted"


def raw_values_restricted(table: str, specs: Optional[Iterable[DatasetSpec]] = None) -> bool:
    """True when a dataset holding ``table`` carries personal/contact data or is
    restricted: stored profiles then keep no raw column values (top_values)."""
    from app.catalog.tables import pattern_matches

    if specs is None:
        from app.catalog.registry import get_catalog

        specs = get_catalog()
    for s in specs:
        if not _sensitive(s):
            continue
        if table in s.tables or ("." not in table and any(pattern_matches(p, table)
                                                          for p in s.table_patterns)):
            return True
    return False


def seed_predicate(columns: Sequence[str], engine=None) -> Tuple[str, Dict[str, Any]]:
    """``(sql, params)`` true for a row any marker column flags as seeded/sample."""
    params: Dict[str, Any] = {"sp_i": SEED_SOURCE_ILIKE, "sp_l": SEED_SOURCE_LIKE}
    values = []
    for i, v in enumerate(SEED_SOURCE_VALUES):
        params[f"sp_v{i}"] = v
        values.append(f":sp_v{i}")
    ilike = "ILIKE" if engine is None or _is_pg(engine) else "LIKE"
    parts = []
    for c in columns:
        col = f"CAST({qi(c)} AS TEXT)"
        parts.append(f"({col} {ilike} :sp_i OR {col} LIKE :sp_l OR {col} IN ({', '.join(values)}))")
    return " OR ".join(parts) or "FALSE", params


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
    catalog = get_catalog()
    claimed = claimed_tables(catalog)
    shared = shared_tables(catalog)
    filtered = {t for s in catalog for t in row_filters(s)}
    out: Dict[str, Dict[str, Any]] = {}
    for spec in specs:
        for t in spec_tables(spec, base, claimed):
            if t in base and t not in out:
                # a shared table is one DQ target, attributed to the first
                # dataset; the others read it through their row filters
                out[t] = {"table": t, "dataset_key": spec.key, "source": spec.source,
                          "domain": None, "rows_estimate": row_estimate(rels.get(t)),
                          "shared_by": shared.get(t, []), "row_filtered": t in filtered}
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
                      "rows_estimate": row_estimate(rels.get(t)),
                      "shared_by": [], "row_filtered": False}
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


# config keys that name the table(s) a job loaded outright
GATE_TABLE_KEYS = ("table", "table_name", "tables")


def _job_tokens(job) -> Set[str]:
    """Lower-case word tokens of the job's config values and of its source
    suffix (``dunl:ports`` -> ``ports``); ``_``-prefixed keys are internal."""
    tokens: Set[str] = set()

    def add(v) -> None:
        if isinstance(v, (list, tuple, set)):
            for x in v:
                add(x)
        elif isinstance(v, (str, int)) and not isinstance(v, bool):
            tokens.update(t for t in re.split(r"[^a-z0-9]+", str(v).lower()) if t)

    config = getattr(job, "config", None)
    if isinstance(config, dict):
        for k, v in config.items():
            if not str(k).startswith("_"):
                add(v)
    source = getattr(job, "source", None) or ""
    if ":" in source:
        add(source.split(":", 1)[1])
    return tokens


def _table_tokens(table: str) -> Set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", table.lower()) if t}


def gate_tables_for_job(db, job, max_tables: int = 6) -> List[str]:
    """The tables the quality gate checks for a finished job, most specific first.

    1. the registry row for the job's source updated since the job started
       (the table it just loaded), and tables the config names outright;
    2. else the dataset's tables that best match the config values
       (``acs5`` + ``2023`` + ``B01001`` -> ``acs5_2023_b01001``; ``oes`` -> ``bls_oes``);
    3. only when nothing more specific is known: the source's latest-updated
       registry table, then the dataset's tables in catalog order.
    """
    engine = engine_of(db.get_bind())
    base = base_tables(relations(engine))
    catalog = dataset_tables_for_job(db, job)

    reg_table, reg_by_job = None, False
    try:
        from app.core.models import DatasetRegistry

        reg = (db.query(DatasetRegistry)
               .filter(DatasetRegistry.source == job.source, DatasetRegistry.ingested())
               .order_by(DatasetRegistry.last_updated_at.desc())
               .first())
        if reg is not None and reg.table_name:
            reg_table = reg.table_name
            started = getattr(job, "started_at", None) or getattr(job, "created_at", None)
            reg_by_job = bool(started and reg.last_updated_at and reg.last_updated_at >= started)
    except Exception as e:
        logger.debug(f"[catalog.quality] gate registry lookup failed: {type(e).__name__}")
        db.rollback()

    specific: List[str] = []
    if reg_by_job:
        specific.append(reg_table)
    config = getattr(job, "config", None)
    if isinstance(config, dict):
        for k in GATE_TABLE_KEYS:
            v = config.get(k)
            for name in (v if isinstance(v, (list, tuple)) else [v]):
                if isinstance(name, str) and name.lower() in base:
                    specific.append(name.lower())
    if not specific and catalog:
        tokens = _job_tokens(job)
        scored = [(len(_table_tokens(t) & tokens), t) for t in catalog]
        top = max((s for s, _ in scored), default=0)
        if top > 0:
            specific = [t for s, t in scored if s == top]
    if specific:
        ordered = specific
    else:
        ordered = ([reg_table] if reg_table else []) + catalog
    out = [t for t in dict.fromkeys(ordered) if t in base or t == reg_table]
    return out[:max_tables]


# =============================================================================
# Data state
# =============================================================================


def classify_live_state(tables: Sequence[Mapping[str, Any]],
                        verified: Optional[str] = None) -> Tuple[str, List[str], List[str]]:
    """``(state, reasons, flags)`` from per-table facts and the verified state.

    Each table: ``{table, exists, rows, key_null_columns, all_null_share,
    seed_rows, seed_scan}``; ``rows`` None means unknown (a filtered estimate),
    not empty. The state is the worst finding: phantom (no declared table
    exists) > empty (none has rows) > defective > seed_contaminated >
    populated. ``flags`` lists every finding, so a defective dataset that is
    also seeded says both. A verified ``key_columns_null`` makes it defective
    and a verified fabricated/seeded/sample/placeholder/demo state makes it
    seed_contaminated even when no row carries a marker.
    """
    existing = [t for t in tables if t.get("exists")]
    missing = [t["table"] for t in tables if not t.get("exists")]
    static = [verified] if verified in FLAG_STATES + ("key_columns_null",) else []
    if not existing:
        return "phantom", [f"no table exists ({', '.join(missing) or 'none declared'})"], static
    with_rows = [t for t in existing if t.get("rows") is None or t["rows"] > 0]
    if not with_rows:
        return "empty", [f"{len(existing)} table(s) exist with 0 rows"], static
    flags: List[str] = []
    defects: List[str] = []
    for t in with_rows:
        cols = t.get("key_null_columns") or []
        if cols:
            defects.append(f"{t['table']}: key column(s) all NULL: {', '.join(cols)}")
            flags.append("key_columns_null")
        share = t.get("all_null_share")
        if share is not None and share >= DEFECTIVE_ALL_NULL_SHARE:
            defects.append(f"{t['table']}: {round(share * 100)}% of data columns all NULL")
            flags.append("all_null_columns")
    seeded = [t for t in with_rows if (t.get("seed_rows") or 0) > 0]
    seeds = [f"{t['table']}: {t['seed_rows']} seed/sample row(s)" for t in seeded]
    if seeded:
        flags.append("seed_rows")
    notes: List[str] = []
    if verified == "key_columns_null" and not defects:
        notes.append("verified: key columns NULL on every row (catalog truth pass)")
    elif verified in FLAG_STATES:
        notes.append(f"verified: {verified.replace('_', ' ')} data (catalog truth pass)")
    skipped = [t["table"] for t in with_rows if t.get("seed_scan") in ("skipped", "failed")]
    if skipped:
        notes.append(f"seed scan skipped or failed: {', '.join(skipped)}")
    if missing:
        notes.append(f"missing table(s): {', '.join(missing)}")
    unknown = [t["table"] for t in with_rows if t.get("rows") is None]
    if unknown:
        notes.append(f"row count unknown: {', '.join(unknown)}")
    flags = list(dict.fromkeys(static + flags))
    if defects or verified == "key_columns_null":
        return "defective", defects + seeds + notes, flags
    if seeds or verified in FLAG_STATES:
        return "seed_contaminated", seeds + notes, flags
    return "populated", notes, flags


def classify_data_state(tables: Sequence[Mapping[str, Any]],
                        verified: Optional[str] = None) -> Tuple[str, List[str]]:
    """``(state, reasons)``: ``classify_live_state`` without the flags."""
    state, reasons, _flags = classify_live_state(tables, verified)
    return state, reasons


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


def _count(engine, table: str, estimate: Optional[int],
           where: Optional[str] = None) -> Tuple[Optional[int], bool]:
    """Exact count up to ``EXACT_COUNT_MAX_ROWS``, else the estimate. With a
    row filter (a validated catalog predicate) the whole-table estimate is
    wrong, so a count that cannot be exact is None."""
    if estimate is None or estimate <= EXACT_COUNT_MAX_ROWS:
        sql = f"SELECT count(*) FROM {_fq(table, engine)}" + (f" WHERE ({where})" if where else "")
        try:
            return int(_run(engine, sql)[0][0]), True
        except Exception:
            pass
    return (None if where else estimate), False


def marker_columns(engine, table: str) -> List[str]:
    """The ``SEED_MARKER_COLUMNS`` the table has."""
    schema, name = split(table)
    if not _is_pg(engine):
        have = {c["name"] for c in inspect(engine).get_columns(name)}
    else:
        rows = _run(engine, "SELECT column_name FROM information_schema.columns "
                            "WHERE table_schema = :s AND table_name = :t AND column_name IN :cs",
                    {"s": schema, "t": name, "cs": list(SEED_MARKER_COLUMNS)}, expanding=("cs",))
        have = {r[0] for r in rows}
    return [c for c in SEED_MARKER_COLUMNS if c in have]


def seed_rows(engine, table: str, estimate: Optional[int],
              where: Optional[str] = None) -> Optional[int]:
    """Rows a provenance column (``source``, ``data_source``, ``origin``)
    marks as seeded, sample, demo or hand-typed; None when not scanned
    (above ``SEED_SCAN_MAX_ROWS``)."""
    if estimate is not None and estimate > SEED_SCAN_MAX_ROWS:
        return None
    cols = marker_columns(engine, table)
    if not cols:
        return 0
    pred, params = seed_predicate(cols, engine)
    sql = f"SELECT count(*) FROM {_fq(table, engine)} WHERE ({pred})" + (f" AND ({where})" if where else "")
    return int(_run(engine, sql, params)[0][0])


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
        # the seed count the profiler stored on the marker column (no scan here)
        seeds: Dict[int, int] = {}
        for sid, stats in _run(engine, "SELECT snapshot_id, stats FROM data_profile_columns "
                                       "WHERE snapshot_id IN :ids AND column_name IN :cs",
                               {"ids": [p["id"] for p in out.values()],
                                "cs": list(SEED_MARKER_COLUMNS)}, expanding=("ids", "cs")):
            if isinstance(stats, str):
                try:
                    stats = json.loads(stats)
                except ValueError:
                    stats = None
            n = stats.get("seed_rows") if isinstance(stats, dict) else None
            if isinstance(n, int):
                seeds[sid] = max(seeds.get(sid, 0), n)
        for p in out.values():
            p["columns"] = by_id.get(p["id"], [])
            p["seed_rows"] = seeds.get(p["id"])
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
                  refresh: bool = False, column_doc_pct: Optional[float] = None,
                  live: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """The quality section of a catalog entry (cached ``CACHE_TTL_S``).

    A plain read (``refresh=False``) scans no user table: row counts come from
    ``live`` (the detail's own ``dataset_live`` result) or the planner
    estimate, and seed counts from the latest profile, where the profiler
    stored them. ``refresh=True`` (admin only at the API) runs the exact
    counts and the seed scans under ``QUERY_TIMEOUT_MS``.
    """
    engine = engine_of(engine)
    if not refresh:
        with _lock:
            hit = _cache.get(f"block:{spec.key}")
        if hit and time.monotonic() - hit[0] < CACHE_TTL_S:
            return hit[1]
    block = _compute_block(engine, spec, now or datetime.utcnow(), column_doc_pct,
                           scan=refresh, live=live)
    with _lock:
        _cache[f"block:{spec.key}"] = (time.monotonic(), block)
    return block


def _live_rows(live: Optional[Mapping[str, Any]]) -> Dict[str, Tuple[Optional[int], bool]]:
    out: Dict[str, Tuple[Optional[int], bool]] = {}
    for t in (live or {}).get("tables") or []:
        if isinstance(t, Mapping) and t.get("exists") and t.get("table"):
            out[t["table"]] = (t.get("rows"), bool(t.get("rows_exact")))
    return out


def _compute_block(engine, spec: DatasetSpec, now: datetime,
                   column_doc_pct: Optional[float], scan: bool = True,
                   live: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    from app.catalog.registry import get_catalog

    rels = relations(engine, cached=True)
    base = base_tables(rels)
    tables = spec_tables(spec, base)
    existing = [t for t in tables if t in base]
    filters = row_filters(spec)
    shared = shared_tables(get_catalog())
    # whole-table signals (profile, score, rules, trend) describe a filtered
    # table's other datasets too: they are not attributed to this one
    whole = [t for t in existing if t not in filters]
    profiles = _safe("profiles", lambda: _latest_profiles(engine, whole), {}) if whole else {}
    since = (now - timedelta(days=TREND_DAYS)).date()
    snaps = _safe("snapshots", lambda: _snapshots(engine, whole, since), []) if whole else []
    rules = _safe("rules", lambda: _rule_results(engine, whole, now - RULE_WINDOW), []) \
        if whole else []
    anomalies = _safe("anomalies", lambda: _open_anomalies(engine, whole), {}) if whole else {}
    live_rows = _live_rows(live)

    keys = key_columns(spec)
    stale_after = cadence_hours(spec.cadence) * STALE_PROFILE_FACTOR
    per_table: List[Dict[str, Any]] = []
    for t in tables:
        entry: Dict[str, Any] = {"table": t, "exists": t in base}
        if t in shared:
            entry["shared_with"] = [k for k in shared[t] if k != spec.key]
        where = filters.get(t)
        if where:
            entry["row_filter"] = where
        if not entry["exists"]:
            per_table.append(entry)
            continue
        est = row_estimate(rels.get(t))
        if scan:
            rows, exact = _safe("count", lambda: _count(engine, t, est, where),
                                (None if where else est, False))
        elif t in live_rows:
            rows, exact = live_rows[t]
        else:
            rows, exact = (None if where else est), False
        entry.update(rows=rows, rows_exact=exact)
        if where:
            # whole-table quality would be another dataset's numbers
            entry.update(profiled_at=None, profile_stale=None, quality_score=None,
                         open_anomalies=None, rules=None)
            if scan and rows:
                n = _safe("seed", lambda: seed_rows(engine, t, est, where), None)
                entry.update(seed_rows=n, seed_scan="scan" if n is not None else "skipped")
            else:
                entry.update(seed_rows=None, seed_scan="filtered")
            per_table.append(entry)
            continue
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
        if rows is None or rows > 0:
            if scan:
                too_big = est is not None and est > SEED_SCAN_MAX_ROWS
                n = _safe("seed", lambda: seed_rows(engine, t, est), None)
                entry.update(seed_rows=n, seed_scan="scan" if n is not None
                             else ("skipped" if too_big else "failed"))
            elif prof and prof.get("seed_rows") is not None:
                entry.update(seed_rows=prof["seed_rows"], seed_scan="profile")
            else:
                entry.update(seed_rows=None, seed_scan="pending")
        latest = [s for s in snaps if s[1] == t]
        entry["quality_score"] = latest[-1][2] if latest else None
        entry["open_anomalies"] = anomalies.get(t, 0)
        mine = [r for r in rules if r[0] == t]
        entry["rules"] = {"passed": sum(1 for r in mine if r[1]), "failed": sum(1 for r in mine if not r[1])}
        per_table.append(entry)

    verified = verified_state(spec)
    state, reasons, flags = classify_live_state(per_table, verified)
    latest_date = max((s[0] for s in snaps), default=None)
    latest_snaps = [s for s in snaps if s[0] == latest_date]
    failed_rules = sorted({r[2] or "unnamed rule" for r in rules if not r[1]})
    profiled = [p["profiled_at"] for p in profiles.values() if p.get("profiled_at")]
    oldest = min(profiled) if profiled else None
    # a trend over part of a dataset's tables would read as a drop or a jump
    trend = _trend((s[0], s[1], s[7]) for s in snaps) if len(whole) == len(existing) else []
    return {
        "live_state": state,
        "live_state_reasons": reasons,
        "flags": flags,
        "verified_state": verified,
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
            "unprofiled_tables": [t for t in whole if t not in profiles],
            "stale": any(e.get("profile_stale") for e in per_table if e["exists"]),
            "stale_after_hours": stale_after,
        },
        "key_columns": list(keys),
        "row_trend": trend,
        "tables": per_table,
        "metadata_completeness": metadata_completeness(spec, column_doc_pct),
        "measured": "scan" if scan else "cached",
        "measured_at": now.isoformat() + "Z",
    }


# =============================================================================
# Row trends (status page sparkline)
# =============================================================================


def row_trends(engine, specs: Sequence[DatasetSpec], days: int = TREND_DAYS,
               today: Optional[date] = None) -> Dict[str, List[Dict[str, Any]]]:
    """Dataset key -> daily row counts (summed over its tables) from the
    daily quality snapshots. One query; datasets with no points are left out,
    and so is any dataset with a row-filtered (shared) table: the snapshots
    count the whole table, not the dataset's share of it."""
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
        if row_filters(spec):
            continue
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
