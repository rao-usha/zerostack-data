"""
Dataset status (SPEC_124): one honest status per catalog dataset.

Three clocks per dataset (PLAN_085 §3):

- **run**      when did we last collect? A UNION of every trustworthy store:
               ``job_queue``, ``ingestion_jobs`` (terminal rows only: their
               PENDING/RUNNING rows are the zombies PLAN_085 §8.2 found),
               ``raw.source_release``, ``core.mart_build`` and
               ``site_intel_collection_job``, mapped to datasets through the
               catalog's producers (``app.catalog.job_keys``).
- **publish**  when did upstream last release? ``raw.source_release`` (bulk).
- **coverage** what period does the data cover (catalog ``coverage_sql``),
               against what it should cover by now (cadence + SLO lag).

The status comes from a closed vocabulary (``STATUSES``); the first matching
rule wins (see ``derive``). There is never a ``current`` without an
expectation: no SLO means ``unknown``.

Everything is fetched in a constant number of statements, whatever the
number of datasets or job rows: one aggregate query per store, one for the
schedules, one for per-schedule success, one for heartbeats, one for relation
stats, one combined coverage statement (cached). Never a loop of queries per
dataset -- ``get_all_source_health`` did 3 per source and took ~17 s.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.catalog.datasets import BATCH_SCHEDULED_DISPATCH
from app.catalog.job_keys import (
    MART_JOB_TYPES,
    ProducerMap,
    base_producer,
)
from app.catalog.spec import DatasetSpec
from app.core.preflight import api_key_preflight

logger = logging.getLogger(__name__)

STATUSES = (
    "current", "awaiting_upstream", "behind", "stalled", "failing",
    "partial", "blocked", "dormant", "never_run", "unknown",
)

FAILING_WINDOW = timedelta(days=30)     # a failure older than this is history, not "failing"
ACTIVE_WINDOW = timedelta(hours=24)     # a queued/running row older than this is not "already running"
WORKER_LIVE_MINUTES = 5
STALL_FACTOR = 1.5                      # same as the SPEC_128 watchdog
MISSED_RUNS_WINDOW = timedelta(days=30)
BATCH_CRON = "0 2 * * *"                # main.py registers the nightly batch at 02:00 UTC
BATCH_JOB_ID = "batch_collection"
COVERAGE_TTL_S = 300
COVERAGE_TIMEOUT_MS = 3000             # the one combined coverage statement
COVERAGE_FALLBACK_TIMEOUT_MS = 1000    # each statement when the combined one failed
COVERAGE_FALLBACK_DEADLINE_S = 5.0     # after this, the rest are "unavailable" until the TTL
PARTIAL_PREFIX = "PARTIAL:"             # app.core.ingestion_job_sync.PARTIAL_PREFIX
SUPERSEDED_PREFIX = "superseded:"       # app.ingest.bulk.retention.SUPERSEDED_PREFIX
BUSY_PREFIX = "busy:"                   # app.marts.build_ledger.BUSY_PREFIX

CADENCE_HOURS = {
    "daily": 24,
    "weekly": 7 * 24,
    "monthly": 31 * 24,
    "quarterly": 92 * 24,
    "annual": 366 * 24,
}

# job:<type> producers the run endpoint can enqueue with an empty payload.
# The collection types (people, pe, lp, fo, agentic, foot_traffic) need a
# per-target payload from their own routers.
RUNNABLE_JOB_TYPES = ("entity_resolve", "pe_mart_build")

RERUN_WARNINGS = {
    "destructive": "a rerun replaces the dataset's rows",
    "currency_only": "keeps only the newest edition: a rerun refreshes currency, it does not recover history",
    "append_only": "a rerun appends rows; nothing is deduplicated",
}


def catalog_specs() -> Tuple[DatasetSpec, ...]:
    from app.catalog.registry import get_catalog

    return get_catalog()


# =============================================================================
# Coverage expectation
# =============================================================================


def _month_end_on_or_before(d: date) -> date:
    if (d + timedelta(days=1)).day == 1:
        return d
    return d.replace(day=1) - timedelta(days=1)


def _quarter_end_on_or_before(d: date) -> date:
    if d.month in (3, 6, 9, 12) and (d + timedelta(days=1)).day == 1:
        return d
    first_month = ((d.month - 1) // 3) * 3 + 1
    return date(d.year, first_month, 1) - timedelta(days=1)


def expected_through(cadence: Optional[str], slo_lag_hours: Optional[int],
                     now: datetime) -> Optional[date]:
    """The last cadence period end that should be loaded by ``now``.

    The period end must fall strictly before the day ``now - slo_lag`` falls
    on. No SLO, or a cadence with no period (``ad_hoc``), is no expectation.
    """
    if slo_lag_hours is None or cadence not in CADENCE_HOURS:
        return None
    d = (now - timedelta(hours=slo_lag_hours)).date() - timedelta(days=1)
    if cadence == "daily":
        return d
    if cadence == "weekly":  # weeks end on Sunday
        return d - timedelta(days=(d.weekday() - 6) % 7)
    if cadence == "monthly":
        return _month_end_on_or_before(d)
    if cadence == "quarterly":
        return _quarter_end_on_or_before(d)
    return d if (d.month, d.day) == (12, 31) else date(d.year - 1, 12, 31)


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _iso(dt: Optional[datetime]) -> Optional[str]:
    dt = _naive_utc(dt)
    return dt.isoformat() + "Z" if dt else None


# =============================================================================
# Evidence
# =============================================================================


@dataclass
class Event:
    store: str
    at: datetime
    status: str          # success | partial | failed | refused | running | pending | claimed | blocked | cancelled
    duration_s: Optional[float] = None
    error: Optional[str] = None


TERMINAL = ("success", "partial", "failed", "refused")


@dataclass
class Evidence:
    last_run: Optional[Event] = None
    last_terminal: Optional[Event] = None
    last_success_at: Optional[datetime] = None
    success_duration_s: Optional[float] = None
    active: int = 0
    mart_refusal: Optional[str] = None
    mart_running: bool = False
    releases: Optional[Dict[str, Any]] = None

    def add(self, ev: Optional[Event]) -> None:
        if ev is None or ev.at is None:
            return
        ev.at = _naive_utc(ev.at)
        if self.last_run is None or ev.at > self.last_run.at:
            self.last_run = ev
        if ev.status in TERMINAL and (self.last_terminal is None or ev.at > self.last_terminal.at):
            self.last_terminal = ev

    def success(self, at: Optional[datetime], duration_s: Optional[float] = None) -> None:
        at = _naive_utc(at)
        if at is None:
            return
        if self.last_success_at is None or at > self.last_success_at:
            self.last_success_at = at
            if duration_s is not None:
                self.success_duration_s = duration_s


def _f(v) -> Optional[float]:
    return round(float(v), 1) if v is not None else None


# =============================================================================
# Facts: a constant number of statements
# =============================================================================

_EXISTS_SQL = """
SELECT to_regclass('public.job_queue') IS NOT NULL,
       to_regclass('public.ingestion_jobs') IS NOT NULL,
       to_regclass('raw.source_release') IS NOT NULL,
       to_regclass('core.mart_build') IS NOT NULL,
       to_regclass('public.site_intel_collection_job') IS NOT NULL,
       to_regclass('public.ingestion_schedules') IS NOT NULL,
       to_regclass('public.worker_heartbeats') IS NOT NULL,
       EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'ingestion_jobs'
                 AND column_name = 'dataset_key')
"""

# One row per (job_type, producer-ish payload keys). ``stx`` folds PARTIAL:
# successes into 'partial'. Statuses compared lower-case (one live row is
# 'FAILED' against thousands of 'failed').
_QUEUE_SQL = """
WITH q AS (
    SELECT id, CAST(job_type AS TEXT) AS job_type,
           payload->>'bulk_source' AS bulk_source,
           payload->>'source' AS source,
           payload->'config'->>'dataset' AS dataset,
           CASE WHEN CAST(job_type AS TEXT) = 'site_intel'
                THEN CAST(payload->'sources' AS TEXT) END AS sources,
           CASE WHEN LOWER(CAST(status AS TEXT)) = 'success'
                     AND COALESCE(error_message, '') LIKE :partial THEN 'partial'
                ELSE LOWER(CAST(status AS TEXT)) END AS stx,
           created_at, started_at, completed_at,
           COALESCE(completed_at, started_at, created_at) AS ts,
           LEFT(COALESCE(error_message, ''), 300) AS err,
           EXTRACT(EPOCH FROM (completed_at - started_at)) AS dur
    FROM job_queue
)
SELECT job_type, bulk_source, source, dataset, sources,
       MAX(ts) AS last_at,
       (array_agg(stx ORDER BY ts DESC, id DESC))[1] AS last_status,
       (array_agg(dur ORDER BY ts DESC, id DESC))[1] AS last_duration_s,
       MAX(ts) FILTER (WHERE stx IN ('success', 'partial', 'failed')) AS term_at,
       (array_agg(stx ORDER BY ts DESC, id DESC)
            FILTER (WHERE stx IN ('success', 'partial', 'failed')))[1] AS term_status,
       (array_agg(err ORDER BY ts DESC, id DESC)
            FILTER (WHERE stx IN ('success', 'partial', 'failed')))[1] AS term_error,
       MAX(completed_at) FILTER (WHERE stx IN ('success', 'partial')) AS success_at,
       (array_agg(dur ORDER BY completed_at DESC)
            FILTER (WHERE stx IN ('success', 'partial') AND dur IS NOT NULL))[1] AS success_duration_s,
       COUNT(*) FILTER (WHERE stx IN ('pending', 'claimed', 'running', 'blocked')
                          AND created_at >= :active_since) AS active
FROM q
GROUP BY job_type, bulk_source, source, dataset, sources
"""

# Terminal rows only: PENDING/RUNNING ingestion_jobs rows are unreliable
# (PLAN_085 §8.2); a live run is visible in job_queue.
_JOBS_SQL = """
WITH j AS (
    SELECT id, {dataset_key} AS dataset_key, source, config->>'dataset' AS dataset,
           CASE WHEN LOWER(CAST(status AS TEXT)) = 'success'
                     AND COALESCE(error_message, '') LIKE :partial THEN 'partial'
                ELSE LOWER(CAST(status AS TEXT)) END AS stx,
           started_at, completed_at,
           COALESCE(completed_at, started_at, created_at) AS ts,
           LEFT(COALESCE(error_message, ''), 300) AS err,
           EXTRACT(EPOCH FROM (completed_at - started_at)) AS dur
    FROM ingestion_jobs
    WHERE LOWER(CAST(status AS TEXT)) IN ('success', 'failed')
)
SELECT dataset_key, source, dataset,
       MAX(ts) AS term_at,
       (array_agg(stx ORDER BY ts DESC, id DESC))[1] AS term_status,
       (array_agg(err ORDER BY ts DESC, id DESC))[1] AS term_error,
       (array_agg(dur ORDER BY ts DESC, id DESC))[1] AS last_duration_s,
       MAX(completed_at) FILTER (WHERE stx IN ('success', 'partial')) AS success_at,
       (array_agg(dur ORDER BY completed_at DESC)
            FILTER (WHERE stx IN ('success', 'partial') AND dur IS NOT NULL))[1] AS success_duration_s
FROM j
GROUP BY dataset_key, source, dataset
"""

# Superseded rows (SPEC_122: failed + 'superseded:') are housekeeping, never
# failures. "unloaded" = published and not loaded, at or after the newest
# loaded release (old failed backfill quarters are not "behind").
_RELEASES_SQL = """
WITH r AS (
    SELECT id, source, release_key, status, bytes, discovered_at, fetched_at, loaded_at,
           updated_at, LEFT(COALESCE(error, ''), 300) AS err,
           (status = 'failed' AND COALESCE(error, '') LIKE :superseded) AS sup
    FROM raw.source_release
), w AS (
    SELECT r.*, MAX(discovered_at) FILTER (WHERE status = 'loaded')
                    OVER (PARTITION BY source) AS newest_loaded
    FROM r
)
SELECT source,
       MAX(discovered_at) FILTER (WHERE NOT sup) AS last_publish_at,
       MAX(loaded_at) FILTER (WHERE status = 'loaded') AS last_loaded_at,
       COUNT(*) FILTER (WHERE status = 'loaded') AS loaded,
       COUNT(*) FILTER (WHERE sup) AS superseded,
       COUNT(*) FILTER (WHERE status = 'failed' AND NOT sup) AS failed,
       COUNT(*) FILTER (WHERE status <> 'loaded' AND NOT sup
                          AND (newest_loaded IS NULL OR discovered_at >= newest_loaded)) AS unloaded,
       MAX(updated_at) FILTER (WHERE status = 'failed' AND NOT sup) AS last_failed_at,
       (array_agg(err ORDER BY updated_at DESC)
            FILTER (WHERE status = 'failed' AND NOT sup))[1] AS last_failed_error,
       (array_agg(bytes ORDER BY COALESCE(fetched_at, loaded_at, discovered_at) DESC)
            FILTER (WHERE bytes IS NOT NULL))[1] AS last_bytes,
       (array_agg(release_key ORDER BY discovered_at DESC, id DESC)
            FILTER (WHERE NOT sup))[1] AS latest_release_key
FROM w
GROUP BY source
"""

# Latest finished real build per mart; busy: refusals (a second build of a
# mart already building) are not a mart problem.
_MARTS_SQL = """
WITH m AS (
    SELECT *, (NOT dry_run AND status <> 'running'
               AND NOT (status = 'refused' AND COALESCE(refusal_reason, '') LIKE :busy)) AS is_real
    FROM core.mart_build
)
SELECT mart,
       (array_agg(status ORDER BY started_at DESC, id DESC) FILTER (WHERE is_real))[1] AS last_status,
       (array_agg(COALESCE(finished_at, started_at) ORDER BY started_at DESC, id DESC)
            FILTER (WHERE is_real))[1] AS last_at,
       (array_agg(LEFT(COALESCE(refusal_reason, error, ''), 300) ORDER BY started_at DESC, id DESC)
            FILTER (WHERE is_real))[1] AS last_reason,
       (array_agg(EXTRACT(EPOCH FROM (finished_at - started_at)) ORDER BY started_at DESC, id DESC)
            FILTER (WHERE is_real))[1] AS last_duration_s,
       MAX(finished_at) FILTER (WHERE status = 'success' AND NOT dry_run) AS success_at,
       (array_agg(EXTRACT(EPOCH FROM (finished_at - started_at)) ORDER BY finished_at DESC)
            FILTER (WHERE status = 'success' AND NOT dry_run AND finished_at IS NOT NULL))[1]
            AS success_duration_s,
       COUNT(*) FILTER (WHERE status = 'running' AND NOT dry_run
                          AND started_at >= :active_since) AS running
FROM m
GROUP BY mart
"""

_COLLECTORS_SQL = """
WITH c AS (
    SELECT id, source, LOWER(COALESCE(status, '')) AS st, created_at, started_at, completed_at,
           COALESCE(completed_at, started_at, created_at) AS ts,
           LEFT(COALESCE(error_message, ''), 300) AS err,
           EXTRACT(EPOCH FROM (completed_at - started_at)) AS dur
    FROM site_intel_collection_job
)
SELECT source,
       MAX(ts) AS last_at,
       (array_agg(st ORDER BY ts DESC, id DESC))[1] AS last_status,
       (array_agg(dur ORDER BY ts DESC, id DESC))[1] AS last_duration_s,
       MAX(ts) FILTER (WHERE st IN ('success', 'failed')) AS term_at,
       (array_agg(st ORDER BY ts DESC, id DESC) FILTER (WHERE st IN ('success', 'failed')))[1] AS term_status,
       (array_agg(err ORDER BY ts DESC, id DESC) FILTER (WHERE st IN ('success', 'failed')))[1] AS term_error,
       MAX(completed_at) FILTER (WHERE st = 'success') AS success_at,
       (array_agg(dur ORDER BY completed_at DESC)
            FILTER (WHERE st = 'success' AND dur IS NOT NULL))[1] AS success_duration_s,
       COUNT(*) FILTER (WHERE st IN ('pending', 'running') AND created_at >= :active_since) AS active
FROM c
GROUP BY source
"""

_RELATIONS_SQL = """
SELECT n.nspname, c.relname, c.reltuples::bigint,
       CASE WHEN c.relkind IN ('r', 'm') THEN pg_total_relation_size(c.oid) END
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%'
"""


@dataclass
class Facts:
    now: datetime
    evidence: Dict[str, Evidence] = field(default_factory=dict)
    schedules: List[Dict[str, Any]] = field(default_factory=list)
    schedule_success: Dict[int, datetime] = field(default_factory=dict)
    schedule_runs_30d: Dict[int, int] = field(default_factory=dict)
    live_workers: int = 0
    last_heartbeat_at: Optional[datetime] = None
    relations: Dict[str, Tuple[Optional[int], Optional[int]]] = field(default_factory=dict)

    def ev(self, key: str) -> Evidence:
        return self.evidence.setdefault(key, Evidence())


def _payload_from_queue_row(r) -> Dict[str, Any]:
    sources = None
    if r["sources"]:
        try:
            sources = json.loads(r["sources"])
        except (TypeError, ValueError):
            sources = None
    return {
        "bulk_source": r["bulk_source"],
        "source": r["source"],
        "config": {"dataset": r["dataset"]} if r["dataset"] else {},
        "sources": sources if isinstance(sources, list) else None,
    }


def collect_facts(db: Session, pmap: ProducerMap, now: datetime) -> Facts:
    facts = Facts(now=now)
    (has_queue, has_jobs, has_releases, has_marts, has_collectors, has_schedules,
     has_heartbeats, has_dataset_key) = db.execute(text(_EXISTS_SQL)).one()
    active_since = now - ACTIVE_WINDOW

    if has_queue:
        rows = db.execute(text(_QUEUE_SQL), {"partial": PARTIAL_PREFIX + "%",
                                             "active_since": active_since}).mappings().all()
        for r in rows:
            producers = pmap.producers_for_queue(r["job_type"], _payload_from_queue_row(r))
            keys = {k for p in producers for k in pmap.datasets_for(p)}
            for key in keys:
                ev = facts.ev(key)
                ev.add(Event("job_queue", r["last_at"], r["last_status"], _f(r["last_duration_s"])))
                if r["term_at"]:
                    ev.add(Event("job_queue", r["term_at"], r["term_status"], None, r["term_error"] or None))
                ev.success(r["success_at"], _f(r["success_duration_s"]))
                ev.active += int(r["active"] or 0)

    if has_jobs:
        sql = _JOBS_SQL.format(dataset_key="dataset_key" if has_dataset_key else "CAST(NULL AS TEXT)")
        rows = db.execute(text(sql), {"partial": PARTIAL_PREFIX + "%"}).mappings().all()
        for r in rows:
            source = r["source"] or ""
            if source.startswith("job:"):
                keys = pmap.datasets_for(source)   # one run rebuilds every stage
            elif r["dataset_key"]:
                keys = (r["dataset_key"],)
            else:
                keys = pmap.datasets_for(
                    pmap.producer_for_job(source, {"dataset": r["dataset"]} if r["dataset"] else {}))
            for key in keys:
                ev = facts.ev(key)
                ev.add(Event("ingestion_jobs", r["term_at"], r["term_status"],
                             _f(r["last_duration_s"]), r["term_error"] or None))
                ev.success(r["success_at"], _f(r["success_duration_s"]))

    if has_releases:
        rows = db.execute(text(_RELEASES_SQL),
                          {"superseded": SUPERSEDED_PREFIX + "%"}).mappings().all()
        for r in rows:
            for key in pmap.datasets_for(f"bulk:{r['source']}"):
                ev = facts.ev(key)
                ev.releases = {
                    "loaded": int(r["loaded"] or 0),
                    "unloaded": int(r["unloaded"] or 0),
                    "failed": int(r["failed"] or 0),
                    "superseded": int(r["superseded"] or 0),
                    "latest_release_key": r["latest_release_key"],
                    "last_bytes": int(r["last_bytes"]) if r["last_bytes"] is not None else None,
                    "last_publish_at": _naive_utc(r["last_publish_at"]),
                    "last_loaded_at": _naive_utc(r["last_loaded_at"]),
                }
                if r["last_loaded_at"]:
                    ev.add(Event("source_release", r["last_loaded_at"], "success"))
                    ev.success(r["last_loaded_at"])
                if r["last_failed_at"]:
                    ev.add(Event("source_release", r["last_failed_at"], "failed", None,
                                 r["last_failed_error"] or None))

    if has_marts:
        rows = db.execute(text(_MARTS_SQL), {"busy": BUSY_PREFIX + "%",
                                             "active_since": active_since}).mappings().all()
        for r in rows:
            job_type = MART_JOB_TYPES.get(r["mart"])
            if not job_type:
                continue
            for key in pmap.datasets_for(f"job:{job_type}"):
                ev = facts.ev(key)
                if r["last_at"]:
                    ev.add(Event("mart_build", r["last_at"], r["last_status"],
                                 _f(r["last_duration_s"]), r["last_reason"] or None))
                ev.success(r["success_at"], _f(r["success_duration_s"]))
                if r["last_status"] == "refused":
                    ev.mart_refusal = r["last_reason"] or "refused on its inputs"
                if r["running"]:
                    ev.mart_running = True

    if has_collectors:
        rows = db.execute(text(_COLLECTORS_SQL), {"active_since": active_since}).mappings().all()
        for r in rows:
            for key in pmap.datasets_for(f"collector:{r['source']}"):
                ev = facts.ev(key)
                ev.add(Event("site_intel_collection_job", r["last_at"], r["last_status"],
                             _f(r["last_duration_s"])))
                if r["term_at"]:
                    ev.add(Event("site_intel_collection_job", r["term_at"], r["term_status"],
                                 None, r["term_error"] or None))
                ev.success(r["success_at"], _f(r["success_duration_s"]))
                ev.active += int(r["active"] or 0)

    if has_schedules:
        facts.schedules = [dict(r) for r in db.execute(text(
            "SELECT id, name, source, config, frequency, cron_expression, is_active, "
            "next_run_at, created_at FROM ingestion_schedules ORDER BY id"
        )).mappings().all()]
        if facts.schedules and has_jobs:
            from app.services.data_watchdog import last_success_by_schedule

            if has_queue:
                facts.schedule_success = last_success_by_schedule(db)
            facts.schedule_runs_30d = {int(r[0]): int(r[1]) for r in db.execute(text(
                "SELECT schedule_id, COUNT(*) FROM ingestion_jobs "
                "WHERE schedule_id IS NOT NULL AND created_at >= :since GROUP BY schedule_id"
            ), {"since": now - MISSED_RUNS_WINDOW}).all()}

    if has_heartbeats:
        live, last = db.execute(text(
            "SELECT COUNT(*) FILTER (WHERE last_seen_at >= :cutoff), MAX(last_seen_at) "
            "FROM worker_heartbeats"
        ), {"cutoff": now - timedelta(minutes=WORKER_LIVE_MINUTES)}).one()
        facts.live_workers = int(live or 0)
        facts.last_heartbeat_at = _naive_utc(last)

    from app.catalog.tables import normalize

    for schema, name, tuples, nbytes in db.execute(text(_RELATIONS_SQL)).all():
        rows = int(tuples) if tuples is not None and tuples >= 0 else None
        facts.relations[normalize(schema, name)] = (rows, int(nbytes) if nbytes is not None else None)
    return facts


# =============================================================================
# Coverage (one combined statement, cached)
# =============================================================================

_coverage_cache: Dict[str, Tuple[float, Any]] = {}
_coverage_lock = threading.Lock()


def clear_cache() -> None:
    with _coverage_lock:
        _coverage_cache.clear()


def _coverage_key(spec: DatasetSpec) -> str:
    return f"{spec.key}\x00{spec.coverage_sql}"


def coverage_for(db: Session, specs: Sequence[DatasetSpec]) -> Dict[str, Any]:
    """spec.key -> coverage value (date/datetime/None), for specs whose tables
    exist. All cache misses in ONE statement under a statement timeout; if
    that statement fails (one bad coverage SQL), each is retried alone."""
    out: Dict[str, Any] = {}
    misses: List[DatasetSpec] = []
    now_mono = time.monotonic()
    with _coverage_lock:
        for spec in specs:
            hit = _coverage_cache.get(_coverage_key(spec))
            if hit and now_mono - hit[0] < COVERAGE_TTL_S:
                out[spec.key] = hit[1]
            else:
                misses.append(spec)
    if not misses:
        return out

    engine = db.get_bind()
    values: Dict[str, Any] = {}
    select = ", ".join(
        f"({s.coverage_sql.strip().rstrip(';')}) AS c{i}" for i, s in enumerate(misses)
    )
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"SET LOCAL statement_timeout = {int(COVERAGE_TIMEOUT_MS)}"))
                row = conn.execute(text(f"SELECT {select}")).one()
        values = {s.key: row[i] for i, s in enumerate(misses)}
    except Exception as e:
        logger.info(f"[dataset_status] combined coverage failed ({type(e).__name__}); per dataset")
        from app.catalog.live import coverage_through

        started = time.monotonic()
        for s in misses:
            if time.monotonic() - started > COVERAGE_FALLBACK_DEADLINE_S:
                values[s.key] = None  # unavailable this time; retried after the TTL
                continue
            res = coverage_through(engine, s, timeout_ms=COVERAGE_FALLBACK_TIMEOUT_MS)
            values[s.key] = res["coverage_through"]
    with _coverage_lock:
        for s in misses:
            _coverage_cache[_coverage_key(s)] = (time.monotonic(), values.get(s.key))
    out.update(values)
    return out


# =============================================================================
# Schedules
# =============================================================================


def _cron_fires(expr: str, start: datetime, end: datetime, cap: int = 5000) -> Optional[int]:
    try:
        from apscheduler.triggers.cron import CronTrigger

        trigger = CronTrigger.from_crontab(expr, timezone="UTC")
    except Exception:
        return None
    n, prev = 0, None
    cursor = start.replace(tzinfo=timezone.utc)
    end_utc = end.replace(tzinfo=timezone.utc)
    while n < cap:
        nxt = trigger.get_next_fire_time(prev, cursor)
        if nxt is None or nxt > end_utc:
            break
        n += 1
        prev, cursor = nxt, nxt + timedelta(seconds=1)
    return n


def _live_jobs(scheduler) -> Dict[str, datetime]:
    """APScheduler job id -> next run, when the in-process scheduler runs."""
    if scheduler is None or not getattr(scheduler, "running", False):
        return {}
    try:
        return {j.id: _naive_utc(j.next_run_time) for j in scheduler.get_jobs() if j.next_run_time}
    except Exception as e:
        logger.info(f"[dataset_status] could not read APScheduler jobs: {type(e).__name__}")
        return {}


def _default_scheduler():
    try:
        from app.core import scheduler_service

        return scheduler_service._scheduler  # never create one here
    except Exception:
        return None


def _schedule_block(row: Dict[str, Any], facts: Facts, live: Dict[str, datetime]) -> Dict[str, Any]:
    from app.services.data_watchdog import _Row, schedule_cadence_hours

    now = facts.now
    cadence = schedule_cadence_hours(_Row(row))
    freq = row.get("frequency")
    freq = str(getattr(freq, "value", freq) or "").lower()
    cron = row.get("cron_expression") if freq == "custom" else None
    live_next = live.get(f"schedule_{row['id']}")
    if live_next is not None:
        next_at, next_src = live_next, "apscheduler"
    elif row.get("next_run_at"):
        next_at, next_src = row["next_run_at"], "db"
    else:
        next_at, next_src = None, None

    missed = None
    start = max(now - MISSED_RUNS_WINDOW, row.get("created_at") or now - MISSED_RUNS_WINDOW)
    if row.get("is_active"):
        if cron:
            fires = _cron_fires(cron, start, now)
        elif cadence:
            fires = int(((now - start).total_seconds() / 3600) // cadence)
        else:
            fires = None
        if fires is not None:
            missed = max(0, fires - facts.schedule_runs_30d.get(row["id"], 0))

    last = facts.schedule_success.get(row["id"])
    return {
        "kind": "schedule",
        "schedule_id": row["id"],
        "name": row.get("name"),
        "cron": cron or freq or None,
        "active": bool(row.get("is_active")),
        "cadence_hours": cadence,
        "next_run_at": _iso(next_at),
        "next_run_source": next_src,
        "last_success_at": _iso(last),
        "missed_runs_30d": missed,
    }


def _stalled(block: Dict[str, Any], row: Dict[str, Any], now: datetime) -> Optional[str]:
    cadence = block.get("cadence_hours")
    if not block["active"] or not cadence:
        return None
    last = row.get("_last_success")
    since = last or row.get("created_at")
    if since is None:
        return None
    age_h = (now - since).total_seconds() / 3600
    if age_h <= cadence * STALL_FACTOR:
        return None
    if last is None:
        return (f"schedule '{row.get('name')}' has never succeeded "
                f"(active {age_h / 24:.1f} days, expected every {cadence:.0f}h)")
    return f"schedule '{row.get('name')}' last succeeded {age_h / 24:.1f} days ago (expected every {cadence:.0f}h)"


# =============================================================================
# Derivation
# =============================================================================


@dataclass
class Context:
    now: datetime
    facts: Facts
    pmap: ProducerMap
    key_checker: Callable[[str], Optional[str]]
    worker_mode: bool
    batch_keys: frozenset
    live_jobs: Dict[str, datetime]
    batch_defaults: Dict[str, Dict[str, Any]]
    schedules_by_dataset: Dict[str, List[Dict[str, Any]]]
    _key_memo: Dict[str, Optional[str]] = field(default_factory=dict)

    def missing_key(self, source: str) -> Optional[str]:
        if source not in self._key_memo:
            try:
                self._key_memo[source] = self.key_checker(source)
            except Exception:
                self._key_memo[source] = None
        return self._key_memo[source]


def _producer_value(spec: DatasetSpec) -> str:
    return base_producer(spec.producer).split(":", 1)[1]


def _needs_worker(spec: DatasetSpec, worker_mode: bool) -> bool:
    kind = spec.producer_kind
    if kind in ("bulk", "job", "collector"):
        return True
    return kind == "dispatch" and worker_mode


def run_path(spec: DatasetSpec) -> Optional[str]:
    kind = spec.producer_kind
    if kind == "bulk":
        return "bulk_ingest"
    if kind == "job":
        job_type = _producer_value(spec)
        return job_type if job_type in RUNNABLE_JOB_TYPES else None
    if kind == "dispatch":
        return "ingestion"
    if kind == "collector":
        return "site_intel"
    return None


def _key_blocker(spec: DatasetSpec, ctx: Context) -> Optional[str]:
    kind = spec.producer_kind
    if kind in ("dispatch", "collector"):
        return ctx.missing_key(_producer_value(spec))
    if kind == "api":
        return ctx.missing_key(spec.source)
    return None


def _batch_scheduled(spec: DatasetSpec, batch_keys: frozenset) -> bool:
    keys = {base_producer(p)[len("dispatch:"):] for p in spec.producers if p.startswith("dispatch:")}
    return bool(keys & batch_keys)


def _human_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{n} B"


def can_run_verdict(spec: DatasetSpec, ev: Evidence, ctx: Context) -> Dict[str, Any]:
    blockers: List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []
    path = run_path(spec)
    kind = spec.producer_kind
    if path is None:
        where = {"api": f"its own /{_producer_value(spec)} router",
                 "job": f"the {_producer_value(spec)} collection endpoints"}.get(kind, "its own endpoint")
        blockers.append({"code": "no_run_path",
                         "message": f"{spec.producer} is started from {where}, not from here"})
    key_error = _key_blocker(spec, ctx)
    if key_error:
        blockers.append({"code": "missing_api_key", "message": key_error})
    if path is not None and _needs_worker(spec, ctx.worker_mode):
        if not ctx.worker_mode:
            blockers.append({"code": "worker_mode_off",
                             "message": f"{path} jobs run only on the worker queue (WORKER_MODE=0)"})
        elif ctx.facts.live_workers == 0:
            blockers.append({"code": "no_live_worker",
                             "message": "no worker heartbeat in the last "
                                        f"{WORKER_LIVE_MINUTES} min; the job would wait in the queue"})
    if ev.active or ev.mart_running:
        blockers.append({"code": "already_running",
                         "message": "a run of this producer is already queued or running"})

    rel = ev.releases or {}
    if rel.get("last_bytes"):
        warnings.append({"code": "download",
                         "message": f"the last release download was {_human_bytes(rel['last_bytes'])}; "
                                    "releases already loaded are skipped"})
    if spec.rerun in RERUN_WARNINGS:
        warnings.append({"code": spec.rerun, "message": RERUN_WARNINGS[spec.rerun]})
    if ev.mart_refusal:
        warnings.append({"code": "mart_refused",
                         "message": f"the last build was refused ({ev.mart_refusal}); "
                                    "it will be refused again unless its inputs changed"})
    if kind == "job":
        siblings = ctx.pmap.datasets_for(spec.producer)
        if len(siblings) > 1:
            warnings.append({"code": "shared_run",
                             "message": f"rebuilds all {len(siblings)} {_producer_value(spec)} datasets: "
                                        + ", ".join(siblings)})
    if kind == "dispatch" and _producer_value(spec) not in ctx.batch_defaults:
        warnings.append({"code": "default_config",
                         "message": "no nightly-batch config for this key; the ingestor's defaults apply"})

    return {
        "allowed": not blockers,
        "requires": "admin",
        "policy": spec.rerun,
        "producer": spec.producer,
        "run_path": path,
        "blockers": blockers,
        "warnings": warnings,
        "estimated_duration_s": ev.success_duration_s,
        "estimated_bytes": rel.get("last_bytes"),
    }


def _tables(spec: DatasetSpec, ctx: Context, claimed) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Declared + pattern tables with estimated rows, or the SPEC_123 live
    cache's exact counts when it holds them. Returns (tables, cached coverage)."""
    from app.catalog.live import CACHE_TTL_S, _cached, resolve_tables

    hit = _cached(spec.key, CACHE_TTL_S)
    rels = ctx.facts.relations
    if hit is not None:
        tables = [{"name": t["table"], "exists": t["exists"], "rows": t["rows"],
                   "rows_exact": t["rows_exact"], "bytes": (rels.get(t["table"]) or (None, None))[1]}
                  for t in hit["tables"]]
        return tables, hit.get("coverage_through") if not hit.get("coverage_error") else None
    tables = []
    for name in resolve_tables(spec, set(rels), claimed):
        stat = rels.get(name)
        tables.append({"name": name, "exists": stat is not None,
                       "rows": stat[0] if stat else None, "rows_exact": False,
                       "bytes": stat[1] if stat else None})
    return tables, None


def derive(spec: DatasetSpec, ev: Evidence, ctx: Context, coverage: Any,
           has_rows: bool) -> Tuple[str, str, List[Dict[str, str]], Optional[Dict[str, Any]], Dict[str, Any]]:
    """(status, reason, blockers, schedule block, clocks) for one dataset."""
    now = ctx.now
    cadence_h = CADENCE_HOURS.get(spec.cadence)

    # --- schedules --------------------------------------------------------
    rows = ctx.schedules_by_dataset.get(spec.key, [])
    blocks = []
    stall_reason = None
    for row in rows:
        row["_last_success"] = ctx.facts.schedule_success.get(row["id"])
        block = _schedule_block(row, ctx.facts, ctx.live_jobs)
        blocks.append(block)
        if stall_reason is None:
            reason = _stalled(block, row, now)
            if reason:
                stall_reason = reason
                block["stalled"] = True
    active = [b for b in blocks if b["active"]]
    batch = _batch_scheduled(spec, ctx.batch_keys)
    schedule = None
    if active:
        schedule = next((b for b in active if b.get("stalled")), active[0])
    elif batch:
        live_next = ctx.live_jobs.get(BATCH_JOB_ID)
        schedule = {"kind": "batch", "schedule_id": None, "name": "Nightly batch collection",
                    "cron": BATCH_CRON, "active": True, "cadence_hours": cadence_h,
                    "next_run_at": _iso(live_next),
                    "next_run_source": "apscheduler" if live_next else None,
                    "last_success_at": _iso(ev.last_success_at), "missed_runs_30d": None}
        if cadence_h and stall_reason is None:
            since = ev.last_success_at
            if since is None and ev.last_run is not None:
                stall_reason = "the nightly batch runs it but it has never succeeded"
            elif since is not None and (now - since).total_seconds() / 3600 > cadence_h * STALL_FACTOR:
                stall_reason = (f"nightly batch: last success {(now - since).days} days ago "
                                f"(expected {spec.cadence})")
    elif blocks:
        schedule = blocks[-1]
    if schedule is not None:
        schedule.pop("stalled", None)
    scheduled = bool(active) or batch

    # --- clocks -----------------------------------------------------------
    rel = ev.releases
    expected = expected_through(spec.cadence, spec.slo_lag_hours, now)
    cov_date = _as_date(coverage)
    basis = None
    if expected is not None:
        basis = "coverage" if spec.coverage_sql else "last_success"
    lag_days = None
    if expected is not None and cov_date is not None:
        lag_days = max(0, (expected - cov_date).days)
    last = ev.last_run
    clocks = {
        "last_run_at": _iso(last.at) if last else None,
        "last_run_status": last.status if last else None,
        "last_run_store": last.store if last else None,
        "last_run_duration_s": last.duration_s if last else None,
        "last_success_at": _iso(ev.last_success_at),
        "last_publish_at": _iso(rel["last_publish_at"]) if rel else None,
        "coverage_through": cov_date.isoformat() if cov_date else None,
        "expected_through": expected.isoformat() if expected else None,
        "expectation_basis": basis,
        "lag_days": lag_days,
    }

    # --- blockers ---------------------------------------------------------
    blockers: List[Dict[str, str]] = []
    key_error = _key_blocker(spec, ctx)
    if key_error:
        blockers.append({"code": "missing_api_key", "message": key_error})
    if scheduled and _needs_worker(spec, ctx.worker_mode) and ctx.facts.live_workers == 0:
        blockers.append({"code": "no_live_worker",
                         "message": f"scheduled, but no worker heartbeat in the last {WORKER_LIVE_MINUTES} min"})
    if ev.mart_refusal:
        blockers.append({"code": "mart_input_refused",
                         "message": f"latest build refused: {ev.mart_refusal}"})

    # --- status (first match wins) ----------------------------------------
    term = ev.last_terminal
    recent = term is not None and term.at >= now - FAILING_WINDOW
    if blockers:
        return "blocked", blockers[0]["message"], blockers, schedule, clocks
    if recent and term.status in ("failed", "refused"):
        why = f": {term.error}" if term.error else ""
        return "failing", f"latest run failed {_iso(term.at)} ({term.store}){why}", blockers, schedule, clocks
    if recent and term.status == "partial":
        return "partial", f"latest run succeeded only in part {_iso(term.at)}", blockers, schedule, clocks
    if last is None and ev.last_success_at is None and not has_rows:
        return "never_run", "no run recorded in any store", blockers, schedule, clocks
    if stall_reason:
        return "stalled", stall_reason, blockers, schedule, clocks
    if not scheduled:
        why = "no active schedule" if last is not None else "no active schedule; no run recorded"
        return "dormant", why, blockers, schedule, clocks
    if rel and rel["unloaded"]:
        return ("behind", f"{rel['unloaded']} published release(s) not loaded "
                f"(latest {rel['latest_release_key']})", blockers, schedule, clocks)
    if expected is None:
        why = ("no SLO: nothing to judge coverage against" if spec.slo_lag_hours is None
               else f"no expectation for a {spec.cadence} cadence")
        return "unknown", why, blockers, schedule, clocks
    if basis == "last_success":
        if ev.last_success_at and cadence_h and \
                ev.last_success_at >= now - timedelta(hours=cadence_h + spec.slo_lag_hours):
            return "current", f"last success {_iso(ev.last_success_at)} within cadence + SLO", \
                blockers, schedule, clocks
        return "behind", "no success within cadence + SLO", blockers, schedule, clocks
    if cov_date is None:
        return "unknown", "coverage unavailable", blockers, schedule, clocks
    if cov_date >= expected:
        return "current", f"coverage {cov_date} meets expected {expected}", blockers, schedule, clocks
    publish = rel["last_publish_at"] if rel else None
    if rel is not None and publish is not None and cadence_h and \
            publish + timedelta(hours=cadence_h * STALL_FACTOR) > now:
        return ("awaiting_upstream", f"every published release is loaded; next publish not due "
                f"(last {_iso(publish)})", blockers, schedule, clocks)
    return "behind", f"coverage {cov_date} is {lag_days} days behind {expected}", blockers, schedule, clocks


# =============================================================================
# Entry points
# =============================================================================


def _batch_defaults(pmap: ProducerMap) -> Dict[str, Dict[str, Any]]:
    """dispatch key -> the nightly batch's default config for it."""
    try:
        from app.core.batch_service import TIERS
    except Exception:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for tier in TIERS:
        for source_def in tier.sources:
            producer = pmap.producer_for_job(source_def.key, source_def.default_config)
            if producer:
                out.setdefault(producer[len("dispatch:"):], dict(source_def.default_config))
    return out


def _schedules_by_dataset(facts: Facts, pmap: ProducerMap) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in facts.schedules:
        producer = pmap.producer_for_job(row.get("source"), row.get("config") or {})
        for key in pmap.datasets_for(producer):
            out.setdefault(key, []).append(dict(row))
    return out


def worker_mode_enabled() -> bool:
    import app.core.job_queue_service as jqs

    return bool(jqs.WORKER_MODE)


def build_status(
    db: Session,
    specs: Optional[Iterable[DatasetSpec]] = None,
    now: Optional[datetime] = None,
    key_checker: Optional[Callable[[str], Optional[str]]] = None,
    worker_mode: Optional[bool] = None,
    scheduler: Any = "default",
    batch_keys: Optional[frozenset] = None,
    kind: Optional[str] = None,
    source: Optional[str] = None,
    status_public: Optional[str] = None,
    status: Optional[str] = None,
    keys: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """The whole status payload. ``summary`` counts the datasets left after
    the kind/source/status_public filters (before the status filter)."""
    all_specs = tuple(specs) if specs is not None else tuple(catalog_specs())
    now = now or datetime.utcnow()
    pmap = ProducerMap(all_specs)
    selected = [s for s in all_specs
                if (kind is None or s.kind == kind)
                and (source is None or s.source == source)
                and (status_public is None or s.status_public == status_public)
                and (keys is None or s.key in set(keys))]

    facts = collect_facts(db, pmap, now)
    ctx = Context(
        now=now,
        facts=facts,
        pmap=pmap,
        key_checker=key_checker or (lambda s: api_key_preflight(s)),
        worker_mode=worker_mode_enabled() if worker_mode is None else worker_mode,
        batch_keys=frozenset(BATCH_SCHEDULED_DISPATCH if batch_keys is None else batch_keys),
        live_jobs=_live_jobs(_default_scheduler() if scheduler == "default" else scheduler),
        batch_defaults=_batch_defaults(pmap),
        schedules_by_dataset=_schedules_by_dataset(facts, pmap),
    )

    from app.catalog.live import claimed_tables

    claimed = claimed_tables(tuple(catalog_specs()) + all_specs)
    prepared = []
    need_coverage = []
    for spec in selected:
        tables, cached_cov = _tables(spec, ctx, claimed)
        prepared.append((spec, tables, cached_cov))
        if spec.coverage_sql and cached_cov is None and spec.tables and \
                all(t in facts.relations for t in spec.tables):
            need_coverage.append(spec)
    coverage = coverage_for(db, need_coverage) if need_coverage else {}

    datasets = []
    summary = {s: 0 for s in STATUSES}
    for spec, tables, cached_cov in prepared:
        ev = facts.evidence.get(spec.key) or Evidence()
        existing = [t for t in tables if t["exists"]]
        rows_total = sum(t["rows"] or 0 for t in existing) if existing else None
        cov = cached_cov if cached_cov is not None else coverage.get(spec.key)
        st, reason, blockers, schedule, clocks = derive(
            spec, ev, ctx, cov, has_rows=bool(rows_total))
        summary[st] += 1
        rel = ev.releases
        datasets.append({
            "key": spec.key,
            "display_name": spec.display_name,
            "source": spec.source,
            "kind": spec.kind,
            "grain": spec.grain,
            "cadence": spec.cadence,
            "status_public": spec.status_public,
            "producer": spec.producer,
            "status": st,
            "status_reason": reason,
            "tables": tables,
            "rows_total": rows_total,
            "rows_exact": bool(existing) and all(t["rows_exact"] for t in existing),
            "clocks": clocks,
            "releases": {k: v for k, v in rel.items()
                         if k not in ("last_publish_at", "last_loaded_at")} if rel else None,
            "schedule": schedule,
            "blockers": blockers,
            "can_run": can_run_verdict(spec, ev, ctx),
        })

    if status is not None:
        datasets = [d for d in datasets if d["status"] == status]
    return {
        "generated_at": _iso(now),
        "summary": summary,
        "count": len(datasets),
        "total": len(all_specs),
        "worker": {
            "worker_mode": ctx.worker_mode,
            "live_workers": facts.live_workers,
            "last_heartbeat_at": _iso(facts.last_heartbeat_at),
        },
        "datasets": datasets,
    }


# =============================================================================
# Run
# =============================================================================


def dispatch_target(key: str, pmap: ProducerMap, batch_defaults: Dict[str, Dict[str, Any]]
                    ) -> Tuple[str, Dict[str, Any]]:
    """(IngestionJob.source, config) that ``_run_dispatched_job`` resolves back to ``key``."""
    base, _, sub = key.partition(":")
    config = dict(batch_defaults.get(key, {}))
    if sub:
        config["dataset"] = sub
    return base, config


def enqueue_run(db: Session, spec: DatasetSpec, actor: Optional[str], background_tasks=None,
                specs: Optional[Iterable[DatasetSpec]] = None) -> Dict[str, Any]:
    """Enqueue the dataset's producer through its existing path. The caller has
    already checked the verdict. Returns job ids and the audit row id."""
    from app.core import audit_service
    from app.core.job_queue_service import submit_job
    from app.core.models import IngestionJob, JobStatus

    pmap = ProducerMap(tuple(specs) if specs is not None else catalog_specs())
    kind = spec.producer_kind
    producer = base_producer(spec.producer)
    value = producer.split(":", 1)[1]
    job = None

    def _ingestion_job(source: str, config: Dict[str, Any]) -> IngestionJob:
        row = IngestionJob(source=source, status=JobStatus.PENDING, config=config,
                           trigger="manual", dataset_key=spec.key)
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    if kind == "bulk":
        job = _ingestion_job(producer, {})
        result = submit_job(db=db, job_type="bulk_ingest",
                            payload={"bulk_source": value, "ingestion_job_id": job.id},
                            job_table_id=job.id)
    elif kind == "job" and value in RUNNABLE_JOB_TYPES:
        job = _ingestion_job(producer, {})
        result = submit_job(db=db, job_type=value, payload={"ingestion_job_id": job.id},
                            job_table_id=job.id)
    elif kind == "dispatch":
        from app.api.v1 import jobs as jobs_api

        source, config = dispatch_target(value, pmap, _batch_defaults(pmap))
        job = _ingestion_job(source, config)
        result = submit_job(
            db=db, job_type="ingestion",
            payload={"source": source, "config": config, "ingestion_job_id": job.id,
                     "trigger": "manual"},
            priority=5, job_table_id=job.id,
            background_tasks=background_tasks,
            background_func=jobs_api.run_ingestion_job,
            background_args=(job.id, source, config),
        )
    elif kind == "collector":
        result = submit_job(db=db, job_type="site_intel", payload={"sources": [value]})
    else:
        raise ValueError(f"{spec.producer} has no run path")

    audit_id = None
    try:
        entry = audit_service.log_collection(
            db,
            trigger_type="api",
            source=(job.source if job is not None else producer)[:50],
            job_id=job.id if job is not None else result.get("job_queue_id"),
            job_type=run_path(spec),
            trigger_source=f"/datasets/{spec.key}/run",
            config_snapshot={"dataset_key": spec.key, "producer": spec.producer,
                             "ingestion_job_id": job.id if job is not None else None,
                             "job_queue_id": result.get("job_queue_id")},
            actor=actor,
        )
        audit_id = entry.id
    except Exception as e:
        db.rollback()
        logger.warning(f"[dataset_status] audit row for {spec.key} run failed: {type(e).__name__}")

    return {
        "dataset_key": spec.key,
        "producer": spec.producer,
        "run_path": run_path(spec),
        "mode": result.get("mode"),
        "ingestion_job_id": job.id if job is not None else None,
        "job_queue_id": result.get("job_queue_id"),
        "audit_id": audit_id,
    }
