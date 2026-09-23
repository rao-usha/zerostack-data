"""
Tests for SPEC 124 — one honest dataset status API.

GET /api/v1/datasets/status answers, per catalog dataset and in one batched
call, when we last collected (run clock), when upstream last published
(publish clock), what period the data covers against what it should cover
(coverage clock), and turns that into one status from a closed vocabulary.
POST /api/v1/datasets/{key}/run is admin-only and refuses with the same
verdict it would show on the page.
"""
import importlib.util
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 23, 12, 0, 0)


def _migration(name):
    path = REPO / "alembic" / "versions" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"mig124_{name}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _spec(key, producer, cadence="monthly", slo=None, coverage=None, tables=("t124_cov",), **kw):
    from app.catalog.spec import DatasetSpec

    return DatasetSpec(
        key=key,
        source=kw.pop("source", "t124"),
        display_name=key.replace("_", " "),
        description="A test dataset for SPEC 124 status derivation.",
        kind=kw.pop("kind", "filings"),
        grain="one row per thing",
        producer=producer,
        cadence=cadence,
        rerun=kw.pop("rerun", "idempotent"),
        license="test",
        redistribution="internal_only",
        pii_class="none",
        origin="official",
        status_public=kw.pop("status_public", "internal"),
        tables=tuple(tables),
        coverage_sql=coverage,
        slo_lag_hours=slo,
        **kw,
    )


def _no_key_missing(source):
    return None


# ---------------------------------------------------------------------------
# T1 expected_through
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExpectedThrough:
    def _et(self, cadence, slo, now=NOW):
        from app.services.dataset_status import expected_through

        return expected_through(cadence, slo, now)

    def test_daily(self):
        # now - 36h = 09-22 00:00 -> the last whole day before it is 09-21
        assert self._et("daily", 36) == date(2026, 9, 21)

    def test_weekly_ends_sunday(self):
        # now - 8d = Tue 09-15 -> last Sunday before it is 09-13
        assert self._et("weekly", 24 * 8) == date(2026, 9, 13)

    def test_monthly(self):
        assert self._et("monthly", 24 * 10) == date(2026, 8, 31)
        # a period end is strictly before the cut-off day
        assert self._et("monthly", 12, now=datetime(2026, 9, 1, 12)) == date(2026, 8, 31)

    def test_quarterly(self):
        assert self._et("quarterly", 24 * 35) == date(2026, 6, 30)

    def test_annual(self):
        assert self._et("annual", 24 * 90) == date(2025, 12, 31)

    def test_no_expectation(self):
        assert self._et("monthly", None) is None
        assert self._et("ad_hoc", 24) is None


# ---------------------------------------------------------------------------
# T2 job -> producer -> dataset resolution
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestJobKeys:
    def test_ingestion_job_producers(self):
        from app.catalog.job_keys import producer_for_job

        assert producer_for_job("treasury", {"dataset": "debt_outstanding"}) == \
            "dispatch:treasury:debt_outstanding"
        assert producer_for_job("treasury", {}) == "dispatch:treasury"
        assert producer_for_job("treasury", None) == "dispatch:treasury"
        assert producer_for_job("sec:formadv", {}) == "dispatch:sec:formadv"
        assert producer_for_job("eia", {"dataset": "electricity"}) == "dispatch:eia:electricity"
        # a dataset value that is not a dispatch key falls back to the base key
        assert producer_for_job("eia", {"dataset": "petroleum_weekly"}) == "dispatch:eia"
        assert producer_for_job("fred:split_0", {}) == "dispatch:fred"
        assert producer_for_job("census", {"survey": "acs5"}) == "dispatch:census"
        assert producer_for_job("bulk:sec_13f", {}) == "bulk:sec_13f"
        assert producer_for_job("job:pe_mart_build", {}) == "job:pe_mart_build"
        assert producer_for_job("no_such_source", {}) is None

    def test_job_type_covers_every_stage(self):
        from app.catalog.job_keys import default_map

        assert set(default_map().datasets_for("job:pe_mart_build")) == {
            "sec_adv_private_funds", "pe_firms_sec", "pe_funds_sec", "pe_people_sec"}
        assert set(default_map().datasets_for("job:entity_resolve")) == {
            "entity_source_records", "entity_cik_crd_bridge", "entity_master"}
        assert default_map().datasets_for("bulk:sec_13f") == ("sec_13f",)

    def test_dataset_key_only_when_unambiguous(self):
        from app.catalog.job_keys import dataset_key_for_job

        assert dataset_key_for_job("bulk:sec_13f", {}) == "sec_13f"
        assert dataset_key_for_job("treasury", {"dataset": "debt_outstanding"}) == \
            "treasury_debt_outstanding"
        assert dataset_key_for_job("job:pe_mart_build", {}) is None
        assert dataset_key_for_job("nope", {}) is None

    def test_queue_producers(self):
        from app.catalog.job_keys import producers_for_queue

        assert producers_for_queue("bulk_ingest", {"bulk_source": "sec_13f"}) == ["bulk:sec_13f"]
        assert producers_for_queue(
            "ingestion", {"source": "treasury", "config": {"dataset": "auctions"}}
        ) == ["dispatch:treasury:auctions"]
        assert producers_for_queue("site_intel", {"sources": ["eia", "fcc"]}) == \
            ["collector:eia", "collector:fcc"]
        assert producers_for_queue("site_intel", {"domains": ["power"]}) == []
        assert producers_for_queue("pe_mart_build", {}) == ["job:pe_mart_build"]

    def test_mart_and_release_producers(self):
        from app.catalog.job_keys import producer_for_mart, producer_for_release

        assert producer_for_mart("pe_marts") == "job:pe_mart_build"
        assert producer_for_mart("entity_resolve") == "job:entity_resolve"
        assert producer_for_release("sec_13f") == "bulk:sec_13f"

    def test_preflight_exposed_and_jobs_alias_kept(self):
        from app.api.v1.jobs import _check_api_key_preflight
        from app.core.preflight import api_key_preflight

        assert api_key_preflight("treasury") is None
        assert _check_api_key_preflight("treasury") is None


# ---------------------------------------------------------------------------
# PG fixture + builders
# ---------------------------------------------------------------------------


@pytest.fixture
def pgdb(monkeypatch):
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session

    from app.core.models import (
        Base, CollectionAuditLog, IngestionJob, IngestionSchedule,
    )
    from app.core.models_queue import JobQueue, WorkerHeartbeat
    from app.core.models_site_intel import SiteIntelCollectionJob
    import app.services.dataset_status as ds

    engine = create_engine(PG_URL)
    tables = [
        IngestionSchedule.__table__, IngestionJob.__table__, JobQueue.__table__,
        WorkerHeartbeat.__table__, CollectionAuditLog.__table__,
        SiteIntelCollectionJob.__table__,
    ]
    with engine.begin() as conn:
        for t in reversed(tables):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t.name} CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS raw CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS core.mart_build"))
        for stmt in _migration("0004_bulk_framework").SOURCE_RELEASE_DDL:
            conn.execute(text(stmt))
        for stmt in _migration("0013_mart_build").UPGRADE_SQL:
            conn.execute(text(stmt))
        for t in ("t124_cov", "t124_old", "t124_q"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t}"))
            conn.execute(text(f"CREATE TABLE public.{t} (d DATE)"))
        conn.execute(text("INSERT INTO t124_cov VALUES ('2026-08-31')"))
        conn.execute(text("INSERT INTO t124_old VALUES ('2026-05-31')"))
        conn.execute(text("INSERT INTO t124_q VALUES ('2026-03-31')"))
    Base.metadata.create_all(engine, tables=tables)
    ds.clear_cache()

    with Session(engine) as db:
        yield db
    ds.clear_cache()
    engine.dispose()


def _schedule(db, source, cron=None, frequency=None, active=1, created_at=None, **kw):
    from app.core.models import IngestionSchedule, ScheduleFrequency

    row = IngestionSchedule(
        name=kw.pop("name", f"sched {source} {datetime.utcnow().timestamp()}"),
        source=source,
        config=kw.pop("config", {}),
        frequency=frequency or ScheduleFrequency.CUSTOM,
        cron_expression=cron,
        is_active=active,
        created_at=created_at or NOW - timedelta(days=400),
        **kw,
    )
    db.add(row)
    db.commit()
    return row


def _job(db, source, status, completed_at, config=None, schedule_id=None, error=None,
         started_at=None, batch_run_id=None):
    from app.core.models import IngestionJob

    row = IngestionJob(source=source, status=status, config=config or {},
                       schedule_id=schedule_id, error_message=error, batch_run_id=batch_run_id,
                       created_at=(started_at or completed_at) - timedelta(minutes=1),
                       started_at=started_at or completed_at - timedelta(minutes=2),
                       completed_at=completed_at)
    db.add(row)
    db.commit()
    return row


def _queue(db, job_type, status, at, payload=None, error=None, job_table_id=None,
           duration_s=60):
    from app.core.models_queue import JobQueue

    row = JobQueue(job_type=job_type, status=status, payload=payload or {},
                   created_at=at - timedelta(seconds=duration_s + 5),
                   started_at=at - timedelta(seconds=duration_s),
                   completed_at=at if status in ("success", "failed") else None,
                   error_message=error, job_table_id=job_table_id)
    db.add(row)
    db.commit()
    return row


def _release(db, source, key, status, discovered_at, loaded_at=None, error=None, nbytes=None):
    from sqlalchemy import text

    db.execute(text(
        "INSERT INTO raw.source_release (source, release_key, url, status, error, bytes, "
        "discovered_at, loaded_at, updated_at) VALUES (:s, :k, 'http://x', :st, :e, :b, "
        ":d, :l, :u)"
    ), {"s": source, "k": key, "st": status, "e": error, "b": nbytes, "d": discovered_at,
        "l": loaded_at, "u": loaded_at or discovered_at})
    db.commit()


def _mart_build(db, mart, status, finished_at, reason=None, dry_run=False):
    from sqlalchemy import text

    db.execute(text(
        "INSERT INTO core.mart_build (mart, status, dry_run, started_at, finished_at, "
        "refusal_reason) VALUES (:m, :s, :dr, :st, :f, :r)"
    ), {"m": mart, "s": status, "dr": dry_run, "st": finished_at - timedelta(minutes=5),
        "f": finished_at, "r": reason})
    db.commit()


def _heartbeat(db, at=NOW):
    from sqlalchemy import text

    db.execute(text(
        "INSERT INTO worker_heartbeats (worker_id, hostname, started_at, last_seen_at) "
        "VALUES ('w1', 'h', :t, :t)"
    ), {"t": at})
    db.commit()


def _build(db, specs, **kw):
    from app.services.dataset_status import build_status

    kw.setdefault("now", NOW)
    kw.setdefault("key_checker", _no_key_missing)
    kw.setdefault("worker_mode", False)
    kw.setdefault("scheduler", None)
    kw.setdefault("batch_keys", frozenset())
    report = build_status(db, specs=specs, **kw)
    return report, {d["key"]: d for d in report["datasets"]}


# ---------------------------------------------------------------------------
# T3 never_run / dormant
# ---------------------------------------------------------------------------


@pg
def test_never_run_and_dormant(pgdb):
    """T3"""
    specs = [_spec("t124_never", "dispatch:t124a"), _spec("t124_dorm", "dispatch:t124b")]
    _job(pgdb, "t124b", "success", NOW - timedelta(days=10))

    report, by = _build(pgdb, specs)
    assert by["t124_never"]["status"] == "never_run"
    assert by["t124_never"]["clocks"]["last_run_at"] is None
    assert by["t124_dorm"]["status"] == "dormant"
    assert by["t124_dorm"]["clocks"]["last_success_at"].startswith("2026-09-13")
    assert by["t124_dorm"]["clocks"]["last_run_store"] == "ingestion_jobs"
    assert report["summary"]["never_run"] == 1 and report["summary"]["dormant"] == 1
    # every status is present in the summary, zero-filled
    from app.services.dataset_status import STATUSES

    assert set(report["summary"]) == set(STATUSES)


# ---------------------------------------------------------------------------
# T4 blocked
# ---------------------------------------------------------------------------


@pg
def test_blocked_missing_api_key(pgdb):
    """T4a: a required key that is absent explains a dead source."""
    specs = [_spec("t124_key", "dispatch:t124key")]
    _job(pgdb, "t124key", "failed", NOW - timedelta(days=2), error="API key required")

    def checker(source):
        return "API key required for 't124key' but not configured" if source.startswith("t124key") else None

    _, by = _build(pgdb, specs, key_checker=checker)
    d = by["t124_key"]
    assert d["status"] == "blocked"
    assert [b["code"] for b in d["blockers"]] == ["missing_api_key"]
    assert d["can_run"]["allowed"] is False
    assert "missing_api_key" in [b["code"] for b in d["can_run"]["blockers"]]


@pg
def test_blocked_no_live_worker_only_for_scheduled_worker_datasets(pgdb):
    """T4b"""
    specs = [_spec("t124_w", "bulk:t124w"), _spec("t124_w_unsched", "bulk:t124wu")]
    sched = _schedule(pgdb, "bulk:t124w", cron="0 8 4 * *")
    _job(pgdb, "bulk:t124w", "success", NOW - timedelta(days=3), schedule_id=sched.id)
    _job(pgdb, "bulk:t124wu", "success", NOW - timedelta(days=3))

    _, by = _build(pgdb, specs)
    assert by["t124_w"]["status"] == "blocked"
    assert [b["code"] for b in by["t124_w"]["blockers"]] == ["no_live_worker"]
    # not scheduled: the dead worker is not what is wrong with it
    assert by["t124_w_unsched"]["status"] == "dormant"
    # bulk loads need the queue: in-process mode cannot run them at all
    assert [b["code"] for b in by["t124_w_unsched"]["can_run"]["blockers"]] == ["worker_mode_off"]
    _, by = _build(pgdb, specs, worker_mode=True)
    assert [b["code"] for b in by["t124_w_unsched"]["can_run"]["blockers"]] == ["no_live_worker"]

    _heartbeat(pgdb)
    _, by = _build(pgdb, specs)
    assert by["t124_w"]["status"] != "blocked"
    assert by["t124_w"]["blockers"] == []


@pg
def test_blocked_refused_mart_input_ignores_busy(pgdb):
    """T4c: a refused build blocks; a later busy: refusal and dry runs are ignored."""
    specs = [_spec("t124_mart", "job:pe_mart_build#t124", kind="derived_mart")]
    _mart_build(pgdb, "pe_marts", "success", NOW - timedelta(days=40))
    _mart_build(pgdb, "pe_marts", "refused", NOW - timedelta(days=2),
                reason="input sec_13f: latest release 2026q2 is failed")
    _mart_build(pgdb, "pe_marts", "refused", NOW - timedelta(days=1),
                reason="busy: another pe_marts build is running")
    _mart_build(pgdb, "pe_marts", "success", NOW - timedelta(hours=5), dry_run=True)

    _, by = _build(pgdb, specs)
    d = by["t124_mart"]
    assert d["status"] == "blocked"
    assert d["blockers"][0]["code"] == "mart_input_refused"
    assert "sec_13f" in d["blockers"][0]["message"]
    assert d["clocks"]["last_success_at"].startswith("2026-08-14")
    # the verdict warns (an admin may override inputs) rather than blocks
    assert "mart_refused" in [w["code"] for w in d["can_run"]["warnings"]]


# ---------------------------------------------------------------------------
# T5 failing / T6 partial
# ---------------------------------------------------------------------------


@pg
def test_failing_is_recent_failure_only(pgdb):
    """T5"""
    specs = [_spec("t124_fail", "dispatch:t124f:alpha"), _spec("t124_oldfail", "dispatch:t124g")]
    _job(pgdb, "t124f", "success", NOW - timedelta(days=5), config={"dataset": "alpha"})
    _queue(pgdb, "ingestion", "failed", NOW - timedelta(days=1),
           payload={"source": "t124f", "config": {"dataset": "alpha"}}, error="HTTP 500")
    _job(pgdb, "t124g", "success", NOW - timedelta(days=70))
    _job(pgdb, "t124g", "failed", NOW - timedelta(days=60))

    _, by = _build(pgdb, specs)
    assert by["t124_fail"]["status"] == "failing"
    assert by["t124_fail"]["clocks"]["last_run_status"] == "failed"
    assert by["t124_fail"]["clocks"]["last_run_store"] == "job_queue"
    assert by["t124_oldfail"]["status"] == "dormant"


@pg
def test_partial(pgdb):
    """T6: success with PARTIAL: is partial, not success."""
    specs = [_spec("t124_part", "bulk:t124p")]
    _heartbeat(pgdb)
    sched = _schedule(pgdb, "bulk:t124p", cron="0 8 4 * *")
    _job(pgdb, "bulk:t124p", "success", NOW - timedelta(days=20), schedule_id=sched.id)
    _queue(pgdb, "bulk_ingest", "success", NOW - timedelta(days=1),
           payload={"bulk_source": "t124p"}, error="PARTIAL: 1 of 4 releases failed")

    _, by = _build(pgdb, specs)
    assert by["t124_part"]["status"] == "partial"
    assert by["t124_part"]["clocks"]["last_run_status"] == "partial"


# ---------------------------------------------------------------------------
# T7 stalled
# ---------------------------------------------------------------------------


@pg
def test_stalled_is_per_schedule(pgdb):
    """T7: a manual success does not mask a schedule that stopped succeeding."""
    from app.core.models import ScheduleFrequency

    specs = [_spec("t124_st", "dispatch:t124st", cadence="daily")]
    sched = _schedule(pgdb, "t124st", frequency=ScheduleFrequency.DAILY,
                      created_at=NOW - timedelta(days=10))
    _job(pgdb, "t124st", "success", NOW - timedelta(days=5), schedule_id=sched.id)
    _job(pgdb, "t124st", "success", NOW - timedelta(hours=1))  # manual run

    _, by = _build(pgdb, specs)
    d = by["t124_st"]
    assert d["status"] == "stalled"
    assert d["schedule"]["schedule_id"] == sched.id
    assert d["schedule"]["last_success_at"].startswith("2026-09-18")
    assert d["clocks"]["last_success_at"].startswith("2026-09-23")


@pg
def test_batch_scheduled_dispatch_stalls_on_catalog_cadence(pgdb):
    """T7b: the nightly batch is a schedule too."""
    specs = [_spec("t124_batch", "dispatch:t124bt", cadence="weekly")]
    _job(pgdb, "t124bt", "success", NOW - timedelta(days=30))

    _, by = _build(pgdb, specs, batch_keys=frozenset({"t124bt"}))
    d = by["t124_batch"]
    assert d["status"] == "stalled"
    assert d["schedule"]["kind"] == "batch"
    assert d["schedule"]["cron"] == "0 2 * * *"


# ---------------------------------------------------------------------------
# T8 current / unknown, T9 behind, T10 awaiting_upstream
# ---------------------------------------------------------------------------


def _healthy_bulk(db, source, days_ago=3):
    _heartbeat(db) if not getattr(db, "_hb", False) else None
    db._hb = True
    sched = _schedule(db, f"bulk:{source}", cron="0 8 4 * *")
    job = _job(db, f"bulk:{source}", "success", NOW - timedelta(days=days_ago),
               schedule_id=sched.id)
    return sched, job


@pg
def test_current_and_unknown(pgdb):
    """T8: coverage meets expectation -> current; no SLO -> unknown, never current."""
    cov = "SELECT max(d) FROM t124_cov"
    specs = [_spec("t124_cur", "bulk:t124cur", slo=240, coverage=cov),
             _spec("t124_nosla", "bulk:t124ns", slo=None, coverage=cov)]
    _healthy_bulk(pgdb, "t124cur")
    _healthy_bulk(pgdb, "t124ns")
    _release(pgdb, "t124cur", "2026-08", "loaded", NOW - timedelta(days=20), NOW - timedelta(days=3))

    _, by = _build(pgdb, specs)
    cur = by["t124_cur"]
    assert cur["status"] == "current", cur["status_reason"]
    assert cur["clocks"]["coverage_through"] == "2026-08-31"
    assert cur["clocks"]["expected_through"] == "2026-08-31"
    assert cur["clocks"]["lag_days"] == 0
    assert cur["clocks"]["expectation_basis"] == "coverage"
    assert cur["clocks"]["last_publish_at"].startswith("2026-09-03")
    assert cur["releases"]["loaded"] == 1
    assert by["t124_nosla"]["status"] == "unknown"
    assert by["t124_nosla"]["clocks"]["expected_through"] is None


@pg
def test_last_success_basis_without_coverage_sql(pgdb):
    """T8b: an SLO but no coverage SQL judges the last success, and says so."""
    specs = [_spec("t124_ls", "bulk:t124ls", slo=24 * 12)]
    _healthy_bulk(pgdb, "t124ls", days_ago=5)
    _, by = _build(pgdb, specs)
    assert by["t124_ls"]["status"] == "current"
    assert by["t124_ls"]["clocks"]["expectation_basis"] == "last_success"


@pg
def test_behind_unloaded_release_and_coverage_lag(pgdb):
    """T9"""
    specs = [
        _spec("t124_unl", "bulk:t124unl", slo=240, coverage="SELECT max(d) FROM t124_cov"),
        _spec("t124_lag", "dispatch:t124lag", slo=240, coverage="SELECT max(d) FROM t124_old"),
    ]
    _healthy_bulk(pgdb, "t124unl")
    _release(pgdb, "t124unl", "2026-07", "loaded", NOW - timedelta(days=40), NOW - timedelta(days=39))
    _release(pgdb, "t124unl", "2026-08", "fetched", NOW - timedelta(days=2))
    from app.core.models import ScheduleFrequency

    sched = _schedule(pgdb, "t124lag", frequency=ScheduleFrequency.MONTHLY)
    _job(pgdb, "t124lag", "success", NOW - timedelta(days=3), schedule_id=sched.id)

    _, by = _build(pgdb, specs)
    assert by["t124_unl"]["status"] == "behind"
    assert by["t124_unl"]["releases"]["unloaded"] == 1
    assert by["t124_unl"]["clocks"]["last_publish_at"].startswith("2026-09-21")
    lag = by["t124_lag"]
    assert lag["status"] == "behind"
    assert lag["clocks"]["lag_days"] == (date(2026, 8, 31) - date(2026, 5, 31)).days


@pg
def test_awaiting_upstream(pgdb):
    """T10: we hold every published release and the next is not due yet."""
    specs = [_spec("t124_aw", "bulk:t124aw", cadence="quarterly", slo=24 * 35,
                   coverage="SELECT max(d) FROM t124_q"),
             _spec("t124_overdue", "bulk:t124od", cadence="quarterly", slo=24 * 35,
                   coverage="SELECT max(d) FROM t124_q")]
    _healthy_bulk(pgdb, "t124aw")
    _healthy_bulk(pgdb, "t124od")
    _release(pgdb, "t124aw", "2026q1", "loaded", NOW - timedelta(days=20), NOW - timedelta(days=19))
    _release(pgdb, "t124od", "2026q1", "loaded", NOW - timedelta(days=200), NOW - timedelta(days=199))

    _, by = _build(pgdb, specs)
    assert by["t124_aw"]["status"] == "awaiting_upstream", by["t124_aw"]["status_reason"]
    assert by["t124_aw"]["clocks"]["lag_days"] > 0
    # the publisher is overdue by our cadence and nothing new was discovered
    assert by["t124_overdue"]["status"] == "behind"


# ---------------------------------------------------------------------------
# T11 superseded, T12 case-insensitive
# ---------------------------------------------------------------------------


@pg
def test_superseded_release_is_not_a_failure(pgdb):
    """T11"""
    specs = [_spec("t124_sup", "bulk:t124sup"), _spec("t124_rf", "bulk:t124rf")]
    _healthy_bulk(pgdb, "t124sup", days_ago=2)
    _healthy_bulk(pgdb, "t124rf", days_ago=2)
    _release(pgdb, "t124sup", "snapshot:2026-09-20", "loaded", NOW - timedelta(days=3),
             NOW - timedelta(days=2))
    _release(pgdb, "t124sup", "snapshot:2026-09-19", "failed", NOW - timedelta(hours=1),
             error="superseded: newer snapshot snapshot:2026-09-20 loaded")
    _release(pgdb, "t124rf", "2026-08", "loaded", NOW - timedelta(days=10), NOW - timedelta(days=9))
    _release(pgdb, "t124rf", "2026-09", "failed", NOW - timedelta(hours=1), error="bad zip")

    _, by = _build(pgdb, specs)
    sup = by["t124_sup"]
    assert sup["status"] not in ("failing", "behind")
    assert sup["releases"]["superseded"] == 1
    assert sup["releases"]["failed"] == 0
    assert sup["releases"]["unloaded"] == 0
    rf = by["t124_rf"]
    assert rf["status"] == "failing"
    assert rf["clocks"]["last_run_store"] == "source_release"


@pg
def test_statuses_are_case_insensitive(pgdb):
    """T12: 'FAILED' / 'SUCCESS' rows count."""
    from sqlalchemy import text

    specs = [_spec("t124_ci", "dispatch:t124ci"), _spec("t124_ci2", "dispatch:t124ci2")]
    pgdb.execute(text(
        "INSERT INTO ingestion_jobs (source, status, config, created_at, completed_at, "
        "retry_count, max_retries, data_origin) VALUES ('t124ci', 'SUCCESS', '{}', :t, :t, 0, 3, 'real')"
    ), {"t": NOW - timedelta(days=4)})
    pgdb.execute(text(
        "INSERT INTO job_queue (job_type, status, payload, priority, created_at, completed_at) "
        "VALUES ('ingestion', 'FAILED', '{\"source\": \"t124ci2\"}', 0, :t, :t)"
    ), {"t": NOW - timedelta(days=1)})
    pgdb.commit()

    _, by = _build(pgdb, specs)
    assert by["t124_ci"]["clocks"]["last_success_at"] is not None
    assert by["t124_ci"]["status"] == "dormant"
    assert by["t124_ci2"]["status"] == "failing"


# ---------------------------------------------------------------------------
# T13 schedule block
# ---------------------------------------------------------------------------


@pg
def test_schedule_block_next_run_and_missed_runs(pgdb):
    """T13"""
    specs = [_spec("t124_sch", "dispatch:t124sch", cadence="daily")]
    sched = _schedule(pgdb, "t124sch", cron="0 6 * * *", created_at=NOW - timedelta(days=60),
                      next_run_at=datetime(2026, 9, 24, 6, 0))
    for i in range(25):
        _job(pgdb, "t124sch", "success", NOW - timedelta(days=i, hours=5), schedule_id=sched.id)

    _, by = _build(pgdb, specs)
    s = by["t124_sch"]["schedule"]
    assert s["kind"] == "schedule"
    assert s["cron"] == "0 6 * * *"
    assert s["next_run_source"] == "db"
    assert s["next_run_at"].startswith("2026-09-24T06:00")
    # fires 08-25 .. 09-23 at 06:00 = 30; 25 runs -> 5 missed
    assert s["missed_runs_30d"] == 5

    live = SimpleNamespace(
        running=True,
        get_jobs=lambda: [SimpleNamespace(id=f"schedule_{sched.id}",
                                          next_run_time=datetime(2026, 9, 24, 6, 0, 1))],
    )
    _, by = _build(pgdb, specs, scheduler=live)
    assert by["t124_sch"]["schedule"]["next_run_source"] == "apscheduler"
    assert by["t124_sch"]["schedule"]["next_run_at"].startswith("2026-09-24T06:00:01")


# ---------------------------------------------------------------------------
# T14 batching
# ---------------------------------------------------------------------------


@pg
def test_statement_count_is_constant(pgdb):
    """T14: no per-dataset / per-row query loops, full catalog included."""
    from sqlalchemy import event

    from sqlalchemy import text

    from app.catalog import get_catalog

    engine = pgdb.get_bind()

    def coverage_runs(spec):
        # other suites can leave a catalog table behind in a different shape;
        # such a dataset's coverage SQL fails here, and that path has its own
        # test (T8c). Leave those out so the combined path is what is measured.
        if not spec.coverage_sql:
            return True
        try:
            with engine.connect() as conn:
                conn.execute(text(spec.coverage_sql)).scalar()
            return True
        except Exception:
            return False

    specs = [s for s in get_catalog() if coverage_runs(s)] + [
        _spec("t124_cnt", "bulk:t124cnt", slo=240, coverage="SELECT max(d) FROM t124_cov")]
    counter = {"n": 0}

    def count(*a, **k):
        counter["n"] += 1

    def measure(cold=True):
        import app.services.dataset_status as ds

        if cold:
            ds.clear_cache()
        counter["n"] = 0
        event.listen(engine, "before_cursor_execute", count)
        try:
            _build(pgdb, specs)
        finally:
            event.remove(engine, "before_cursor_execute", count)
        return counter["n"]

    _heartbeat(pgdb)
    _schedule(pgdb, "bulk:t124cnt", cron="0 8 4 * *")
    first = measure()

    from app.core.models import ScheduleFrequency

    for i, src in enumerate(["treasury", "fred", "eia", "noaa", "bls", "fdic", "cms", "bea"]):
        s = _schedule(pgdb, src, frequency=ScheduleFrequency.DAILY)
        for k in range(3):
            _job(pgdb, src, "success" if k else "failed", NOW - timedelta(days=k + i), schedule_id=s.id)
            _queue(pgdb, "ingestion", "success", NOW - timedelta(days=k),
                   payload={"source": src, "config": {}})
    for b in ["sec_13f", "sec_form_d", "sec_insider", "sec_iapd_feed"]:
        _schedule(pgdb, f"bulk:{b}", cron="0 8 4 * *")
        _release(pgdb, b, "r1", "loaded", NOW - timedelta(days=5), NOW - timedelta(days=4), nbytes=10)
        _queue(pgdb, "bulk_ingest", "success", NOW - timedelta(days=4), payload={"bulk_source": b})
    _mart_build(pgdb, "pe_marts", "success", NOW - timedelta(days=5))
    _mart_build(pgdb, "entity_resolve", "success", NOW - timedelta(days=5))
    second = measure()

    assert first == second, (first, second)
    # Warm coverage cache: the per-store queries only (SET LOCAL timeout,
    # existence, 5 stores, batch runs, schedules, schedule success/runs,
    # heartbeats, relation stats).
    warm = measure(cold=False)
    assert warm <= 13, warm
    # Cold: exactly one combined coverage statement on top (SET LOCAL + SELECT),
    # never one per dataset.
    assert first == warm + 2, (first, warm)


# ---------------------------------------------------------------------------
# T15 GET endpoint, T16 run endpoint
# ---------------------------------------------------------------------------


ADMIN = {"user_id": 1, "email": "admin@example.com", "name": "Admin", "role": "admin"}
USER = {"user_id": 2, "email": "user@example.com", "name": "User", "role": "user"}


@pytest.fixture
def client(pgdb, monkeypatch):
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session

    import app.api.v1.dataset_status as router_mod
    import app.services.dataset_status as ds
    from app.core.authz import current_principal, require_admin_for_writes
    from app.core.database import get_db

    engine = pgdb.get_bind()
    app = FastAPI()
    app.include_router(router_mod.router, prefix="/api/v1",
                       dependencies=[Depends(require_admin_for_writes)])

    def _db():
        with Session(engine) as s:
            yield s

    state = {"principal": ADMIN}
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[current_principal] = lambda: state["principal"]

    specs = [
        _spec("t124_r_bulk", "bulk:t124rb", rerun="currency_only"),
        _spec("t124_r_job", "job:pe_mart_build#t124", kind="derived_mart"),
        _spec("t124_r_disp", "dispatch:t124rd:beta", kind="timeseries"),
        _spec("t124_r_coll", "collector:t124rc", kind="geo"),
        _spec("t124_r_api", "api:t124ra", status_public="archival"),
        _spec("t124_r_key", "dispatch:t124rk"),
    ]
    monkeypatch.setattr(ds, "catalog_specs", lambda: tuple(specs))
    monkeypatch.setattr(ds, "api_key_preflight",
                        lambda s: "API key required for 't124rk' but not configured"
                        if s.startswith("t124rk") else None)
    monkeypatch.setattr(ds, "BATCH_SCHEDULED_DISPATCH", frozenset())
    c = TestClient(app)
    c.state = state
    c.specs = specs
    return c


@pg
def test_status_endpoint_filters_summary_and_model(client, pgdb):
    """T15"""
    from app.api.v1.dataset_status import DatasetStatusResponse

    r = client.get("/api/v1/datasets/status")
    assert r.status_code == 200, r.text
    body = DatasetStatusResponse.model_validate(r.json())
    assert body.count == body.total == len(client.specs)
    assert sum(body.summary.values()) == body.count
    assert body.summary["blocked"] == 1  # the missing key

    client.state["principal"] = USER  # reads are user-level
    r = client.get("/api/v1/datasets/status", params={"kind": "geo"})
    assert r.status_code == 200
    assert [d["key"] for d in r.json()["datasets"]] == ["t124_r_coll"]
    assert r.json()["total"] == len(client.specs)

    r = client.get("/api/v1/datasets/status", params={"status": "blocked"})
    assert [d["key"] for d in r.json()["datasets"]] == ["t124_r_key"]
    r = client.get("/api/v1/datasets/status", params={"status_public": "archival"})
    assert [d["key"] for d in r.json()["datasets"]] == ["t124_r_api"]
    r = client.get("/api/v1/datasets/status", params={"source": "nope"})
    assert r.json()["count"] == 0
    assert client.get("/api/v1/datasets/status", params={"status": "fresh"}).status_code == 422
    assert client.get("/api/v1/datasets/status", params={"kind": "x"}).status_code == 422


@pg
def test_run_requires_admin_and_known_key(client):
    """T16a"""
    client.state["principal"] = USER
    assert client.post("/api/v1/datasets/t124_r_bulk/run").status_code == 403
    client.state["principal"] = ADMIN
    assert client.post("/api/v1/datasets/nope/run").status_code == 404


@pg
def test_run_refused_with_verdict(client, monkeypatch):
    """T16b: 409 carries the same verdict the page shows."""
    import app.core.job_queue_service as jqs

    monkeypatch.setattr(jqs, "WORKER_MODE", True)
    r = client.post("/api/v1/datasets/t124_r_key/run")
    assert r.status_code == 409
    verdict = r.json()["can_run"]
    assert verdict["allowed"] is False and verdict["requires"] == "admin"
    assert "missing_api_key" in [b["code"] for b in verdict["blockers"]]

    r = client.post("/api/v1/datasets/t124_r_api/run")
    assert r.status_code == 409
    assert "no_run_path" in [b["code"] for b in r.json()["can_run"]["blockers"]]

    # queue mode but no worker heartbeat
    r = client.post("/api/v1/datasets/t124_r_bulk/run")
    assert r.status_code == 409
    assert "no_live_worker" in [b["code"] for b in r.json()["can_run"]["blockers"]]

    monkeypatch.setattr(jqs, "WORKER_MODE", False)
    r = client.post("/api/v1/datasets/t124_r_bulk/run")
    assert r.status_code == 409
    assert "worker_mode_off" in [b["code"] for b in r.json()["can_run"]["blockers"]]


@pg
def test_run_enqueues_each_producer_kind_with_audit_actor(client, pgdb, monkeypatch):
    """T16c"""
    from sqlalchemy import text

    import app.core.job_queue_service as jqs

    monkeypatch.setattr(jqs, "WORKER_MODE", True)
    _heartbeat(pgdb, at=datetime.utcnow())  # the endpoint judges against the real clock

    r = client.post("/api/v1/datasets/t124_r_bulk/run")
    assert r.status_code == 202, r.text
    out = r.json()
    job = pgdb.execute(text(
        "SELECT source, trigger, dataset_key FROM ingestion_jobs WHERE id = :i"
    ), {"i": out["ingestion_job_id"]}).one()
    assert tuple(job) == ("bulk:t124rb", "manual", "t124_r_bulk")
    q = pgdb.execute(text("SELECT job_type, payload, job_table_id FROM job_queue WHERE id = :i"),
                     {"i": out["job_queue_id"]}).one()
    assert q[0] == "bulk_ingest"
    assert q[1]["bulk_source"] == "t124rb" and q[1]["ingestion_job_id"] == out["ingestion_job_id"]
    assert q[2] == out["ingestion_job_id"]
    audit = pgdb.execute(text(
        "SELECT actor, trigger_type, trigger_source, source, job_id, config_snapshot "
        "FROM collection_audit_log WHERE id = :i"), {"i": out["audit_id"]}).one()
    assert audit[0] == "admin@example.com"
    assert audit[1] == "api" and audit[2] == "/datasets/t124_r_bulk/run"
    assert audit[3] == "bulk:t124rb" and audit[4] == out["ingestion_job_id"]
    assert audit[5]["dataset_key"] == "t124_r_bulk"

    # a second run while the first is queued is refused
    r = client.post("/api/v1/datasets/t124_r_bulk/run")
    assert r.status_code == 409
    assert "already_running" in [b["code"] for b in r.json()["can_run"]["blockers"]]

    r = client.post("/api/v1/datasets/t124_r_job/run")
    assert r.status_code == 202, r.text
    q = pgdb.execute(text("SELECT job_type, payload FROM job_queue WHERE id = :i"),
                     {"i": r.json()["job_queue_id"]}).one()
    assert q[0] == "pe_mart_build" and q[1]["ingestion_job_id"] == r.json()["ingestion_job_id"]
    assert pgdb.execute(text("SELECT source, dataset_key FROM ingestion_jobs WHERE id = :i"),
                        {"i": r.json()["ingestion_job_id"]}).one() == ("job:pe_mart_build", "t124_r_job")

    r = client.post("/api/v1/datasets/t124_r_disp/run")
    assert r.status_code == 202, r.text
    q = pgdb.execute(text("SELECT job_type, payload FROM job_queue WHERE id = :i"),
                     {"i": r.json()["job_queue_id"]}).one()
    assert q[0] == "ingestion"
    assert q[1]["source"] == "t124rd" and q[1]["config"]["dataset"] == "beta"
    assert pgdb.execute(text("SELECT source, dataset_key FROM ingestion_jobs WHERE id = :i"),
                        {"i": r.json()["ingestion_job_id"]}).one() == ("t124rd", "t124_r_disp")

    r = client.post("/api/v1/datasets/t124_r_coll/run")
    assert r.status_code == 202, r.text
    q = pgdb.execute(text("SELECT job_type, payload FROM job_queue WHERE id = :i"),
                     {"i": r.json()["job_queue_id"]}).one()
    assert q[0] == "site_intel" and q[1]["sources"] == ["t124rc"]
    assert r.json()["ingestion_job_id"] is None

    actors = pgdb.execute(text("SELECT DISTINCT actor FROM collection_audit_log")).scalars().all()
    assert actors == ["admin@example.com"]


@pg
def test_run_dispatch_in_process_when_worker_mode_off(client, pgdb, monkeypatch):
    """T16d: WORKER_MODE off runs a dispatch key through BackgroundTasks, like POST /jobs."""
    import app.api.v1.jobs as jobs
    import app.core.job_queue_service as jqs

    monkeypatch.setattr(jqs, "WORKER_MODE", False)
    calls = []

    async def fake_run(job_id, source, config):
        calls.append((job_id, source, config))

    monkeypatch.setattr(jobs, "run_ingestion_job", fake_run)
    r = client.post("/api/v1/datasets/t124_r_disp/run")
    assert r.status_code == 202, r.text
    assert r.json()["mode"] == "background"
    assert calls == [(r.json()["ingestion_job_id"], "t124rd", {"dataset": "beta"})]


@pg
def test_listener_projects_dataset_key(pgdb):
    """T2b: every new IngestionJob gets its dataset_key when unambiguous."""
    from app.core.models import IngestionJob

    a = IngestionJob(source="bulk:sec_13f", status="pending", config={})
    b = IngestionJob(source="job:pe_mart_build", status="pending", config={})
    c = IngestionJob(source="treasury", status="pending", config={"dataset": "auctions"})
    d = IngestionJob(source="bulk:sec_13f", status="pending", config={}, dataset_key="custom_key")
    pgdb.add_all([a, b, c, d])
    pgdb.commit()
    assert a.dataset_key == "sec_13f"
    assert b.dataset_key is None
    assert c.dataset_key == "treasury_auctions"
    assert d.dataset_key == "custom_key"


# ---------------------------------------------------------------------------
# T17 migration, T18 batched per-schedule success
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMigration0014:
    def test_revision_chain(self):
        mod = _migration("0014_dataset_status")
        assert mod.revision == "0014_dataset_status"
        assert mod.down_revision == "0013_mart_build"
        ddl = " ".join(mod.UPGRADE_SQL)
        assert "to_regclass('public.collection_audit_log')" in ddl
        assert "to_regclass('public.ingestion_jobs')" in ddl
        assert "ADD COLUMN IF NOT EXISTS actor" in ddl
        assert "ADD COLUMN IF NOT EXISTS dataset_key" in ddl
        assert "ix_ingestion_jobs_dataset_key" in ddl

    def test_single_head(self):
        parents = []
        for path in (REPO / "alembic" / "versions").glob("*.py"):
            if '"0013_mart_build"' in path.read_text(encoding="utf-8").split("down_revision", 1)[-1][:40]:
                parents.append(path.name)
        assert parents == ["0014_dataset_status.py"]


@pg
def test_migration_0014_applies_twice_and_on_empty_db():
    """T17"""
    from sqlalchemy import create_engine, text

    mod = _migration("0014_dataset_status")
    engine = create_engine(PG_URL)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS public.collection_audit_log CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS public.ingestion_jobs CASCADE"))
            for stmt in mod.UPGRADE_SQL:  # tables absent: a no-op, not an error
                conn.execute(text(stmt))
            conn.execute(text("CREATE TABLE collection_audit_log (id SERIAL PRIMARY KEY, "
                              "source VARCHAR(50) NOT NULL)"))
            conn.execute(text("CREATE TABLE ingestion_jobs (id SERIAL PRIMARY KEY, "
                              "source VARCHAR(50) NOT NULL)"))
            conn.execute(text("INSERT INTO ingestion_jobs (source) VALUES ('fred')"))
            for _ in range(2):
                for stmt in mod.UPGRADE_SQL:
                    conn.execute(text(stmt))
            cols = set(conn.execute(text(
                "SELECT table_name || '.' || column_name FROM information_schema.columns "
                "WHERE table_name IN ('collection_audit_log', 'ingestion_jobs')"
            )).scalars())
            assert {"collection_audit_log.actor", "ingestion_jobs.dataset_key"} <= cols
            assert conn.execute(text(
                "SELECT to_regclass('public.ix_ingestion_jobs_dataset_key')")).scalar()
            assert conn.execute(text("SELECT dataset_key FROM ingestion_jobs")).scalar() is None
            for stmt in mod.DOWNGRADE_SQL:
                conn.execute(text(stmt))
            conn.execute(text("DROP TABLE collection_audit_log"))
            conn.execute(text("DROP TABLE ingestion_jobs"))
    finally:
        engine.dispose()


@pg
def test_batched_last_success_matches_per_schedule(pgdb):
    """T18"""
    from app.services.data_watchdog import last_success_by_schedule, last_success_for_schedule

    s1 = _schedule(pgdb, "fred")
    s2 = _schedule(pgdb, "bulk:sec_13f")
    s3 = _schedule(pgdb, "job:pe_mart_build")
    _job(pgdb, "fred", "success", NOW - timedelta(days=3), schedule_id=s1.id)
    _job(pgdb, "fred", "failed", NOW - timedelta(days=1), schedule_id=s1.id)
    j = _job(pgdb, "job:pe_mart_build", "pending", NOW - timedelta(days=2), schedule_id=s3.id)
    _queue(pgdb, "pe_mart_build", "success", NOW - timedelta(days=2),
           payload={"ingestion_job_id": j.id}, job_table_id=j.id)

    batched = last_success_by_schedule(pgdb)
    for s in (s1, s2, s3):
        assert batched.get(s.id) == last_success_for_schedule(pgdb, s.id)
    assert s2.id not in batched


@pg
def test_one_bad_coverage_sql_does_not_blank_the_others(pgdb):
    """T8c: the combined coverage statement fails -> each is retried alone."""
    from app.core.models import ScheduleFrequency

    specs = [_spec("t124_good", "dispatch:t124good", slo=240,
                   coverage="SELECT max(d) FROM t124_cov"),
             _spec("t124_bad", "dispatch:t124bad", slo=240, tables=("t124_old",),
                   coverage="SELECT max(no_such_column) FROM t124_old")]
    for src in ("t124good", "t124bad"):
        s = _schedule(pgdb, src, frequency=ScheduleFrequency.MONTHLY)
        _job(pgdb, src, "success", NOW - timedelta(days=2), schedule_id=s.id)

    _, by = _build(pgdb, specs)
    assert by["t124_good"]["status"] == "current"
    assert by["t124_good"]["clocks"]["coverage_through"] == "2026-08-31"
    assert by["t124_bad"]["status"] == "unknown"
    assert by["t124_bad"]["status_reason"] == "coverage unavailable: the coverage query failed"
    assert by["t124_bad"]["clocks"]["coverage_error"] == "error"
    assert by["t124_good"]["clocks"]["coverage_error"] is None


# ---------------------------------------------------------------------------
# Review fixes (spec-124-fix)
# ---------------------------------------------------------------------------


@pg
def test_backfill_releases_sharing_one_discovered_at_are_not_behind(pgdb):
    """R1: one run upserts every release in one transaction (one discovered_at).
    Historic quarters of that run that failed or were skipped do not pin the
    dataset to 'behind'; a release discovered by a later run does."""
    cov = "SELECT max(d) FROM t124_cov"
    specs = [_spec("t124_bf", "bulk:t124bf", slo=240, coverage=cov),
             _spec("t124_bf2", "bulk:t124bf2", slo=240, coverage=cov)]
    _healthy_bulk(pgdb, "t124bf")
    _healthy_bulk(pgdb, "t124bf2")
    run1 = NOW - timedelta(days=30)
    for src in ("t124bf", "t124bf2"):
        _release(pgdb, src, "2026-08", "loaded", run1, NOW - timedelta(days=29))
        _release(pgdb, src, "2025-11", "failed", run1, error="BadZipFile: truncated")
        _release(pgdb, src, "2025-10", "discovered", run1)   # max_releases cut it
        _release(pgdb, src, "2025-09", "fetched", run1)
    # a later run discovered a new release and has not loaded it
    _release(pgdb, "t124bf2", "2026-09", "discovered", NOW - timedelta(days=1))

    _, by = _build(pgdb, specs)
    bf = by["t124_bf"]
    assert bf["releases"]["unloaded"] == 0
    assert bf["releases"]["failed"] == 1
    assert bf["status"] == "current", bf["status_reason"]
    bf2 = by["t124_bf2"]
    assert bf2["releases"]["unloaded"] == 1
    assert bf2["status"] == "behind"
    assert "2026-09" in bf2["status_reason"]


@pg
def test_slow_coverage_query_is_isolated_and_remembered(pgdb, monkeypatch):
    """R2: a slow coverage query times out alone, is reported as a timeout
    (not an empty table), and next time runs apart from the combined
    statement so it cannot blank the others."""
    import app.services.dataset_status as ds
    from app.core.models import ScheduleFrequency
    from sqlalchemy import event

    monkeypatch.setattr(ds, "COVERAGE_TIMEOUT_MS", 300)
    monkeypatch.setattr(ds, "COVERAGE_FALLBACK_TIMEOUT_MS", 200)
    monkeypatch.setattr(ds, "BACKGROUND_REFRESH", False)
    slow = "SELECT max(d) FROM t124_old, (SELECT pg_sleep(1)) s"
    specs = [_spec("t124_fast", "dispatch:t124fast", slo=240, coverage="SELECT max(d) FROM t124_cov"),
             _spec("t124_slow", "dispatch:t124slow", slo=240, tables=("t124_old",), coverage=slow)]
    for src in ("t124fast", "t124slow"):
        s = _schedule(pgdb, src, frequency=ScheduleFrequency.MONTHLY)
        _job(pgdb, src, "success", NOW - timedelta(days=2), schedule_id=s.id)

    _, by = _build(pgdb, specs)
    assert by["t124_fast"]["status"] == "current"
    assert by["t124_slow"]["status"] == "unknown"
    assert by["t124_slow"]["clocks"]["coverage_error"] == "timeout"
    assert by["t124_slow"]["status_reason"] == "coverage unavailable: the coverage query timed out"

    # expire both entries; the refresh (inline here) runs the known-slow one alone
    with ds._coverage_lock:
        for k, (_, v, e) in list(ds._coverage_cache.items()):
            ds._coverage_cache[k] = (0.0, v, e)
    seen = []

    def spy(conn, cursor, statement, *a):
        if "max(d)" in statement:
            seen.append(statement)

    engine = pgdb.get_bind()
    event.listen(engine, "before_cursor_execute", spy)
    try:
        _, by = _build(pgdb, specs)
    finally:
        event.remove(engine, "before_cursor_execute", spy)
    combined = [s for s in seen if "AS c0" in s]
    assert combined and all("pg_sleep" not in s for s in combined), seen
    assert by["t124_fast"]["status"] == "current"


@pg
def test_stale_coverage_is_served_while_refreshing_in_background(pgdb, monkeypatch):
    """R2b: once computed, coverage never runs on the request path again."""
    import threading

    import app.services.dataset_status as ds

    specs = [_spec("t124_swr", "dispatch:t124swr", slo=240, coverage="SELECT max(d) FROM t124_cov")]
    _build(pgdb, specs)                                   # cold: computed inline
    with ds._coverage_lock:
        for k, (_, v, e) in list(ds._coverage_cache.items()):
            ds._coverage_cache[k] = (0.0, v, e)
    threads = []
    real = ds._compute_coverage

    def compute(engine, todo):
        threads.append(threading.current_thread().name)
        return real(engine, todo)

    monkeypatch.setattr(ds, "_compute_coverage", compute)
    _, by = _build(pgdb, specs)
    assert by["t124_swr"]["clocks"]["coverage_through"] == "2026-08-31"   # the stale value
    for t in threading.enumerate():
        if t.name == "dataset-status-coverage":
            t.join(5)
    assert threads == ["dataset-status-coverage"]


@pg
def test_store_timeout_degrades_instead_of_failing(pgdb, monkeypatch):
    """R9: every fact query runs under statement_timeout; a store that times
    out is named in 'degraded' and absence-based statuses become unknown."""
    import app.services.dataset_status as ds

    monkeypatch.setattr(ds, "STATUS_TIMEOUT_MS", 100)
    monkeypatch.setattr(ds, "_QUEUE_SQL",
                        "SELECT pg_sleep(1) AS x WHERE CAST(:partial AS TEXT) IS NOT NULL "
                        "AND CAST(:active_since AS TIMESTAMP) IS NOT NULL")
    specs = [_spec("t124_deg", "dispatch:t124deg"), _spec("t124_degf", "dispatch:t124degf")]
    _job(pgdb, "t124degf", "failed", NOW - timedelta(days=1), error="boom")

    report, by = _build(pgdb, specs)
    assert report["degraded"] == ["job_queue"]
    assert by["t124_deg"]["status"] == "unknown"
    assert by["t124_deg"]["status_reason"].startswith("evidence incomplete (job_queue unavailable)")
    # positive evidence from the stores that answered still counts
    assert by["t124_degf"]["status"] == "failing"


@pytest.mark.unit
def test_redact_masks_secrets():
    """R7"""
    from app.services.dataset_status import redact

    msg = ("Client error '403 Forbidden' for url 'https://api.eia.gov/v2/electricity/"
           "?api_key=ABCSECRET123&frequency=monthly&token=t0k'")
    out = redact(msg)
    assert "ABCSECRET123" not in out and "t0k" not in out
    assert "api_key=***" in out and "frequency=monthly" in out
    assert "SECRET" not in redact("Authorization: Bearer SECRETTOKEN.abc")
    assert redact("postgresql://user:hunter2@db:5432/x") == "postgresql://user:***@db:5432/x"
    assert redact("monkey=1 donkey=2") == "monkey=1 donkey=2"
    assert redact(None) is None


@pg
def test_error_text_admin_only_and_redacted(client, pgdb):
    """R7: users see that a run failed and when; admins also see the
    (redacted) error text."""
    _job(pgdb, "t124rd", "failed", datetime.utcnow() - timedelta(hours=2),
         config={"dataset": "beta"},
         error="HTTPStatusError: 403 for url 'https://x.example/api?api_key=SECRET999&q=1'")

    client.state["principal"] = ADMIN
    d = {x["key"]: x for x in client.get("/api/v1/datasets/status").json()["datasets"]}
    assert d["t124_r_disp"]["status"] == "failing"
    assert "api_key=***" in d["t124_r_disp"]["status_reason"]
    assert "SECRET999" not in d["t124_r_disp"]["status_reason"]

    client.state["principal"] = USER
    d = {x["key"]: x for x in client.get("/api/v1/datasets/status").json()["datasets"]}
    assert d["t124_r_disp"]["status"] == "failing"
    assert "HTTPStatusError" not in d["t124_r_disp"]["status_reason"]
    assert "x.example" not in d["t124_r_disp"]["status_reason"]


@pg
def test_in_process_run_counts_as_already_running(pgdb):
    """R8: with WORKER_MODE=0 a recent pending/running ingestion_jobs row is
    the only sign of a run in flight; in queue mode such rows are ignored
    (PLAN_085 §8.2 zombies) because job_queue is the evidence."""
    specs = [_spec("t124_ip", "dispatch:t124ip"), _spec("t124_ipold", "dispatch:t124ipo")]
    _job(pgdb, "t124ip", "running", NOW - timedelta(minutes=5))
    _job(pgdb, "t124ipo", "running", NOW - timedelta(days=3))   # an old zombie

    _, by = _build(pgdb, specs, worker_mode=False)
    assert "already_running" in [b["code"] for b in by["t124_ip"]["can_run"]["blockers"]]
    assert by["t124_ipold"]["can_run"]["allowed"] is True
    # the running row is not a terminal run
    assert by["t124_ip"]["clocks"]["last_run_at"] is None
    _heartbeat(pgdb)
    _, by = _build(pgdb, specs, worker_mode=True)
    assert "already_running" not in [b["code"] for b in by["t124_ip"]["can_run"]["blockers"]]


@pg
def test_run_lock_refuses_a_concurrent_request(client, pgdb, monkeypatch):
    """R8: check-then-enqueue is serialized per dataset."""
    import app.api.v1.jobs as jobs
    import app.core.job_queue_service as jqs
    import app.services.dataset_status as ds

    monkeypatch.setattr(jqs, "WORKER_MODE", False)

    async def fake_run(job_id, source, config):
        return None

    monkeypatch.setattr(jobs, "run_ingestion_job", fake_run)
    engine = pgdb.get_bind()
    with ds.run_lock(engine, "t124_r_disp") as held:
        assert held
        r = client.post("/api/v1/datasets/t124_r_disp/run")
    assert r.status_code == 409
    msgs = [b["message"] for b in r.json()["can_run"]["blockers"]]
    assert any("another run request" in m for m in msgs)
    # released: the request goes through, and a second one sees it in flight
    r = client.post("/api/v1/datasets/t124_r_disp/run")
    assert r.status_code == 202, r.text


@pg
def test_second_run_in_process_is_refused(client, pgdb, monkeypatch):
    """R8: WORKER_MODE=0, the first POST's job is pending/running: the second is refused."""
    import app.api.v1.jobs as jobs
    import app.core.job_queue_service as jqs

    monkeypatch.setattr(jqs, "WORKER_MODE", False)

    async def fake_run(job_id, source, config):
        return None   # leaves the job pending, as a long in-process run would

    monkeypatch.setattr(jobs, "run_ingestion_job", fake_run)
    assert client.post("/api/v1/datasets/t124_r_disp/run").status_code == 202
    r = client.post("/api/v1/datasets/t124_r_disp/run")
    assert r.status_code == 409
    assert "already_running" in [b["code"] for b in r.json()["can_run"]["blockers"]]


@pg
def test_batch_missed_runs(pgdb):
    """Scope: missed_runs_30d for nightly-batch datasets."""
    specs = [_spec("t124_bm", "dispatch:t124bm", cadence="daily"),
             _spec("t124_bm2", "dispatch:t124bm2", cadence="daily")]
    batch = frozenset({"t124bm", "t124bm2"})
    _, by = _build(pgdb, specs, batch_keys=batch)
    assert by["t124_bm"]["schedule"]["missed_runs_30d"] is None   # no batch has ever run
    # the batch has existed for 10 days (10 fires of 02:00 since NOW-10d 12:00)
    _job(pgdb, "other_src", "success", NOW - timedelta(days=10), batch_run_id="batch_first")
    for i in (1, 2, 3):
        _job(pgdb, "t124bm", "success", NOW - timedelta(days=i, hours=9),
             batch_run_id=f"batch_{i}")
    _, by = _build(pgdb, specs, batch_keys=batch)
    assert by["t124_bm"]["schedule"]["kind"] == "batch"
    assert by["t124_bm"]["schedule"]["missed_runs_30d"] == 10 - 3
    assert by["t124_bm2"]["schedule"]["missed_runs_30d"] == 10
    # an older batch: the whole 30-day window counts
    _job(pgdb, "other_src", "success", NOW - timedelta(days=45), batch_run_id="batch_old")
    _, by = _build(pgdb, specs, batch_keys=batch)
    assert by["t124_bm"]["schedule"]["missed_runs_30d"] == 30 - 3
    assert by["t124_bm2"]["schedule"]["missed_runs_30d"] == 30


@pytest.mark.unit
def test_freshness_lists_never_run_catalog_sources():
    """Scope (item 8): catalog producers never attempted, scheduled or SLA'd
    are listed apart as never_run; attempted ones are not."""
    from unittest.mock import MagicMock, patch

    from app.api.v1.freshness import _catalog_job_sources, get_freshness_dashboard

    catalog = _catalog_job_sources()
    assert "bulk:sec_13f" in catalog
    assert "sec_13f" in catalog["bulk:sec_13f"][1]

    db = MagicMock()
    q = MagicMock()
    q.filter.return_value.all.return_value = []
    q.all.return_value = []
    q.distinct.return_value.all.return_value = [MagicMock(source="bulk:sec_13f")]
    db.query.return_value = q
    fake = {"bulk:sec_13f": ({"bulk:sec_13f"}, ["sec_13f"]),
            "treasury": ({"treasury", "treasury:auctions"}, ["treasury_auctions"])}
    with patch("app.services.data_watchdog.last_success_by_source", return_value={}), \
            patch("app.api.v1.freshness._catalog_job_sources", return_value=fake):
        result = get_freshness_dashboard(db=db)
    assert result["never_run"] == [{"source": "treasury", "datasets": ["treasury_auctions"],
                                    "freshness": "never_run"}]
    assert result["never_run_count"] == 1
    assert [s["source"] for s in result["sources"]] == ["bulk:sec_13f"]


@pg
def test_migration_0014_gives_up_on_a_held_lock():
    """R5: the ALTER does not queue behind a long transaction forever."""
    import time as _time

    from sqlalchemy import create_engine, text

    mod = _migration("0014_dataset_status")
    engine = create_engine(PG_URL)
    holder = engine.connect()
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS public.collection_audit_log CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS public.ingestion_jobs CASCADE"))
            conn.execute(text("CREATE TABLE ingestion_jobs (id SERIAL PRIMARY KEY, "
                              "source VARCHAR(50) NOT NULL)"))
        tx = holder.begin()
        holder.execute(text("SELECT count(*) FROM ingestion_jobs"))   # AccessShareLock held

        sql = mod.upgrade_sql(lock_timeout="100ms", attempts=3, sleep_s=0.05)
        started = _time.monotonic()
        with pytest.raises(Exception) as exc:
            with engine.begin() as conn:
                conn.execute(text(sql))
        assert _time.monotonic() - started < 5
        assert "lock" in str(exc.value).lower()
        tx.rollback()

        with engine.begin() as conn:   # the lock is gone: it applies
            conn.execute(text(sql))
            assert conn.execute(text(
                "SELECT 1 FROM information_schema.columns WHERE table_name = 'ingestion_jobs' "
                "AND column_name = 'dataset_key'")).first()
    finally:
        holder.close()
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS public.ingestion_jobs CASCADE"))
        engine.dispose()


@pg
def test_startup_refuses_an_unmigrated_schema():
    """R4/R6: a failed 0014 must stop startup, not break every IngestionJob query."""
    from sqlalchemy import create_engine, text

    from app.core.migrate import SchemaNotMigrated, missing_mapped_columns, verify_mapped_columns

    engine = create_engine(PG_URL)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS public.collection_audit_log CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS public.ingestion_jobs CASCADE"))
        assert missing_mapped_columns(engine) == []      # fresh DB: create_all makes them
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE ingestion_jobs (id SERIAL PRIMARY KEY)"))
            conn.execute(text("CREATE TABLE collection_audit_log (id SERIAL PRIMARY KEY, "
                              "actor VARCHAR(255))"))
        assert missing_mapped_columns(engine) == ["ingestion_jobs.dataset_key"]
        with pytest.raises(SchemaNotMigrated, match="ingestion_jobs.dataset_key"):
            verify_mapped_columns(engine)
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE ingestion_jobs ADD COLUMN dataset_key VARCHAR(64)"))
        verify_mapped_columns(engine)
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS public.collection_audit_log CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS public.ingestion_jobs CASCADE"))
        engine.dispose()
