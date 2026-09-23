"""
Tests for SPEC 128 — data watchdog and honest health.

The API ingestor fleet was dead for ~159 days and nothing told anyone:
/health could not see the worker and always said "healthy", staleness counted
failed jobs as activity, and nothing pushed an alert anywhere.
"""
import asyncio
import importlib.util
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 23, 12, 0, 0)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _health_client(engine_factory):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import app.api.health as health

    app = FastAPI()
    app.include_router(health.router)
    health_get_engine = health.get_engine
    health.get_engine = engine_factory
    return TestClient(app), lambda: setattr(health, "get_engine", health_get_engine)


def _source_release_ddl():
    path = REPO / "alembic" / "versions" / "0004_bulk_framework.py"
    spec = importlib.util.spec_from_file_location("_mig_0004", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.SOURCE_RELEASE_DDL


class Recorder:
    """httpx transport that records requests and answers with a fixed status."""

    def __init__(self, status=200):
        self.status = status
        self.requests = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.status, text="ok")

    @property
    def transport(self):
        return httpx.MockTransport(self)

    def posts(self):
        return [json.loads(r.content) for r in self.requests if r.method == "POST"]


@pytest.fixture
def pgdb(monkeypatch):
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session

    from app.core.models import Base, IngestionJob, IngestionSchedule, SourceFreshnessSLA
    from app.core.models_queue import JobQueue, WorkerHeartbeat
    from app.core.models_watchdog import WatchdogAlert
    import app.services.data_watchdog as wd

    engine = create_engine(PG_URL)
    tables = [
        IngestionSchedule.__table__, IngestionJob.__table__, JobQueue.__table__,
        WorkerHeartbeat.__table__, SourceFreshnessSLA.__table__, WatchdogAlert.__table__,
    ]
    with engine.begin() as conn:
        for t in reversed(tables):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t.name} CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS raw CASCADE"))
        for stmt in _source_release_ddl():
            conn.execute(text(stmt))
    Base.metadata.create_all(engine, tables=tables)

    # Registered-subscriber fan-out opens its own session on DATABASE_URL.
    monkeypatch.setattr(wd, "_fanout_to_subscribers", lambda *a, **k: _noop())
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("HEARTBEAT_PING_URL", raising=False)

    with Session(engine) as db:
        db.engine_ = engine
        yield db
    engine.dispose()


async def _noop():
    return None


def _schedule(db, **kw):
    from app.core.models import IngestionSchedule, ScheduleFrequency

    row = IngestionSchedule(
        name=kw.pop("name"),
        source=kw.pop("source"),
        config={},
        frequency=kw.pop("frequency", ScheduleFrequency.CUSTOM),
        cron_expression=kw.pop("cron", None),
        is_active=kw.pop("is_active", 1),
        created_at=kw.pop("created_at", NOW - timedelta(days=400)),
        **kw,
    )
    db.add(row)
    db.commit()
    return row


def _ingestion_job(db, source, status, completed_at, schedule_id=None):
    from app.core.models import IngestionJob

    row = IngestionJob(source=source, status=status, config={}, schedule_id=schedule_id,
                       created_at=completed_at - timedelta(minutes=5), completed_at=completed_at)
    db.add(row)
    db.commit()
    return row


def _queue(db, job_type, status, created_at, completed_at=None, payload=None,
           job_table_id=None, worker_id=None):
    from app.core.models_queue import JobQueue

    row = JobQueue(job_type=job_type, status=status, payload=payload or {},
                   created_at=created_at, completed_at=completed_at,
                   job_table_id=job_table_id, worker_id=worker_id)
    db.add(row)
    db.commit()
    return row


def _keys(findings):
    return {f.key for f in findings}


# ---------------------------------------------------------------------------
# /livez /readyz /health
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestProbes:
    def test_livez_is_200_without_db(self):
        """T1: liveness must not depend on the database."""
        def boom():
            raise AssertionError("livez touched the DB")

        client, restore = _health_client(boom)
        try:
            r = client.get("/livez")
        finally:
            restore()
        assert r.status_code == 200
        assert r.json()["status"] == "alive"

    def test_readyz_and_health_503_when_db_down(self):
        """T2: DB down is a 503, and driver text (hosts, users) never leaks."""
        class DeadEngine:
            def connect(self):
                raise RuntimeError("could not connect to host 10.0.0.5 user=nexdata password=hunter2")

        client, restore = _health_client(lambda: DeadEngine())
        try:
            ready = client.get("/readyz")
            health = client.get("/health")
        finally:
            restore()

        assert ready.status_code == 503
        assert health.status_code == 503
        body = health.json()
        assert body["status"] == "unhealthy"
        assert body["database"] == "unreachable"
        assert {"status", "service", "database", "worker"} <= set(body)
        for r in (ready, health):
            assert "hunter2" not in r.text and "10.0.0.5" not in r.text

    def test_readyz_200_when_db_up(self):
        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, *a, **k):
                return None

        class Engine:
            def connect(self):
                return Conn()

        client, restore = _health_client(lambda: Engine())
        try:
            r = client.get("/readyz")
        finally:
            restore()
        assert r.status_code == 200
        assert r.json()["database"] == "connected"


@pg
def test_health_reads_worker_heartbeats(pgdb):
    """T3: worker state comes from worker_heartbeats, statuses are lowercase."""
    from sqlalchemy import text

    import app.api.health as health
    import app.core.job_queue_service as jqs

    engine = pgdb.engine_
    client, restore = _health_client(lambda: engine)
    prior, jqs.WORKER_MODE = jqs.WORKER_MODE, True
    try:
        # No heartbeat at all: the worker is unavailable, and we say so.
        body = client.get("/health").json()
        assert body["worker"] == "unavailable"
        assert body["status"] == "degraded"
        assert body["database"] == "connected"

        # A fresh heartbeat with nothing running: idle.
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO worker_heartbeats (worker_id, hostname, started_at, last_seen_at) "
                "VALUES ('w1', 'h', NOW(), NOW())"))
        now = datetime.utcnow()
        _queue(pgdb, "bulk_ingest", "pending", now)
        _queue(pgdb, "bulk_ingest", "pending", now)
        body = client.get("/health").json()
        assert body["worker"] == "idle"
        assert body["status"] == "healthy"
        assert body["workers_alive"] == 1
        assert body["queue"]["pending"] == 2

        # A claimed job (lowercase, as models_queue stores it): active.
        _queue(pgdb, "bulk_ingest", "running", now, worker_id="w1")
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["worker"] == "active"
        assert r.json()["queue"]["running"] == 1

        # A heartbeat older than 5 minutes is a dead worker.
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE worker_heartbeats SET last_seen_at = NOW() - INTERVAL '20 minutes'"))
        assert client.get("/health").json()["worker"] == "unavailable"
    finally:
        jqs.WORKER_MODE = prior
        restore()
    assert health  # module imported


# ---------------------------------------------------------------------------
# cadence
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_cadence_from_cron_and_frequency():
    """T4: cadence drives the 1.5x stall threshold."""
    from types import SimpleNamespace

    from app.core.models import ScheduleFrequency
    from app.services.data_watchdog import schedule_cadence_hours

    def s(freq, cron=None):
        return SimpleNamespace(frequency=freq, cron_expression=cron)

    assert schedule_cadence_hours(s(ScheduleFrequency.HOURLY)) == 1
    assert schedule_cadence_hours(s(ScheduleFrequency.DAILY)) == 24
    assert schedule_cadence_hours(s(ScheduleFrequency.WEEKLY)) == 168
    assert schedule_cadence_hours(s(ScheduleFrequency.MONTHLY)) == 31 * 24
    assert schedule_cadence_hours(s(ScheduleFrequency.QUARTERLY)) == 92 * 24
    # Monthly cron: the largest gap is a 31-day month.
    assert schedule_cadence_hours(s(ScheduleFrequency.CUSTOM, "0 6 10 * *")) == 31 * 24
    assert schedule_cadence_hours(s(ScheduleFrequency.CUSTOM, "0 6 * * *")) == 24
    # Quarterly cron: the largest gap between Jan/Apr/Jul/Oct fires.
    assert schedule_cadence_hours(s(ScheduleFrequency.CUSTOM, "0 6 2 1,4,7,10 *")) == 92 * 24
    # Unreadable cron: no cadence, no stall check (never a crash).
    assert schedule_cadence_hours(s(ScheduleFrequency.CUSTOM, "not a cron")) is None
    # Plain strings, as raw SQL returns them, work too.
    assert schedule_cadence_hours(s("daily")) == 24


# ---------------------------------------------------------------------------
# rules
# ---------------------------------------------------------------------------

@pg
def test_stalled_schedule_uses_success_only(pgdb):
    """T5: a schedule failing every day is stalled; failed rows are not activity."""
    from app.services.data_watchdog import evaluate

    daily = _schedule(pgdb, name="fred daily", source="fred", cron="0 6 * * *")
    # Last success 3 days ago; failures since then, including one an hour ago.
    _ingestion_job(pgdb, "fred", "success", NOW - timedelta(days=3), daily.id)
    _ingestion_job(pgdb, "fred", "failed", NOW - timedelta(hours=1), daily.id)
    _ingestion_job(pgdb, "fred", "FAILED", NOW - timedelta(hours=2), daily.id)

    # A monthly bulk loader whose IngestionJob never got written back, but whose
    # queue row succeeded 20 days ago: not stalled (threshold 46.5 days).
    bulk = _schedule(pgdb, name="13f", source="bulk:sec_13f", cron="30 9 9 * *")
    ij = _ingestion_job(pgdb, "bulk:sec_13f", "pending", NOW - timedelta(days=20), bulk.id)
    _queue(pgdb, "bulk_ingest", "success", NOW - timedelta(days=20),
           completed_at=NOW - timedelta(days=20), job_table_id=ij.id,
           payload={"bulk_source": "sec_13f"})

    # A monthly loader that only has a loaded release 10 days ago: fine.
    rel = _schedule(pgdb, name="form d", source="bulk:sec_form_d", cron="0 8 8 * *")
    from sqlalchemy import text
    pgdb.execute(text(
        "INSERT INTO raw.source_release (source, release_key, url, status, loaded_at) "
        "VALUES ('sec_form_d', '2026Q3', 'u', 'loaded', :t)"), {"t": NOW - timedelta(days=10)})
    pgdb.commit()

    # A mart schedule that has never succeeded and is older than its threshold.
    never = _schedule(pgdb, name="marts", source="job:pe_mart_build", cron="0 6 10 * *")
    _queue(pgdb, "pe_mart_build", "failed", NOW - timedelta(days=5),
           completed_at=NOW - timedelta(days=5))

    # Brand-new schedule, never ran, still inside its first cadence: no alert.
    _schedule(pgdb, name="new", source="eia", cron="0 6 * * *",
              created_at=NOW - timedelta(hours=3))
    # Paused schedules are not watched.
    _schedule(pgdb, name="paused", source="bls", cron="0 6 * * *", is_active=0)

    findings = {f.key: f for f in evaluate(pgdb, NOW) if f.rule == "stalled_schedule"}
    assert set(findings) == {f"schedule:stalled:{daily.id}", f"schedule:stalled:{never.id}"}
    assert findings[f"schedule:stalled:{never.id}"].details["last_success_at"] is None
    assert findings[f"schedule:stalled:{daily.id}"].severity == "critical"
    assert rel.id and bulk.id


@pg
def test_dead_worker_with_pending_jobs(pgdb):
    """T6: no heartbeat is only an alert when work is waiting for a worker."""
    from sqlalchemy import text

    from app.services.data_watchdog import evaluate

    assert "worker:none_alive" not in _keys(evaluate(pgdb, NOW))

    # Pending for 2 minutes: not yet (a worker may just be slow to claim).
    _queue(pgdb, "bulk_ingest", "pending", NOW - timedelta(minutes=2))
    assert "worker:none_alive" not in _keys(evaluate(pgdb, NOW))

    _queue(pgdb, "bulk_ingest", "pending", NOW - timedelta(minutes=45))
    found = [f for f in evaluate(pgdb, NOW) if f.key == "worker:none_alive"]
    assert found and found[0].severity == "critical"
    assert found[0].details["pending_jobs"] == 2

    # A stale heartbeat does not count...
    pgdb.execute(text(
        "INSERT INTO worker_heartbeats (worker_id, started_at, last_seen_at) "
        "VALUES ('w1', :t, :t)"), {"t": NOW - timedelta(hours=3)})
    pgdb.commit()
    assert "worker:none_alive" in _keys(evaluate(pgdb, NOW))
    # ...a live one does.
    pgdb.execute(text("UPDATE worker_heartbeats SET last_seen_at = :t"),
                 {"t": NOW - timedelta(minutes=1)})
    pgdb.commit()
    assert "worker:none_alive" not in _keys(evaluate(pgdb, NOW))


@pg
def test_failed_release_alert(pgdb):
    """T7: one alert per source for releases that failed in the last 24h."""
    from sqlalchemy import text

    from app.services.data_watchdog import evaluate

    rows = [
        ("sec_13f", "2026Q2", "failed", NOW - timedelta(hours=2)),
        ("sec_13f", "2026Q3", "failed", NOW - timedelta(hours=1)),
        ("sec_form_d", "2026Q1", "failed", NOW - timedelta(days=3)),   # too old
        ("sec_insider", "2026Q3", "loaded", NOW - timedelta(hours=1)),
    ]
    for source, key, status, ts in rows:
        pgdb.execute(text(
            "INSERT INTO raw.source_release (source, release_key, url, status, error, updated_at) "
            "VALUES (:s, :k, 'u', :st, 'HTTP 503 from sec.gov', :t)"),
            {"s": source, "k": key, "st": status, "t": ts})
    pgdb.commit()

    found = {f.key: f for f in evaluate(pgdb, NOW) if f.rule == "failed_release"}
    assert set(found) == {"release:failed:sec_13f"}
    assert sorted(found["release:failed:sec_13f"].details["release_keys"]) == ["2026Q2", "2026Q3"]


@pg
def test_failure_spike(pgdb, monkeypatch):
    """T8: many failed queue jobs in the last hour is an alert."""
    from app.services.data_watchdog import evaluate

    monkeypatch.setenv("WATCHDOG_FAILURE_SPIKE", "3")
    for i in range(2):
        _queue(pgdb, "site_intel", "failed", NOW - timedelta(minutes=30),
               completed_at=NOW - timedelta(minutes=30 - i))
    _queue(pgdb, "site_intel", "failed", NOW - timedelta(hours=5),
           completed_at=NOW - timedelta(hours=5))      # outside the window
    assert "queue:failure_spike" not in _keys(evaluate(pgdb, NOW))

    _queue(pgdb, "bulk_ingest", "failed", NOW - timedelta(minutes=10),
           completed_at=NOW - timedelta(minutes=5))
    found = [f for f in evaluate(pgdb, NOW) if f.key == "queue:failure_spike"]
    assert found and found[0].details["failed_last_hour"] == 3
    assert found[0].details["by_job_type"] == {"site_intel": 2, "bulk_ingest": 1}


@pg
def test_sla_violation_and_never_succeeded(pgdb):
    """T9: SLA rows are checked on a timer now (check_freshness_violations had no caller)."""
    from app.core.models import SourceFreshnessSLA
    from app.services.data_watchdog import evaluate

    pgdb.add_all([
        SourceFreshnessSLA(source="fred", max_age_hours=24, alert_on_violation=1),
        SourceFreshnessSLA(source="eia", max_age_hours=24, alert_on_violation=1),
        SourceFreshnessSLA(source="bls", max_age_hours=24, alert_on_violation=1),
        SourceFreshnessSLA(source="bea", max_age_hours=1, alert_on_violation=0),
    ])
    pgdb.commit()
    _ingestion_job(pgdb, "fred", "success", NOW - timedelta(hours=48))
    _ingestion_job(pgdb, "fred", "failed", NOW - timedelta(hours=1))   # not activity
    _ingestion_job(pgdb, "eia", "success", NOW - timedelta(hours=2))
    _ingestion_job(pgdb, "bls", "failed", NOW - timedelta(hours=2))    # never succeeded
    _ingestion_job(pgdb, "bea", "success", NOW - timedelta(hours=48))  # alerts disabled

    found = {f.key: f for f in evaluate(pgdb, NOW) if f.rule == "sla"}
    assert set(found) == {"sla:fred", "sla:bls"}
    assert found["sla:bls"].severity == "critical"
    assert found["sla:fred"].details["age_hours"] == 48.0

    import app.api.v1.freshness as freshness
    assert not hasattr(freshness, "check_freshness_violations")


# ---------------------------------------------------------------------------
# dedupe, reminders, resolve, delivery
# ---------------------------------------------------------------------------

@pg
def test_dedupe_remind_and_resolve(pgdb, monkeypatch):
    """T10: an alert fires once, reminds after 24h, and says when it is resolved."""
    from app.core.models_watchdog import WatchdogAlert
    from app.services.data_watchdog import run_watchdog

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example/abc")
    rec = Recorder()
    _queue(pgdb, "bulk_ingest", "pending", NOW - timedelta(hours=1))

    def run(at):
        return asyncio.run(run_watchdog(pgdb, now=at, transport=rec.transport))

    first = run(NOW)
    assert "worker:none_alive" in first["opened"]
    assert len(rec.posts()) == 1
    assert "worker:none_alive" in rec.posts()[0]["text"] or "No live worker" in rec.posts()[0]["text"]

    # 15 minutes later, still broken: nothing new to say.
    second = run(NOW + timedelta(minutes=15))
    assert second["opened"] == [] and second["reminded"] == []
    assert len(rec.posts()) == 1

    # A day later, still broken: one reminder.
    third = run(NOW + timedelta(hours=24, minutes=5))
    assert third["reminded"] == ["worker:none_alive"]
    assert len(rec.posts()) == 2

    # The job is picked up: resolved notice, row kept as resolved.
    from app.core.models_queue import JobQueue
    pgdb.query(JobQueue).update({"status": "success"})
    pgdb.commit()
    fourth = run(NOW + timedelta(hours=25))
    assert fourth["resolved"] == ["worker:none_alive"]
    assert len(rec.posts()) == 3
    assert "RESOLVED" in rec.posts()[-1]["text"]
    row = pgdb.get(WatchdogAlert, "worker:none_alive")
    assert row.status == "resolved" and row.resolved_at is not None
    assert row.notify_count == 2

    # Resolved alerts stay quiet.
    run(NOW + timedelta(hours=26))
    assert len(rec.posts()) == 3

    # Breaks again: it re-opens and fires again.
    _queue(pgdb, "bulk_ingest", "pending", NOW + timedelta(hours=26))
    again = run(NOW + timedelta(hours=27))
    assert again["opened"] == ["worker:none_alive"]
    assert len(rec.posts()) == 4


@pg
def test_failed_delivery_is_retried(pgdb, monkeypatch):
    """T11: if Slack is down, the alert is not marked sent and goes out next run."""
    from app.core.models_watchdog import WatchdogAlert
    from app.services.data_watchdog import run_watchdog

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example/abc")
    _queue(pgdb, "bulk_ingest", "pending", NOW - timedelta(hours=1))

    down = Recorder(status=500)
    r1 = asyncio.run(run_watchdog(pgdb, now=NOW, transport=down.transport))
    assert r1["delivered"] is False
    assert pgdb.get(WatchdogAlert, "worker:none_alive").last_notified_at is None

    up = Recorder()
    r2 = asyncio.run(run_watchdog(pgdb, now=NOW + timedelta(minutes=15), transport=up.transport))
    assert r2["delivered"] is True
    assert "worker:none_alive" in r2["reminded"] + r2["opened"]
    assert len(up.posts()) == 1
    assert pgdb.get(WatchdogAlert, "worker:none_alive").last_notified_at is not None


@pytest.mark.unit
def test_webhook_payload_is_slack_text(monkeypatch):
    """T12: one Slack-compatible {"text": ...} POST, with a timeout."""
    from app.services.data_watchdog import Finding, deliver

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example/abc")
    rec = Recorder()
    changes = [
        ("opened", Finding(key="schedule:stalled:7", rule="stalled_schedule", severity="critical",
                           message="Schedule 'fred daily' has not succeeded in 72.0h")),
        ("resolved", Finding(key="release:failed:sec_13f", rule="failed_release", severity="warning",
                             message="sec_13f releases failed")),
    ]
    ok = asyncio.run(deliver(changes, transport=rec.transport))
    assert ok is True
    assert len(rec.requests) == 1
    req = rec.requests[0]
    assert str(req.url) == "https://hooks.example/abc"
    assert req.headers["content-type"].startswith("application/json")
    body = json.loads(req.content)
    assert set(body) == {"text"}
    assert "CRITICAL" in body["text"] and "fred daily" in body["text"]
    assert "RESOLVED" in body["text"] and "sec_13f" in body["text"]

    # Unset: no request, still counts as delivered (the log is the delivery).
    monkeypatch.delenv("ALERT_WEBHOOK_URL")
    rec2 = Recorder()
    assert asyncio.run(deliver(changes, transport=rec2.transport)) is True
    assert rec2.requests == []


@pytest.mark.unit
def test_webhook_failure_is_logged_not_raised(monkeypatch, caplog):
    from app.services.data_watchdog import Finding, deliver

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example/abc")

    def boom(request):
        raise httpx.ConnectError("refused")

    changes = [("opened", Finding(key="k", rule="r", severity="critical", message="m"))]
    with caplog.at_level("WARNING"):
        ok = asyncio.run(deliver(changes, transport=httpx.MockTransport(boom)))
    assert ok is False
    # Every alert is logged regardless of webhook outcome.
    assert any("m" in r.getMessage() and r.levelname == "ERROR" for r in caplog.records)


@pytest.mark.unit
def test_heartbeat_ping_and_noop_when_unset(monkeypatch):
    """T13: the dead-man's switch is pinged on each completed run; unset = no-op."""
    from app.services.data_watchdog import ping_heartbeat

    monkeypatch.delenv("HEARTBEAT_PING_URL", raising=False)
    rec = Recorder()
    assert asyncio.run(ping_heartbeat(transport=rec.transport)) is False
    assert rec.requests == []

    monkeypatch.setenv("HEARTBEAT_PING_URL", "https://hc-ping.example/uuid")
    assert asyncio.run(ping_heartbeat(transport=rec.transport)) is True
    assert str(rec.requests[0].url) == "https://hc-ping.example/uuid"


@pg
def test_run_pings_heartbeat_and_status_lists_open(pgdb, monkeypatch):
    from app.services.data_watchdog import open_alerts, run_watchdog

    monkeypatch.setenv("HEARTBEAT_PING_URL", "https://hc-ping.example/uuid")
    rec = Recorder()
    _queue(pgdb, "bulk_ingest", "pending", NOW - timedelta(hours=1))
    result = asyncio.run(run_watchdog(pgdb, now=NOW, transport=rec.transport))
    assert result["pinged"] is True
    assert [r.method for r in rec.requests] == ["GET"]   # no webhook configured
    keys = [a["key"] for a in open_alerts(pgdb)]
    assert keys == ["worker:none_alive"]


@pg
def test_broken_rule_does_not_hide_others(pgdb, monkeypatch):
    """A crashing rule becomes its own alert instead of silencing the watchdog."""
    import app.services.data_watchdog as wd

    def broken(db, now):
        raise RuntimeError("boom")

    monkeypatch.setattr(wd, "RULES", [("broken", broken)] + list(wd.RULES))
    _queue(pgdb, "bulk_ingest", "pending", NOW - timedelta(hours=1))
    keys = _keys(wd.evaluate(pgdb, NOW))
    assert "watchdog:rule_error:broken" in keys
    assert "worker:none_alive" in keys


@pytest.mark.unit
def test_watchdog_registers_on_scheduler():
    from app.services.data_watchdog import register_watchdog_job

    class FakeScheduler:
        def __init__(self):
            self.jobs = {}

        def add_job(self, func, trigger, id, **kw):
            self.jobs[id] = (func, trigger, kw)

    sched = FakeScheduler()
    assert register_watchdog_job(interval_minutes=15, scheduler=sched) is True
    func, trigger, kw = sched.jobs["system_data_watchdog"]
    assert kw["max_instances"] == 1 and kw["coalesce"] is True
    assert trigger.interval == timedelta(minutes=15)


# ---------------------------------------------------------------------------
# honest pull surfaces
# ---------------------------------------------------------------------------

@pg
def test_monitoring_staleness_success_only(pgdb):
    """T14: failed jobs are not activity; never succeeded is critical."""
    from app.core.monitoring import JobMonitor

    now = datetime.utcnow()
    _schedule(pgdb, name="fred daily", source="fred", cron="0 6 * * *")
    _ingestion_job(pgdb, "fred", "success", now - timedelta(days=5))
    _ingestion_job(pgdb, "fred", "failed", now - timedelta(minutes=30))
    _ingestion_job(pgdb, "bls", "failed", now - timedelta(minutes=30))
    _ingestion_job(pgdb, "eia", "success", now - timedelta(hours=1))

    alerts = {a["source"]: a for a in JobMonitor(pgdb).check_alerts()
              if a["alert_type"] == "data_staleness"}
    assert alerts["fred"]["severity"] == "critical"          # 5d > 1.5 x 24h
    assert alerts["fred"]["last_success_at"] is not None
    assert alerts["bls"]["severity"] == "critical"           # never succeeded
    assert alerts["bls"]["last_success_at"] is None
    assert "eia" not in alerts


@pytest.mark.unit
def test_freshness_unknown_and_never_run():
    """T15: no SLA and no schedule is 'unknown', not 'fresh'; never-run is listed."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from app.api.v1.freshness import get_freshness_dashboard
    from app.core.models import ScheduleFrequency

    now = datetime.utcnow()
    calls = [0]

    def query(*args):
        calls[0] += 1
        q = MagicMock()
        if calls[0] == 1:   # last success per source
            q.filter.return_value.group_by.return_value.all.return_value = [
                SimpleNamespace(source="adhoc", last_success=now - timedelta(hours=500)),
                SimpleNamespace(source="fred", last_success=now - timedelta(hours=2)),
            ]
        elif calls[0] == 2:  # active schedules
            q.filter.return_value.all.return_value = [
                SimpleNamespace(source="fred", frequency=ScheduleFrequency.DAILY, cron_expression=None),
                SimpleNamespace(source="bulk:sec_13f", frequency=ScheduleFrequency.CUSTOM,
                                cron_expression="30 9 9 * *"),
            ]
        elif calls[0] == 3:  # SLAs
            q.all.return_value = []
        elif calls[0] == 4:  # every source that ever had a job
            q.distinct.return_value.all.return_value = [
                SimpleNamespace(source="adhoc"), SimpleNamespace(source="fred"),
                SimpleNamespace(source="bls"),
            ]
        return q

    db = MagicMock()
    db.query.side_effect = query
    result = get_freshness_dashboard(db=db)
    by = {s["source"]: s for s in result["sources"]}

    assert by["adhoc"]["freshness"] == "unknown"
    assert by["adhoc"]["is_stale"] is False
    assert by["fred"]["freshness"] == "fresh"
    # Scheduled but never succeeded, and attempted but never succeeded.
    for src in ("bulk:sec_13f", "bls"):
        assert by[src]["freshness"] == "never_succeeded"
        assert by[src]["last_success_at"] is None
        assert by[src]["is_stale"] is True
    # Monthly cron: cadence comes from the cron, not the 48h CUSTOM fallback.
    assert by["bulk:sec_13f"]["expected_cadence_hours"] == 31 * 24 * 1.5
    assert result["unknown_count"] == 1
    assert result["fresh_count"] == 1
    assert result["stale_count"] == 2
    assert result["total_sources"] == 4
