"""
Tests for SPEC 160 — runtime split: one scheduler, by flag and by leader lock.

Every API process started APScheduler and re-registered all its jobs into the
shared ``apscheduler_jobs`` store, so a second API process (laptop + cloud, or
two Cloud Run instances) fired every schedule twice, and every API start failed
any job RUNNING for more than 2 h. RUN_SCHEDULER turns the scheduler off per
process; a Postgres advisory lock makes sure that even with it on in two
places only one process runs it.
"""
import ast
import asyncio
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]

# Each PG test takes its own key, so a lock leaked by one test (or a real
# scheduler on the same server) can never make another one flaky.
_KEYS = iter(range(916_016_000, 916_016_999))


def _memory_scheduler():
    from apscheduler.jobstores.memory import MemoryJobStore

    from app.core import scheduler_service

    return scheduler_service._build_scheduler({"default": MemoryJobStore()})


@pytest.fixture
def runtime(monkeypatch):
    """A fresh in-memory scheduler and clean leadership state for one test."""
    from app.core import scheduler_leader, scheduler_service

    sched = _memory_scheduler()
    monkeypatch.setattr(scheduler_service, "_scheduler", sched)
    monkeypatch.setattr(scheduler_leader, "_leadership", None)
    monkeypatch.setattr(scheduler_leader, "_enabled", None)
    yield sched
    if sched.running:
        sched.shutdown(wait=False)


class FakeLock:
    """Stands in for LeaderLock: `free` decides whether try_acquire wins."""

    def __init__(self, free=True):
        self.free = free
        self.held = False
        self.attempts = 0
        self.released = False

    def try_acquire(self):
        self.attempts += 1
        if self.free:
            self.held = True
        return self.held

    def is_held(self):
        return self.held

    def release(self):
        self.released = True
        self.held = False

    def holder(self):
        return {"pid": 4242, "application_name": "nexdata-scheduler-leader"}


def _register_one():
    """A stand-in for main._register_scheduled_jobs."""
    from app.core.scheduler_service import get_scheduler

    calls.append("register")
    get_scheduler().add_job(_noop, "interval", minutes=5, id="t160_job", replace_existing=True)


calls = []


def _noop():
    pass


# ---------------------------------------------------------------------------
# Unit — no database
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFlagOff:
    def test_flag_off_registers_nothing(self, runtime, monkeypatch):
        """T1: RUN_SCHEDULER=0 -> no jobs, no lock attempt, no stale resolver,
        and the scheduler is up only in paused mode (job-store writes only)."""
        from apscheduler.schedulers.base import STATE_PAUSED

        from app.core import scheduler_leader

        resolved = []
        monkeypatch.setattr(scheduler_leader, "resolve_stale_running_jobs",
                            lambda *a, **k: resolved.append(1) or 0)
        lock = FakeLock()
        calls.clear()

        async def go():
            status = await scheduler_leader.start_scheduler_runtime(
                _register_one, enabled=False, lock=lock)
            jobs = runtime.get_jobs()
            state = runtime.state
            await scheduler_leader.stop_scheduler_runtime()
            return status, jobs, state

        status, jobs, state = asyncio.run(go())
        assert status is None
        assert calls == []
        assert jobs == []
        assert lock.attempts == 0
        assert resolved == []
        assert state == STATE_PAUSED
        assert scheduler_leader.leader_status() is None
        assert scheduler_leader.may_run_jobs() is False

    def test_start_endpoint_cannot_unpause_a_non_leader(self, runtime):
        """T2: POST /schedules/start calls start_scheduler(); off-leader it
        must leave the scheduler paused."""
        from apscheduler.schedulers.base import STATE_PAUSED

        from app.core import scheduler_leader, scheduler_service

        async def go():
            await scheduler_leader.start_scheduler_runtime(_register_one, enabled=False, lock=FakeLock())
            scheduler_service.start_scheduler()
            state = runtime.state
            status = scheduler_service.get_scheduler_status()
            await scheduler_leader.stop_scheduler_runtime()
            return state, status

        state, status = asyncio.run(go())
        assert state == STATE_PAUSED
        assert status["paused"] is True
        assert status["scheduler_leader"] is None

    def test_standby_registers_nothing_until_elected(self, runtime, monkeypatch):
        """T3: flag on but the lock is held elsewhere -> standby: nothing is
        registered, the stale resolver does not run; once the lock frees, the
        retry elects this process, which resolves, registers and resumes."""
        from apscheduler.schedulers.base import STATE_PAUSED, STATE_RUNNING

        from app.core import scheduler_leader

        resolved = []
        monkeypatch.setattr(scheduler_leader, "resolve_stale_running_jobs",
                            lambda *a, **k: resolved.append(1) or 0)
        lock = FakeLock(free=False)
        calls.clear()

        async def go():
            status = await scheduler_leader.start_scheduler_runtime(
                _register_one, enabled=True, lock=lock, retry_seconds=0.05, check_seconds=0.05)
            before = (status, list(calls), list(resolved), runtime.state, runtime.get_jobs())
            lock.free = True
            for _ in range(100):
                await asyncio.sleep(0.02)
                if scheduler_leader.leader_status():
                    break
            after = (scheduler_leader.leader_status(), list(calls), list(resolved), runtime.state,
                     [j.id for j in runtime.get_jobs()])
            await scheduler_leader.stop_scheduler_runtime()
            return before, after

        before, after = asyncio.run(go())
        assert before[0] is False
        assert before[1] == [] and before[2] == []
        assert before[3] == STATE_PAUSED
        assert before[4] == []
        assert after[0] is True
        assert after[1] == ["register"] and after[2] == [1]
        assert after[3] == STATE_RUNNING
        assert after[4] == ["t160_job"]
        assert lock.released

    def test_leader_pauses_when_lock_is_lost(self, runtime, monkeypatch):
        """T4: a leader whose lock connection drops pauses its scheduler and
        reports false; it does not keep firing jobs."""
        from apscheduler.schedulers.base import STATE_PAUSED, STATE_RUNNING

        from app.core import scheduler_leader

        monkeypatch.setattr(scheduler_leader, "resolve_stale_running_jobs", lambda *a, **k: 0)
        lock = FakeLock(free=True)

        async def go():
            await scheduler_leader.start_scheduler_runtime(
                _register_one, enabled=True, lock=lock, retry_seconds=10, check_seconds=0.05)
            elected = (scheduler_leader.leader_status(), runtime.state)
            lock.held = False  # the server dropped our session
            lock.free = False  # ...and someone else took the lock
            for _ in range(100):
                await asyncio.sleep(0.02)
                if scheduler_leader.leader_status() is False:
                    break
            lost = (scheduler_leader.leader_status(), runtime.state)
            await scheduler_leader.stop_scheduler_runtime()
            return elected, lost

        elected, lost = asyncio.run(go())
        assert elected == (True, STATE_RUNNING)
        assert lost == (False, STATE_PAUSED)

    def test_shutdown_stops_scheduler_before_releasing(self, runtime, monkeypatch):
        """T5: the next leader must not overlap with this one's scheduler."""
        from app.core import scheduler_leader

        monkeypatch.setattr(scheduler_leader, "resolve_stale_running_jobs", lambda *a, **k: 0)
        order = []

        class OrderLock(FakeLock):
            def release(self):
                order.append(("release", runtime.running))
                super().release()

        async def go():
            await scheduler_leader.start_scheduler_runtime(
                _register_one, enabled=True, lock=OrderLock(), check_seconds=10)
            await scheduler_leader.stop_scheduler_runtime()

        asyncio.run(go())
        assert order == [("release", False)]
        assert scheduler_leader.leader_status() is None

    def test_no_runtime_keeps_old_behaviour(self, runtime, monkeypatch):
        """T6: scripts and tests that never start the runtime (flag default
        true) may still start the scheduler, as before."""
        from app.core import scheduler_leader
        from app.core.config import get_settings

        monkeypatch.setattr(get_settings(), "run_scheduler", True)
        assert scheduler_leader.leader_status() is None
        assert scheduler_leader.may_run_jobs() is True
        monkeypatch.setattr(get_settings(), "run_scheduler", False)
        assert scheduler_leader.may_run_jobs() is False


@pytest.mark.unit
class TestSettings:
    def test_defaults(self, monkeypatch):
        """T7: defaults keep today's behaviour on the laptop."""
        from app.core.config import Settings

        for k in ("RUN_SCHEDULER", "DB_POOL_SIZE", "DB_MAX_OVERFLOW", "DB_POOL_RECYCLE",
                  "SCHEDULER_LEADER_RETRY_SECONDS", "SCHEDULER_LEADER_CHECK_SECONDS"):
            monkeypatch.delenv(k, raising=False)
        s = Settings(database_url="postgresql://x:x@h/db", _env_file=None)
        assert s.run_scheduler is True
        assert (s.db_pool_size, s.db_max_overflow, s.db_pool_recycle) == (5, 10, -1)
        assert s.scheduler_leader_retry_seconds > s.scheduler_leader_check_seconds

    def test_env_parsing(self, monkeypatch):
        from app.core.config import Settings

        monkeypatch.setenv("RUN_SCHEDULER", "0")
        monkeypatch.setenv("DB_POOL_SIZE", "3")
        monkeypatch.setenv("DB_MAX_OVERFLOW", "4")
        monkeypatch.setenv("DB_POOL_RECYCLE", "1800")
        s = Settings(database_url="postgresql://x:x@h/db", _env_file=None)
        assert s.run_scheduler is False
        assert (s.db_pool_size, s.db_max_overflow, s.db_pool_recycle) == (3, 4, 1800)

    def test_engine_uses_pool_settings(self, monkeypatch):
        """T8: DB_POOL_SIZE / DB_MAX_OVERFLOW / DB_POOL_RECYCLE / application
        name reach the shared engine."""
        import app.core.database as database
        from app.core.config import get_settings

        s = get_settings()
        monkeypatch.setattr(s, "database_url", "postgresql://x:x@127.0.0.1:1/x")
        monkeypatch.setattr(s, "db_pool_size", 3)
        monkeypatch.setattr(s, "db_max_overflow", 4)
        monkeypatch.setattr(s, "db_pool_recycle", 1800)
        monkeypatch.setattr(s, "db_application_name", None)
        monkeypatch.setattr(database, "_engine", None)
        monkeypatch.setattr(database, "_process_role", "nexdata-worker")
        captured = {}

        def fake_create_engine(url, **kw):
            captured.update(kw, url=url)
            return object()

        monkeypatch.setattr(database, "create_engine", fake_create_engine)
        database.get_engine()
        assert captured["pool_size"] == 3
        assert captured["max_overflow"] == 4
        assert captured["pool_recycle"] == 1800
        assert captured["connect_args"]["application_name"] == "nexdata-worker"


@pytest.mark.unit
class TestHealth:
    def _client(self, monkeypatch, status):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import app.api.health as health
        from app.core import scheduler_leader

        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, *a, **k):
                raise RuntimeError("no db in this test")

        class Engine:
            def connect(self):
                return Conn()

        monkeypatch.setattr(health, "get_engine", lambda: Engine())
        monkeypatch.setattr(scheduler_leader, "leader_status", lambda: status)
        app = FastAPI()
        app.include_router(health.router)
        return TestClient(app)

    @pytest.mark.parametrize("status", [True, False, None])
    def test_health_exposes_scheduler_leader(self, monkeypatch, status):
        """T9: /health says whether this process runs the scheduler, even
        when the database is down."""
        body = self._client(monkeypatch, status).get("/health").json()
        assert "scheduler_leader" in body
        assert body["scheduler_leader"] is status


@pytest.mark.unit
class TestMainWiring:
    def test_every_registration_is_inside_register_scheduled_jobs(self):
        """T10: nothing in main.py registers or starts APScheduler jobs outside
        _register_scheduled_jobs, which only the elected leader calls."""
        tree = ast.parse((REPO / "app" / "main.py").read_text(encoding="utf-8"))
        register_fn = next(
            n for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "_register_scheduled_jobs"
        )
        inside = {id(n) for n in ast.walk(register_fn)}
        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or id(node) in inside:
                continue
            name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            if name in {"add_job", "start_scheduler", "load_all_schedules",
                        "install_default_bulk_schedules"} or (name or "").startswith("register_"):
                offenders.append((name, node.lineno))
        assert offenders == []

    def test_lifespan_uses_runtime_and_has_no_inline_stale_resolver(self):
        src = (REPO / "app" / "main.py").read_text(encoding="utf-8")
        assert "start_scheduler_runtime(" in src
        assert "stop_scheduler_runtime(" in src
        assert "Stale job auto-resolved on startup" not in src


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


@pg
class TestLeaderLockPG:
    def test_two_processes_one_leader(self):
        """T11: two lock connections -> exactly one leader; release hands over."""
        from app.core.scheduler_leader import LeaderLock

        key = next(_KEYS)
        a, b = LeaderLock(PG_URL, key=key), LeaderLock(PG_URL, key=key)
        try:
            assert a.try_acquire() is True
            assert b.try_acquire() is False
            assert a.is_held() is True
            assert b.is_held() is False
            assert a.try_acquire() is True  # idempotent for the holder
            holder = b.holder()
            assert holder and holder["application_name"] == "nexdata-scheduler-leader"
            a.release()
            assert a.is_held() is False
            assert b.try_acquire() is True
        finally:
            a.release()
            b.release()

    def test_connection_drop_loses_leadership(self):
        """T12: when the leader's lock connection dies, the server releases
        the lock, the leader sees it, and a standby can take over."""
        from sqlalchemy import create_engine, text

        from app.core.scheduler_leader import LeaderLock

        key = next(_KEYS)
        a, b = LeaderLock(PG_URL, key=key), LeaderLock(PG_URL, key=key)
        admin = create_engine(PG_URL)
        try:
            assert a.try_acquire()
            pid = a.backend_pid
            assert pid
            with admin.connect() as c:
                c.execute(text("SELECT pg_terminate_backend(:p)"), {"p": pid})
            for _ in range(50):
                with admin.connect() as c:
                    alive = c.execute(text("SELECT count(*) FROM pg_stat_activity WHERE pid=:p"),
                                      {"p": pid}).scalar()
                if not alive:
                    break
            assert a.is_held() is False
            assert b.try_acquire() is True
            assert a.try_acquire() is False
        finally:
            a.release()
            b.release()
            admin.dispose()

    def test_lock_session_is_never_idle_in_transaction(self):
        """T13: the lock connection is AUTOCOMMIT: holding the lock for hours
        must not hold a transaction (snapshot) open."""
        from sqlalchemy import create_engine, text

        from app.core.scheduler_leader import LeaderLock

        key = next(_KEYS)
        a = LeaderLock(PG_URL, key=key)
        admin = create_engine(PG_URL)
        try:
            assert a.try_acquire()
            assert a.is_held()
            with admin.connect() as c:
                state = c.execute(text("SELECT state FROM pg_stat_activity WHERE pid=:p"),
                                  {"p": a.backend_pid}).scalar()
            assert state == "idle"
        finally:
            a.release()
            admin.dispose()

    def test_two_async_leaderships_one_elected(self):
        """T14: two SchedulerLeadership loops (two API processes) -> one runs
        on_elected; when it stops, the other takes over on its next retry."""
        from app.core.scheduler_leader import LeaderLock, SchedulerLeadership

        key = next(_KEYS)
        events = []

        async def go():
            a = SchedulerLeadership(LeaderLock(PG_URL, key=key),
                                    on_elected=lambda: events.append("a"),
                                    retry_seconds=0.05, check_seconds=0.05)
            b = SchedulerLeadership(LeaderLock(PG_URL, key=key),
                                    on_elected=lambda: events.append("b"),
                                    retry_seconds=0.05, check_seconds=0.05)
            await a.start()
            await b.start()
            await asyncio.sleep(0.3)
            first = (a.is_leader, b.is_leader, list(events))
            await a.stop()
            for _ in range(100):
                await asyncio.sleep(0.02)
                if b.is_leader:
                    break
            second = (a.is_leader, b.is_leader, list(events))
            await b.stop()
            return first, second

        first, second = asyncio.run(go())
        assert first == (True, False, ["a"])
        assert second == (False, True, ["a", "b"])


@pytest.fixture
def jobsdb():
    from sqlalchemy import create_engine, text

    from app.core.models import Base, IngestionJob, IngestionSchedule
    from app.core.models_queue import JobEvent, JobQueue

    engine = create_engine(PG_URL)
    tables = [IngestionSchedule.__table__, IngestionJob.__table__, JobQueue.__table__,
              JobEvent.__table__]
    with engine.begin() as conn:
        for t in reversed(tables):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t.name} CASCADE"))
    Base.metadata.create_all(engine, tables=tables)
    yield engine
    engine.dispose()


def _ing(engine, status, started_at):
    from sqlalchemy import text

    with engine.begin() as conn:
        return conn.execute(text("""
            INSERT INTO ingestion_jobs (source, status, config, created_at, started_at,
                                        retry_count, max_retries, data_origin)
            VALUES ('cms', :st, '{}', :sa, :sa, 0, 3, 'real') RETURNING id
        """), {"st": status, "sa": started_at}).scalar()


def _queue(engine, ing_id, status):
    import json

    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO job_queue (job_type, job_table_id, status, priority, payload, created_at)
            VALUES ('ingestion', :i, :st, 0, CAST(:p AS json), NOW())
        """), {"i": ing_id, "st": status, "p": json.dumps({"ingestion_job_id": ing_id})})


@pg
def test_stale_resolver_skips_jobs_a_worker_is_running(jobsdb):
    """T15: the resolver fails RUNNING rows > 2 h old that nothing is running,
    and leaves alone rows whose queue job is still live (a 4 h CMS load)."""
    from sqlalchemy import text

    from app.core.scheduler_leader import resolve_stale_running_jobs

    old = datetime.utcnow() - timedelta(hours=5)
    orphan = _ing(jobsdb, "running", old)                  # in-process run, API died
    live = _ing(jobsdb, "running", old)
    _queue(jobsdb, live, "running")                        # worker still on it
    done = _ing(jobsdb, "running", old)
    _queue(jobsdb, done, "failed")                         # worker finished
    young = _ing(jobsdb, "running", datetime.utcnow() - timedelta(minutes=30))

    assert resolve_stale_running_jobs(jobsdb) == 2
    with jobsdb.connect() as c:
        status = dict(c.execute(text("SELECT id, status FROM ingestion_jobs")).all())
    assert status[orphan] == "failed"
    assert status[done] == "failed"
    assert status[live] == "running"
    assert status[young] == "running"


@pg
def test_worker_holds_no_transaction_while_executor_runs(jobsdb, monkeypatch):
    """T16: the worker used to re-SELECT the expired JobQueue row after its
    RUNNING commit and keep that transaction open ("idle in transaction")
    across the rate-limit wait and the whole executor run."""
    import json

    from sqlalchemy import text
    from sqlalchemy.orm import sessionmaker

    import app.worker.main as wm
    from app.core.models_queue import JobQueue, QueueJobType

    factory = sessionmaker(bind=jobsdb)
    monkeypatch.setattr(wm, "get_session_factory", lambda: factory)
    with jobsdb.begin() as conn:
        qid = conn.execute(text("""
            INSERT INTO job_queue (job_type, status, priority, payload, created_at)
            VALUES ('pe_mart_build', 'claimed', 0, CAST(:p AS json), NOW()) RETURNING id
        """), {"p": json.dumps({"x": 1})}).scalar()

    seen = {}

    async def executor(job, db):
        seen["before"] = db.in_transaction()
        seen["payload"] = job.payload
        seen["after_read"] = db.in_transaction()

    monkeypatch.setitem(wm.EXECUTORS, QueueJobType.PE_MART_BUILD, executor)
    db = factory()
    try:
        job = db.get(JobQueue, qid)
        asyncio.run(wm.execute_job(job, db))
    finally:
        db.close()
    assert seen == {"before": False, "payload": {"x": 1}, "after_read": False}
    with jobsdb.connect() as c:
        assert c.execute(text("SELECT status FROM job_queue WHERE id=:i"), {"i": qid}).scalar() == "success"
