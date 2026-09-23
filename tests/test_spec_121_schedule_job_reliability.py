"""
Tests for SPEC 121 — schedule and job reliability.

Every `job:<type>` schedule ran once and then froze: the worker updated only
job_queue, the IngestionJob it was linked to stayed PENDING forever, and
run_scheduled_job skips any schedule with a PENDING job. Row 3891
(`job:pe_mart_build`) was exactly that zombie, one run away from silently
skipping the 10-10 mart build.
"""
import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Unit — no database
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLinkRule:
    def test_link_rule(self):
        """T1: job_table_id alone is ambiguous — agentic and LP rows point at
        their own tables. Only a payload that names the ingestion job links."""
        from app.core.ingestion_job_sync import linked_ingestion_job_id

        # scheduler job:/bulk:, ingestion, batch, backfill
        assert linked_ingestion_job_id(3891, {"ingestion_job_id": 3891}) == 3891
        # job_splitter can submit without a job_table_id
        assert linked_ingestion_job_id(None, {"ingestion_job_id": 7}) == 7
        # agentic_research: job_table_id -> portfolio job table
        assert linked_ingestion_job_id(12, {"job_id": 12, "investor_id": 1}) is None
        # lp_collection: config.to_dict()
        assert linked_ingestion_job_id(12, {"mode": "full"}) is None
        # disagreeing ids: trust neither
        assert linked_ingestion_job_id(12, {"ingestion_job_id": 13}) is None
        assert linked_ingestion_job_id(12, None) is None
        assert linked_ingestion_job_id(12, {"ingestion_job_id": "x"}) is None


@pytest.mark.unit
class TestDecide:
    NOW = datetime(2026, 10, 10, 12, 0, 0)
    GRACE = timedelta(minutes=10)
    ORPHAN = timedelta(hours=24)

    def _decide(self, **kw):
        from app.core.ingestion_job_sync import decide

        args = dict(
            ing_status="pending",
            ing_started_at=None,
            ing_created_at=self.NOW - timedelta(days=13),
            q_status=None,
            q_completed_at=None,
            q_created_at=None,
            now=self.NOW,
            grace=self.GRACE,
            orphan_after=self.ORPHAN,
        )
        args.update(kw)
        return decide(**args)

    def test_decide_terminal_queue_mirrors_outcome(self):
        done = self.NOW - timedelta(days=13)
        assert self._decide(q_status="success", q_completed_at=done)[0] == "success"
        assert self._decide(q_status="failed", q_completed_at=done)[0] == "failed"
        assert self._decide(q_status="cancelled", q_completed_at=done)[0] == "failed"

    def test_decide_waits_out_the_grace(self):
        """A retry resets the row and submits a new queue row a moment later;
        between the two the latest queue row is the old failed one."""
        just = self.NOW - timedelta(minutes=2)
        assert self._decide(q_status="failed", q_completed_at=just) is None

    def test_decide_skips_in_process_rerun(self):
        """process_scheduled_retries re-runs the same id in-process, with no
        new queue row: started after the queue row completed = a new run."""
        done = self.NOW - timedelta(hours=3)
        assert self._decide(
            ing_status="running",
            ing_started_at=self.NOW - timedelta(minutes=5),
            q_status="failed",
            q_completed_at=done,
        ) is None

    def test_decide_live_queue_is_left_alone(self):
        for live in ("pending", "blocked", "claimed", "running"):
            assert self._decide(q_status=live) is None

    def test_decide_queueless_orphans(self):
        # PENDING, no queue row, older than the orphan window
        assert self._decide()[0] == "failed"
        # too young
        assert self._decide(ing_created_at=self.NOW - timedelta(hours=1)) is None
        # RUNNING and never started — nobody else will ever clean it
        assert self._decide(ing_status="running")[0] == "failed"
        # RUNNING with started_at belongs to cleanup_stuck_jobs (per-source timeout)
        assert self._decide(
            ing_status="running", ing_started_at=self.NOW - timedelta(days=3)
        ) is None


@pytest.mark.unit
class TestScheduler:
    def _schedule(self, **kw):
        from app.core.models import IngestionSchedule, ScheduleFrequency

        kw.setdefault("frequency", ScheduleFrequency.DAILY)
        return IngestionSchedule(id=1, name="s", source="fred", config={}, is_active=1, hour=6, **kw)

    def test_cadence_and_grace(self):
        """T3: daily -> hours, monthly -> about a day."""
        from app.core.models import ScheduleFrequency
        from app.core.scheduler_service import (
            schedule_cadence_seconds,
            schedule_misfire_grace_seconds,
        )

        daily = self._schedule()
        assert schedule_cadence_seconds(daily) == 86400
        assert 3600 <= schedule_misfire_grace_seconds(daily) <= 6 * 3600

        mart = self._schedule(frequency=ScheduleFrequency.CUSTOM, cron_expression="0 6 10 * *")
        assert 28 * 86400 <= schedule_cadence_seconds(mart) <= 31 * 86400
        assert schedule_misfire_grace_seconds(mart) == 86400

        hourly = self._schedule(frequency=ScheduleFrequency.HOURLY)
        assert 300 <= schedule_misfire_grace_seconds(hourly) < 3600

        daily_cron = self._schedule(frequency=ScheduleFrequency.CUSTOM, cron_expression="30 2 * * *")
        assert schedule_cadence_seconds(daily_cron) == 86400

    def test_scheduler_job_defaults(self):
        """T4: the 1 s default grace dropped any run the event loop was late for."""
        from apscheduler.jobstores.memory import MemoryJobStore

        from app.core.scheduler_service import SCHEDULER_JOB_DEFAULTS, _build_scheduler

        assert SCHEDULER_JOB_DEFAULTS["coalesce"] is True
        assert SCHEDULER_JOB_DEFAULTS["max_instances"] == 1
        assert SCHEDULER_JOB_DEFAULTS["misfire_grace_time"] >= 3600

        sched = _build_scheduler({"default": MemoryJobStore()})
        assert sched._job_defaults["misfire_grace_time"] == SCHEDULER_JOB_DEFAULTS["misfire_grace_time"]
        assert sched._job_defaults["coalesce"] is True
        assert sched._job_defaults["max_instances"] == 1

    def test_register_keeps_missed_run_within_grace(self, monkeypatch):
        """T5: re-registering at startup recomputed next_run_time from now, so
        a run missed while the API was down vanished. Keep it within grace."""
        from apscheduler.jobstores.memory import MemoryJobStore

        import app.core.scheduler_service as ss

        schedule = self._schedule()
        missed = datetime.now(timezone.utc) - timedelta(minutes=30)
        stale = datetime.now(timezone.utc) - timedelta(days=2)

        async def scenario(prev):
            sched = ss._build_scheduler({"default": MemoryJobStore()})
            monkeypatch.setattr(ss, "_scheduler", sched)
            sched.start(paused=True)
            try:
                sched.add_job(ss.run_scheduled_job, trigger=ss._get_trigger_for_schedule(schedule),
                              id="schedule_1", args=[1], next_run_time=prev)
                assert ss.register_schedule(schedule)
                job = sched.get_job("schedule_1")
                return job.next_run_time, job.misfire_grace_time
            finally:
                sched.shutdown(wait=False)

        kept, grace = asyncio.run(scenario(missed))
        assert kept == missed
        assert grace == ss.schedule_misfire_grace_seconds(schedule)

        # too old: not resurrected, next regular fire time instead
        fresh, _ = asyncio.run(scenario(stale))
        assert fresh > datetime.now(timezone.utc)


@pytest.mark.unit
def test_compose_restart_depends_pin():
    """T6: api/worker restart on crash, wait for the proxy they actually use,
    and the proxy image does not float."""
    import re

    import yaml

    compose = yaml.safe_load((REPO / "docker-compose.yml").read_text())
    services = compose["services"]
    for name in ("api", "worker"):
        svc = services[name]
        assert svc.get("restart") == "unless-stopped", name
        deps = svc.get("depends_on") or {}
        assert "cloudsqlproxy" in deps, name
        assert deps["cloudsqlproxy"]["condition"] == "service_healthy", name
        # review fix: local dev without GCP credentials must still start, and
        # the default local postgres keeps its readiness wait
        assert deps["cloudsqlproxy"].get("required") is False, name
        assert deps["postgres"]["condition"] == "service_healthy", name

    image = services["cloudsqlproxy"]["image"]
    tag = image.rsplit(":", 1)[1]
    assert re.fullmatch(r"\d+\.\d+\.\d+", tag), image


# ---------------------------------------------------------------------------
# PostgreSQL-backed
# ---------------------------------------------------------------------------


@pytest.fixture
def pgdb(monkeypatch):
    """Fresh ingestion_schedules / ingestion_jobs / job_queue, and every
    get_session_factory the code under test uses pointed at them."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    from app.core.models import Base, IngestionJob, IngestionSchedule
    from app.core.models_queue import JobEvent, JobQueue

    engine = create_engine(PG_URL)
    tables = [IngestionSchedule.__table__, IngestionJob.__table__, JobQueue.__table__,
              JobEvent.__table__]
    with engine.begin() as conn:
        for t in reversed(tables):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t.name} CASCADE"))
    Base.metadata.create_all(engine, tables=tables)

    factory = sessionmaker(bind=engine)
    import app.core.scheduler_service as ss
    import app.worker.executors.bulk_ingest as bi
    import app.worker.main as wm

    for mod in (ss, wm, bi):
        monkeypatch.setattr(mod, "get_session_factory", lambda: factory)

    import app.core.job_queue_service as jqs
    monkeypatch.setattr(jqs, "WORKER_MODE", True)

    yield engine, factory
    engine.dispose()


def _ing(engine, *, source="job:pe_mart_build", status="pending", schedule_id=None,
         created_at=None, started_at=None):
    from sqlalchemy import text

    with engine.begin() as conn:
        return conn.execute(text("""
            INSERT INTO ingestion_jobs (source, status, config, created_at, started_at,
                                        schedule_id, retry_count, max_retries, data_origin)
            VALUES (:s, :st, '{}', :c, :sa, :sid, 0, 3, 'real') RETURNING id
        """), {"s": source, "st": status, "c": created_at or datetime.utcnow(),
               "sa": started_at, "sid": schedule_id}).scalar()


def _queue(engine, *, ing_id, status="pending", job_type="pe_mart_build", completed_at=None,
           payload=None, job_table_id="same", error=None, created_at=None):
    import json

    from sqlalchemy import text

    if payload is None:
        payload = {"ingestion_job_id": ing_id}
    with engine.begin() as conn:
        return conn.execute(text("""
            INSERT INTO job_queue (job_type, job_table_id, status, priority, payload,
                                   created_at, completed_at, error_message)
            VALUES (:t, :jt, :st, 0, CAST(:p AS json), :c, :done, :err) RETURNING id
        """), {"t": job_type, "jt": ing_id if job_table_id == "same" else job_table_id,
               "st": status, "p": json.dumps(payload),
               "c": created_at or datetime.utcnow(), "done": completed_at,
               "err": error}).scalar()


def _ing_row(engine, ing_id):
    from sqlalchemy import text

    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT status, completed_at, error_message, started_at FROM ingestion_jobs WHERE id=:i"
        ), {"i": ing_id}).one()


def _schedule(engine, *, source="job:pe_mart_build", cron="0 6 10 * *"):
    from sqlalchemy import text

    with engine.begin() as conn:
        return conn.execute(text("""
            INSERT INTO ingestion_schedules (name, source, config, frequency, cron_expression,
                                             hour, is_active, created_at, updated_at, priority)
            VALUES (:n, :s, '{}', 'custom', :c, 6, 1, NOW(), NOW(), 5) RETURNING id
        """), {"n": f"sched-{source}", "s": source, "c": cron}).scalar()


def _run_worker_job(factory, queue_id, executor):
    import app.worker.main as wm
    from app.core.models_queue import JobQueue, QueueJobType

    wm.EXECUTORS[QueueJobType.PE_MART_BUILD] = executor
    db = factory()
    try:
        job = db.get(JobQueue, queue_id)
        asyncio.run(wm.execute_job(job, db))
    finally:
        wm.EXECUTORS.pop(QueueJobType.PE_MART_BUILD, None)
        db.close()


@pg
def test_worker_writes_back_success(pgdb):
    """T7: the mart executor never touches ingestion_jobs; the worker must."""
    from sqlalchemy import text

    engine, factory = pgdb
    sid = _schedule(engine)
    ing = _ing(engine, schedule_id=sid)
    qid = _queue(engine, ing_id=ing)

    async def ok(job, db):
        return None

    _run_worker_job(factory, qid, ok)

    status, completed, error, started = _ing_row(engine, ing)
    assert status == "success"
    assert completed is not None and started is not None
    with engine.connect() as conn:
        last_run = conn.execute(text("SELECT last_run_at FROM ingestion_schedules WHERE id=:i"),
                                {"i": sid}).scalar()
    assert last_run is not None


@pg
def test_worker_writes_back_failure(pgdb):
    """T8"""
    engine, factory = pgdb
    ing = _ing(engine)
    qid = _queue(engine, ing_id=ing)

    async def boom(job, db):
        raise RuntimeError("entity master missing")

    _run_worker_job(factory, qid, boom)

    status, completed, error, _ = _ing_row(engine, ing)
    assert status == "failed"
    assert completed is not None
    assert "entity master missing" in error


@pg
def test_worker_write_back_is_idempotent(pgdb):
    """T9: bulk_ingest and ingestion write their own outcome; the worker must
    not overwrite it (a FAILED row stays FAILED even if the queue job ends
    success, and completed_at is not moved)."""
    from sqlalchemy import text

    engine, factory = pgdb
    ing = _ing(engine)
    qid = _queue(engine, ing_id=ing)
    written = datetime(2026, 9, 1, 12, 0, 0)

    async def writes_its_own(job, db):
        db.execute(text("UPDATE ingestion_jobs SET status='failed', completed_at=:c, "
                        "error_message='own' WHERE id=:i"), {"c": written, "i": ing})
        db.commit()

    _run_worker_job(factory, qid, writes_its_own)

    status, completed, error, _ = _ing_row(engine, ing)
    assert (status, completed, error) == ("failed", written, "own")


@pg
def test_worker_ignores_unlinked_job_table_id(pgdb):
    """T10: agentic_research sets job_table_id to its own table's id, which can
    collide with an unrelated ingestion_jobs id."""
    engine, factory = pgdb
    ing = _ing(engine, source="fred")
    qid = _queue(engine, ing_id=ing, payload={"job_id": ing, "investor_id": 1})

    async def ok(job, db):
        return None

    _run_worker_job(factory, qid, ok)
    assert _ing_row(engine, ing)[0] == "pending"


@pg
def test_sweep_repairs_zombie(pgdb):
    """T11: row 3891's shape — PENDING, queue row success days ago. Also any
    casing of a terminal status (raw SQL elsewhere writes upper case)."""
    from app.core.ingestion_job_sync import reconcile_orphaned_ingestion_jobs

    engine, factory = pgdb
    long_ago = datetime.utcnow() - timedelta(days=13)
    zombie = _ing(engine, created_at=long_ago)
    _queue(engine, ing_id=zombie, status="success", completed_at=long_ago, created_at=long_ago)
    failed = _ing(engine, created_at=long_ago)
    _queue(engine, ing_id=failed, status="FAILED", completed_at=long_ago, error="boom")
    cancelled = _ing(engine, status="running", created_at=long_ago)
    _queue(engine, ing_id=cancelled, status="Cancelled", completed_at=long_ago)

    db = factory()
    try:
        result = reconcile_orphaned_ingestion_jobs(db)
    finally:
        db.close()

    assert result["reconciled"] == 3
    assert _ing_row(engine, zombie)[0] == "success"
    status, _, error, _ = _ing_row(engine, failed)
    assert status == "failed" and "boom" in error
    assert _ing_row(engine, cancelled)[0] == "failed"

    # second run is a no-op
    db = factory()
    try:
        assert reconcile_orphaned_ingestion_jobs(db)["reconciled"] == 0
    finally:
        db.close()


@pg
def test_sweep_leaves_live_and_fresh(pgdb):
    """T12"""
    from app.core.ingestion_job_sync import reconcile_orphaned_ingestion_jobs

    engine, factory = pgdb
    old = datetime.utcnow() - timedelta(days=2)

    live = _ing(engine, status="running", started_at=old)
    _queue(engine, ing_id=live, status="running")

    fresh = _ing(engine)
    _queue(engine, ing_id=fresh, status="success", completed_at=datetime.utcnow())

    # retried: old queue row failed, new queue row pending -> latest is live
    retried = _ing(engine)
    _queue(engine, ing_id=retried, status="failed", completed_at=old, created_at=old)
    _queue(engine, ing_id=retried, status="pending")

    # in-process re-run of the same id after the queue row finished
    rerun = _ing(engine, status="running", started_at=datetime.utcnow() - timedelta(minutes=1))
    _queue(engine, ing_id=rerun, status="failed", completed_at=old, created_at=old)

    db = factory()
    try:
        assert reconcile_orphaned_ingestion_jobs(db)["reconciled"] == 0
    finally:
        db.close()
    for i in (live, rerun):
        assert _ing_row(engine, i)[0] == "running"
    for i in (fresh, retried):
        assert _ing_row(engine, i)[0] == "pending"


@pg
def test_sweep_fails_queueless_orphans(pgdb):
    """T13"""
    from app.core.ingestion_job_sync import reconcile_orphaned_ingestion_jobs

    engine, factory = pgdb
    ancient = _ing(engine, source="fred", created_at=datetime.utcnow() - timedelta(days=2))
    young = _ing(engine, source="fred", created_at=datetime.utcnow() - timedelta(hours=1))
    running = _ing(engine, source="fred", status="running",
                   created_at=datetime.utcnow() - timedelta(days=2),
                   started_at=datetime.utcnow() - timedelta(days=2))

    db = factory()
    try:
        assert reconcile_orphaned_ingestion_jobs(db)["reconciled"] == 1
    finally:
        db.close()
    assert _ing_row(engine, ancient)[0] == "failed"
    assert _ing_row(engine, young)[0] == "pending"
    assert _ing_row(engine, running)[0] == "running"  # cleanup_stuck_jobs' job


@pg
def test_cleanup_stuck_jobs_runs_sweep(pgdb):
    """T14: the sweep rides the existing 30-minute cleanup job."""
    from app.core.scheduler_service import cleanup_stuck_jobs

    engine, factory = pgdb
    long_ago = datetime.utcnow() - timedelta(days=13)
    zombie = _ing(engine, created_at=long_ago)
    _queue(engine, ing_id=zombie, status="success", completed_at=long_ago)

    result = asyncio.run(cleanup_stuck_jobs())
    assert result.get("orphans_reconciled") == 1
    assert _ing_row(engine, zombie)[0] == "success"


@pg
def test_schedule_unblocks_after_2x_cadence(pgdb, caplog):
    """T15: a skipped schedule is a WARNING; a blocker older than 2x cadence is
    reconciled and the schedule runs."""
    from sqlalchemy import text

    from app.core.scheduler_service import run_scheduled_job

    engine, factory = pgdb

    # Young blocker on a monthly schedule: skipped, loudly.
    young_sid = _schedule(engine, source="job:entity_resolve", cron="0 4 10 * *")
    young = _ing(engine, source="job:entity_resolve", schedule_id=young_sid)
    _queue(engine, ing_id=young, job_type="entity_resolve", status="running")
    with caplog.at_level(logging.WARNING, logger="app.core.scheduler_service"):
        asyncio.run(run_scheduled_job(young_sid))
    assert any("skipped" in r.message and r.levelno == logging.WARNING for r in caplog.records)

    # Row 3891: pending for > 2 months behind a finished queue row.
    sid = _schedule(engine)
    ancient = datetime.utcnow() - timedelta(days=70)
    zombie = _ing(engine, schedule_id=sid, created_at=ancient)
    _queue(engine, ing_id=zombie, status="success", completed_at=ancient, created_at=ancient)

    asyncio.run(run_scheduled_job(sid))

    assert _ing_row(engine, zombie)[0] == "success"
    with engine.connect() as conn:
        jobs = conn.execute(text(
            "SELECT id, status FROM ingestion_jobs WHERE schedule_id=:s ORDER BY id"), {"s": sid}).fetchall()
        queued = conn.execute(text(
            "SELECT count(*) FROM job_queue WHERE job_type='pe_mart_build'")).scalar()
    assert len(jobs) == 2 and jobs[1][1] == "pending"
    assert queued == 2


@pg
def test_bulk_ingest_finishes_when_discover_raises(pgdb, monkeypatch):
    """T16: discover() raising left the IngestionJob RUNNING until timeout,
    which also blocked the schedule."""
    import app.worker.executors.bulk_ingest as bi
    from app.core.models_queue import JobQueue

    engine, factory = pgdb
    ing = _ing(engine, source="bulk:sec_13f")
    qid = _queue(engine, ing_id=ing, job_type="bulk_ingest",
                 payload={"bulk_source": "sec_13f", "ingestion_job_id": ing})

    monkeypatch.setattr(bi, "get_source", lambda name: object())

    def discover_raises(*a, **kw):
        raise ConnectionError("sec.gov unreachable")

    monkeypatch.setattr(bi, "run_source", discover_raises)

    db = factory()
    try:
        job = db.get(JobQueue, qid)
        with pytest.raises(ConnectionError):
            asyncio.run(bi.execute(job, db))
    finally:
        db.close()

    status, completed, error, _ = _ing_row(engine, ing)
    assert status == "failed"
    assert completed is not None
    assert "sec.gov unreachable" in error


# ---------------------------------------------------------------------------
# Review fixes (spec-121-fix)
# ---------------------------------------------------------------------------


@pg
def test_sweep_links_queue_rows_with_null_job_table_id(pgdb):
    """T17: /batch/{id}/unstick resubmitted with no job_table_id. The worker's
    link rule accepts that; the sweep must too, or it fails a row >24h old as
    'no job_queue row' while its queue job is still live."""
    from app.core.ingestion_job_sync import reconcile_orphaned_ingestion_jobs

    engine, factory = pgdb
    old = datetime.utcnow() - timedelta(days=2)

    live = _ing(engine, source="fred", created_at=old)
    _queue(engine, ing_id=live, job_type="ingestion", status="running", job_table_id=None)

    done = _ing(engine, source="fred", created_at=old)
    _queue(engine, ing_id=done, job_type="ingestion", status="success", job_table_id=None,
           completed_at=old, created_at=old)

    # a NULL job_table_id with a different payload id links nothing
    other = _ing(engine, source="fred", created_at=datetime.utcnow())
    _queue(engine, ing_id=other, job_type="ingestion", status="success", job_table_id=None,
           payload={"ingestion_job_id": other + 1000}, completed_at=old)

    db = factory()
    try:
        result = reconcile_orphaned_ingestion_jobs(db)
    finally:
        db.close()

    assert result["jobs"] == [{"id": done, "status": "success"}]
    assert _ing_row(engine, live)[0] == "pending"
    assert _ing_row(engine, other)[0] == "pending"


@pg
def test_unstick_links_resubmitted_queue_row(pgdb):
    """T18: unstick sets job_table_id on its resubmit, and a queue row it (or
    an older build) submitted with a NULL job_table_id counts as live."""
    from sqlalchemy import text

    from app.api.v1.jobs import unstick_batch

    engine, factory = pgdb
    old = datetime.utcnow() - timedelta(hours=5)
    stuck = _ing(engine, source="fred", created_at=old)
    covered = _ing(engine, source="fred", created_at=old)
    with engine.begin() as conn:
        conn.execute(text("UPDATE ingestion_jobs SET batch_run_id='b1' WHERE id IN (:a, :b)"),
                     {"a": stuck, "b": covered})
    _queue(engine, ing_id=covered, job_type="ingestion", status="pending", job_table_id=None,
           payload={"ingestion_job_id": covered, "batch_id": "b1"})

    db = factory()
    try:
        assert unstick_batch("b1", db)["resubmitted"] == 1
        db.commit()
    finally:
        db.close()
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT job_table_id FROM job_queue WHERE payload->>'ingestion_job_id' = :i"),
            {"i": str(stuck)}).fetchall()
    assert [r[0] for r in rows] == [stuck]


@pg
def test_schedule_cancels_hung_blocker_after_2x_cadence(pgdb, caplog):
    """T19: a job:/bulk worker job has no execution timeout and a hung
    executor keeps heartbeating, so its IngestionJob stays PENDING forever.
    Past 2x cadence the schedule cancels the queue job and runs."""
    from sqlalchemy import text

    from app.core.scheduler_service import run_scheduled_job

    engine, factory = pgdb
    sid = _schedule(engine, source="job:entity_resolve", cron="0 4 * * *")  # daily
    ancient = datetime.utcnow() - timedelta(days=3)
    hung = _ing(engine, source="job:entity_resolve", schedule_id=sid, created_at=ancient)
    hung_q = _queue(engine, ing_id=hung, job_type="entity_resolve", status="running",
                    created_at=ancient)

    with caplog.at_level(logging.WARNING):
        asyncio.run(run_scheduled_job(sid))

    status, completed, error, _ = _ing_row(engine, hung)
    assert status == "failed" and completed is not None
    assert "Cancelled" in error
    with engine.connect() as conn:
        q_status, q_error = conn.execute(text(
            "SELECT status, error_message FROM job_queue WHERE id=:i"), {"i": hung_q}).one()
        jobs = conn.execute(text(
            "SELECT status FROM ingestion_jobs WHERE schedule_id=:s ORDER BY id"), {"s": sid}).fetchall()
    # the form the worker heartbeat treats as a cancellation
    assert q_status == "failed" and "Cancelled" in q_error
    assert [j[0] for j in jobs] == ["failed", "pending"]
    assert any("Cancelled queue job" in r.message for r in caplog.records)


@pg
def test_schedule_keeps_young_live_blocker(pgdb):
    """T20: under 2x cadence a live queue job is never cancelled."""
    from sqlalchemy import text

    from app.core.scheduler_service import run_scheduled_job

    engine, factory = pgdb
    sid = _schedule(engine, source="job:entity_resolve", cron="0 4 * * *")
    recent = datetime.utcnow() - timedelta(hours=30)
    busy = _ing(engine, source="job:entity_resolve", schedule_id=sid, created_at=recent)
    busy_q = _queue(engine, ing_id=busy, job_type="entity_resolve", status="running")

    asyncio.run(run_scheduled_job(sid))

    assert _ing_row(engine, busy)[0] == "pending"
    with engine.connect() as conn:
        assert conn.execute(text("SELECT status FROM job_queue WHERE id=:i"),
                            {"i": busy_q}).scalar() == "running"


@pg
def test_heartbeat_stops_cancelled_blocker(pgdb, monkeypatch):
    """T21: the scheduler's cancellation reaches a running executor."""
    import app.worker.main as wm
    from app.core.ingestion_job_sync import cancel_blocking_queue_job

    engine, factory = pgdb
    ing = _ing(engine)
    qid = _queue(engine, ing_id=ing, status="running")
    assert cancel_blocking_queue_job(factory(), ing, "test") == qid

    monkeypatch.setattr(wm, "HEARTBEAT_INTERVAL", 0)
    with pytest.raises(wm.JobCancelledError):
        asyncio.run(asyncio.wait_for(wm._heartbeat_loop(factory, qid), timeout=5))
