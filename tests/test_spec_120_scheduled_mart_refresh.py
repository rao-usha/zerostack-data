"""
Tests for SPEC 120 — scheduled mart refresh.

The marts had no schedule at all, so a month from now the raw SEC tables would
have moved and pe_funds / pe_people would still hold today's answers with
nothing to say they had diverged.
"""
import os

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


@pytest.mark.unit
class TestJobPrefix:
    def test_mart_schedules_are_defined_and_ordered(self):
        """T4: entity_resolve must land before pe_mart_build.

        pe_mart_build reads the entity master the resolver writes. Two hours
        apart is slack, not a dependency: both are idempotent, so a late
        loader costs one stale month rather than a corrupt table.
        """
        from app.core.scheduler_service import DEFAULT_BULK_SCHEDULES

        by_source = {s["source"]: s for s in DEFAULT_BULK_SCHEDULES}
        assert "job:entity_resolve" in by_source
        assert "job:pe_mart_build" in by_source

        def when(source):
            minute, hour, dom, _mon, _dow = by_source[source]["cron_expression"].split()
            return int(dom), int(hour), int(minute)

        assert when("job:entity_resolve") < when("job:pe_mart_build")

        # and both run after the last monthly loader (13F, the 9th at 09:30)
        assert when("job:entity_resolve") > (9, 9, 30)

    def test_every_scheduled_job_type_exists(self):
        """T2: a typo must not wait until the 10th of next month to surface."""
        from app.core.models_queue import QueueJobType
        from app.core.scheduler_service import (
            DEFAULT_BULK_SCHEDULES,
            SCHEDULE_JOB_PREFIX,
            validate_schedule_source,
        )

        valid = {t.value for t in QueueJobType}
        for spec in DEFAULT_BULK_SCHEDULES:
            source = spec["source"]
            if source.startswith(SCHEDULE_JOB_PREFIX):
                assert source[len(SCHEDULE_JOB_PREFIX):] in valid, source

        validate_schedule_source("job:pe_mart_build")       # does not raise
        validate_schedule_source("bulk:sec_13f")
        validate_schedule_source("fred")
        with pytest.raises(ValueError, match="pe_mart_buil"):
            validate_schedule_source("job:pe_mart_buil")

    def test_schedule_d_loader_is_scheduled(self):
        """The loader the whole fund attribution rests on was the only one
        with no schedule row, because rows seed at API startup."""
        from app.core.scheduler_service import DEFAULT_BULK_SCHEDULES

        assert "bulk:sec_adv_schedule_d" in {s["source"] for s in DEFAULT_BULK_SCHEDULES}


@pytest.mark.unit
class TestPruneSelection:
    def test_prune_keeps_anything_that_ever_ran(self):
        """T6: last_run_at is the only record of when a source was last
        collected, so a paused schedule that ran once is kept."""
        from app.core.scheduler_service import prunable

        rows = [
            {"id": 1, "is_active": 0, "last_run_at": None},          # never ran, paused
            {"id": 2, "is_active": 0, "last_run_at": "2025-01-01"},  # ran once
            {"id": 3, "is_active": 1, "last_run_at": None},          # active
        ]
        assert [r["id"] for r in rows if prunable(r["is_active"], r["last_run_at"])] == [1]


@pg
def test_job_schedule_queues_the_named_job_type():
    """T1/T3: `job:` queues that type; `bulk:` still queues bulk_ingest."""
    import asyncio

    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session

    from app.core.scheduler_service import _run_bulk_schedule, _run_job_schedule

    from app.core.models import Base, IngestionJob, IngestionSchedule
    from app.core.models_queue import JobQueue

    engine = create_engine(PG_URL)
    # Build the three tables from the ORM rather than by hand: a hand-written
    # ingestion_jobs drifts from the model and fails on a column the code has
    # always written.
    tables = [IngestionSchedule.__table__, IngestionJob.__table__, JobQueue.__table__]
    with engine.begin() as conn:
        for t in reversed(tables):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t.name} CASCADE"))
    Base.metadata.create_all(engine, tables=tables)

    # WORKER_MODE is read at import; both schedules queue rather than fall back
    # to BackgroundTasks, which is what production does.
    import app.core.job_queue_service as jqs
    prior, jqs.WORKER_MODE = jqs.WORKER_MODE, True

    with Session(engine) as db:
        mart = IngestionSchedule(name="marts", source="job:pe_mart_build", config={},
                                 frequency="custom", cron_expression="0 6 10 * *",
                                 is_active=1, priority=5)
        bulk = IngestionSchedule(name="13f", source="bulk:sec_13f", config={},
                                 frequency="custom", cron_expression="30 9 9 * *",
                                 is_active=1, priority=5)
        db.add_all([mart, bulk])
        db.commit()
        try:
            asyncio.run(_run_job_schedule(db, mart))
            asyncio.run(_run_bulk_schedule(db, bulk))
        finally:
            jqs.WORKER_MODE = prior

    with engine.connect() as conn:
        queued = conn.execute(text(
            "SELECT job_type, payload FROM job_queue ORDER BY id")).fetchall()

    assert [q[0] for q in queued] == ["pe_mart_build", "bulk_ingest"]
    assert queued[1][1]["bulk_source"] == "sec_13f"
    engine.dispose()
