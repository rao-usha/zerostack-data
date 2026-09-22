"""
Tests for SPEC 114 — Scheduled bulk SEC loads.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _schedule(source="bulk:sec_form_d", config=None, is_active=1, sid=7):
    from app.core.models import IngestionSchedule, ScheduleFrequency

    sched = MagicMock(spec=IngestionSchedule)
    sched.id = sid
    sched.name = f"schedule for {source}"
    sched.source = source
    sched.config = config if config is not None else {}
    sched.is_active = is_active
    sched.frequency = ScheduleFrequency.CUSTOM
    sched.cron_expression = "10 5 * * *"
    sched.last_run_at = None
    sched.hour = 5
    sched.day_of_week = None
    sched.day_of_month = None
    return sched


def _db_for(schedule, active_job=None):
    db = MagicMock()
    db.query.return_value.filter.return_value.first.side_effect = [schedule, active_job]
    return db


@pytest.mark.unit
class TestBulkScheduleDispatch:
    @pytest.mark.asyncio
    async def test_bulk_schedule_queues_worker_job(self):
        """T1"""
        from app.core import scheduler_service

        sched = _schedule()
        db = _db_for(sched)
        with patch.object(scheduler_service, "get_session_factory", return_value=lambda: db), \
             patch.object(scheduler_service, "submit_job", return_value={"job_queue_id": 55}) as submit, \
             patch.object(scheduler_service, "_execute_ingestion_job", AsyncMock()) as ingest:
            await scheduler_service.run_scheduled_job(sched.id)

        ingest.assert_not_called()
        kwargs = submit.call_args.kwargs
        assert kwargs["job_type"] == "bulk_ingest"
        assert kwargs["payload"]["bulk_source"] == "sec_form_d"
        assert "job_table_id" in kwargs                     # linked to the ingestion job
        assert "ingestion_job_id" in kwargs["payload"]      # executor mirrors status back
        assert db.add.called                                # an IngestionJob row was created

    @pytest.mark.asyncio
    async def test_bulk_schedule_passes_config_options(self):
        """T2"""
        from app.core import scheduler_service

        sched = _schedule(config={"since": "2025-01-01", "max_releases": 2,
                                  "release_keys": ["2025q1"], "ignored": "x"})
        db = _db_for(sched)
        with patch.object(scheduler_service, "get_session_factory", return_value=lambda: db), \
             patch.object(scheduler_service, "submit_job", return_value={"job_queue_id": 1}) as submit, \
             patch.object(scheduler_service, "_execute_ingestion_job", AsyncMock()):
            await scheduler_service.run_scheduled_job(sched.id)

        payload = submit.call_args.kwargs["payload"]
        assert payload["since"] == "2025-01-01"
        assert payload["max_releases"] == 2
        assert payload["release_keys"] == ["2025q1"]
        assert "ignored" not in payload

    @pytest.mark.asyncio
    async def test_non_bulk_schedule_unchanged(self):
        """T3"""
        from app.core import scheduler_service

        sched = _schedule(source="fred", config={"category": "interest_rates"})
        db = _db_for(sched)
        with patch.object(scheduler_service, "get_session_factory", return_value=lambda: db), \
             patch.object(scheduler_service, "submit_job") as submit, \
             patch.object(scheduler_service, "_execute_ingestion_job", AsyncMock()) as ingest:
            await scheduler_service.run_scheduled_job(sched.id)

        submit.assert_not_called()
        ingest.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("is_active,active_job", [(0, None), (1, MagicMock())])
    async def test_paused_or_duplicate_schedule_skipped(self, is_active, active_job):
        """T4"""
        from app.core import scheduler_service

        sched = _schedule(is_active=is_active)
        db = _db_for(sched, active_job=active_job)
        with patch.object(scheduler_service, "get_session_factory", return_value=lambda: db), \
             patch.object(scheduler_service, "submit_job") as submit, \
             patch.object(scheduler_service, "_execute_ingestion_job", AsyncMock()):
            await scheduler_service.run_scheduled_job(sched.id)

        submit.assert_not_called()


@pytest.mark.unit
class TestExecutorUpdatesIngestionJob:
    async def _run(self, summary, ingestion_job):
        from app.worker.executors import bulk_ingest

        job = MagicMock(payload={"bulk_source": "sec_form_d", "ingestion_job_id": 42})
        db = MagicMock()
        session = MagicMock()
        session.get.return_value = ingestion_job

        async def fake_to_thread(fn, *args, **kwargs):
            return summary

        with patch.object(bulk_ingest.asyncio, "to_thread", fake_to_thread), \
             patch.object(bulk_ingest, "get_source", return_value=MagicMock()), \
             patch.object(bulk_ingest, "get_session_factory", return_value=lambda: session):
            return await bulk_ingest.execute(job, db)

    @pytest.mark.asyncio
    async def test_executor_marks_ingestion_job_success(self):
        """T5"""
        from app.core.models import JobStatus

        ing = MagicMock()
        await self._run({"loaded": 2, "failed": 0, "skipped": 1, "rows": 1500, "errors": [],
                         "source": "sec_form_d"}, ing)
        assert ing.status == JobStatus.SUCCESS
        assert ing.rows_inserted == 1500
        assert ing.completed_at is not None

    @pytest.mark.asyncio
    async def test_executor_marks_ingestion_job_failed(self):
        """T6"""
        from app.core.models import JobStatus

        ing = MagicMock()
        with pytest.raises(RuntimeError):
            await self._run({"loaded": 0, "failed": 2, "skipped": 0, "rows": 0,
                             "errors": ["2025q1: boom"], "source": "sec_form_d"}, ing)
        assert ing.status == JobStatus.FAILED
        assert "boom" in (ing.error_message or "")


@pytest.mark.unit
class TestDefaultSchedules:
    def test_default_schedules_build_valid_triggers(self):
        """T8"""
        from apscheduler.triggers.cron import CronTrigger

        from app.core.models_queue import QueueJobType
        from app.core.scheduler_service import DEFAULT_BULK_SCHEDULES
        from app.ingest.bulk.registry import list_sources

        # The list carries two kinds of source since SPEC_120: every registered
        # bulk loader, and the `job:` marts that read what they write. Each is
        # checked against its own registry -- a typo in either would otherwise
        # only surface when the schedule fires, unwatched, next month.
        bulk = {d["source"].split(":", 1)[1] for d in DEFAULT_BULK_SCHEDULES
                if d["source"].startswith("bulk:")}
        jobs = {d["source"].split(":", 1)[1] for d in DEFAULT_BULK_SCHEDULES
                if d["source"].startswith("job:")}
        assert bulk == set(list_sources())
        assert jobs == {"entity_resolve", "pe_mart_build"}
        assert jobs <= {t.value for t in QueueJobType}
        for d in DEFAULT_BULK_SCHEDULES:
            assert d["source"].startswith(("bulk:", "job:"))
            CronTrigger.from_crontab(d["cron_expression"])  # raises if invalid
        names = [d["name"] for d in DEFAULT_BULK_SCHEDULES]
        assert len(names) == len(set(names))

    def test_install_defaults_idempotent(self):
        """T7"""
        from app.core.models import IngestionSchedule, ScheduleFrequency
        from app.core.scheduler_service import DEFAULT_BULK_SCHEDULES, install_default_bulk_schedules

        existing = IngestionSchedule(
            name=DEFAULT_BULK_SCHEDULES[0]["name"], source=DEFAULT_BULK_SCHEDULES[0]["source"],
            config={}, frequency=ScheduleFrequency.CUSTOM,
            cron_expression=DEFAULT_BULK_SCHEDULES[0]["cron_expression"], is_active=0,
        )
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [existing]
        created = []
        with patch("app.core.scheduler_service.create_schedule",
                   side_effect=lambda **kw: created.append(kw["name"])):
            result = install_default_bulk_schedules(db)

        assert result["existing"] == [existing.name]
        assert set(result["created"]) == {d["name"] for d in DEFAULT_BULK_SCHEDULES[1:]}
        assert existing.is_active == 0  # paused schedule left alone


@pytest.mark.unit
def test_bulk_schedule_endpoints():
    """T9"""
    from app.api.v1 import bulk

    db = MagicMock()
    with patch.object(bulk, "install_default_bulk_schedules",
                      return_value={"created": ["a"], "existing": []}) as install:
        assert bulk.install_bulk_schedules(db=db) == {"created": ["a"], "existing": []}
        install.assert_called_once_with(db)

    rows = [MagicMock(id=1, name="n", source="bulk:sec_form_d", cron_expression="0 8 8 * *",
                      is_active=1, last_run_at=None, next_run_at=None, last_job_id=None)]
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = rows
    out = bulk.list_bulk_schedules(db=db)
    assert out["schedules"][0]["source"] == "bulk:sec_form_d"
    assert out["schedules"][0]["is_active"] is True


@pytest.mark.unit
def test_cron_schedule_next_run_is_in_the_future():
    """CUSTOM schedules get their next_run_at from the cron expression, not 'now'."""
    from datetime import datetime

    from app.core.scheduler_service import _calculate_next_run

    sched = _schedule()
    sched.cron_expression = "10 5 * * *"
    next_run = _calculate_next_run(sched)
    assert next_run > datetime.utcnow()
    assert (next_run.hour, next_run.minute) == (5, 10)
