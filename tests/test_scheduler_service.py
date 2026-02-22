"""
Unit tests for app/core/scheduler_service.py

Tests cover schedule creation, enable/disable, next-run calculation,
job dispatch from schedule, incremental parameter injection, trigger
construction, and error handling.

All tests are fully offline (no DB, no network).
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock

from app.core.models import (
    IngestionSchedule,
    IngestionJob,
    JobStatus,
    ScheduleFrequency,
)
from app.core.scheduler_service import (
    _inject_incremental_params,
    _calculate_next_run,
    _get_trigger_for_schedule,
    create_schedule,
    update_schedule,
    delete_schedule,
    load_all_schedules,
    get_schedule_history,
    run_scheduled_job,
    INCREMENTAL_PARAM_MAP,
    DEFAULT_SCHEDULES,
)


def _make_schedule(**overrides):
    defaults = dict(
        id=1, name="FRED Daily", source="fred",
        config={"category": "interest_rates"},
        frequency=ScheduleFrequency.DAILY, hour=6,
        cron_expression=None, day_of_week=None, day_of_month=None,
        is_active=1, last_run_at=None, next_run_at=None, last_job_id=None,
        description="Test schedule", priority=5,
        created_at=datetime(2025, 1, 1), updated_at=datetime(2025, 1, 1),
    )
    defaults.update(overrides)
    schedule = MagicMock(spec=IngestionSchedule)
    for k, v in defaults.items():
        setattr(schedule, k, v)
    return schedule

class TestInjectIncrementalParams:

    def test_returns_config_unchanged_when_no_incremental_flag(self):
        config = {"category": "gdp"}
        result = _inject_incremental_params(config, "fred", datetime(2025, 1, 1))
        assert result == config

    def test_returns_empty_dict_when_config_is_none(self):
        result = _inject_incremental_params(None, "fred", None)
        assert result == {}

    def test_full_load_when_no_last_run_at(self):
        config = {"incremental": True, "category": "all"}
        result = _inject_incremental_params(config, "fred", None)
        assert result == config
        assert "observation_start" not in result

    def test_fred_incremental_injects_observation_start(self):
        config = {"incremental": True, "category": "rates"}
        dt = datetime(2025, 3, 15, 10, 30)
        result = _inject_incremental_params(config, "fred", dt)
        assert result["observation_start"] == "2025-03-15"

    def test_bls_incremental_injects_start_year(self):
        config = {"incremental": True}
        dt = datetime(2024, 6, 1)
        result = _inject_incremental_params(config, "bls", dt)
        assert result["start_year"] == 2024

    def test_eia_incremental_injects_start_date(self):
        config = {"incremental": True}
        dt = datetime(2025, 12, 25)
        result = _inject_incremental_params(config, "eia", dt)
        assert result["start"] == "2025-12-25"

    def test_census_incremental_injects_year(self):
        config = {"incremental": True}
        dt = datetime(2023, 7, 4)
        result = _inject_incremental_params(config, "census", dt)
        assert result["year"] == 2023

    def test_bea_incremental_injects_year_as_string(self):
        config = {"incremental": True}
        dt = datetime(2024, 1, 1)
        result = _inject_incremental_params(config, "bea", dt)
        assert result["year"] == "2024"

    def test_unknown_source_returns_config_unchanged(self):
        config = {"incremental": True, "foo": "bar"}
        dt = datetime(2025, 1, 1)
        result = _inject_incremental_params(config, "unknown_source", dt)
        assert result == config

    def test_does_not_mutate_original_config(self):
        config = {"incremental": True, "category": "rates"}
        original = dict(config)
        _inject_incremental_params(config, "fred", datetime(2025, 1, 1))
        assert config == original

    def test_sec_incremental_injects_start_date(self):
        config = {"incremental": True}
        dt = datetime(2025, 9, 1)
        result = _inject_incremental_params(config, "sec", dt)
        assert result["start_date"] == "2025-09-01"

    def test_treasury_incremental_injects_start_date(self):
        config = {"incremental": True}
        dt = datetime(2025, 4, 20)
        result = _inject_incremental_params(config, "treasury", dt)
        assert result["start_date"] == "2025-04-20"

    def test_bts_incremental_injects_start_date(self):
        config = {"incremental": True}
        dt = datetime(2025, 2, 28)
        result = _inject_incremental_params(config, "bts", dt)
        assert result["start_date"] == "2025-02-28"

class TestCalculateNextRun:

    @patch("app.core.scheduler_service.datetime")
    def test_hourly_returns_one_hour_ahead(self, mock_dt):
        now = datetime(2025, 6, 15, 10, 0, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(frequency=ScheduleFrequency.HOURLY)
        result = _calculate_next_run(schedule)
        assert result == now + timedelta(hours=1)

    @patch("app.core.scheduler_service.datetime")
    def test_daily_returns_next_day_if_hour_past(self, mock_dt):
        now = datetime(2025, 6, 15, 10, 30, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(frequency=ScheduleFrequency.DAILY, hour=6)
        result = _calculate_next_run(schedule)
        assert result == datetime(2025, 6, 16, 6, 0, 0)

    @patch("app.core.scheduler_service.datetime")
    def test_daily_returns_same_day_if_hour_not_past(self, mock_dt):
        now = datetime(2025, 6, 15, 10, 0, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(frequency=ScheduleFrequency.DAILY, hour=18)
        result = _calculate_next_run(schedule)
        assert result == datetime(2025, 6, 15, 18, 0, 0)

    @patch("app.core.scheduler_service.datetime")
    def test_weekly_returns_correct_weekday(self, mock_dt):
        now = datetime(2025, 6, 15, 10, 0, 0)  # Sunday
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(
            frequency=ScheduleFrequency.WEEKLY, day_of_week=0, hour=6
        )
        result = _calculate_next_run(schedule)
        assert result.weekday() == 0
        assert result.hour == 6

    @patch("app.core.scheduler_service.datetime")
    def test_monthly_next_month_if_day_past(self, mock_dt):
        now = datetime(2025, 6, 20, 10, 0, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(
            frequency=ScheduleFrequency.MONTHLY, day_of_month=5, hour=6
        )
        result = _calculate_next_run(schedule)
        assert result == datetime(2025, 7, 5, 6, 0, 0)

    @patch("app.core.scheduler_service.datetime")
    def test_monthly_same_month_if_day_not_past(self, mock_dt):
        now = datetime(2025, 6, 1, 0, 0, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(
            frequency=ScheduleFrequency.MONTHLY, day_of_month=15, hour=6
        )
        result = _calculate_next_run(schedule)
        assert result == datetime(2025, 6, 15, 6, 0, 0)

    @patch("app.core.scheduler_service.datetime")
    def test_monthly_december_wraps_to_january(self, mock_dt):
        now = datetime(2025, 12, 25, 10, 0, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(
            frequency=ScheduleFrequency.MONTHLY, day_of_month=10, hour=6
        )
        result = _calculate_next_run(schedule)
        assert result.year == 2026
        assert result.month == 1

    @patch("app.core.scheduler_service.datetime")
    def test_quarterly_returns_next_quarter_month(self, mock_dt):
        now = datetime(2025, 2, 15, 10, 0, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(
            frequency=ScheduleFrequency.QUARTERLY, day_of_month=2, hour=6
        )
        result = _calculate_next_run(schedule)
        assert result.month in [4, 7, 10, 1]

    @patch("app.core.scheduler_service.datetime")
    def test_quarterly_wraps_to_next_year(self, mock_dt):
        now = datetime(2025, 11, 15, 10, 0, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(
            frequency=ScheduleFrequency.QUARTERLY, day_of_month=2, hour=6
        )
        result = _calculate_next_run(schedule)
        assert result.year == 2026
        assert result.month == 1

    @patch("app.core.scheduler_service.datetime")
    def test_unknown_frequency_defaults_to_one_hour(self, mock_dt):
        now = datetime(2025, 6, 15, 10, 0, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(frequency="nonexistent")
        result = _calculate_next_run(schedule)
        assert result == now + timedelta(hours=1)

    @patch("app.core.scheduler_service.datetime")
    def test_daily_defaults_to_hour_6_when_none(self, mock_dt):
        now = datetime(2025, 6, 15, 10, 0, 0)
        mock_dt.utcnow.return_value = now
        schedule = _make_schedule(frequency=ScheduleFrequency.DAILY, hour=None)
        result = _calculate_next_run(schedule)
        assert result.hour == 6

class TestGetTriggerForSchedule:

    @pytest.fixture(autouse=True)
    def skip_if_no_apscheduler(self):
        try:
            from apscheduler.triggers.cron import CronTrigger  # noqa: F401
            from apscheduler.triggers.interval import IntervalTrigger  # noqa: F401
        except ImportError:
            pytest.skip("APScheduler not installed")

    def test_custom_cron_expression(self):
        from apscheduler.triggers.cron import CronTrigger
        schedule = _make_schedule(
            frequency=ScheduleFrequency.CUSTOM, cron_expression="30 2 * * 1-5",
        )
        trigger = _get_trigger_for_schedule(schedule)
        assert isinstance(trigger, CronTrigger)

    def test_hourly_uses_interval_trigger(self):
        from apscheduler.triggers.interval import IntervalTrigger
        schedule = _make_schedule(frequency=ScheduleFrequency.HOURLY)
        trigger = _get_trigger_for_schedule(schedule)
        assert isinstance(trigger, IntervalTrigger)

    def test_daily_uses_cron_trigger(self):
        from apscheduler.triggers.cron import CronTrigger
        schedule = _make_schedule(frequency=ScheduleFrequency.DAILY, hour=10)
        trigger = _get_trigger_for_schedule(schedule)
        assert isinstance(trigger, CronTrigger)

    def test_weekly_uses_cron_trigger(self):
        from apscheduler.triggers.cron import CronTrigger
        schedule = _make_schedule(
            frequency=ScheduleFrequency.WEEKLY, day_of_week=2, hour=11,
        )
        trigger = _get_trigger_for_schedule(schedule)
        assert isinstance(trigger, CronTrigger)

    def test_monthly_uses_cron_trigger(self):
        from apscheduler.triggers.cron import CronTrigger
        schedule = _make_schedule(
            frequency=ScheduleFrequency.MONTHLY, day_of_month=15, hour=8,
        )
        trigger = _get_trigger_for_schedule(schedule)
        assert isinstance(trigger, CronTrigger)

    def test_quarterly_uses_cron_trigger(self):
        from apscheduler.triggers.cron import CronTrigger
        schedule = _make_schedule(
            frequency=ScheduleFrequency.QUARTERLY, day_of_month=2, hour=6,
        )
        trigger = _get_trigger_for_schedule(schedule)
        assert isinstance(trigger, CronTrigger)

class TestCreateSchedule:

    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_creates_schedule_in_db(self, mock_dt, mock_register, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        schedule = create_schedule(
            db=test_db, name="Test FRED Schedule", source="fred",
            config={"category": "rates"},
            frequency=ScheduleFrequency.DAILY, hour=10,
        )
        assert schedule.id is not None
        assert schedule.name == "Test FRED Schedule"
        assert schedule.source == "fred"
        assert schedule.is_active == 1

    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_inactive_schedule_not_registered(self, mock_dt, mock_register, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        create_schedule(
            db=test_db, name="Inactive Schedule", source="bls",
            config={}, is_active=False,
        )
        mock_register.assert_not_called()

    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_active_schedule_is_registered(self, mock_dt, mock_register, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        create_schedule(
            db=test_db, name="Active Schedule", source="eia",
            config={"dataset": "petroleum"}, is_active=True,
        )
        mock_register.assert_called_once()

    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_next_run_at_is_set(self, mock_dt, mock_register, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        schedule = create_schedule(
            db=test_db, name="Scheduled Run", source="treasury",
            config={}, frequency=ScheduleFrequency.DAILY, hour=10,
        )
        assert schedule.next_run_at is not None


class TestUpdateSchedule:

    @patch("app.core.scheduler_service.unregister_schedule", return_value=True)
    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_update_returns_none_for_missing(self, mock_dt, mock_reg, mock_unreg, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        result = update_schedule(test_db, schedule_id=999, name="Ghost")
        assert result is None

    @patch("app.core.scheduler_service.unregister_schedule", return_value=True)
    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_update_changes_fields(self, mock_dt, mock_reg, mock_unreg, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        schedule = create_schedule(
            db=test_db, name="Update Me", source="fred", config={}, is_active=True,
        )
        mock_reg.reset_mock()
        updated = update_schedule(
            test_db, schedule_id=schedule.id, hour=12, description="Updated",
        )
        assert updated is not None
        assert updated.hour == 12
        assert updated.description == "Updated"

    @patch("app.core.scheduler_service.unregister_schedule", return_value=True)
    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_disable_calls_unregister(self, mock_dt, mock_reg, mock_unreg, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        schedule = create_schedule(
            db=test_db, name="Disable Me", source="sec", config={}, is_active=True,
        )
        mock_unreg.reset_mock()
        update_schedule(test_db, schedule_id=schedule.id, is_active=False)
        mock_unreg.assert_called_once_with(schedule.id)

class TestDeleteSchedule:

    @patch("app.core.scheduler_service.unregister_schedule", return_value=True)
    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_delete_existing_schedule(self, mock_dt, mock_reg, mock_unreg, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        schedule = create_schedule(
            db=test_db, name="Delete Me", source="fred", config={}, is_active=False,
        )
        result = delete_schedule(test_db, schedule.id)
        assert result is True
        remaining = (
            test_db.query(IngestionSchedule)
            .filter(IngestionSchedule.id == schedule.id)
            .first()
        )
        assert remaining is None

    @patch("app.core.scheduler_service.unregister_schedule", return_value=True)
    def test_delete_nonexistent_returns_false(self, mock_unreg, test_db):
        result = delete_schedule(test_db, 9999)
        assert result is False


class TestLoadAllSchedules:

    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_loads_only_active_schedules(self, mock_dt, mock_register, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        create_schedule(
            db=test_db, name="Active One", source="fred", config={}, is_active=True,
        )
        create_schedule(
            db=test_db, name="Inactive One", source="bls", config={}, is_active=False,
        )
        mock_register.reset_mock()
        count = load_all_schedules(test_db)
        assert count == 1
        assert mock_register.call_count == 1

class TestRunScheduledJob:

    @pytest.mark.asyncio
    @patch("app.core.scheduler_service._execute_ingestion_job", new_callable=AsyncMock)
    @patch("app.core.scheduler_service.get_session_factory")
    async def test_skips_inactive_schedule(self, mock_factory, mock_execute):
        db = MagicMock()
        mock_factory.return_value = MagicMock(return_value=db)
        schedule = _make_schedule(is_active=0)
        db.query.return_value.filter.return_value.first.return_value = schedule
        await run_scheduled_job(1)
        mock_execute.assert_not_called()
        db.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("app.core.scheduler_service._execute_ingestion_job", new_callable=AsyncMock)
    @patch("app.core.scheduler_service.get_session_factory")
    async def test_skips_missing_schedule(self, mock_factory, mock_execute):
        db = MagicMock()
        mock_factory.return_value = MagicMock(return_value=db)
        db.query.return_value.filter.return_value.first.return_value = None
        await run_scheduled_job(999)
        mock_execute.assert_not_called()
        db.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("app.core.scheduler_service._calculate_next_run")
    @patch("app.core.scheduler_service._execute_ingestion_job", new_callable=AsyncMock)
    @patch("app.core.scheduler_service.get_session_factory")
    async def test_creates_job_and_updates_schedule(
        self, mock_factory, mock_execute, mock_calc
    ):
        db = MagicMock()
        mock_factory.return_value = MagicMock(return_value=db)
        schedule = _make_schedule(is_active=1, config={"category": "rates"})
        db.query.return_value.filter.return_value.first.return_value = schedule

        def refresh_job(obj):
            if isinstance(obj, IngestionJob):
                obj.id = 42

        db.refresh.side_effect = refresh_job
        mock_calc.return_value = datetime(2025, 6, 16, 6, 0, 0)
        await run_scheduled_job(1)

        # add is called for IngestionJob + optionally audit log
        assert db.add.call_count >= 1
        added_job = db.add.call_args_list[0][0][0]
        assert isinstance(added_job, IngestionJob)
        assert added_job.source == "fred"
        assert added_job.status == JobStatus.PENDING
        assert schedule.last_run_at is not None
        assert schedule.next_run_at == datetime(2025, 6, 16, 6, 0, 0)
        mock_execute.assert_called_once()

class TestDefaultSchedules:

    def test_default_schedules_not_empty(self):
        assert len(DEFAULT_SCHEDULES) > 0

    def test_all_defaults_have_required_keys(self):
        for template in DEFAULT_SCHEDULES:
            assert "name" in template
            assert "source" in template
            assert "config" in template
            assert "frequency" in template

    def test_all_defaults_have_valid_frequency(self):
        valid = set(ScheduleFrequency)
        for template in DEFAULT_SCHEDULES:
            assert template["frequency"] in valid

    def test_all_defaults_have_unique_names(self):
        names = [t["name"] for t in DEFAULT_SCHEDULES]
        assert len(names) == len(set(names))


class TestIncrementalParamMap:

    def test_all_mapped_sources_have_two_tuple(self):
        for source, mapping in INCREMENTAL_PARAM_MAP.items():
            assert len(mapping) == 2
            param_name, formatter = mapping
            assert isinstance(param_name, str)
            assert callable(formatter)

    def test_formatters_accept_datetime(self):
        dt = datetime(2025, 6, 15, 12, 30, 0)
        for source, (param_name, formatter) in INCREMENTAL_PARAM_MAP.items():
            result = formatter(dt)
            assert result is not None

    def test_expected_sources_are_mapped(self):
        expected = {"fred", "bls", "eia", "sec", "treasury", "bts", "census", "bea"}
        assert expected.issubset(set(INCREMENTAL_PARAM_MAP.keys()))


class TestSchedulerErrorHandling:

    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_create_with_all_optional_fields(self, mock_dt, mock_reg, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        schedule = create_schedule(
            db=test_db, name="Full Options", source="fred",
            config={"category": "all"},
            frequency=ScheduleFrequency.WEEKLY, hour=10, day_of_week=2,
            description="Wednesday FRED refresh", is_active=True, priority=2,
        )
        assert schedule.day_of_week == 2
        assert schedule.priority == 2
        assert schedule.description == "Wednesday FRED refresh"

    @pytest.mark.asyncio
    @patch("app.core.scheduler_service.get_session_factory")
    async def test_run_scheduled_job_handles_db_error(self, mock_factory):
        db = MagicMock()
        mock_factory.return_value = MagicMock(return_value=db)
        db.query.side_effect = Exception("connection lost")
        await run_scheduled_job(1)
        db.close.assert_called_once()

    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_create_with_custom_cron(self, mock_dt, mock_reg, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        schedule = create_schedule(
            db=test_db, name="Custom Cron", source="fred",
            config={"category": "all"},
            frequency=ScheduleFrequency.CUSTOM,
            cron_expression="0 6 * * 1-5", is_active=True,
        )
        assert schedule.frequency == ScheduleFrequency.CUSTOM
        assert schedule.cron_expression == "0 6 * * 1-5"


class TestGetScheduleHistory:

    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_returns_empty_for_nonexistent(self, mock_dt, mock_reg, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        result = get_schedule_history(test_db, schedule_id=9999)
        assert result == []

    @patch("app.core.scheduler_service.register_schedule", return_value=True)
    @patch("app.core.scheduler_service.datetime")
    def test_returns_jobs_for_existing_schedule(self, mock_dt, mock_reg, test_db):
        mock_dt.utcnow.return_value = datetime(2025, 6, 1, 0, 0, 0)
        schedule = create_schedule(
            db=test_db, name="History Test", source="fred",
            config={"category": "rates"}, is_active=False,
        )
        job = IngestionJob(
            source="fred", status=JobStatus.SUCCESS,
            config={"category": "rates"},
        )
        test_db.add(job)
        test_db.commit()
        history = get_schedule_history(test_db, schedule.id, limit=5)
        assert len(history) >= 1
