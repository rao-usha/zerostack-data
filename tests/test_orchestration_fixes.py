"""
Tests for orchestration bug fixes (PLAN_020).

Covers:
1. Duplicate scheduled job guard — skip if PENDING/RUNNING job exists
2. _handle_job_completion called after cancel and pre-flight failure
3. Watermark injection failure raises RuntimeError (fails the job)
4. wait_for_tiers computed from effective tiers (not hardcoded range)
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, AsyncMock

from app.core.models import IngestionJob, IngestionSchedule, JobStatus


# =============================================================================
# Helpers
# =============================================================================


def _make_job(
    job_id=1,
    source="fred",
    status=JobStatus.SUCCESS,
    schedule_id=None,
    completed_at=None,
    config=None,
    batch_run_id=None,
    trigger=None,
    tier=None,
):
    job = IngestionJob()
    job.id = job_id
    job.source = source
    job.status = status
    job.schedule_id = schedule_id
    job.completed_at = completed_at or datetime(2026, 3, 4, 12, 0, 0)
    job.config = config or {}
    job.batch_run_id = batch_run_id
    job.trigger = trigger
    job.tier = tier
    job.error_message = None
    job.rows_committed = 0
    job.retry_count = 0
    job.max_retries = 3
    job.created_at = datetime(2026, 3, 4, 11, 0, 0)
    return job


def _make_schedule(schedule_id=10, source="fred", is_active=True, last_run_at=None):
    sched = MagicMock(spec=IngestionSchedule)
    sched.id = schedule_id
    sched.source = source
    sched.name = f"test-{source}"
    sched.is_active = is_active
    sched.last_run_at = last_run_at
    sched.config = {"category": "rates"}
    return sched


# =============================================================================
# Fix 1: Duplicate Scheduled Job Guard
# =============================================================================


class TestDuplicateScheduledJobGuard:
    """run_scheduled_job should skip if a PENDING/RUNNING job already exists."""

    @pytest.mark.asyncio
    @patch("app.core.scheduler_service._execute_ingestion_job", new_callable=AsyncMock)
    @patch("app.core.scheduler_service._calculate_next_run")
    @patch("app.core.scheduler_service.get_session_factory")
    async def test_skips_when_active_job_exists(
        self, mock_factory, mock_calc_next, mock_execute
    ):
        """Should NOT create a new job when one is already RUNNING."""
        from app.core.scheduler_service import run_scheduled_job

        schedule = _make_schedule(schedule_id=10, source="fred")
        active_job = _make_job(job_id=99, status=JobStatus.RUNNING, schedule_id=10)

        db = MagicMock()
        mock_factory.return_value = MagicMock(return_value=db)

        # Chain: db.query(Model).filter(...).first()
        def query_side_effect(model):
            q = MagicMock()
            if model == IngestionSchedule:
                q.filter.return_value.first.return_value = schedule
            elif model == IngestionJob:
                q.filter.return_value.first.return_value = active_job
            return q

        db.query.side_effect = query_side_effect

        await run_scheduled_job(10)

        # Should NOT have executed any job
        mock_execute.assert_not_called()

    @pytest.mark.asyncio
    @patch("app.core.scheduler_service._execute_ingestion_job", new_callable=AsyncMock)
    @patch("app.core.scheduler_service._calculate_next_run", return_value=datetime(2026, 3, 5))
    @patch("app.core.scheduler_service.get_session_factory")
    async def test_proceeds_when_no_active_job(
        self, mock_factory, mock_calc_next, mock_execute
    ):
        """Should create and execute a job when no active job exists."""
        from app.core.scheduler_service import run_scheduled_job

        schedule = _make_schedule(schedule_id=10, source="fred")

        db = MagicMock()
        mock_factory.return_value = MagicMock(return_value=db)

        def query_side_effect(model):
            q = MagicMock()
            if model == IngestionSchedule:
                q.filter.return_value.first.return_value = schedule
            elif model == IngestionJob:
                # No active job found
                q.filter.return_value.first.return_value = None
            return q

        db.query.side_effect = query_side_effect

        await run_scheduled_job(10)

        # Should have executed (we don't count db.add since audit log also adds)
        mock_execute.assert_called_once()


# =============================================================================
# Fix 2: _handle_job_completion After Cancel & Pre-flight
# =============================================================================


class TestCompletionAfterCancel:
    """cancel_job endpoint should call _handle_job_completion."""

    @pytest.mark.asyncio
    @patch("app.api.v1.jobs._handle_job_completion", new_callable=AsyncMock)
    async def test_cancel_calls_completion_handler(self, mock_completion):
        """Cancelling a job should trigger completion handler for dependents."""
        from app.api.v1.jobs import cancel_job

        job = _make_job(job_id=5, status=JobStatus.RUNNING, batch_run_id="batch_123")

        db = MagicMock()
        call_count = [0]

        def query_side_effect(model):
            call_count[0] += 1
            q = MagicMock()
            if call_count[0] == 1:
                q.filter.return_value.first.return_value = job
            else:
                q.filter.return_value.first.return_value = None
            return q

        db.query.side_effect = query_side_effect

        with patch("app.api.v1.jobs.JobResponse") as mock_response:
            mock_response.model_validate.return_value = MagicMock()
            await cancel_job(job_id=5, db=db)

        mock_completion.assert_called_once_with(db, job)

    @pytest.mark.asyncio
    @patch("app.api.v1.jobs._handle_job_completion", new_callable=AsyncMock)
    async def test_cancel_completion_error_does_not_fail_cancel(self, mock_completion):
        """If completion handler errors, cancel should still succeed."""
        mock_completion.side_effect = Exception("completion error")

        from app.api.v1.jobs import cancel_job

        job = _make_job(job_id=5, status=JobStatus.RUNNING)

        db = MagicMock()
        call_count = [0]

        def query_side_effect(model):
            call_count[0] += 1
            q = MagicMock()
            if call_count[0] == 1:
                q.filter.return_value.first.return_value = job
            else:
                q.filter.return_value.first.return_value = None
            return q

        db.query.side_effect = query_side_effect

        with patch("app.api.v1.jobs.JobResponse") as mock_response:
            mock_response.model_validate.return_value = MagicMock()
            # Should not raise even though completion handler fails
            result = await cancel_job(job_id=5, db=db)
            assert result is not None


class TestCompletionAfterPreflight:
    """Pre-flight failure should call _handle_job_completion."""

    @pytest.mark.asyncio
    @patch("app.api.v1.jobs._handle_job_completion", new_callable=AsyncMock)
    @patch("app.api.v1.jobs._check_api_key_preflight", return_value="API key required for 'eia'")
    @patch("app.core.database.get_session_factory")
    async def test_preflight_failure_calls_completion(
        self, mock_factory, mock_preflight, mock_completion
    ):
        """Pre-flight API key check failure should trigger completion handler."""
        from app.api.v1.jobs import run_ingestion_job

        job = _make_job(job_id=7, source="eia", status=JobStatus.PENDING)

        db = MagicMock()
        mock_factory.return_value = MagicMock(return_value=db)
        db.query.return_value.filter.return_value.first.return_value = job

        await run_ingestion_job(7, "eia", {})

        mock_completion.assert_called_once()
        assert job.status == JobStatus.FAILED
        assert "API key required" in job.error_message


# =============================================================================
# Fix 3: Watermark Injection Failure Raises RuntimeError
# =============================================================================


class TestWatermarkInjectionFailure:
    """Watermark injection failure should raise, not silently continue."""

    @pytest.mark.asyncio
    async def test_raises_on_watermark_injection_error(self):
        """If watermark injection fails, executor should raise RuntimeError."""
        from app.worker.executors.ingestion import execute

        job = MagicMock()
        job.payload = {
            "source": "fred",
            "config": {"incremental": True},
            "ingestion_job_id": 42,
            "trigger": "batch",
        }
        job.id = 1

        db = MagicMock()

        with patch(
            "app.core.watermark_service.inject_incremental_from_watermark",
            side_effect=Exception("DB connection lost"),
        ):
            with pytest.raises(RuntimeError, match="Watermark injection failed"):
                await execute(job, db)

    @pytest.mark.asyncio
    async def test_no_error_when_not_incremental(self):
        """Non-incremental jobs should not call watermark injection at all."""
        from app.worker.executors.ingestion import execute

        job = MagicMock()
        job.payload = {
            "source": "fred",
            "config": {},  # No "incremental" flag
            "ingestion_job_id": 42,
            "trigger": "batch",
        }
        job.id = 1
        job.progress_pct = 0
        job.progress_message = ""

        db = MagicMock()

        with patch(
            "app.core.watermark_service.inject_incremental_from_watermark"
        ) as mock_inject, patch(
            "app.api.v1.jobs.run_ingestion_job", new_callable=AsyncMock
        ):
            # Will fail at later stage (run_ingestion_job mock), but watermark should not be called
            try:
                await execute(job, db)
            except Exception:
                pass

            mock_inject.assert_not_called()


# =============================================================================
# Fix 4: BLOCKED Status for Tier Dependencies
# =============================================================================


class TestBlockedStatusBatchLaunch:
    """Tier 2+ jobs should be created as BLOCKED, not with wait_for_tiers."""

    @pytest.mark.asyncio
    @patch("app.core.nightly_batch_service.WORKER_MODE", True)
    @patch("app.core.nightly_batch_service.resolve_effective_tiers")
    @patch("app.core.nightly_batch_service.submit_job")
    async def test_skipped_tier_blocked_correctly(self, mock_submit, mock_resolve):
        """If Tier 2 is disabled, Tier 3 should be BLOCKED (has lower tier 1)."""
        from app.core.nightly_batch_service import launch_batch_collection
        from app.core.models_queue import QueueJobStatus

        tier1 = MagicMock()
        tier1.level = 1
        tier1.priority = 10
        tier1.max_concurrent = 2
        tier1.sources = [MagicMock(key="treasury", default_config={})]

        tier3 = MagicMock()
        tier3.level = 3
        tier3.priority = 5
        tier3.max_concurrent = 3
        tier3.sources = [MagicMock(key="bls", default_config={})]

        mock_resolve.return_value = [tier1, tier3]

        db = MagicMock()
        await launch_batch_collection(db)

        assert mock_submit.call_count == 2

        statuses = {}
        for call in mock_submit.call_args_list:
            payload = call.kwargs.get("payload", {})
            status = call.kwargs.get("status")
            statuses[payload["source"]] = status

        # Tier 1 → PENDING (no status override)
        assert statuses["treasury"] is None
        # Tier 3 → BLOCKED
        assert statuses["bls"] == QueueJobStatus.BLOCKED

    @pytest.mark.asyncio
    @patch("app.core.nightly_batch_service.WORKER_MODE", True)
    @patch("app.core.nightly_batch_service.resolve_effective_tiers")
    @patch("app.core.nightly_batch_service.submit_job")
    async def test_all_tiers_blocked_status(self, mock_submit, mock_resolve):
        """With all 4 tiers, only tier 1 is PENDING; 2-4 are BLOCKED."""
        from app.core.nightly_batch_service import launch_batch_collection
        from app.core.models_queue import QueueJobStatus

        tiers = []
        for level in [1, 2, 3, 4]:
            t = MagicMock()
            t.level = level
            t.priority = 10 - level
            t.max_concurrent = 2
            t.sources = [MagicMock(key=f"source_{level}", default_config={})]
            tiers.append(t)

        mock_resolve.return_value = tiers

        db = MagicMock()
        await launch_batch_collection(db)

        assert mock_submit.call_count == 4

        statuses = {}
        for call in mock_submit.call_args_list:
            payload = call.kwargs.get("payload", {})
            status = call.kwargs.get("status")
            statuses[payload["source"]] = status

        assert statuses["source_1"] is None  # PENDING
        assert statuses["source_2"] == QueueJobStatus.BLOCKED
        assert statuses["source_3"] == QueueJobStatus.BLOCKED
        assert statuses["source_4"] == QueueJobStatus.BLOCKED

    @pytest.mark.asyncio
    @patch("app.core.nightly_batch_service.WORKER_MODE", True)
    @patch("app.core.nightly_batch_service.resolve_effective_tiers")
    @patch("app.core.nightly_batch_service.submit_job")
    async def test_single_tier_not_blocked(self, mock_submit, mock_resolve):
        """With only 1 tier, it should be PENDING (no lower tiers)."""
        from app.core.nightly_batch_service import launch_batch_collection

        tier2 = MagicMock()
        tier2.level = 2
        tier2.priority = 7
        tier2.max_concurrent = 2
        tier2.sources = [MagicMock(key="eia", default_config={})]

        mock_resolve.return_value = [tier2]

        db = MagicMock()
        await launch_batch_collection(db)

        assert mock_submit.call_count == 1
        status = mock_submit.call_args_list[0].kwargs.get("status")
        assert status is None  # PENDING, no lower tiers
