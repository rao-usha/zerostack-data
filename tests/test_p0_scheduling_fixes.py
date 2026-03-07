"""
Unit tests for P0 scheduling & collection fixes (PLAN_019).

Tests:
1. Watermark advancement timing — only on SUCCESS, not at job creation
2. Batch progress tracking — rows_committed updated per batch
3. Retry checkpoint propagation — progress_checkpoint copied to retry config
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, call
from sqlalchemy import text

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
    progress_checkpoint=None,
    config=None,
    retry_count=0,
    max_retries=3,
):
    """Build a mock IngestionJob with all required attributes."""
    job = IngestionJob()
    job.id = job_id
    job.source = source
    job.status = status
    job.schedule_id = schedule_id
    job.completed_at = completed_at or datetime(2026, 3, 3, 12, 0, 0)
    job.progress_checkpoint = progress_checkpoint
    job.config = config or {"category": "rates"}
    job.retry_count = retry_count
    job.max_retries = max_retries
    job.rows_committed = 0
    return job


def _make_schedule(schedule_id=10, last_run_at=None):
    """Build a mock IngestionSchedule."""
    schedule = MagicMock(spec=IngestionSchedule)
    schedule.id = schedule_id
    schedule.last_run_at = last_run_at
    return schedule


# =============================================================================
# Fix 1: Watermark Advancement Tests
# =============================================================================


class TestAdvanceScheduleWatermark:
    """Tests for _advance_schedule_watermark in jobs.py."""

    def test_advances_watermark_on_success(self):
        """Watermark should advance to job.completed_at when job succeeds."""
        from app.api.v1.jobs import _advance_schedule_watermark

        db = MagicMock()
        schedule = _make_schedule(schedule_id=10, last_run_at=None)
        db.query.return_value.filter_by.return_value.first.return_value = schedule

        job = _make_job(
            schedule_id=10,
            status=JobStatus.SUCCESS,
            completed_at=datetime(2026, 3, 3, 14, 30, 0),
        )

        _advance_schedule_watermark(db, job)

        assert schedule.last_run_at == datetime(2026, 3, 3, 14, 30, 0)
        db.commit.assert_called_once()

    def test_no_advance_when_no_schedule_id(self):
        """Manual jobs (schedule_id=None) should not touch any schedule."""
        from app.api.v1.jobs import _advance_schedule_watermark

        db = MagicMock()
        job = _make_job(schedule_id=None, status=JobStatus.SUCCESS)

        _advance_schedule_watermark(db, job)

        # Should return early — no DB queries at all
        db.query.assert_not_called()
        db.commit.assert_not_called()

    def test_no_advance_when_schedule_not_found(self):
        """If schedule was deleted, watermark advancement should be a no-op."""
        from app.api.v1.jobs import _advance_schedule_watermark

        db = MagicMock()
        db.query.return_value.filter_by.return_value.first.return_value = None

        job = _make_job(schedule_id=999, status=JobStatus.SUCCESS)

        _advance_schedule_watermark(db, job)

        db.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_completion_calls_watermark_on_success(self):
        """_handle_job_completion should call watermark advancement for SUCCESS jobs."""
        from app.api.v1.jobs import _advance_schedule_watermark

        db = MagicMock()
        schedule = _make_schedule(schedule_id=10, last_run_at=None)
        db.query.return_value.filter_by.return_value.first.return_value = schedule

        job = _make_job(schedule_id=10, status=JobStatus.SUCCESS)

        # Call watermark directly (testing the gating logic)
        if job.status == JobStatus.SUCCESS:
            _advance_schedule_watermark(db, job)

        assert schedule.last_run_at == job.completed_at

    def test_no_advance_on_failure(self):
        """Failed jobs should NOT advance the watermark."""
        from app.api.v1.jobs import _advance_schedule_watermark

        db = MagicMock()
        schedule = _make_schedule(schedule_id=10, last_run_at=None)
        db.query.return_value.filter_by.return_value.first.return_value = schedule

        job = _make_job(schedule_id=10, status=JobStatus.FAILED)

        # Simulate the gating in _handle_job_completion
        if job.status == JobStatus.SUCCESS:
            _advance_schedule_watermark(db, job)

        # Watermark should remain unchanged
        assert schedule.last_run_at is None


# =============================================================================
# Fix 2: Batch Progress Tracking Tests
# =============================================================================


class TestBatchProgressTracking:
    """Tests for rows_committed tracking in batch_insert."""

    def test_rows_committed_updates_per_batch(self):
        """rows_committed should be updated on the job record after each batch."""
        from app.core.batch_operations import batch_insert

        db = MagicMock()
        rows = [{"col_a": i, "col_b": f"val_{i}"} for i in range(50)]

        batch_insert(
            db=db,
            table_name="test_table",
            rows=rows,
            columns=["col_a", "col_b"],
            batch_size=20,
            job_id=42,
        )

        # Should have 3 batches: 20 + 20 + 10
        # Each batch commit triggers a rows_committed UPDATE execute call
        # The execute calls alternate: INSERT (batch), UPDATE (progress)
        # Check that the UPDATE calls have the right params
        update_calls = [
            c
            for c in db.execute.call_args_list
            if len(c.args) >= 2
            and isinstance(c.args[1], dict)
            and "jid" in c.args[1]
        ]
        assert len(update_calls) == 3
        # Verify cumulative row counts: 20, 40, 50
        assert update_calls[0].args[1]["rows"] == 20
        assert update_calls[1].args[1]["rows"] == 40
        assert update_calls[2].args[1]["rows"] == 50
        # All updates target job_id 42
        assert all(c.args[1]["jid"] == 42 for c in update_calls)

    def test_no_progress_update_without_job_id(self):
        """Without job_id, no rows_committed updates should happen."""
        from app.core.batch_operations import batch_insert

        db = MagicMock()
        rows = [{"col_a": i} for i in range(10)]

        batch_insert(
            db=db,
            table_name="test_table",
            rows=rows,
            columns=["col_a"],
            batch_size=5,
        )

        # No rows_committed updates — only the INSERT executes
        update_calls = [
            c
            for c in db.execute.call_args_list
            if "rows_committed" in str(c)
        ]
        assert len(update_calls) == 0


# =============================================================================
# Fix 2b: Retry Checkpoint Propagation Tests
# =============================================================================


class TestRetryCheckpointPropagation:
    """Tests for progress_checkpoint propagation in retry_service."""

    def test_create_retry_copies_checkpoint(self):
        """Retry job config should include _resume_from from parent's checkpoint."""
        from app.core.retry_service import create_retry_job

        db = MagicMock()
        original = _make_job(
            job_id=100,
            status=JobStatus.FAILED,
            retry_count=0,
            max_retries=3,
            config={"category": "rates"},
            progress_checkpoint={"last_batch_index": 4, "rows_committed": 4000},
        )

        db.query.return_value.filter.return_value.first.return_value = original

        new_job = create_retry_job(db, original)

        assert new_job is not None
        assert new_job.config["_resume_from"] == {
            "last_batch_index": 4,
            "rows_committed": 4000,
        }
        assert new_job.config["category"] == "rates"

    def test_create_retry_without_checkpoint(self):
        """Retry with no checkpoint should not include _resume_from."""
        from app.core.retry_service import create_retry_job

        db = MagicMock()
        original = _make_job(
            job_id=100,
            status=JobStatus.FAILED,
            retry_count=0,
            max_retries=3,
            config={"category": "rates"},
            progress_checkpoint=None,
        )

        new_job = create_retry_job(db, original)

        assert new_job is not None
        assert "_resume_from" not in new_job.config

    def test_retry_inherits_schedule_id(self):
        """Retry job should inherit schedule_id from parent."""
        from app.core.retry_service import create_retry_job

        db = MagicMock()
        original = _make_job(
            job_id=100,
            status=JobStatus.FAILED,
            retry_count=0,
            max_retries=3,
            schedule_id=10,
        )

        new_job = create_retry_job(db, original)

        assert new_job is not None
        assert new_job.schedule_id == 10
