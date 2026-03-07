"""
Unit tests for #9: Stuck Job Auto-Retry.

Tests that cleanup_stuck_jobs() triggers auto_schedule_retry() for
stuck jobs that have retries remaining, and skips exhausted ones.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock

from app.core.models import IngestionJob, JobStatus


def _make_stuck_job(
    job_id=1,
    source="fred",
    retry_count=0,
    max_retries=3,
    started_hours_ago=8,
):
    """Build a stuck IngestionJob."""
    job = IngestionJob()
    job.id = job_id
    job.source = source
    job.status = JobStatus.RUNNING
    job.started_at = datetime.utcnow() - timedelta(hours=started_hours_ago)
    job.retry_count = retry_count
    job.max_retries = max_retries
    job.completed_at = None
    job.error_message = None
    return job


class TestStuckJobAutoRetry:
    """Tests for auto-retry logic in cleanup_stuck_jobs."""

    @pytest.mark.asyncio
    @patch("app.core.source_config_service.get_timeout_seconds", return_value=21600)
    @patch("app.core.webhook_service.notify_cleanup_completed", new_callable=AsyncMock)
    async def test_cleanup_triggers_retry_for_retryable_jobs(
        self, mock_notify, mock_timeout
    ):
        """Stuck jobs with retries remaining should be auto-retried."""
        from app.core.scheduler_service import cleanup_stuck_jobs

        db = MagicMock()
        stuck_job = _make_stuck_job(job_id=42, retry_count=0, max_retries=3)
        db.query.return_value.filter.return_value.all.return_value = [stuck_job]

        # After FAILED commit, re-query returns the now-FAILED job
        failed_job = _make_stuck_job(job_id=42, retry_count=0, max_retries=3)
        failed_job.status = JobStatus.FAILED
        db.query.return_value.filter.return_value.first.return_value = failed_job

        mock_factory = MagicMock(return_value=db)

        with patch(
            "app.core.scheduler_service.get_session_factory",
            return_value=mock_factory,
        ):
            with patch(
                "app.core.retry_service.auto_schedule_retry", return_value=True
            ) as mock_retry:
                result = await cleanup_stuck_jobs()

        assert result["cleaned_up"] == 1
        assert result["retried"] == 1
        assert result["jobs"][0].get("retry_scheduled") is True
        mock_retry.assert_called_once()

    @pytest.mark.asyncio
    @patch("app.core.source_config_service.get_timeout_seconds", return_value=21600)
    @patch("app.core.webhook_service.notify_cleanup_completed", new_callable=AsyncMock)
    async def test_cleanup_skips_exhausted_retries(
        self, mock_notify, mock_timeout
    ):
        """Stuck jobs that have exhausted retries should NOT be retried."""
        from app.core.scheduler_service import cleanup_stuck_jobs

        db = MagicMock()
        stuck_job = _make_stuck_job(job_id=99, retry_count=3, max_retries=3)
        db.query.return_value.filter.return_value.all.return_value = [stuck_job]

        exhausted_job = _make_stuck_job(job_id=99, retry_count=3, max_retries=3)
        exhausted_job.status = JobStatus.FAILED
        db.query.return_value.filter.return_value.first.return_value = exhausted_job

        mock_factory = MagicMock(return_value=db)

        with patch(
            "app.core.scheduler_service.get_session_factory",
            return_value=mock_factory,
        ):
            with patch(
                "app.core.retry_service.auto_schedule_retry"
            ) as mock_retry:
                result = await cleanup_stuck_jobs()

        assert result["cleaned_up"] == 1
        assert result["retried"] == 0
        mock_retry.assert_not_called()

    @pytest.mark.asyncio
    @patch("app.core.source_config_service.get_timeout_seconds", return_value=21600)
    async def test_cleanup_return_dict_has_retried_key(self, mock_timeout):
        """Return dict should always include 'retried' count."""
        from app.core.scheduler_service import cleanup_stuck_jobs

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        mock_factory = MagicMock(return_value=db)

        with patch(
            "app.core.scheduler_service.get_session_factory",
            return_value=mock_factory,
        ):
            result = await cleanup_stuck_jobs()

        assert "retried" in result
        assert result["retried"] == 0
