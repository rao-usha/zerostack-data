"""
Tests for cancel_stale_pending_jobs — auto-cancel jobs stuck pending with no worker.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from app.core.models_queue import JobQueue, QueueJobStatus


@pytest.mark.unit
class TestCancelStalePendingJobs:
    """Tests for cancel_stale_pending_jobs function."""

    def _make_job(self, status, age_hours, worker_id=None):
        job = MagicMock(spec=JobQueue)
        job.id = 1
        job.job_type = "ingestion"
        job.status = status
        job.worker_id = worker_id
        job.created_at = datetime.utcnow() - timedelta(hours=age_hours)
        job.error_message = None
        job.completed_at = None
        return job

    @patch("app.core.job_queue_service.get_session_factory")
    def test_cancels_old_pending_jobs(self, mock_factory):
        from app.core.job_queue_service import cancel_stale_pending_jobs

        old_job = self._make_job(QueueJobStatus.PENDING, age_hours=6)
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [old_job]
        mock_factory.return_value = MagicMock(return_value=db)

        result = cancel_stale_pending_jobs(max_age_hours=4)

        assert result == 1
        assert old_job.status == QueueJobStatus.FAILED
        assert "no_worker_available" in old_job.error_message
        assert old_job.completed_at is not None
        db.commit.assert_called_once()

    @patch("app.core.job_queue_service.get_session_factory")
    def test_skips_recently_created_jobs(self, mock_factory):
        from app.core.job_queue_service import cancel_stale_pending_jobs

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []
        mock_factory.return_value = MagicMock(return_value=db)

        result = cancel_stale_pending_jobs(max_age_hours=4)

        assert result == 0
        db.commit.assert_not_called()

    @patch("app.core.job_queue_service.get_session_factory")
    def test_cancels_blocked_jobs_too(self, mock_factory):
        from app.core.job_queue_service import cancel_stale_pending_jobs

        blocked_job = self._make_job(QueueJobStatus.BLOCKED, age_hours=5)
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [blocked_job]
        mock_factory.return_value = MagicMock(return_value=db)

        result = cancel_stale_pending_jobs(max_age_hours=4)

        assert result == 1
        assert blocked_job.status == QueueJobStatus.FAILED

    @patch("app.core.job_queue_service.get_session_factory")
    def test_handles_db_error(self, mock_factory):
        from app.core.job_queue_service import cancel_stale_pending_jobs

        db = MagicMock()
        db.query.side_effect = Exception("connection lost")
        mock_factory.return_value = MagicMock(return_value=db)

        result = cancel_stale_pending_jobs(max_age_hours=4)

        assert result == 0
        db.rollback.assert_called_once()
        db.close.assert_called_once()

    @patch("app.core.job_queue_service.get_session_factory")
    def test_multiple_jobs_cancelled(self, mock_factory):
        from app.core.job_queue_service import cancel_stale_pending_jobs

        jobs = [
            self._make_job(QueueJobStatus.PENDING, age_hours=10),
            self._make_job(QueueJobStatus.PENDING, age_hours=8),
            self._make_job(QueueJobStatus.BLOCKED, age_hours=6),
        ]
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = jobs
        mock_factory.return_value = MagicMock(return_value=db)

        result = cancel_stale_pending_jobs(max_age_hours=4)

        assert result == 3
        for job in jobs:
            assert job.status == QueueJobStatus.FAILED
            assert "no_worker_available" in job.error_message
