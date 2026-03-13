"""
Tests for batch cancellation endpoint (#1).

Covers:
- Cancel mix of PENDING/RUNNING/SUCCESS jobs
- 404 for nonexistent batch
- Already-completed batch is no-op
- Queue rows marked too
- Idempotent second cancel
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime

from app.core.models import IngestionJob, JobStatus
from app.core.batch_service import cancel_batch_run


def _make_job(status, job_id=1, batch_run_id="batch_123"):
    """Create a mock IngestionJob."""
    job = MagicMock(spec=IngestionJob)
    job.id = job_id
    job.status = status
    job.batch_run_id = batch_run_id
    job.error_message = None
    job.completed_at = None
    return job


class TestCancelBatchRun:
    """Test cancel_batch_run() service function."""

    def test_cancel_mixed_statuses(self):
        """Batch with PENDING, RUNNING, and SUCCESS jobs."""
        pending_job = _make_job(JobStatus.PENDING, job_id=1)
        running_job = _make_job(JobStatus.RUNNING, job_id=2)
        success_job = _make_job(JobStatus.SUCCESS, job_id=3)

        db = MagicMock()
        # IngestionJob query returns all 3 jobs
        db.query.return_value.filter.return_value.all.return_value = [
            pending_job, running_job, success_job
        ]

        # JobQueue query returns None (no queue rows) — simplifies test
        inner_query = MagicMock()
        inner_query.filter.return_value.first.return_value = None

        # We need to handle the two different query calls:
        # 1. db.query(IngestionJob).filter(...).all() → jobs
        # 2. db.query(JobQueue).filter(...).first() → None (per job)
        call_count = [0]
        original_query = db.query

        def side_effect_query(*args):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: IngestionJob query
                mock_q = MagicMock()
                mock_q.filter.return_value.all.return_value = [
                    pending_job, running_job, success_job
                ]
                return mock_q
            else:
                # Subsequent calls: JobQueue queries
                mock_q = MagicMock()
                mock_q.filter.return_value.first.return_value = None
                return mock_q

        db.query.side_effect = side_effect_query

        result = cancel_batch_run(db, "batch_123")

        assert result is not None
        assert result["batch_run_id"] == "batch_123"
        assert result["cancelled_pending"] == 1
        assert result["cancelled_running"] == 1
        assert result["already_complete"] == 1
        assert result["total_jobs"] == 3

        # Verify pending job was set to FAILED
        assert pending_job.status == JobStatus.FAILED
        assert "cancelled" in pending_job.error_message.lower()
        assert pending_job.completed_at is not None

        # Verify running job was set to FAILED
        assert running_job.status == JobStatus.FAILED
        assert "cancelled" in running_job.error_message.lower()

        # Verify success job was NOT modified
        assert success_job.status == JobStatus.SUCCESS

    def test_nonexistent_batch_returns_none(self):
        """Unknown batch_run_id returns None (404 at API layer)."""
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        result = cancel_batch_run(db, "nonexistent_batch")
        assert result is None

    def test_already_completed_batch_is_noop(self):
        """Batch where all jobs are SUCCESS/FAILED — counts as already_complete."""
        success_job = _make_job(JobStatus.SUCCESS, job_id=1)
        failed_job = _make_job(JobStatus.FAILED, job_id=2)

        db = MagicMock()
        call_count = [0]

        def side_effect_query(*args):
            call_count[0] += 1
            if call_count[0] == 1:
                mock_q = MagicMock()
                mock_q.filter.return_value.all.return_value = [success_job, failed_job]
                return mock_q
            else:
                mock_q = MagicMock()
                mock_q.filter.return_value.first.return_value = None
                return mock_q

        db.query.side_effect = side_effect_query

        result = cancel_batch_run(db, "batch_done")

        assert result["cancelled_pending"] == 0
        assert result["cancelled_running"] == 0
        assert result["already_complete"] == 2

    def test_idempotent_second_cancel(self):
        """Calling cancel again on an already-cancelled batch is a no-op."""
        # All jobs already FAILED from first cancel
        job1 = _make_job(JobStatus.FAILED, job_id=1)
        job2 = _make_job(JobStatus.FAILED, job_id=2)

        db = MagicMock()
        call_count = [0]

        def side_effect_query(*args):
            call_count[0] += 1
            if call_count[0] == 1:
                mock_q = MagicMock()
                mock_q.filter.return_value.all.return_value = [job1, job2]
                return mock_q
            else:
                mock_q = MagicMock()
                mock_q.filter.return_value.first.return_value = None
                return mock_q

        db.query.side_effect = side_effect_query

        result = cancel_batch_run(db, "batch_already_cancelled")

        assert result["cancelled_pending"] == 0
        assert result["cancelled_running"] == 0
        assert result["already_complete"] == 2

    def test_queue_rows_marked_on_cancel(self):
        """JobQueue rows are also marked FAILED when batch is cancelled."""
        from app.core.models_queue import JobQueue, QueueJobStatus

        pending_job = _make_job(JobStatus.PENDING, job_id=1)
        queue_row = MagicMock(spec=JobQueue)
        queue_row.status = QueueJobStatus.PENDING
        queue_row.error_message = None
        queue_row.completed_at = None

        db = MagicMock()
        call_count = [0]

        def side_effect_query(*args):
            call_count[0] += 1
            if call_count[0] == 1:
                mock_q = MagicMock()
                mock_q.filter.return_value.all.return_value = [pending_job]
                return mock_q
            else:
                mock_q = MagicMock()
                mock_q.filter.return_value.first.return_value = queue_row
                return mock_q

        db.query.side_effect = side_effect_query

        result = cancel_batch_run(db, "batch_with_queue")

        assert result["cancelled_pending"] == 1
        assert queue_row.status == QueueJobStatus.FAILED
        assert "cancelled" in queue_row.error_message.lower()
        assert queue_row.completed_at is not None
