"""
Tests for worker status endpoint and health check worker detection.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from app.api.v1.jobs import get_worker_status


def _make_db_mock(worker_rows=None, summary_row=None):
    """Build a mock db that returns worker_rows then summary_row."""
    db = MagicMock()
    call_count = [0]

    def fake_execute(query, params=None):
        result = MagicMock()
        if call_count[0] == 0:
            result.fetchall.return_value = worker_rows or []
            call_count[0] += 1
            return result
        else:
            result.fetchone.return_value = summary_row
            return result

    db.execute = fake_execute
    return db


@pytest.mark.unit
class TestWorkerStatusEndpoint:
    """Tests for GET /api/v1/jobs/workers."""

    def test_no_workers_no_activity(self):
        """No workers, no recent claims → worker_available=False."""
        summary = (5, 10, 0, 0, 0, 0, None, None)  # pending, blocked, claimed, running, completed_1h, failed_1h, last_claimed, last_completed
        db = _make_db_mock(worker_rows=[], summary_row=summary)

        result = get_worker_status(db=db)

        assert result["total_active_workers"] == 0
        assert result["worker_available"] is False
        assert result["last_job_claimed_at"] is None
        assert result["queue"]["pending"] == 5
        assert result["queue"]["blocked"] == 10
        assert result["queue"]["depth"] == 15

    def test_active_workers(self):
        """Workers with recent heartbeat → worker_available=True."""
        now = datetime.utcnow()
        workers = [
            ("worker-1", 2, 0, now - timedelta(seconds=30), [{"job_id": 1}]),
        ]
        summary = (3, 5, 0, 2, 10, 1, now - timedelta(minutes=1), now - timedelta(minutes=2))
        db = _make_db_mock(worker_rows=workers, summary_row=summary)

        result = get_worker_status(db=db)

        assert result["total_active_workers"] == 1
        assert result["worker_available"] is True
        assert result["queue"]["completed_1h"] == 10
        assert result["queue"]["failed_1h"] == 1

    def test_no_heartbeat_but_recent_claim(self):
        """No active heartbeat, but job claimed <10 min ago → worker_available=True."""
        now = datetime.utcnow()
        summary = (2, 0, 1, 0, 0, 0, now - timedelta(minutes=3), None)
        db = _make_db_mock(worker_rows=[], summary_row=summary)

        result = get_worker_status(db=db)

        assert result["worker_available"] is True
        assert result["last_job_claimed_at"] is not None

    def test_no_heartbeat_stale_claim(self):
        """No heartbeat and last claim >10 min ago → worker_available=False."""
        now = datetime.utcnow()
        summary = (2, 0, 0, 0, 0, 0, now - timedelta(minutes=20), None)
        db = _make_db_mock(worker_rows=[], summary_row=summary)

        result = get_worker_status(db=db)

        assert result["worker_available"] is False

    def test_queue_depth_is_pending_plus_blocked(self):
        """Queue depth = pending + blocked."""
        summary = (7, 3, 0, 0, 0, 0, None, None)
        db = _make_db_mock(worker_rows=[], summary_row=summary)

        result = get_worker_status(db=db)

        assert result["queue"]["depth"] == 10

    def test_last_completed_at_present(self):
        """last_job_completed_at populated from completed jobs."""
        now = datetime.utcnow()
        summary = (0, 0, 0, 0, 5, 0, now - timedelta(hours=1), now - timedelta(minutes=5))
        db = _make_db_mock(worker_rows=[], summary_row=summary)

        result = get_worker_status(db=db)

        assert result["last_job_completed_at"] is not None
