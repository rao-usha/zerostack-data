"""
Tests for batch health and unstick endpoints.

Covers:
1. GET /batch/{batch_run_id}/health — per-tier breakdown, stuck detection, promotable count
2. POST /batch/{batch_run_id}/unstick — promote blocked jobs, resubmit orphans
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from app.core.models import IngestionJob, JobStatus


def _make_batch_job(
    job_id, source, status, tier=1, batch_run_id="batch_test",
    created_at=None, started_at=None,
):
    job = IngestionJob()
    job.id = job_id
    job.source = source
    job.status = status
    job.tier = tier
    job.batch_run_id = batch_run_id
    job.created_at = created_at or datetime.utcnow()
    job.started_at = started_at
    job.completed_at = None
    job.config = {}
    job.error_message = None
    return job


class TestBatchHealth:
    """GET /batch/{batch_run_id}/health endpoint."""

    def test_returns_per_tier_breakdown(self):
        from app.api.v1.jobs import get_batch_health

        jobs = [
            _make_batch_job(1, "treasury", JobStatus.SUCCESS, tier=1),
            _make_batch_job(2, "fred", JobStatus.SUCCESS, tier=1),
            _make_batch_job(3, "bls", JobStatus.BLOCKED, tier=2),
            _make_batch_job(4, "eia", JobStatus.BLOCKED, tier=2),
            _make_batch_job(5, "census", JobStatus.BLOCKED, tier=3),
        ]

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = jobs

        result = get_batch_health("batch_test", db)

        assert result["total_jobs"] == 5
        assert result["by_tier"]["1"]["success"] == 2
        assert result["by_tier"]["2"]["blocked"] == 2
        assert result["by_tier"]["3"]["blocked"] == 1
        assert result["completion_pct"] == 40.0  # 2/5 terminal

    def test_detects_stuck_pending_jobs(self):
        from app.api.v1.jobs import get_batch_health

        old = datetime.utcnow() - timedelta(hours=2)
        jobs = [
            _make_batch_job(1, "treasury", JobStatus.PENDING, tier=1, created_at=old),
            _make_batch_job(2, "fred", JobStatus.SUCCESS, tier=1),
        ]

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = jobs

        result = get_batch_health("batch_test", db)

        assert result["overall_status"] == "stuck"
        assert len(result["stuck_jobs"]) == 1
        assert result["stuck_jobs"][0]["source"] == "treasury"
        assert result["stuck_jobs"][0]["reason"] == "Pending for over 1 hour"

    def test_complete_with_failures_status(self):
        from app.api.v1.jobs import get_batch_health

        jobs = [
            _make_batch_job(1, "treasury", JobStatus.SUCCESS, tier=1),
            _make_batch_job(2, "fred", JobStatus.FAILED, tier=1),
        ]

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = jobs

        result = get_batch_health("batch_test", db)

        assert result["overall_status"] == "complete_with_failures"
        assert result["completion_pct"] == 100.0

    def test_promotable_blocked_count(self):
        """Blocked tier-2 jobs should be promotable when tier-1 is all terminal."""
        from app.api.v1.jobs import get_batch_health

        jobs = [
            _make_batch_job(1, "treasury", JobStatus.SUCCESS, tier=1),
            _make_batch_job(2, "fred", JobStatus.FAILED, tier=1),
            _make_batch_job(3, "bls", JobStatus.BLOCKED, tier=2),
            _make_batch_job(4, "eia", JobStatus.BLOCKED, tier=2),
        ]

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = jobs

        result = get_batch_health("batch_test", db)

        assert result["promotable_blocked"] == 2

    def test_404_when_no_jobs(self):
        from app.api.v1.jobs import get_batch_health
        from fastapi import HTTPException

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        with pytest.raises(HTTPException) as exc_info:
            get_batch_health("nonexistent", db)
        assert exc_info.value.status_code == 404


class TestBatchUnstick:
    """POST /batch/{batch_run_id}/unstick endpoint."""

    @patch("app.core.job_queue_service.promote_blocked_jobs", return_value=5)
    def test_promotes_blocked_jobs(self, mock_promote):
        from app.api.v1.jobs import unstick_batch

        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []

        result = unstick_batch("batch_test", db)

        mock_promote.assert_called_once_with(db, "batch_test")
        assert result["promoted"] == 5
        assert result["resubmitted"] == 0

    @patch("app.core.job_queue_service.promote_blocked_jobs", return_value=0)
    def test_returns_zero_when_nothing_to_unstick(self, mock_promote):
        from app.api.v1.jobs import unstick_batch

        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []

        result = unstick_batch("batch_test", db)

        assert result["promoted"] == 0
        assert result["resubmitted"] == 0
