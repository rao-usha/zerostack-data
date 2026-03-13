"""
Unit tests for batch concurrency control via BLOCKED queue status.

Tests:
- Tier dataclass has max_concurrent defaults
- promote_blocked_jobs() respects concurrency limits
- promote_blocked_jobs() only promotes when lower tiers are terminal
- No double-promotion
- IngestionJob also promoted alongside queue job
"""

import pytest
from unittest.mock import MagicMock, patch

from app.core.models_queue import QueueJobStatus


class TestTierMaxConcurrentConfig:
    """Tests for max_concurrent on Tier dataclass."""

    def test_tier_has_max_concurrent_default(self):
        """Tier dataclass should have max_concurrent with default 2."""
        from app.core.batch_service import Tier

        tier = Tier(level=1, priority=10, name="Test")
        assert tier.max_concurrent == 2

    def test_tier_3_has_max_concurrent_3(self):
        """Tier 3 should have max_concurrent=3."""
        from app.core.batch_service import TIER_3

        assert TIER_3.max_concurrent == 3

    def test_tier_1_has_max_concurrent_2(self):
        """Tier 1 should have max_concurrent=2 (default)."""
        from app.core.batch_service import TIER_1

        assert TIER_1.max_concurrent == 2


class TestPromoteBlockedJobs:
    """Tests for promote_blocked_jobs()."""

    def _make_queue_job(self, job_id, tier, status, batch_id="batch_123", max_concurrent=2):
        """Create a mock JobQueue object."""
        qj = MagicMock()
        qj.id = job_id
        qj.status = status.value if hasattr(status, "value") else status
        qj.job_table_id = job_id * 100
        qj.payload = {
            "batch_id": batch_id,
            "tier": tier,
            "tier_max_concurrent": max_concurrent,
        }
        return qj

    def _make_ing_job(self, job_id, status):
        """Create a mock IngestionJob."""
        from app.core.models import JobStatus

        ij = MagicMock()
        ij.id = job_id
        ij.status = status
        return ij

    def test_promotes_tier2_when_tier1_complete(self):
        """All tier 1 jobs terminal → tier 2 BLOCKED should become PENDING."""
        from app.core.job_queue_service import promote_blocked_jobs
        from app.core.models import JobStatus

        t1_done = self._make_queue_job(1, 1, QueueJobStatus.SUCCESS)
        t2_blocked = self._make_queue_job(2, 2, QueueJobStatus.BLOCKED)

        ing_job = self._make_ing_job(200, JobStatus.BLOCKED)

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [t1_done, t2_blocked]
        db.query.return_value.filter.return_value.first.return_value = ing_job

        result = promote_blocked_jobs(db, "batch_123")

        assert result == 1
        assert t2_blocked.status == QueueJobStatus.PENDING
        assert ing_job.status == JobStatus.PENDING

    def test_no_promote_when_tier1_incomplete(self):
        """Tier 1 still running → tier 2 stays BLOCKED."""
        from app.core.job_queue_service import promote_blocked_jobs

        t1_running = self._make_queue_job(1, 1, QueueJobStatus.RUNNING)
        t2_blocked = self._make_queue_job(2, 2, QueueJobStatus.BLOCKED)

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [t1_running, t2_blocked]

        result = promote_blocked_jobs(db, "batch_123")

        assert result == 0
        assert t2_blocked.status == QueueJobStatus.BLOCKED.value

    def test_respects_max_concurrent(self):
        """Should promote at most max_concurrent jobs per tier."""
        from app.core.job_queue_service import promote_blocked_jobs

        t1_done = self._make_queue_job(1, 1, QueueJobStatus.SUCCESS)
        # 3 blocked tier 2 jobs with max_concurrent=2
        t2_b1 = self._make_queue_job(2, 2, QueueJobStatus.BLOCKED, max_concurrent=2)
        t2_b2 = self._make_queue_job(3, 2, QueueJobStatus.BLOCKED, max_concurrent=2)
        t2_b3 = self._make_queue_job(4, 2, QueueJobStatus.BLOCKED, max_concurrent=2)

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [
            t1_done, t2_b1, t2_b2, t2_b3,
        ]
        db.query.return_value.filter.return_value.first.return_value = self._make_ing_job(
            200, MagicMock()
        )

        result = promote_blocked_jobs(db, "batch_123")

        assert result == 2
        assert t2_b1.status == QueueJobStatus.PENDING
        assert t2_b2.status == QueueJobStatus.PENDING
        # Third stays blocked
        assert t2_b3.status == QueueJobStatus.BLOCKED.value

    def test_no_double_promotion(self):
        """Already PENDING jobs should not be re-promoted."""
        from app.core.job_queue_service import promote_blocked_jobs

        t1_done = self._make_queue_job(1, 1, QueueJobStatus.SUCCESS)
        t2_pending = self._make_queue_job(2, 2, QueueJobStatus.PENDING)
        t2_blocked = self._make_queue_job(3, 2, QueueJobStatus.BLOCKED)

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [
            t1_done, t2_pending, t2_blocked,
        ]
        db.query.return_value.filter.return_value.first.return_value = self._make_ing_job(
            300, MagicMock()
        )

        result = promote_blocked_jobs(db, "batch_123")

        # Only 1 promoted (the blocked one), and only 1 slot available (2 max - 1 active)
        assert result == 1
        assert t2_blocked.status == QueueJobStatus.PENDING

    def test_cascading_tiers(self):
        """Tier 1 done + Tier 2 done → Tier 3 should promote."""
        from app.core.job_queue_service import promote_blocked_jobs

        t1_done = self._make_queue_job(1, 1, QueueJobStatus.SUCCESS)
        t2_done = self._make_queue_job(2, 2, QueueJobStatus.SUCCESS)
        t3_blocked = self._make_queue_job(3, 3, QueueJobStatus.BLOCKED)

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [
            t1_done, t2_done, t3_blocked,
        ]
        db.query.return_value.filter.return_value.first.return_value = self._make_ing_job(
            300, MagicMock()
        )

        result = promote_blocked_jobs(db, "batch_123")

        assert result == 1
        assert t3_blocked.status == QueueJobStatus.PENDING

    def test_empty_batch_returns_zero(self):
        """No jobs in batch → return 0."""
        from app.core.job_queue_service import promote_blocked_jobs

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        assert promote_blocked_jobs(db, "batch_123") == 0
