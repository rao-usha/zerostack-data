"""
Unit tests for BLOCKED → PENDING promotion flow.

Tests:
- Batch launch creates BLOCKED jobs for tier 2+
- Per-job timeout still works (executor timeout)
- Promotion triggers after job completion in worker
"""

import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

from app.core.models_queue import QueueJobStatus, QueueJobType
from app.core.models import JobStatus


class TestBatchLaunchBlockedStatus:
    """Tests that batch launch creates tier 2+ jobs as BLOCKED."""

    @pytest.mark.asyncio
    @patch("app.core.nightly_batch_service.WORKER_MODE", True)
    @patch("app.core.job_splitter.get_split_config", return_value=None)
    @patch("app.core.nightly_batch_service.resolve_effective_tiers")
    @patch("app.core.nightly_batch_service.submit_job")
    async def test_tier1_pending_tier2_blocked(self, mock_submit, mock_resolve, _mock_split):
        """Tier 1 jobs should be PENDING, tier 2+ should be BLOCKED."""
        from app.core.nightly_batch_service import launch_batch_collection

        tier1 = MagicMock()
        tier1.level = 1
        tier1.priority = 10
        tier1.max_concurrent = 2
        tier1.sources = [MagicMock(key="treasury", default_config={})]

        tier2 = MagicMock()
        tier2.level = 2
        tier2.priority = 7
        tier2.max_concurrent = 2
        tier2.sources = [MagicMock(key="eia", default_config={})]

        mock_resolve.return_value = [tier1, tier2]

        db = MagicMock()
        result = await launch_batch_collection(db)

        assert mock_submit.call_count == 2

        # Check statuses passed to submit_job
        calls_by_source = {}
        for call in mock_submit.call_args_list:
            payload = call.kwargs.get("payload", {})
            status = call.kwargs.get("status")
            calls_by_source[payload["source"]] = status

        # Tier 1 → no status override (defaults to PENDING)
        assert calls_by_source["treasury"] is None
        # Tier 2 → BLOCKED
        assert calls_by_source["eia"] == QueueJobStatus.BLOCKED

    @pytest.mark.asyncio
    @patch("app.core.nightly_batch_service.WORKER_MODE", True)
    @patch("app.core.job_splitter.get_split_config", return_value=None)
    @patch("app.core.nightly_batch_service.resolve_effective_tiers")
    @patch("app.core.nightly_batch_service.submit_job")
    async def test_no_wait_for_tiers_in_payload(self, mock_submit, mock_resolve, _mock_split):
        """Payloads should NOT contain wait_for_tiers anymore."""
        from app.core.nightly_batch_service import launch_batch_collection

        tiers = []
        for level in [1, 2, 3]:
            t = MagicMock()
            t.level = level
            t.priority = 10 - level
            t.max_concurrent = 2
            t.sources = [MagicMock(key=f"source_{level}", default_config={})]
            tiers.append(t)

        mock_resolve.return_value = tiers
        db = MagicMock()
        await launch_batch_collection(db)

        for call in mock_submit.call_args_list:
            payload = call.kwargs.get("payload", {})
            assert "wait_for_tiers" not in payload

    @pytest.mark.asyncio
    @patch("app.core.nightly_batch_service.WORKER_MODE", True)
    @patch("app.core.job_splitter.get_split_config", return_value=None)
    @patch("app.core.nightly_batch_service.resolve_effective_tiers")
    @patch("app.core.nightly_batch_service.submit_job")
    async def test_ingestion_job_status_matches(self, mock_submit, mock_resolve, _mock_split):
        """IngestionJob should also be BLOCKED for tier 2+."""
        from app.core.nightly_batch_service import launch_batch_collection, IngestionJob

        tier1 = MagicMock()
        tier1.level = 1
        tier1.priority = 10
        tier1.max_concurrent = 2
        tier1.sources = [MagicMock(key="treasury", default_config={})]

        tier2 = MagicMock()
        tier2.level = 2
        tier2.priority = 7
        tier2.max_concurrent = 2
        tier2.sources = [MagicMock(key="eia", default_config={})]

        mock_resolve.return_value = [tier1, tier2]

        created_jobs = []
        original_init = IngestionJob.__init__

        db = MagicMock()

        # Capture the status passed to IngestionJob constructor
        with patch.object(IngestionJob, "__init__", lambda self, **kw: created_jobs.append(kw) or original_init(self, **kw)):
            await launch_batch_collection(db)

        statuses = {j["source"]: j["status"] for j in created_jobs}
        assert statuses["treasury"] == JobStatus.PENDING
        assert statuses["eia"] == JobStatus.BLOCKED


class TestPerJobTimeout:
    """Tests for per-job execution timeout in execute_job."""

    @pytest.mark.asyncio
    async def test_job_timeout_marks_failed(self):
        """When job exceeds source timeout, it should be marked FAILED with timeout error."""
        from app.worker.main import execute_job, EXECUTORS

        db = MagicMock()
        db.get = MagicMock()

        job = MagicMock()
        job.id = 100
        job.job_type = QueueJobType.INGESTION.value
        job.payload = {"source": "test_source"}
        job.priority = 5
        job.status = None
        job.error_message = None

        db.get.return_value = job

        async def slow_executor(j, d):
            await asyncio.sleep(3600)

        EXECUTORS[QueueJobType.INGESTION] = slow_executor

        async def mock_heartbeat(factory, job_id):
            await asyncio.sleep(3600)

        with patch("app.core.source_config_service.get_timeout_seconds", return_value=0.1):
            with patch("app.worker.main.get_session_factory") as mock_factory:
                mock_session_factory = MagicMock()
                mock_session = MagicMock()
                mock_session_factory.return_value = mock_session
                mock_factory.return_value = mock_session_factory
                with patch("app.worker.main._heartbeat_loop", side_effect=mock_heartbeat):
                    with patch("app.worker.main.send_job_event"):
                        await execute_job(job, db)

        assert job.status == QueueJobStatus.FAILED
        assert "timed out" in (job.error_message or "").lower()
