"""
Tests for SPEC 106 — Job lifecycle (D25) and API keys stored in job config (D34).
"""
import importlib.util
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

REPO = Path(__file__).resolve().parents[1]


def _queue_job(job_id, batch_id=None, job_table_id=None):
    from app.core.models_queue import JobQueue, QueueJobStatus

    job = MagicMock(spec=JobQueue)
    job.id = job_id
    job.job_type = "ingestion"
    job.status = QueueJobStatus.BLOCKED
    job.worker_id = None
    job.created_at = datetime.utcnow() - timedelta(hours=6)
    job.job_table_id = job_table_id
    job.payload = {"batch_id": batch_id} if batch_id else {}
    return job


@pytest.mark.unit
class TestStaleCleanup:
    @patch("app.core.job_queue_service.promote_blocked_jobs")
    @patch("app.core.database.get_session_factory")
    def test_stale_cleanup_fails_linked_ingestion_job(self, mock_factory, mock_promote):
        """T1"""
        from app.core.job_queue_service import cancel_stale_pending_jobs
        from app.core.models import JobStatus

        qjob = _queue_job(1, batch_id="b1", job_table_id=42)
        ing = MagicMock()
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [qjob]
        db.get.return_value = ing
        mock_factory.return_value = MagicMock(return_value=db)

        assert cancel_stale_pending_jobs(max_age_hours=4) == 1
        assert ing.status == JobStatus.FAILED
        assert "no_worker_available" in ing.error_message
        assert ing.completed_at is not None

    @patch("app.core.job_queue_service.promote_blocked_jobs")
    @patch("app.core.database.get_session_factory")
    def test_stale_cleanup_promotes_batches(self, mock_factory, mock_promote):
        """T2"""
        from app.core.job_queue_service import cancel_stale_pending_jobs

        jobs = [_queue_job(1, "b1", 1), _queue_job(2, "b1", 2), _queue_job(3, "b2", 3), _queue_job(4)]
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = jobs
        mock_factory.return_value = MagicMock(return_value=db)

        cancel_stale_pending_jobs(max_age_hours=4)
        called = sorted(c.args[1] for c in mock_promote.call_args_list)
        assert called == ["b1", "b2"]


@pytest.mark.unit
class TestWorkerHeartbeat:
    def test_worker_heartbeat_model_and_migration(self):
        """T3"""
        from app.core.models_queue import WorkerHeartbeat

        cols = set(WorkerHeartbeat.__table__.columns.keys())
        assert {"worker_id", "hostname", "last_seen_at", "started_at"} <= cols
        assert WorkerHeartbeat.__tablename__ == "worker_heartbeats"

        path = REPO / "alembic" / "versions" / "0002_worker_heartbeats.py"
        spec = importlib.util.spec_from_file_location("mig0002", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.down_revision == "0001_pe_firm_people_role_type"
        executed = []
        op = MagicMock()
        op.execute.side_effect = lambda sql: executed.append(str(sql).lower())
        with patch.object(mod, "op", op):
            mod.upgrade()
        assert any("create table if not exists worker_heartbeats" in s for s in executed)

    def test_record_worker_heartbeat_upserts(self):
        """T4"""
        from app.core.job_queue_service import record_worker_heartbeat

        db = MagicMock()
        record_worker_heartbeat(db, "w-1", "host-a")
        sql = str(db.execute.call_args.args[0]).lower()
        params = db.execute.call_args.args[1]
        assert "insert into worker_heartbeats" in sql
        assert "on conflict (worker_id)" in sql
        assert params["worker_id"] == "w-1"
        db.commit.assert_called_once()


@pytest.mark.unit
class TestBatchLaunchWorkerCheck:
    @pytest.mark.asyncio
    @patch("app.core.batch_service.WORKER_MODE", True)
    @patch("app.core.batch_service.has_live_worker", return_value=False)
    async def test_launch_batch_refuses_without_live_worker(self, _hlw):
        """T5"""
        from app.core.batch_service import launch_batch_collection

        with pytest.raises(RuntimeError, match="worker"):
            await launch_batch_collection(MagicMock())

    @pytest.mark.asyncio
    @patch("app.core.batch_service.WORKER_MODE", True)
    @patch("app.core.batch_service.has_live_worker", return_value=False)
    @patch("app.core.batch_service.resolve_effective_tiers", return_value=[])
    async def test_launch_batch_force_bypasses_worker_check(self, _tiers, _hlw):
        """T6"""
        from app.core.batch_service import launch_batch_collection

        result = await launch_batch_collection(MagicMock(), force=True)
        assert result["total_jobs"] == 0


@pytest.mark.unit
class TestApiKeyStorage:
    @pytest.mark.parametrize("router", ["yelp.py", "eia.py", "us_trade.py"])
    def test_routers_do_not_store_api_key(self, router):
        """T7"""
        src = (REPO / "app" / "api" / "v1" / router).read_text(encoding="utf-8")
        assert '"api_key": api_key' not in src

    @pytest.mark.asyncio
    async def test_dispatch_injects_api_key_from_settings(self):
        """T8"""
        from app.api.v1 import jobs

        captured = {}

        async def fake_ingest(db, location=None, term=None, categories=None,
                              limit=None, api_key=None):
            captured["api_key"] = api_key
            return {"rows_inserted": 0}

        fake_module = MagicMock(ingest_businesses_by_location=fake_ingest)
        job = MagicMock()
        with patch.object(jobs.importlib, "import_module", return_value=fake_module), \
             patch.object(jobs, "_resolve_runtime_api_key", return_value="secret-from-env"), \
             patch.object(jobs, "_run_quality_gate", AsyncMock()):
            await jobs._run_dispatched_job(
                MagicMock(), job, 1, "yelp",
                {"dataset": "businesses", "location": "Austin, TX"},
                MagicMock(notify_job_completion=AsyncMock()),
            )
        assert captured["api_key"] == "secret-from-env"
