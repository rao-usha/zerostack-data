"""
Tests for SPEC 020 — Source Health Scoring Service.
"""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime, timedelta
import statistics


@pytest.mark.unit
class TestSpec020SourceHealth:
    """Tests for source health scoring engine."""

    def _mock_db(self, watermark_row=None, config_row=None,
                 job_rows=None, source_list=None):
        """Build a mock DB session with canned query results."""
        db = MagicMock()

        # Mock query().filter().first() chains
        def mock_query(model):
            q = MagicMock()
            filter_mock = MagicMock()
            q.filter.return_value = filter_mock

            model_name = model.__name__ if hasattr(model, '__name__') else str(model)

            if model_name == 'SourceWatermark':
                filter_mock.first.return_value = watermark_row
            elif model_name == 'SourceConfig':
                filter_mock.first.return_value = config_row
            elif model_name == 'IngestionJob':
                filter_mock.filter.return_value = filter_mock
                filter_mock.order_by.return_value = filter_mock
                filter_mock.limit.return_value = filter_mock
                filter_mock.all.return_value = job_rows or []
                filter_mock.count.return_value = len(job_rows) if job_rows else 0
            return q

        db.query.side_effect = mock_query

        # For get_all_source_health — distinct source list
        if source_list is not None:
            result_mock = MagicMock()
            result_mock.scalars.return_value.all.return_value = source_list
            db.execute.return_value = result_mock

        return db

    def _make_watermark(self, last_success_at):
        wm = MagicMock()
        wm.last_success_at = last_success_at
        wm.last_job_id = 100
        return wm

    def _make_config(self, schedule_frequency="daily", enabled=True):
        cfg = MagicMock()
        cfg.schedule_frequency = schedule_frequency
        cfg.enabled = enabled
        cfg.supports_incremental = False
        return cfg

    def _make_job(self, status="success", hours_ago=1, rows=1000):
        job = MagicMock()
        job.status = status
        job.created_at = datetime.utcnow() - timedelta(hours=hours_ago)
        job.completed_at = datetime.utcnow() - timedelta(hours=hours_ago - 0.5)
        job.rows_inserted = rows
        job.error_message = None if status == "success" else "test error"
        job.source = "test_source"
        job.id = 1
        return job

    def test_calculate_health_healthy_source(self):
        """T1: Source with recent success, high reliability -> Healthy tier."""
        from app.core.source_health import calculate_source_health

        now = datetime.utcnow()
        watermark = self._make_watermark(now - timedelta(hours=2))
        config = self._make_config("daily")
        # 10 successful jobs in last 7 days
        jobs = [self._make_job("success", hours_ago=i * 12, rows=1000) for i in range(10)]

        db = self._mock_db(watermark_row=watermark, config_row=config, job_rows=jobs)
        result = calculate_source_health(db, "census")

        assert result["source"] == "census"
        assert result["score"] >= 80
        assert result["tier"] == "Healthy"
        assert "components" in result
        assert result["components"]["freshness"] >= 80

    def test_calculate_health_degraded_source(self):
        """T2: Source with old data, low success rate -> Degraded/Critical."""
        from app.core.source_health import calculate_source_health

        now = datetime.utcnow()
        # Last success was 5 days ago for a daily source
        watermark = self._make_watermark(now - timedelta(days=5))
        config = self._make_config("daily")
        # 3 successes, 7 failures in last 7 days
        jobs = (
            [self._make_job("success", hours_ago=i * 24, rows=500) for i in range(3)] +
            [self._make_job("failed", hours_ago=i * 12, rows=0) for i in range(7)]
        )

        db = self._mock_db(watermark_row=watermark, config_row=config, job_rows=jobs)
        result = calculate_source_health(db, "failing_source")

        assert result["score"] < 60
        assert result["tier"] in ("Degraded", "Critical")

    def test_calculate_health_no_jobs(self):
        """T3: Source with zero ingestion history -> Critical (0)."""
        from app.core.source_health import calculate_source_health

        db = self._mock_db(watermark_row=None, config_row=None, job_rows=[])
        result = calculate_source_health(db, "new_source")

        assert result["score"] == 0
        assert result["tier"] == "Critical"

    def test_calculate_health_no_config(self):
        """T4: Source without SourceConfig row -> uses sensible defaults."""
        from app.core.source_health import calculate_source_health

        now = datetime.utcnow()
        watermark = self._make_watermark(now - timedelta(hours=6))
        # No config row — should default to 24h expected frequency
        jobs = [self._make_job("success", hours_ago=i * 6, rows=800) for i in range(5)]

        db = self._mock_db(watermark_row=watermark, config_row=None, job_rows=jobs)
        result = calculate_source_health(db, "unconfigured_source")

        # Should still produce a valid result with defaults
        assert 0 <= result["score"] <= 100
        assert result["tier"] in ("Healthy", "Warning", "Degraded", "Critical")

    def test_freshness_scoring(self):
        """T5: Freshness score decays correctly based on hours since last success."""
        from app.core.source_health import _calculate_freshness_score

        # Just succeeded — should be 100
        assert _calculate_freshness_score(0, 24) == 100

        # Half the expected window — should be ~50
        score_half = _calculate_freshness_score(24, 24)
        assert 40 <= score_half <= 60

        # Way overdue — should be 0
        assert _calculate_freshness_score(100, 24) == 0

    def test_reliability_scoring(self):
        """T6: Reliability score = success_count / total_count over 7 days."""
        from app.core.source_health import _calculate_reliability_score

        # All successes
        assert _calculate_reliability_score(10, 10) == 100

        # Half successes
        assert _calculate_reliability_score(5, 10) == 50

        # No jobs at all
        assert _calculate_reliability_score(0, 0) == 0

    def test_consistency_scoring(self):
        """T7: Low variance in row counts -> high consistency score."""
        from app.core.source_health import _calculate_consistency_score

        # Perfectly consistent
        assert _calculate_consistency_score([1000, 1000, 1000, 1000]) >= 95

        # High variance
        score_var = _calculate_consistency_score([100, 5000, 200, 8000])
        assert score_var < 50

        # Empty list
        assert _calculate_consistency_score([]) == 0

    def test_get_health_detail_structure(self):
        """T8: Detail response has all expected keys."""
        from app.core.source_health import get_source_health_detail

        now = datetime.utcnow()
        watermark = self._make_watermark(now - timedelta(hours=3))
        config = self._make_config("daily")
        jobs = [self._make_job("success", hours_ago=i * 8, rows=1000) for i in range(5)]

        db = self._mock_db(watermark_row=watermark, config_row=config, job_rows=jobs)
        result = get_source_health_detail(db, "census")

        assert "source" in result
        assert "score" in result
        assert "tier" in result
        assert "components" in result
        assert "recent_jobs" in result
        assert "recommendations" in result
        assert "last_success_at" in result

        # Components should have all 4 dimensions
        components = result["components"]
        assert "freshness" in components
        assert "reliability" in components
        assert "coverage" in components
        assert "consistency" in components

    def test_get_all_source_health_sorted(self):
        """T9: Results sorted worst-first (lowest score first)."""
        from app.core.source_health import get_all_source_health

        now = datetime.utcnow()

        # We need to mock the DB to return multiple sources
        db = MagicMock()

        # Mock execute for distinct sources query
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = ["good_source", "bad_source"]
        db.execute.return_value = result_mock

        call_count = [0]

        def mock_query(model):
            q = MagicMock()
            filter_mock = MagicMock()
            q.filter.return_value = filter_mock

            model_name = model.__name__ if hasattr(model, '__name__') else str(model)

            if model_name == 'SourceWatermark':
                # Alternate between good and bad watermarks
                if call_count[0] % 3 == 0:
                    wm = MagicMock()
                    wm.last_success_at = now - timedelta(hours=2)
                    wm.last_job_id = 100
                    filter_mock.first.return_value = wm
                else:
                    filter_mock.first.return_value = None
                call_count[0] += 1
            elif model_name == 'SourceConfig':
                cfg = MagicMock()
                cfg.schedule_frequency = "daily"
                cfg.enabled = True
                cfg.supports_incremental = False
                filter_mock.first.return_value = cfg
                call_count[0] += 1
            elif model_name == 'IngestionJob':
                filter_mock.filter.return_value = filter_mock
                filter_mock.order_by.return_value = filter_mock
                filter_mock.limit.return_value = filter_mock
                filter_mock.all.return_value = []
                filter_mock.count.return_value = 0
                call_count[0] += 1
            return q

        db.query.side_effect = mock_query

        results = get_all_source_health(db)

        assert isinstance(results, list)
        # Should be sorted worst-first
        if len(results) >= 2:
            assert results[0]["score"] <= results[1]["score"]

    def test_get_health_summary_counts(self):
        """T10: Summary has correct tier counts and overall score."""
        from app.core.source_health import get_health_summary

        db = MagicMock()

        # Mock to return 2 sources
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = ["src_a", "src_b"]
        db.execute.return_value = result_mock

        # Both sources return no data → Critical
        def mock_query(model):
            q = MagicMock()
            filter_mock = MagicMock()
            q.filter.return_value = filter_mock
            filter_mock.first.return_value = None
            filter_mock.filter.return_value = filter_mock
            filter_mock.order_by.return_value = filter_mock
            filter_mock.limit.return_value = filter_mock
            filter_mock.all.return_value = []
            filter_mock.count.return_value = 0
            return q

        db.query.side_effect = mock_query

        result = get_health_summary(db)

        assert "overall_score" in result
        assert "total_sources" in result
        assert "by_tier" in result
        assert "critical_sources" in result
        assert result["total_sources"] == 2
        assert result["overall_score"] == 0
        assert result["by_tier"]["Critical"] == 2
