"""
Tests for SPEC 022 — Collection Recommendations Engine.
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta


@pytest.mark.unit
class TestSpec022CollectionRecommender:
    """Tests for collection recommendation engine."""

    def _make_watermark(self, hours_ago):
        wm = MagicMock()
        wm.last_success_at = datetime.utcnow() - timedelta(hours=hours_ago)
        wm.last_job_id = 1
        return wm

    def _make_config(self, frequency="daily", enabled=True):
        cfg = MagicMock()
        cfg.schedule_frequency = frequency
        cfg.enabled = enabled
        cfg.supports_incremental = False
        return cfg

    def _make_job(self, status="success", hours_ago=1, rows=1000):
        job = MagicMock()
        job.status = status
        job.created_at = datetime.utcnow() - timedelta(hours=hours_ago)
        job.completed_at = datetime.utcnow() - timedelta(hours=hours_ago - 0.5)
        job.rows_inserted = rows
        job.error_message = None if status == "success" else "error"
        job.source = "test"
        job.id = 1
        return job

    def _mock_db_for_recs(self, sources_health, configs, watermarks, job_lists):
        """
        Build a mock DB that supports generate_recommendations flow.

        sources_health: list of health dicts (from get_all_source_health)
        configs: dict of source -> config mock
        watermarks: dict of source -> watermark mock
        job_lists: dict of source -> list of job mocks
        """
        db = MagicMock()

        def mock_query(model):
            q = MagicMock()
            filter_mock = MagicMock()
            q.filter.return_value = filter_mock

            model_name = model.__name__ if hasattr(model, '__name__') else str(model)

            if model_name == 'SourceConfig':
                def config_first(**kwargs):
                    # Extract source from the filter call
                    return None
                # We need to track what source is being queried
                filter_mock.first.side_effect = lambda: None
            elif model_name == 'SourceWatermark':
                filter_mock.first.side_effect = lambda: None
            elif model_name == 'IngestionJob':
                filter_mock.filter.return_value = filter_mock
                filter_mock.order_by.return_value = filter_mock
                filter_mock.limit.return_value = filter_mock
                filter_mock.all.return_value = []
            return q

        db.query.side_effect = mock_query
        return db

    def test_recommendations_stale_source(self):
        """T1: Stale source gets collect_now recommendation."""
        from app.core.collection_recommender import generate_recommendations

        stale_health = [
            {"source": "stale_src", "score": 30, "tier": "Critical",
             "components": {"freshness": 10, "reliability": 80, "coverage": 50, "consistency": 50}},
        ]

        db = MagicMock()

        with patch("app.core.collection_recommender.get_all_source_health", return_value=stale_health):
            # Config: daily source
            config_mock = self._make_config("daily", enabled=True)
            watermark_mock = self._make_watermark(hours_ago=72)  # 3 days old for daily

            def mock_query(model):
                q = MagicMock()
                f = MagicMock()
                q.filter.return_value = f
                model_name = model.__name__ if hasattr(model, '__name__') else str(model)
                if model_name == 'SourceConfig':
                    f.first.return_value = config_mock
                elif model_name == 'SourceWatermark':
                    f.first.return_value = watermark_mock
                elif model_name == 'IngestionJob':
                    f.order_by.return_value = f
                    f.limit.return_value = f
                    f.all.return_value = [self._make_job("success", 72)]
                return q

            db.query.side_effect = mock_query

            recs = generate_recommendations(db)

        assert len(recs) >= 1
        assert recs[0]["source"] == "stale_src"
        assert recs[0]["action"] == "collect_now"

    def test_recommendations_failing_source(self):
        """T2: Repeatedly failing source gets investigate recommendation."""
        from app.core.collection_recommender import generate_recommendations

        failing_health = [
            {"source": "broken_src", "score": 25, "tier": "Critical",
             "components": {"freshness": 0, "reliability": 20, "coverage": 30, "consistency": 50}},
        ]

        db = MagicMock()

        with patch("app.core.collection_recommender.get_all_source_health", return_value=failing_health):
            config_mock = self._make_config("daily", enabled=True)
            watermark_mock = self._make_watermark(hours_ago=48)

            # 5 consecutive failures
            failed_jobs = [self._make_job("failed", hours_ago=i * 4) for i in range(5)]

            def mock_query(model):
                q = MagicMock()
                f = MagicMock()
                q.filter.return_value = f
                model_name = model.__name__ if hasattr(model, '__name__') else str(model)
                if model_name == 'SourceConfig':
                    f.first.return_value = config_mock
                elif model_name == 'SourceWatermark':
                    f.first.return_value = watermark_mock
                elif model_name == 'IngestionJob':
                    f.order_by.return_value = f
                    f.limit.return_value = f
                    f.all.return_value = failed_jobs
                return q

            db.query.side_effect = mock_query
            recs = generate_recommendations(db)

        assert len(recs) >= 1
        assert any(r["action"] == "investigate" for r in recs)

    def test_recommendations_healthy_source(self):
        """T3: Healthy source gets no urgent recommendations."""
        from app.core.collection_recommender import generate_recommendations

        healthy_health = [
            {"source": "good_src", "score": 95, "tier": "Healthy",
             "components": {"freshness": 95, "reliability": 100, "coverage": 90, "consistency": 90}},
        ]

        db = MagicMock()

        with patch("app.core.collection_recommender.get_all_source_health", return_value=healthy_health):
            config_mock = self._make_config("daily", enabled=True)
            watermark_mock = self._make_watermark(hours_ago=6)  # fresh

            success_jobs = [self._make_job("success", hours_ago=i * 6) for i in range(5)]

            def mock_query(model):
                q = MagicMock()
                f = MagicMock()
                q.filter.return_value = f
                model_name = model.__name__ if hasattr(model, '__name__') else str(model)
                if model_name == 'SourceConfig':
                    f.first.return_value = config_mock
                elif model_name == 'SourceWatermark':
                    f.first.return_value = watermark_mock
                elif model_name == 'IngestionJob':
                    f.order_by.return_value = f
                    f.limit.return_value = f
                    f.all.return_value = success_jobs
                return q

            db.query.side_effect = mock_query
            recs = generate_recommendations(db)

        # Healthy source should have no urgent recommendations
        urgent = [r for r in recs if r["action"] in ("collect_now", "investigate", "disable")]
        assert len(urgent) == 0

    def test_optimal_plan_respects_concurrency(self):
        """T4: Plan respects max_concurrent limit."""
        from app.core.collection_recommender import get_optimal_collection_plan

        db = MagicMock()

        # Mock 6 stale sources needing collection
        fake_recs = [
            {"source": f"src_{i}", "priority": 1, "action": "collect_now",
             "reason": "stale", "health_score": 20}
            for i in range(6)
        ]

        with patch("app.core.collection_recommender.generate_recommendations", return_value=fake_recs):
            plan = get_optimal_collection_plan(db, max_concurrent=2)

        assert plan["max_concurrent"] == 2
        assert plan["total_sources"] == 6
        # Should have 3 waves of 2
        assert plan["wave_count"] == 3
        for wave in plan["waves"]:
            assert len(wave) <= 2

    def test_collection_history_stats(self):
        """T5: Returns correct success rate and avg rows."""
        from app.core.collection_recommender import get_collection_history_stats

        db = MagicMock()

        # 8 success, 2 failed
        jobs = (
            [self._make_job("success", hours_ago=i * 24, rows=1000) for i in range(8)] +
            [self._make_job("failed", hours_ago=i * 12, rows=0) for i in range(2)]
        )

        query_mock = MagicMock()
        filter_mock = MagicMock()
        query_mock.filter.return_value = filter_mock
        filter_mock.order_by.return_value = filter_mock
        filter_mock.all.return_value = jobs
        db.query.return_value = query_mock

        stats = get_collection_history_stats(db, "census", days=30)

        assert stats["source"] == "census"
        assert stats["total_runs"] == 10
        assert stats["success_count"] == 8
        assert stats["failure_count"] == 2
        assert stats["success_rate"] == 80.0
        assert stats["avg_rows_per_run"] == 1000

    def test_recommendations_disabled_source(self):
        """T6: Disabled source gets no collect_now recommendation."""
        from app.core.collection_recommender import generate_recommendations

        disabled_health = [
            {"source": "disabled_src", "score": 10, "tier": "Critical",
             "components": {"freshness": 0, "reliability": 0, "coverage": 0, "consistency": 0}},
        ]

        db = MagicMock()

        with patch("app.core.collection_recommender.get_all_source_health", return_value=disabled_health):
            config_mock = self._make_config("daily", enabled=False)

            def mock_query(model):
                q = MagicMock()
                f = MagicMock()
                q.filter.return_value = f
                model_name = model.__name__ if hasattr(model, '__name__') else str(model)
                if model_name == 'SourceConfig':
                    f.first.return_value = config_mock
                else:
                    f.first.return_value = None
                    f.order_by.return_value = f
                    f.limit.return_value = f
                    f.all.return_value = []
                return q

            db.query.side_effect = mock_query
            recs = generate_recommendations(db)

        # Disabled source should not appear in recommendations
        collect_recs = [r for r in recs if r["source"] == "disabled_src"]
        assert len(collect_recs) == 0
