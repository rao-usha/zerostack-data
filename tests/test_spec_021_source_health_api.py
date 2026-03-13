"""
Tests for SPEC 021 — Source Health API Endpoints.
"""
import pytest
from unittest.mock import patch, MagicMock


@pytest.mark.unit
class TestSpec021SourceHealthAPI:
    """Tests for source health API endpoints."""

    def test_list_all_health(self):
        """T1: GET /source-health returns list sorted by score."""
        from app.api.v1.source_health import list_source_health
        import asyncio

        mock_results = [
            {"source": "bad", "score": 20, "tier": "Critical", "components": {}},
            {"source": "ok", "score": 70, "tier": "Warning", "components": {}},
        ]

        db = MagicMock()
        with patch("app.api.v1.source_health.health_service") as mock_svc:
            mock_svc.get_all_source_health.return_value = mock_results
            result = asyncio.get_event_loop().run_until_complete(
                list_source_health(db=db)
            )

        assert result["total"] == 2
        assert result["sources"][0]["source"] == "bad"
        assert result["sources"][0]["score"] == 20

    def test_health_summary(self):
        """T2: GET /source-health/summary has tier counts."""
        from app.api.v1.source_health import health_summary
        import asyncio

        mock_summary = {
            "overall_score": 65,
            "total_sources": 10,
            "by_tier": {"Healthy": 5, "Warning": 3, "Degraded": 1, "Critical": 1},
            "critical_sources": ["failing_src"],
        }

        db = MagicMock()
        with patch("app.api.v1.source_health.health_service") as mock_svc:
            mock_svc.get_health_summary.return_value = mock_summary
            result = asyncio.get_event_loop().run_until_complete(
                health_summary(db=db)
            )

        assert result["overall_score"] == 65
        assert result["total_sources"] == 10
        assert result["by_tier"]["Critical"] == 1
        assert "failing_src" in result["critical_sources"]

    def test_source_detail(self):
        """T3: GET /source-health/{source} returns full breakdown."""
        from app.api.v1.source_health import source_detail
        import asyncio

        mock_detail = {
            "source": "census",
            "score": 85,
            "tier": "Healthy",
            "components": {
                "freshness": 90,
                "reliability": 80,
                "coverage": 85,
                "consistency": 75,
            },
            "recent_jobs": [{"id": 1, "status": "success"}],
            "recommendations": [],
            "last_success_at": "2026-03-13T10:00:00",
        }

        db = MagicMock()
        with patch("app.api.v1.source_health.health_service") as mock_svc:
            mock_svc.get_source_health_detail.return_value = mock_detail
            result = asyncio.get_event_loop().run_until_complete(
                source_detail(source="census", db=db)
            )

        assert result["source"] == "census"
        assert result["score"] == 85
        assert "components" in result
        assert "recent_jobs" in result

    def test_refresh_source(self):
        """T4: POST /source-health/{source}/refresh triggers recalculation."""
        from app.api.v1.source_health import refresh_source_health
        import asyncio

        mock_detail = {
            "source": "fred",
            "score": 72,
            "tier": "Warning",
            "components": {
                "freshness": 60,
                "reliability": 90,
                "coverage": 70,
                "consistency": 65,
            },
            "recent_jobs": [],
            "recommendations": ["Source data is stale"],
            "last_success_at": "2026-03-12T08:00:00",
        }

        db = MagicMock()
        with patch("app.api.v1.source_health.health_service") as mock_svc:
            mock_svc.get_source_health_detail.return_value = mock_detail
            result = asyncio.get_event_loop().run_until_complete(
                refresh_source_health(source="fred", db=db)
            )

        assert result["refreshed"] is True
        assert result["source"] == "fred"
        assert result["score"] == 72
