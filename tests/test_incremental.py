"""
Tests for incremental data loading features.

Covers:
- INCREMENTAL_PARAM_MAP constant
- _inject_incremental_params() logic
- _run_quality_gate() advisory behavior
- Freshness dashboard response shape
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from app.core.scheduler_service import (
    INCREMENTAL_PARAM_MAP,
    _inject_incremental_params,
)


# =============================================================================
# Gap 1 — Incremental param injection
# =============================================================================


class TestIncrementalParamMap:
    """Validate that every mapped source produces the right param name."""

    def test_all_sources_present(self):
        expected = {"fred", "bls", "eia", "sec", "treasury", "bts", "census", "bea"}
        assert set(INCREMENTAL_PARAM_MAP.keys()) == expected

    @pytest.mark.parametrize("source,param", [
        ("fred", "observation_start"),
        ("bls", "start_year"),
        ("eia", "start"),
        ("sec", "start_date"),
        ("treasury", "start_date"),
        ("bts", "start_date"),
        ("census", "year"),
        ("bea", "year"),
    ])
    def test_param_names(self, source, param):
        assert INCREMENTAL_PARAM_MAP[source][0] == param


class TestInjectIncrementalParams:
    """Test _inject_incremental_params under various conditions."""

    def test_no_incremental_flag_returns_unchanged(self):
        config = {"category": "interest_rates"}
        result = _inject_incremental_params(config, "fred", datetime(2025, 6, 1))
        assert result == config  # no mutation

    def test_incremental_false_returns_unchanged(self):
        config = {"category": "interest_rates", "incremental": False}
        result = _inject_incremental_params(config, "fred", datetime(2025, 6, 1))
        assert "observation_start" not in result

    def test_incremental_true_no_last_run_full_load(self):
        config = {"category": "interest_rates", "incremental": True}
        result = _inject_incremental_params(config, "fred", None)
        # Should return config unchanged (full load on first run)
        assert "observation_start" not in result

    def test_incremental_fred_injects_observation_start(self):
        config = {"category": "interest_rates", "incremental": True}
        dt = datetime(2025, 6, 15, 10, 30, 0)
        result = _inject_incremental_params(config, "fred", dt)
        assert result["observation_start"] == "2025-06-15"
        assert result["category"] == "interest_rates"

    def test_incremental_bls_injects_start_year(self):
        config = {"dataset": "ces", "incremental": True}
        dt = datetime(2025, 3, 1)
        result = _inject_incremental_params(config, "bls", dt)
        assert result["start_year"] == 2025

    def test_incremental_bea_injects_year_as_string(self):
        config = {"dataset": "gdp", "incremental": True}
        dt = datetime(2024, 12, 1)
        result = _inject_incremental_params(config, "bea", dt)
        assert result["year"] == "2024"

    def test_unknown_source_returns_unchanged(self):
        config = {"something": "value", "incremental": True}
        result = _inject_incremental_params(config, "unknown_source", datetime(2025, 1, 1))
        # Unknown source should return config as-is
        assert "observation_start" not in result
        assert result["something"] == "value"

    def test_does_not_mutate_original_config(self):
        config = {"category": "gdp", "incremental": True}
        original = dict(config)
        _inject_incremental_params(config, "fred", datetime(2025, 1, 1))
        assert config == original  # original dict unchanged

    def test_none_config_returns_empty_dict(self):
        result = _inject_incremental_params(None, "fred", datetime(2025, 1, 1))
        assert result == {}

    def test_empty_config_returns_empty_dict(self):
        result = _inject_incremental_params({}, "fred", datetime(2025, 1, 1))
        assert result == {}


# =============================================================================
# Gap 4 — Quality gate (advisory, never raises)
# =============================================================================


class TestRunQualityGate:
    """_run_quality_gate should be advisory and never propagate errors."""

    @pytest.mark.asyncio
    async def test_no_registry_entry_skips_silently(self):
        from app.api.v1.jobs import _run_quality_gate

        db = MagicMock()
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None
        job = MagicMock(id=1, source="fred")

        # Should not raise
        await _run_quality_gate(db, job)

    @pytest.mark.asyncio
    async def test_exception_in_evaluate_swallowed(self):
        from app.api.v1.jobs import _run_quality_gate

        db = MagicMock()
        registry = MagicMock(table_name="fred_interest_rates")
        db.query.return_value.filter.return_value.order_by.return_value.first.return_value = registry
        job = MagicMock(id=1, source="fred")

        with patch(
            "app.core.data_quality_service.evaluate_rules_for_job",
            side_effect=Exception("boom"),
        ):
            # Should not raise
            await _run_quality_gate(db, job)


# =============================================================================
# Gap 2 — Freshness dashboard response shape
# =============================================================================


class TestFreshnessDashboard:
    """Validate the response structure from the freshness endpoint."""

    def test_response_shape_empty_db(self):
        from app.api.v1.freshness import get_freshness_dashboard

        db = MagicMock()
        # No jobs
        db.query.return_value.filter.return_value.group_by.return_value.all.return_value = []
        # No schedules
        db.query.return_value.filter.return_value.all.return_value = []

        result = get_freshness_dashboard(db=db)
        assert result["total_sources"] == 0
        assert result["stale_count"] == 0
        assert result["fresh_count"] == 0
        assert result["sources"] == []
