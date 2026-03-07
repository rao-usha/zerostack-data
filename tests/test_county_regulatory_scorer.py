"""Tests for CountyRegulatoryScorer."""

import pytest
from unittest.mock import MagicMock, patch

from app.ml.county_regulatory_scorer import (
    CountyRegulatoryScorer,
    WEIGHTS,
    GRADE_THRESHOLDS,
    MODEL_VERSION,
)


@pytest.mark.unit
class TestCountyRegulatoryScorer:
    """Unit tests for CountyRegulatoryScorer static/class methods."""

    def test_percentile_rank_basic(self):
        """[10, 20, 30] should produce percentile ranks [0, 50, 100]."""
        result = CountyRegulatoryScorer._percentile_rank([10, 20, 30])
        assert result == [0.0, 50.0, 100.0]

    def test_percentile_rank_empty(self):
        """Empty list should return empty list."""
        result = CountyRegulatoryScorer._percentile_rank([])
        assert result == []

    def test_percentile_rank_single(self):
        """Single value should get rank 50."""
        result = CountyRegulatoryScorer._percentile_rank([42])
        assert result == [50.0]

    def test_inverted_percentile_rank(self):
        """Inverted percentile: [10, 20, 30] -> [100, 50, 0]."""
        result = CountyRegulatoryScorer._inverted_percentile_rank([10, 20, 30])
        assert result == [100.0, 50.0, 0.0]

    def test_get_grade_thresholds(self):
        """Grade thresholds: 85->A, 70->B, 55->C, 40->D, 20->F."""
        assert CountyRegulatoryScorer._get_grade(85) == "A"
        assert CountyRegulatoryScorer._get_grade(80) == "A"
        assert CountyRegulatoryScorer._get_grade(70) == "B"
        assert CountyRegulatoryScorer._get_grade(55) == "C"
        assert CountyRegulatoryScorer._get_grade(40) == "D"
        assert CountyRegulatoryScorer._get_grade(20) == "F"
        assert CountyRegulatoryScorer._get_grade(0) == "F"

    def test_weights_sum_to_one(self):
        """All WEIGHTS values must sum to 1.0."""
        total = sum(WEIGHTS.values())
        assert abs(total - 1.0) < 1e-9, f"Weights sum to {total}, expected 1.0"

    def test_weights_has_four_factors(self):
        """Should have exactly 4 scoring factors."""
        assert len(WEIGHTS) == 4

    def test_methodology_returns_expected_keys(self):
        """get_methodology() must contain model_version, weights, factors."""
        methodology = CountyRegulatoryScorer.get_methodology()
        assert "model_version" in methodology
        assert "weights" in methodology
        assert "factors" in methodology
        assert methodology["model_version"] == MODEL_VERSION


@pytest.mark.unit
class TestCountyRegulatoryScorerFactors:
    """Test factor computation methods with mocked DB."""

    def setup_method(self, method):
        self.mock_db = MagicMock()
        with patch("app.core.database.get_engine"):
            self.scorer = CountyRegulatoryScorer(self.mock_db)

    def test_compute_permit_velocity(self):
        """Higher permits_per_10k_pop should rank higher."""
        counties = [
            {"permits_per_10k_pop": 10, "yoy_growth_pct": 5},
            {"permits_per_10k_pop": 50, "yoy_growth_pct": 10},
            {"permits_per_10k_pop": 1, "yoy_growth_pct": -2},
        ]
        scores = self.scorer._compute_permit_velocity(counties)
        assert len(scores) == 3
        # County with 50 permits should score highest
        assert scores[1] > scores[0] > scores[2]

    def test_compute_jurisdictional_simplicity(self):
        """Fewer govts_per_10k_pop should score HIGHER (inverted)."""
        counties = [
            {"govts_per_10k_pop": 2},   # few = simple = high score
            {"govts_per_10k_pop": 10},  # many = complex = low score
            {"govts_per_10k_pop": 50},  # very many = very low score
        ]
        scores = self.scorer._compute_jurisdictional_simplicity(counties)
        assert scores[0] > scores[1] > scores[2]

    def test_compute_energy_siting_all_zeros(self):
        """When all counties have 0 DC programs, all get 50 (neutral)."""
        counties = [
            {"dc_incentive_programs": 0},
            {"dc_incentive_programs": 0},
        ]
        scores = self.scorer._compute_energy_siting_friendliness(counties)
        assert all(s == 50.0 for s in scores)

    def test_compute_historical_dc_deals_all_zeros(self):
        """When no counties have DC deals, all get 50 (neutral)."""
        counties = [
            {"dc_disclosed_deals": 0},
            {"dc_disclosed_deals": 0},
        ]
        scores = self.scorer._compute_historical_dc_deals(counties)
        assert all(s == 50.0 for s in scores)
