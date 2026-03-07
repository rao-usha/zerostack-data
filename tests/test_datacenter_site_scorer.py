"""Tests for DatacenterSiteScorer."""

import pytest
from unittest.mock import MagicMock, patch

from app.ml.datacenter_site_scorer import (
    DatacenterSiteScorer,
    WEIGHTS,
    GRADE_THRESHOLDS,
    MODEL_VERSION,
)


@pytest.mark.unit
class TestDatacenterSiteScorerStatic:
    """Unit tests for static/class methods — no DB needed."""

    def test_percentile_rank(self):
        """Basic percentile ranking should work correctly."""
        result = DatacenterSiteScorer._percentile_rank([10, 20, 30])
        assert result == [0.0, 50.0, 100.0]

    def test_percentile_rank_equal_values(self):
        """Equal values should still produce a valid ranking."""
        result = DatacenterSiteScorer._percentile_rank([5, 5, 5])
        assert len(result) == 3

    def test_inverted_percentile_rank(self):
        result = DatacenterSiteScorer._inverted_percentile_rank([10, 20, 30])
        assert result == [100.0, 50.0, 0.0]

    def test_get_grade(self):
        """Grade thresholds should be correct."""
        assert DatacenterSiteScorer._get_grade(90) == "A"
        assert DatacenterSiteScorer._get_grade(80) == "A"
        assert DatacenterSiteScorer._get_grade(75) == "B"
        assert DatacenterSiteScorer._get_grade(60) == "C"
        assert DatacenterSiteScorer._get_grade(45) == "D"
        assert DatacenterSiteScorer._get_grade(25) == "F"

    def test_weights_sum_to_one(self):
        """All 6 domain weights must sum to 1.0."""
        total = sum(WEIGHTS.values())
        assert len(WEIGHTS) == 6, f"Expected 6 domain weights, got {len(WEIGHTS)}"
        assert abs(total - 1.0) < 1e-9, f"Weights sum to {total}, expected 1.0"

    def test_methodology_has_all_domains(self):
        """Methodology should document all 6 scoring domains."""
        methodology = DatacenterSiteScorer.get_methodology()
        assert "domains" in methodology
        assert len(methodology["domains"]) == 6
        assert methodology["model_version"] == MODEL_VERSION


@pytest.mark.unit
class TestDatacenterSiteScorerDomains:
    """Test domain scoring methods with mocked DB."""

    def setup_method(self, method):
        self.mock_db = MagicMock()
        with patch("app.core.database.get_engine"):
            self.scorer = DatacenterSiteScorer(self.mock_db)

    def test_score_power_infrastructure_ordering(self):
        """Higher power capacity should yield a higher score."""
        counties = [
            {"power_capacity_mw": 500000, "substations_count": 5000, "elec_price": 5},
            {"power_capacity_mw": 1000, "substations_count": 10, "elec_price": 15},
        ]
        scores = self.scorer.score_power_infrastructure(counties)
        assert scores[0] > scores[1]

    def test_score_connectivity_ordering(self):
        """More IXs and DCs should score higher."""
        counties = [
            {"ix_count": 20, "dc_count": 100, "broadband_pct": 95},
            {"ix_count": 0, "dc_count": 1, "broadband_pct": 30},
        ]
        scores = self.scorer.score_connectivity(counties)
        assert scores[0] > scores[1]

    def test_score_risk_inverted(self):
        """Higher flood risk should produce LOWER score (inverted)."""
        counties = [
            {"flood_risk": 90},  # high risk -> low score
            {"flood_risk": 10},  # low risk -> high score
        ]
        scores = self.scorer.score_risk_environment(counties)
        assert scores[1] > scores[0]

    def test_score_cost_incentives(self):
        """Low electricity + incentives + OZ should score higher."""
        counties = [
            {"elec_price": 3, "incentive_count": 10, "has_oz": True},
            {"elec_price": 15, "incentive_count": 0, "has_oz": False},
        ]
        scores = self.scorer.score_cost_incentives(counties)
        assert scores[0] > scores[1]

    def test_score_labor_workforce(self):
        """More tech employment should score higher."""
        counties = [
            {"tech_employment": 50000, "tech_wage": 2000},
            {"tech_employment": 100, "tech_wage": 500},
        ]
        scores = self.scorer.score_labor_workforce(counties)
        assert scores[0] > scores[1]
