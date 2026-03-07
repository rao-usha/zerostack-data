"""Tests for DatacenterSiteTemplate report generation."""

import pytest
from unittest.mock import MagicMock, patch

from app.reports.templates.datacenter_site import DatacenterSiteTemplate


@pytest.mark.unit
class TestDatacenterSiteTemplate:
    """Unit tests for DatacenterSiteTemplate."""

    def setup_method(self):
        self.template = DatacenterSiteTemplate()

    def test_template_name(self):
        """Template name should be 'datacenter_site'."""
        assert self.template.name == "datacenter_site"

    def test_capital_model_tiers(self):
        """_build_capital_model should return Tier III and Tier IV keys."""
        result = self.template._build_capital_model(target_mw=50)
        assert "Tier III" in result
        assert "Tier IV" in result

    def test_capital_model_tier3_cheaper(self):
        """Tier III build cost should be less than Tier IV."""
        result = self.template._build_capital_model(target_mw=50)
        tier3_cost = result["Tier III"]["total_build_cost"]
        tier4_cost = result["Tier IV"]["total_build_cost"]
        assert tier3_cost < tier4_cost

    def test_capital_model_scales_with_mw(self):
        """Higher target MW should produce higher total cost."""
        result_50 = self.template._build_capital_model(target_mw=50)
        result_100 = self.template._build_capital_model(target_mw=100)
        assert result_100["Tier III"]["total_build_cost"] > result_50["Tier III"]["total_build_cost"]

    def test_capital_model_has_cost_per_mw(self):
        """Each tier should include cost_per_mw field."""
        result = self.template._build_capital_model(target_mw=50)
        assert result["Tier III"]["cost_per_mw"] == 8_000_000
        assert result["Tier IV"]["cost_per_mw"] == 12_000_000

    def test_deal_scenarios_limit(self):
        """_build_deal_scenarios should return at most 3 scenarios."""
        mock_counties = [
            {
                "county_fips": f"4800{i}",
                "county_name": f"County {i}",
                "state": "TX",
                "overall_score": 85 - i * 5,
            }
            for i in range(5)
        ]
        result = self.template._build_deal_scenarios(mock_counties, target_mw=50)
        assert len(result) <= 3

    def test_deal_scenarios_has_irr(self):
        """Deal scenarios should include estimated_irr."""
        counties = [
            {"county_name": "Harris County", "state": "TX", "overall_score": 80},
        ]
        result = self.template._build_deal_scenarios(counties, target_mw=50)
        assert len(result) == 1
        assert "estimated_irr" in result[0]
        assert "build_cost" in result[0]
        assert "annual_revenue" in result[0]

    def test_ceo_overview_narrative_with_data(self):
        """Rich data should produce narrative paragraphs and next steps."""
        mock_data = {
            "summary": {
                "total_counties": 100,
                "a_grade": 15,
                "b_grade": 30,
                "avg_score": 75,
                "max_score": 92,
                "states_covered": 30,
            },
            "top_counties": [
                {
                    "county_name": "Loudoun County",
                    "state": "VA",
                    "overall_score": 92,
                    "grade": "A",
                    "power_score": 90,
                    "connectivity_score": 85,
                    "regulatory_score": 70,
                    "labor_score": 80,
                    "risk_score": 60,
                    "cost_incentive_score": 75,
                }
            ],
            "deal_scenarios": [
                {"county_name": "Loudoun", "estimated_irr": 18.0}
            ],
            "data_sources": [
                {"name": f"src_{i}", "row_count": 100} for i in range(10)
            ],
        }
        result = self.template._compute_ceo_overview(mock_data)
        assert "scope_para" in result
        assert "100" in result["scope_para"]
        assert "opps_para" in result
        assert "Loudoun" in result["opps_para"]
        assert result["coverage_pct"] == 100
        assert result["best_irr"] == 18.0

    def test_ceo_overview_narrative_empty_data(self):
        """Empty data should still produce valid narrative."""
        mock_data = {
            "summary": {
                "total_counties": 5,
                "a_grade": 0,
                "b_grade": 0,
                "avg_score": 25,
                "max_score": 30,
                "states_covered": 1,
            },
            "top_counties": [],
            "deal_scenarios": [],
            "data_sources": [],
        }
        result = self.template._compute_ceo_overview(mock_data)
        assert "scope_para" in result
        assert result["coverage_pct"] == 0
        assert len(result["data_gaps"]) == 0
