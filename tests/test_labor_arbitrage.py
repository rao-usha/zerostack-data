"""Unit tests for Labor Arbitrage service."""

import pytest
from unittest.mock import MagicMock, patch

from app.services.labor_arbitrage import (
    LaborArbitrageService,
    VERTICAL_OCCUPATIONS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def svc(mock_db):
    return LaborArbitrageService(mock_db)


# ---------------------------------------------------------------------------
# VERTICAL_OCCUPATIONS mapping
# ---------------------------------------------------------------------------

class TestVerticalOccupations:
    def test_all_verticals_present(self):
        expected = {"medspa", "dental", "hvac", "veterinary", "car_wash", "physical_therapy"}
        assert set(VERTICAL_OCCUPATIONS.keys()) == expected

    def test_each_vertical_has_occupations(self):
        for slug, occs in VERTICAL_OCCUPATIONS.items():
            assert len(occs) >= 2, f"{slug} should have at least 2 occupations"

    def test_occupation_tuples_format(self):
        for slug, occs in VERTICAL_OCCUPATIONS.items():
            for code, title in occs:
                assert "-" in code, f"SOC code {code} should contain a dash"
                assert len(title) > 3, f"Title for {code} too short"

    def test_dental_has_dentists(self):
        codes = [c for c, _ in VERTICAL_OCCUPATIONS["dental"]]
        assert "29-1021" in codes

    def test_hvac_has_mechanics(self):
        codes = [c for c, _ in VERTICAL_OCCUPATIONS["hvac"]]
        assert "49-9021" in codes

    def test_medspa_has_nurses(self):
        codes = [c for c, _ in VERTICAL_OCCUPATIONS["medspa"]]
        assert "29-1141" in codes


# ---------------------------------------------------------------------------
# compare_wages
# ---------------------------------------------------------------------------

class TestCompareWages:
    def test_returns_results_structure(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = [
            {
                "area_type": "state", "area_code": "ST48", "area_name": "Texas",
                "occupation_code": "29-1021", "occupation_title": "Dentists",
                "employment": 5000, "mean_hourly_wage": 80.0,
                "median_hourly_wage": 75.0, "pct_10_hourly": 40.0,
                "pct_25_hourly": 55.0, "pct_75_hourly": 95.0,
                "pct_90_hourly": 120.0, "mean_annual_wage": 166400.0,
                "median_annual_wage": 156000.0, "period_year": 2023,
            }
        ]
        result = svc.compare_wages(occupation_code="29-1021")
        assert result["occupation_code"] == "29-1021"
        assert result["total_areas"] == 1
        assert len(result["results"]) == 1

    def test_base_area_differential(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = [
            {
                "area_type": "state", "area_code": "ST48", "area_name": "Texas",
                "occupation_code": "29-1021", "occupation_title": "Dentists",
                "employment": 5000, "mean_hourly_wage": 80.0,
                "median_hourly_wage": 75.0, "pct_10_hourly": 40.0,
                "pct_25_hourly": 55.0, "pct_75_hourly": 95.0,
                "pct_90_hourly": 120.0, "mean_annual_wage": 150000.0,
                "median_annual_wage": 140000.0, "period_year": 2023,
            },
            {
                "area_type": "state", "area_code": "ST06", "area_name": "California",
                "occupation_code": "29-1021", "occupation_title": "Dentists",
                "employment": 8000, "mean_hourly_wage": 100.0,
                "median_hourly_wage": 95.0, "pct_10_hourly": 50.0,
                "pct_25_hourly": 70.0, "pct_75_hourly": 120.0,
                "pct_90_hourly": 150.0, "mean_annual_wage": 200000.0,
                "median_annual_wage": 190000.0, "period_year": 2023,
            },
        ]
        result = svc.compare_wages(
            occupation_code="29-1021", base_area="ST06"
        )
        assert result["base_wage"] == 200000.0
        # Texas should show negative differential (cheaper)
        tx = next(r for r in result["results"] if r["area_code"] == "ST48")
        assert tx["wage_differential"] == -50000.0
        assert tx["wage_differential_pct"] == -25.0

    def test_handles_db_error(self, svc, mock_db):
        mock_db.execute.side_effect = Exception("connection lost")
        result = svc.compare_wages(occupation_code="29-1021")
        assert "error" in result

    def test_empty_results(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = []
        result = svc.compare_wages(occupation_code="99-9999")
        assert result["total_areas"] == 0
        assert result["results"] == []


# ---------------------------------------------------------------------------
# vertical_profile
# ---------------------------------------------------------------------------

class TestVerticalProfile:
    def test_unknown_vertical(self, svc):
        result = svc.vertical_profile(slug="unknown_vertical")
        assert "error" in result
        assert "available_verticals" in result

    def test_returns_all_occupations(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = []
        result = svc.vertical_profile(slug="dental")
        assert result["vertical"] == "dental"
        assert len(result["occupations"]) == len(VERTICAL_OCCUPATIONS["dental"])

    def test_with_area_filter(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = [
            {
                "area_type": "state", "area_code": "ST06",
                "area_name": "California", "occupation_code": "29-1021",
                "occupation_title": "Dentists", "employment": 8000,
                "mean_hourly_wage": 100.0, "median_hourly_wage": 95.0,
                "mean_annual_wage": 200000.0, "median_annual_wage": 190000.0,
                "period_year": 2023,
            }
        ]
        result = svc.vertical_profile(slug="dental", area_codes=["ST06"])
        assert result["area_filter"] == ["ST06"]

    def test_occupation_summary_stats(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = [
            {
                "area_type": "state", "area_code": "ST48",
                "area_name": "Texas", "occupation_code": "29-1021",
                "occupation_title": "Dentists", "employment": 5000,
                "mean_hourly_wage": 80.0, "median_hourly_wage": 75.0,
                "mean_annual_wage": 150000.0, "median_annual_wage": 140000.0,
                "period_year": 2023,
            },
            {
                "area_type": "state", "area_code": "ST06",
                "area_name": "California", "occupation_code": "29-1021",
                "occupation_title": "Dentists", "employment": 8000,
                "mean_hourly_wage": 100.0, "median_hourly_wage": 95.0,
                "mean_annual_wage": 200000.0, "median_annual_wage": 190000.0,
                "period_year": 2023,
            },
        ]
        result = svc.vertical_profile(slug="dental")
        dentist_occ = next(
            (o for o in result["occupations"] if o["occupation_code"] == "29-1021"),
            None,
        )
        assert dentist_occ is not None
        assert dentist_occ["areas_with_data"] == 2
        assert dentist_occ["min_wage"] == 150000.0
        assert dentist_occ["max_wage"] == 200000.0
        assert dentist_occ["wage_spread"] == 50000.0


# ---------------------------------------------------------------------------
# list_occupations / list_areas
# ---------------------------------------------------------------------------

class TestListEndpoints:
    def test_list_occupations(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = [
            {"occupation_code": "29-1021", "occupation_title": "Dentists",
             "area_count": 51, "avg_wage": 180000.0},
        ]
        result = svc.list_occupations()
        assert result["total_occupations"] == 1
        assert "vertical_mappings" in result

    def test_list_areas(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = [
            {"area_type": "state", "area_code": "ST06",
             "area_name": "California", "occupation_count": 25},
        ]
        result = svc.list_areas()
        assert result["total_areas"] == 1

    def test_list_areas_with_filter(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = []
        result = svc.list_areas(area_type="msa")
        assert result["total_areas"] == 0

    def test_list_occupations_db_error(self, svc, mock_db):
        mock_db.execute.side_effect = Exception("db error")
        result = svc.list_occupations()
        assert "error" in result

    def test_list_areas_db_error(self, svc, mock_db):
        mock_db.execute.side_effect = Exception("db error")
        result = svc.list_areas()
        assert "error" in result
