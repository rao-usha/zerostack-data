"""Unit tests for Location Due Diligence service."""

import pytest
from unittest.mock import MagicMock, patch, call

from app.services.location_diligence import (
    LocationDiligenceService,
    DD_SECTIONS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    db = MagicMock()
    # Default: return empty results for all queries
    db.execute.return_value.mappings.return_value.fetchall.return_value = []
    db.execute.return_value.scalar.return_value = 0
    return db


@pytest.fixture
def svc(mock_db):
    return LocationDiligenceService(mock_db)


# ---------------------------------------------------------------------------
# DD_SECTIONS structure
# ---------------------------------------------------------------------------

class TestDDSections:
    def test_all_sections_present(self):
        keys = {s["key"] for s in DD_SECTIONS}
        expected = {
            "market_overview", "environmental", "labor_market",
            "infrastructure", "risk_profile", "incentives",
            "competitive_landscape", "healthcare_providers",
        }
        assert keys == expected

    def test_sections_have_required_fields(self):
        for section in DD_SECTIONS:
            assert "key" in section
            assert "label" in section
            assert "description" in section
            assert "tables" in section
            assert len(section["tables"]) >= 1

    def test_get_sections_static(self):
        result = LocationDiligenceService.get_sections()
        assert len(result) == 8


# ---------------------------------------------------------------------------
# get_package
# ---------------------------------------------------------------------------

class TestGetPackage:
    def test_requires_location(self, svc):
        result = svc.get_package()
        assert "error" in result

    def test_returns_all_sections(self, svc):
        result = svc.get_package(county_fips="06037")
        assert "sections" in result
        assert len(result["sections"]) == 8
        assert "coverage" in result

    def test_derives_state_from_county(self, svc):
        result = svc.get_package(county_fips="06037")
        assert result["state_fips"] == "06"

    def test_coverage_calculation(self, svc):
        result = svc.get_package(county_fips="06037")
        cov = result["coverage"]
        assert cov["total_sections"] == 8
        assert 0 <= cov["coverage_pct"] <= 100

    def test_passes_naics_code(self, svc):
        result = svc.get_package(county_fips="06037", naics_code="621111")
        assert result["naics_code"] == "621111"

    def test_state_fips_only(self, svc):
        result = svc.get_package(state_fips="06")
        assert result["state_fips"] == "06"
        assert result["county_fips"] is None


# ---------------------------------------------------------------------------
# compare_locations
# ---------------------------------------------------------------------------

class TestCompareLocations:
    def test_requires_two_locations(self, svc):
        result = svc.compare_locations(locations=["06037"])
        assert "error" in result

    def test_compares_multiple(self, svc):
        result = svc.compare_locations(locations=["06037", "48201"])
        assert result["locations_compared"] == 2
        assert "06037" in result["packages"]
        assert "48201" in result["packages"]

    def test_caps_at_10(self, svc):
        locs = [f"{i:05d}" for i in range(15)]
        result = svc.compare_locations(locations=locs)
        assert result["locations_compared"] == 10


# ---------------------------------------------------------------------------
# check_coverage
# ---------------------------------------------------------------------------

class TestCheckCoverage:
    def test_returns_section_coverage(self, svc, mock_db):
        mock_db.execute.return_value.scalar.return_value = 0
        result = svc.check_coverage(county_fips="06037")
        assert "sections" in result
        assert result["total_sections"] == 8

    def test_handles_db_errors_gracefully(self, svc, mock_db):
        mock_db.execute.side_effect = Exception("table not found")
        result = svc.check_coverage(state="CA")
        # Should not crash — sections just show 0 counts
        assert result["sections_with_data"] == 0


# ---------------------------------------------------------------------------
# Section builders (via _safe_query)
# ---------------------------------------------------------------------------

class TestSectionBuilders:
    def test_market_overview_no_county(self, svc):
        result = svc._market_overview(None, "06")
        assert result["data_available"] is False

    def test_market_overview_with_data(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = [
            {
                "state_abbr": "CA", "county_name": "Los Angeles",
                "total_returns": 100000, "total_agi": 5000000,
                "returns_100k_plus": 30000, "total_wages": 4000000,
            }
        ]
        result = svc._market_overview("06037", "06")
        assert result["data_available"] is True
        assert result["total_tax_returns"] == 100000
        assert result["avg_agi"] == 50.0
        assert result["pct_returns_100k_plus"] == 30.0

    def test_environmental_empty(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = [
            {"facility_count": 0, "violators": 0, "total_penalties": 0}
        ]
        result = svc._environmental("06037", "06")
        assert result["data_available"] is False

    def test_labor_market_no_state(self, svc):
        result = svc._labor_market(None)
        assert result["data_available"] is False

    def test_infrastructure_no_state(self, svc):
        result = svc._infrastructure(None)
        assert result["data_available"] is False

    def test_risk_profile_empty(self, svc, mock_db):
        mock_db.execute.return_value.mappings.return_value.fetchall.return_value = []
        result = svc._risk_profile("06037", "06")
        assert result["data_available"] is False

    def test_incentives_no_state(self, svc):
        result = svc._incentives(None)
        assert result["data_available"] is False

    def test_healthcare_no_location(self, svc):
        result = svc._healthcare(None, None)
        assert result["data_available"] is False

    def test_safe_query_handles_exception(self, svc, mock_db):
        mock_db.execute.side_effect = Exception("table missing")
        result = svc._safe_query("SELECT 1", {})
        assert result == []
