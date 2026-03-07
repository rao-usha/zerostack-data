"""Tests for CensusBPSCollector – datacenter site selection labor/growth layer."""

import pytest
from unittest.mock import MagicMock

from app.sources.site_intel.labor.census_bps_collector import (
    CensusBPSCollector,
    STATE_FIPS,
    FIPS_TO_STATE,
)


@pytest.mark.unit
class TestCensusBPSCollector:
    """Unit tests for Census Building Permits Survey collector."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = CensusBPSCollector(db=self.mock_db)

    def test_transform_row_valid(self):
        """Valid Census JSON row transforms into the expected dict structure."""
        row = {
            "state": "48",
            "county": "201",
            "NAME": "Harris County",
            "BLDGS": "150",
            "UNITS": "320",
            "VALUATION": "85000000",
        }
        result = self.collector._transform_row(row, "TX", 2023)
        assert result is not None
        assert result["county_fips"] == "48201"
        assert result["state"] == "TX"
        assert result["period_year"] == 2023
        assert result["total_units"] == 320
        assert result["single_family_units"] == 150

    def test_transform_row_missing_state(self):
        """Row with missing state FIPS returns None."""
        row = {
            "state": "",
            "county": "201",
            "BLDGS": "100",
            "UNITS": "200",
            "VALUATION": "50000000",
        }
        result = self.collector._transform_row(row, "TX", 2023)
        assert result is None

    def test_transform_row_missing_county(self):
        """Row with missing county FIPS returns None."""
        row = {
            "state": "48",
            "county": "",
            "BLDGS": "100",
            "UNITS": "200",
            "VALUATION": "50000000",
        }
        result = self.collector._transform_row(row, "TX", 2023)
        assert result is None

    def test_multi_family_calculation(self):
        """Multi-family units = total_units - single_family_units."""
        row = {
            "state": "48",
            "county": "201",
            "NAME": "Harris County",
            "BLDGS": "100",
            "UNITS": "350",
            "VALUATION": "90000000",
        }
        result = self.collector._transform_row(row, "TX", 2023)
        assert result is not None
        assert result["total_units"] == 350
        assert result["single_family_units"] == 100
        assert result["multi_family_units"] == 250  # 350 - 100

    def test_valuation_scaled_to_thousands(self):
        """Total valuation is converted to thousands."""
        row = {
            "state": "48",
            "county": "201",
            "NAME": "Harris County",
            "BLDGS": "100",
            "UNITS": "200",
            "VALUATION": "85000000",
        }
        result = self.collector._transform_row(row, "TX", 2023)
        assert result is not None
        assert result["total_valuation_thousand"] == 85000  # 85000000 // 1000

    def test_state_fips_mapping(self):
        """STATE_FIPS contains expected states."""
        assert STATE_FIPS["TX"] == "48"
        assert STATE_FIPS["VA"] == "51"
        assert STATE_FIPS["CA"] == "06"
        assert len(STATE_FIPS) > 40  # All states

    def test_fips_to_state_reverse_mapping(self):
        """FIPS_TO_STATE is the inverse of STATE_FIPS."""
        assert FIPS_TO_STATE["48"] == "TX"
        assert FIPS_TO_STATE["51"] == "VA"

    def test_safe_int_valid(self):
        """_safe_int parses numeric strings correctly."""
        assert self.collector._safe_int("150") == 150
        assert self.collector._safe_int("0") == 0

    def test_safe_int_invalid(self):
        """_safe_int returns None for non-numeric strings."""
        assert self.collector._safe_int(None) is None
        assert self.collector._safe_int("") is None
        assert self.collector._safe_int("-") is None

    def test_yoy_defaults_to_none(self):
        """YoY growth is None before post-processing computation."""
        row = {
            "state": "48",
            "county": "201",
            "BLDGS": "100",
            "UNITS": "200",
            "VALUATION": "50000000",
        }
        result = self.collector._transform_row(row, "TX", 2023)
        assert result is not None
        assert result["yoy_growth_pct"] is None

    def test_source_field(self):
        """Transformed rows include source='census_bps'."""
        row = {
            "state": "48",
            "county": "201",
            "BLDGS": "10",
            "UNITS": "20",
            "VALUATION": "5000000",
        }
        result = self.collector._transform_row(row, "TX", 2023)
        assert result is not None
        assert result["source"] == "census_bps"
