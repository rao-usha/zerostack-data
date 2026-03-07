"""Tests for CensusGovCollector – datacenter site selection labor/governance layer."""

import pytest
from unittest.mock import MagicMock

from app.sources.site_intel.labor.census_gov_collector import (
    CensusGovCollector,
    GOV_TYPE_MAP,
)


@pytest.mark.unit
class TestCensusGovCollector:
    """Unit tests for Census of Governments collector."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = CensusGovCollector(db=self.mock_db)

    def test_gov_type_mapping(self):
        """Government type codes 1-5 map to correct lowercase category names."""
        expected_mapping = {
            "1": "county",
            "2": "municipal",
            "3": "township",
            "4": "special_district",
            "5": "school_district",
        }
        for code, expected_name in expected_mapping.items():
            assert GOV_TYPE_MAP[code] == expected_name

    def test_aggregate_by_county_basic(self):
        """CSV with multiple govt units in same county aggregates correctly."""
        # Uses real FIPS_TO_STATE mapping (48=TX exists in bls_qcew_collector)
        csv_text = (
            "STATE_CODE,COUNTY_CODE,GOVERNMENT_TYPE,COUNTY_NAME,POPULATION\n"
            "48,201,1,Harris County,4700000\n"
            "48,201,2,City of Houston,\n"
            "48,201,5,Harris Flood Control,\n"
        )
        result = self.collector._aggregate_by_county(csv_text)
        assert "48201" in result
        county = result["48201"]
        assert county["county_fips"] == "48201"
        assert county["state"] == "TX"
        assert county["total_governments"] == 3
        assert county["county_govts"] == 1
        assert county["municipal_govts"] == 1
        assert county["school_district_govts"] == 1

    def test_aggregate_by_county_with_population(self):
        """govts_per_10k_pop is computed when population is available."""
        csv_text = (
            "STATE_CODE,COUNTY_CODE,GOVERNMENT_TYPE,COUNTY_NAME,POPULATION\n"
            "48,201,1,Harris County,100000\n"
            "48,201,2,City of Houston,\n"
            "48,201,5,Water District,\n"
        )
        result = self.collector._aggregate_by_county(csv_text)
        county = result["48201"]
        # 3 govts / 100000 * 10000 = 0.3 per 10k
        expected = round(3 / 100000 * 10000, 2)
        assert county["govts_per_10k_pop"] == pytest.approx(expected, rel=0.01)

    def test_aggregate_by_county_state_filter(self):
        """Only rows matching the state filter are included."""
        csv_text = (
            "STATE_CODE,COUNTY_CODE,GOVERNMENT_TYPE,COUNTY_NAME,POPULATION\n"
            "48,201,1,Harris County,4700000\n"
            "36,061,2,New York City,8300000\n"
        )
        result = self.collector._aggregate_by_county(csv_text, states=["TX"])
        assert "48201" in result
        assert "36061" not in result

    def test_aggregate_empty_csv(self):
        """Empty CSV produces an empty dict."""
        csv_text = "STATE_CODE,COUNTY_CODE,GOVERNMENT_TYPE,COUNTY_NAME,POPULATION\n"
        result = self.collector._aggregate_by_county(csv_text)
        assert len(result) == 0

    def test_aggregate_skips_state_level_records(self):
        """Rows with county_code='000' (state-level) are skipped."""
        csv_text = (
            "STATE_CODE,COUNTY_CODE,GOVERNMENT_TYPE,COUNTY_NAME,POPULATION\n"
            "48,000,1,State of Texas,29000000\n"
            "48,201,1,Harris County,4700000\n"
        )
        result = self.collector._aggregate_by_county(csv_text)
        assert "48000" not in result
        assert "48201" in result

    def test_safe_int_valid(self):
        """_safe_int parses numeric strings correctly."""
        assert self.collector._safe_int("100") == 100
        assert self.collector._safe_int("0") == 0

    def test_safe_int_invalid(self):
        """_safe_int returns None for non-numeric strings."""
        assert self.collector._safe_int(None) is None
        assert self.collector._safe_int("") is None
        assert self.collector._safe_int("-") is None
