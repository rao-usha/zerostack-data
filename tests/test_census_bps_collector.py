"""Tests for CensusBPSCollector – datacenter site selection labor/growth layer."""

import pytest
from unittest.mock import MagicMock

from app.sources.site_intel.labor.census_bps_collector import (
    CensusBPSCollector,
    STATE_FIPS,
    FIPS_TO_STATE,
    BPS_COUNTY_URL,
)


# Sample BPS county file format:
# 2 header lines, blank line, then CSV data
# Columns: year, state_fips, county_fips, region, division, county_name,
#   1u_bldgs, 1u_units, 1u_value, 2u_bldgs, 2u_units, 2u_value,
#   3-4u_bldgs, 3-4u_units, 3-4u_value, 5+u_bldgs, 5+u_units, 5+u_value
SAMPLE_BPS_FILE = """\
Annual County-Level Building Permits Data
Year,State,County,Region,Division,County Name,1-unit Bldgs,1-unit Units,1-unit Value,2-unit Bldgs,2-unit Units,2-unit Value,3-4 unit Bldgs,3-4 unit Units,3-4 unit Value,5+ unit Bldgs,5+ unit Units,5+ unit Value

2023,48,201,3,7,Harris County,150,150,85000000,5,10,3000000,3,9,2000000,20,161,50000000
2023,48,113,3,7,Dallas County,100,100,60000000,3,6,2000000,2,6,1500000,15,120,40000000
2023,51,059,3,5,Fairfax County,200,200,120000000,10,20,8000000,5,15,5000000,30,250,80000000
"""


@pytest.mark.unit
class TestCensusBPSCollector:
    """Unit tests for Census Building Permits Survey collector."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = CensusBPSCollector(db=self.mock_db)

    def test_parse_county_file_valid(self):
        """Valid BPS file parses into expected records."""
        records = self.collector._parse_county_file(SAMPLE_BPS_FILE, 2023)
        assert len(records) == 3

    def test_parse_county_file_fips(self):
        """County FIPS is constructed from state + county codes."""
        records = self.collector._parse_county_file(SAMPLE_BPS_FILE, 2023)
        fips_codes = {r["county_fips"] for r in records}
        assert "48201" in fips_codes
        assert "48113" in fips_codes
        assert "51059" in fips_codes

    def test_parse_county_file_state(self):
        """State abbreviation is derived from FIPS."""
        records = self.collector._parse_county_file(SAMPLE_BPS_FILE, 2023)
        harris = next(r for r in records if r["county_fips"] == "48201")
        assert harris["state"] == "TX"
        fairfax = next(r for r in records if r["county_fips"] == "51059")
        assert fairfax["state"] == "VA"

    def test_parse_county_file_units(self):
        """Total units sums all unit types."""
        records = self.collector._parse_county_file(SAMPLE_BPS_FILE, 2023)
        harris = next(r for r in records if r["county_fips"] == "48201")
        # 150 + 10 + 9 + 161 = 330
        assert harris["total_units"] == 330
        assert harris["single_family_units"] == 150
        assert harris["multi_family_units"] == 180  # 330 - 150

    def test_parse_county_file_valuation(self):
        """Valuation is converted to thousands."""
        records = self.collector._parse_county_file(SAMPLE_BPS_FILE, 2023)
        harris = next(r for r in records if r["county_fips"] == "48201")
        # (85000000 + 3000000 + 2000000 + 50000000) // 1000 = 140000
        assert harris["total_valuation_thousand"] == 140000

    def test_parse_county_file_year(self):
        """Year comes from the CSV row."""
        records = self.collector._parse_county_file(SAMPLE_BPS_FILE, 2023)
        assert all(r["period_year"] == 2023 for r in records)

    def test_parse_county_file_source(self):
        """All records have source='census_bps'."""
        records = self.collector._parse_county_file(SAMPLE_BPS_FILE, 2023)
        assert all(r["source"] == "census_bps" for r in records)

    def test_parse_county_file_yoy_none(self):
        """YoY growth is None before post-processing."""
        records = self.collector._parse_county_file(SAMPLE_BPS_FILE, 2023)
        assert all(r["yoy_growth_pct"] is None for r in records)

    def test_parse_county_file_state_filter(self):
        """State filter restricts results."""
        records = self.collector._parse_county_file(
            SAMPLE_BPS_FILE, 2023, states_filter={"TX"}
        )
        assert len(records) == 2
        assert all(r["state"] == "TX" for r in records)

    def test_parse_county_file_skips_state_level(self):
        """County code 000 (state-level) is skipped."""
        data = """\
header1
header2

2023,48,000,3,7,State of Texas,1000,1000,500000000,0,0,0,0,0,0,0,0,0
2023,48,201,3,7,Harris County,150,150,85000000,0,0,0,0,0,0,0,0,0
"""
        records = self.collector._parse_county_file(data, 2023)
        assert len(records) == 1
        assert records[0]["county_fips"] == "48201"

    def test_state_fips_mapping(self):
        """STATE_FIPS contains expected states."""
        assert STATE_FIPS["TX"] == "48"
        assert STATE_FIPS["VA"] == "51"
        assert STATE_FIPS["CA"] == "06"
        assert len(STATE_FIPS) > 40

    def test_fips_to_state_reverse_mapping(self):
        """FIPS_TO_STATE is the inverse of STATE_FIPS."""
        assert FIPS_TO_STATE["48"] == "TX"
        assert FIPS_TO_STATE["51"] == "VA"

    def test_safe_int_valid(self):
        assert self.collector._safe_int("150") == 150
        assert self.collector._safe_int("0") == 0

    def test_safe_int_invalid(self):
        assert self.collector._safe_int(None) is None
        assert self.collector._safe_int("") is None
        assert self.collector._safe_int("-") is None

    def test_bps_url_template(self):
        """URL template includes year placeholder."""
        url = BPS_COUNTY_URL.format(year=2023)
        assert "co2023a.txt" in url
        assert "census.gov" in url
