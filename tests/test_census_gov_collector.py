"""Tests for CensusGovCollector – datacenter site selection labor/governance layer."""

import pytest
from unittest.mock import MagicMock

from app.sources.site_intel.labor.census_gov_collector import (
    CensusGovCollector,
    UNIT_TYPE_PREFIX_MAP,
    COG_2022_URL,
)


@pytest.mark.unit
class TestCensusGovCollector:
    """Unit tests for Census of Governments collector."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = CensusGovCollector(db=self.mock_db)

    def test_unit_type_mapping(self):
        """Government type prefixes 1-5 map to correct category names."""
        expected = {
            "1": "county",
            "2": "municipal",
            "3": "township",
            "4": "special_district",
            "5": "school_district",
        }
        for code, name in expected.items():
            assert UNIT_TYPE_PREFIX_MAP[code] == name

    def test_unit_type_mapping_complete(self):
        """All 5 government types are mapped."""
        assert len(UNIT_TYPE_PREFIX_MAP) == 5

    def test_collector_attributes(self):
        """Collector has correct domain and source."""
        from app.sources.site_intel.types import SiteIntelDomain, SiteIntelSource

        assert self.collector.domain == SiteIntelDomain.LABOR
        assert self.collector.source == SiteIntelSource.CENSUS_GOV

    def test_collector_registered(self):
        """Census Gov collector is registered in COLLECTOR_REGISTRY."""
        from app.sources.site_intel.runner import COLLECTOR_REGISTRY
        from app.sources.site_intel.types import SiteIntelSource

        assert SiteIntelSource.CENSUS_GOV in COLLECTOR_REGISTRY

    def test_default_base_url(self):
        """Base URL points to Census datasets."""
        url = self.collector.get_default_base_url()
        assert "census.gov" in url

    def test_cog_url_is_zip(self):
        """COG data URL points to a ZIP file."""
        assert COG_2022_URL.endswith(".ZIP")
        assert "2022" in COG_2022_URL

    def test_safe_int_valid(self):
        assert self.collector._safe_int("100") == 100
        assert self.collector._safe_int("0") == 0

    def test_safe_int_invalid(self):
        assert self.collector._safe_int(None) is None
        assert self.collector._safe_int("") is None
        assert self.collector._safe_int("-") is None

    def test_safe_int_float_string(self):
        """Handles float-like strings (Excel exports)."""
        assert self.collector._safe_int("100.0") == 100
        assert self.collector._safe_int("4700000.0") == 4700000
