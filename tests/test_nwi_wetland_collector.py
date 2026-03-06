"""Tests for NWIWetlandCollector – USFWS National Wetlands Inventory."""

import pytest
from unittest.mock import MagicMock

from app.sources.site_intel.risk.nwi_wetland_collector import (
    NWIWetlandCollector,
    classify_wetland_type,
    STATE_BBOX,
    NWI_QUERY_URL,
)


# Sample outStatistics response features (aggregated by NWI code per state)
SAMPLE_STATS_FEATURE = {
    "attributes": {
        "Wetlands.ATTRIBUTE": "PFO1A",
        "Wetlands.WETLAND_TYPE": "Freshwater Forested/Shrub Wetland",
        "total_acres": 12345.67,
        "feature_count": 892,
    }
}

SAMPLE_STATS_FEATURE_NO_CODE = {
    "attributes": {
        "Wetlands.ATTRIBUTE": "",
        "Wetlands.WETLAND_TYPE": "Unknown",
        "total_acres": 0,
        "feature_count": 0,
    }
}

SAMPLE_STATS_FEATURE_NO_RAW_TYPE = {
    "attributes": {
        "Wetlands.ATTRIBUTE": "PEM1C",
        "Wetlands.WETLAND_TYPE": "",
        "total_acres": 55.0,
        "feature_count": 10,
    }
}


@pytest.mark.unit
class TestClassifyWetlandType:
    """Tests for the Cowardin code classifier."""

    def test_palustrine_forested(self):
        wtype, modifier = classify_wetland_type("PFO1A")
        assert wtype == "Palustrine"
        assert modifier == "Forested"

    def test_estuarine_emergent(self):
        wtype, modifier = classify_wetland_type("EEM")
        assert wtype == "Estuarine"
        assert modifier == "Emergent"

    def test_lacustrine(self):
        wtype, modifier = classify_wetland_type("LUB")
        assert wtype == "Lacustrine"
        assert modifier == "Unconsolidated Bottom"

    def test_riverine(self):
        wtype, modifier = classify_wetland_type("ROW")
        assert wtype == "Riverine"
        assert modifier == "Open Water"

    def test_marine(self):
        wtype, modifier = classify_wetland_type("MRF")
        assert wtype == "Marine"
        assert modifier == "Reef"

    def test_single_char_code(self):
        wtype, modifier = classify_wetland_type("P")
        assert wtype == "Palustrine"
        assert modifier is None

    def test_unknown_system(self):
        wtype, modifier = classify_wetland_type("XYZ")
        assert wtype == "Unknown"

    def test_empty_string(self):
        wtype, modifier = classify_wetland_type("")
        assert wtype == "Unknown"
        assert modifier is None

    def test_none(self):
        wtype, modifier = classify_wetland_type(None)
        assert wtype == "Unknown"
        assert modifier is None

    def test_scrub_shrub(self):
        wtype, modifier = classify_wetland_type("PSS")
        assert wtype == "Palustrine"
        assert modifier == "Scrub-Shrub"


@pytest.mark.unit
class TestNWIWetlandCollector:
    """Unit tests for the NWI wetland collector (outStatistics aggregation)."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = NWIWetlandCollector(db=self.mock_db)

    def test_collector_attributes(self):
        """Collector has correct domain and source."""
        from app.sources.site_intel.types import SiteIntelDomain, SiteIntelSource
        assert self.collector.domain == SiteIntelDomain.RISK
        assert self.collector.source == SiteIntelSource.USFWS_NWI

    def test_state_bbox_coverage(self):
        """All 50 states + DC have bounding boxes."""
        assert len(STATE_BBOX) == 51
        assert "CT" in STATE_BBOX
        assert "TX" in STATE_BBOX
        assert "DC" in STATE_BBOX

    def test_state_bbox_format(self):
        """Bounding boxes are (min_lng, min_lat, max_lng, max_lat) tuples."""
        for state, bbox in STATE_BBOX.items():
            assert len(bbox) == 4, f"{state} bbox has wrong length"
            min_lng, min_lat, max_lng, max_lat = bbox
            assert min_lat < max_lat, f"{state} min_lat >= max_lat"
            if state != "AK":
                assert min_lng < max_lng, f"{state} min_lng >= max_lng"

    def test_default_base_url(self):
        """Base URL points to NWI query endpoint."""
        url = self.collector.get_default_base_url()
        assert url == NWI_QUERY_URL
        assert "wetlandsmapservice" in url
        assert "MapServer" in url

    def test_collector_registered(self):
        """NWI collector is registered in COLLECTOR_REGISTRY."""
        from app.sources.site_intel.runner import COLLECTOR_REGISTRY
        from app.sources.site_intel.types import SiteIntelSource
        assert SiteIntelSource.USFWS_NWI in COLLECTOR_REGISTRY

    def test_timeout_is_generous(self):
        """outStatistics queries can be slow — timeout should be >= 120s."""
        assert self.collector.default_timeout >= 120.0

    def test_rate_limit_set(self):
        """Rate limit delay is set."""
        assert self.collector.rate_limit_delay > 0
