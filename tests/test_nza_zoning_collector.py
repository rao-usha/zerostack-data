"""Tests for NZAZoningCollector – National Zoning Atlas."""

import io
import json
import zipfile

import pytest
from unittest.mock import MagicMock

from app.sources.site_intel.incentives.nza_zoning_collector import (
    NZAZoningCollector,
    normalize_category,
    infer_uses,
    NZA_STATE_FILES,
    MERCATUS_BASE,
)


SAMPLE_FEATURE = {
    "type": "Feature",
    "properties": {
        "ZoneCode": "I-1",
        "ZoneName": "Light Industrial",
        "Jurisdiction": "Hartford",
        "ZoneCategory": "Industrial",
    },
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[-72.7, 41.8], [-72.6, 41.8], [-72.6, 41.9], [-72.7, 41.9], [-72.7, 41.8]]],
    },
}

SAMPLE_FEATURE_RESIDENTIAL = {
    "type": "Feature",
    "properties": {
        "ZoneCode": "R-1",
        "ZoneName": "Single Family Residential",
        "Jurisdiction": "New Haven",
        "ZoneCategory": "Residential",
    },
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[-72.9, 41.3], [-72.8, 41.3], [-72.8, 41.4], [-72.9, 41.4], [-72.9, 41.3]]],
    },
}

SAMPLE_FEATURE_NH_KEYS = {
    "type": "Feature",
    "properties": {
        "AbbreviatedDistrict": "AV",
        "Full District Name": "Residential Acworth Village",
        "Jurisdiction": "Acworth",
        "Type of Zoning District": "Mixed with Residential",
    },
    "geometry": None,
}

SAMPLE_FEATURE_MT_KEYS = {
    "type": "Feature",
    "properties": {
        "T": "Belgrade",
        "Z": "Agricultural suburban",
        "Ty": "M",
    },
    "geometry": None,
}

SAMPLE_FEATURE_NO_CODE = {
    "type": "Feature",
    "properties": {
        "SomeUnrelatedField": "test",
    },
    "geometry": None,
}


@pytest.mark.unit
class TestNormalizeCategory:
    """Tests for category normalization."""

    def test_industrial(self):
        assert normalize_category("Industrial") == "industrial"

    def test_residential(self):
        assert normalize_category("Residential") == "residential"

    def test_single_family(self):
        assert normalize_category("Single Family Residential") == "residential"

    def test_commercial(self):
        assert normalize_category("Commercial") == "commercial"

    def test_mixed_use(self):
        assert normalize_category("Mixed Use") == "mixed"

    def test_mixed_use_hyphen(self):
        assert normalize_category("Mixed-Use") == "mixed"

    def test_manufacturing(self):
        assert normalize_category("Manufacturing") == "industrial"

    def test_agricultural(self):
        assert normalize_category("Agricultural") == "agricultural"

    def test_unknown(self):
        assert normalize_category("Something Weird") == "other"

    def test_none(self):
        assert normalize_category(None) == "other"

    def test_empty(self):
        assert normalize_category("") == "other"

    def test_case_insensitive(self):
        assert normalize_category("INDUSTRIAL") == "industrial"
        assert normalize_category("commercial") == "commercial"


@pytest.mark.unit
class TestInferUses:
    """Tests for use inference from category."""

    def test_industrial_allows_all(self):
        uses = infer_uses("industrial")
        assert uses["allows_manufacturing"] is True
        assert uses["allows_warehouse"] is True
        assert uses["allows_data_center"] is True

    def test_commercial_allows_warehouse_dc(self):
        uses = infer_uses("commercial")
        assert uses["allows_manufacturing"] is False
        assert uses["allows_warehouse"] is True
        assert uses["allows_data_center"] is True

    def test_residential_denies_all(self):
        uses = infer_uses("residential")
        assert uses["allows_manufacturing"] is False
        assert uses["allows_warehouse"] is False
        assert uses["allows_data_center"] is False

    def test_mixed_allows_all(self):
        uses = infer_uses("mixed")
        assert uses["allows_manufacturing"] is True
        assert uses["allows_warehouse"] is True
        assert uses["allows_data_center"] is True

    def test_other_denies_all(self):
        uses = infer_uses("other")
        assert uses["allows_manufacturing"] is False
        assert uses["allows_warehouse"] is False
        assert uses["allows_data_center"] is False


@pytest.mark.unit
class TestNZAZoningCollector:
    """Unit tests for the NZA zoning collector."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = NZAZoningCollector(db=self.mock_db)

    def test_collector_attributes(self):
        """Collector has correct domain and source."""
        from app.sources.site_intel.types import SiteIntelDomain, SiteIntelSource
        assert self.collector.domain == SiteIntelDomain.INCENTIVES
        assert self.collector.source == SiteIntelSource.NATIONAL_ZONING_ATLAS

    def test_transform_feature_industrial(self):
        """Industrial zone feature is transformed correctly."""
        record = self.collector._transform_feature(SAMPLE_FEATURE, "CT")
        assert record is not None
        assert record["zone_code"] == "I-1"
        assert record["zone_name"] == "Light Industrial"
        assert record["jurisdiction"] == "Hartford"
        assert record["state"] == "CT"
        assert record["zone_category"] == "industrial"
        assert record["allows_manufacturing"] is True
        assert record["allows_warehouse"] is True
        assert record["allows_data_center"] is True
        assert record["source"] == "national_zoning_atlas"

    def test_transform_feature_residential(self):
        """Residential zone feature denies industrial uses."""
        record = self.collector._transform_feature(SAMPLE_FEATURE_RESIDENTIAL, "CT")
        assert record is not None
        assert record["zone_code"] == "R-1"
        assert record["zone_category"] == "residential"
        assert record["allows_manufacturing"] is False
        assert record["allows_warehouse"] is False
        assert record["allows_data_center"] is False

    def test_transform_feature_nh_keys(self):
        """NH-style properties (AbbreviatedDistrict, Full District Name) are parsed."""
        record = self.collector._transform_feature(SAMPLE_FEATURE_NH_KEYS, "NH")
        assert record is not None
        assert record["zone_code"] == "AV"
        assert record["zone_name"] == "Residential Acworth Village"
        assert record["jurisdiction"] == "Acworth"
        assert record["zone_category"] == "mixed"

    def test_transform_feature_mt_keys(self):
        """MT-style properties (T, Z, Ty) are parsed."""
        record = self.collector._transform_feature(SAMPLE_FEATURE_MT_KEYS, "MT")
        assert record is not None
        assert record["zone_code"] == "Agricultural suburban"
        assert record["zone_name"] == "Agricultural suburban"
        assert record["jurisdiction"] == "Belgrade"
        assert record["zone_category"] == "mixed"

    def test_transform_feature_no_code(self):
        """Feature without zone_code returns None."""
        record = self.collector._transform_feature(SAMPLE_FEATURE_NO_CODE, "CT")
        assert record is None

    def test_transform_feature_geometry_passthrough(self):
        """GeoJSON geometry is passed through as-is."""
        record = self.collector._transform_feature(SAMPLE_FEATURE, "CT")
        assert record["geometry_geojson"]["type"] == "Polygon"
        assert len(record["geometry_geojson"]["coordinates"]) == 1

    def test_transform_feature_null_geometry(self):
        """Feature with null geometry still produces a record."""
        record = self.collector._transform_feature(SAMPLE_FEATURE_NH_KEYS, "NH")
        assert record is not None
        assert record["geometry_geojson"] is None

    def test_state_files_coverage(self):
        """At least 7 states are available."""
        assert len(NZA_STATE_FILES) >= 7
        assert "MT" in NZA_STATE_FILES
        assert "NH" in NZA_STATE_FILES

    def test_state_files_are_tuples(self):
        """Each state entry is a (path, region) tuple."""
        for state, entry in NZA_STATE_FILES.items():
            assert isinstance(entry, tuple), f"{state} entry is not a tuple"
            assert len(entry) == 2, f"{state} entry should be (path, region)"
            assert entry[0].startswith("/"), f"{state} path should start with /"

    def test_default_base_url(self):
        """Base URL points to Mercatus Center."""
        url = self.collector.get_default_base_url()
        assert url == MERCATUS_BASE
        assert "mercatus.org" in url

    def test_extract_geojson_from_zip(self):
        """GeoJSON features are extracted from a valid ZIP."""
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"ZoneCode": "I-1", "ZoneName": "Industrial"},
                    "geometry": None,
                }
            ],
        }
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("test_zoning.geojson", json.dumps(geojson_data))
        features = self.collector._extract_geojson_from_zip(buf.getvalue(), "CT")
        assert features is not None
        assert len(features) == 1
        assert features[0]["properties"]["ZoneCode"] == "I-1"

    def test_extract_geojson_from_zip_no_geojson(self):
        """ZIP without GeoJSON files returns None."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no data here")
        features = self.collector._extract_geojson_from_zip(buf.getvalue(), "CT")
        assert features is None

    def test_extract_geojson_bad_zip(self):
        """Invalid ZIP bytes returns None."""
        features = self.collector._extract_geojson_from_zip(b"not a zip", "CT")
        assert features is None

    def test_collector_registered(self):
        """NZA collector is registered in COLLECTOR_REGISTRY."""
        from app.sources.site_intel.runner import COLLECTOR_REGISTRY
        from app.sources.site_intel.types import SiteIntelSource
        assert SiteIntelSource.NATIONAL_ZONING_ATLAS in COLLECTOR_REGISTRY

    def test_transform_sets_collected_at(self):
        """Transformed record has a collected_at timestamp."""
        record = self.collector._transform_feature(SAMPLE_FEATURE, "CT")
        assert record["collected_at"] is not None
