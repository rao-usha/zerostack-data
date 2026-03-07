"""Tests for HIFLD transmission line collection."""

import pytest
from unittest.mock import MagicMock

from app.sources.site_intel.power.hifld_collector import HIFLDInfraCollector


SAMPLE_TRANSMISSION_FEATURE = {
    "attributes": {
        "OBJECTID_1": 12345,
        "OBJECTID": 305,
        "ID": "141176",
        "OWNER": "Pacific Gas & Electric",
        "VOLTAGE": 345,
        "VOLT_CLASS": "230-345",
        "Shape__Length": 50000.0,  # meters
        "TYPE": "AC; OVERHEAD",
        "STATUS": "IN SERVICE",
    }
}

SAMPLE_TRANSMISSION_FEATURE_DC = {
    "attributes": {
        "OBJECTID_1": 12346,
        "OBJECTID": 306,
        "ID": None,  # Falls back to OBJECTID_1
        "OWNER": "TransGrid Corp",
        "VOLTAGE": 500000,  # Volts, should auto-convert to kV
        "VOLT_CLASS": "500",
        "Shape__Length": 120000.0,
        "TYPE": "DC; UNDERGROUND",
        "STATUS": None,
    }
}

SAMPLE_TRANSMISSION_FEATURE_MINIMAL = {
    "attributes": {
        "OBJECTID_1": 12347,
    }
}

SAMPLE_TRANSMISSION_FEATURE_NO_ID = {
    "attributes": {
        "VOLTAGE": 115,
    }
}


@pytest.mark.unit
class TestTransmissionLineTransform:
    """Tests for _transform_transmission_line method."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = HIFLDInfraCollector(db=self.mock_db)

    def test_basic_transform(self):
        """Standard AC overhead transmission line."""
        result = self.collector._transform_transmission_line(
            SAMPLE_TRANSMISSION_FEATURE
        )
        assert result is not None
        assert result["hifld_id"] == "141176"
        assert result["owner"] == "Pacific Gas & Electric"
        assert result["voltage_kv"] == 345
        assert result["voltage_class"] == "230-345"
        assert result["line_type"] == "AC"
        assert result["sub_type"] == "overhead"
        assert result["status"] == "in service"
        assert result["source"] == "hifld"
        # Length: 50000m * 0.000621371 = ~31.069 miles
        assert result["length_miles"] is not None
        assert 31.0 < result["length_miles"] < 31.2

    def test_dc_underground_with_voltage_conversion(self):
        """DC underground line with voltage in volts (auto-convert to kV)."""
        result = self.collector._transform_transmission_line(
            SAMPLE_TRANSMISSION_FEATURE_DC
        )
        assert result is not None
        # OBJECTID_1 fallback when ID is None
        assert result["hifld_id"] == "12346"
        # Voltage: 500000V -> 500kV
        assert result["voltage_kv"] == 500.0
        assert result["line_type"] == "DC"
        assert result["sub_type"] == "underground"
        assert result["owner"] == "TransGrid Corp"

    def test_minimal_feature(self):
        """Feature with only OBJECTID still transforms."""
        result = self.collector._transform_transmission_line(
            SAMPLE_TRANSMISSION_FEATURE_MINIMAL
        )
        assert result is not None
        assert result["hifld_id"] == "12347"
        assert result["voltage_kv"] is None
        assert result["length_miles"] is None

    def test_no_id_returns_none(self):
        """Feature with no ID or OBJECTID is skipped."""
        result = self.collector._transform_transmission_line(
            SAMPLE_TRANSMISSION_FEATURE_NO_ID
        )
        assert result is None

    def test_empty_feature(self):
        """Empty attributes dict returns None."""
        result = self.collector._transform_transmission_line({"attributes": {}})
        assert result is None

    def test_length_conversion(self):
        """Shape__Length in meters converts to miles."""
        feature = {
            "attributes": {
                "OBJECTID_1": 1,
                "Shape__Length": 1609.344,  # exactly 1 mile in meters
            }
        }
        result = self.collector._transform_transmission_line(feature)
        assert result is not None
        assert abs(result["length_miles"] - 1.0) < 0.01

    def test_zero_length(self):
        """Zero Shape__Length yields None length."""
        feature = {
            "attributes": {
                "OBJECTID_1": 1,
                "Shape__Length": 0,
            }
        }
        result = self.collector._transform_transmission_line(feature)
        assert result["length_miles"] is None


@pytest.mark.unit
class TestHIFLDCollectorConfig:
    """Tests for HIFLD collector configuration."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = HIFLDInfraCollector(db=self.mock_db)

    def test_transmission_url_defined(self):
        """Transmission line URL is set."""
        assert self.collector.TRANSMISSION_URL is not None
        assert "Transmission_Lines" in self.collector.TRANSMISSION_URL
        assert "FeatureServer" in self.collector.TRANSMISSION_URL

    def test_collector_registered(self):
        """HIFLD collector is registered in COLLECTOR_REGISTRY."""
        from app.sources.site_intel.runner import COLLECTOR_REGISTRY
        from app.sources.site_intel.types import SiteIntelSource

        assert SiteIntelSource.HIFLD in COLLECTOR_REGISTRY

    def test_transmission_line_model_imported(self):
        """TransmissionLine model is importable."""
        from app.core.models_site_intel import TransmissionLine

        assert TransmissionLine.__tablename__ == "transmission_line"
