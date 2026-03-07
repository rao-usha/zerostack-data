"""Tests for EpochDatacenterCollector – datacenter site selection telecom layer."""

import pytest
from unittest.mock import MagicMock

from app.sources.site_intel.telecom.epoch_dc_collector import (
    EpochDatacenterCollector,
    parse_dms,
    clean_tag,
)


@pytest.mark.unit
class TestEpochDatacenterCollector:
    """Unit tests for Epoch datacenter facility collector."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = EpochDatacenterCollector(db=self.mock_db)

    def test_parse_dms_north(self):
        """DMS coordinates with N direction parse to positive decimal."""
        result = parse_dms('32\u00b035\'25"N')
        assert result is not None
        assert result == pytest.approx(32.5902778, abs=0.0001)

    def test_parse_dms_west(self):
        """DMS coordinates with W direction parse to negative decimal."""
        result = parse_dms('90\u00b005\'35"W')
        assert result is not None
        assert result == pytest.approx(-90.0930556, abs=0.0001)

    def test_parse_dms_empty(self):
        """Empty/None DMS strings return None."""
        assert parse_dms("") is None
        assert parse_dms(None) is None

    def test_clean_tag(self):
        """Epoch confidence tags are stripped from values."""
        assert clean_tag("Amazon #confident") == "Amazon"
        assert clean_tag("Google Cloud #confident") == "Google Cloud"
        assert clean_tag("Anthropic #speculative") == "Anthropic"
        assert clean_tag("Plain text") == "Plain text"

    def test_transform_row_us_facility(self):
        """US facility rows are transformed correctly using new CSV format."""
        row = {
            "Handle": "Google Pryor Oklahoma",
            "Title": "Google Pryor (North)",
            "Address": "4581 Webb St, Pryor, OK 74361",
            "Latitude": '36\u00b014\'28"N',
            "Longitude": '95\u00b019\'50"W',
            "Owner": "Google Cloud #confident",
            "Current power (MW)": "65",
            "Current H100 equivalents": "17938.35",
        }
        result = self.collector._transform_row(row)
        assert result is not None
        assert result["epoch_id"] == "google_pryor_oklahoma"
        assert result["company"] == "Google Cloud"
        assert result["facility_name"] == "Google Pryor (North)"
        assert result["state"] == "OK"
        assert result["city"] == "Pryor"
        assert result["country"] == "US"
        assert result["latitude"] == pytest.approx(36.2411, abs=0.001)
        assert result["longitude"] == pytest.approx(-95.3306, abs=0.001)
        assert result["power_capacity_mw"] == pytest.approx(65.0)
        assert result["status"] == "operational"

    def test_transform_row_non_us_filtered(self):
        """Non-US datacenter rows return None (filtered out)."""
        row = {
            "Handle": "Alibaba Zhangbei Zhangjiakou Hebei",
            "Title": "Alibaba Zhangbei",
            "Address": "Zhangbei, Hebei, China",
            "Latitude": '40\u00b041\'00"N',
            "Longitude": '114\u00b042\'00"E',
            "Owner": "Alibaba",
            "Current power (MW)": "102",
        }
        result = self.collector._transform_row(row)
        assert result is None

    def test_safe_float_with_commas(self):
        """Numeric strings with commas are parsed correctly."""
        assert self.collector._safe_float("1,234.5") == pytest.approx(1234.5)
        assert self.collector._safe_float("2,500,000") == pytest.approx(2500000.0)

    def test_safe_float_invalid(self):
        """_safe_float returns None for non-numeric values."""
        assert self.collector._safe_float(None) is None
        assert self.collector._safe_float("") is None
        assert self.collector._safe_float("-") is None
        assert self.collector._safe_float("N/A") is None

    def test_safe_int_with_commas(self):
        """Integer strings with commas are parsed correctly."""
        assert self.collector._safe_int("2,500,000") == 2500000
        assert self.collector._safe_int("100") == 100

    def test_safe_int_invalid(self):
        """_safe_int returns None for non-numeric values."""
        assert self.collector._safe_int(None) is None
        assert self.collector._safe_int("") is None
        assert self.collector._safe_int("N/A") is None

    def test_transform_row_state_filter(self):
        """When states filter is passed, only matching states are included."""
        row_tx = {
            "Handle": "xAI Colossus Memphis Tennessee",
            "Title": "xAI Colossus",
            "Address": "Memphis, TN 38118",
            "Latitude": '35\u00b007\'00"N',
            "Longitude": '89\u00b058\'00"W',
            "Owner": "xAI",
            "Current power (MW)": "150",
        }
        row_ok = {
            "Handle": "Google Pryor Oklahoma",
            "Title": "Google Pryor",
            "Address": "4581 Webb St, Pryor, OK 74361",
            "Latitude": '36\u00b014\'28"N',
            "Longitude": '95\u00b019\'50"W',
            "Owner": "Google",
            "Current power (MW)": "65",
        }
        assert self.collector._transform_row(row_tx, states=["TN", "TX"]) is not None
        assert self.collector._transform_row(row_ok, states=["TN", "TX"]) is None

    def test_address_parsing_with_state_code(self):
        """State code in address is correctly extracted."""
        loc = self.collector._parse_address(
            "55001 Larrison Blvd, New Carlisle, IN 46552",
            "Anthropic-Amazon Project Rainier New Carlisle Indiana",
        )
        assert loc["state"] == "IN"
        assert loc["city"] == "New Carlisle"

    def test_address_parsing_full_state_name(self):
        """Full state name in address is correctly matched."""
        loc = self.collector._parse_address(
            "Canton, Mississippi Madison County",
            "Amazon Canton Mississippi",
        )
        assert loc["state"] == "MS"

    def test_address_parsing_handle_fallback(self):
        """State is extracted from Handle when address has no state."""
        loc = self.collector._parse_address(
            "Some unknown address format",
            "Meta Temple Texas",
        )
        assert loc["state"] == "TX"

    def test_source_field(self):
        """Transformed rows include source='epoch_ai'."""
        row = {
            "Handle": "QTS Cedar Rapids Iowa",
            "Title": "QTS Cedar Rapids",
            "Address": "Cedar Rapids, IA 52404",
            "Latitude": '41\u00b058\'00"N',
            "Longitude": '91\u00b040\'00"W',
            "Owner": "QTS",
            "Current power (MW)": "0",
        }
        result = self.collector._transform_row(row)
        assert result is not None
        assert result["source"] == "epoch_ai"

    def test_collector_attributes(self):
        """Collector has correct domain and source attributes."""
        from app.sources.site_intel.types import SiteIntelDomain, SiteIntelSource
        assert self.collector.domain == SiteIntelDomain.TELECOM
        assert self.collector.source == SiteIntelSource.EPOCH_DC

    def test_status_operational_when_power(self):
        """Facilities with power > 0 are marked operational."""
        row = {
            "Handle": "Test DC Texas",
            "Title": "Test DC",
            "Address": "Dallas, TX 75001",
            "Latitude": '32\u00b048\'00"N',
            "Longitude": '96\u00b049\'00"W',
            "Owner": "Test",
            "Current power (MW)": "50",
            "Current H100 equivalents": "0",
        }
        result = self.collector._transform_row(row)
        assert result["status"] == "operational"

    def test_status_under_construction_when_no_power(self):
        """Facilities with no power are marked under_construction."""
        row = {
            "Handle": "Test DC2 Texas",
            "Title": "Test DC2",
            "Address": "Austin, TX 73301",
            "Latitude": '30\u00b016\'00"N',
            "Longitude": '97\u00b044\'00"W',
            "Owner": "Test",
            "Current power (MW)": "0",
            "Current H100 equivalents": "0",
        }
        result = self.collector._transform_row(row)
        assert result["status"] == "under_construction"
