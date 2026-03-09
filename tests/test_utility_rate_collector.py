"""Tests for UtilityRateCollector — dual-source datacenter power cost dataset."""

import pytest
from unittest.mock import MagicMock

from app.sources.site_intel.power.utility_rate_collector import (
    UtilityRateCollector,
    CUSTOMER_CLASS_MAP,
    EIA_RETAIL_URL,
    OPENEI_API_URL,
    ALL_STATES,
)


@pytest.mark.unit
class TestUtilityRateCollector:
    """Unit tests for the utility rate collector."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = UtilityRateCollector(db=self.mock_db)

    def test_collector_attributes(self):
        """Collector has correct domain and source."""
        from app.sources.site_intel.types import SiteIntelDomain, SiteIntelSource

        assert self.collector.domain == SiteIntelDomain.POWER
        assert self.collector.source == SiteIntelSource.OPENEI_URDB

    def test_collector_registered(self):
        """UtilityRateCollector is registered in COLLECTOR_REGISTRY."""
        from app.sources.site_intel.runner import COLLECTOR_REGISTRY
        from app.sources.site_intel.types import SiteIntelSource

        assert SiteIntelSource.OPENEI_URDB in COLLECTOR_REGISTRY
        assert COLLECTOR_REGISTRY[SiteIntelSource.OPENEI_URDB] is UtilityRateCollector

    def test_default_base_url(self):
        """Base URL points to OpenEI."""
        url = self.collector.get_default_base_url()
        assert "openei.org" in url

    def test_all_states_count(self):
        """ALL_STATES has 50 states + DC."""
        assert len(ALL_STATES) == 51
        assert "TX" in ALL_STATES
        assert "CA" in ALL_STATES
        assert "DC" in ALL_STATES

    def test_customer_class_mapping(self):
        """Customer class map covers key sectors."""
        assert CUSTOMER_CLASS_MAP["Residential"] == "residential"
        assert CUSTOMER_CLASS_MAP["Commercial"] == "commercial"
        assert CUSTOMER_CLASS_MAP["Industrial"] == "industrial"
        assert CUSTOMER_CLASS_MAP["General"] == "commercial"

    def test_eia_url(self):
        """EIA retail URL is correct."""
        assert "eia.gov" in EIA_RETAIL_URL
        assert "retail-sales" in EIA_RETAIL_URL

    def test_openei_url(self):
        """OpenEI URDB URL is correct."""
        assert "openei.org" in OPENEI_API_URL
        assert "utility_rates" in OPENEI_API_URL

    def test_rate_limit_delay(self):
        """Rate limit is set for DEMO_KEY (30 req/hr)."""
        assert self.collector.rate_limit_delay >= 2.0


@pytest.mark.unit
class TestEIARateTransform:
    """Test EIA rate record transformation."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = UtilityRateCollector(db=self.mock_db)

    def test_transform_commercial(self):
        """Commercial EIA row transforms correctly."""
        row = {
            "period": "2023",
            "sectorid": "COM",
            "stateid": "TX",
            "stateDescription": "Texas",
            "sectorName": "Commercial",
            "price": "10.5",
        }
        result = self.collector._transform_eia_rate(row, "TX")
        assert result is not None
        assert result["rate_schedule_id"] == "eia_TX_COM_2023"
        assert result["customer_class"] == "commercial"
        assert result["state"] == "TX"
        # 10.5 cents/kWh -> 0.105 $/kWh
        assert result["energy_rate_kwh"] == 0.105
        assert result["source"] == "eia"
        assert result["has_demand_charges"] is False
        assert result["has_time_of_use"] is False

    def test_transform_industrial(self):
        """Industrial EIA row transforms correctly."""
        row = {
            "period": "2023",
            "sectorid": "IND",
            "stateid": "VA",
            "stateDescription": "Virginia",
            "sectorName": "Industrial",
            "price": "7.2",
        }
        result = self.collector._transform_eia_rate(row, "VA")
        assert result is not None
        assert result["customer_class"] == "industrial"
        assert result["energy_rate_kwh"] == 0.072

    def test_transform_no_price(self):
        """Missing price returns None."""
        row = {"period": "2023", "sectorid": "COM", "stateid": "TX"}
        result = self.collector._transform_eia_rate(row, "TX")
        assert result is None

    def test_transform_no_period(self):
        """Missing period returns None."""
        row = {"sectorid": "COM", "stateid": "TX", "price": "10.5"}
        result = self.collector._transform_eia_rate(row, "TX")
        assert result is None

    def test_transform_unknown_sector(self):
        """Unknown sector (not COM/IND) returns None."""
        row = {"period": "2023", "sectorid": "RES", "stateid": "TX", "price": "12.0"}
        result = self.collector._transform_eia_rate(row, "TX")
        assert result is None

    def test_transform_effective_date(self):
        """Effective date is set from period year."""
        row = {
            "period": "2022",
            "sectorid": "COM",
            "stateid": "OH",
            "stateDescription": "Ohio",
            "sectorName": "Commercial",
            "price": "9.5",
        }
        result = self.collector._transform_eia_rate(row, "OH")
        assert result["effective_date"].year == 2022


@pytest.mark.unit
class TestOpenEIRateTransform:
    """Test OpenEI tariff record transformation."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = UtilityRateCollector(db=self.mock_db)

    def test_transform_basic(self):
        """Basic OpenEI rate transforms correctly."""
        rate = {
            "label": "URDB-12345",
            "utility": "Pacific Gas & Electric",
            "eiaid": 14328,
            "sector": "Commercial",
            "name": "E-19 Large General Service",
        }
        result = self.collector._transform_openei_rate(rate, "CA")
        assert result is not None
        assert result["rate_schedule_id"] == "URDB-12345"
        assert result["utility_name"] == "Pacific Gas & Electric"
        assert result["customer_class"] == "commercial"
        assert result["state"] == "CA"
        assert result["source"] == "openei"

    def test_transform_with_energy_structure(self):
        """Energy rate structure is parsed into tiers."""
        rate = {
            "label": "RATE-001",
            "utility": "Test Utility",
            "sector": "Industrial",
            "energyratestructure": [
                [{"rate": 0.08, "max": 500}, {"rate": 0.10}]
            ],
        }
        result = self.collector._transform_openei_rate(rate, "TX")
        assert result is not None
        assert result["energy_rate_kwh"] == 0.08
        assert result["energy_tiers"] is not None

    def test_transform_with_demand_charges(self):
        """Demand charges are parsed correctly."""
        rate = {
            "label": "RATE-002",
            "utility": "Test Utility",
            "sector": "Commercial",
            "demandratestructure": [
                [{"rate": 15.5, "max": 1000}, {"rate": 12.0}]
            ],
        }
        result = self.collector._transform_openei_rate(rate, "VA")
        assert result is not None
        assert result["demand_charge_kw"] == 15.5
        assert result["has_demand_charges"] is True
        assert result["demand_tiers"] is not None

    def test_transform_with_flat_demand(self):
        """Flat demand charge fallback works."""
        rate = {
            "label": "RATE-003",
            "utility": "Test Utility",
            "sector": "Industrial",
            "flatdemandunit": "20.0",
        }
        result = self.collector._transform_openei_rate(rate, "OH")
        assert result is not None
        assert result["demand_charge_kw"] == 20.0
        assert result["has_demand_charges"] is True

    def test_transform_with_tou(self):
        """Time-of-use detection works."""
        rate = {
            "label": "RATE-004",
            "utility": "Test Utility",
            "sector": "Commercial",
            "energyweekdayschedule": [[0] * 24] * 12,
            "energyweekendschedule": [[0] * 24] * 12,
        }
        result = self.collector._transform_openei_rate(rate, "CA")
        assert result is not None
        assert result["has_time_of_use"] is True
        assert result["tou_periods"] is not None

    def test_transform_no_label(self):
        """Missing label returns None."""
        rate = {"utility": "Test Utility"}
        result = self.collector._transform_openei_rate(rate, "TX")
        assert result is None

    def test_transform_no_utility(self):
        """Missing utility returns None."""
        rate = {"label": "RATE-005"}
        result = self.collector._transform_openei_rate(rate, "TX")
        assert result is None

    def test_transform_fixed_charge(self):
        """Fixed monthly charge is parsed."""
        rate = {
            "label": "RATE-006",
            "utility": "Test Utility",
            "sector": "Commercial",
            "fixedmonthlycharge": "25.00",
        }
        result = self.collector._transform_openei_rate(rate, "NJ")
        assert result is not None
        assert result["fixed_monthly_charge"] == 25.0

    def test_transform_net_metering(self):
        """Net metering flag is detected."""
        rate = {
            "label": "RATE-007",
            "utility": "Test Utility",
            "sector": "Residential",
            "usenetmetering": True,
        }
        result = self.collector._transform_openei_rate(rate, "AZ")
        assert result is not None
        assert result["has_net_metering"] is True

    def test_transform_state_fallback(self):
        """State is extracted from rate data when not provided."""
        rate = {
            "label": "RATE-008",
            "utility": "Test Utility",
            "sector": "Commercial",
            "state": "NY",
        }
        result = self.collector._transform_openei_rate(rate, None)
        assert result["state"] == "NY"


@pytest.mark.unit
class TestParseFloat:
    """Test the _parse_float helper."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = UtilityRateCollector(db=self.mock_db)

    def test_valid_float(self):
        assert self.collector._parse_float("10.5") == 10.5

    def test_valid_int(self):
        assert self.collector._parse_float("100") == 100.0

    def test_none(self):
        assert self.collector._parse_float(None) is None

    def test_empty_string(self):
        assert self.collector._parse_float("") is None

    def test_dash(self):
        assert self.collector._parse_float("-") is None

    def test_numeric_input(self):
        assert self.collector._parse_float(0.08) == 0.08
