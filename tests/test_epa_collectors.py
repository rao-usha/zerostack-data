"""
Unit tests for EPA SDWIS and Envirofacts collectors.

Tests pagination logic, date parsing, record transformation,
and bulk upsert behavior using mocked HTTP responses.
"""
import pytest
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import app.core.models_site_intel  # noqa: F401 — register site_intel tables with Base

from app.sources.site_intel.water_utilities.epa_sdwis_collector import (
    EPASDWISCollector,
    PWS_TYPE_MAP,
)
from app.sources.site_intel.risk.epa_envirofacts_collector import EPAEnvirofactsCollector
from app.sources.site_intel.types import (
    CollectionConfig, CollectionStatus, SiteIntelDomain, SiteIntelSource,
)


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def sdwis_collector(test_db):
    """Create an SDWIS collector with a test DB session."""
    collector = EPASDWISCollector(db=test_db)
    # Give it a mock job so update_progress/create_result don't fail
    collector._job = MagicMock()
    collector._job.id = 1
    collector._job.started_at = datetime.utcnow()
    return collector


@pytest.fixture
def enviro_collector(test_db):
    """Create an Envirofacts collector with a test DB session."""
    collector = EPAEnvirofactsCollector(db=test_db)
    collector._job = MagicMock()
    collector._job.id = 1
    collector._job.started_at = datetime.utcnow()
    return collector


@pytest.fixture
def sdwis_config():
    """Collection config for SDWIS single state."""
    return CollectionConfig(
        domain=SiteIntelDomain.WATER_UTILITIES,
        source=SiteIntelSource.EPA_SDWIS,
        job_type="collect",
        states=["DE"],
    )


@pytest.fixture
def enviro_config():
    """Collection config for Envirofacts single state."""
    return CollectionConfig(
        domain=SiteIntelDomain.RISK,
        source=SiteIntelSource.EPA_ENVIROFACTS,
        job_type="collect",
        states=["DE"],
    )


# =========================================================================
# SDWIS — Date Parsing
# =========================================================================


class TestSDWISDateParsing:
    """Test _parse_date handles ISO and EPA DD-MON-YY formats."""

    def test_iso_format(self, sdwis_collector):
        assert sdwis_collector._parse_date("2024-03-15") == date(2024, 3, 15)

    def test_epa_dd_mon_yy(self, sdwis_collector):
        assert sdwis_collector._parse_date("01-JAN-23") == date(2023, 1, 1)

    def test_epa_dd_mon_yyyy(self, sdwis_collector):
        assert sdwis_collector._parse_date("15-MAR-2024") == date(2024, 3, 15)

    def test_none_returns_none(self, sdwis_collector):
        assert sdwis_collector._parse_date(None) is None

    def test_empty_returns_none(self, sdwis_collector):
        assert sdwis_collector._parse_date("") is None

    def test_garbage_returns_none(self, sdwis_collector):
        assert sdwis_collector._parse_date("not-a-date") is None

    def test_whitespace_stripped(self, sdwis_collector):
        assert sdwis_collector._parse_date("  2024-03-15  ") == date(2024, 3, 15)


# =========================================================================
# SDWIS — Water System Transform
# =========================================================================


class TestSDWISTransform:
    """Test _transform_water_system field mapping."""

    def test_basic_transform(self, sdwis_collector):
        raw = {
            "pwsid": "TX0001234",
            "pws_name": "City of Dallas",
            "pws_type_code": "CWS",
            "pws_activity_code": "A",
            "state_code": "TX",
            "county_served": "Dallas",
            "city_name": "Dallas",
            "zip_code": "75201",
            "population_served_count": "1500000",
            "service_connections_count": "350000",
            "service_area_type_code": "R",
            "primary_source_code": "SW",
            "source_water_protection_code": "Y",
        }
        result = sdwis_collector._transform_water_system(raw)

        assert result is not None
        assert result["pwsid"] == "TX0001234"
        assert result["pws_name"] == "City of Dallas"
        assert result["pws_type"] == "community"
        assert result["is_active"] is True
        assert result["population_served"] == 1500000
        assert result["service_connections"] == 350000
        assert result["primary_source_name"] == "surface_water"
        assert result["source_water_protection"] is True

    def test_missing_pwsid_returns_none(self, sdwis_collector):
        assert sdwis_collector._transform_water_system({}) is None

    def test_inactive_system(self, sdwis_collector):
        raw = {"pwsid": "TX999", "pws_activity_code": "I", "pws_type_code": ""}
        result = sdwis_collector._transform_water_system(raw)
        assert result["is_active"] is False
        assert result["compliance_status"] == "inactive"


# =========================================================================
# SDWIS — Violation Transform
# =========================================================================


class TestSDWISViolationTransform:
    """Test _transform_violation field mapping."""

    def test_basic_violation(self, sdwis_collector):
        raw = {
            "pwsid": "TX0001234",
            "violation_id": "V001",
            "violation_category_code": "MCL",
            "contaminant_code": "1005",
            "contaminant_name": "Barium",
            "rule_group_code": "IOC",
            "compl_per_begin_date": "01-JAN-23",
            "is_health_based_ind": "Y",
            "severity_ind_cnt": "3",
            "rtc_date": "15-MAR-23",
        }
        result = sdwis_collector._transform_violation(raw)

        assert result is not None
        assert result["violation_id"] == "TX0001234_V001"
        assert result["violation_type"] == "MCL"
        assert result["contaminant_name"] == "Barium"
        assert result["violation_date"] == date(2023, 1, 1)
        assert result["is_health_based"] is True
        assert result["returned_to_compliance"] is True
        assert result["returned_to_compliance_date"] == date(2023, 3, 15)

    def test_missing_ids_returns_none(self, sdwis_collector):
        assert sdwis_collector._transform_violation({}) is None
        assert sdwis_collector._transform_violation({"pwsid": "TX001"}) is None


# =========================================================================
# SDWIS — Pagination
# =========================================================================


class TestSDWISPagination:
    """Test that water system collection paginates correctly."""

    @pytest.mark.asyncio
    async def test_paginates_large_state(self, sdwis_collector, sdwis_config):
        """When API returns PAGE_SIZE records, collector fetches another page."""
        page1 = [{"pwsid": f"DE{i:07d}", "pws_type_code": "CWS", "pws_activity_code": "A",
                   "state_code": "DE"} for i in range(5000)]
        page2 = [{"pwsid": f"DE{i:07d}", "pws_type_code": "CWS", "pws_activity_code": "A",
                   "state_code": "DE"} for i in range(5000, 5100)]

        mock_responses = []
        for data in [page1, page2]:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = data
            mock_responses.append(resp)

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=mock_responses)

        with patch.object(sdwis_collector, "get_client", return_value=mock_client), \
             patch.object(sdwis_collector, "apply_rate_limit", new_callable=AsyncMock), \
             patch.object(sdwis_collector, "bulk_upsert", return_value=(5100, 0)):

            result = await sdwis_collector._collect_water_systems(sdwis_config)

        assert result["processed"] == 5100
        assert mock_client.get.await_count == 2

    @pytest.mark.asyncio
    async def test_single_page_no_extra_request(self, sdwis_collector, sdwis_config):
        """When API returns fewer than PAGE_SIZE records, no second request."""
        page1 = [{"pwsid": f"DE{i:04d}", "pws_type_code": "CWS", "pws_activity_code": "A",
                   "state_code": "DE"} for i in range(200)]

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = page1

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)

        with patch.object(sdwis_collector, "get_client", return_value=mock_client), \
             patch.object(sdwis_collector, "apply_rate_limit", new_callable=AsyncMock), \
             patch.object(sdwis_collector, "bulk_upsert", return_value=(200, 0)):

            result = await sdwis_collector._collect_water_systems(sdwis_config)

        assert result["processed"] == 200
        assert mock_client.get.await_count == 1


# =========================================================================
# Envirofacts — TRI Facility Transform
# =========================================================================


class TestEnvirofactsTransform:
    """Test _transform_tri_facility field mapping."""

    def test_basic_transform(self, enviro_collector):
        raw = {
            "tri_facility_id": "77001CHMCL12345",
            "facility_name": "ACME Chemical Plant",
            "street_address": "123 Industrial Blvd",
            "city_name": "Houston",
            "state_abbr": "TX",
            "zip_code": "77001",
            "latitude": "29.7604",
            "longitude": "-95.3698",
            "industry_sector_code": "Chemical Manufacturing",
            "parent_co_name": "ACME Corp",
            "closed_ind": None,
        }
        result = enviro_collector._transform_tri_facility(raw)

        assert result is not None
        assert result["epa_id"] == "77001CHMCL12345"
        assert result["facility_name"] == "ACME Chemical Plant"
        assert result["state"] == "TX"
        assert result["latitude"] == pytest.approx(29.7604)
        assert result["longitude"] == pytest.approx(-95.3698)
        assert result["permits"] == ["TRI"]
        assert result["is_superfund"] is False
        assert result["is_brownfield"] is False
        assert result["source"] == "epa_tri"

    def test_closed_facility_excluded(self, enviro_collector):
        raw = {
            "tri_facility_id": "CLOSED001",
            "facility_name": "Old Factory",
            "closed_ind": "Y",
        }
        assert enviro_collector._transform_tri_facility(raw) is None

    def test_missing_id_returns_none(self, enviro_collector):
        assert enviro_collector._transform_tri_facility({}) is None

    def test_missing_name_returns_none(self, enviro_collector):
        raw = {"tri_facility_id": "ID001", "facility_name": None}
        assert enviro_collector._transform_tri_facility(raw) is None

    def test_bad_coordinates_become_none(self, enviro_collector):
        raw = {
            "tri_facility_id": "ID002",
            "facility_name": "Some Plant",
            "latitude": "not_a_number",
            "longitude": "",
        }
        result = enviro_collector._transform_tri_facility(raw)
        assert result["latitude"] is None
        assert result["longitude"] is None


# =========================================================================
# Envirofacts — Pagination
# =========================================================================


class TestEnvirofactsPagination:
    """Test TRI collection paginates correctly."""

    @pytest.mark.asyncio
    async def test_paginates_large_state(self, enviro_collector, enviro_config):
        page1 = [{"tri_facility_id": f"DE{i:05d}", "facility_name": f"Plant {i}",
                   "state_abbr": "DE"} for i in range(5000)]
        page2 = [{"tri_facility_id": f"DE{i:05d}", "facility_name": f"Plant {i}",
                   "state_abbr": "DE"} for i in range(5000, 5050)]

        mock_responses = []
        for data in [page1, page2]:
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = data
            mock_responses.append(resp)

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=mock_responses)

        with patch.object(enviro_collector, "get_client", return_value=mock_client), \
             patch.object(enviro_collector, "apply_rate_limit", new_callable=AsyncMock), \
             patch.object(enviro_collector, "bulk_upsert", return_value=(5050, 0)):

            result = await enviro_collector._collect_tri_facilities(enviro_config)

        assert result["processed"] == 5050
        assert mock_client.get.await_count == 2

    @pytest.mark.asyncio
    async def test_empty_state_returns_zero(self, enviro_collector, enviro_config):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = []

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)

        with patch.object(enviro_collector, "get_client", return_value=mock_client), \
             patch.object(enviro_collector, "apply_rate_limit", new_callable=AsyncMock):

            result = await enviro_collector._collect_tri_facilities(enviro_config)

        assert result["processed"] == 0
        assert result["inserted"] == 0


# =========================================================================
# Envirofacts — Full Collection Flow
# =========================================================================


class TestEnvirofactsCollect:
    """Test the top-level collect() method."""

    @pytest.mark.asyncio
    async def test_collect_success(self, enviro_collector, enviro_config):
        facilities = [
            {"tri_facility_id": "DE00001", "facility_name": "Plant A", "state_abbr": "DE",
             "latitude": "39.0", "longitude": "-75.0"},
        ]

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = facilities

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)

        with patch.object(enviro_collector, "get_client", return_value=mock_client), \
             patch.object(enviro_collector, "apply_rate_limit", new_callable=AsyncMock), \
             patch.object(enviro_collector, "bulk_upsert", return_value=(1, 0)):

            result = await enviro_collector.collect(enviro_config)

        assert result.status == CollectionStatus.SUCCESS
        assert result.inserted_items == 1

    @pytest.mark.asyncio
    async def test_collect_handles_api_error(self, enviro_collector, enviro_config):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("Connection timeout"))

        with patch.object(enviro_collector, "get_client", return_value=mock_client), \
             patch.object(enviro_collector, "apply_rate_limit", new_callable=AsyncMock):

            result = await enviro_collector.collect(enviro_config)

        # Should return success with 0 records (error is per-state, not fatal)
        assert result.status == CollectionStatus.SUCCESS
        assert result.inserted_items == 0
