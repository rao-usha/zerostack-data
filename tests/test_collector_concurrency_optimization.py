"""
Tests for collector concurrency optimizations (PLAN 024 Phase 3).

Verifies that EPA Envirofacts, FCC Broadband, FRA Rail, and EPA SDWIS
collectors use collect_states_concurrent for state-level parallelism.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch


@pytest.mark.unit
class TestEPAEnvirofactsConcurrency:
    """EPA Envirofacts TRI collector uses collect_states_concurrent."""

    def setup_method(self):
        self.mock_db = MagicMock()

    @pytest.mark.asyncio
    async def test_uses_collect_states_concurrent(self):
        from app.sources.site_intel.risk.epa_envirofacts_collector import EPAEnvirofactsCollector
        from app.sources.site_intel.types import CollectionConfig, SiteIntelDomain, SiteIntelSource

        collector = EPAEnvirofactsCollector(db=self.mock_db)
        concurrent_calls = []
        original = collector.collect_states_concurrent

        async def tracking_concurrent(states, fn, max_concurrent=4):
            concurrent_calls.append({"states": states, "max_concurrent": max_concurrent})
            # Return empty list to skip DB ops
            return []

        collector.collect_states_concurrent = tracking_concurrent

        config = CollectionConfig(domain=SiteIntelDomain.RISK, source=SiteIntelSource.EPA_ENVIROFACTS, states=["TX", "CA"])
        await collector.collect(config)

        assert len(concurrent_calls) == 1
        assert concurrent_calls[0]["states"] == ["TX", "CA"]
        assert concurrent_calls[0]["max_concurrent"] == 4


@pytest.mark.unit
class TestFCCBroadbandConcurrency:
    """FCC Broadband collector uses collect_states_concurrent."""

    def setup_method(self):
        self.mock_db = MagicMock()

    @pytest.mark.asyncio
    async def test_uses_collect_states_concurrent(self):
        from app.sources.site_intel.telecom.fcc_broadband_collector import FCCBroadbandCollector
        from app.sources.site_intel.types import CollectionConfig, SiteIntelDomain, SiteIntelSource

        collector = FCCBroadbandCollector(db=self.mock_db)
        concurrent_calls = []

        async def tracking_concurrent(states, fn, max_concurrent=4):
            concurrent_calls.append({"states": states, "max_concurrent": max_concurrent})
            return []

        collector.collect_states_concurrent = tracking_concurrent
        collector.bulk_upsert = MagicMock(return_value=(0, 0))

        config = CollectionConfig(domain=SiteIntelDomain.TELECOM, source=SiteIntelSource.FCC, states=["TX", "NY"])
        await collector.collect(config)

        assert len(concurrent_calls) == 1
        assert concurrent_calls[0]["states"] == ["TX", "NY"]
        assert concurrent_calls[0]["max_concurrent"] == 4


@pytest.mark.unit
class TestFRARailConcurrency:
    """FRA Rail collector uses collect_states_concurrent."""

    def setup_method(self):
        self.mock_db = MagicMock()

    @pytest.mark.asyncio
    async def test_uses_collect_states_concurrent(self):
        from app.sources.site_intel.transport.fra_rail_collector import FRARailCollector
        from app.sources.site_intel.types import CollectionConfig, SiteIntelDomain, SiteIntelSource

        collector = FRARailCollector(db=self.mock_db)
        concurrent_calls = []

        async def tracking_concurrent(states, fn, max_concurrent=4):
            concurrent_calls.append({"states": states, "max_concurrent": max_concurrent})
            return []

        collector.collect_states_concurrent = tracking_concurrent
        collector.bulk_upsert = MagicMock(return_value=(0, 0))

        config = CollectionConfig(domain=SiteIntelDomain.TRANSPORT, source=SiteIntelSource.FRA, states=["TX", "OH"])
        await collector.collect(config)

        assert len(concurrent_calls) == 1
        assert concurrent_calls[0]["states"] == ["TX", "OH"]
        assert concurrent_calls[0]["max_concurrent"] == 4


@pytest.mark.unit
class TestEPASDWISConcurrency:
    """EPA SDWIS collector uses collect_states_concurrent for both loops."""

    def setup_method(self):
        self.mock_db = MagicMock()

    @pytest.mark.asyncio
    async def test_water_systems_uses_concurrent(self):
        from app.sources.site_intel.water_utilities.epa_sdwis_collector import EPASDWISCollector
        from app.sources.site_intel.types import CollectionConfig, SiteIntelDomain, SiteIntelSource

        collector = EPASDWISCollector(db=self.mock_db)
        concurrent_calls = []

        async def tracking_concurrent(states, fn, max_concurrent=4):
            concurrent_calls.append({"states": states, "max_concurrent": max_concurrent})
            return []

        collector.collect_states_concurrent = tracking_concurrent

        config = CollectionConfig(domain=SiteIntelDomain.WATER_UTILITIES, source=SiteIntelSource.EPA_SDWIS, states=["TX"])
        await collector.collect(config)

        # Should call collect_states_concurrent for water systems
        # Violations skipped since 0 water systems processed
        assert len(concurrent_calls) >= 1
        assert concurrent_calls[0]["states"] == ["TX"]
        assert concurrent_calls[0]["max_concurrent"] == 4

    @pytest.mark.asyncio
    async def test_violations_uses_concurrent(self):
        from app.sources.site_intel.water_utilities.epa_sdwis_collector import EPASDWISCollector
        from app.sources.site_intel.types import CollectionConfig, SiteIntelDomain, SiteIntelSource

        collector = EPASDWISCollector(db=self.mock_db)
        concurrent_calls = []

        async def tracking_concurrent(states, fn, max_concurrent=4):
            concurrent_calls.append({"states": states, "max_concurrent": max_concurrent})
            # Return some records for water systems to trigger violations phase
            return [{"pwsid": "TX0001", "pws_name": "Test"}]

        collector.collect_states_concurrent = tracking_concurrent
        collector.bulk_upsert = MagicMock(return_value=(1, 0))

        # Need to mock _transform_water_system to return a record
        collector._transform_water_system = MagicMock(return_value={
            "pwsid": "TX0001", "pws_name": "Test", "pws_type": "community",
            "state": "TX", "county": None, "city": None, "zip_code": None,
            "population_served": 1000, "service_connections": 100,
            "service_area_type": None, "primary_source_code": "GW",
            "primary_source_name": "groundwater", "source_water_protection": False,
            "is_active": True, "compliance_status": "compliant",
            "source": "epa_sdwis", "collected_at": "2026-01-01",
        })

        config = CollectionConfig(domain=SiteIntelDomain.WATER_UTILITIES, source=SiteIntelSource.EPA_SDWIS, states=["TX"])
        await collector.collect(config)

        # Should call collect_states_concurrent twice: once for systems, once for violations
        assert len(concurrent_calls) == 2
        assert concurrent_calls[1]["states"] == ["TX"]
        assert concurrent_calls[1]["max_concurrent"] == 4
