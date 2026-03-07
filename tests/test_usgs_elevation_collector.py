"""Tests for USGS 3DEP Elevation Collector."""

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from app.sources.site_intel.risk.usgs_elevation_collector import (
    USGS3DEPElevationCollector,
    EPQS_URL,
    COUNTY_CENTROIDS_URL,
    STATE_NAME_TO_ABBR,
    LAT_OFFSET,
    LNG_OFFSET,
    METERS_TO_FEET,
    MAX_CONCURRENT_COUNTIES,
    MAX_CONCURRENT_POINTS,
    BATCH_COMMIT_SIZE,
)


@pytest.mark.unit
class TestStateNameMapping:
    """Tests for the state name to abbreviation mapping."""

    def test_all_50_states_plus_dc_pr(self):
        """Should have all 50 states + DC + PR."""
        assert len(STATE_NAME_TO_ABBR) == 52

    def test_common_states(self):
        assert STATE_NAME_TO_ABBR["Texas"] == "TX"
        assert STATE_NAME_TO_ABBR["California"] == "CA"
        assert STATE_NAME_TO_ABBR["New York"] == "NY"
        assert STATE_NAME_TO_ABBR["Florida"] == "FL"

    def test_dc_and_pr(self):
        assert STATE_NAME_TO_ABBR["District of Columbia"] == "DC"
        assert STATE_NAME_TO_ABBR["Puerto Rico"] == "PR"


@pytest.mark.unit
class TestUSGSElevationCollector:
    """Unit tests for the USGS 3DEP elevation collector."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = USGS3DEPElevationCollector(db=self.mock_db)

    def test_collector_attributes(self):
        """Collector has correct domain and source."""
        from app.sources.site_intel.types import SiteIntelDomain, SiteIntelSource

        assert self.collector.domain == SiteIntelDomain.RISK
        assert self.collector.source == SiteIntelSource.USGS_3DEP

    def test_default_base_url(self):
        """Base URL points to EPQS endpoint."""
        url = self.collector.get_default_base_url()
        assert url == EPQS_URL
        assert "epqs.nationalmap.gov" in url

    def test_collector_registered(self):
        """USGS 3DEP collector is registered in COLLECTOR_REGISTRY."""
        from app.sources.site_intel.runner import COLLECTOR_REGISTRY
        from app.sources.site_intel.types import SiteIntelSource

        assert SiteIntelSource.USGS_3DEP in COLLECTOR_REGISTRY

    def test_rate_limit_delay_reduced(self):
        """Rate limit is reduced for concurrent operation."""
        assert self.collector.rate_limit_delay == 0.05

    def test_county_centroids_url(self):
        """County centroids URL points to Census Bureau."""
        assert "census.gov" in COUNTY_CENTROIDS_URL
        assert "CenPop2020" in COUNTY_CENTROIDS_URL

    def test_offsets_reasonable(self):
        """LAT/LNG offsets are roughly 5 miles."""
        assert 0.05 < LAT_OFFSET < 0.1
        assert 0.05 < LNG_OFFSET < 0.15

    def test_meters_to_feet_constant(self):
        """Conversion constant is correct."""
        assert abs(METERS_TO_FEET - 3.28084) < 0.001

    def test_concurrency_constants(self):
        """Concurrency settings are reasonable."""
        assert MAX_CONCURRENT_COUNTIES == 8
        assert MAX_CONCURRENT_POINTS == 5
        assert BATCH_COMMIT_SIZE == 50


@pytest.mark.unit
class TestCountyElevationModel:
    """Tests for the CountyElevation database model."""

    def test_model_tablename(self):
        from app.core.models_site_intel import CountyElevation

        assert CountyElevation.__tablename__ == "county_elevation"

    def test_model_has_fips(self):
        """Model has fips_code column."""
        from app.core.models_site_intel import CountyElevation

        columns = {c.name for c in CountyElevation.__table__.columns}
        assert "fips_code" in columns
        assert "state" in columns
        assert "county" in columns

    def test_model_has_elevation_fields(self):
        """Model has min/max/mean elevation fields."""
        from app.core.models_site_intel import CountyElevation

        columns = {c.name for c in CountyElevation.__table__.columns}
        assert "min_elevation_ft" in columns
        assert "max_elevation_ft" in columns
        assert "mean_elevation_ft" in columns
        assert "elevation_range_ft" in columns
        assert "sample_points" in columns

    def test_model_unique_constraint(self):
        """Model has unique constraint on fips_code."""
        from app.core.models_site_intel import CountyElevation

        constraints = [
            c.name
            for c in CountyElevation.__table__.constraints
            if hasattr(c, "name") and c.name
        ]
        assert "uq_county_elevation_fips" in constraints


@pytest.mark.unit
class TestConcurrentElevation:
    """Tests for concurrent elevation query behavior."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = USGS3DEPElevationCollector(db=self.mock_db)

    @pytest.mark.asyncio
    async def test_county_elevation_queries_5_points(self):
        """Should query 5 points per county concurrently."""
        call_count = 0

        async def mock_query(lat, lng):
            nonlocal call_count
            call_count += 1
            return 500.0

        self.collector._query_elevation = mock_query
        county = {"state": "TX", "county": "Harris", "fips_code": "48201", "lat": 29.76, "lng": -95.37}
        result = await self.collector._collect_county_elevation(county)

        assert call_count == 5
        assert result is not None
        assert result["fips_code"] == "48201"
        assert result["min_elevation_ft"] == 500.0
        assert result["max_elevation_ft"] == 500.0
        assert result["sample_points"] == 5

    @pytest.mark.asyncio
    async def test_county_elevation_handles_partial_failures(self):
        """Should succeed even if some points fail."""
        call_idx = 0

        async def mock_query(lat, lng):
            nonlocal call_idx
            call_idx += 1
            if call_idx % 2 == 0:
                raise Exception("EPQS error")
            return 1000.0

        self.collector._query_elevation = mock_query
        county = {"state": "CO", "county": "Denver", "fips_code": "08031", "lat": 39.74, "lng": -104.99}
        result = await self.collector._collect_county_elevation(county)

        assert result is not None
        assert result["sample_points"] == 3  # 3 of 5 succeed

    @pytest.mark.asyncio
    async def test_county_elevation_returns_none_all_fail(self):
        """Should return None if all 5 points fail."""
        async def mock_query(lat, lng):
            raise Exception("all fail")

        self.collector._query_elevation = mock_query
        county = {"state": "AK", "county": "Test", "fips_code": "02000", "lat": 64.0, "lng": -153.0}
        result = await self.collector._collect_county_elevation(county)

        assert result is None


@pytest.mark.unit
class TestBaseCollectorConcurrency:
    """Tests for gather_with_limit and collect_states_concurrent."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = USGS3DEPElevationCollector(db=self.mock_db)
        self.collector.rate_limit_delay = 0  # no delay in tests

    @pytest.mark.asyncio
    async def test_gather_with_limit_bounds_concurrency(self):
        """Semaphore should bound concurrent execution."""
        max_concurrent_seen = 0
        current_concurrent = 0
        lock = asyncio.Lock()

        async def tracked_coro(i):
            nonlocal max_concurrent_seen, current_concurrent
            async with lock:
                current_concurrent += 1
                if current_concurrent > max_concurrent_seen:
                    max_concurrent_seen = current_concurrent
            await asyncio.sleep(0.01)
            async with lock:
                current_concurrent -= 1
            return i

        coros = [tracked_coro(i) for i in range(20)]
        results = await self.collector.gather_with_limit(coros, max_concurrent=4)

        assert max_concurrent_seen <= 4
        non_exceptions = [r for r in results if not isinstance(r, Exception)]
        assert len(non_exceptions) == 20

    @pytest.mark.asyncio
    async def test_gather_with_limit_returns_exceptions(self):
        """Should capture exceptions without killing other tasks."""
        async def maybe_fail(i):
            if i == 2:
                raise ValueError("boom")
            return i

        coros = [maybe_fail(i) for i in range(5)]
        results = await self.collector.gather_with_limit(coros, max_concurrent=3)

        assert isinstance(results[2], ValueError)
        assert results[0] == 0
        assert results[1] == 1
        assert results[3] == 3
        assert results[4] == 4

    @pytest.mark.asyncio
    async def test_collect_states_concurrent_aggregates(self):
        """Should flatten results from all states."""
        async def mock_collect(state):
            return [{"state": state, "value": 1}, {"state": state, "value": 2}]

        result = await self.collector.collect_states_concurrent(
            ["TX", "CA", "NY"], mock_collect, max_concurrent=2
        )

        assert len(result) == 6
        states_seen = {r["state"] for r in result}
        assert states_seen == {"TX", "CA", "NY"}

    @pytest.mark.asyncio
    async def test_collect_states_concurrent_skips_failed(self):
        """Should skip failed states and continue."""
        async def mock_collect(state):
            if state == "CA":
                raise RuntimeError("API down")
            return [{"state": state}]

        result = await self.collector.collect_states_concurrent(
            ["TX", "CA", "NY"], mock_collect, max_concurrent=3
        )

        assert len(result) == 2
        states_seen = {r["state"] for r in result}
        assert states_seen == {"TX", "NY"}
