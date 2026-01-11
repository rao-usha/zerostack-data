"""
FCC Broadband integration tests.

Tests the FCC Broadband module against real FCC APIs.
Run with: pytest tests/test_fcc_broadband_integration.py -v

Note: Integration tests require network access.
Set RUN_INTEGRATION_TESTS=true to run these tests.
"""
import pytest
import os

# Skip integration tests unless explicitly enabled
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS", "false").lower() != "true",
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=true to run."
)


class TestFCCBroadbandClient:
    """Test FCCBroadbandClient against real FCC APIs."""
    
    @pytest.mark.asyncio
    async def test_fetch_state_summary(self):
        """Test fetching state broadband summary."""
        from app.sources.fcc_broadband.client import FCCBroadbandClient
        
        client = FCCBroadbandClient()
        try:
            # Test with California (FIPS: 06)
            result = await client.fetch_state_summary("06")
            
            # Should return some data (even if error/not_found response)
            assert result is not None
            assert isinstance(result, dict)
        finally:
            await client.close()
    
    @pytest.mark.asyncio
    async def test_fetch_county_summary(self):
        """Test fetching county broadband summary."""
        from app.sources.fcc_broadband.client import FCCBroadbandClient
        
        client = FCCBroadbandClient()
        try:
            # Test with Alameda County, CA (FIPS: 06001)
            result = await client.fetch_county_summary("06001")
            
            assert result is not None
            assert isinstance(result, dict)
        finally:
            await client.close()
    
    @pytest.mark.asyncio
    async def test_fetch_fixed_broadband_data(self):
        """Test fetching fixed broadband deployment data."""
        from app.sources.fcc_broadband.client import FCCBroadbandClient
        
        client = FCCBroadbandClient()
        try:
            # Fetch a small sample
            result = await client.fetch_fixed_broadband_data(
                state_fips="06",  # California
                limit=100,
                offset=0
            )
            
            assert result is not None
            assert isinstance(result, list)
            # Should return records (may be empty if API unavailable)
        finally:
            await client.close()
    
    @pytest.mark.asyncio
    async def test_fetch_location_coverage(self):
        """Test fetching coverage for a specific location."""
        from app.sources.fcc_broadband.client import FCCBroadbandClient
        
        client = FCCBroadbandClient()
        try:
            # San Francisco coordinates
            result = await client.fetch_location_coverage(37.7749, -122.4194)
            
            assert result is not None
            assert isinstance(result, dict)
        finally:
            await client.close()
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test that rate limiting works correctly."""
        from app.sources.fcc_broadband.client import FCCBroadbandClient
        import time
        
        client = FCCBroadbandClient(max_concurrency=2)
        try:
            start_time = time.time()
            
            # Make multiple requests
            for _ in range(3):
                await client.fetch_state_summary("06")
            
            elapsed = time.time() - start_time
            
            # Should take at least ~2 seconds due to rate limiting (60 req/min = 1 req/sec)
            # But allow some variance
            assert elapsed >= 1.5, "Rate limiting may not be working"
        finally:
            await client.close()


class TestFCCBroadbandMetadata:
    """Test metadata parsing and schema generation."""
    
    def test_generate_table_name(self):
        """Test table name generation."""
        from app.sources.fcc_broadband.metadata import generate_table_name
        
        assert generate_table_name("broadband_coverage") == "fcc_broadband_coverage"
        assert generate_table_name("broadband_summary") == "fcc_broadband_summary"
        assert generate_table_name("providers") == "fcc_providers"
    
    def test_classify_speed_tier(self):
        """Test speed tier classification."""
        from app.sources.fcc_broadband.metadata import classify_speed_tier
        
        assert classify_speed_tier(10) == "sub_broadband"
        assert classify_speed_tier(25) == "basic_broadband"
        assert classify_speed_tier(100) == "high_speed"
        assert classify_speed_tier(1000) == "gigabit"
        assert classify_speed_tier(None) == "unknown"
    
    def test_classify_competition(self):
        """Test competition classification."""
        from app.sources.fcc_broadband.metadata import classify_competition
        
        assert classify_competition(1) == "monopoly"
        assert classify_competition(2) == "duopoly"
        assert classify_competition(3) == "limited"
        assert classify_competition(5) == "competitive"
    
    def test_get_technology_name(self):
        """Test technology name lookup."""
        from app.sources.fcc_broadband.metadata import get_technology_name
        
        assert get_technology_name("50") == "Fiber to the Premises (FTTP)"
        assert get_technology_name("40") == "Cable Modem - DOCSIS 3.0"
        assert get_technology_name("10") == "Asymmetric xDSL"
        assert "Unknown" in get_technology_name("999")
    
    def test_is_fiber(self):
        """Test fiber detection."""
        from app.sources.fcc_broadband.metadata import is_fiber
        
        assert is_fiber("50") is True
        assert is_fiber("40") is False
        assert is_fiber(50) is False  # Must be string
    
    def test_generate_create_table_sql(self):
        """Test SQL generation."""
        from app.sources.fcc_broadband.metadata import generate_create_table_sql
        
        sql = generate_create_table_sql("fcc_broadband_coverage", "broadband_coverage")
        
        # Check for key elements
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "fcc_broadband_coverage" in sql
        assert "geography_type" in sql
        assert "provider_id" in sql
        assert "technology_code" in sql
        assert "SERIAL PRIMARY KEY" in sql
        assert "CREATE INDEX IF NOT EXISTS" in sql
    
    def test_parse_broadband_coverage_response(self):
        """Test coverage response parsing."""
        from app.sources.fcc_broadband.metadata import parse_broadband_coverage_response
        
        # Mock API response
        mock_records = [
            {
                "provider_id": "123456",
                "provider_name": "Test ISP",
                "technology_code": "50",
                "max_advertised_down_mbps": 1000,
                "max_advertised_up_mbps": 1000,
                "business": True,
                "consumer": True
            },
            {
                "frn": "654321",
                "dba_name": "Another ISP",
                "techcode": "40",
                "maxaddown": 500,
                "maxadup": 20
            }
        ]
        
        parsed = parse_broadband_coverage_response(
            mock_records,
            geography_type="state",
            geography_id="06",
            geography_name="California"
        )
        
        assert len(parsed) == 2
        assert parsed[0]["provider_id"] == "123456"
        assert parsed[0]["technology_name"] == "Fiber to the Premises (FTTP)"
        assert parsed[0]["speed_tier"] == "gigabit"
        assert parsed[1]["provider_id"] == "654321"
    
    def test_parse_broadband_summary(self):
        """Test summary generation."""
        from app.sources.fcc_broadband.metadata import parse_broadband_summary
        
        mock_records = [
            {
                "provider_id": "1",
                "technology_code": "50",
                "max_advertised_down_mbps": 1000
            },
            {
                "provider_id": "2",
                "technology_code": "40",
                "max_advertised_down_mbps": 500
            },
            {
                "provider_id": "3",
                "technology_code": "10",
                "max_advertised_down_mbps": 20
            }
        ]
        
        summary = parse_broadband_summary(
            mock_records,
            geography_type="state",
            geography_id="06",
            geography_name="California"
        )
        
        assert summary is not None
        assert summary["total_providers"] == 3
        assert summary["fiber_available"] is True
        assert summary["cable_available"] is True
        assert summary["dsl_available"] is True
        assert summary["max_speed_down_mbps"] == 1000
        assert summary["provider_competition"] == "limited"


class TestFCCBroadbandReferences:
    """Test reference data."""
    
    def test_state_fips_mapping(self):
        """Test state FIPS code mapping."""
        from app.sources.fcc_broadband.client import STATE_FIPS, US_STATES
        
        # All 50 states + DC should be present
        assert len(US_STATES) == 51
        
        # Check some known mappings
        assert STATE_FIPS["CA"] == "06"
        assert STATE_FIPS["NY"] == "36"
        assert STATE_FIPS["TX"] == "48"
        assert STATE_FIPS["DC"] == "11"
    
    def test_technology_codes(self):
        """Test technology codes reference."""
        from app.sources.fcc_broadband.client import TECHNOLOGY_CODES
        
        # Should have common technology types
        assert "50" in TECHNOLOGY_CODES  # Fiber
        assert "40" in TECHNOLOGY_CODES  # Cable
        assert "10" in TECHNOLOGY_CODES  # DSL
        assert "60" in TECHNOLOGY_CODES  # Satellite


class TestFCCBroadbandAPI:
    """Test API endpoints (requires running server)."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from app.main import app
        return TestClient(app)
    
    def test_reference_states_endpoint(self, client):
        """Test GET /fcc-broadband/reference/states."""
        response = client.get("/api/v1/fcc-broadband/reference/states")
        
        assert response.status_code == 200
        data = response.json()
        assert "states" in data
        assert data["count"] == 51
        
        # Check structure
        states = data["states"]
        assert len(states) > 0
        assert "code" in states[0]
        assert "fips" in states[0]
        assert "name" in states[0]
    
    def test_reference_technologies_endpoint(self, client):
        """Test GET /fcc-broadband/reference/technologies."""
        response = client.get("/api/v1/fcc-broadband/reference/technologies")
        
        assert response.status_code == 200
        data = response.json()
        assert "technologies" in data
        assert "categories" in data
    
    def test_reference_speed_tiers_endpoint(self, client):
        """Test GET /fcc-broadband/reference/speed-tiers."""
        response = client.get("/api/v1/fcc-broadband/reference/speed-tiers")
        
        assert response.status_code == 200
        data = response.json()
        assert "fcc_broadband_definition" in data
        assert "speed_tiers" in data
        assert data["fcc_broadband_definition"]["download_mbps"] == 25
    
    def test_datasets_endpoint(self, client):
        """Test GET /fcc-broadband/datasets."""
        response = client.get("/api/v1/fcc-broadband/datasets")
        
        assert response.status_code == 200
        data = response.json()
        assert "datasets" in data
        assert "use_cases" in data
        assert "api_info" in data
        assert data["api_info"]["api_key_required"] is False
    
    def test_invalid_state_code_rejected(self, client):
        """Test that invalid state codes are rejected."""
        response = client.post(
            "/api/v1/fcc-broadband/state/ingest",
            json={
                "state_codes": ["XX", "YY"],
                "include_summary": True
            }
        )
        
        assert response.status_code == 400
        assert "Invalid state codes" in response.json()["detail"]
    
    def test_invalid_county_fips_rejected(self, client):
        """Test that invalid county FIPS codes are rejected."""
        response = client.post(
            "/api/v1/fcc-broadband/county/ingest",
            json={
                "county_fips_codes": ["123", "abcde"],  # Invalid
                "include_summary": True
            }
        )
        
        assert response.status_code == 400
        assert "Invalid county FIPS" in response.json()["detail"]


# Unit tests (don't require network)
class TestFCCBroadbandUnit:
    """Unit tests that don't require network access."""
    
    def test_client_initialization(self):
        """Test client initializes correctly."""
        from app.sources.fcc_broadband.client import FCCBroadbandClient
        
        client = FCCBroadbandClient(
            max_concurrency=5,
            max_retries=2,
            backoff_factor=1.5
        )
        
        assert client.max_concurrency == 5
        assert client.max_retries == 2
        assert client.backoff_factor == 1.5
    
    def test_semaphore_created(self):
        """Test that semaphore is created for bounded concurrency."""
        from app.sources.fcc_broadband.client import FCCBroadbandClient
        import asyncio
        
        client = FCCBroadbandClient(max_concurrency=3)
        
        assert isinstance(client.semaphore, asyncio.Semaphore)
    
    def test_state_fips_lookup(self):
        """Test state FIPS code lookup."""
        from app.sources.fcc_broadband.metadata import STATE_FIPS
        
        assert STATE_FIPS.get("CA") == "06"
        assert STATE_FIPS.get("NY") == "36"
        assert STATE_FIPS.get("INVALID") is None
    
    def test_state_names_lookup(self):
        """Test state name lookup from FIPS."""
        from app.sources.fcc_broadband.metadata import STATE_NAMES
        
        assert STATE_NAMES.get("06") == "California"
        assert STATE_NAMES.get("36") == "New York"
        assert STATE_NAMES.get("99") is None
