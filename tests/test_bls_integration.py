"""
BLS integration tests.

These tests require:
- BLS_API_KEY environment variable (optional but recommended)
- RUN_INTEGRATION_TESTS=true environment variable
- Network access to BLS API

Run with: pytest tests/test_bls_integration.py -v
"""
import pytest
import os
from datetime import datetime

# Skip all tests if integration tests not enabled
pytestmark = pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION_TESTS", "").lower() == "true",
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=true to enable."
)


@pytest.fixture
def bls_client():
    """Create BLS client with API key from environment."""
    from app.sources.bls.client import BLSClient
    
    api_key = os.environ.get("BLS_API_KEY")
    return BLSClient(api_key=api_key)


@pytest.fixture
def current_year():
    """Get current year for test queries."""
    return datetime.now().year


class TestBLSClient:
    """Tests for BLS API client."""
    
    @pytest.mark.asyncio
    async def test_fetch_unemployment_rate(self, bls_client, current_year):
        """Test fetching unemployment rate series."""
        result = await bls_client.fetch_series(
            series_ids=["LNS14000000"],  # Unemployment rate
            start_year=current_year - 1,
            end_year=current_year
        )
        
        assert result is not None
        assert result.get("status") == "REQUEST_SUCCEEDED"
        assert "Results" in result
        assert "series" in result["Results"]
        assert len(result["Results"]["series"]) > 0
        
        series = result["Results"]["series"][0]
        assert series["seriesID"] == "LNS14000000"
        assert len(series["data"]) > 0
        
        await bls_client.close()
    
    @pytest.mark.asyncio
    async def test_fetch_multiple_series(self, bls_client, current_year):
        """Test fetching multiple series in one request."""
        series_ids = [
            "LNS14000000",  # Unemployment rate
            "CUUR0000SA0",  # CPI All items
        ]
        
        result = await bls_client.fetch_series(
            series_ids=series_ids,
            start_year=current_year - 1,
            end_year=current_year
        )
        
        assert result is not None
        assert result.get("status") == "REQUEST_SUCCEEDED"
        
        fetched_ids = [s["seriesID"] for s in result["Results"]["series"]]
        for sid in series_ids:
            assert sid in fetched_ids
        
        await bls_client.close()
    
    @pytest.mark.asyncio
    async def test_fetch_cpi_data(self, bls_client, current_year):
        """Test fetching CPI inflation data."""
        result = await bls_client.fetch_series(
            series_ids=["CUUR0000SA0", "CUUR0000SA0L1E"],  # CPI All items, Core CPI
            start_year=current_year - 1,
            end_year=current_year
        )
        
        assert result is not None
        assert result.get("status") == "REQUEST_SUCCEEDED"
        
        await bls_client.close()
    
    @pytest.mark.asyncio
    async def test_fetch_employment_data(self, bls_client, current_year):
        """Test fetching CES employment data."""
        result = await bls_client.fetch_series(
            series_ids=["CES0000000001"],  # Total nonfarm employment
            start_year=current_year - 1,
            end_year=current_year
        )
        
        assert result is not None
        assert result.get("status") == "REQUEST_SUCCEEDED"
        
        series = result["Results"]["series"][0]
        assert len(series["data"]) > 0
        
        # Verify data structure
        obs = series["data"][0]
        assert "year" in obs
        assert "period" in obs
        assert "value" in obs
        
        await bls_client.close()
    
    @pytest.mark.asyncio
    async def test_fetch_jolts_data(self, bls_client, current_year):
        """Test fetching JOLTS job openings data."""
        result = await bls_client.fetch_series(
            series_ids=["JTS000000000000000JOL"],  # Job openings level
            start_year=current_year - 2,
            end_year=current_year
        )
        
        assert result is not None
        assert result.get("status") == "REQUEST_SUCCEEDED"
        
        await bls_client.close()
    
    @pytest.mark.asyncio
    async def test_invalid_series_id(self, bls_client, current_year):
        """Test that invalid series IDs are handled gracefully."""
        # BLS returns empty data for invalid series, not an error
        result = await bls_client.fetch_series(
            series_ids=["INVALID_SERIES_ID_12345"],
            start_year=current_year - 1,
            end_year=current_year
        )
        
        # Should still succeed but with no data
        assert result is not None
        
        await bls_client.close()
    
    @pytest.mark.asyncio
    async def test_year_range_validation(self, bls_client):
        """Test year range validation."""
        # Without API key: 10 years max
        # With API key: 20 years max
        api_key_present = bls_client.api_key is not None
        
        if api_key_present:
            # Should succeed with 15 years
            result = await bls_client.fetch_series(
                series_ids=["LNS14000000"],
                start_year=2010,
                end_year=2024
            )
            assert result.get("status") == "REQUEST_SUCCEEDED"
        else:
            # Should fail with >10 years without key
            with pytest.raises(ValueError, match="Year range too large"):
                await bls_client.fetch_series(
                    series_ids=["LNS14000000"],
                    start_year=2000,
                    end_year=2024
                )
        
        await bls_client.close()


class TestBLSMetadata:
    """Tests for BLS metadata utilities."""
    
    def test_generate_table_name(self):
        """Test table name generation."""
        from app.sources.bls.metadata import generate_table_name
        
        assert generate_table_name("ces") == "bls_ces_employment"
        assert generate_table_name("cps") == "bls_cps_labor_force"
        assert generate_table_name("cpi") == "bls_cpi"
        assert generate_table_name("ppi") == "bls_ppi"
        assert generate_table_name("jolts") == "bls_jolts"
        
        with pytest.raises(ValueError):
            generate_table_name("invalid_dataset")
    
    def test_parse_observation(self):
        """Test parsing BLS API observation."""
        from app.sources.bls.metadata import parse_bls_observation
        
        obs = {
            "year": "2024",
            "period": "M01",
            "periodName": "January",
            "value": "3.7",
            "footnotes": [{"code": "P", "text": "Preliminary"}]
        }
        
        parsed = parse_bls_observation(obs, "LNS14000000")
        
        assert parsed is not None
        assert parsed["series_id"] == "LNS14000000"
        assert parsed["year"] == 2024
        assert parsed["period"] == "M01"
        assert parsed["period_name"] == "January"
        assert parsed["value"] == 3.7
        assert parsed["footnote_codes"] == "P"
    
    def test_parse_missing_value(self):
        """Test parsing observation with missing value."""
        from app.sources.bls.metadata import parse_bls_observation
        
        obs = {
            "year": "2024",
            "period": "M01",
            "periodName": "January",
            "value": "-",
            "footnotes": []
        }
        
        parsed = parse_bls_observation(obs, "TEST123")
        
        assert parsed is not None
        assert parsed["value"] is None
    
    def test_validate_year_range(self):
        """Test year range validation."""
        from app.sources.bls.metadata import validate_year_range
        
        # Valid range
        assert validate_year_range(2020, 2024, api_key_present=False) is True
        
        # Invalid: start > end
        with pytest.raises(ValueError):
            validate_year_range(2024, 2020, api_key_present=False)
        
        # Invalid: too many years without key
        with pytest.raises(ValueError):
            validate_year_range(2000, 2024, api_key_present=False)
        
        # Valid with key (up to 20 years)
        assert validate_year_range(2005, 2024, api_key_present=True) is True
    
    def test_get_default_date_range(self):
        """Test default date range calculation."""
        from app.sources.bls.metadata import get_default_date_range
        
        current_year = datetime.now().year
        
        # Without key: 10 years
        start, end = get_default_date_range(api_key_present=False)
        assert end == current_year
        assert end - start == 9  # 10 years inclusive
        
        # With key: 20 years
        start, end = get_default_date_range(api_key_present=True)
        assert end == current_year
        assert end - start == 19  # 20 years inclusive


class TestBLSSeriesReference:
    """Tests for BLS series reference data."""
    
    def test_get_series_for_dataset(self):
        """Test getting series IDs for a dataset."""
        from app.sources.bls.client import get_series_for_dataset
        
        cps_series = get_series_for_dataset("cps")
        assert len(cps_series) > 0
        assert "LNS14000000" in cps_series  # Unemployment rate
        
        ces_series = get_series_for_dataset("ces")
        assert "CES0000000001" in ces_series  # Total nonfarm
        
        with pytest.raises(ValueError):
            get_series_for_dataset("invalid")
    
    def test_get_series_info(self):
        """Test getting info about a series ID."""
        from app.sources.bls.client import get_series_info
        
        info = get_series_info("LNS14000000")
        assert info is not None
        assert info["dataset"] == "cps"
        assert info["name"] == "unemployment_rate"
        
        # Unknown series
        info = get_series_info("UNKNOWN123")
        assert info is None
    
    def test_series_reference(self):
        """Test series reference endpoint data."""
        from app.sources.bls.metadata import get_series_reference
        
        # All series
        ref = get_series_reference()
        assert "cps" in ref
        assert "ces" in ref
        assert "cpi" in ref
        
        # Single dataset
        ref = get_series_reference("cps")
        assert "cps" in ref
        assert "ces" not in ref


class TestBLSCreateTableSQL:
    """Tests for SQL generation."""
    
    def test_generate_create_table_sql(self):
        """Test CREATE TABLE SQL generation."""
        from app.sources.bls.metadata import generate_create_table_sql
        
        sql = generate_create_table_sql("bls_cpi", "cpi")
        
        # Check table creation
        assert "CREATE TABLE IF NOT EXISTS bls_cpi" in sql
        
        # Check required columns
        assert "series_id TEXT NOT NULL" in sql
        assert "year INTEGER NOT NULL" in sql
        assert "period TEXT NOT NULL" in sql
        assert "value NUMERIC" in sql
        
        # Check unique constraint
        assert "CONSTRAINT bls_cpi_unique UNIQUE (series_id, year, period)" in sql
        
        # Check indexes
        assert "CREATE INDEX IF NOT EXISTS idx_bls_cpi_series_id" in sql
        assert "CREATE INDEX IF NOT EXISTS idx_bls_cpi_year" in sql


# =============================================================================
# UNIT TESTS (no network required)
# =============================================================================

class TestBLSUnitTests:
    """Unit tests that don't require network access."""
    
    def test_client_initialization(self):
        """Test client initializes correctly."""
        from app.sources.bls.client import BLSClient
        
        # Without key
        client = BLSClient(api_key=None)
        assert client.max_series_per_request == 25
        
        # With key
        client = BLSClient(api_key="test_key")
        assert client.max_series_per_request == 50
    
    def test_common_series_structure(self):
        """Test that common series dictionaries are properly structured."""
        from app.sources.bls.client import (
            CPS_SERIES, CES_SERIES, JOLTS_SERIES, CPI_SERIES, PPI_SERIES
        )
        
        # Each should have entries
        assert len(CPS_SERIES) > 0
        assert len(CES_SERIES) > 0
        assert len(JOLTS_SERIES) > 0
        assert len(CPI_SERIES) > 0
        assert len(PPI_SERIES) > 0
        
        # Key series should exist
        assert "unemployment_rate" in CPS_SERIES
        assert "total_nonfarm" in CES_SERIES
        assert "job_openings_rate" in JOLTS_SERIES
        assert "cpi_all_items" in CPI_SERIES
        assert "ppi_final_demand" in PPI_SERIES
    
    def test_dataset_tables_mapping(self):
        """Test dataset to table mapping."""
        from app.sources.bls.metadata import DATASET_TABLES
        
        assert DATASET_TABLES["ces"] == "bls_ces_employment"
        assert DATASET_TABLES["cps"] == "bls_cps_labor_force"
        assert DATASET_TABLES["jolts"] == "bls_jolts"
        assert DATASET_TABLES["cpi"] == "bls_cpi"
        assert DATASET_TABLES["ppi"] == "bls_ppi"
