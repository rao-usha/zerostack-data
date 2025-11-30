"""
Unit tests for Census client URL building.

Tests URL construction WITHOUT making any network requests.
"""
import pytest
from app.sources.census.client import CensusClient


@pytest.mark.unit
def test_census_client_initialization():
    """Test Census client initialization."""
    client = CensusClient(
        api_key="test_key",
        max_concurrency=4,
        max_retries=3,
        backoff_factor=2.0
    )
    
    assert client.api_key == "test_key"
    assert client.max_concurrency == 4
    assert client.max_retries == 3
    assert client.backoff_factor == 2.0


@pytest.mark.unit
def test_build_metadata_url():
    """Test metadata URL construction."""
    client = CensusClient(api_key="test_key")
    
    url = client.build_metadata_url(
        survey="acs5",
        year=2023,
        table_id="B01001"
    )
    
    assert url == "https://api.census.gov/data/2023/acs/acs5/variables.json"


@pytest.mark.unit
def test_build_data_url_simple():
    """Test data URL construction with simple parameters."""
    client = CensusClient(api_key="test_key_123")
    
    url = client.build_data_url(
        survey="acs5",
        year=2023,
        variables=["B01001_001E", "B01001_002E"],
        geo_level="state"
    )
    
    # Should include all required components
    assert "https://api.census.gov/data/2023/acs/acs5" in url
    assert "get=NAME" in url
    assert "B01001_001E" in url
    assert "B01001_002E" in url
    assert "for=state%3A%2A" in url or "for=state:*" in url
    assert "key=test_key_123" in url


@pytest.mark.unit
def test_build_data_url_with_filter():
    """Test data URL construction with geographic filter."""
    client = CensusClient(api_key="test_key")
    
    url = client.build_data_url(
        survey="acs5",
        year=2023,
        variables=["B01001_001E"],
        geo_level="county",
        geo_filter={"state": "06"}
    )
    
    # Should include state filter
    assert "state:06" in url or "state%3A06" in url
    assert "county" in url


@pytest.mark.unit
def test_build_data_url_different_surveys():
    """Test URL building for different survey types."""
    client = CensusClient(api_key="test_key")
    
    # ACS 1-year
    url = client.build_data_url(
        survey="acs1",
        year=2022,
        variables=["B01001_001E"],
        geo_level="state"
    )
    assert "/2022/acs/acs1" in url
    
    # ACS 5-year
    url = client.build_data_url(
        survey="acs5",
        year=2021,
        variables=["B01001_001E"],
        geo_level="state"
    )
    assert "/2021/acs/acs5" in url


@pytest.mark.unit
def test_build_data_url_multiple_variables():
    """Test URL building with many variables."""
    client = CensusClient(api_key="test_key")
    
    variables = [f"B01001_{i:03d}E" for i in range(1, 10)]
    
    url = client.build_data_url(
        survey="acs5",
        year=2023,
        variables=variables,
        geo_level="state"
    )
    
    # All variables should be in URL
    for var in variables:
        assert var in url





