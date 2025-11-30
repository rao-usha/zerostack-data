"""
Integration tests for Census ingestion.

These tests make REAL API calls and require:
1. CENSUS_SURVEY_API_KEY to be set
2. RUN_INTEGRATION_TESTS=true

Run with: RUN_INTEGRATION_TESTS=true pytest tests/integration/
"""
import pytest
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.core.models import Base, IngestionJob, DatasetRegistry, JobStatus
from app.core.config import get_settings, reset_settings
from app.sources.census.ingest import ingest_acs_table


# Skip all tests in this module unless explicitly enabled
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def check_integration_enabled():
    """
    Check if integration tests are enabled.
    
    Skip entire module if not enabled.
    """
    # Check environment variable directly
    enabled = os.getenv("RUN_INTEGRATION_TESTS", "false").lower() in ("true", "1", "yes")
    
    if not enabled:
        pytest.skip(
            "Integration tests disabled. "
            "Set RUN_INTEGRATION_TESTS=true to enable."
        )
    
    # Check API key is present
    api_key = os.getenv("CENSUS_SURVEY_API_KEY")
    if not api_key:
        pytest.skip(
            "CENSUS_SURVEY_API_KEY not found. "
            "Integration tests require a valid Census API key."
        )


@pytest.fixture(scope="function")
def integration_db(check_integration_enabled):
    """
    Create a test database for integration tests.
    
    Uses SQLite for simplicity. In production, you'd use a test Postgres instance.
    """
    # Create in-memory SQLite database
    engine = create_engine("sqlite:///:memory:")
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    # Create session factory
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Create session
    db = TestingSessionLocal()
    
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)


@pytest.mark.asyncio
async def test_ingest_small_table(integration_db, check_integration_enabled):
    """
    Test ingesting a small ACS table.
    
    Uses B01001 (Sex by Age) at state level - relatively small dataset.
    """
    # Create a job record
    job = IngestionJob(
        source="census",
        status=JobStatus.PENDING,
        config={
            "survey": "acs5",
            "year": 2021,
            "table_id": "B01001",
            "geo_level": "state"
        }
    )
    integration_db.add(job)
    integration_db.commit()
    integration_db.refresh(job)
    
    # Run ingestion
    result = await ingest_acs_table(
        db=integration_db,
        job_id=job.id,
        survey="acs5",
        year=2021,
        table_id="B01001",
        geo_level="state"
    )
    
    # Verify results
    assert result["table_name"] == "acs5_2021_b01001"
    assert result["rows_inserted"] > 0  # Should have data for all states
    assert result["rows_inserted"] >= 50  # At least 50 states/territories
    
    # Verify table was created
    table_name = result["table_name"]
    count_query = integration_db.execute(
        text(f"SELECT COUNT(*) FROM {table_name}")
    )
    count = count_query.scalar()
    assert count == result["rows_inserted"]
    
    # Verify dataset was registered
    dataset = integration_db.query(DatasetRegistry).filter(
        DatasetRegistry.table_name == table_name
    ).first()
    assert dataset is not None
    assert dataset.source == "census"
    assert dataset.dataset_id == "acs5_2021_b01001"


@pytest.mark.asyncio
async def test_ingest_with_geo_filter(integration_db, check_integration_enabled):
    """
    Test ingesting data with geographic filter.
    
    Fetches data for California counties only.
    """
    job = IngestionJob(
        source="census",
        status=JobStatus.PENDING,
        config={
            "survey": "acs5",
            "year": 2021,
            "table_id": "B01001",
            "geo_level": "county",
            "geo_filter": {"state": "06"}  # California
        }
    )
    integration_db.add(job)
    integration_db.commit()
    integration_db.refresh(job)
    
    result = await ingest_acs_table(
        db=integration_db,
        job_id=job.id,
        survey="acs5",
        year=2021,
        table_id="B01001",
        geo_level="county",
        geo_filter={"state": "06"}
    )
    
    # Verify results
    assert result["table_name"] == "acs5_2021_b01001"
    assert result["rows_inserted"] > 0
    # California has 58 counties
    assert result["rows_inserted"] >= 50


@pytest.mark.asyncio
async def test_metadata_fetching(check_integration_enabled):
    """
    Test that metadata fetching works correctly.
    """
    from app.sources.census.client import CensusClient
    from app.sources.census import metadata
    
    settings = get_settings()
    api_key = settings.require_census_api_key()
    
    client = CensusClient(api_key=api_key)
    
    try:
        # Fetch metadata
        census_metadata = await client.fetch_table_metadata("acs5", 2021, "B01001")
        
        # Verify structure
        assert "variables" in census_metadata
        assert len(census_metadata["variables"]) > 0
        
        # Parse metadata
        table_vars = metadata.parse_table_metadata(census_metadata, "B01001")
        
        # B01001 has ~49 variables (sex by age breakdowns)
        assert len(table_vars) > 40
        
        # Verify all have required fields
        for var_name, var_info in table_vars.items():
            assert "label" in var_info
            assert "postgres_type" in var_info
            assert "column_name" in var_info
    
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_rate_limiting(check_integration_enabled):
    """
    Test that rate limiting works correctly.
    
    Makes multiple concurrent requests to verify bounded concurrency.
    """
    from app.sources.census.client import CensusClient
    import asyncio
    
    settings = get_settings()
    api_key = settings.require_census_api_key()
    
    # Create client with low concurrency
    client = CensusClient(api_key=api_key, max_concurrency=2)
    
    try:
        # Make multiple concurrent requests
        tasks = [
            client.fetch_table_metadata("acs5", 2021, "B01001")
            for _ in range(5)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # All should succeed
        assert len(results) == 5
        for result in results:
            assert "variables" in result
    
    finally:
        await client.close()





