"""
Integration tests for FRED data ingestion.

These tests verify the complete FRED ingestion pipeline:
- API client with rate limiting and retries
- Table creation with typed columns
- Data parsing and insertion
- Job tracking
- Dataset registry

Run with: pytest tests/test_fred_integration.py -v
"""
import pytest
import asyncio
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, DatasetRegistry, JobStatus
from app.sources.fred.client import FREDClient, COMMON_SERIES
from app.sources.fred import ingest, metadata


@pytest.fixture
def mock_fred_api_response():
    """Mock FRED API response for testing."""
    return {
        "realtime_start": "2023-01-01",
        "realtime_end": "2023-12-31",
        "observation_start": "2020-01-01",
        "observation_end": "2020-12-31",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 5,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {
                "realtime_start": "2023-01-01",
                "realtime_end": "2023-12-31",
                "date": "2020-01-01",
                "value": "1.55"
            },
            {
                "realtime_start": "2023-01-01",
                "realtime_end": "2023-12-31",
                "date": "2020-01-02",
                "value": "1.58"
            },
            {
                "realtime_start": "2023-01-01",
                "realtime_end": "2023-12-31",
                "date": "2020-01-03",
                "value": "."  # Missing data
            },
            {
                "realtime_start": "2023-01-01",
                "realtime_end": "2023-12-31",
                "date": "2020-01-04",
                "value": "1.60"
            },
            {
                "realtime_start": "2023-01-01",
                "realtime_end": "2023-12-31",
                "date": "2020-01-05",
                "value": "1.62"
            }
        ]
    }


class TestFREDMetadata:
    """Test FRED metadata utilities."""
    
    def test_generate_table_name(self):
        """Test table name generation."""
        assert metadata.generate_table_name("interest_rates") == "fred_interest_rates"
        assert metadata.generate_table_name("monetary_aggregates") == "fred_monetary_aggregates"
        assert metadata.generate_table_name("Industrial Production") == "fred_industrial_production"
    
    def test_get_series_for_category(self):
        """Test getting series IDs for categories."""
        interest_rates = metadata.get_series_for_category("interest_rates")
        assert isinstance(interest_rates, list)
        assert len(interest_rates) > 0
        assert "DFF" in interest_rates  # Federal Funds Rate
        
        monetary_aggregates = metadata.get_series_for_category("monetary_aggregates")
        assert "M1SL" in monetary_aggregates
        assert "M2SL" in monetary_aggregates
        
        with pytest.raises(ValueError):
            metadata.get_series_for_category("invalid_category")
    
    def test_parse_observations(self, mock_fred_api_response):
        """Test parsing FRED API observations."""
        parsed = metadata.parse_observations(mock_fred_api_response, "DFF")
        
        # Should skip the missing data point (".")
        assert len(parsed) == 4
        
        # Check first row
        assert parsed[0]["series_id"] == "DFF"
        assert parsed[0]["date"] == "2020-01-01"
        assert parsed[0]["value"] == 1.55
        assert parsed[0]["realtime_start"] == "2023-01-01"
        assert parsed[0]["realtime_end"] == "2023-12-31"
    
    def test_validate_date_format(self):
        """Test date format validation."""
        assert metadata.validate_date_format("2023-01-01")
        assert metadata.validate_date_format("2020-12-31")
        assert not metadata.validate_date_format("2023/01/01")
        assert not metadata.validate_date_format("01-01-2023")
        assert not metadata.validate_date_format("invalid")
    
    def test_get_category_display_name(self):
        """Test category display name generation."""
        assert "Interest Rates" in metadata.get_category_display_name("interest_rates")
        assert "Monetary Aggregates" in metadata.get_category_display_name("monetary_aggregates")
    
    def test_get_category_description(self):
        """Test category description generation."""
        desc = metadata.get_category_description("interest_rates")
        assert "Federal Funds Rate" in desc or "interest rates" in desc.lower()


class TestFREDClient:
    """Test FRED API client."""
    
    @pytest.mark.asyncio
    async def test_client_initialization(self):
        """Test client initialization with bounded concurrency."""
        client = FREDClient(api_key="test_key", max_concurrency=5)
        
        assert client.api_key == "test_key"
        assert client.max_concurrency == 5
        assert client.semaphore._value == 5  # Semaphore initialized correctly
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_client_without_api_key(self):
        """Test client works without API key (throttled mode)."""
        client = FREDClient(api_key=None)
        
        assert client.api_key is None
        assert client.max_concurrency == FREDClient.DEFAULT_MAX_CONCURRENCY
        
        await client.close()
    
    def test_common_series_structure(self):
        """Test that COMMON_SERIES has expected structure."""
        assert "interest_rates" in COMMON_SERIES
        assert "monetary_aggregates" in COMMON_SERIES
        assert "industrial_production" in COMMON_SERIES
        assert "economic_indicators" in COMMON_SERIES
        
        # Check interest rates
        ir = COMMON_SERIES["interest_rates"]
        assert "federal_funds_rate" in ir
        assert ir["federal_funds_rate"] == "DFF"
        assert "10y_treasury" in ir
        
        # Check monetary aggregates
        ma = COMMON_SERIES["monetary_aggregates"]
        assert "m1" in ma
        assert ma["m1"] == "M1SL"
        assert "m2" in ma
        assert ma["m2"] == "M2SL"


class TestFREDTableCreation:
    """Test FRED table creation."""
    
    def test_generate_create_table_sql(self):
        """Test CREATE TABLE SQL generation."""
        series_ids = ["DFF", "DGS10", "M1SL"]
        sql = metadata.generate_create_table_sql("fred_test", series_ids)
        
        # Check table creation
        assert "CREATE TABLE IF NOT EXISTS fred_test" in sql
        
        # Check column definitions (typed columns, not JSON)
        assert "series_id TEXT NOT NULL" in sql
        assert "date DATE NOT NULL" in sql
        assert "value NUMERIC" in sql
        assert "realtime_start DATE" in sql
        assert "realtime_end DATE" in sql
        assert "ingested_at TIMESTAMP DEFAULT NOW()" in sql
        
        # Check primary key
        assert "PRIMARY KEY (series_id, date)" in sql
        
        # Check indexes
        assert "CREATE INDEX IF NOT EXISTS idx_fred_test_date" in sql
        assert "CREATE INDEX IF NOT EXISTS idx_fred_test_series_id" in sql
        
        # Check comment
        assert "COMMENT ON TABLE fred_test" in sql


@pytest.mark.asyncio
@pytest.mark.integration
class TestFREDIntegration:
    """
    Integration tests for FRED ingestion.

    These tests require:
    - Database connection (DATABASE_URL env var)
    - FRED API key (optional, will use throttled mode without key)

    Run with: pytest tests/test_fred_integration.py -v -m integration
    """

    async def test_full_ingestion_pipeline(self, db_session: Session):
        """Test complete FRED ingestion pipeline."""
        import os

        # Skip if DATABASE_URL not configured (required by Settings)
        if not os.environ.get("DATABASE_URL"):
            pytest.skip("DATABASE_URL not configured - skipping integration test")

        # Skip if no database
        if not db_session:
            pytest.skip("Database not available")
        
        # Create job
        job = IngestionJob(
            source="fred",
            status=JobStatus.PENDING,
            config={"category": "interest_rates"}
        )
        db_session.add(job)
        db_session.commit()
        db_session.refresh(job)
        
        # Run ingestion (small date range to be fast)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)  # Only 30 days
        
        try:
            result = await ingest.ingest_fred_category(
                db=db_session,
                job_id=job.id,
                category="interest_rates",
                series_ids=["DFF", "DGS10"],  # Only 2 series
                observation_start=start_date.strftime("%Y-%m-%d"),
                observation_end=end_date.strftime("%Y-%m-%d"),
                api_key=None  # Test without API key
            )
            
            # Verify result
            assert result["category"] == "interest_rates"
            assert result["series_count"] == 2
            assert result["rows_inserted"] > 0
            
            # Check job status
            db_session.refresh(job)
            assert job.status == JobStatus.SUCCESS
            assert job.rows_inserted > 0
            
            # Check dataset registry
            dataset = db_session.query(DatasetRegistry).filter(
                DatasetRegistry.source == "fred",
                DatasetRegistry.dataset_id == "fred_interest_rates"
            ).first()
            
            assert dataset is not None
            assert dataset.table_name == "fred_interest_rates"
            assert dataset.display_name is not None
            
            # Verify data in table
            table_name = result["table_name"]
            query = text(f"SELECT COUNT(*) FROM {table_name}")
            count = db_session.execute(query).scalar()
            assert count > 0
            
            # Verify data structure
            query = text(f"SELECT * FROM {table_name} LIMIT 1")
            row = db_session.execute(query).first()
            assert row is not None
            
        except Exception as e:
            # If it's a network error or rate limit, that's expected without API key
            if "rate" in str(e).lower() or "network" in str(e).lower():
                pytest.skip(f"Expected error without API key: {e}")
            else:
                raise


def test_fred_categories_complete():
    """Test that all required FRED categories are defined."""
    required_categories = [
        "interest_rates",
        "monetary_aggregates",
        "industrial_production",
        "economic_indicators"
    ]
    
    for category in required_categories:
        assert category in COMMON_SERIES, f"Missing category: {category}"
        assert len(COMMON_SERIES[category]) > 0, f"Empty category: {category}"


def test_fred_series_coverage():
    """Test that key FRED series are included."""
    # Key series that should be present
    key_series = {
        "DFF": "interest_rates",  # Federal Funds Rate
        "DGS10": "interest_rates",  # 10-Year Treasury
        "M1SL": "monetary_aggregates",  # M1 Money Stock
        "M2SL": "monetary_aggregates",  # M2 Money Stock
        "GDP": "economic_indicators",  # GDP
        "UNRATE": "economic_indicators",  # Unemployment Rate
        "INDPRO": "industrial_production",  # Industrial Production
    }
    
    for series_id, expected_category in key_series.items():
        category_series = COMMON_SERIES[expected_category]
        found = series_id in category_series.values()
        assert found, f"Missing key series {series_id} in {expected_category}"


def test_fred_api_urls():
    """Test FRED API URL configuration."""
    client = FREDClient()
    
    assert client.BASE_URL == "https://api.stlouisfed.org/fred"
    assert "fred" in client.BASE_URL.lower()
    assert client.BASE_URL.startswith("https://")

