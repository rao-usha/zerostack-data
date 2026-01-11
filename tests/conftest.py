"""
Pytest configuration and shared fixtures.
"""
import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.models import Base
from app.core.config import reset_settings


@pytest.fixture(scope="function")
def clean_env(monkeypatch):
    """
    Clean environment for testing.
    
    Removes all app-related env vars to ensure clean state.
    """
    env_vars = [
        "DATABASE_URL",
        "CENSUS_SURVEY_API_KEY",
        "MAX_CONCURRENCY",
        "LOG_LEVEL",
        "RUN_INTEGRATION_TESTS",
        "MAX_RETRIES",
        "RETRY_BACKOFF_FACTOR",
        "MAX_REQUESTS_PER_SECOND"
    ]
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)
    
    # Reset settings singleton
    reset_settings()
    
    yield
    
    # Reset again after test
    reset_settings()


@pytest.fixture(scope="function")
def test_db():
    """
    Create an in-memory SQLite database for testing.
    
    Fresh database for each test.
    """
    # Use in-memory SQLite
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


@pytest.fixture(scope="function")
def db_session(test_db):
    """
    Alias for test_db fixture for backward compatibility.
    """
    yield test_db


@pytest.fixture
def sample_census_metadata():
    """
    Sample Census metadata for testing (no network required).
    
    Simulates response from https://api.census.gov/data/2023/acs/acs5/variables.json
    """
    return {
        "variables": {
            "B01001_001E": {
                "label": "Estimate!!Total:",
                "concept": "SEX BY AGE",
                "predicateType": "int",
                "group": "B01001",
                "limit": 0,
                "predicateOnly": True
            },
            "B01001_002E": {
                "label": "Estimate!!Total:!!Male:",
                "concept": "SEX BY AGE",
                "predicateType": "int",
                "group": "B01001",
                "limit": 0,
                "predicateOnly": True
            },
            "B01001_003E": {
                "label": "Estimate!!Total:!!Male:!!Under 5 years",
                "concept": "SEX BY AGE",
                "predicateType": "int",
                "group": "B01001",
                "limit": 0,
                "predicateOnly": True
            },
            "B01001_001M": {
                "label": "Margin of Error!!Total:",
                "concept": "SEX BY AGE",
                "predicateType": "int",
                "group": "B01001",
                "limit": 0,
                "predicateOnly": True
            },
            "NAME": {
                "label": "Geographic Area Name",
                "concept": "Geography",
                "predicateType": "string",
                "group": "N/A",
                "limit": 0,
                "predicateOnly": False
            },
            "GEO_ID": {
                "label": "Geographic Identifier",
                "concept": "Geography",
                "predicateType": "string",
                "group": "N/A",
                "limit": 0,
                "predicateOnly": False
            },
            # Different table to test filtering
            "B02001_001E": {
                "label": "Estimate!!Total:",
                "concept": "RACE",
                "predicateType": "int",
                "group": "B02001",
                "limit": 0,
                "predicateOnly": True
            }
        }
    }





