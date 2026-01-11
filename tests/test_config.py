"""
Unit tests for configuration module.

Tests run WITHOUT .env file and WITHOUT API keys.
"""
import pytest
import os
from app.core.config import (
    Settings,
    get_settings,
    reset_settings,
    MissingCensusAPIKeyError
)


@pytest.mark.unit
def test_config_requires_database_url(clean_env, monkeypatch):
    """Database URL is required for app startup."""
    # Should raise validation error without DATABASE_URL
    with pytest.raises(Exception):  # Pydantic validation error
        Settings()


@pytest.mark.unit
def test_config_census_key_optional_for_startup(clean_env, monkeypatch):
    """Census API key is optional for app startup."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")

    # Disable .env file loading for this test
    settings = Settings(_env_file=None)
    assert settings.database_url == "postgresql://test"
    assert settings.census_survey_api_key is None


@pytest.mark.unit
def test_config_census_key_required_for_ingestion(clean_env, monkeypatch):
    """Census API key is required when calling require_census_api_key()."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")

    # Disable .env file loading for this test
    settings = Settings(_env_file=None)

    # Should raise clear error when trying to ingest without key
    with pytest.raises(MissingCensusAPIKeyError) as exc_info:
        settings.require_census_api_key()

    assert "CENSUS_SURVEY_API_KEY is required" in str(exc_info.value)
    assert "https://api.census.gov" in str(exc_info.value)


@pytest.mark.unit
def test_config_census_key_validation(clean_env, monkeypatch):
    """Census API key is validated when present."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    monkeypatch.setenv("CENSUS_SURVEY_API_KEY", "test_key_123")
    
    settings = Settings()
    
    # Should return the key without error
    key = settings.require_census_api_key()
    assert key == "test_key_123"


@pytest.mark.unit
def test_config_defaults(clean_env, monkeypatch):
    """Test default values for optional settings."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    
    settings = Settings()
    
    assert settings.max_concurrency == 4
    assert settings.max_requests_per_second == 5.0
    assert settings.log_level == "INFO"
    assert settings.run_integration_tests is False
    assert settings.max_retries == 3
    assert settings.retry_backoff_factor == 2.0


@pytest.mark.unit
def test_config_custom_values(clean_env, monkeypatch):
    """Test setting custom configuration values."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://custom")
    monkeypatch.setenv("MAX_CONCURRENCY", "8")
    monkeypatch.setenv("MAX_REQUESTS_PER_SECOND", "10.0")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("RUN_INTEGRATION_TESTS", "true")
    
    settings = Settings()
    
    assert settings.database_url == "postgresql://custom"
    assert settings.max_concurrency == 8
    assert settings.max_requests_per_second == 10.0
    assert settings.log_level == "DEBUG"
    assert settings.run_integration_tests is True


@pytest.mark.unit
def test_config_log_level_validation(clean_env, monkeypatch):
    """Test log level validation."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    monkeypatch.setenv("LOG_LEVEL", "INVALID")
    
    with pytest.raises(ValueError) as exc_info:
        Settings()
    
    assert "log_level must be one of" in str(exc_info.value)


@pytest.mark.unit
def test_config_concurrency_bounds(clean_env, monkeypatch):
    """Test concurrency value bounds."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    
    # Too low
    monkeypatch.setenv("MAX_CONCURRENCY", "0")
    with pytest.raises(Exception):  # Pydantic validation error
        Settings()
    
    # Too high
    monkeypatch.setenv("MAX_CONCURRENCY", "100")
    with pytest.raises(Exception):  # Pydantic validation error
        Settings()
    
    # Valid range
    monkeypatch.setenv("MAX_CONCURRENCY", "10")
    settings = Settings()
    assert settings.max_concurrency == 10


@pytest.mark.unit
def test_get_settings_singleton(clean_env, monkeypatch):
    """Test that get_settings returns the same instance."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    
    settings1 = get_settings()
    settings2 = get_settings()
    
    assert settings1 is settings2


@pytest.mark.unit
def test_reset_settings(clean_env, monkeypatch):
    """Test that reset_settings creates a new instance."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    
    settings1 = get_settings()
    reset_settings()
    settings2 = get_settings()
    
    assert settings1 is not settings2





