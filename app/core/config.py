"""
Configuration module with strict validation.

Key principles:
- APP STARTUP does NOT require CENSUS_SURVEY_API_KEY
- Real Census ingestion DOES require the key (fails early with clear error)
- All rate limits and concurrency settings are configurable
- Safe defaults for all optional settings
"""
import os
from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class MissingCensusAPIKeyError(Exception):
    """Raised when Census ingestion is requested without an API key."""
    pass


class Settings(BaseSettings):
    """Application settings with validation.
    
    Loads from environment variables and .env file.
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Database (REQUIRED for API startup)
    database_url: str = Field(
        ...,
        description="PostgreSQL connection URL"
    )
    
    # Census API Configuration (OPTIONAL for startup, REQUIRED for ingestion)
    census_survey_api_key: Optional[str] = Field(
        default=None,
        description="Census API key - required only for actual Census operations"
    )
    
    # BLS API Configuration (OPTIONAL for startup, RECOMMENDED for ingestion)
    bls_api_key: Optional[str] = Field(
        default=None,
        description="BLS API key - optional but recommended for higher rate limits"
    )
    
    # FRED API Configuration (OPTIONAL for startup, RECOMMENDED for ingestion)
    fred_api_key: Optional[str] = Field(
        default=None,
        description="FRED API key - optional but recommended for higher rate limits"
    )
    
    # EIA API Configuration (OPTIONAL for startup, REQUIRED for ingestion)
    eia_api_key: Optional[str] = Field(
        default=None,
        description="EIA API key - required for EIA operations"
    )
    
    # NOAA API Configuration (OPTIONAL for startup, REQUIRED for ingestion)
    noaa_api_token: Optional[str] = Field(
        default=None,
        description="NOAA API token - required for NOAA operations"
    )
    
    # Rate Limiting and Concurrency
    max_concurrency: int = Field(
        default=4,
        ge=1,
        le=50,
        description="Maximum concurrent requests to external APIs"
    )
    
    max_requests_per_second: float = Field(
        default=5.0,
        ge=0.1,
        le=100.0,
        description="Maximum requests per second (for rate limiting)"
    )
    
    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    
    # Testing
    run_integration_tests: bool = Field(
        default=False,
        description="Enable integration tests (requires API keys and network)"
    )
    
    # Retry Configuration
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum number of retries for failed API requests"
    )
    
    retry_backoff_factor: float = Field(
        default=2.0,
        ge=1.0,
        le=10.0,
        description="Exponential backoff factor for retries"
    )
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is one of the standard levels."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v_upper
    
    def require_census_api_key(self) -> str:
        """
        Get Census API key, raising clear error if missing.
        
        Call this at the START of any Census ingestion operation.
        
        Raises:
            MissingCensusAPIKeyError: If the key is not configured
            
        Returns:
            str: The API key
        """
        if not self.census_survey_api_key:
            raise MissingCensusAPIKeyError(
                "CENSUS_SURVEY_API_KEY is required for Census ingestion operations. "
                "Please set it in your .env file or environment variables. "
                "Get a key at: https://api.census.gov/data/key_signup.html"
            )
        return self.census_survey_api_key
    
    def get_bls_api_key(self) -> Optional[str]:
        """
        Get BLS API key if configured.
        
        BLS API key is optional but recommended for better rate limits:
        - Without key: 25 queries per day, 10 years per query
        - With key: 500 queries per day, 20 years per query
        
        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.bls_api_key
    
    def get_fred_api_key(self) -> Optional[str]:
        """
        Get FRED API key if configured.
        
        FRED API key is optional but recommended for better rate limits:
        - Without key: Limited access, throttled
        - With key: 120 requests per minute per IP
        
        Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html
        
        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.fred_api_key
    
    def require_eia_api_key(self) -> str:
        """
        Get EIA API key, raising clear error if missing.
        
        Call this at the START of any EIA ingestion operation.
        
        Raises:
            ValueError: If the key is not configured
            
        Returns:
            str: The API key
        """
        if not self.eia_api_key:
            raise ValueError(
                "EIA_API_KEY is required for EIA ingestion operations. "
                "Please set it in your .env file or environment variables. "
                "Get a free key at: https://www.eia.gov/opendata/register.php"
            )
        return self.eia_api_key
    
    def get_eia_api_key(self) -> Optional[str]:
        """
        Get EIA API key if configured.
        
        EIA API key is required for all EIA operations.
        
        Get a free key at: https://www.eia.gov/opendata/register.php
        
        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.eia_api_key


# Global settings instance
# This can be imported throughout the application
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Get or create the global settings instance.
    
    This pattern allows:
    - Easy testing (can reset settings between tests)
    - Lazy loading (only loads when first accessed)
    - Singleton pattern (same instance used everywhere)
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """
    Reset the global settings instance.
    
    Useful for testing to ensure clean state between tests.
    """
    global _settings
    _settings = None


