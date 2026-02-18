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
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # Database (REQUIRED for API startup)
    database_url: str = Field(..., description="PostgreSQL connection URL")

    # Census API Configuration (OPTIONAL for startup, REQUIRED for ingestion)
    census_survey_api_key: Optional[str] = Field(
        default=None,
        description="Census API key - required only for actual Census operations",
    )

    # BLS API Configuration (OPTIONAL for startup, RECOMMENDED for ingestion)
    bls_api_key: Optional[str] = Field(
        default=None,
        description="BLS API key - optional but recommended for higher rate limits",
    )

    # FRED API Configuration (OPTIONAL for startup, RECOMMENDED for ingestion)
    fred_api_key: Optional[str] = Field(
        default=None,
        description="FRED API key - optional but recommended for higher rate limits",
    )

    # EIA API Configuration (OPTIONAL for startup, REQUIRED for ingestion)
    eia_api_key: Optional[str] = Field(
        default=None, description="EIA API key - required for EIA operations"
    )

    # NOAA API Configuration (OPTIONAL for startup, REQUIRED for ingestion)
    noaa_api_token: Optional[str] = Field(
        default=None, description="NOAA API token - required for NOAA operations"
    )

    # Kaggle API Configuration (OPTIONAL for startup, REQUIRED for Kaggle ingestion)
    kaggle_username: Optional[str] = Field(
        default=None,
        description="Kaggle username - required for Kaggle dataset downloads",
    )
    kaggle_key: Optional[str] = Field(
        default=None,
        description="Kaggle API key - required for Kaggle dataset downloads",
    )
    kaggle_data_dir: str = Field(
        default="./data/kaggle",
        description="Local directory for storing downloaded Kaggle datasets",
    )

    # Data.gov API Key (used for FBI Crime, and other data.gov APIs)
    data_gov_api: Optional[str] = Field(
        default=None,
        description="Data.gov API key - used for FBI Crime Data and other government APIs",
    )

    # BTS API Configuration (OPTIONAL - public data, higher limits with app token)
    bts_app_token: Optional[str] = Field(
        default=None,
        description="BTS Socrata app token - optional but recommended for higher rate limits",
    )

    # BEA API Configuration (REQUIRED for BEA ingestion)
    bea_api_key: Optional[str] = Field(
        default=None, description="BEA API key (UserID) - required for BEA operations"
    )

    # Data Commons API Configuration (OPTIONAL - higher rate limits with key)
    data_commons_api_key: Optional[str] = Field(
        default=None,
        description="Google Data Commons API key - optional but recommended for higher rate limits",
    )

    # Yelp Fusion API Configuration (REQUIRED for Yelp ingestion)
    yelp_api_key: Optional[str] = Field(
        default=None,
        description="Yelp Fusion API key - required for Yelp operations (500 calls/day free tier)",
    )

    # Rate Limiting and Concurrency
    max_concurrency: int = Field(
        default=4,
        ge=1,
        le=50,
        description="Maximum concurrent requests to external APIs",
    )

    max_requests_per_second: float = Field(
        default=5.0,
        ge=0.1,
        le=100.0,
        description="Maximum requests per second (for rate limiting)",
    )

    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )

    # Testing
    run_integration_tests: bool = Field(
        default=False,
        description="Enable integration tests (requires API keys and network)",
    )

    # Retry Configuration
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum number of retries for failed API requests",
    )

    retry_backoff_factor: float = Field(
        default=2.0,
        ge=1.0,
        le=10.0,
        description="Exponential backoff factor for retries",
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

    def require_kaggle_credentials(self) -> tuple[str, str]:
        """
        Get Kaggle credentials, raising clear error if missing.

        Call this at the START of any Kaggle ingestion operation.

        Kaggle credentials can be configured via:
        1. Environment variables: KAGGLE_USERNAME, KAGGLE_KEY
        2. .env file with kaggle_username, kaggle_key
        3. ~/.kaggle/kaggle.json file (standard Kaggle CLI method)

        Get credentials at: https://www.kaggle.com/account (API section)

        Raises:
            ValueError: If credentials are not configured

        Returns:
            tuple[str, str]: (username, key) tuple
        """
        if not self.kaggle_username or not self.kaggle_key:
            raise ValueError(
                "KAGGLE_USERNAME and KAGGLE_KEY are required for Kaggle operations. "
                "Please set them in your .env file or environment variables. "
                "Get your API credentials at: https://www.kaggle.com/account (API section). "
                "Alternatively, configure ~/.kaggle/kaggle.json as per Kaggle CLI docs."
            )
        return self.kaggle_username, self.kaggle_key

    def get_kaggle_data_dir(self) -> str:
        """
        Get the directory for storing downloaded Kaggle datasets.

        Returns:
            str: Path to Kaggle data directory
        """
        return self.kaggle_data_dir

    def get_fbi_crime_api_key(self) -> Optional[str]:
        """
        Get FBI Crime Data Explorer API key if configured.

        Uses the data.gov API key which works for FBI Crime Data.

        Get a free key at: https://api.data.gov/signup/

        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.data_gov_api

    def require_fbi_crime_api_key(self) -> str:
        """
        Get FBI Crime API key, raising clear error if missing.

        Call this at the START of any FBI Crime ingestion operation.

        Raises:
            ValueError: If the key is not configured

        Returns:
            str: The API key
        """
        if not self.data_gov_api:
            raise ValueError(
                "DATA_GOV_API is required for FBI Crime Data operations. "
                "Please set it in your .env file or environment variables. "
                "Get a free key at: https://api.data.gov/signup/"
            )
        return self.data_gov_api

    def get_bts_app_token(self) -> Optional[str]:
        """
        Get BTS Socrata app token if configured.

        BTS app token is optional for public data but recommended for higher rate limits:
        - Without token: ~1,000 requests/hour
        - With token: ~4,000+ requests/hour

        No sign-up required for public data. Get app token at:
        https://data.transportation.gov/profile/edit/developer_settings

        Returns:
            Optional[str]: The app token if configured, None otherwise
        """
        return self.bts_app_token

    def get_bea_api_key(self) -> Optional[str]:
        """
        Get BEA API key (UserID) if configured.

        BEA API key is required for all BEA operations.
        Rate limits: 100 requests/minute, 100 MB/minute

        Get a free key at: https://apps.bea.gov/api/signup/

        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.bea_api_key

    def require_bea_api_key(self) -> str:
        """
        Get BEA API key, raising clear error if missing.

        Call this at the START of any BEA ingestion operation.

        Raises:
            ValueError: If the key is not configured

        Returns:
            str: The API key
        """
        if not self.bea_api_key:
            raise ValueError(
                "BEA_API_KEY is required for BEA operations. "
                "Please set it in your .env file or environment variables. "
                "Get a free key at: https://apps.bea.gov/api/signup/"
            )
        return self.bea_api_key

    def get_data_commons_api_key(self) -> Optional[str]:
        """
        Get Data Commons API key if configured.

        Data Commons API key is optional but recommended for higher rate limits.
        Without key: Works but may be throttled
        With key: Higher rate limits

        Enable "Data Commons API" in Google Cloud Console:
        https://console.cloud.google.com/apis/credentials

        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.data_commons_api_key

    def get_yelp_api_key(self) -> Optional[str]:
        """
        Get Yelp Fusion API key if configured.

        Yelp API key is required for all Yelp operations.
        Rate limits: 500 calls/day for free tier (new clients)

        Get a free key at: https://www.yelp.com/developers/v3/manage_app

        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.yelp_api_key

    def require_yelp_api_key(self) -> str:
        """
        Get Yelp API key, raising clear error if missing.

        Call this at the START of any Yelp ingestion operation.

        Raises:
            ValueError: If the key is not configured

        Returns:
            str: The API key
        """
        if not self.yelp_api_key:
            raise ValueError(
                "YELP_API_KEY is required for Yelp operations. "
                "Please set it in your .env file or environment variables. "
                "Get a free key at: https://www.yelp.com/developers/v3/manage_app"
            )
        return self.yelp_api_key

    # ==========================================================================
    # AGENTIC PORTFOLIO RESEARCH SETTINGS
    # ==========================================================================

    # LLM API Keys (for news extraction strategy - Phase 3)
    openai_api_key: Optional[str] = Field(
        default=None, description="OpenAI API key for LLM-powered entity extraction"
    )
    anthropic_api_key: Optional[str] = Field(
        default=None, description="Anthropic API key (alternative to OpenAI)"
    )

    # ==========================================================================
    # FOOT TRAFFIC & LOCATION INTELLIGENCE SETTINGS
    # ==========================================================================

    # SafeGraph API (Recommended - best ROI for foot traffic data)
    safegraph_api_key: Optional[str] = Field(
        default=None, description="SafeGraph API key for foot traffic patterns data"
    )

    # Placer.ai API (Optional - enterprise pricing)
    placer_api_key: Optional[str] = Field(
        default=None, description="Placer.ai API key for retail analytics"
    )

    # Foursquare Places API (Recommended for POI enrichment)
    foursquare_api_key: Optional[str] = Field(
        default=None, description="Foursquare Places API key for POI data"
    )

    # USPTO PatentsView API (Required for patent data)
    uspto_patentsview_api_key: Optional[str] = Field(
        default=None, description="USPTO PatentsView API key for patent data"
    )

    # ==========================================================================
    # KEYS ALREADY USED IN CODEBASE (not previously in _API_KEY_MAP)
    # ==========================================================================

    usda_api_key: Optional[str] = Field(
        default=None, description="USDA API key for agricultural data"
    )
    github_token: Optional[str] = Field(
        default=None, description="GitHub personal access token for higher rate limits"
    )
    google_api_key: Optional[str] = Field(
        default=None, description="Google API key for Custom Search"
    )
    google_cse_id: Optional[str] = Field(
        default=None, description="Google Custom Search Engine ID"
    )
    similarweb_api_key: Optional[str] = Field(
        default=None, description="SimilarWeb API key for web traffic analytics"
    )
    opencorporates_api_key: Optional[str] = Field(
        default=None, description="OpenCorporates API key for company registry data"
    )
    peeringdb_api_key: Optional[str] = Field(
        default=None, description="PeeringDB API key for network/peering data"
    )
    crunchbase_api_key: Optional[str] = Field(
        default=None, description="Crunchbase API key for startup/VC data"
    )
    newsapi_key: Optional[str] = Field(
        default=None, description="NewsAPI key for news article search"
    )
    linkedin_api_key: Optional[str] = Field(
        default=None, description="LinkedIn API key for professional network data"
    )

    # ==========================================================================
    # AI / LLM PROVIDER KEYS
    # ==========================================================================

    gemini_api_key: Optional[str] = Field(
        default=None, description="Google Gemini API key"
    )
    xai_api_key: Optional[str] = Field(default=None, description="xAI / Grok API key")
    deepseek_api_key: Optional[str] = Field(
        default=None, description="DeepSeek API key for reasoning models"
    )
    groq_api_key: Optional[str] = Field(
        default=None, description="Groq API key for ultra-fast inference"
    )
    mistral_api_key: Optional[str] = Field(default=None, description="Mistral API key")
    cohere_api_key: Optional[str] = Field(
        default=None, description="Cohere API key for LLM and embeddings"
    )
    perplexity_api_key: Optional[str] = Field(
        default=None, description="Perplexity API key for search-augmented LLM"
    )

    # ==========================================================================
    # FINANCIAL / MARKET DATA KEYS
    # ==========================================================================

    fmp_api_key: Optional[str] = Field(
        default=None, description="Financial Modeling Prep API key"
    )
    alpha_vantage_api_key: Optional[str] = Field(
        default=None, description="Alpha Vantage API key for stock/forex/crypto data"
    )
    polygon_api_key: Optional[str] = Field(
        default=None, description="Polygon.io API key for market data"
    )
    finnhub_api_key: Optional[str] = Field(
        default=None, description="Finnhub API key for stock data and news"
    )
    tiingo_api_key: Optional[str] = Field(
        default=None, description="Tiingo API key for EOD prices and news"
    )
    quandl_api_key: Optional[str] = Field(
        default=None, description="Quandl/Nasdaq Data Link API key"
    )

    # ==========================================================================
    # BUSINESS / ENRICHMENT KEYS
    # ==========================================================================

    hunter_api_key: Optional[str] = Field(
        default=None, description="Hunter.io API key for email finding"
    )
    clearbit_api_key: Optional[str] = Field(
        default=None, description="Clearbit API key for company/person enrichment"
    )
    zoominfo_api_key: Optional[str] = Field(
        default=None, description="ZoomInfo API key for B2B contact data"
    )
    pitchbook_api_key: Optional[str] = Field(
        default=None, description="Pitchbook API key for PE/VC deal data"
    )

    # Google Popular Times scraping (Free but ToS risk)
    foot_traffic_enable_google_scraping: bool = Field(
        default=False,
        description="Enable Google Popular Times scraping (use with caution - ToS risk)",
    )
    foot_traffic_requests_per_day_google: int = Field(
        default=100,
        ge=1,
        le=500,
        description="Maximum Google Popular Times requests per day",
    )

    # Foot Traffic Rate Limiting
    foot_traffic_requests_per_second: float = Field(
        default=0.2,
        ge=0.05,
        le=2.0,
        description="Default rate limit for foot traffic APIs (1 per 5 seconds = 0.2)",
    )

    # Agent Behavior
    agentic_max_strategies_per_job: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Maximum number of strategies to try per collection job",
    )
    agentic_max_requests_per_strategy: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum HTTP requests per strategy execution",
    )
    agentic_timeout_per_strategy: int = Field(
        default=300,
        ge=30,
        le=900,
        description="Timeout in seconds per strategy (5 minutes default)",
    )

    # Agentic Rate Limiting (per domain)
    agentic_requests_per_second: float = Field(
        default=0.5,
        ge=0.1,
        le=10.0,
        description="Default rate limit for agentic web scraping (1 req per 2 seconds)",
    )
    agentic_max_concurrent: int = Field(
        default=3, ge=1, le=10, description="Maximum concurrent agentic requests"
    )

    # LLM Settings (for Phase 3 news strategy)
    agentic_llm_model: str = Field(
        default="gpt-4o-mini", description="LLM model for entity extraction"
    )
    agentic_llm_max_tokens: int = Field(
        default=500, ge=100, le=4000, description="Maximum tokens per LLM request"
    )

    def get_openai_api_key(self) -> Optional[str]:
        """
        Get OpenAI API key if configured.

        Used for LLM-powered entity extraction in news strategy.

        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.openai_api_key

    def get_anthropic_api_key(self) -> Optional[str]:
        """
        Get Anthropic API key if configured.

        Alternative to OpenAI for LLM-powered extraction.

        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.anthropic_api_key

    # ==========================================================================
    # FOOT TRAFFIC API KEY METHODS
    # ==========================================================================

    def get_safegraph_api_key(self) -> Optional[str]:
        """
        Get SafeGraph API key if configured.

        SafeGraph provides weekly foot traffic patterns from mobile location data.
        Cost: $100-500/month depending on tier.

        Get access at: https://www.safegraph.com/

        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.safegraph_api_key

    def require_safegraph_api_key(self) -> str:
        """
        Get SafeGraph API key, raising clear error if missing.

        Raises:
            ValueError: If the key is not configured

        Returns:
            str: The API key
        """
        if not self.safegraph_api_key:
            raise ValueError(
                "SAFEGRAPH_API_KEY is required for SafeGraph foot traffic operations. "
                "Please set it in your .env file or environment variables. "
                "Get access at: https://www.safegraph.com/"
            )
        return self.safegraph_api_key

    def get_placer_api_key(self) -> Optional[str]:
        """
        Get Placer.ai API key if configured.

        Placer.ai provides retail analytics and competitive benchmarking.
        Cost: $500-2,000+/month (enterprise pricing).

        Get access at: https://www.placer.ai/

        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.placer_api_key

    def get_foursquare_api_key(self) -> Optional[str]:
        """
        Get Foursquare Places API key if configured.

        Foursquare provides POI data including addresses, hours, categories.
        Free tier available with $0.01-0.05 per API call for premium.

        Get access at: https://developer.foursquare.com/

        Returns:
            Optional[str]: The API key if configured, None otherwise
        """
        return self.foursquare_api_key

    def require_foursquare_api_key(self) -> str:
        """
        Get Foursquare API key, raising clear error if missing.

        Raises:
            ValueError: If the key is not configured

        Returns:
            str: The API key
        """
        if not self.foursquare_api_key:
            raise ValueError(
                "FOURSQUARE_API_KEY is required for Foursquare POI operations. "
                "Please set it in your .env file or environment variables. "
                "Get access at: https://developer.foursquare.com/"
            )
        return self.foursquare_api_key

    def is_google_scraping_enabled(self) -> bool:
        """
        Check if Google Popular Times scraping is enabled.

        WARNING: Scraping Google Maps may violate their ToS.
        Use with caution and conservative rate limiting.

        Returns:
            bool: True if enabled, False otherwise
        """
        return self.foot_traffic_enable_google_scraping

    # ==========================================================================
    # UNIFIED API KEY ACCESS
    # ==========================================================================

    # Mapping of source names to (field_name, signup_url)
    _API_KEY_MAP = {
        # Government Data (11)
        "census": (
            "census_survey_api_key",
            "https://api.census.gov/data/key_signup.html",
        ),
        "fred": ("fred_api_key", "https://fred.stlouisfed.org/docs/api/api_key.html"),
        "eia": ("eia_api_key", "https://www.eia.gov/opendata/register.php"),
        "bls": ("bls_api_key", "https://data.bls.gov/registrationEngine/"),
        "noaa": ("noaa_api_token", "https://www.ncdc.noaa.gov/cdo-web/token"),
        "bea": ("bea_api_key", "https://apps.bea.gov/api/signup/"),
        "bts": (
            "bts_app_token",
            "https://data.transportation.gov/profile/edit/developer_settings",
        ),
        "fbi_crime": ("data_gov_api", "https://api.data.gov/signup/"),
        "data_commons": (
            "data_commons_api_key",
            "https://console.cloud.google.com/apis/credentials",
        ),
        "usda": ("usda_api_key", "https://api.data.gov/signup/"),
        "uspto": (
            "uspto_patentsview_api_key",
            "https://patentsview-support.atlassian.net/servicedesk/customer/portal/1",
        ),
        # LLM / AI (9)
        "openai": ("openai_api_key", "https://platform.openai.com/api-keys"),
        "anthropic": ("anthropic_api_key", "https://console.anthropic.com/"),
        "gemini": ("gemini_api_key", "https://aistudio.google.com/app/apikey"),
        "xai": ("xai_api_key", "https://console.x.ai/"),
        "deepseek": ("deepseek_api_key", "https://platform.deepseek.com/api_keys"),
        "groq": ("groq_api_key", "https://console.groq.com/keys"),
        "mistral": ("mistral_api_key", "https://console.mistral.ai/api-keys"),
        "cohere": ("cohere_api_key", "https://dashboard.cohere.com/api-keys"),
        "perplexity": ("perplexity_api_key", "https://www.perplexity.ai/settings/api"),
        # Financial & Market Data (6)
        "fmp": ("fmp_api_key", "https://site.financialmodelingprep.com/developer/docs"),
        "alpha_vantage": (
            "alpha_vantage_api_key",
            "https://www.alphavantage.co/support/#api-key",
        ),
        "polygon": ("polygon_api_key", "https://polygon.io/dashboard/signup"),
        "finnhub": ("finnhub_api_key", "https://finnhub.io/register"),
        "tiingo": ("tiingo_api_key", "https://www.tiingo.com/account/api/token"),
        "quandl": ("quandl_api_key", "https://data.nasdaq.com/sign-up"),
        # Location & Foot Traffic (5)
        "safegraph": ("safegraph_api_key", "https://www.safegraph.com/"),
        "placer": ("placer_api_key", "https://www.placer.ai/"),
        "foursquare": ("foursquare_api_key", "https://developer.foursquare.com/"),
        "yelp": ("yelp_api_key", "https://www.yelp.com/developers/v3/manage_app"),
        "peeringdb": ("peeringdb_api_key", "https://www.peeringdb.com/register"),
        # Search & Web Data (4)
        "google_search": (
            "google_api_key",
            "https://console.cloud.google.com/apis/credentials",
        ),
        "google_cse": ("google_cse_id", "https://programmablesearchengine.google.com/"),
        "similarweb": (
            "similarweb_api_key",
            "https://www.similarweb.com/corp/developer/",
        ),
        "github": ("github_token", "https://github.com/settings/tokens"),
        # Business Intelligence (6)
        "crunchbase": ("crunchbase_api_key", "https://www.crunchbase.com/home"),
        "newsapi": ("newsapi_key", "https://newsapi.org/register"),
        "linkedin": ("linkedin_api_key", "https://www.linkedin.com/developers/"),
        "opencorporates": (
            "opencorporates_api_key",
            "https://opencorporates.com/api_accounts/new",
        ),
        "kaggle_username": ("kaggle_username", "https://www.kaggle.com/account"),
        "kaggle_key": ("kaggle_key", "https://www.kaggle.com/account"),
        # Enrichment (4)
        "hunter": ("hunter_api_key", "https://hunter.io/users/sign_up"),
        "clearbit": ("clearbit_api_key", "https://clearbit.com/"),
        "zoominfo": ("zoominfo_api_key", "https://www.zoominfo.com/"),
        "pitchbook": ("pitchbook_api_key", "https://pitchbook.com/"),
    }

    def get_api_key(self, source: str, required: bool = False) -> Optional[str]:
        """
        Unified method to get API key for any source.

        Checks DB (source_api_keys table) first, falls back to env var.

        Args:
            source: Source name (e.g., 'fred', 'eia', 'census')
            required: If True, raises ValueError when key is missing

        Returns:
            API key string or None if not configured

        Raises:
            ValueError: If required=True and key is not configured
            KeyError: If source is not recognized

        Example:
            settings = get_settings()
            fred_key = settings.get_api_key("fred")  # Optional
            eia_key = settings.get_api_key("eia", required=True)  # Required
        """
        source_lower = source.lower()

        if source_lower not in self._API_KEY_MAP:
            available = ", ".join(sorted(self._API_KEY_MAP.keys()))
            raise KeyError(
                f"Unknown API source: {source}. " f"Available sources: {available}"
            )

        # Check DB first
        db_key = self._get_api_key_from_db(source_lower)
        if db_key:
            return db_key

        # Fall back to env var
        field_name, signup_url = self._API_KEY_MAP[source_lower]
        key_value = getattr(self, field_name, None)

        if required and not key_value:
            raise ValueError(
                f"{field_name.upper()} is required for {source} operations. "
                f"Please set it in your .env file or environment variables. "
                f"Get a key at: {signup_url}"
            )

        return key_value

    def _get_api_key_from_db(self, source: str) -> Optional[str]:
        """
        Try to load an API key from the source_api_keys table.
        Uses the settings module's cache to avoid repeated DB hits.
        Returns None on any failure (missing table, decryption error, etc.).
        """
        try:
            from app.api.v1.settings import get_cached_db_key
            from app.core.database import get_session_factory

            SessionLocal = get_session_factory()
            db = SessionLocal()
            try:
                return get_cached_db_key(source, db)
            finally:
                db.close()
        except Exception:
            return None

    def has_api_key(self, source: str) -> bool:
        """
        Check if an API key is configured for a source.

        Args:
            source: Source name

        Returns:
            True if key is configured, False otherwise
        """
        try:
            return self.get_api_key(source) is not None
        except KeyError:
            return False


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
