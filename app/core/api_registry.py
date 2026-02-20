"""
Centralized API configuration registry.

Consolidates all API-specific settings in one place:
- Base URLs
- Rate limits
- Required vs optional keys
- Default concurrency settings

This eliminates magic strings scattered across client files.
"""

from dataclasses import dataclass
from typing import Optional, Dict
from enum import Enum


class APIKeyRequirement(Enum):
    """Whether an API key is required, recommended, or not needed."""

    REQUIRED = "required"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"


@dataclass
class APIConfig:
    """Configuration for a single external API."""

    source_name: str
    base_url: str
    api_key_requirement: APIKeyRequirement
    config_key: str  # Key name in Settings (e.g., "fred_api_key")
    signup_url: str

    # Rate limiting
    max_concurrency: int = 2
    rate_limit_per_minute: Optional[int] = None  # None = no specific limit
    rate_limit_interval: Optional[float] = None  # Seconds between requests

    # Request settings
    timeout_seconds: float = 30.0
    connect_timeout_seconds: float = 10.0
    max_retries: int = 3

    # API-specific notes
    notes: Optional[str] = None

    def get_rate_limit_interval(self) -> Optional[float]:
        """Calculate rate limit interval from per-minute limit."""
        if self.rate_limit_interval is not None:
            return self.rate_limit_interval
        if self.rate_limit_per_minute is not None:
            return 60.0 / self.rate_limit_per_minute
        return None


# =============================================================================
# API REGISTRY - All external API configurations
# =============================================================================

API_REGISTRY: Dict[str, APIConfig] = {
    # -------------------------------------------------------------------------
    # ECONOMIC DATA
    # -------------------------------------------------------------------------
    "fred": APIConfig(
        source_name="fred",
        base_url="https://api.stlouisfed.org/fred",
        api_key_requirement=APIKeyRequirement.RECOMMENDED,
        config_key="fred_api_key",
        signup_url="https://fred.stlouisfed.org/docs/api/api_key.html",
        max_concurrency=2,
        rate_limit_per_minute=120,
        notes="With key: 120 req/min. Without: limited and throttled.",
    ),
    "bea": APIConfig(
        source_name="bea",
        base_url="https://apps.bea.gov/api/data",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="bea_api_key",
        signup_url="https://apps.bea.gov/api/signup/",
        max_concurrency=2,
        rate_limit_per_minute=100,
        notes="100 req/min, 100 MB/min limit",
    ),
    "bls": APIConfig(
        source_name="bls",
        base_url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
        api_key_requirement=APIKeyRequirement.RECOMMENDED,
        config_key="bls_api_key",
        signup_url="https://data.bls.gov/registrationEngine/",
        max_concurrency=2,
        rate_limit_interval=0.5,  # 2 req/sec to be safe
        notes="Without key: 25 queries/day, 10 years. With key: 500/day, 20 years.",
    ),
    # -------------------------------------------------------------------------
    # ENERGY DATA
    # -------------------------------------------------------------------------
    "eia": APIConfig(
        source_name="eia",
        base_url="https://api.eia.gov/v2",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="eia_api_key",
        signup_url="https://www.eia.gov/opendata/register.php",
        max_concurrency=2,
        rate_limit_per_minute=83,  # 5000/hour = ~83/min
        timeout_seconds=60.0,
        notes="5,000 requests/hour with API key",
    ),
    # -------------------------------------------------------------------------
    # CENSUS & DEMOGRAPHICS
    # -------------------------------------------------------------------------
    "census": APIConfig(
        source_name="census",
        base_url="https://api.census.gov/data",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="census_survey_api_key",
        signup_url="https://api.census.gov/data/key_signup.html",
        max_concurrency=4,
        rate_limit_interval=0.2,  # 5 req/sec
        notes="Required for production use",
    ),
    # -------------------------------------------------------------------------
    # WEATHER
    # -------------------------------------------------------------------------
    "noaa": APIConfig(
        source_name="noaa",
        base_url="https://www.ncdc.noaa.gov/cdo-web/api/v2",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="noaa_api_token",
        signup_url="https://www.ncdc.noaa.gov/cdo-web/token",
        max_concurrency=2,
        rate_limit_per_minute=5,  # Very low limit
        timeout_seconds=60.0,
        notes="5 requests/second, 10,000/day limit",
    ),
    # -------------------------------------------------------------------------
    # SEC / FINANCIAL
    # -------------------------------------------------------------------------
    "sec": APIConfig(
        source_name="sec",
        base_url="https://data.sec.gov",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",  # No key required
        signup_url="",
        max_concurrency=2,
        rate_limit_per_minute=10,  # SEC is strict
        timeout_seconds=60.0,
        notes="No key required. 10 req/sec max. Must set User-Agent.",
    ),
    # -------------------------------------------------------------------------
    # TRANSPORTATION
    # -------------------------------------------------------------------------
    "bts": APIConfig(
        source_name="bts",
        base_url="https://data.transportation.gov/resource",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="bts_app_token",
        signup_url="https://data.transportation.gov/profile/edit/developer_settings",
        max_concurrency=2,
        rate_limit_per_minute=60,
        notes="Without token: ~1000/hr. With token: ~4000/hr.",
    ),
    # -------------------------------------------------------------------------
    # CRIME DATA
    # -------------------------------------------------------------------------
    "fbi_crime": APIConfig(
        source_name="fbi_crime",
        base_url="https://api.usa.gov/crime/fbi/sapi",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="data_gov_api",
        signup_url="https://api.data.gov/signup/",
        max_concurrency=2,
        rate_limit_per_minute=60,
        notes="Uses data.gov API key",
    ),
    # -------------------------------------------------------------------------
    # DISASTER / FEMA
    # -------------------------------------------------------------------------
    "fema": APIConfig(
        source_name="fema",
        base_url="https://www.fema.gov/api/open/v2",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",
        signup_url="",
        max_concurrency=2,
        rate_limit_per_minute=60,
        timeout_seconds=60.0,
        notes="No key required. OpenFEMA API.",
    ),
    # -------------------------------------------------------------------------
    # BANKING / FDIC
    # -------------------------------------------------------------------------
    "fdic": APIConfig(
        source_name="fdic",
        base_url="https://banks.data.fdic.gov/api",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",
        signup_url="",
        max_concurrency=2,
        rate_limit_per_minute=60,
        notes="No key required. BankFind API.",
    ),
    # -------------------------------------------------------------------------
    # TREASURY
    # -------------------------------------------------------------------------
    "treasury": APIConfig(
        source_name="treasury",
        base_url="https://api.fiscaldata.treasury.gov/services/api",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",
        signup_url="",
        max_concurrency=2,
        rate_limit_per_minute=60,
        notes="No key required. FiscalData API.",
    ),
    # -------------------------------------------------------------------------
    # CFTC
    # -------------------------------------------------------------------------
    "cftc_cot": APIConfig(
        source_name="cftc_cot",
        base_url="https://publicreporting.cftc.gov/api/views",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",
        signup_url="",
        max_concurrency=2,
        rate_limit_per_minute=30,
        notes="Socrata-based API. No key required but rate limited.",
    ),
    # -------------------------------------------------------------------------
    # INTERNATIONAL
    # -------------------------------------------------------------------------
    "worldbank": APIConfig(
        source_name="worldbank",
        base_url="https://api.worldbank.org/v2",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",
        signup_url="",
        max_concurrency=2,
        rate_limit_per_minute=60,
        notes="No key required. Returns XML by default, add format=json.",
    ),
    "oecd": APIConfig(
        source_name="oecd",
        base_url="https://sdmx.oecd.org/public/rest",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",
        signup_url="",
        max_concurrency=2,
        rate_limit_per_minute=30,
        timeout_seconds=120.0,  # OECD can be slow
        notes="SDMX format API. No key required.",
    ),
    # -------------------------------------------------------------------------
    # AGRICULTURE
    # -------------------------------------------------------------------------
    "usda": APIConfig(
        source_name="usda",
        base_url="https://quickstats.nass.usda.gov/api",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="usda_api_key",
        signup_url="https://quickstats.nass.usda.gov/api",
        max_concurrency=2,
        rate_limit_per_minute=30,
        notes="NASS QuickStats API",
    ),
    # -------------------------------------------------------------------------
    # BROADBAND / FCC
    # -------------------------------------------------------------------------
    "fcc_broadband": APIConfig(
        source_name="fcc_broadband",
        base_url="https://broadbandmap.fcc.gov/api/public",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",
        signup_url="",
        max_concurrency=2,
        rate_limit_per_minute=60,
        notes="No key required. FCC Broadband Map API.",
    ),
    # -------------------------------------------------------------------------
    # TAX / IRS
    # -------------------------------------------------------------------------
    "irs_soi": APIConfig(
        source_name="irs_soi",
        base_url="https://www.irs.gov/statistics",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",
        signup_url="",
        max_concurrency=2,
        rate_limit_per_minute=10,
        notes="File downloads, not true API. Rate limit accordingly.",
    ),
    # -------------------------------------------------------------------------
    # DATA AGGREGATORS
    # -------------------------------------------------------------------------
    "data_commons": APIConfig(
        source_name="data_commons",
        base_url="https://api.datacommons.org",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="data_commons_api_key",
        signup_url="https://console.cloud.google.com/apis/credentials",
        max_concurrency=2,
        rate_limit_per_minute=60,
        notes="Google Data Commons. Optional key for higher limits.",
    ),
    # -------------------------------------------------------------------------
    # BUSINESS / LOCAL
    # -------------------------------------------------------------------------
    "yelp": APIConfig(
        source_name="yelp",
        base_url="https://api.yelp.com/v3",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="yelp_api_key",
        signup_url="https://www.yelp.com/developers/v3/manage_app",
        max_concurrency=2,
        rate_limit_per_minute=300,  # 500 calls/day = spread over 8 hours
        notes="Free tier: 500 calls/day",
    ),
    # -------------------------------------------------------------------------
    # KAGGLE
    # -------------------------------------------------------------------------
    "kaggle": APIConfig(
        source_name="kaggle",
        base_url="https://www.kaggle.com/api/v1",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="kaggle_username",  # Also needs kaggle_key
        signup_url="https://www.kaggle.com/account",
        max_concurrency=2,
        rate_limit_per_minute=30,
        notes="Download datasets. Uses kaggle library.",
    ),
    # -------------------------------------------------------------------------
    # FOOT TRAFFIC
    # -------------------------------------------------------------------------
    "safegraph": APIConfig(
        source_name="safegraph",
        base_url="https://api.safegraph.com",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="safegraph_api_key",
        signup_url="https://www.safegraph.com/",
        max_concurrency=2,
        rate_limit_per_minute=60,
        notes="$100-500/month. Weekly foot traffic patterns.",
    ),
    "placer": APIConfig(
        source_name="placer",
        base_url="https://api.placer.ai",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="placer_api_key",
        signup_url="https://www.placer.ai/",
        max_concurrency=2,
        rate_limit_per_minute=30,
        notes="Enterprise pricing. Retail analytics.",
    ),
    "foursquare": APIConfig(
        source_name="foursquare",
        base_url="https://api.foursquare.com/v3",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="foursquare_api_key",
        signup_url="https://developer.foursquare.com/",
        max_concurrency=2,
        rate_limit_per_minute=100,
        notes="POI data. Free tier available.",
    ),
    # -------------------------------------------------------------------------
    # PATENTS
    # -------------------------------------------------------------------------
    "uspto": APIConfig(
        source_name="uspto",
        base_url="https://search.patentsview.org/api/v1",
        api_key_requirement=APIKeyRequirement.REQUIRED,
        config_key="uspto_patentsview_api_key",
        signup_url="https://patentsview-support.atlassian.net/servicedesk/customer/portal/1/group/1/create/18",
        max_concurrency=2,
        rate_limit_per_minute=45,
        timeout_seconds=60.0,
        notes="PatentsView API. 45 req/min, max 1000 records/request. Free API key required.",
    ),
    # -------------------------------------------------------------------------
    # REFERENCE DATA
    # -------------------------------------------------------------------------
    "dunl": APIConfig(
        source_name="dunl",
        base_url="https://dunl.org",
        api_key_requirement=APIKeyRequirement.OPTIONAL,
        config_key="",
        signup_url="https://dunl.org",
        max_concurrency=2,
        rate_limit_per_minute=60,
        notes="S&P Global open data (DUNL.org). No key required. CC licensed.",
    ),
}


def get_api_config(source: str) -> APIConfig:
    """
    Get API configuration for a source.

    Args:
        source: Source name (e.g., 'fred', 'eia')

    Returns:
        APIConfig for the source

    Raises:
        KeyError: If source not found in registry
    """
    source_lower = source.lower()
    if source_lower not in API_REGISTRY:
        available = ", ".join(sorted(API_REGISTRY.keys()))
        raise KeyError(
            f"Unknown API source: {source}. " f"Available sources: {available}"
        )
    return API_REGISTRY[source_lower]


def get_all_sources() -> list[str]:
    """Get list of all registered API sources."""
    return sorted(API_REGISTRY.keys())


def get_sources_by_requirement(requirement: APIKeyRequirement) -> list[str]:
    """Get list of sources with a specific key requirement."""
    return sorted(
        [
            name
            for name, config in API_REGISTRY.items()
            if config.api_key_requirement == requirement
        ]
    )
