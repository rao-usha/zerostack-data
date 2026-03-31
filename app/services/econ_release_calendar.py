"""
Release calendar for economic data sources.
Maps series types to expected ingestion lag after official release.
"""
from dataclasses import dataclass
from typing import Dict


@dataclass
class ReleaseConfig:
    series_type: str
    description: str
    expected_lag_days: float      # calendar days after period end
    release_day_of_month: int     # approx day of month release publishes (0 = N/A)
    frequency: str                # "daily", "monthly", "quarterly", "annual"
    sla_multiplier: float = 2.0   # flag as stale after expected_lag_days * multiplier


RELEASE_CONFIGS: Dict[str, ReleaseConfig] = {
    # FRED daily series
    "fred_daily": ReleaseConfig(
        series_type="fred_daily",
        description="FRED daily series (DFF, DGS10, DGS2)",
        expected_lag_days=1,
        release_day_of_month=0,
        frequency="daily",
    ),
    # BLS Monthly Employment (CES/CPS)
    "bls_monthly_employment": ReleaseConfig(
        series_type="bls_monthly_employment",
        description="BLS CES/CPS (Employment Situation, 1st Friday of following month)",
        expected_lag_days=10,
        release_day_of_month=7,
        frequency="monthly",
    ),
    # BLS JOLTS
    "bls_jolts": ReleaseConfig(
        series_type="bls_jolts",
        description="BLS JOLTS (released ~35 days after month end)",
        expected_lag_days=35,
        release_day_of_month=5,
        frequency="monthly",
    ),
    # BLS CPI
    "bls_cpi": ReleaseConfig(
        series_type="bls_cpi",
        description="BLS CPI (released 2nd or 3rd week of following month)",
        expected_lag_days=15,
        release_day_of_month=12,
        frequency="monthly",
    ),
    # FRED monthly economic indicators
    "fred_monthly": ReleaseConfig(
        series_type="fred_monthly",
        description="FRED monthly series (UNRATE, CPIAUCSL, retail sales)",
        expected_lag_days=10,
        release_day_of_month=0,
        frequency="monthly",
    ),
    # BEA GDP (preliminary)
    "bea_gdp": ReleaseConfig(
        series_type="bea_gdp",
        description="BEA GDP preliminary estimate (released last week of following month)",
        expected_lag_days=30,
        release_day_of_month=25,
        frequency="quarterly",
    ),
    # BEA regional (annual)
    "bea_regional": ReleaseConfig(
        series_type="bea_regional",
        description="BEA Regional GDP and Income (annual, released September)",
        expected_lag_days=240,
        release_day_of_month=0,
        frequency="annual",
        sla_multiplier=1.1,  # tight — annual release is expected
    ),
    # Census ACS (annual)
    "census_acs": ReleaseConfig(
        series_type="census_acs",
        description="Census ACS 5-year estimates (annual, released December)",
        expected_lag_days=365,
        release_day_of_month=0,
        frequency="annual",
        sla_multiplier=1.1,
    ),
}

# Map source prefixes to release config keys
SOURCE_TO_RELEASE_CONFIG = {
    "fred_interest_rates": "fred_daily",     # DFF, DGS10 are daily
    "fred_economic_indicators": "fred_monthly",
    "fred_housing_market": "fred_monthly",
    "fred_consumer_sentiment": "fred_monthly",
    "bls_jolts": "bls_jolts",
    "bls_ces_employment": "bls_monthly_employment",
    "bls_cps_labor_force": "bls_monthly_employment",
    "bls_cpi": "bls_cpi",
    "bls_laus_state": "bls_monthly_employment",
    "bea_nipa": "bea_gdp",
    "bea_regional": "bea_regional",
}


def get_release_config(table_name: str) -> ReleaseConfig:
    """Get release config for a table name. Returns weekly as default."""
    for prefix, config_key in SOURCE_TO_RELEASE_CONFIG.items():
        if table_name.startswith(prefix):
            return RELEASE_CONFIGS[config_key]
    # Default for unknown econ tables: weekly
    return ReleaseConfig(
        series_type="unknown",
        description="Unknown economic series",
        expected_lag_days=7,
        release_day_of_month=0,
        frequency="weekly",
    )
