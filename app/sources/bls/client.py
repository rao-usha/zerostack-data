"""
BLS (Bureau of Labor Statistics) API client with rate limiting and retry logic.

Official BLS API documentation:
https://www.bls.gov/developers/api_signature_v2.htm

BLS API provides access to Bureau of Labor Statistics data:
- CES (Current Employment Statistics) - Employment, hours, earnings by industry
- CPS (Current Population Survey) - Labor force status, unemployment
- JOLTS (Job Openings and Labor Turnover Survey) - Job openings, hires, quits
- CPI (Consumer Price Index) - Inflation measures
- PPI (Producer Price Index) - Wholesale/producer prices
- OES (Occupational Employment Statistics) - Employment and wages by occupation

Rate limits:
- Without API key: 25 queries/day, 10 years per query, 25 series per query
- With API key (free): 500 queries/day, 20 years per query, 50 series per query
- API key available at: https://data.bls.gov/registrationEngine/
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import (
    FatalError,
    ValidationError,
    RateLimitError,
)
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class BLSClient(BaseAPIClient):
    """
    HTTP client for BLS API v2 with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    Note: BLS API uses POST for data requests.
    """

    SOURCE_NAME = "bls"
    BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    # Series limits based on API key presence
    MAX_SERIES_WITH_KEY = 50
    MAX_SERIES_WITHOUT_KEY = 25
    MAX_YEARS_WITH_KEY = 20
    MAX_YEARS_WITHOUT_KEY = 10

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize BLS API client.

        Args:
            api_key: Optional BLS API key (recommended for production)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        # Get config from registry
        config = get_api_config("bls")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=60.0,  # BLS can be slow
            connect_timeout=15.0,
            rate_limit_interval=config.rate_limit_interval or 0.5,
        )

        # Determine max series per request based on API key presence
        self.max_series_per_request = (
            self.MAX_SERIES_WITH_KEY if api_key else self.MAX_SERIES_WITHOUT_KEY
        )
        self.max_years = (
            self.MAX_YEARS_WITH_KEY if api_key else self.MAX_YEARS_WITHOUT_KEY
        )

        if not api_key:
            logger.warning(
                "BLS API key not provided. Limited to 25 queries/day, 10 years per query. "
                f"Get a free key at: {config.signup_url}"
            )

        logger.info(
            f"Initialized BLSClient: "
            f"api_key_present={api_key is not None}, "
            f"max_series_per_request={self.max_series_per_request}"
        )

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        """Check for BLS-specific API errors."""
        status = data.get("status")

        if status == "REQUEST_SUCCEEDED":
            return None

        if status == "REQUEST_FAILED":
            message = data.get("message", [])
            if isinstance(message, list):
                message = "; ".join(message)

            # Check for known error types
            message_lower = str(message).lower()

            if "invalid series" in message_lower:
                return ValidationError(
                    message=f"Invalid series ID: {message}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )

            if "rate limit" in message_lower:
                return RateLimitError(
                    message=f"Rate limited by BLS: {message}",
                    source=self.SOURCE_NAME,
                    retry_after=30,
                    response_data=data,
                )

            return FatalError(
                message=f"BLS API error: {message}",
                source=self.SOURCE_NAME,
                response_data=data,
            )

        # Unknown status - log but don't fail
        if status:
            logger.warning(f"Unexpected BLS status: {status}")

        return None

    async def fetch_series(
        self,
        series_ids: List[str],
        start_year: int,
        end_year: int,
        calculations: bool = False,
        annual_average: bool = False,
    ) -> Dict[str, Any]:
        """
        Fetch time series data for one or more BLS series.

        Args:
            series_ids: List of BLS series IDs (e.g., ["LNS14000000", "CUUR0000SA0"])
            start_year: Start year (e.g., 2020)
            end_year: End year (e.g., 2024)
            calculations: Include net changes and percent changes (API key required)
            annual_average: Include annual averages (API key required)

        Returns:
            Dict containing API response with series data

        Raises:
            ValueError: If series count or year range exceeds limits
        """
        # Validate series count
        if len(series_ids) > self.max_series_per_request:
            raise ValueError(
                f"Too many series ({len(series_ids)}). "
                f"Max is {self.max_series_per_request} per request "
                f"({'with' if self.api_key else 'without'} API key)"
            )

        # Validate year range
        year_range = end_year - start_year
        if year_range > self.max_years:
            raise ValueError(
                f"Year range too large ({year_range} years). "
                f"Max is {self.max_years} years {'with' if self.api_key else 'without'} API key"
            )

        # Build request payload
        payload: Dict[str, Any] = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
        }

        if self.api_key:
            payload["registrationkey"] = self.api_key
            if calculations:
                payload["calculations"] = True
            if annual_average:
                payload["annualaverage"] = True

        series_list = ", ".join(series_ids[:3])
        if len(series_ids) > 3:
            series_list += f"... ({len(series_ids)} total)"

        return await self.post(
            self.BASE_URL, json_body=payload, resource_id=f"series:[{series_list}]"
        )

    async def fetch_multiple_batches(
        self, series_ids: List[str], start_year: int, end_year: int
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch multiple series, splitting into batches if necessary.

        Args:
            series_ids: List of BLS series IDs
            start_year: Start year
            end_year: End year

        Returns:
            Dict mapping series_id to list of observations
        """
        results: Dict[str, List[Dict[str, Any]]] = {}

        # Split into batches
        batch_size = self.max_series_per_request
        batches = [
            series_ids[i : i + batch_size]
            for i in range(0, len(series_ids), batch_size)
        ]

        for batch_num, batch in enumerate(batches, 1):
            logger.info(
                f"Fetching batch {batch_num}/{len(batches)}: {len(batch)} series"
            )

            try:
                response = await self.fetch_series(
                    series_ids=batch, start_year=start_year, end_year=end_year
                )

                # Parse response
                if response.get("status") == "REQUEST_SUCCEEDED":
                    for series_data in response.get("Results", {}).get("series", []):
                        series_id = series_data.get("seriesID")
                        data = series_data.get("data", [])
                        results[series_id] = data
                        logger.debug(
                            f"Fetched {len(data)} observations for {series_id}"
                        )
                else:
                    logger.warning(
                        f"BLS API request did not succeed: {response.get('status')}"
                    )
                    for series_id in batch:
                        if series_id not in results:
                            results[series_id] = []

            except Exception as e:
                logger.error(f"Failed to fetch batch: {e}")
                for series_id in batch:
                    if series_id not in results:
                        results[series_id] = []

        return results


# =============================================================================
# COMMON BLS SERIES IDS ORGANIZED BY DATASET
# =============================================================================

# Unemployment & Labor Force (CPS - Current Population Survey)
CPS_SERIES = {
    "unemployment_rate": "LNS14000000",  # Unemployment Rate (seasonally adjusted)
    "labor_force_participation_rate": "LNS11300000",  # Labor Force Participation Rate
    "employment_level": "LNS12000000",  # Employment Level
    "unemployment_level": "LNS13000000",  # Unemployment Level
    "civilian_labor_force": "LNS11000000",  # Civilian Labor Force Level
    "employment_population_ratio": "LNS12300000",  # Employment-Population Ratio
    "u6_unemployment": "LNS13327709",  # U-6 Total unemployed + marginally attached + part-time for economic reasons
}

# Employment (CES - Current Employment Statistics)
CES_SERIES = {
    "total_nonfarm": "CES0000000001",  # Total Nonfarm Employment
    "total_private": "CES0500000001",  # Total Private Employment
    "manufacturing": "CES3000000001",  # Manufacturing Employment
    "construction": "CES2000000001",  # Construction Employment
    "retail_trade": "CES4200000001",  # Retail Trade Employment
    "leisure_hospitality": "CES7000000001",  # Leisure and Hospitality Employment
    "professional_business": "CES6000000001",  # Professional and Business Services
    "education_health": "CES6500000001",  # Education and Health Services
    "financial_activities": "CES5500000001",  # Financial Activities
    "average_hourly_earnings": "CES0500000003",  # Average Hourly Earnings, All Private
    "average_weekly_hours": "CES0500000002",  # Average Weekly Hours, All Private
}

# Job Openings and Labor Turnover (JOLTS)
JOLTS_SERIES = {
    "job_openings_total": "JTS000000000000000JOL",  # Total Job Openings Level
    "job_openings_rate": "JTS000000000000000JOR",  # Job Openings Rate
    "hires_total": "JTS000000000000000HIL",  # Total Hires Level
    "hires_rate": "JTS000000000000000HIR",  # Hires Rate
    "quits_total": "JTS000000000000000QUL",  # Total Quits Level
    "quits_rate": "JTS000000000000000QUR",  # Quits Rate
    "layoffs_total": "JTS000000000000000LDL",  # Total Layoffs and Discharges Level
    "layoffs_rate": "JTS000000000000000LDR",  # Layoffs and Discharges Rate
    "separations_total": "JTS000000000000000TSL",  # Total Separations Level
    "separations_rate": "JTS000000000000000TSR",  # Total Separations Rate
}

# Consumer Price Index (CPI)
CPI_SERIES = {
    "cpi_all_items": "CUUR0000SA0",  # CPI-U All Items (urban consumers)
    "cpi_core": "CUUR0000SA0L1E",  # Core CPI (less food and energy)
    "cpi_food": "CUUR0000SAF1",  # CPI Food
    "cpi_food_at_home": "CUUR0000SAF11",  # CPI Food at Home
    "cpi_energy": "CUUR0000SA0E",  # CPI Energy
    "cpi_gasoline": "CUUR0000SETB01",  # CPI Gasoline
    "cpi_electricity": "CUUR0000SEHE",  # CPI Electricity
    "cpi_shelter": "CUUR0000SAH1",  # CPI Shelter
    "cpi_medical": "CUUR0000SAM",  # CPI Medical Care
    "cpi_transportation": "CUUR0000SAT",  # CPI Transportation
    "cpi_apparel": "CUUR0000SAA",  # CPI Apparel
    # Seasonally adjusted versions
    "cpi_all_items_sa": "CUSR0000SA0",  # CPI-U All Items (seasonally adjusted)
    "cpi_core_sa": "CUSR0000SA0L1E",  # Core CPI (seasonally adjusted)
}

# Producer Price Index (PPI)
PPI_SERIES = {
    "ppi_final_demand": "WPSFD4",  # PPI Final Demand
    "ppi_final_demand_goods": "WPSFD41",  # PPI Final Demand Goods
    "ppi_final_demand_services": "WPSFD42",  # PPI Final Demand Services
    "ppi_intermediate_demand": "WPSID61",  # PPI Intermediate Demand
    "ppi_crude_goods": "WPUIP1000000",  # PPI Crude Materials for Further Processing
    "ppi_finished_goods": "WPUFD49104",  # PPI Finished Goods
    # Industry-specific
    "ppi_manufacturing": "PCU31-33--31-33--",  # PPI Manufacturing Industries
    "ppi_construction": "PCU23----23----",  # PPI Construction
}

# Occupational Employment Statistics (OES)
OES_SERIES = {
    # Note: OES uses a different series ID structure
    # These are examples - actual implementation may vary
    "median_wage_all": "OEUM000000000000000000001",  # Median hourly wage, all occupations
}


# All series organized by dataset
COMMON_SERIES = {
    "cps": CPS_SERIES,
    "ces": CES_SERIES,
    "jolts": JOLTS_SERIES,
    "cpi": CPI_SERIES,
    "ppi": PPI_SERIES,
    "oes": OES_SERIES,
}


def get_series_for_dataset(dataset: str) -> List[str]:
    """
    Get list of common series IDs for a BLS dataset.

    Args:
        dataset: Dataset name (cps, ces, jolts, cpi, ppi, oes)

    Returns:
        List of series IDs

    Raises:
        ValueError: If dataset is not recognized
    """
    dataset_lower = dataset.lower()

    if dataset_lower not in COMMON_SERIES:
        available = ", ".join(COMMON_SERIES.keys())
        raise ValueError(
            f"Unknown BLS dataset: {dataset}. " f"Available datasets: {available}"
        )

    return list(COMMON_SERIES[dataset_lower].values())


def get_series_info(series_id: str) -> Optional[Dict[str, str]]:
    """
    Get information about a BLS series ID.

    Args:
        series_id: BLS series ID

    Returns:
        Dict with dataset and name, or None if not found
    """
    for dataset, series_dict in COMMON_SERIES.items():
        for name, sid in series_dict.items():
            if sid == series_id:
                return {"dataset": dataset, "name": name, "series_id": sid}
    return None
