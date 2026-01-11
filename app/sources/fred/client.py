"""
FRED API client with rate limiting and retry logic.

Official FRED API documentation:
https://fred.stlouisfed.org/docs/api/fred/

FRED API provides access to Federal Reserve Economic Data:
- Core time series (GDP, unemployment, etc.)
- Interest rates (H.15)
- Monetary aggregates (M1, M2)
- Industrial production indices

Rate limits:
- Without API key: Limited access, throttled
- With API key (free): 120 requests per minute per IP
- API key available at: https://fred.stlouisfed.org/docs/api/api_key.html
"""
import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, RetryableError
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class FREDClient(BaseAPIClient):
    """
    HTTP client for FRED API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "fred"
    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize FRED API client.

        Args:
            api_key: Optional FRED API key (recommended for production)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        # Get config from registry
        config = get_api_config("fred")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval()
        )

        if not api_key:
            logger.warning(
                "FRED API key not provided. Access may be limited. "
                f"Get a free key at: {config.signup_url}"
            )

    def _add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add API key to request parameters."""
        if self.api_key:
            params["api_key"] = self.api_key
        params["file_type"] = "json"
        return params

    def _check_api_error(
        self,
        data: Dict[str, Any],
        resource_id: str
    ) -> Optional[Exception]:
        """Check for FRED-specific API errors."""
        if "error_code" in data:
            error_code = data.get("error_code")
            error_message = data.get("error_message", "Unknown error")

            logger.warning(f"FRED API error: {error_code} - {error_message}")

            # Non-retryable errors
            if error_code in [400, 404]:
                return FatalError(
                    message=f"FRED API error {error_code}: {error_message}",
                    source=self.SOURCE_NAME,
                    status_code=error_code,
                    response_data=data
                )

            # Retryable errors
            return RetryableError(
                message=f"FRED API error {error_code}: {error_message}",
                source=self.SOURCE_NAME,
                status_code=error_code,
                response_data=data
            )

        return None

    async def get_series_observations(
        self,
        series_id: str,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        units: str = "lin",
        frequency: Optional[str] = None,
        aggregation_method: str = "avg"
    ) -> Dict[str, Any]:
        """
        Fetch observations (data points) for a FRED series.

        Args:
            series_id: FRED series ID (e.g., "GDP", "UNRATE", "DFF")
            observation_start: Start date in YYYY-MM-DD format (optional)
            observation_end: End date in YYYY-MM-DD format (optional)
            units: Units transformation (lin, chg, ch1, pch, pc1, pca, cch, cca, log)
            frequency: Frequency aggregation (d, w, bw, m, q, sa, a, wef, weth, wew, wetu, wem, wesu, wesa, bwew, bwem)
            aggregation_method: Aggregation method (avg, sum, eop)

        Returns:
            Dict containing API response with observations

        Raises:
            APIError: On API errors after retries
        """
        params: Dict[str, Any] = {
            "series_id": series_id,
            "units": units,
            "aggregation_method": aggregation_method
        }

        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end
        if frequency:
            params["frequency"] = frequency

        return await self.get(
            "series/observations",
            params=params,
            resource_id=f"series:{series_id}"
        )

    async def get_series_info(self, series_id: str) -> Dict[str, Any]:
        """
        Fetch metadata for a FRED series.

        Args:
            series_id: FRED series ID

        Returns:
            Dict containing series metadata
        """
        params = {"series_id": series_id}
        return await self.get("series", params=params, resource_id=f"info:{series_id}")

    async def get_multiple_series(
        self,
        series_ids: List[str],
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch multiple series concurrently (with bounded concurrency).

        Args:
            series_ids: List of FRED series IDs
            observation_start: Start date in YYYY-MM-DD format (optional)
            observation_end: End date in YYYY-MM-DD format (optional)

        Returns:
            Dict mapping series_id to list of observations
        """
        async def fetch_one(series_id: str) -> List[Dict[str, Any]]:
            response = await self.get_series_observations(
                series_id=series_id,
                observation_start=observation_start,
                observation_end=observation_end
            )
            return response.get("observations", [])

        return await self.fetch_multiple(
            items=series_ids,
            fetch_func=fetch_one,
            item_id_func=lambda x: x
        )


# Common FRED series IDs organized by category
COMMON_SERIES = {
    "interest_rates": {
        "federal_funds_rate": "DFF",  # Effective Federal Funds Rate (daily)
        "10y_treasury": "DGS10",  # 10-Year Treasury Constant Maturity Rate
        "30y_treasury": "DGS30",  # 30-Year Treasury Constant Maturity Rate
        "3m_treasury": "DGS3MO",  # 3-Month Treasury Constant Maturity Rate
        "2y_treasury": "DGS2",  # 2-Year Treasury Constant Maturity Rate
        "5y_treasury": "DGS5",  # 5-Year Treasury Constant Maturity Rate
        "prime_rate": "DPRIME",  # Bank Prime Loan Rate
    },
    "monetary_aggregates": {
        "m1": "M1SL",  # M1 Money Stock (seasonally adjusted)
        "m2": "M2SL",  # M2 Money Stock (seasonally adjusted)
        "monetary_base": "BOGMBASE",  # Monetary Base
        "currency_in_circulation": "CURRCIR",  # Currency in Circulation
    },
    "industrial_production": {
        "total": "INDPRO",  # Industrial Production: Total Index
        "manufacturing": "IPMAN",  # Industrial Production: Manufacturing
        "mining": "IPMINE",  # Industrial Production: Mining
        "utilities": "IPU",  # Industrial Production: Electric and Gas Utilities
        "capacity_utilization": "TCU",  # Capacity Utilization: Total Industry
    },
    "economic_indicators": {
        "gdp": "GDP",  # Gross Domestic Product
        "real_gdp": "GDPC1",  # Real Gross Domestic Product
        "unemployment_rate": "UNRATE",  # Unemployment Rate
        "cpi": "CPIAUCSL",  # Consumer Price Index for All Urban Consumers
        "pce": "PCE",  # Personal Consumption Expenditures
        "retail_sales": "RSXFS",  # Retail Sales
    }
}
