"""
EIA API client with rate limiting and retry logic.

Official EIA API documentation:
https://www.eia.gov/opendata/

EIA API provides access to Energy Information Administration data:
- Petroleum & gas production, consumption, prices
- Electricity generation, consumption, prices
- Retail gas prices
- STEO (Short-Term Energy Outlook) projections

Rate limits:
- With API key (free): 5,000 requests per hour per IP
- API key required (free registration): https://www.eia.gov/opendata/register.php

API v2 is the current version as of 2025.
"""

import logging
from typing import Dict, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import RetryableError
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class EIAClient(BaseAPIClient):
    """
    HTTP client for EIA API v2 with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "eia"
    BASE_URL = "https://api.eia.gov/v2"

    def __init__(
        self,
        api_key: str,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize EIA API client.

        Args:
            api_key: EIA API key (required)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier

        Raises:
            ValueError: If api_key is not provided
        """
        if not api_key:
            raise ValueError(
                "EIA_API_KEY is required for EIA operations. "
                "Get a free key at: https://www.eia.gov/opendata/register.php"
            )

        # Get config from registry
        config = get_api_config("eia")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

    def _add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add API key to request parameters."""
        params["api_key"] = self.api_key
        return params

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        """Check for EIA-specific API errors."""
        if "error" in data or "errors" in data:
            error_msg = data.get("error", data.get("errors", "Unknown error"))
            logger.warning(f"EIA API error: {error_msg}")

            # EIA errors are generally retryable (transient issues)
            return RetryableError(
                message=f"EIA API error: {error_msg}",
                source=self.SOURCE_NAME,
                response_data=data,
            )

        return None

    async def get_petroleum_data(
        self,
        route: str = "pet/cons/psup/a",
        frequency: str = "annual",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000,
    ) -> Dict[str, Any]:
        """
        Fetch petroleum data from EIA API.

        Args:
            route: API route path (e.g., "pet/cons/psup/a" for petroleum consumption)
            frequency: Data frequency (annual, monthly, weekly, daily)
            start: Start date (format depends on frequency)
            end: End date (format depends on frequency)
            facets: Optional facet filters (e.g., {"process": "VPP", "product": "EPP0"})
            offset: Pagination offset
            length: Number of records to return (max 5000)

        Returns:
            Dict containing API response with data
        """
        params: Dict[str, Any] = {
            "frequency": frequency,
            "offset": offset,
            "length": length,
        }

        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value

        return await self.get(
            f"{route}/data/", params=params, resource_id=f"petroleum:{route}"
        )

    async def get_natural_gas_data(
        self,
        route: str = "natural-gas/cons/sum/a",
        frequency: str = "annual",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000,
    ) -> Dict[str, Any]:
        """
        Fetch natural gas data from EIA API.

        Args:
            route: API route path (e.g., "natural-gas/cons/sum/a")
            frequency: Data frequency (annual, monthly)
            start: Start date
            end: End date
            facets: Optional facet filters
            offset: Pagination offset
            length: Number of records to return

        Returns:
            Dict containing API response with data
        """
        params: Dict[str, Any] = {
            "frequency": frequency,
            "offset": offset,
            "length": length,
        }

        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value

        return await self.get(
            f"{route}/data/", params=params, resource_id=f"natural-gas:{route}"
        )

    async def get_electricity_data(
        self,
        route: str = "electricity/retail-sales",
        frequency: str = "annual",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000,
    ) -> Dict[str, Any]:
        """
        Fetch electricity data from EIA API.

        Args:
            route: API route path (e.g., "electricity/retail-sales")
            frequency: Data frequency (annual, monthly)
            start: Start date
            end: End date
            facets: Optional facet filters (e.g., {"sectorid": "RES", "stateid": "CA"})
            offset: Pagination offset
            length: Number of records to return

        Returns:
            Dict containing API response with data
        """
        params: Dict[str, Any] = {
            "frequency": frequency,
            "offset": offset,
            "length": length,
        }

        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value

        return await self.get(
            f"{route}/data/", params=params, resource_id=f"electricity:{route}"
        )

    async def get_retail_gas_prices(
        self,
        frequency: str = "weekly",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000,
    ) -> Dict[str, Any]:
        """
        Fetch retail gas prices from EIA API.

        Args:
            frequency: Data frequency (weekly, daily)
            start: Start date
            end: End date
            facets: Optional facet filters (e.g., {"product": "EPM0", "area": "NUS"})
            offset: Pagination offset
            length: Number of records to return

        Returns:
            Dict containing API response with data
        """
        params: Dict[str, Any] = {
            "frequency": frequency,
            "offset": offset,
            "length": length,
        }

        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value

        return await self.get(
            "petroleum/pri/gnd/data/", params=params, resource_id="retail-gas-prices"
        )

    async def get_steo_projections(
        self,
        route: str = "steo",
        frequency: str = "monthly",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000,
    ) -> Dict[str, Any]:
        """
        Fetch Short-Term Energy Outlook (STEO) projections from EIA API.

        Args:
            route: API route path (default "steo")
            frequency: Data frequency (monthly)
            start: Start date
            end: End date
            facets: Optional facet filters
            offset: Pagination offset
            length: Number of records to return

        Returns:
            Dict containing API response with data
        """
        params: Dict[str, Any] = {
            "frequency": frequency,
            "offset": offset,
            "length": length,
        }

        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value

        return await self.get(
            f"{route}/data/", params=params, resource_id="steo-projections"
        )

    async def get_facets(self, route: str) -> Dict[str, Any]:
        """
        Get available facets (dimensions/filters) for a given route.

        Args:
            route: API route path (e.g., "petroleum/cons/psup/a")

        Returns:
            Dict containing available facets
        """
        return await self.get(
            f"{route}/facets/", params={}, resource_id=f"facets:{route}"
        )


# Common EIA data categories and routes
COMMON_ROUTES = {
    "petroleum": {
        "consumption": "pet/cons/psup/a",
        "production": "pet/prod/table/a",
        "imports": "pet/move/imp/a",
        "exports": "pet/move/exp/a",
        "stocks": "pet/stoc/wstk/w",
    },
    "natural_gas": {
        "consumption": "natural-gas/cons/sum/a",
        "production": "natural-gas/prod/sum/a",
        "storage": "natural-gas/stor/sum/a",
        "prices": "natural-gas/pri/sum/a",
    },
    "electricity": {
        "generation": "electricity/electric-power-operational-data",
        "retail_sales": "electricity/retail-sales",
        "revenue": "electricity/revenue",
        "customers": "electricity/customers",
    },
    "retail_gas_prices": {
        "all_grades": "petroleum/pri/gnd",
    },
    "steo": {
        "projections": "steo",
    },
}
