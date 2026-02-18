"""
USDA NASS QuickStats API client.

Data source: https://quickstats.nass.usda.gov/api

Provides access to:
- Crop production data (corn, soybeans, wheat, etc.)
- Livestock inventory and prices
- Farm economics and income
- Census of Agriculture data

API Key required - register free at: https://quickstats.nass.usda.gov/api

Rate limits:
- 50,000 records per request maximum
- No documented rate limit, but use conservative approach
"""

import asyncio
import logging
import random
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
import httpx

logger = logging.getLogger(__name__)


class USDAClient:
    """
    HTTP client for USDA NASS QuickStats API.

    Provides access to agricultural statistics including:
    - Crop production and yields
    - Prices received by farmers
    - Livestock inventory
    - Farm economics
    """

    BASE_URL = "https://quickstats.nass.usda.gov/api"

    # Common API parameters
    SECTORS = [
        "CROPS",
        "ANIMALS & PRODUCTS",
        "ECONOMICS",
        "ENVIRONMENTAL",
        "DEMOGRAPHICS",
    ]

    # Key commodities
    COMMODITIES = {
        "grains": ["CORN", "SOYBEANS", "WHEAT", "OATS", "BARLEY", "SORGHUM", "RICE"],
        "oilseeds": ["SOYBEANS", "SUNFLOWER", "CANOLA", "PEANUTS"],
        "cotton": ["COTTON"],
        "fruits": ["APPLES", "ORANGES", "GRAPES", "STRAWBERRIES"],
        "vegetables": ["TOMATOES", "POTATOES", "ONIONS", "LETTUCE"],
        "livestock": ["CATTLE", "HOGS", "SHEEP", "CHICKENS", "TURKEYS"],
        "dairy": ["MILK"],
    }

    # Data items for crops
    CROP_DATA_ITEMS = [
        "PRODUCTION",
        "YIELD",
        "AREA PLANTED",
        "AREA HARVESTED",
        "PRICE RECEIVED",
        "CONDITION",
        "PROGRESS",
    ]

    DEFAULT_MAX_CONCURRENCY = 2
    DEFAULT_REQUESTS_PER_SECOND = 0.5  # Conservative

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize USDA client.

        Args:
            api_key: USDA NASS API key (or set USDA_API_KEY env var)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff multiplier
        """
        self.api_key = api_key or os.environ.get("USDA_API_KEY")
        if not self.api_key:
            raise ValueError(
                "USDA API key required. Register at https://quickstats.nass.usda.gov/api "
                "and set USDA_API_KEY environment variable"
            )

        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: Optional[httpx.AsyncClient] = None
        self._last_request_time: float = 0
        self._min_request_interval = 1.0 / self.DEFAULT_REQUESTS_PER_SECOND

        logger.info(f"Initialized USDAClient: max_concurrency={max_concurrency}")

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(120.0, connect=30.0), follow_redirects=True
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _rate_limit(self):
        """Enforce rate limiting between requests."""
        now = asyncio.get_running_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - elapsed)
        self._last_request_time = asyncio.get_running_loop().time()

    async def get_crop_production(
        self,
        commodity: str,
        year: int = None,
        state: Optional[str] = None,
        data_item: str = "PRODUCTION",
    ) -> List[Dict[str, Any]]:
        """
        Get crop production data.

        Args:
            commodity: Commodity name (e.g., "CORN", "SOYBEANS")
            year: Year (defaults to current)
            state: State name or "US TOTAL" for national
            data_item: Type of data (PRODUCTION, YIELD, AREA PLANTED, etc.)

        Returns:
            List of production records
        """
        if year is None:
            year = datetime.now().year

        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "commodity_desc": commodity.upper(),
            "statisticcat_desc": data_item,
            "year": year,
            "format": "JSON",
        }

        if state:
            if state.upper() == "US TOTAL":
                params["agg_level_desc"] = "NATIONAL"
            else:
                params["state_name"] = state.upper()
                params["agg_level_desc"] = "STATE"

        return await self._api_request(params, f"crop_{commodity}_{data_item}")

    async def get_crop_progress(
        self, commodity: str, year: int = None, state: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get weekly crop progress data (planting, emergence, harvest progress).

        Args:
            commodity: Commodity name
            year: Year
            state: State name

        Returns:
            List of progress records
        """
        if year is None:
            year = datetime.now().year

        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "commodity_desc": commodity.upper(),
            "statisticcat_desc": "PROGRESS",
            "year": year,
            "format": "JSON",
        }

        if state:
            params["state_name"] = state.upper()
            params["agg_level_desc"] = "STATE"

        return await self._api_request(params, f"progress_{commodity}")

    async def get_crop_condition(
        self, commodity: str, year: int = None, state: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get weekly crop condition ratings (good/excellent, fair, poor, etc.).

        Args:
            commodity: Commodity name
            year: Year
            state: State name

        Returns:
            List of condition records
        """
        if year is None:
            year = datetime.now().year

        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "commodity_desc": commodity.upper(),
            "statisticcat_desc": "CONDITION",
            "year": year,
            "format": "JSON",
        }

        if state:
            params["state_name"] = state.upper()

        return await self._api_request(params, f"condition_{commodity}")

    async def get_prices_received(
        self, commodity: str, year: int = None, state: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get prices received by farmers for commodities.

        Args:
            commodity: Commodity name
            year: Year
            state: State name (optional)

        Returns:
            List of price records
        """
        if year is None:
            year = datetime.now().year

        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "commodity_desc": commodity.upper(),
            "statisticcat_desc": "PRICE RECEIVED",
            "year": year,
            "format": "JSON",
        }

        if state:
            params["state_name"] = state.upper()

        return await self._api_request(params, f"prices_{commodity}")

    async def get_livestock_inventory(
        self, commodity: str, year: int = None, state: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get livestock inventory data.

        Args:
            commodity: Livestock type (CATTLE, HOGS, etc.)
            year: Year
            state: State name

        Returns:
            List of inventory records
        """
        if year is None:
            year = datetime.now().year

        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "sector_desc": "ANIMALS & PRODUCTS",
            "commodity_desc": commodity.upper(),
            "statisticcat_desc": "INVENTORY",
            "year": year,
            "format": "JSON",
        }

        if state:
            params["state_name"] = state.upper()

        return await self._api_request(params, f"livestock_{commodity}")

    async def get_all_crops_production(
        self, year: int = None, state: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get production data for all major crops.

        Args:
            year: Year
            state: State name (optional, defaults to national)

        Returns:
            List of production records for all crops
        """
        if year is None:
            year = datetime.now().year

        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "statisticcat_desc": "PRODUCTION",
            "year": year,
            "agg_level_desc": "NATIONAL" if not state else "STATE",
            "format": "JSON",
        }

        if state:
            params["state_name"] = state.upper()

        return await self._api_request(params, f"all_crops_production_{year}")

    async def get_annual_summary(self, year: int = None) -> List[Dict[str, Any]]:
        """
        Get annual crop production summary for major commodities.

        Args:
            year: Year

        Returns:
            List of annual production records
        """
        if year is None:
            year = datetime.now().year

        params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "statisticcat_desc": "PRODUCTION",
            "freq_desc": "ANNUAL",
            "year": year,
            "agg_level_desc": "NATIONAL",
            "format": "JSON",
        }

        return await self._api_request(params, f"annual_summary_{year}")

    async def _api_request(
        self, params: Dict[str, Any], data_id: str
    ) -> List[Dict[str, Any]]:
        """
        Make USDA API request with retry logic.

        Args:
            params: Query parameters
            data_id: Identifier for logging

        Returns:
            List of records from API
        """
        url = f"{self.BASE_URL}/api_GET/"

        async with self.semaphore:
            await self._rate_limit()
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.debug(
                        f"Fetching USDA {data_id} (attempt {attempt+1}/{self.max_retries})"
                    )

                    response = await client.get(url, params=params)

                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After", "60")
                        logger.warning(f"Rate limited. Waiting {retry_after}s")
                        await asyncio.sleep(int(retry_after))
                        continue

                    response.raise_for_status()

                    data = response.json()

                    # USDA returns {"data": [...]} on success
                    if "data" in data:
                        records = data["data"]
                        logger.debug(
                            f"Successfully fetched {len(records)} {data_id} records"
                        )
                        return records

                    # Error response
                    if "error" in data:
                        raise Exception(f"USDA API error: {data['error']}")

                    return []

                except httpx.HTTPStatusError as e:
                    logger.warning(
                        f"HTTP error fetching USDA data (attempt {attempt+1}): {e}"
                    )
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(
                            f"USDA API HTTP error: {e.response.status_code} - "
                            f"{e.response.text[:500]}"
                        )

                except httpx.RequestError as e:
                    logger.warning(
                        f"Request error fetching USDA data (attempt {attempt+1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"USDA API request failed: {str(e)}")

                except Exception as e:
                    logger.error(f"Unexpected error fetching USDA data: {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise

            raise Exception(
                f"Failed to fetch USDA data after {self.max_retries} attempts"
            )

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        base_delay = 1.0
        max_delay = 60.0
        delay = min(base_delay * (self.backoff_factor**attempt), max_delay)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        await asyncio.sleep(max(0.1, delay + jitter))


# Major crop states for reference
MAJOR_CROP_STATES = {
    "CORN": ["IOWA", "ILLINOIS", "NEBRASKA", "MINNESOTA", "INDIANA"],
    "SOYBEANS": ["ILLINOIS", "IOWA", "MINNESOTA", "INDIANA", "OHIO"],
    "WHEAT": ["KANSAS", "NORTH DAKOTA", "MONTANA", "WASHINGTON", "OKLAHOMA"],
    "COTTON": ["TEXAS", "GEORGIA", "MISSISSIPPI", "ARKANSAS", "ALABAMA"],
}

# Units for common data items
DATA_UNITS = {
    "PRODUCTION": {
        "CORN": "BU",
        "SOYBEANS": "BU",
        "WHEAT": "BU",
        "COTTON": "480 LB BALES",
    },
    "YIELD": {"CORN": "BU / ACRE", "SOYBEANS": "BU / ACRE", "WHEAT": "BU / ACRE"},
    "AREA PLANTED": "ACRES",
    "AREA HARVESTED": "ACRES",
    "PRICE RECEIVED": "$ / BU",
}
