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
import asyncio
import logging
import random
from typing import Dict, List, Optional, Any
import httpx
from datetime import datetime

logger = logging.getLogger(__name__)


class EIAClient:
    """
    HTTP client for EIA API v2 with bounded concurrency and rate limiting.
    
    Responsibilities:
    - Make HTTP requests to EIA API
    - Implement retry logic with exponential backoff
    - Respect rate limits via semaphore
    - Handle API errors gracefully
    """
    
    # EIA API v2 endpoints
    BASE_URL = "https://api.eia.gov/v2"
    
    # Rate limit defaults (conservative)
    # EIA allows 5,000 requests per hour with API key
    # That's about 83 requests per minute or 1.4 per second
    # We default to 2 concurrent requests to be conservative
    DEFAULT_MAX_CONCURRENCY = 2
    DEFAULT_MAX_REQUESTS_PER_MINUTE = 60  # Conservative default
    
    def __init__(
        self,
        api_key: str,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_retries: int = 3,
        backoff_factor: float = 2.0
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
        
        self.api_key = api_key
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        
        # Semaphore for bounded concurrency - MANDATORY per RULES
        self.semaphore = asyncio.Semaphore(max_concurrency)
        
        # HTTP client (will be created in async context)
        self._client: Optional[httpx.AsyncClient] = None
        
        logger.info(
            f"Initialized EIAClient: "
            f"api_key_present=True, "
            f"max_concurrency={max_concurrency}"
        )
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0)
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def get_petroleum_data(
        self,
        route: str = "pet/cons/psup/a",
        frequency: str = "annual",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000
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
            
        Raises:
            Exception: On API errors after retries
        """
        url = f"{self.BASE_URL}/{route}/data/"
        
        params = {
            "api_key": self.api_key,
            "frequency": frequency,
            "offset": offset,
            "length": length
        }
        
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value
        
        return await self._request_with_retry(url, params, f"petroleum:{route}")
    
    async def get_natural_gas_data(
        self,
        route: str = "natural-gas/cons/sum/a",
        frequency: str = "annual",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000
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
        url = f"{self.BASE_URL}/{route}/data/"
        
        params = {
            "api_key": self.api_key,
            "frequency": frequency,
            "offset": offset,
            "length": length
        }
        
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value
        
        return await self._request_with_retry(url, params, f"natural-gas:{route}")
    
    async def get_electricity_data(
        self,
        route: str = "electricity/retail-sales",
        frequency: str = "annual",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000
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
        url = f"{self.BASE_URL}/{route}/data/"
        
        params = {
            "api_key": self.api_key,
            "frequency": frequency,
            "offset": offset,
            "length": length
        }
        
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value
        
        return await self._request_with_retry(url, params, f"electricity:{route}")
    
    async def get_retail_gas_prices(
        self,
        frequency: str = "weekly",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000
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
        url = f"{self.BASE_URL}/petroleum/pri/gnd/data/"
        
        params = {
            "api_key": self.api_key,
            "frequency": frequency,
            "offset": offset,
            "length": length
        }
        
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value
        
        return await self._request_with_retry(url, params, "retail-gas-prices")
    
    async def get_steo_projections(
        self,
        route: str = "steo",
        frequency: str = "monthly",
        start: Optional[str] = None,
        end: Optional[str] = None,
        facets: Optional[Dict[str, str]] = None,
        offset: int = 0,
        length: int = 5000
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
        url = f"{self.BASE_URL}/{route}/data/"
        
        params = {
            "api_key": self.api_key,
            "frequency": frequency,
            "offset": offset,
            "length": length
        }
        
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if facets:
            for key, value in facets.items():
                params[f"facets[{key}]"] = value
        
        return await self._request_with_retry(url, params, "steo-projections")
    
    async def get_facets(
        self,
        route: str
    ) -> Dict[str, Any]:
        """
        Get available facets (dimensions/filters) for a given route.
        
        Args:
            route: API route path (e.g., "petroleum/cons/psup/a")
            
        Returns:
            Dict containing available facets
        """
        url = f"{self.BASE_URL}/{route}/facets/"
        
        params = {
            "api_key": self.api_key
        }
        
        return await self._request_with_retry(url, params, f"facets:{route}")
    
    async def _request_with_retry(
        self,
        url: str,
        params: Dict[str, Any],
        data_id: str
    ) -> Dict[str, Any]:
        """
        Make HTTP GET request with exponential backoff retry.
        
        Args:
            url: API endpoint URL
            params: Query parameters
            data_id: Data identifier (for logging)
            
        Returns:
            Parsed JSON response
            
        Raises:
            Exception: After all retries exhausted
        """
        async with self.semaphore:  # Bounded concurrency
            client = await self._get_client()
            
            for attempt in range(self.max_retries):
                try:
                    logger.debug(
                        f"Fetching EIA data {data_id} "
                        f"(attempt {attempt+1}/{self.max_retries})"
                    )
                    
                    response = await client.get(url, params=params)
                    
                    # Check for HTTP errors
                    response.raise_for_status()
                    
                    # Parse JSON response
                    data = response.json()
                    
                    # Check for EIA API errors
                    if "error" in data or "errors" in data:
                        error_msg = data.get("error", data.get("errors", "Unknown error"))
                        
                        logger.warning(
                            f"EIA API returned error: {error_msg}"
                        )
                        
                        # Retry on transient errors
                        if attempt < self.max_retries - 1:
                            await self._backoff(attempt)
                            continue
                        else:
                            raise Exception(f"EIA API error: {error_msg}")
                    
                    # Success!
                    logger.debug(f"Successfully fetched data {data_id}")
                    return data
                
                except httpx.HTTPStatusError as e:
                    logger.warning(
                        f"HTTP error fetching EIA data (attempt {attempt+1}): {e}"
                    )
                    
                    # Check for rate limiting (429)
                    if e.response.status_code == 429:
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            wait_time = int(retry_after)
                            logger.warning(f"Rate limited. Waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue
                    
                    # Retry on 5xx errors, not on 4xx
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(
                            f"EIA API HTTP error: {e.response.status_code} - "
                            f"{e.response.text}"
                        )
                
                except httpx.RequestError as e:
                    logger.warning(
                        f"Request error fetching EIA data (attempt {attempt+1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"EIA API request failed: {str(e)}")
                
                except Exception as e:
                    logger.error(
                        f"Unexpected error fetching EIA data (attempt {attempt+1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise
            
            # Should never reach here, but just in case
            raise Exception(f"Failed to fetch EIA data after {self.max_retries} attempts")
    
    async def _backoff(self, attempt: int):
        """
        Exponential backoff with jitter.
        
        Args:
            attempt: Current attempt number (0-indexed)
        """
        # Calculate backoff: base * factor^attempt + random jitter
        base_delay = 1.0
        max_delay = 60.0
        
        delay = min(
            base_delay * (self.backoff_factor ** attempt),
            max_delay
        )
        
        # Add jitter (±25%)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        delay_with_jitter = max(0.1, delay + jitter)
        
        logger.debug(f"Backing off for {delay_with_jitter:.2f}s")
        await asyncio.sleep(delay_with_jitter)


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
    }
}

