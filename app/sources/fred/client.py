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
import asyncio
import logging
import random
from typing import Dict, List, Optional, Any
import httpx
from datetime import datetime

logger = logging.getLogger(__name__)


class FREDClient:
    """
    HTTP client for FRED API with bounded concurrency and rate limiting.
    
    Responsibilities:
    - Make HTTP requests to FRED API
    - Implement retry logic with exponential backoff
    - Respect rate limits via semaphore
    - Handle API errors gracefully
    """
    
    # FRED API endpoints
    BASE_URL = "https://api.stlouisfed.org/fred"
    
    # Rate limit defaults (conservative)
    # FRED allows 120 requests per minute with API key
    # We default to 2 concurrent requests to be conservative
    DEFAULT_MAX_CONCURRENCY = 2
    DEFAULT_MAX_REQUESTS_PER_MINUTE = 60  # Conservative default
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
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
        self.api_key = api_key
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        
        # Semaphore for bounded concurrency - MANDATORY per RULES
        self.semaphore = asyncio.Semaphore(max_concurrency)
        
        # HTTP client (will be created in async context)
        self._client: Optional[httpx.AsyncClient] = None
        
        if not api_key:
            logger.warning(
                "FRED API key not provided. Access may be limited. "
                "Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html"
            )
        
        logger.info(
            f"Initialized FREDClient: "
            f"api_key_present={api_key is not None}, "
            f"max_concurrency={max_concurrency}"
        )
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0)
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
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
            Exception: On API errors after retries
        """
        params = {
            "series_id": series_id,
            "file_type": "json"
        }
        
        if self.api_key:
            params["api_key"] = self.api_key
        
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end
        if frequency:
            params["frequency"] = frequency
        
        params["units"] = units
        params["aggregation_method"] = aggregation_method
        
        url = f"{self.BASE_URL}/series/observations"
        
        return await self._request_with_retry(url, params, series_id)
    
    async def get_series_info(self, series_id: str) -> Dict[str, Any]:
        """
        Fetch metadata for a FRED series.
        
        Args:
            series_id: FRED series ID
            
        Returns:
            Dict containing series metadata
        """
        params = {
            "series_id": series_id,
            "file_type": "json"
        }
        
        if self.api_key:
            params["api_key"] = self.api_key
        
        url = f"{self.BASE_URL}/series"
        
        return await self._request_with_retry(url, params, series_id)
    
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
        tasks = []
        for series_id in series_ids:
            task = self.get_series_observations(
                series_id=series_id,
                observation_start=observation_start,
                observation_end=observation_end
            )
            tasks.append((series_id, task))
        
        results = {}
        for series_id, task in tasks:
            try:
                response = await task
                observations = response.get("observations", [])
                results[series_id] = observations
            except Exception as e:
                logger.error(f"Failed to fetch series {series_id}: {e}")
                results[series_id] = []
        
        return results
    
    async def _request_with_retry(
        self,
        url: str,
        params: Dict[str, Any],
        series_id: str
    ) -> Dict[str, Any]:
        """
        Make HTTP GET request with exponential backoff retry.
        
        Args:
            url: API endpoint URL
            params: Query parameters
            series_id: Series ID being requested (for logging)
            
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
                        f"Fetching FRED series {series_id} "
                        f"(attempt {attempt+1}/{self.max_retries})"
                    )
                    
                    response = await client.get(url, params=params)
                    
                    # Check for HTTP errors
                    response.raise_for_status()
                    
                    # Parse JSON response
                    data = response.json()
                    
                    # Check for FRED API errors
                    if "error_code" in data:
                        error_code = data.get("error_code")
                        error_message = data.get("error_message", "Unknown error")
                        
                        logger.warning(
                            f"FRED API returned error: {error_code} - {error_message}"
                        )
                        
                        # Some errors are not retryable
                        if error_code in [400, 404]:  # Bad request or not found
                            raise Exception(
                                f"FRED API error {error_code}: {error_message}"
                            )
                        
                        # Retry on other errors
                        if attempt < self.max_retries - 1:
                            await self._backoff(attempt)
                            continue
                        else:
                            raise Exception(
                                f"FRED API error {error_code}: {error_message}"
                            )
                    
                    # Success!
                    logger.debug(f"Successfully fetched series {series_id}")
                    return data
                
                except httpx.HTTPStatusError as e:
                    logger.warning(
                        f"HTTP error fetching FRED data (attempt {attempt+1}): {e}"
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
                            f"FRED API HTTP error: {e.response.status_code} - "
                            f"{e.response.text}"
                        )
                
                except httpx.RequestError as e:
                    logger.warning(
                        f"Request error fetching FRED data (attempt {attempt+1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"FRED API request failed: {str(e)}")
                
                except Exception as e:
                    logger.error(
                        f"Unexpected error fetching FRED data (attempt {attempt+1}): {e}"
                    )
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise
            
            # Should never reach here, but just in case
            raise Exception(f"Failed to fetch FRED data after {self.max_retries} attempts")
    
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

