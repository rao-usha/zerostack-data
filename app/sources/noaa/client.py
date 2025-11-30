"""
NOAA API client with rate limiting and retry logic.

Official NOAA NCEI Climate Data Online (CDO) API documentation:
https://www.ncdc.noaa.gov/cdo-web/webservices/v2

API Requirements:
- Token required (free, register at https://www.ncdc.noaa.gov/cdo-web/token)
- Rate limit: 5 requests per second, 10,000 requests per day
- All requests require 'token' header

Endpoints:
- /datasets - Available datasets
- /datacategories - Data categories
- /datatypes - Data types within datasets
- /locationcategories - Location categories
- /locations - Weather station locations
- /stations - Weather stations
- /data - Actual weather/climate data
"""
import asyncio
import logging
import random
from typing import Dict, List, Optional, Any
import httpx
from datetime import datetime, date

logger = logging.getLogger(__name__)


class NOAAClient:
    """
    HTTP client for NOAA NCEI CDO API with bounded concurrency and rate limiting.
    
    Responsibilities:
    - Make HTTP requests to NOAA CDO API
    - Implement retry logic with exponential backoff
    - Respect rate limits via semaphore (5 req/sec, 10k req/day)
    - Handle API errors gracefully
    """
    
    # NOAA CDO API base URL
    BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
    
    # Rate limit defaults (conservative to respect NOAA's 5 req/sec limit)
    DEFAULT_MAX_CONCURRENCY = 3
    DEFAULT_REQUESTS_PER_SECOND = 4  # Conservative: stay under 5 req/sec
    
    def __init__(
        self,
        token: str,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize NOAA CDO API client.
        
        Args:
            token: NOAA CDO API token (get from https://www.ncdc.noaa.gov/cdo-web/token)
            max_concurrency: Maximum concurrent requests
            requests_per_second: Rate limit (default 4 to stay under 5 req/sec)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        self.token = token
        self.max_concurrency = max_concurrency
        self.requests_per_second = requests_per_second
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        
        # Semaphore for bounded concurrency - MANDATORY per RULES
        self.semaphore = asyncio.Semaphore(max_concurrency)
        
        # Rate limiting: minimum time between requests
        self.min_request_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0
        self._rate_limit_lock = asyncio.Lock()
        
        # HTTP client (will be created in async context)
        self._client: Optional[httpx.AsyncClient] = None
        
        logger.info(
            f"Initialized NOAAClient: "
            f"max_concurrency={max_concurrency}, "
            f"requests_per_second={requests_per_second}, "
            f"max_retries={max_retries}"
        )
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client with proper headers."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                headers={
                    "token": self.token,
                    "Accept": "application/json"
                }
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _wait_for_rate_limit(self):
        """
        Enforce rate limiting by waiting if needed.
        Ensures we don't exceed requests_per_second.
        """
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self.last_request_time
            
            if time_since_last < self.min_request_interval:
                wait_time = self.min_request_interval - time_since_last
                await asyncio.sleep(wait_time)
            
            self.last_request_time = asyncio.get_event_loop().time()
    
    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """
        Make HTTP request to NOAA CDO API with retry logic.
        
        Args:
            endpoint: API endpoint (e.g., "datasets", "data")
            params: Query parameters
            retry_count: Current retry attempt
            
        Returns:
            JSON response as dictionary
            
        Raises:
            httpx.HTTPError: If request fails after all retries
        """
        url = f"{self.BASE_URL}/{endpoint}"
        
        async with self.semaphore:
            await self._wait_for_rate_limit()
            
            try:
                client = await self._get_client()
                response = await client.get(url, params=params)
                
                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    logger.warning(
                        f"Rate limited by NOAA API. "
                        f"Retry-After: {retry_after}s. Waiting..."
                    )
                    await asyncio.sleep(retry_after)
                    return await self._make_request(endpoint, params, retry_count)
                
                # Raise for bad status codes
                response.raise_for_status()
                
                return response.json()
                
            except httpx.HTTPError as e:
                if retry_count < self.max_retries:
                    # Exponential backoff with jitter
                    wait_time = (self.backoff_factor ** retry_count) + random.uniform(0, 1)
                    logger.warning(
                        f"Request failed (attempt {retry_count + 1}/{self.max_retries}): {e}. "
                        f"Retrying in {wait_time:.2f}s..."
                    )
                    await asyncio.sleep(wait_time)
                    return await self._make_request(endpoint, params, retry_count + 1)
                else:
                    logger.error(
                        f"Request failed after {self.max_retries} retries: {e}"
                    )
                    raise
    
    async def get_datasets(self) -> List[Dict[str, Any]]:
        """
        Fetch available datasets from NOAA CDO API.
        
        Returns:
            List of dataset metadata
        """
        logger.info("Fetching NOAA datasets")
        result = await self._make_request("datasets", params={"limit": 1000})
        return result.get("results", [])
    
    async def get_data_types(
        self,
        dataset_id: str,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fetch available data types for a dataset.
        
        Args:
            dataset_id: Dataset identifier (e.g., "GHCND" for daily summaries)
            limit: Maximum number of results
            
        Returns:
            List of data type metadata
        """
        logger.info(f"Fetching data types for dataset: {dataset_id}")
        result = await self._make_request(
            "datatypes",
            params={"datasetid": dataset_id, "limit": limit}
        )
        return result.get("results", [])
    
    async def get_locations(
        self,
        dataset_id: str,
        location_category_id: Optional[str] = None,
        limit: int = 1000,
        offset: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Fetch locations (geographic areas) for a dataset.
        
        Args:
            dataset_id: Dataset identifier
            location_category_id: Optional location category filter (e.g., "ST" for states)
            limit: Maximum number of results (max 1000 per request)
            offset: Pagination offset (1-indexed)
            
        Returns:
            List of location metadata
        """
        params = {
            "datasetid": dataset_id,
            "limit": limit,
            "offset": offset
        }
        if location_category_id:
            params["locationcategoryid"] = location_category_id
        
        logger.info(f"Fetching locations for dataset: {dataset_id}")
        result = await self._make_request("locations", params=params)
        return result.get("results", [])
    
    async def get_stations(
        self,
        dataset_id: str,
        location_id: Optional[str] = None,
        limit: int = 1000,
        offset: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Fetch weather stations for a dataset and location.
        
        Args:
            dataset_id: Dataset identifier
            location_id: Optional location filter (e.g., "FIPS:06" for California)
            limit: Maximum number of results (max 1000 per request)
            offset: Pagination offset (1-indexed)
            
        Returns:
            List of station metadata
        """
        params = {
            "datasetid": dataset_id,
            "limit": limit,
            "offset": offset
        }
        if location_id:
            params["locationid"] = location_id
        
        logger.info(f"Fetching stations for dataset: {dataset_id}")
        result = await self._make_request("stations", params=params)
        return result.get("results", [])
    
    async def get_data(
        self,
        dataset_id: str,
        start_date: date,
        end_date: date,
        data_type_ids: Optional[List[str]] = None,
        location_id: Optional[str] = None,
        station_id: Optional[str] = None,
        limit: int = 1000,
        offset: int = 1,
        units: str = "standard",
        include_metadata: bool = True
    ) -> Dict[str, Any]:
        """
        Fetch actual weather/climate data.
        
        Args:
            dataset_id: Dataset identifier (e.g., "GHCND")
            start_date: Start date for data
            end_date: End date for data
            data_type_ids: List of data type IDs to fetch (e.g., ["TMAX", "TMIN", "PRCP"])
            location_id: Optional location filter
            station_id: Optional station filter
            limit: Maximum number of results (max 1000 per request)
            offset: Pagination offset (1-indexed)
            units: "standard" or "metric"
            include_metadata: Include metadata flags in response
            
        Returns:
            Dictionary with 'results' (data) and 'metadata' (pagination info)
        """
        params = {
            "datasetid": dataset_id,
            "startdate": start_date.isoformat(),
            "enddate": end_date.isoformat(),
            "limit": limit,
            "offset": offset,
            "units": units,
            "includemetadata": str(include_metadata).lower()
        }
        
        if data_type_ids:
            params["datatypeid"] = ",".join(data_type_ids)
        if location_id:
            params["locationid"] = location_id
        if station_id:
            params["stationid"] = station_id
        
        logger.info(
            f"Fetching data: dataset={dataset_id}, "
            f"dates={start_date} to {end_date}, "
            f"types={data_type_ids}"
        )
        return await self._make_request("data", params=params)
    
    async def get_all_data_paginated(
        self,
        dataset_id: str,
        start_date: date,
        end_date: date,
        data_type_ids: Optional[List[str]] = None,
        location_id: Optional[str] = None,
        station_id: Optional[str] = None,
        max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all data with automatic pagination.
        
        Args:
            dataset_id: Dataset identifier
            start_date: Start date for data
            end_date: End date for data
            data_type_ids: List of data type IDs to fetch
            location_id: Optional location filter
            station_id: Optional station filter
            max_results: Optional maximum total results to fetch
            
        Returns:
            List of all data records
        """
        all_data = []
        offset = 1
        limit = 1000  # Max per request
        
        while True:
            result = await self.get_data(
                dataset_id=dataset_id,
                start_date=start_date,
                end_date=end_date,
                data_type_ids=data_type_ids,
                location_id=location_id,
                station_id=station_id,
                limit=limit,
                offset=offset
            )
            
            results = result.get("results", [])
            if not results:
                break
            
            all_data.extend(results)
            
            # Check if we've reached max_results
            if max_results and len(all_data) >= max_results:
                all_data = all_data[:max_results]
                break
            
            # Check if there are more results
            metadata = result.get("metadata", {})
            result_set = metadata.get("resultset", {})
            count = result_set.get("count", 0)
            
            if len(results) < limit or offset + len(results) >= count:
                break
            
            offset += len(results)
            
            logger.info(
                f"Fetched {len(all_data)} records so far "
                f"(offset={offset}, total={count})"
            )
        
        logger.info(f"Fetched total of {len(all_data)} records")
        return all_data






