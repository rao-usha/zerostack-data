"""
Census API client with rate limiting and retry logic.

This module handles all HTTP communication with Census APIs.
STEP 2: Structure only - HTTP calls implemented in STEP 4.
"""
import asyncio
import logging
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


class CensusClient:
    """
    HTTP client for Census API with bounded concurrency and rate limiting.
    
    Responsibilities:
    - Build Census API URLs
    - Make HTTP requests with retry/backoff
    - Respect rate limits via semaphore
    - Handle API errors gracefully
    """
    
    # Census API base URLs
    BASE_URL = "https://api.census.gov/data"
    
    def __init__(
        self,
        api_key: str,
        max_concurrency: int = 4,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize Census API client.
        
        Args:
            api_key: Census API key
            max_concurrency: Maximum concurrent requests (bounded concurrency)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        self.api_key = api_key
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        
        # Semaphore for bounded concurrency - MANDATORY per GLOBAL RULES
        self.semaphore = asyncio.Semaphore(max_concurrency)
        
        logger.info(
            f"Initialized CensusClient with max_concurrency={max_concurrency}, "
            f"max_retries={max_retries}"
        )
    
    def build_metadata_url(self, survey: str, year: int, table_id: str) -> str:
        """
        Build URL for fetching table metadata (variables/schema).
        
        Example:
            https://api.census.gov/data/2023/acs/acs5/variables.json
        
        Args:
            survey: Survey type (e.g., "acs5")
            year: Survey year
            table_id: Table identifier (e.g., "B01001")
            
        Returns:
            Full URL for metadata endpoint
        """
        # Variables endpoint provides schema/metadata
        url = f"{self.BASE_URL}/{year}/acs/{survey}/variables.json"
        return url
    
    def build_data_url(
        self,
        survey: str,
        year: int,
        variables: List[str],
        geo_level: str,
        geo_filter: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Build URL for fetching actual data.
        
        Example:
            https://api.census.gov/data/2023/acs/acs5?get=NAME,B01001_001E&for=state:*&key=...
        
        Args:
            survey: Survey type (e.g., "acs5")
            year: Survey year
            variables: List of variable names to fetch
            geo_level: Geographic level (state, county, tract, etc.)
            geo_filter: Optional geographic filters (e.g., {"state": "06"})
            
        Returns:
            Full URL for data endpoint
        """
        base = f"{self.BASE_URL}/{year}/acs/{survey}"
        
        # Build query parameters
        params = {
            "get": ",".join(["NAME"] + variables),  # NAME gives us geo name
            "key": self.api_key
        }
        
        # Add geography specification
        if geo_filter:
            # Specific geography, e.g., for=county:*&in=state:06
            # The 'for' is the geo_level we want
            # The 'in' is the filter (parent geography)
            params["for"] = f"{geo_level}:*"
            # Build 'in' clause from filter
            in_parts = []
            for key, value in geo_filter.items():
                in_parts.append(f"{key}:{value}")
            if in_parts:
                params["in"] = " ".join(in_parts)
        else:
            # All geographies at this level, e.g., for=state:*
            params["for"] = f"{geo_level}:*"
        
        query_string = urlencode(params)
        return f"{base}?{query_string}"
    
    async def fetch_table_metadata(
        self,
        survey: str,
        year: int,
        table_id: str
    ) -> Dict[str, Any]:
        """
        Fetch metadata for a specific Census table.
        
        Returns dictionary containing:
        - variables: Dict of variable definitions
        - Each variable has: label, concept, predicateType (data type)
        
        Args:
            survey: Survey type (e.g., "acs5")
            year: Survey year
            table_id: Table identifier (e.g., "B01001")
            
        Returns:
            Metadata dictionary from Census API
            
        Raises:
            httpx.HTTPError: On API errors
        """
        import httpx
        import random
        
        url = self.build_metadata_url(survey, year, table_id)
        
        async with self.semaphore:  # Bounded concurrency
            for attempt in range(self.max_retries + 1):
                try:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.get(url)
                        response.raise_for_status()
                        return response.json()
                
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:  # Rate limited
                        # Respect Retry-After header if present
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            wait_time = float(retry_after)
                        else:
                            # Exponential backoff with jitter
                            wait_time = (self.backoff_factor ** attempt) + random.uniform(0, 1)
                        
                        logger.warning(f"Rate limited, waiting {wait_time:.2f}s before retry")
                        await asyncio.sleep(wait_time)
                        
                        if attempt < self.max_retries:
                            continue
                    
                    # Non-retryable error or max retries exceeded
                    logger.error(f"HTTP error fetching metadata: {e}")
                    raise
                
                except (httpx.RequestError, httpx.TimeoutException) as e:
                    if attempt < self.max_retries:
                        # Exponential backoff with jitter
                        wait_time = (self.backoff_factor ** attempt) + random.uniform(0, 1)
                        logger.warning(f"Request failed, retrying in {wait_time:.2f}s: {e}")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    logger.error(f"Failed to fetch metadata after {self.max_retries} retries: {e}")
                    raise
        
        raise Exception("Failed to fetch metadata")
    
    async def fetch_acs_data(
        self,
        survey: str,
        year: int,
        variables: List[str],
        geo_level: str,
        geo_filter: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch actual ACS data for specified variables and geography.
        
        Args:
            survey: Survey type (e.g., "acs5")
            year: Survey year
            variables: List of variable names to fetch
            geo_level: Geographic level (state, county, tract, etc.)
            geo_filter: Optional geographic filters
            
        Returns:
            List of data records as dictionaries
            
        Raises:
            httpx.HTTPError: On API errors
        """
        import httpx
        import random
        
        url = self.build_data_url(survey, year, variables, geo_level, geo_filter)
        
        async with self.semaphore:  # Bounded concurrency
            for attempt in range(self.max_retries + 1):
                try:
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        response = await client.get(url)
                        response.raise_for_status()
                        data = response.json()
                        
                        # Parse Census API response format
                        # First row is headers, remaining rows are data
                        if not data or len(data) < 2:
                            return []
                        
                        headers = data[0]
                        rows = data[1:]
                        
                        # Convert to list of dictionaries
                        result = []
                        for row in rows:
                            record = {}
                            for i, header in enumerate(headers):
                                value = row[i] if i < len(row) else None
                                record[header] = value
                            result.append(record)
                        
                        return result
                
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:  # Rate limited
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            wait_time = float(retry_after)
                        else:
                            wait_time = (self.backoff_factor ** attempt) + random.uniform(0, 1)
                        
                        logger.warning(f"Rate limited, waiting {wait_time:.2f}s before retry")
                        await asyncio.sleep(wait_time)
                        
                        if attempt < self.max_retries:
                            continue
                    
                    logger.error(f"HTTP error fetching data: {e}")
                    raise
                
                except (httpx.RequestError, httpx.TimeoutException) as e:
                    if attempt < self.max_retries:
                        wait_time = (self.backoff_factor ** attempt) + random.uniform(0, 1)
                        logger.warning(f"Request failed, retrying in {wait_time:.2f}s: {e}")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    logger.error(f"Failed to fetch data after {self.max_retries} retries: {e}")
                    raise
        
        raise Exception("Failed to fetch data")
    
    async def close(self):
        """
        Clean up resources.
        
        Currently no persistent resources to clean up (httpx clients are context-managed).
        """
        pass

