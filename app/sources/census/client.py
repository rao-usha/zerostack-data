"""
Census API client with rate limiting and retry logic.

Official Census API documentation:
https://www.census.gov/data/developers/data-sets.html

The Census API provides access to:
- American Community Survey (ACS) 1-year and 5-year estimates
- Decennial Census
- Economic Census
- Population Estimates

Rate limits:
- 500 queries per day without API key
- Unlimited queries with API key (free registration)
"""
import logging
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class CensusClient(BaseAPIClient):
    """
    HTTP client for Census API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "census"
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
        if not api_key:
            raise ValueError(
                "CENSUS_API_KEY is required. "
                "Get a free key at: https://api.census.gov/data/key_signup.html"
            )

        config = get_api_config("census")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval()
        )

    def _add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add API key to request parameters."""
        params["key"] = self.api_key
        return params

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
        return f"{self.BASE_URL}/{year}/acs/{survey}/variables.json"

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

        params = {
            "get": ",".join(["NAME"] + variables),
            "key": self.api_key
        }

        if geo_filter:
            params["for"] = f"{geo_level}:*"
            in_parts = []
            for key, value in geo_filter.items():
                in_parts.append(f"{key}:{value}")
            if in_parts:
                params["in"] = " ".join(in_parts)
        else:
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

        Args:
            survey: Survey type (e.g., "acs5")
            year: Survey year
            table_id: Table identifier (e.g., "B01001")

        Returns:
            Metadata dictionary from Census API
        """
        # Use full URL for metadata endpoint
        url = f"{year}/acs/{survey}/variables.json"

        return await self.get(url, resource_id=f"metadata:{survey}:{year}:{table_id}")

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
        """
        url = f"{year}/acs/{survey}"

        params = {
            "get": ",".join(["NAME"] + variables),
        }

        if geo_filter:
            params["for"] = f"{geo_level}:*"
            in_parts = []
            for key, value in geo_filter.items():
                in_parts.append(f"{key}:{value}")
            if in_parts:
                params["in"] = " ".join(in_parts)
        else:
            params["for"] = f"{geo_level}:*"

        data = await self.get(
            url,
            params=params,
            resource_id=f"acs:{survey}:{year}:{geo_level}"
        )

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
