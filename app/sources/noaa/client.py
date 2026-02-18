"""
NOAA API client with rate limiting and retry logic.

Official NOAA NCEI Climate Data Online (CDO) API documentation:
https://www.ncdc.noaa.gov/cdo-web/webservices/v2

API Requirements:
- Token required (free, register at https://www.ncdc.noaa.gov/cdo-web/token)
- Rate limit: 5 requests per second, 10,000 requests per day
- All requests require 'token' header
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import date

from app.core.http_client import BaseAPIClient
from app.core.api_errors import RetryableError
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class NOAAClient(BaseAPIClient):
    """
    HTTP client for NOAA NCEI CDO API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "noaa"
    BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"

    def __init__(
        self,
        token: str,
        max_concurrency: int = 3,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize NOAA CDO API client.

        Args:
            token: NOAA CDO API token (get from https://www.ncdc.noaa.gov/cdo-web/token)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        if not token:
            raise ValueError(
                "NOAA_API_TOKEN is required. "
                "Get a free token at: https://www.ncdc.noaa.gov/cdo-web/token"
            )

        config = get_api_config("noaa")

        super().__init__(
            api_key=token,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=0.25,  # 4 req/sec to stay under 5 req/sec limit
        )

        self.token = token

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers with token."""
        return {
            "token": self.token,
            "Accept": "application/json",
            "User-Agent": "Nexdata/noaa-client",
        }

    async def get_datasets(self) -> List[Dict[str, Any]]:
        """Fetch available datasets from NOAA CDO API."""
        logger.info("Fetching NOAA datasets")
        result = await self.get(
            "datasets", params={"limit": 1000}, resource_id="datasets"
        )
        return result.get("results", [])

    async def get_data_types(
        self, dataset_id: str, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Fetch available data types for a dataset."""
        logger.info(f"Fetching data types for dataset: {dataset_id}")
        result = await self.get(
            "datatypes",
            params={"datasetid": dataset_id, "limit": limit},
            resource_id=f"datatypes:{dataset_id}",
        )
        return result.get("results", [])

    async def get_locations(
        self,
        dataset_id: str,
        location_category_id: Optional[str] = None,
        limit: int = 1000,
        offset: int = 1,
    ) -> List[Dict[str, Any]]:
        """Fetch locations (geographic areas) for a dataset."""
        params = {"datasetid": dataset_id, "limit": limit, "offset": offset}
        if location_category_id:
            params["locationcategoryid"] = location_category_id

        logger.info(f"Fetching locations for dataset: {dataset_id}")
        result = await self.get(
            "locations", params=params, resource_id=f"locations:{dataset_id}"
        )
        return result.get("results", [])

    async def get_stations(
        self,
        dataset_id: str,
        location_id: Optional[str] = None,
        limit: int = 1000,
        offset: int = 1,
    ) -> List[Dict[str, Any]]:
        """Fetch weather stations for a dataset and location."""
        params = {"datasetid": dataset_id, "limit": limit, "offset": offset}
        if location_id:
            params["locationid"] = location_id

        logger.info(f"Fetching stations for dataset: {dataset_id}")
        result = await self.get(
            "stations", params=params, resource_id=f"stations:{dataset_id}"
        )
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
        include_metadata: bool = True,
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
            "includemetadata": str(include_metadata).lower(),
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
        return await self.get("data", params=params, resource_id=f"data:{dataset_id}")

    async def get_all_data_paginated(
        self,
        dataset_id: str,
        start_date: date,
        end_date: date,
        data_type_ids: Optional[List[str]] = None,
        location_id: Optional[str] = None,
        station_id: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all data with automatic pagination."""
        all_data = []
        offset = 1
        limit = 1000

        while True:
            result = await self.get_data(
                dataset_id=dataset_id,
                start_date=start_date,
                end_date=end_date,
                data_type_ids=data_type_ids,
                location_id=location_id,
                station_id=station_id,
                limit=limit,
                offset=offset,
            )

            results = result.get("results", [])
            if not results:
                break

            all_data.extend(results)

            if max_results and len(all_data) >= max_results:
                all_data = all_data[:max_results]
                break

            metadata = result.get("metadata", {})
            result_set = metadata.get("resultset", {})
            count = result_set.get("count", 0)

            if len(results) < limit or offset + len(results) >= count:
                break

            offset += len(results)
            logger.info(
                f"Fetched {len(all_data)} records so far (offset={offset}, total={count})"
            )

        logger.info(f"Fetched total of {len(all_data)} records")
        return all_data
