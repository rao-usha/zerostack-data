"""
EPA Greenhouse Gas Reporting Program (GHGRP) API client.

Official EPA Envirofacts REST API documentation:
https://www.epa.gov/enviro/envirofacts-data-service-api

The GHGRP requires large facilities to report greenhouse gas emissions
annually. Data is accessed via the Envirofacts REST API with row-based
pagination (rows/{start}:{end}/JSON).

Rate limits:
- No API key required
- No documented rate limits, but respectful usage recommended (1-2 req/sec)
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError

logger = logging.getLogger(__name__)


class EpaGhgClient(BaseAPIClient):
    """
    HTTP client for EPA Envirofacts GHGRP API with bounded concurrency.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "epa_ghg"
    BASE_URL = "https://data.epa.gov/efservice"

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize EPA GHG API client.

        No API key required for EPA Envirofacts.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        super().__init__(
            api_key=None,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=60.0,
            connect_timeout=15.0,
            rate_limit_interval=1.0,
        )

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        """Check for EPA Envirofacts-specific API errors."""
        # Envirofacts returns an error message string when something goes wrong
        if isinstance(data, dict) and data.get("error"):
            error_message = data.get("error", "Unknown Envirofacts API error")
            logger.warning(f"Envirofacts API error for {resource_id}: {error_message}")
            return FatalError(
                message=f"Envirofacts API error: {error_message}",
                source=self.SOURCE_NAME,
                response_data=data,
            )
        return None

    async def fetch_facilities(
        self,
        offset: int = 0,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch GHGRP facility records from Envirofacts.

        Uses row-based pagination: rows/{start}:{end}/JSON

        Args:
            offset: Starting row index (0-based)
            limit: Number of rows to fetch (max 1000 per request)

        Returns:
            List of facility records

        Raises:
            APIError: On API errors after retries
        """
        end = offset + limit - 1
        url = f"{self.BASE_URL}/PUB_DIM_FACILITY/rows/{offset}:{end}/JSON"

        resource_id = f"ghg_facilities_{offset}_{end}"
        logger.info(f"Fetching GHGRP facilities rows {offset}:{end}")

        response = await self.get(url, resource_id=resource_id)

        # Envirofacts returns a JSON array directly
        if isinstance(response, list):
            return response

        # If wrapped in a dict, try to extract data
        if isinstance(response, dict):
            error = self._check_api_error(response, resource_id)
            if error:
                raise error
            return []

        return []

    async def fetch_all_facilities(
        self,
        max_pages: int = 200,
        page_size: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all GHGRP facility records with automatic pagination.

        Iterates through pages until no more data is returned.

        Args:
            max_pages: Maximum number of pages to fetch (safety limit)
            page_size: Number of rows per page

        Returns:
            List of all facility records
        """
        all_records: List[Dict[str, Any]] = []

        for page in range(max_pages):
            offset = page * page_size
            logger.info(
                f"Fetching GHGRP page {page + 1} (offset={offset}, "
                f"total so far={len(all_records)})"
            )

            records = await self.fetch_facilities(offset=offset, limit=page_size)

            if not records:
                logger.info(
                    f"No more records at offset {offset}. "
                    f"Total fetched: {len(all_records)}"
                )
                break

            all_records.extend(records)

            # If we got fewer than page_size, we've reached the end
            if len(records) < page_size:
                logger.info(
                    f"Last page reached (got {len(records)} < {page_size}). "
                    f"Total fetched: {len(all_records)}"
                )
                break

        return all_records
