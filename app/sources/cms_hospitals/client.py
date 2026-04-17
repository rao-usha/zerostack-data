"""
CMS Hospital Provider Data API client.

CMS Provider Data API documentation:
https://data.cms.gov/provider-data/

Provides access to hospital quality ratings, ownership, services,
and domain-specific performance ratings. Data is paginated via
limit/offset query parameters.

Rate limits:
- No API key required
- No documented rate limits, but respectful usage recommended (1-2 req/sec)
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError

logger = logging.getLogger(__name__)


class CmsHospitalClient(BaseAPIClient):
    """
    HTTP client for CMS Provider Data API with bounded concurrency.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "cms_hospitals"
    BASE_URL = "https://data.cms.gov/provider-data/api/1"

    # Hospital General Information dataset identifier
    DATASET_ID = "xubh-q36u"

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize CMS Hospital API client.

        No API key required for CMS Provider Data.

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
        """Check for CMS-specific API errors."""
        if isinstance(data, dict):
            error = data.get("error") or data.get("message")
            if error and "results" not in data:
                logger.warning(f"CMS API error for {resource_id}: {error}")
                return FatalError(
                    message=f"CMS API error: {error}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )
        return None

    async def fetch_hospitals(
        self,
        limit: int = 500,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Fetch hospital provider records from CMS.

        Uses limit/offset pagination on the datastore query endpoint.

        Args:
            limit: Number of records per request (max 500)
            offset: Starting offset for pagination

        Returns:
            List of hospital records

        Raises:
            APIError: On API errors after retries
        """
        url = (
            f"{self.BASE_URL}/datastore/query/{self.DATASET_ID}/0"
        )
        params = {
            "limit": limit,
            "offset": offset,
        }

        resource_id = f"cms_hospitals_{offset}_{offset + limit}"
        logger.info(f"Fetching CMS hospitals offset={offset}, limit={limit}")

        response = await self.get(url, params=params, resource_id=resource_id)

        # CMS returns { "results": [...], "count": N, ... }
        if isinstance(response, dict):
            error = self._check_api_error(response, resource_id)
            if error:
                raise error
            return response.get("results", [])

        # If it's already a list
        if isinstance(response, list):
            return response

        return []

    async def fetch_all_hospitals(
        self,
        max_pages: int = 50,
        page_size: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all hospital records with automatic pagination.

        Iterates through pages until no more data is returned.

        Args:
            max_pages: Maximum number of pages to fetch (safety limit)
            page_size: Number of records per page

        Returns:
            List of all hospital records
        """
        all_records: List[Dict[str, Any]] = []

        for page in range(max_pages):
            offset = page * page_size
            logger.info(
                f"Fetching CMS hospitals page {page + 1} (offset={offset}, "
                f"total so far={len(all_records)})"
            )

            records = await self.fetch_hospitals(limit=page_size, offset=offset)

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
