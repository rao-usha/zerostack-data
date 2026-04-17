"""
FERC Energy Filings API client via EIA electricity profiles.

Uses the EIA API v2 to retrieve state-level electricity data:
https://api.eia.gov/v2/electricity/state-electricity-profiles/summary

This serves as a reliable proxy for FERC energy data, providing
state-level generation, consumption, prices, and utility counts.

Requires EIA_API_KEY environment variable.

Rate limits:
- No documented strict rate limits with API key
- Respectful usage recommended (1-2 req/sec)
"""

import logging
import os
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, AuthenticationError

logger = logging.getLogger(__name__)


class FercEnergyClient(BaseAPIClient):
    """
    HTTP client for EIA electricity profile API with bounded concurrency.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "ferc_energy"
    BASE_URL = "https://api.eia.gov/v2"

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize EIA electricity profile API client.

        Args:
            api_key: EIA API key (defaults to EIA_API_KEY env var)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        self.eia_api_key = api_key or os.getenv("EIA_API_KEY", "")
        if not self.eia_api_key:
            logger.warning(
                "EIA_API_KEY not set. FERC energy ingestion will fail."
            )

        super().__init__(
            api_key=self.eia_api_key,
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
        """Check for EIA API-specific errors."""
        if isinstance(data, dict):
            # EIA v2 returns errors in a structured format
            error = data.get("error")
            if error:
                error_message = str(error)
                logger.warning(f"EIA API error for {resource_id}: {error_message}")

                if "invalid api_key" in error_message.lower():
                    return AuthenticationError(
                        message=f"EIA API authentication failed: {error_message}",
                        source=self.SOURCE_NAME,
                    )

                return FatalError(
                    message=f"EIA API error: {error_message}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )
        return None

    async def fetch_state_profiles(
        self,
        period: Optional[str] = None,
        state: Optional[str] = None,
        offset: int = 0,
        length: int = 5000,
    ) -> Dict[str, Any]:
        """
        Fetch state electricity profile summary data from EIA API.

        Args:
            period: Year filter (e.g., "2022")
            state: Two-letter state code filter (e.g., "TX")
            offset: Pagination offset
            length: Number of records per page

        Returns:
            Dict containing API response with state profile data

        Raises:
            APIError: On API errors after retries
        """
        url = (
            f"{self.BASE_URL}/electricity/"
            f"state-electricity-profiles/summary/data/"
        )

        params: Dict[str, Any] = {
            "api_key": self.eia_api_key,
            "data[]": [
                "total-consumption",
                "total-generation",
                "average-retail-price",
                "total-revenue",
                "total-number-of-utilities",
            ],
            "offset": offset,
            "length": length,
        }

        if period:
            params["facets[period][]"] = period
        if state:
            params["facets[stateid][]"] = state.upper()

        resource_id = f"eia_state_profiles_{period or 'all'}_{state or 'all'}"
        return await self.get(url, params=params, resource_id=resource_id)

    async def fetch_all_state_profiles(
        self,
        period: Optional[str] = None,
        state: Optional[str] = None,
        max_records: int = 50000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all state electricity profile records with automatic pagination.

        Args:
            period: Year filter (e.g., "2022")
            state: Two-letter state code filter
            max_records: Maximum total records to fetch

        Returns:
            List of all state profile records
        """
        all_records: List[Dict[str, Any]] = []
        offset = 0
        page_size = 5000

        while offset < max_records:
            logger.info(
                f"Fetching EIA state profiles offset={offset} "
                f"for period={period}"
            )

            response = await self.fetch_state_profiles(
                period=period,
                state=state,
                offset=offset,
                length=page_size,
            )

            # EIA v2 response: {"response": {"data": [...], "total": N}}
            resp_body = response.get("response", {})
            data_list = resp_body.get("data", [])

            if not data_list:
                logger.info(f"No more records at offset={offset}")
                break

            all_records.extend(data_list)
            logger.info(
                f"Fetched {len(data_list)} records (total: {len(all_records)})"
            )

            total = resp_body.get("total", 0)
            if len(all_records) >= total or len(data_list) < page_size:
                break

            offset += page_size

        logger.info(
            f"Total EIA state profile records fetched: {len(all_records)}"
        )
        return all_records
