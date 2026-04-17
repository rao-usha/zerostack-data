"""
DOT Infrastructure Grants API client (via USAspending.gov).

USAspending API documentation:
https://api.usaspending.gov/

Provides access to Department of Transportation grant spending data
aggregated by state. Uses the spending_by_geography POST endpoint
with agency and time period filters.

Rate limits:
- No API key required
- No documented rate limits, but respectful usage recommended (1-2 req/sec)
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError

logger = logging.getLogger(__name__)


class DotGrantsClient(BaseAPIClient):
    """
    HTTP client for USAspending.gov API with bounded concurrency.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "dot_grants"
    BASE_URL = "https://api.usaspending.gov/api/v2"

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize DOT Grants API client.

        No API key required for USAspending.

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
            timeout=90.0,
            connect_timeout=15.0,
            rate_limit_interval=1.0,
        )

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        """Check for USAspending-specific API errors."""
        if isinstance(data, dict):
            # USAspending returns {"detail": "..."} on errors
            detail = data.get("detail")
            if detail and "results" not in data:
                logger.warning(f"USAspending API error for {resource_id}: {detail}")
                return FatalError(
                    message=f"USAspending API error: {detail}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )
        return None

    async def fetch_grants_by_state(
        self,
        agency: str = "Department of Transportation",
        start_date: str = "2021-01-01",
        end_date: str = "2026-12-31",
    ) -> List[Dict[str, Any]]:
        """
        Fetch DOT grant spending aggregated by state.

        Uses the spending_by_geography POST endpoint with state-level
        aggregation filtered by agency and time period.

        Args:
            agency: Top-tier awarding agency name
            start_date: Start date for time period filter (YYYY-MM-DD)
            end_date: End date for time period filter (YYYY-MM-DD)

        Returns:
            List of state-level spending records

        Raises:
            APIError: On API errors after retries
        """
        url = f"{self.BASE_URL}/search/spending_by_geography/"

        payload = {
            "scope": "place_of_performance",
            "geo_layer": "state",
            "filters": {
                "agencies": [
                    {
                        "type": "awarding",
                        "tier": "toptier",
                        "name": agency,
                    }
                ],
                "time_period": [
                    {
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                ],
            },
        }

        resource_id = f"dot_grants_{agency}_{start_date}_{end_date}"
        logger.info(
            f"Fetching DOT grants by state: agency={agency}, "
            f"period={start_date} to {end_date}"
        )

        response = await self.post(url, json_body=payload, resource_id=resource_id)

        if isinstance(response, dict):
            error = self._check_api_error(response, resource_id)
            if error:
                raise error
            return response.get("results", [])

        if isinstance(response, list):
            return response

        return []

    async def fetch_grants_by_year(
        self,
        agency: str = "Department of Transportation",
        start_year: int = 2021,
        end_year: int = 2026,
    ) -> List[Dict[str, Any]]:
        """
        Fetch DOT grant spending by state for each fiscal year.

        Makes one API call per fiscal year to get year-level granularity.

        Args:
            agency: Top-tier awarding agency name
            start_year: First fiscal year
            end_year: Last fiscal year (inclusive)

        Returns:
            List of state-level spending records with fiscal_year attached
        """
        all_records: List[Dict[str, Any]] = []

        for year in range(start_year, end_year + 1):
            fy_start = f"{year}-01-01"
            fy_end = f"{year}-12-31"

            logger.info(f"Fetching DOT grants for fiscal year {year}")

            records = await self.fetch_grants_by_state(
                agency=agency,
                start_date=fy_start,
                end_date=fy_end,
            )

            # Attach fiscal year to each record
            for record in records:
                record["fiscal_year"] = year
                record["agency"] = agency

            all_records.extend(records)
            logger.info(
                f"FY{year}: {len(records)} state records "
                f"(total so far: {len(all_records)})"
            )

        return all_records
