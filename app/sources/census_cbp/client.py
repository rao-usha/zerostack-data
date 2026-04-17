"""
Census County Business Patterns (CBP) API client with rate limiting and retry logic.

Official Census CBP API documentation:
https://www.census.gov/data/developers/data-sets/cbp-nonemp-zbp.html

Census CBP provides establishment, employment, and payroll data:
- Number of establishments by NAICS industry
- Employee counts
- Annual payroll ($1000s)

Rate limits:
- No API key required (optional key for higher limits)
- Respectful usage recommended (1-2 req/sec)
"""

import logging
import os
from typing import Dict, List, Any, Optional

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, RetryableError

logger = logging.getLogger(__name__)


class CensusCBPClient(BaseAPIClient):
    """
    HTTP client for Census County Business Patterns API.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "census_cbp"
    BASE_URL = "https://api.census.gov/data"

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize Census CBP API client.

        API key is optional but allows higher rate limits.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        api_key = os.environ.get("CENSUS_SURVEY_API_KEY", "") or None
        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=60.0,
            connect_timeout=15.0,
            rate_limit_interval=1.0,
        )

    def _check_api_error(
        self, data: Any, resource_id: str
    ) -> Optional[Exception]:
        """Check for Census API-specific errors."""
        if isinstance(data, dict):
            error = data.get("error") or data.get("message")
            if error:
                error_message = str(error)
                logger.warning(
                    f"Census CBP API error for {resource_id}: {error_message}"
                )
                return FatalError(
                    message=f"Census CBP API error: {error_message}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )
        return None

    async def fetch_business_patterns(
        self,
        year: int = 2022,
        state_fips: Optional[str] = None,
        naics_code: Optional[str] = None,
    ) -> List[List[str]]:
        """
        Fetch County Business Patterns data from Census API.

        Census API returns a JSON array where the first row is column headers
        and subsequent rows are data values.

        Args:
            year: Data year (e.g., 2022)
            state_fips: Optional specific state FIPS code (default: all states)
            naics_code: Optional NAICS code filter (default: all industries)

        Returns:
            List of lists (first row = headers, rest = data)

        Raises:
            APIError: On API errors after retries
        """
        url = f"{self.BASE_URL}/{year}/cbp"
        params: Dict[str, Any] = {
            "get": "ESTAB,EMP,PAYANN,NAICS2017_LABEL",
        }

        if state_fips:
            params["for"] = f"state:{state_fips}"
        else:
            params["for"] = "state:*"

        if naics_code:
            params["NAICS2017"] = naics_code
        else:
            params["NAICS2017"] = "*"

        if self.api_key:
            params["key"] = self.api_key

        resource_id = f"cbp_{year}_{state_fips or 'all'}_{naics_code or 'all'}"

        logger.info(
            f"Fetching Census CBP data: year={year}, "
            f"state={state_fips or 'all'}, naics={naics_code or 'all'}"
        )

        response = await self.get(url, params=params, resource_id=resource_id)

        # Census API returns a list of lists directly
        if isinstance(response, list):
            logger.info(
                f"Census CBP returned {len(response) - 1} data rows "
                f"(plus header row)"
            )
            return response

        # Unexpected response format
        logger.warning(
            f"Unexpected Census CBP response type: {type(response)}"
        )
        return []
