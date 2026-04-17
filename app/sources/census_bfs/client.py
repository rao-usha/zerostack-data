"""
Census Business Formation Statistics (BFS) API client with rate limiting and retry logic.

Official Census BFS API documentation:
https://www.census.gov/data/developers/data-sets/business-formation-statistics.html

Census BFS provides business application data by state and time period:
- Total business applications
- High-propensity business applications
- Applications with planned wages
- Applications with first payroll

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


class CensusBFSClient(BaseAPIClient):
    """
    HTTP client for Census Business Formation Statistics API.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "census_bfs"
    BASE_URL = "https://api.census.gov/data/timeseries/eits/bfs"

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize Census BFS API client.

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
        # Census API returns error messages as plain strings or dicts
        if isinstance(data, dict):
            error = data.get("error") or data.get("message")
            if error:
                error_message = str(error)
                logger.warning(
                    f"Census BFS API error for {resource_id}: {error_message}"
                )
                return FatalError(
                    message=f"Census BFS API error: {error_message}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )
        return None

    async def fetch_business_formation(
        self,
        time_from: str = "2020",
        state_fips: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch business formation statistics from Census BFS API.

        The BFS timeseries API uses cell_value format with data_type_code
        to distinguish metrics. We fetch all 4 data types and pivot them
        into one row per time period.

        Args:
            time_from: Start year for time series (e.g., "2020")
            state_fips: Ignored — BFS only supports US-level geography

        Returns:
            List of dicts with keys: time_period, BA_BA, BA_WBA, BA_HBA, BA_CBA
        """
        data_types = ["BA_BA", "BA_WBA", "BA_HBA", "BA_CBA"]
        all_values: Dict[str, Dict[str, Any]] = {}  # time -> {metric: value}

        key_param = f"&key={self.api_key}" if self.api_key else ""

        for dt in data_types:
            url = (
                f"{self.BASE_URL}"
                f"?get=cell_value,time_slot_id,data_type_code"
                f"&for=us:*"
                f"&time=from+{time_from}"
                f"&category_code=TOTAL"
                f"&data_type_code={dt}"
                f"&seasonally_adj=yes"
                f"{key_param}"
            )

            resource_id = f"bfs_{dt}_{time_from}"
            logger.info(f"Fetching Census BFS {dt} from {time_from}")

            response = await self._request("GET", url, resource_id=resource_id)

            if isinstance(response, list) and len(response) > 1:
                for row in response[1:]:
                    time_period = row[3] if len(row) > 3 else None
                    cell_value = row[0] if row[0] else "0"
                    if time_period:
                        if time_period not in all_values:
                            all_values[time_period] = {"time_period": time_period}
                        try:
                            all_values[time_period][dt] = int(cell_value)
                        except (ValueError, TypeError):
                            all_values[time_period][dt] = 0

        result = sorted(all_values.values(), key=lambda x: x.get("time_period", ""))
        logger.info(f"Census BFS: {len(result)} time periods fetched with {len(data_types)} metrics")
        return result
