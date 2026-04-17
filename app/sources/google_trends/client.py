"""
Google Trends API client with aggressive rate limiting.

Uses the unofficial Google Trends API endpoints:
- Daily trends: https://trends.google.com/trends/api/dailytrends
- Interest by region: https://trends.google.com/trends/api/widgetdata/comparedgeo

IMPORTANT: Google Trends heavily rate-limits and blocks automated access.
This client uses very conservative rate limiting (1 request per 5 seconds).
For production use, consider using the `pytrends` library as an alternative,
which handles session management and token rotation.

No API key required, but 429 errors are common and expected.
"""

import json
import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, RateLimitError

logger = logging.getLogger(__name__)


class GoogleTrendsClient(BaseAPIClient):
    """
    HTTP client for Google Trends API with aggressive rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.

    WARNING: Google Trends blocks automated access aggressively.
    This client may return empty results if rate-limited.
    Consider using pytrends as a fallback.
    """

    SOURCE_NAME = "google_trends"
    BASE_URL = "https://trends.google.com/trends/api"

    def __init__(
        self,
        max_concurrency: int = 1,
        max_retries: int = 3,
        backoff_factor: float = 5.0,
    ):
        """
        Initialize Google Trends API client.

        No API key required, but very slow rate limiting is enforced.

        Args:
            max_concurrency: Maximum concurrent requests (keep at 1)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier (high due to rate limits)
        """
        super().__init__(
            api_key=None,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=30.0,
            connect_timeout=10.0,
            rate_limit_interval=5.0,  # 1 request per 5 seconds
        )

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        """Check for Google Trends-specific API errors."""
        # Google Trends API prefixes JSON responses with ")]}'\n"
        # This is handled in _parse_trends_response
        if isinstance(data, dict) and data.get("status") == "error":
            error_msg = data.get("message", "Unknown Google Trends error")
            logger.warning(f"Google Trends error for {resource_id}: {error_msg}")
            return FatalError(
                message=f"Google Trends error: {error_msg}",
                source=self.SOURCE_NAME,
                response_data=data,
            )
        return None

    @staticmethod
    def _parse_trends_response(raw_text: str) -> Dict[str, Any]:
        """
        Parse Google Trends API response.

        Google Trends prefixes JSON responses with ")]}'" which must be stripped.

        Args:
            raw_text: Raw response text from Google Trends

        Returns:
            Parsed JSON dict
        """
        # Strip the anti-XSSI prefix
        if raw_text.startswith(")]}'"):
            raw_text = raw_text[5:]  # Remove ")]}'\n"

        try:
            return json.loads(raw_text)
        except json.JSONDecodeError:
            logger.warning("Failed to parse Google Trends response as JSON")
            return {}

    async def fetch_daily_trends(
        self,
        geo: str = "US",
        date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch daily trending searches from Google Trends.

        Args:
            geo: Geographic region code (e.g., "US", "GB")
            date: Date in YYYYMMDD format (defaults to today)

        Returns:
            Dict containing trending search data

        Raises:
            APIError: On API errors after retries
        """
        url = f"{self.BASE_URL}/dailytrends"
        params: Dict[str, Any] = {
            "hl": "en-US",
            "tz": "300",
            "geo": geo,
            "ns": "15",
        }
        if date:
            params["ed"] = date

        resource_id = f"daily_trends_{geo}_{date or 'today'}"

        # Use raw get since Google Trends returns non-standard JSON
        response = await self._request(
            method="GET",
            url=url,
            params=params,
            resource_id=resource_id,
        )
        return response

    async def fetch_interest_by_region(
        self,
        keyword: str,
        geo: str = "US",
        resolution: str = "REGION",
    ) -> Dict[str, Any]:
        """
        Fetch interest by region for a keyword.

        NOTE: This endpoint requires a session token that is normally
        obtained through the explore page. For reliable access,
        use the pytrends library instead.

        This method attempts direct access but may fail due to
        Google's anti-automation measures.

        Args:
            keyword: Search keyword to analyze
            geo: Geographic region code (e.g., "US")
            resolution: Resolution level ("REGION", "CITY", "DMA")

        Returns:
            Dict containing regional interest data

        Raises:
            APIError: On API errors after retries
        """
        url = f"{self.BASE_URL}/explore"
        params: Dict[str, Any] = {
            "hl": "en-US",
            "tz": "300",
            "req": json.dumps({
                "comparisonItem": [
                    {"keyword": keyword, "geo": geo, "time": "today 12-m"}
                ],
                "category": 0,
                "property": "",
            }),
        }

        resource_id = f"interest_region_{keyword}_{geo}"
        response = await self._request(
            method="GET",
            url=url,
            params=params,
            resource_id=resource_id,
        )
        return response
