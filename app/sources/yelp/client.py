"""
Yelp Fusion API client with rate limiting and retry logic.

Official API documentation:
https://docs.developer.yelp.com/docs/fusion-intro

Yelp Fusion API provides access to:
- Business search (by location, category, keyword)
- Business details
- Business reviews (limited)
- Autocomplete
- Categories

Rate limits:
- Free tier: 500 API calls per day (new clients as of May 2023)
- Legacy clients: 5,000 API calls per day
- QPS rate limiting applies (HTTP 429 when exceeded)

API Key:
Required. Get at: https://www.yelp.com/developers/v3/manage_app
"""
import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class YelpClient(BaseAPIClient):
    """
    HTTP client for Yelp Fusion API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "yelp"
    BASE_URL = "https://api.yelp.com/v3"

    DEFAULT_DAILY_LIMIT = 500

    def __init__(
        self,
        api_key: str,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize Yelp Fusion API client.

        Args:
            api_key: Yelp Fusion API key (required)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        if not api_key:
            raise ValueError(
                "YELP_API_KEY is required. "
                "Get a free key at: https://www.yelp.com/developers/v3/manage_app"
            )

        config = get_api_config("yelp")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=0.5  # Conservative due to daily limits
        )

        self._daily_requests = 0

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers with Bearer token."""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }

    def _check_daily_limit(self):
        """Check if daily limit is approaching."""
        if self._daily_requests >= self.DEFAULT_DAILY_LIMIT:
            raise Exception(
                f"Daily API limit reached ({self.DEFAULT_DAILY_LIMIT} calls). "
                "Please wait until tomorrow or upgrade your API plan."
            )

    async def search_businesses(
        self,
        location: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        term: Optional[str] = None,
        categories: Optional[str] = None,
        radius: int = 10000,
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "best_match",
        price: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Search for businesses.

        Args:
            location: Location string (e.g., "San Francisco, CA")
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            term: Search term (e.g., "restaurants", "coffee")
            categories: Category filter (e.g., "restaurants,bars")
            radius: Search radius in meters (max 40000)
            limit: Number of results (max 50)
            offset: Pagination offset (max 1000)
            sort_by: Sorting mode (best_match, rating, review_count, distance)
            price: Price filter (1, 2, 3, 4 or combinations like "1,2")

        Returns:
            Dict containing businesses array and total count
        """
        self._check_daily_limit()

        params = {
            "limit": min(limit, 50),
            "offset": min(offset, 1000),
            "sort_by": sort_by,
        }

        if location:
            params["location"] = location
        elif latitude is not None and longitude is not None:
            params["latitude"] = latitude
            params["longitude"] = longitude
        else:
            raise ValueError("Either location or latitude/longitude is required")

        if term:
            params["term"] = term
        if categories:
            params["categories"] = categories
        if radius:
            params["radius"] = min(radius, 40000)
        if price:
            params["price"] = price

        result = await self.get(
            "businesses/search",
            params=params,
            resource_id=f"BusinessSearch:{location or f'{latitude},{longitude}'}"
        )

        self._daily_requests += 1
        return result

    async def get_business_details(self, business_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific business."""
        self._check_daily_limit()
        result = await self.get(
            f"businesses/{business_id}",
            resource_id=f"BusinessDetails:{business_id}"
        )
        self._daily_requests += 1
        return result

    async def get_business_reviews(
        self,
        business_id: str,
        locale: str = "en_US",
        offset: int = 0,
        limit: int = 3
    ) -> Dict[str, Any]:
        """Get reviews for a business (limited to 3 reviews on free tier)."""
        self._check_daily_limit()

        params = {
            "locale": locale,
            "offset": offset,
            "limit": min(limit, 3),
        }

        result = await self.get(
            f"businesses/{business_id}/reviews",
            params=params,
            resource_id=f"BusinessReviews:{business_id}"
        )

        self._daily_requests += 1
        return result

    async def get_all_categories(self, locale: str = "en_US") -> Dict[str, Any]:
        """Get all Yelp business categories."""
        self._check_daily_limit()
        result = await self.get(
            "categories",
            params={"locale": locale},
            resource_id="AllCategories"
        )
        self._daily_requests += 1
        return result

    async def autocomplete(
        self,
        text: str,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        locale: str = "en_US"
    ) -> Dict[str, Any]:
        """Get autocomplete suggestions."""
        self._check_daily_limit()

        params = {"text": text, "locale": locale}
        if latitude is not None:
            params["latitude"] = latitude
        if longitude is not None:
            params["longitude"] = longitude

        result = await self.get(
            "autocomplete",
            params=params,
            resource_id=f"Autocomplete:{text}"
        )

        self._daily_requests += 1
        return result

    async def search_by_phone(self, phone: str) -> Dict[str, Any]:
        """Search for a business by phone number (E.164 format)."""
        self._check_daily_limit()
        result = await self.get(
            "businesses/search/phone",
            params={"phone": phone},
            resource_id=f"PhoneSearch:{phone}"
        )
        self._daily_requests += 1
        return result


# Common business categories
YELP_CATEGORIES = {
    "restaurants": "Restaurants",
    "food": "Food",
    "bars": "Bars",
    "coffee": "Coffee & Tea",
    "shopping": "Shopping",
    "hotels": "Hotels",
    "health": "Health & Medical",
}

PRICE_LEVELS = {
    "1": "$ (Under $10)",
    "2": "$$ ($11-30)",
    "3": "$$$ ($31-60)",
    "4": "$$$$ (Above $61)",
}
