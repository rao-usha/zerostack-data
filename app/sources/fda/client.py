"""
openFDA Device Registration API client with rate limiting and retry logic.

Official openFDA API documentation:
https://open.fda.gov/apis/device/registrationlisting/

The openFDA API provides access to:
- Device registration and listing data
- Manufacturer/establishment information
- Product classification codes
- 510(k) clearance cross-references

Rate limits:
- Without API key: 40 requests/minute, 1,000 requests/day
- With API key (free): 240 requests/minute, 120,000 requests/day
- Max 100 results per request
- Max skip offset: 26,000 (use additional filters to partition larger sets)
"""

import logging
import os
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError

logger = logging.getLogger(__name__)

# openFDA skip limit — the API returns 404 beyond this offset
OPENFDA_MAX_SKIP = 26000


class OpenFDAClient(BaseAPIClient):
    """
    HTTP client for openFDA API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "fda"
    BASE_URL = "https://api.fda.gov/"

    # Pagination limits
    MAX_LIMIT = 100  # openFDA hard limit per request
    DEFAULT_LIMIT = 100

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize openFDA API client.

        Args:
            api_key: Optional openFDA API key (free registration at open.fda.gov)
            max_concurrency: Maximum concurrent requests (bounded concurrency)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        # Resolve API key from param or env
        resolved_key = api_key or os.getenv("OPENFDA_API_KEY")

        super().__init__(
            api_key=resolved_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=30.0,
            connect_timeout=10.0,
            # Slower without API key to stay within 40 req/min limit
            rate_limit_interval=0.5 if resolved_key else 2.0,
        )

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers."""
        return {
            "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
            "Accept": "application/json",
        }

    def _add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add API key to request parameters if available."""
        if self.api_key:
            params["api_key"] = self.api_key
        return params

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[FatalError]:
        """
        Check openFDA response for API-specific errors.

        openFDA returns errors as:
        {"error": {"code": "...", "message": "..."}}
        """
        if "error" in data:
            error = data["error"]
            if isinstance(error, dict):
                code = error.get("code", "UNKNOWN")
                message = error.get("message", "Unknown error")

                # NOT_FOUND is expected when a state has no registrations
                if code == "NOT_FOUND":
                    logger.debug(f"[{resource_id}] No results: {message}")
                    return None

                return FatalError(
                    message=f"openFDA error {code}: {message}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )
            else:
                return FatalError(
                    message=f"openFDA error: {error}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )
        return None

    async def search_device_registrations(
        self,
        state: Optional[str] = None,
        search_query: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
    ) -> Dict[str, Any]:
        """
        Search device registration and listing data.

        Args:
            state: US state code filter (e.g., "CA", "TX")
            search_query: Additional Lucene search query
            limit: Max results per request (capped at 100)
            skip: Pagination offset (max 26,000)

        Returns:
            Raw API response dict with meta and results keys.
            Returns empty results dict if no data found.
        """
        url = f"{self.BASE_URL}device/registrationlisting.json"
        params = {
            "limit": min(limit, self.MAX_LIMIT),
            "skip": skip,
        }

        # Build Lucene search string
        search_parts = []
        if state:
            search_parts.append(f'registration.state_code:"{state}"')
        if search_query:
            search_parts.append(search_query)
        if search_parts:
            params["search"] = " AND ".join(search_parts)

        resource_id = f"fda_devices_{state or 'all'}_{skip}"

        try:
            response = await self.get(url, params=params, resource_id=resource_id)
            return response
        except Exception as e:
            error_msg = str(e).lower()
            # openFDA returns 404 when no results match the query
            if "not_found" in error_msg or "no matches" in error_msg:
                logger.debug(f"No results for {resource_id}")
                return {"meta": {"results": {"total": 0}}, "results": []}
            raise

    async def fetch_all_for_state(
        self, state: str, search_query: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all device registrations for a given state, handling pagination.

        Paginates through results in batches of 100, respecting the
        openFDA skip limit of 26,000. For states with more registrations
        than the skip limit allows, logs a warning.

        Args:
            state: US state code (e.g., "CA")
            search_query: Additional Lucene search filter

        Returns:
            List of all registration result dicts for the state
        """
        all_results = []
        skip = 0

        # First request to get total count
        response = await self.search_device_registrations(
            state=state, search_query=search_query, limit=self.MAX_LIMIT, skip=0
        )

        results = response.get("results", [])
        if not results:
            logger.debug(f"No registrations found for state {state}")
            return []

        total = response.get("meta", {}).get("results", {}).get("total", 0)
        all_results.extend(results)
        skip += len(results)

        logger.info(
            f"State {state}: {total} total registrations, "
            f"fetched {len(results)} in first batch"
        )

        # Paginate through remaining results
        while skip < total and skip < OPENFDA_MAX_SKIP:
            response = await self.search_device_registrations(
                state=state,
                search_query=search_query,
                limit=self.MAX_LIMIT,
                skip=skip,
            )
            results = response.get("results", [])
            if not results:
                break

            all_results.extend(results)
            skip += len(results)
            logger.debug(f"State {state}: fetched {len(all_results)}/{total}")

        if total > OPENFDA_MAX_SKIP:
            logger.warning(
                f"State {state} has {total} registrations but openFDA "
                f"skip limit is {OPENFDA_MAX_SKIP}. Only {len(all_results)} "
                f"records retrieved. Use additional search filters to partition."
            )

        return all_results

    async def search_aesthetic_devices(
        self, state: Optional[str] = None, limit: int = 100, skip: int = 0
    ) -> Dict[str, Any]:
        """
        Search for aesthetic/MedSpa device registrations using known product codes.

        Builds a Lucene OR query across all aesthetic product codes defined
        in metadata.AESTHETIC_PRODUCT_CODES.

        Args:
            state: Optional US state code filter
            search_query: Additional search terms
            limit: Max results per request
            skip: Pagination offset

        Returns:
            Raw API response dict
        """
        from app.sources.fda.metadata import AESTHETIC_PRODUCT_CODES

        # Build OR query for all aesthetic product codes
        code_queries = [
            f'products.product_code:"{code}"'
            for code in AESTHETIC_PRODUCT_CODES.keys()
        ]
        aesthetic_query = "(" + " OR ".join(code_queries) + ")"

        return await self.search_device_registrations(
            state=state,
            search_query=aesthetic_query,
            limit=limit,
            skip=skip,
        )
