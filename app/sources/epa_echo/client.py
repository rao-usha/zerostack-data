"""
EPA ECHO API client with rate limiting and retry logic.

Official EPA ECHO API documentation:
https://echo.epa.gov/tools/web-services

EPA ECHO provides access to facility-level compliance and enforcement data:
- Facility search by geography, industry, and media program
- Detailed facility reports (DFR)
- Compliance and violation history
- Inspection and penalty records

Rate limits:
- No API key required
- No documented rate limits, but respectful usage recommended (1-2 req/sec)
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, RetryableError

logger = logging.getLogger(__name__)

# US states and territories for full ingestion
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
]


class EPAECHOClient(BaseAPIClient):
    """
    HTTP client for EPA ECHO API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "epa_echo"
    BASE_URL = "https://echodata.epa.gov/echo/"

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize EPA ECHO API client.

        No API key required for EPA ECHO.

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
        """Check for EPA ECHO-specific API errors."""
        # ECHO API wraps errors in a Results object
        results = data.get("Results", {})

        if isinstance(results, dict):
            error = results.get("Error", {})
            if error:
                error_message = error.get("ErrorMessage", "Unknown ECHO API error")
                logger.warning(f"ECHO API error for {resource_id}: {error_message}")
                return FatalError(
                    message=f"ECHO API error: {error_message}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )

        return None

    async def search_facilities(
        self,
        state: Optional[str] = None,
        naics: Optional[str] = None,
        sic: Optional[str] = None,
        zip_code: Optional[str] = None,
        facility_name: Optional[str] = None,
        media: Optional[str] = None,
        rows_per_page: int = 5000,
        page_number: int = 1,
    ) -> Dict[str, Any]:
        """
        Search for facilities in the ECHO database.

        ECHO uses a two-step process:
        1. get_facilities — returns a QueryID (no facility data)
        2. get_qid — fetches actual facility rows using that QueryID

        Args:
            state: Two-letter state code (e.g., "TX", "CA")
            naics: NAICS code filter
            sic: SIC code filter
            zip_code: ZIP code filter
            facility_name: Facility name filter (partial match)
            media: Media program filter (AIR, WATER, RCRA, ALL)
            rows_per_page: Number of results per page (max 5000)
            page_number: Page number for pagination

        Returns:
            Dict containing API response with facility data

        Raises:
            APIError: On API errors after retries
        """
        # Step 1: Get QueryID
        url = f"{self.BASE_URL}echo_rest_services.get_facilities"
        params: Dict[str, Any] = {
            "output": "JSON",
            "p_act": "Y",  # Active facilities only
        }

        if state:
            params["p_st"] = state.upper()
        if naics:
            params["p_naics"] = naics
        if sic:
            params["p_sic"] = sic
        if zip_code:
            params["p_zip"] = zip_code
        if facility_name:
            params["p_fac_name"] = facility_name
        if media:
            params["p_med"] = media.upper()

        resource_id = f"echo_search_{state or 'all'}_{media or 'all'}"
        init_response = await self.get(url, params=params, resource_id=resource_id)

        # Extract QueryID from initial response
        results = init_response.get("Results", {})
        query_id = results.get("QueryID")
        if not query_id:
            logger.warning(f"No QueryID in ECHO response for {resource_id}")
            return init_response

        total_rows = int(results.get("QueryRows", "0"))
        logger.info(
            f"ECHO query for {resource_id}: QueryID={query_id}, total={total_rows}"
        )

        # Step 2: Fetch facility data using QueryID
        qid_url = f"{self.BASE_URL}echo_rest_services.get_qid"
        qid_params: Dict[str, Any] = {
            "output": "JSON",
            "qid": query_id,
            "pageno": page_number,
            "responseset": rows_per_page,
        }

        return await self.get(
            qid_url, params=qid_params, resource_id=f"echo_qid_{query_id}_p{page_number}"
        )

    async def get_facility_info(
        self,
        registry_id: str,
    ) -> Dict[str, Any]:
        """
        Get detailed facility information from ECHO DFR.

        Uses the dfr_rest_services.get_facility_info endpoint.

        Args:
            registry_id: EPA facility registry ID

        Returns:
            Dict containing detailed facility information

        Raises:
            APIError: On API errors after retries
        """
        url = f"{self.BASE_URL}dfr_rest_services.get_facility_info"
        params: Dict[str, Any] = {
            "output": "JSON",
            "p_id": registry_id,
        }

        return await self.get(
            url, params=params, resource_id=f"dfr_{registry_id}"
        )

    async def search_facilities_all_pages(
        self,
        state: Optional[str] = None,
        naics: Optional[str] = None,
        sic: Optional[str] = None,
        zip_code: Optional[str] = None,
        facility_name: Optional[str] = None,
        media: Optional[str] = None,
        max_pages: int = 100,
        rows_per_page: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Search facilities with automatic pagination.

        Uses the two-step ECHO process: get_facilities for QueryID,
        then get_qid for paginated facility data.

        Args:
            state: Two-letter state code
            naics: NAICS code filter
            sic: SIC code filter
            zip_code: ZIP code filter
            facility_name: Facility name filter
            media: Media program filter
            max_pages: Maximum number of pages to fetch
            rows_per_page: Results per page (max 5000)

        Returns:
            List of all facility records across pages
        """
        # Step 1: Get QueryID
        url = f"{self.BASE_URL}echo_rest_services.get_facilities"
        params: Dict[str, Any] = {
            "output": "JSON",
            "p_act": "Y",
        }
        if state:
            params["p_st"] = state.upper()
        if naics:
            params["p_naics"] = naics
        if sic:
            params["p_sic"] = sic
        if zip_code:
            params["p_zip"] = zip_code
        if facility_name:
            params["p_fac_name"] = facility_name
        if media:
            params["p_med"] = media.upper()

        resource_id = f"echo_search_{state or 'all'}_{media or 'all'}"
        init_response = await self.get(url, params=params, resource_id=resource_id)

        results = init_response.get("Results", {})
        query_id = results.get("QueryID")
        if not query_id:
            logger.warning(f"No QueryID in ECHO response for {resource_id}")
            return []

        total_rows = int(results.get("QueryRows", "0"))
        logger.info(
            f"ECHO query for {resource_id}: QueryID={query_id}, total={total_rows}"
        )

        if total_rows == 0:
            return []

        # Step 2: Paginate through get_qid
        all_facilities = []
        page = 1
        qid_url = f"{self.BASE_URL}echo_rest_services.get_qid"

        while page <= max_pages:
            logger.info(
                f"Fetching page {page} for state={state} (qid={query_id})"
            )

            qid_params: Dict[str, Any] = {
                "output": "JSON",
                "qid": query_id,
                "pageno": page,
                "responseset": rows_per_page,
            }

            response = await self.get(
                qid_url, params=qid_params,
                resource_id=f"echo_qid_{query_id}_p{page}",
            )

            page_results = response.get("Results", {})
            facilities = page_results.get("Facilities", [])

            if not facilities:
                logger.info(f"No more facilities on page {page}")
                break

            all_facilities.extend(facilities)

            if len(all_facilities) >= total_rows:
                logger.info(
                    f"Fetched all {len(all_facilities)} facilities for state={state}"
                )
                break

            page += 1

        return all_facilities
