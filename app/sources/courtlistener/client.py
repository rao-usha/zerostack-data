"""
CourtListener REST API v4 client.

Official API documentation:
https://www.courtlistener.com/api/rest-info/

CourtListener (Free Law Project) provides access to:
- Federal court docket data
- Bankruptcy filings and case information
- Court opinions and documents

Rate limits:
- No API key required for basic access
- ~100 requests per minute (unauthenticated)
- Higher limits with authentication token
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, RetryableError, RateLimitError

logger = logging.getLogger(__name__)

# Bankruptcy court IDs in CourtListener
BANKRUPTCY_COURTS = [
    "almb", "alnb", "alsb",  # Alabama
    "akb",                     # Alaska
    "azb",                     # Arizona
    "areb", "arwb",            # Arkansas
    "cacb", "caeb", "canb", "casb",  # California
    "cob",                     # Colorado
    "ctb",                     # Connecticut
    "deb",                     # Delaware
    "dcb",                     # District of Columbia
    "flmb", "flnb", "flsb",   # Florida
    "gamb", "ganb", "gasb",    # Georgia
    "hib",                     # Hawaii
    "idb",                     # Idaho
    "ilcb", "ilnb", "ilsb",   # Illinois
    "innb", "insb",            # Indiana
    "ianb", "iasb",            # Iowa
    "ksb",                     # Kansas
    "kyeb", "kywb",            # Kentucky
    "laeb", "lamb", "lawb",    # Louisiana
    "meb",                     # Maine
    "mdb",                     # Maryland
    "mab",                     # Massachusetts
    "mieb", "miwb",            # Michigan
    "mnb",                     # Minnesota
    "msnb", "mssb",            # Mississippi
    "moeb", "mowb",            # Missouri
    "mtb",                     # Montana
    "neb",                     # Nebraska
    "nvb",                     # Nevada
    "nhb",                     # New Hampshire
    "njb",                     # New Jersey
    "nmb",                     # New Mexico
    "nyeb", "nynb", "nysb", "nywb",  # New York
    "nceb", "ncmb", "ncwb",   # North Carolina
    "ndb",                     # North Dakota
    "ohnb", "ohsb",            # Ohio
    "okeb", "oknb", "okwb",   # Oklahoma
    "orb",                     # Oregon
    "paeb", "pamb", "pawb",   # Pennsylvania
    "rib",                     # Rhode Island
    "scb",                     # South Carolina
    "sdb",                     # South Dakota
    "tneb", "tnmb", "tnwb",   # Tennessee
    "txeb", "txnb", "txsb", "txwb",  # Texas
    "utb",                     # Utah
    "vtb",                     # Vermont
    "vaeb", "vawb",            # Virginia
    "waeb", "wawb",            # Washington
    "wvnb", "wvsb",            # West Virginia
    "wieb", "wiwb",            # Wisconsin
    "wyb",                     # Wyoming
]


class CourtListenerClient(BaseAPIClient):
    """
    HTTP client for the CourtListener REST API v4.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "courtlistener"
    BASE_URL = "https://www.courtlistener.com/api/rest/v4"

    def __init__(
        self,
        api_token: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize CourtListener API client.

        Args:
            api_token: Optional authentication token for higher rate limits
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff multiplier
        """
        super().__init__(
            api_key=api_token,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=60.0,
            connect_timeout=15.0,
            # ~100 req/min unauthenticated = ~0.6s interval, use 1s for safety
            rate_limit_interval=1.0,
        )

        if api_token:
            logger.info("CourtListener client initialized with auth token")
        else:
            logger.info(
                "CourtListener client initialized without auth token "
                "(rate limited to ~100 req/min)"
            )

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers with optional auth token."""
        headers = {
            "Accept": "application/json",
            "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
        }
        if self.api_key:
            headers["Authorization"] = f"Token {self.api_key}"
        return headers

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        """Check for CourtListener-specific API errors."""
        if "detail" in data and isinstance(data["detail"], str):
            detail = data["detail"]

            if "throttl" in detail.lower() or "rate" in detail.lower():
                return RateLimitError(
                    message=f"CourtListener rate limited: {detail}",
                    source=self.SOURCE_NAME,
                    retry_after=60,
                )

            if "not found" in detail.lower():
                return FatalError(
                    message=f"CourtListener resource not found: {detail}",
                    source=self.SOURCE_NAME,
                    status_code=404,
                )

            return FatalError(
                message=f"CourtListener API error: {detail}",
                source=self.SOURCE_NAME,
                response_data=data,
            )

        return None

    async def search_dockets(
        self,
        query: Optional[str] = None,
        court: Optional[str] = None,
        filed_after: Optional[str] = None,
        filed_before: Optional[str] = None,
        type_filter: str = "d",
        page: int = 1,
    ) -> Dict[str, Any]:
        """
        Search for court dockets.

        Args:
            query: Search query string (case name, party, etc.)
            court: Court ID (e.g., "nysb" for NY Southern Bankruptcy)
            filed_after: Filter cases filed after this date (YYYY-MM-DD)
            filed_before: Filter cases filed before this date (YYYY-MM-DD)
            type_filter: Search type ("d" for dockets, "r" for RECAP)
            page: Page number (1-indexed)

        Returns:
            Dict with count, next, previous, and results
        """
        params: Dict[str, Any] = {
            "type": type_filter,
            "page": page,
        }

        if query:
            params["q"] = query
        if court:
            params["court"] = court
        if filed_after:
            params["filed_after"] = filed_after
        if filed_before:
            params["filed_before"] = filed_before

        resource_id = f"dockets:q={query},court={court},page={page}"
        return await self.get("search/", params=params, resource_id=resource_id)

    async def search_bankruptcy_dockets(
        self,
        query: Optional[str] = None,
        court: Optional[str] = None,
        filed_after: Optional[str] = None,
        filed_before: Optional[str] = None,
        max_pages: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Search for bankruptcy dockets across all pages.

        If no court is specified, searches across all bankruptcy courts.

        Args:
            query: Search query string
            court: Specific bankruptcy court ID (optional)
            filed_after: Filter cases filed after this date
            filed_before: Filter cases filed before this date
            max_pages: Maximum pages to fetch (safety limit)

        Returns:
            List of all docket results across pages
        """
        all_results = []
        page = 1

        # If a court is specified, validate it's a bankruptcy court
        if court and court not in BANKRUPTCY_COURTS:
            logger.warning(
                f"Court '{court}' is not a recognized bankruptcy court. "
                "Proceeding anyway."
            )

        while page <= max_pages:
            response = await self.search_dockets(
                query=query,
                court=court,
                filed_after=filed_after,
                filed_before=filed_before,
                type_filter="d",
                page=page,
            )

            results = response.get("results", [])
            total_count = response.get("count", 0)

            if not results:
                break

            all_results.extend(results)
            logger.info(
                f"CourtListener page {page}: fetched {len(results)} dockets "
                f"({len(all_results)}/{total_count} total)"
            )

            # Check if there are more pages
            if not response.get("next"):
                break

            page += 1

        logger.info(f"CourtListener search complete: {len(all_results)} total dockets")
        return all_results

    async def get_docket(self, docket_id: int) -> Dict[str, Any]:
        """
        Fetch a specific docket by ID.

        Args:
            docket_id: CourtListener docket ID

        Returns:
            Docket detail dict
        """
        return await self.get(
            f"dockets/{docket_id}/",
            resource_id=f"docket:{docket_id}",
        )
