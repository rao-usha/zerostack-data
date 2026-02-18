"""
SEC Form ADV API client.

Handles:
- Searching for investment advisers by name
- Downloading Form ADV data from SEC IAPD
- Downloading bulk CSV data files from SEC
- Parsing adviser information and contact details
"""

import asyncio
import logging
import random
from typing import Dict, List, Optional, Any
import httpx
import csv
import io
from datetime import datetime

logger = logging.getLogger(__name__)


class FormADVClient:
    """
    HTTP client for SEC Form ADV / IAPD data with rate limiting.

    Data sources:
    1. SEC IAPD API (individual adviser lookups)
    2. SEC bulk CSV downloads (batch processing)

    Note: The IAPD website doesn't have a well-documented public API,
    so we use a combination of approaches:
    - Search by firm name
    - Download bulk data files
    - Parse CSV data
    """

    # IAPD URLs
    IAPD_BASE_URL = "https://adviserinfo.sec.gov"
    IAPD_SEARCH_URL = f"{IAPD_BASE_URL}/api/search"
    IAPD_FIRM_URL = f"{IAPD_BASE_URL}/firm"

    # SEC bulk data URLs
    # Note: These URLs may change - verify at:
    # https://www.sec.gov/foia-services/frequently-requested-documents/form-adv-data
    SEC_DATA_BASE = "https://www.sec.gov/files"

    # Rate limit (be conservative with IAPD)
    DEFAULT_MAX_REQUESTS_PER_SECOND = 2
    DEFAULT_MAX_CONCURRENCY = 1

    # User-Agent is required
    USER_AGENT = (
        "Nexdata External Data Ingestion Service (contact: compliance@nexdata.com)"
    )

    def __init__(
        self,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_requests_per_second: float = DEFAULT_MAX_REQUESTS_PER_SECOND,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize Form ADV client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_requests_per_second: Rate limit
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff multiplier
        """
        self.max_concurrency = max_concurrency
        self.max_requests_per_second = max_requests_per_second
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        # Semaphore for bounded concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)

        # Rate limiting
        self.request_times: List[float] = []
        self.rate_limit_lock = asyncio.Lock()

        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None

        logger.info(
            f"Initialized FormADVClient: "
            f"max_concurrency={max_concurrency}, "
            f"max_requests_per_second={max_requests_per_second}"
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                headers={
                    "User-Agent": self.USER_AGENT,
                    "Accept": "application/json, text/csv",
                },
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _rate_limit(self):
        """Implement rate limiting using sliding window."""
        async with self.rate_limit_lock:
            import time

            now = time.time()

            # Remove requests older than 1 second
            self.request_times = [t for t in self.request_times if now - t < 1.0]

            # If we've hit the limit, wait
            if len(self.request_times) >= self.max_requests_per_second:
                oldest = self.request_times[0]
                wait_time = 1.0 - (now - oldest)

                if wait_time > 0:
                    logger.debug(f"Rate limit reached, waiting {wait_time:.3f}s")
                    await asyncio.sleep(wait_time)

                    now = time.time()
                    self.request_times = [
                        t for t in self.request_times if now - t < 1.0
                    ]

            # Record this request
            self.request_times.append(time.time())

    async def search_firms(
        self, firm_name: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Search for investment adviser firms by name.

        Args:
            firm_name: Firm name to search for
            limit: Maximum number of results

        Returns:
            List of firm search results with CRD numbers
        """
        logger.info(f"Searching for firm: {firm_name}")

        # Note: IAPD API structure may vary
        # This is based on reverse engineering the website
        # May need adjustment based on actual API response

        search_params = {"query": firm_name, "category": "firm", "limit": limit}

        try:
            data = await self._request_with_retry(
                "POST",
                self.IAPD_SEARCH_URL,
                json=search_params,
                identifier=f"search:{firm_name}",
            )

            # Parse search results
            results = []
            hits = data.get("hits", []) or data.get("results", [])

            for hit in hits[:limit]:
                result = {
                    "crd_number": hit.get("crdNumber") or hit.get("crd_number"),
                    "sec_number": hit.get("secNumber") or hit.get("sec_number"),
                    "firm_name": hit.get("firmName") or hit.get("name"),
                    "city": hit.get("city"),
                    "state": hit.get("state"),
                    "registration_status": hit.get("status"),
                }
                results.append(result)

            logger.info(f"Found {len(results)} firms matching '{firm_name}'")
            return results

        except Exception as e:
            logger.error(f"Error searching for firm '{firm_name}': {e}")
            # Return empty list rather than failing
            return []

    async def get_firm_details(self, crd_number: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a specific firm by CRD number.

        Args:
            crd_number: Firm CRD number

        Returns:
            Firm details including Form ADV data
        """
        logger.info(f"Fetching firm details for CRD {crd_number}")

        url = f"{self.IAPD_FIRM_URL}/{crd_number}"

        try:
            data = await self._request_with_retry(
                "GET", url, identifier=f"crd:{crd_number}"
            )

            return data

        except Exception as e:
            logger.error(f"Error fetching firm details for CRD {crd_number}: {e}")
            return None

    async def download_bulk_data(
        self, output_file: Optional[str] = None
    ) -> Optional[str]:
        """
        Download SEC Form ADV bulk CSV data file.

        Note: The exact URL for current bulk data may change.
        Check https://www.sec.gov/foia-services/frequently-requested-documents/form-adv-data

        Args:
            output_file: Optional path to save CSV file

        Returns:
            CSV content as string, or None on error
        """
        logger.info("Downloading Form ADV bulk data")

        # Note: This URL is a placeholder
        # Actual URL needs to be verified from SEC website
        # As of 2025, data is at adviserinfo.sec.gov/adv
        # Historical data may be in CSV format at different URLs

        # For now, log that bulk download needs manual configuration
        logger.warning(
            "Bulk data download requires manual URL configuration. "
            "Please visit https://www.sec.gov/foia-services/frequently-requested-documents/form-adv-data "
            "to get the current bulk data file URLs."
        )

        return None

    async def parse_bulk_csv(self, csv_content: str) -> List[Dict[str, Any]]:
        """
        Parse Form ADV bulk CSV data.

        Args:
            csv_content: CSV file content as string

        Returns:
            List of parsed firm records
        """
        logger.info("Parsing Form ADV bulk CSV data")

        firms = []

        try:
            csv_file = io.StringIO(csv_content)
            reader = csv.DictReader(csv_file)

            for row in reader:
                # Parse CSV row into standardized format
                # Note: Column names depend on actual SEC CSV format
                firm = {
                    "crd_number": row.get("CRD Number") or row.get("crd_number"),
                    "sec_number": row.get("SEC Number") or row.get("sec_number"),
                    "firm_name": row.get("Firm Name") or row.get("firm_name"),
                    "legal_name": row.get("Legal Name") or row.get("legal_name"),
                    "business_address_street1": row.get("Business Street 1"),
                    "business_address_street2": row.get("Business Street 2"),
                    "business_address_city": row.get("Business City"),
                    "business_address_state": row.get("Business State"),
                    "business_address_zip": row.get("Business Zip"),
                    "business_phone": row.get("Business Phone"),
                    "website": row.get("Website") or row.get("Web Address"),
                    "registration_status": row.get("Registration Status"),
                    "assets_under_management": row.get("AUM"),
                    # Add more fields as needed based on actual CSV structure
                }

                firms.append(firm)

            logger.info(f"Parsed {len(firms)} firms from CSV")
            return firms

        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            raise

    async def _request_with_retry(
        self, method: str, url: str, identifier: str, **kwargs
    ) -> Dict[str, Any]:
        """
        Make HTTP request with exponential backoff retry.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: API endpoint URL
            identifier: Identifier for logging
            **kwargs: Additional arguments for httpx request

        Returns:
            Parsed response data

        Raises:
            Exception: After all retries exhausted
        """
        async with self.semaphore:
            await self._rate_limit()

            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.debug(
                        f"Fetching Form ADV data for {identifier} "
                        f"(attempt {attempt+1}/{self.max_retries})"
                    )

                    response = await client.request(method, url, **kwargs)
                    response.raise_for_status()

                    # Try to parse as JSON
                    try:
                        data = response.json()
                        logger.debug(f"Successfully fetched data for {identifier}")
                        return data
                    except (ValueError, KeyError):
                        # If not JSON, return text
                        return {"content": response.text}

                except httpx.HTTPStatusError as e:
                    logger.warning(
                        f"HTTP error fetching Form ADV data (attempt {attempt+1}): "
                        f"{e.response.status_code}"
                    )

                    # 429 Rate limit - respect Retry-After
                    if e.response.status_code == 429:
                        retry_after = e.response.headers.get("Retry-After")
                        if retry_after:
                            wait_time = int(retry_after)
                            logger.warning(f"Rate limited. Waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue

                    # 404 Not Found - not retryable
                    if e.response.status_code == 404:
                        raise Exception(f"Form ADV data not found for {identifier}")

                    # Retry on 5xx
                    if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(
                            f"Form ADV API HTTP error: {e.response.status_code}"
                        )

                except httpx.RequestError as e:
                    logger.warning(f"Request error (attempt {attempt+1}): {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise Exception(f"Form ADV API request failed: {str(e)}")

                except Exception as e:
                    logger.error(f"Unexpected error (attempt {attempt+1}): {e}")
                    if attempt < self.max_retries - 1:
                        await self._backoff(attempt)
                    else:
                        raise

            raise Exception(
                f"Failed to fetch Form ADV data after {self.max_retries} attempts"
            )

    async def _backoff(self, attempt: int):
        """Exponential backoff with jitter."""
        base_delay = 1.0
        max_delay = 60.0

        delay = min(base_delay * (self.backoff_factor**attempt), max_delay)

        # Add jitter (±25%)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        delay_with_jitter = max(0.1, delay + jitter)

        logger.debug(f"Backing off for {delay_with_jitter:.2f}s")
        await asyncio.sleep(delay_with_jitter)


# Helper function to search for specific family offices
async def search_family_offices(
    client: FormADVClient, family_office_names: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Search for multiple family offices by name.

    Args:
        client: FormADVClient instance
        family_office_names: List of family office names to search

    Returns:
        Dict mapping family office name to search results
    """
    results = {}

    for name in family_office_names:
        try:
            search_results = await client.search_firms(name, limit=5)
            results[name] = search_results
        except Exception as e:
            logger.error(f"Error searching for {name}: {e}")
            results[name] = []

    return results
