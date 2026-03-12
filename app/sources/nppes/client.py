"""
NPPES NPI Registry API client with rate limiting and retry logic.

Handles HTTP communication with the NPPES NPI Registry API.
No API key is required. The API returns up to 200 results per request.

API docs: https://npiregistry.cms.hhs.gov/api-page
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError

logger = logging.getLogger(__name__)


class NPPESClient(BaseAPIClient):
    """
    HTTP client for the NPPES NPI Registry API.

    Provides methods to search for healthcare providers by state,
    taxonomy code, city, postal code, and other criteria.

    The NPPES API is free, requires no authentication, and returns
    up to 200 results per request with skip-based pagination.
    """

    SOURCE_NAME = "nppes"
    BASE_URL = "https://npiregistry.cms.hhs.gov/api/"

    # NPPES-specific constants
    API_VERSION = "2.1"
    MAX_RESULTS_PER_PAGE = 200

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize NPPES API client.

        Args:
            max_concurrency: Maximum concurrent requests (bounded concurrency)
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        super().__init__(
            api_key=None,  # No API key needed
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=30.0,
            connect_timeout=10.0,
            rate_limit_interval=0.5,  # Be respectful — 2 req/sec max
        )

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[FatalError]:
        """
        Check NPPES response for API-specific errors.

        The NPPES API returns errors in the top-level "Errors" field.

        Args:
            data: Parsed JSON response
            resource_id: Resource being requested (for logging)

        Returns:
            FatalError if error detected, None otherwise
        """
        errors = data.get("Errors")
        if errors:
            # Errors is a list of dicts with "description" and "number" fields
            if isinstance(errors, list) and len(errors) > 0:
                error_descs = [e.get("description", str(e)) for e in errors]
                error_msg = "; ".join(error_descs)

                # Check if it's a "no results" error vs a real error
                for err in errors:
                    desc = err.get("description", "").lower()
                    if "no result" in desc or "no match" in desc or "no taxonomy codes" in desc:
                        logger.debug(
                            f"[nppes] No results for {resource_id}: {error_msg}"
                        )
                        return None  # Not an error, just empty results

                return FatalError(
                    message=f"NPPES API error: {error_msg}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )

        return None

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers for NPPES API."""
        return {
            "Accept": "application/json",
            "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
        }

    async def search_providers(
        self,
        state: Optional[str] = None,
        city: Optional[str] = None,
        postal_code: Optional[str] = None,
        taxonomy_description: Optional[str] = None,
        enumeration_type: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        organization_name: Optional[str] = None,
        limit: int = 200,
        skip: int = 0,
    ) -> Dict[str, Any]:
        """
        Search the NPPES NPI Registry.

        Args:
            state: Two-letter state abbreviation (e.g., "CA")
            city: City name
            postal_code: 5- or 9-digit ZIP code
            taxonomy_description: Taxonomy description to search (e.g., "Dermatology")
            enumeration_type: NPI-1 (individual) or NPI-2 (organization)
            first_name: Individual provider first name
            last_name: Individual provider last name
            organization_name: Organization name
            limit: Max results per page (max 200)
            skip: Number of results to skip (for pagination)

        Returns:
            API response dict with result_count and results
        """
        params = {
            "version": self.API_VERSION,
            "limit": min(limit, self.MAX_RESULTS_PER_PAGE),
            "skip": skip,
        }

        if state:
            params["state"] = state.upper()
        if city:
            params["city"] = city
        if postal_code:
            params["postal_code"] = postal_code
        if taxonomy_description:
            params["taxonomy_description"] = taxonomy_description
        if enumeration_type:
            params["enumeration_type"] = enumeration_type
        if first_name:
            params["first_name"] = first_name
        if last_name:
            params["last_name"] = last_name
        if organization_name:
            params["organization_name"] = organization_name

        resource_id = f"nppes_search(state={state}, taxonomy={taxonomy_description}, skip={skip})"

        return await self.get(
            url=self.BASE_URL,
            params=params,
            resource_id=resource_id,
        )

    async def search_all_pages(
        self,
        state: Optional[str] = None,
        city: Optional[str] = None,
        postal_code: Optional[str] = None,
        taxonomy_description: Optional[str] = None,
        enumeration_type: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        organization_name: Optional[str] = None,
        max_records: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search NPPES with automatic pagination.

        Iterates through all pages of results until no more data or
        max_records is reached.

        Args:
            state: Two-letter state abbreviation
            city: City name
            postal_code: ZIP code
            taxonomy_description: Taxonomy description
            enumeration_type: NPI-1 or NPI-2
            first_name: Provider first name
            last_name: Provider last name
            organization_name: Organization name
            max_records: Maximum total records to fetch (None = all)

        Returns:
            List of provider result dicts
        """
        all_results = []
        skip = 0

        while True:
            # Check if we've reached max_records
            if max_records and len(all_results) >= max_records:
                break

            # Adjust limit if approaching max_records
            page_limit = self.MAX_RESULTS_PER_PAGE
            if max_records:
                remaining = max_records - len(all_results)
                page_limit = min(page_limit, remaining)

            response = await self.search_providers(
                state=state,
                city=city,
                postal_code=postal_code,
                taxonomy_description=taxonomy_description,
                enumeration_type=enumeration_type,
                first_name=first_name,
                last_name=last_name,
                organization_name=organization_name,
                limit=page_limit,
                skip=skip,
            )

            result_count = response.get("result_count", 0)
            results = response.get("results", [])

            if not results:
                break

            all_results.extend(results)
            skip += len(results)

            logger.info(
                f"[nppes] Fetched {len(all_results)} of {result_count} total results"
            )

            # Check if we got fewer than requested (last page)
            if len(results) < page_limit:
                break

            # Safety: NPPES API caps at ~1200 results per query combination
            # If result_count > skip, there's more data
            if result_count <= skip:
                break

        logger.info(f"[nppes] Total records fetched: {len(all_results)}")
        return all_results

    async def lookup_npi(self, npi: str) -> Optional[Dict[str, Any]]:
        """
        Look up a specific NPI number.

        Args:
            npi: 10-digit NPI number

        Returns:
            Provider dict or None if not found
        """
        params = {
            "version": self.API_VERSION,
            "number": npi,
        }

        response = await self.get(
            url=self.BASE_URL,
            params=params,
            resource_id=f"nppes_npi_{npi}",
        )

        results = response.get("results", [])
        return results[0] if results else None
