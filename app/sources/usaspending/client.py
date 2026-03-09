"""
USAspending.gov API client with rate limiting and retry logic.

Official USAspending API documentation:
https://api.usaspending.gov/

USAspending API provides access to federal spending data:
- Award-level contract and grant data
- Recipient information (name, UEI)
- Place of performance details
- NAICS code classification
- Agency and sub-agency information

Rate limits:
- No API key required
- POST-based search API
- Generous limits but responses can be slow
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


# Fields to request from the USAspending API
AWARD_FIELDS = [
    "Award ID",
    "Recipient Name",
    "Recipient UEI",
    "Award Amount",
    "Total Obligation",
    "NAICS Code",
    "NAICS Description",
    "Awarding Agency",
    "Place of Performance City Code",
    "Place of Performance State Code",
    "Place of Performance Zip5",
    "Period of Performance Start Date",
    "Period of Performance Current End Date",
    "Award Type",
    "Description",
]


# NAICS codes of interest for Nexdata verticals
# USAspending API only accepts codes with lengths 2, 4, or 6
NAICS_CODES_OF_INTEREST = {
    "62": "Health Care and Social Assistance (includes hospitals, ambulatory)",
    "518210": "Data Processing, Hosting, and Related Services (Colocation)",
    "517311": "Wired Telecommunications Carriers",
    "54": "Professional, Scientific, and Technical Services",
    "2381": "Foundation, Structure, and Building Exterior Contractors",
    "2382": "Building Equipment Contractors",
    "2383": "Building Finishing Contractors",
    "2389": "Other Specialty Trade Contractors",
}


# USAspending award type codes
AWARD_TYPE_CODES = {
    "contracts": ["A", "B", "C", "D"],
    "grants": ["02", "03", "04", "05"],
    "direct_payments": ["06", "10"],
    "loans": ["07", "08"],
    "other": ["09", "11"],
}


class USASpendingClient(BaseAPIClient):
    """
    HTTP client for USAspending.gov API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    Uses POST-based search endpoints (not GET).
    """

    SOURCE_NAME = "usaspending"
    BASE_URL = "https://api.usaspending.gov/api/v2/"

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize USAspending API client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("usaspending")

        super().__init__(
            api_key=None,  # No API key required
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

    async def search_awards(
        self,
        filters: Dict[str, Any],
        page: int = 1,
        limit: int = 100,
        sort: str = "Award Amount",
        order: str = "desc",
    ) -> Dict[str, Any]:
        """
        Search federal awards by filters.

        Endpoint: POST /search/spending_by_award/

        Args:
            filters: USAspending filter object (naics_codes, time_period, etc.)
            page: Page number (1-indexed)
            limit: Results per page (max 100)
            sort: Sort field name
            order: Sort order ("asc" or "desc")

        Returns:
            Dict containing API response with results and pagination info
        """
        url = f"{self.BASE_URL}search/spending_by_award/"
        body = {
            "filters": filters,
            "fields": AWARD_FIELDS,
            "page": page,
            "limit": limit,
            "sort": sort,
            "order": order,
        }

        return await self.post(
            url,
            json_body=body,
            resource_id=f"awards_page_{page}",
        )

    async def search_awards_by_naics(
        self,
        naics_codes: List[str],
        states: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        award_type_codes: Optional[List[str]] = None,
        min_amount: Optional[float] = None,
        page: int = 1,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Search awards by NAICS codes with optional location and date filters.

        Args:
            naics_codes: List of NAICS codes (e.g., ["621", "622"])
            states: Optional list of state codes (e.g., ["TX", "CA"])
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            award_type_codes: Optional award type codes (default: contracts)
            min_amount: Minimum award amount filter
            page: Page number
            limit: Results per page (max 100)

        Returns:
            Dict containing API response
        """
        filters = self._build_filters(
            naics_codes=naics_codes,
            states=states,
            start_date=start_date,
            end_date=end_date,
            award_type_codes=award_type_codes,
            min_amount=min_amount,
        )

        return await self.search_awards(filters=filters, page=page, limit=limit)

    @staticmethod
    def _build_filters(
        naics_codes: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        award_type_codes: Optional[List[str]] = None,
        min_amount: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Build a USAspending filters object.

        Args:
            naics_codes: List of NAICS codes
            states: List of state abbreviation codes
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            award_type_codes: Award type codes
            min_amount: Minimum award amount

        Returns:
            Filters dict for the USAspending API
        """
        filters: Dict[str, Any] = {}

        if naics_codes:
            valid_lengths = {2, 4, 6}
            valid_codes = [c for c in naics_codes if len(c) in valid_lengths]
            invalid_codes = [c for c in naics_codes if len(c) not in valid_lengths]
            if invalid_codes:
                logger.warning(
                    f"Dropping NAICS codes with invalid lengths (must be 2, 4, or 6): {invalid_codes}"
                )
            if valid_codes:
                filters["naics_codes"] = valid_codes

        if states:
            filters["place_of_performance_locations"] = [
                {"country": "USA", "state": state} for state in states
            ]

        if start_date or end_date:
            time_period = {}
            if start_date:
                time_period["start_date"] = start_date
            if end_date:
                time_period["end_date"] = end_date
            filters["time_period"] = [time_period]

        if award_type_codes:
            filters["award_type_codes"] = award_type_codes
        else:
            # Default to contracts
            filters["award_type_codes"] = AWARD_TYPE_CODES["contracts"]

        if min_amount is not None:
            filters["award_amounts"] = [
                {"lower_bound": min_amount}
            ]

        return filters
