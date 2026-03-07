"""
SAM.gov Entity Information API client.

Official API documentation:
https://open.gsa.gov/api/entity-api/

SAM.gov provides access to entity registration data including:
- Active federal contractor registrations
- NAICS codes and business types
- Physical addresses and contact information
- UEI (Unique Entity Identifier) and CAGE codes

Rate limits:
- Free API key required
- 1,000 requests per day
- Max 100 records per page
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, RetryableError

logger = logging.getLogger(__name__)


class SAMGovClient(BaseAPIClient):
    """
    HTTP client for SAM.gov Entity Information API v3.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "sam_gov"
    BASE_URL = "https://api.sam.gov/entity-information/v3"

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize SAM.gov API client.

        Args:
            api_key: SAM.gov API key (required for production use)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=60.0,
            connect_timeout=15.0,
            # 1000 req/day = ~0.7/min, but we pace at ~1 req/sec for safety
            rate_limit_interval=1.0,
        )

        if not api_key:
            logger.warning(
                "SAM.gov API key not provided. Requests will fail. "
                "Get a free key at: https://sam.gov/content/entity-information"
            )

    def _add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add API key to request parameters."""
        if self.api_key:
            params["api_key"] = self.api_key
        return params

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        """Check for SAM.gov-specific API errors."""
        if "error" in data:
            error_msg = data.get("error", {})
            if isinstance(error_msg, dict):
                message = error_msg.get("message", str(error_msg))
                code = error_msg.get("code", "UNKNOWN")
            else:
                message = str(error_msg)
                code = "UNKNOWN"

            logger.warning(f"SAM.gov API error: {code} - {message}")

            if code in ("INVALID_API_KEY", "API_KEY_REQUIRED"):
                return FatalError(
                    message=f"SAM.gov authentication error: {message}",
                    source=self.SOURCE_NAME,
                    status_code=403,
                    response_data=data,
                )

            return RetryableError(
                message=f"SAM.gov API error {code}: {message}",
                source=self.SOURCE_NAME,
                response_data=data,
            )

        return None

    async def search_entities(
        self,
        state: Optional[str] = None,
        naics_code: Optional[str] = None,
        legal_business_name: Optional[str] = None,
        registration_status: str = "A",
        sam_registered: str = "Yes",
        page: int = 0,
        size: int = 100,
    ) -> Dict[str, Any]:
        """
        Search for registered entities in SAM.gov.

        Args:
            state: Two-letter state code (e.g., "TX", "CA")
            naics_code: Primary NAICS code to filter by
            legal_business_name: Business name search string
            registration_status: "A" for active (default)
            sam_registered: "Yes" for SAM-registered entities
            page: Page number (0-indexed)
            size: Results per page (max 100)

        Returns:
            Dict containing totalRecords and entityData array
        """
        params: Dict[str, Any] = {
            "samRegistered": sam_registered,
            "registrationStatus": registration_status,
            "page": page,
            "size": min(size, 100),
        }

        if state:
            params["physicalAddressStateOrProvinceCode"] = state.upper()
        if naics_code:
            params["naicsCode"] = naics_code
        if legal_business_name:
            params["legalBusinessName"] = legal_business_name

        resource_id = f"entities:state={state},naics={naics_code},page={page}"
        return await self.get("entities", params=params, resource_id=resource_id)

    async def search_all_pages(
        self,
        state: Optional[str] = None,
        naics_code: Optional[str] = None,
        legal_business_name: Optional[str] = None,
        max_pages: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages of entity search results.

        Args:
            state: Two-letter state code
            naics_code: Primary NAICS code
            legal_business_name: Business name search
            max_pages: Maximum number of pages to fetch (safety limit)

        Returns:
            List of all entity records across pages
        """
        all_entities = []
        page = 0

        while page < max_pages:
            response = await self.search_entities(
                state=state,
                naics_code=naics_code,
                legal_business_name=legal_business_name,
                page=page,
                size=100,
            )

            entities = response.get("entityData", [])
            total_records = response.get("totalRecords", 0)

            if not entities:
                break

            all_entities.extend(entities)
            logger.info(
                f"SAM.gov page {page}: fetched {len(entities)} entities "
                f"({len(all_entities)}/{total_records} total)"
            )

            if len(all_entities) >= total_records:
                break

            page += 1

        logger.info(f"SAM.gov search complete: {len(all_entities)} total entities")
        return all_entities
