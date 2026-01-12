"""
FDIC BankFind Suite API client with rate limiting and retry logic.

Official FDIC BankFind API documentation:
https://banks.data.fdic.gov/docs/

FDIC BankFind provides access to bank data:
- Bank Financials (balance sheets, income statements)
- Institutions (bank demographics, locations)
- Failed Banks (historical failures)
- Summary of Deposits (branch-level deposit data)

Rate limits:
- No API key required
- No documented rate limits, but we use conservative defaults
- Max 10,000 records per request (pagination required for larger sets)
"""
import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class FDICClient(BaseAPIClient):
    """
    HTTP client for FDIC BankFind API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "fdic"
    # Updated 2025: FDIC migrated API from banks.data.fdic.gov to api.fdic.gov
    BASE_URL = "https://api.fdic.gov/banks"

    # Pagination limits
    MAX_LIMIT = 10000
    DEFAULT_LIMIT = 10000

    def __init__(
        self,
        max_concurrency: int = 3,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize FDIC BankFind API client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("fdic")

        super().__init__(
            api_key=None,  # No API key required
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval()
        )

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers."""
        return {
            "User-Agent": "Nexdata External Data Ingestion Service (FDIC BankFind)",
            "Accept": "application/json"
        }

    async def get_bank_financials(
        self,
        cert: Optional[int] = None,
        filters: Optional[str] = None,
        fields: Optional[str] = None,
        sort_by: str = "REPDTE",
        sort_order: str = "DESC",
        limit: int = DEFAULT_LIMIT,
        offset: int = 0,
        agg_by: Optional[str] = None,
        agg_term_fields: Optional[str] = None,
        agg_sum_fields: Optional[str] = None,
        agg_limit: int = 100
    ) -> Dict[str, Any]:
        """
        Fetch bank financial data (balance sheet, income statement).

        Args:
            cert: FDIC Certificate number (optional, for specific bank)
            filters: Filter expression (e.g., "REPDTE:20230630")
            fields: Comma-separated list of fields to return
            sort_by: Field to sort by (default: REPDTE - report date)
            sort_order: Sort order (ASC or DESC)
            limit: Number of records to return (max 10,000)
            offset: Offset for pagination
            agg_by: Field to aggregate by
            agg_term_fields: Fields for term aggregation
            agg_sum_fields: Fields to sum in aggregation
            agg_limit: Limit for aggregation results

        Returns:
            Dict containing API response with financial data
        """
        params = {
            "sort_by": sort_by,
            "sort_order": sort_order,
            "limit": min(limit, self.MAX_LIMIT),
            "offset": offset,
            "format": "json"
        }

        if cert is not None:
            params["filters"] = f"CERT:{cert}" + (f" AND {filters}" if filters else "")
        elif filters:
            params["filters"] = filters

        if fields:
            params["fields"] = fields

        if agg_by:
            params["agg_by"] = agg_by
        if agg_term_fields:
            params["agg_term_fields"] = agg_term_fields
        if agg_sum_fields:
            params["agg_sum_fields"] = agg_sum_fields
        if agg_by:
            params["agg_limit"] = agg_limit

        return await self.get("financials", params=params, resource_id="financials")

    async def get_all_bank_financials(
        self,
        cert: Optional[int] = None,
        filters: Optional[str] = None,
        fields: Optional[str] = None,
        sort_by: str = "REPDTE",
        sort_order: str = "DESC"
    ) -> List[Dict[str, Any]]:
        """Fetch all bank financial data with automatic pagination."""
        all_data = []
        offset = 0
        total = None

        while True:
            response = await self.get_bank_financials(
                cert=cert,
                filters=filters,
                fields=fields,
                sort_by=sort_by,
                sort_order=sort_order,
                limit=self.MAX_LIMIT,
                offset=offset
            )

            data = response.get("data", [])
            all_data.extend(data)

            meta = response.get("meta", {})
            if total is None:
                total = meta.get("total", len(data))
                logger.info(f"Total financial records to fetch: {total}")

            logger.info(f"Fetched {len(all_data)}/{total} financial records")

            if len(data) < self.MAX_LIMIT or len(all_data) >= total:
                break

            offset += self.MAX_LIMIT

        return all_data

    async def get_institutions(
        self,
        cert: Optional[int] = None,
        filters: Optional[str] = None,
        fields: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = "NAME",
        sort_order: str = "ASC",
        limit: int = DEFAULT_LIMIT,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Fetch bank institution demographics data.

        Args:
            cert: FDIC Certificate number (optional)
            filters: Filter expression (e.g., "ACTIVE:1")
            fields: Comma-separated list of fields to return
            search: Full-text search term (bank name, city, etc.)
            sort_by: Field to sort by (default: NAME)
            sort_order: Sort order (ASC or DESC)
            limit: Number of records to return (max 10,000)
            offset: Offset for pagination

        Returns:
            Dict containing API response with institution data
        """
        params = {
            "sort_by": sort_by,
            "sort_order": sort_order,
            "limit": min(limit, self.MAX_LIMIT),
            "offset": offset,
            "format": "json"
        }

        if cert is not None:
            params["filters"] = f"CERT:{cert}" + (f" AND {filters}" if filters else "")
        elif filters:
            params["filters"] = filters

        if fields:
            params["fields"] = fields

        if search:
            params["search"] = search

        return await self.get("institutions", params=params, resource_id="institutions")

    async def get_all_institutions(
        self,
        filters: Optional[str] = None,
        fields: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = "NAME",
        sort_order: str = "ASC",
        active_only: bool = True
    ) -> List[Dict[str, Any]]:
        """Fetch all bank institutions with automatic pagination."""
        if active_only:
            if filters:
                filters = f"ACTIVE:1 AND {filters}"
            else:
                filters = "ACTIVE:1"

        all_data = []
        offset = 0
        total = None

        while True:
            response = await self.get_institutions(
                filters=filters,
                fields=fields,
                search=search,
                sort_by=sort_by,
                sort_order=sort_order,
                limit=self.MAX_LIMIT,
                offset=offset
            )

            data = response.get("data", [])
            all_data.extend(data)

            meta = response.get("meta", {})
            if total is None:
                total = meta.get("total", len(data))
                logger.info(f"Total institutions to fetch: {total}")

            logger.info(f"Fetched {len(all_data)}/{total} institutions")

            if len(data) < self.MAX_LIMIT or len(all_data) >= total:
                break

            offset += self.MAX_LIMIT

        return all_data

    async def get_failed_banks(
        self,
        filters: Optional[str] = None,
        fields: Optional[str] = None,
        sort_by: str = "FAILDATE",
        sort_order: str = "DESC",
        limit: int = DEFAULT_LIMIT,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Fetch failed banks list.

        Args:
            filters: Filter expression (e.g., year range)
            fields: Comma-separated list of fields to return
            sort_by: Field to sort by (default: FAILDATE)
            sort_order: Sort order (ASC or DESC)
            limit: Number of records to return (max 10,000)
            offset: Offset for pagination

        Returns:
            Dict containing API response with failed banks data
        """
        params = {
            "sort_by": sort_by,
            "sort_order": sort_order,
            "limit": min(limit, self.MAX_LIMIT),
            "offset": offset,
            "format": "json"
        }

        if filters:
            params["filters"] = filters

        if fields:
            params["fields"] = fields

        return await self.get("failures", params=params, resource_id="failed_banks")

    async def get_all_failed_banks(
        self,
        filters: Optional[str] = None,
        fields: Optional[str] = None,
        sort_by: str = "FAILDATE",
        sort_order: str = "DESC"
    ) -> List[Dict[str, Any]]:
        """Fetch all failed banks with automatic pagination."""
        all_data = []
        offset = 0
        total = None

        while True:
            response = await self.get_failed_banks(
                filters=filters,
                fields=fields,
                sort_by=sort_by,
                sort_order=sort_order,
                limit=self.MAX_LIMIT,
                offset=offset
            )

            data = response.get("data", [])
            all_data.extend(data)

            meta = response.get("meta", {})
            if total is None:
                total = meta.get("total", len(data))
                logger.info(f"Total failed banks to fetch: {total}")

            logger.info(f"Fetched {len(all_data)}/{total} failed banks")

            if len(data) < self.MAX_LIMIT or len(all_data) >= total:
                break

            offset += self.MAX_LIMIT

        return all_data

    async def get_summary_of_deposits(
        self,
        cert: Optional[int] = None,
        filters: Optional[str] = None,
        fields: Optional[str] = None,
        sort_by: str = "YEAR",
        sort_order: str = "DESC",
        limit: int = DEFAULT_LIMIT,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Fetch Summary of Deposits (SOD) data - branch level deposit data.

        Args:
            cert: FDIC Certificate number (optional)
            filters: Filter expression
            fields: Comma-separated list of fields to return
            sort_by: Field to sort by (default: YEAR)
            sort_order: Sort order (ASC or DESC)
            limit: Number of records to return (max 10,000)
            offset: Offset for pagination

        Returns:
            Dict containing API response with SOD data
        """
        params = {
            "sort_by": sort_by,
            "sort_order": sort_order,
            "limit": min(limit, self.MAX_LIMIT),
            "offset": offset,
            "format": "json"
        }

        if cert is not None:
            params["filters"] = f"CERT:{cert}" + (f" AND {filters}" if filters else "")
        elif filters:
            params["filters"] = filters

        if fields:
            params["fields"] = fields

        return await self.get("sod", params=params, resource_id="sod")

    async def get_all_summary_of_deposits(
        self,
        cert: Optional[int] = None,
        filters: Optional[str] = None,
        fields: Optional[str] = None,
        year: Optional[int] = None,
        sort_by: str = "YEAR",
        sort_order: str = "DESC"
    ) -> List[Dict[str, Any]]:
        """Fetch all Summary of Deposits data with automatic pagination."""
        if year:
            year_filter = f"YEAR:{year}"
            if filters:
                filters = f"{year_filter} AND {filters}"
            else:
                filters = year_filter

        all_data = []
        offset = 0
        total = None

        while True:
            response = await self.get_summary_of_deposits(
                cert=cert,
                filters=filters,
                fields=fields,
                sort_by=sort_by,
                sort_order=sort_order,
                limit=self.MAX_LIMIT,
                offset=offset
            )

            data = response.get("data", [])
            all_data.extend(data)

            meta = response.get("meta", {})
            if total is None:
                total = meta.get("total", len(data))
                logger.info(f"Total SOD records to fetch: {total}")

            logger.info(f"Fetched {len(all_data)}/{total} SOD records")

            if len(data) < self.MAX_LIMIT or len(all_data) >= total:
                break

            offset += self.MAX_LIMIT

        return all_data

    async def search_banks(
        self,
        query: str,
        active_only: bool = True,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search for banks by name, city, or other text.

        Args:
            query: Search term
            active_only: Only return active banks
            limit: Maximum results to return

        Returns:
            List of matching institutions
        """
        filters = "ACTIVE:1" if active_only else None

        response = await self.get_institutions(
            search=query,
            filters=filters,
            limit=min(limit, self.MAX_LIMIT)
        )

        return response.get("data", [])
