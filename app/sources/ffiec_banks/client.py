"""
FFIEC Bank Call Reports API client via FDIC BankFind Suite.

Uses the FDIC BankFind Suite API to retrieve bank financial data:
https://banks.data.fdic.gov/api/

No API key required. The FDIC API is publicly accessible.

Rate limits:
- No documented rate limits, but respectful usage recommended (1-2 req/sec)
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, RetryableError

logger = logging.getLogger(__name__)


class FfiecBankClient(BaseAPIClient):
    """
    HTTP client for FDIC BankFind Suite API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "ffiec_banks"
    BASE_URL = "https://banks.data.fdic.gov/api"

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize FDIC BankFind API client.

        No API key required.

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
        """Check for FDIC API-specific errors."""
        if isinstance(data, dict):
            error = data.get("error")
            if error:
                error_message = str(error)
                logger.warning(f"FDIC API error for {resource_id}: {error_message}")
                return FatalError(
                    message=f"FDIC API error: {error_message}",
                    source=self.SOURCE_NAME,
                    response_data=data,
                )
        return None

    async def fetch_financials(
        self,
        report_date: str = "20231231",
        limit: int = 10000,
        offset: int = 0,
        state: Optional[str] = None,
        fields: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch bank financial data from FDIC BankFind Suite.

        Args:
            report_date: Report date in YYYYMMDD format (e.g., "20231231")
            limit: Maximum number of records to return (max 10000)
            offset: Offset for pagination
            state: Optional state name filter (e.g., "Texas")
            fields: Comma-separated list of fields to return

        Returns:
            Dict containing API response with bank financial data

        Raises:
            APIError: On API errors after retries
        """
        url = f"{self.BASE_URL}/financials"

        default_fields = (
            "REPDTE,CERT,INSTNAME,STNAME,CITY,ASSET,DEP,LNLSNET,"
            "EQTOT,NITEFDSM,ROAPTX,OFFDOM"
        )

        filters = f"REPDTE:{report_date}"
        if state:
            filters += f" AND STNAME:{state}"

        params: Dict[str, Any] = {
            "filters": filters,
            "fields": fields or default_fields,
            "limit": limit,
            "offset": offset,
            "sort_by": "ASSET",
            "sort_order": "DESC",
        }

        resource_id = f"fdic_financials_{report_date}_off{offset}"
        return await self.get(url, params=params, resource_id=resource_id)

    async def fetch_all_financials(
        self,
        report_date: str = "20231231",
        state: Optional[str] = None,
        max_records: int = 50000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all bank financial records with automatic pagination.

        Args:
            report_date: Report date in YYYYMMDD format
            state: Optional state name filter
            max_records: Maximum total records to fetch

        Returns:
            List of all bank financial records
        """
        all_records: List[Dict[str, Any]] = []
        offset = 0
        page_size = 10000

        while offset < max_records:
            logger.info(
                f"Fetching FDIC financials offset={offset} for date={report_date}"
            )

            response = await self.fetch_financials(
                report_date=report_date,
                limit=page_size,
                offset=offset,
                state=state,
            )

            # FDIC API response: {"data": [{"data": {...}}, ...], "totals": {...}}
            data_list = response.get("data", [])

            if not data_list:
                logger.info(f"No more records at offset={offset}")
                break

            all_records.extend(data_list)
            logger.info(
                f"Fetched {len(data_list)} records (total: {len(all_records)})"
            )

            if len(data_list) < page_size:
                break

            offset += page_size

        logger.info(
            f"Total FDIC financial records fetched: {len(all_records)}"
        )
        return all_records
