"""
Treasury FiscalData API client with rate limiting and retry logic.

Official Treasury FiscalData API documentation:
https://fiscaldata.treasury.gov/api-documentation/

Treasury FiscalData API provides access to U.S. Treasury fiscal data:
- Daily Treasury balance and cash operations
- Public debt outstanding
- Average interest rates on Treasury securities
- Monthly Treasury Statement (revenue & spending)
- Treasury auction results

Rate limits:
- No API key required
- 1,000 requests per minute limit
- Generous response size limits (up to 10,000 records per request)
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class TreasuryClient(BaseAPIClient):
    """
    HTTP client for Treasury FiscalData API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "treasury"
    BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

    def __init__(
        self,
        max_concurrency: int = 5,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize Treasury FiscalData API client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("treasury")

        super().__init__(
            api_key=None,  # No API key required
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

    async def get_daily_treasury_balance(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page_size: int = 10000,
        page_number: int = 1,
    ) -> Dict[str, Any]:
        """
        Fetch Daily Treasury Statement - Deposits/Withdrawals/Operating Cash.

        Endpoint: v1/accounting/dts/deposits_withdrawals_operating_cash

        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            page_size: Number of records per page (max 10,000)
            page_number: Page number for pagination

        Returns:
            Dict containing API response with data
        """
        filters = []
        if start_date:
            filters.append(f"record_date:gte:{start_date}")
        if end_date:
            filters.append(f"record_date:lte:{end_date}")

        params = {
            "page[size]": page_size,
            "page[number]": page_number,
            "sort": "-record_date",
        }

        if filters:
            params["filter"] = ",".join(filters)

        return await self.get(
            "v1/accounting/dts/deposits_withdrawals_operating_cash",
            params=params,
            resource_id="daily_treasury_balance",
        )

    async def get_debt_outstanding(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page_size: int = 10000,
        page_number: int = 1,
    ) -> Dict[str, Any]:
        """
        Fetch Debt to the Penny - Total Public Debt Outstanding.

        Endpoint: v2/accounting/od/debt_outstanding

        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            page_size: Number of records per page (max 10,000)
            page_number: Page number for pagination

        Returns:
            Dict containing API response with data
        """
        filters = []
        if start_date:
            filters.append(f"record_date:gte:{start_date}")
        if end_date:
            filters.append(f"record_date:lte:{end_date}")

        params = {
            "page[size]": page_size,
            "page[number]": page_number,
            "sort": "-record_date",
        }

        if filters:
            params["filter"] = ",".join(filters)

        return await self.get(
            "v2/accounting/od/debt_outstanding",
            params=params,
            resource_id="debt_outstanding",
        )

    async def get_interest_rates(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        security_type: Optional[str] = None,
        page_size: int = 10000,
        page_number: int = 1,
    ) -> Dict[str, Any]:
        """
        Fetch Average Interest Rates on U.S. Treasury Securities.

        Endpoint: v2/accounting/od/avg_interest_rates

        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            security_type: Filter by security type (e.g., "Treasury Bills")
            page_size: Number of records per page (max 10,000)
            page_number: Page number for pagination

        Returns:
            Dict containing API response with data
        """
        filters = []
        if start_date:
            filters.append(f"record_date:gte:{start_date}")
        if end_date:
            filters.append(f"record_date:lte:{end_date}")
        if security_type:
            filters.append(f"security_type_desc:eq:{security_type}")

        params = {
            "page[size]": page_size,
            "page[number]": page_number,
            "sort": "-record_date",
        }

        if filters:
            params["filter"] = ",".join(filters)

        return await self.get(
            "v2/accounting/od/avg_interest_rates",
            params=params,
            resource_id="interest_rates",
        )

    async def get_monthly_treasury_statement(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        classification: Optional[str] = None,
        page_size: int = 10000,
        page_number: int = 1,
    ) -> Dict[str, Any]:
        """
        Fetch Monthly Treasury Statement - Table 4 (Revenue & Spending).

        Endpoint: v1/accounting/mts/mts_table_4

        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            classification: Filter by classification (e.g., "Receipts", "Outlays")
            page_size: Number of records per page (max 10,000)
            page_number: Page number for pagination

        Returns:
            Dict containing API response with data
        """
        filters = []
        if start_date:
            filters.append(f"record_date:gte:{start_date}")
        if end_date:
            filters.append(f"record_date:lte:{end_date}")
        if classification:
            filters.append(f"classification_desc:eq:{classification}")

        params = {
            "page[size]": page_size,
            "page[number]": page_number,
            "sort": "-record_date",
        }

        if filters:
            params["filter"] = ",".join(filters)

        return await self.get(
            "v1/accounting/mts/mts_table_4",
            params=params,
            resource_id="monthly_treasury_statement",
        )

    async def get_auction_results(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        security_type: Optional[str] = None,
        page_size: int = 10000,
        page_number: int = 1,
    ) -> Dict[str, Any]:
        """
        Fetch Treasury Securities Auction Data.

        Endpoint: v1/accounting/od/auctions_query

        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            security_type: Filter by security type (e.g., "Bill", "Note", "Bond")
            page_size: Number of records per page (max 10,000)
            page_number: Page number for pagination

        Returns:
            Dict containing API response with data
        """
        filters = []
        if start_date:
            filters.append(f"auction_date:gte:{start_date}")
        if end_date:
            filters.append(f"auction_date:lte:{end_date}")
        if security_type:
            filters.append(f"security_type:eq:{security_type}")

        params = {
            "page[size]": page_size,
            "page[number]": page_number,
            "sort": "-auction_date",
        }

        if filters:
            params["filter"] = ",".join(filters)

        return await self.get(
            "v1/accounting/od/auctions_query",
            params=params,
            resource_id="auction_results",
        )


# Treasury dataset definitions
TREASURY_DATASETS = {
    "daily_balance": {
        "endpoint": "v1/accounting/dts/deposits_withdrawals_operating_cash",
        "table_name": "treasury_daily_balance",
        "description": "Daily Treasury Statement - Deposits, Withdrawals, and Operating Cash",
        "date_field": "record_date",
    },
    "debt_outstanding": {
        "endpoint": "v2/accounting/od/debt_outstanding",
        "table_name": "treasury_debt_outstanding",
        "description": "Total Public Debt Outstanding (Debt to the Penny)",
        "date_field": "record_date",
    },
    "interest_rates": {
        "endpoint": "v2/accounting/od/avg_interest_rates",
        "table_name": "treasury_interest_rates",
        "description": "Average Interest Rates on U.S. Treasury Securities",
        "date_field": "record_date",
    },
    "monthly_statement": {
        "endpoint": "v1/accounting/mts/mts_table_4",
        "table_name": "treasury_monthly_statement",
        "description": "Monthly Treasury Statement - Revenue and Spending",
        "date_field": "record_date",
    },
    "auctions": {
        "endpoint": "v1/accounting/od/auctions_query",
        "table_name": "treasury_auctions",
        "description": "Treasury Securities Auction Results",
        "date_field": "auction_date",
    },
}


# Security types for interest rates
SECURITY_TYPES = [
    "Treasury Bills",
    "Treasury Notes",
    "Treasury Bonds",
    "Treasury Inflation-Protected Securities (TIPS)",
    "Treasury Floating Rate Notes (FRN)",
]


# Auction security types
AUCTION_SECURITY_TYPES = [
    "Bill",
    "Note",
    "Bond",
    "TIPS",
    "FRN",
    "CMB",  # Cash Management Bill
]
