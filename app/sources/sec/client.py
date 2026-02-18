"""
SEC EDGAR API client with rate limiting and retry logic.

Official SEC EDGAR API documentation:
https://www.sec.gov/edgar/sec-api-documentation

SEC EDGAR provides access to corporate filings:
- 10-K (Annual reports)
- 10-Q (Quarterly reports)
- 8-K (Current reports)
- S-1/S-3/S-4 (Registration statements)
- XBRL data

Rate limits:
- 10 requests per second per IP (strictly enforced)
- User-Agent header REQUIRED (must identify yourself)
- Rate limit documentation: https://www.sec.gov/os/accessing-edgar-data
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class SECClient(BaseAPIClient):
    """
    HTTP client for SEC EDGAR API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "sec"
    BASE_URL = "https://data.sec.gov"

    # User-Agent is REQUIRED by SEC
    USER_AGENT = (
        "Nexdata External Data Ingestion Service (contact: compliance@nexdata.com)"
    )

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize SEC EDGAR API client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("sec")

        super().__init__(
            api_key=None,  # No API key required
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers with required User-Agent."""
        return {"User-Agent": self.USER_AGENT, "Accept": "application/json"}

    async def get_company_submissions(self, cik: str) -> Dict[str, Any]:
        """
        Fetch company submission history from SEC EDGAR.

        Args:
            cik: Company CIK (Central Index Key), e.g., "0000320193" for Apple

        Returns:
            Dict containing company information and filing history

        Raises:
            Exception: On API errors after retries
        """
        # Ensure CIK is 10 digits with leading zeros
        cik_padded = str(cik).zfill(10)

        return await self.get(
            f"submissions/CIK{cik_padded}.json", resource_id=f"submissions:{cik_padded}"
        )

    async def get_company_facts(self, cik: str) -> Dict[str, Any]:
        """
        Fetch company facts (XBRL data) from SEC EDGAR.

        Args:
            cik: Company CIK (Central Index Key)

        Returns:
            Dict containing XBRL facts for the company
        """
        cik_padded = str(cik).zfill(10)

        return await self.get(
            f"api/xbrl/companyfacts/CIK{cik_padded}.json",
            resource_id=f"facts:{cik_padded}",
        )

    async def get_multiple_companies(
        self, ciks: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch multiple companies using the base class fetch_multiple method.

        Args:
            ciks: List of CIK numbers

        Returns:
            Dict mapping CIK to company submission data
        """
        items = [(cik, cik) for cik in ciks]

        async def fetch_one(item):
            cik = item[0]
            return await self.get_company_submissions(cik)

        return await self.fetch_multiple(
            items=items, fetch_func=fetch_one, item_id_func=lambda item: item[0]
        )


# Common CIK numbers for major companies
COMMON_COMPANIES = {
    "tech": {
        "apple": "0000320193",
        "microsoft": "0000789019",
        "alphabet": "0001652044",
        "amazon": "0001018724",
        "meta": "0001326801",
        "tesla": "0001318605",
        "nvidia": "0001045810",
    },
    "finance": {
        "jpmorgan": "0000019617",
        "bank_of_america": "0000070858",
        "wells_fargo": "0000072971",
        "goldman_sachs": "0000886982",
        "morgan_stanley": "0000895421",
        "berkshire_hathaway": "0001067983",
    },
    "healthcare": {
        "johnson_johnson": "0000200406",
        "pfizer": "0000078003",
        "unitedhealth": "0000731766",
        "abbvie": "0001551152",
        "merck": "0000310158",
    },
    "energy": {
        "exxon": "0000034088",
        "chevron": "0000093410",
        "conocophillips": "0001163165",
    },
}
