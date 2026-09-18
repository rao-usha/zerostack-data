"""
SEC Form ADV Client.

Fetches investment adviser data from SEC IAPD and data files.
"""

import logging
import httpx
from typing import Dict, List, Optional, Any
import asyncio

logger = logging.getLogger(__name__)


class FormADVClient:
    """
    Client for accessing SEC Form ADV data.

    Data sources:
    - IAPD (Investment Adviser Public Disclosure): https://adviserinfo.sec.gov/
    - SEC quarterly CSV files
    """

    IAPD_API_URL = "https://api.adviserinfo.sec.gov"
    SEC_DATA_URL = "https://www.sec.gov"

    USER_AGENT = "Nexdata Data Ingestion Service (contact: support@nexdata.io)"

    # Rate limit: be conservative with SEC
    RATE_LIMIT_DELAY = 0.2  # 200ms between requests

    def __init__(self):
        self._last_request_time = 0

    async def _rate_limit(self):
        """Enforce rate limits."""
        now = asyncio.get_running_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            await asyncio.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = asyncio.get_running_loop().time()

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "User-Agent": self.USER_AGENT,
            "Accept": "application/json, text/csv, */*",
        }

    async def search_advisers(
        self,
        name: Optional[str] = None,
        crd: Optional[str] = None,
        state: Optional[str] = None,
        page: int = 1,
        page_size: int = 50,
    ) -> Dict[str, Any]:
        """
        Search for investment advisers via IAPD API.

        Args:
            name: Adviser name to search
            crd: CRD number
            state: State code (e.g., "NY")
            page: Page number
            page_size: Results per page

        Returns:
            Search results
        """
        await self._rate_limit()

        # Build search query
        params = {
            "page": page,
            "pageSize": page_size,
        }

        if name:
            params["name"] = name
        if crd:
            params["crd"] = crd
        if state:
            params["state"] = state

        url = f"{self.IAPD_API_URL}/search/adviser"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url, params=params, headers=self._get_headers(), timeout=30
                )
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(f"IAPD search returned {response.status_code}")
            except Exception as e:
                logger.warning(f"IAPD search failed: {e}")

        return {"hits": [], "total": 0}

    async def get_adviser_details(self, crd_number: str) -> Optional[Dict]:
        """
        Get detailed information for a specific adviser.

        Args:
            crd_number: CRD number

        Returns:
            Adviser details or None
        """
        await self._rate_limit()

        url = f"{self.IAPD_API_URL}/adviser/{crd_number}"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    url, headers=self._get_headers(), timeout=30
                )
                if response.status_code == 200:
                    return response.json()
            except Exception as e:
                logger.warning(f"Failed to get adviser {crd_number}: {e}")

        return None

    async def get_adviser_brochure_url(self, crd_number: str) -> Optional[str]:
        """
        Get URL to adviser's Form ADV Part 2 brochure PDF.

        Args:
            crd_number: CRD number

        Returns:
            URL to brochure PDF
        """
        return f"https://adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BESSION_ID={crd_number}"
