"""
DUNL.org (S&P Global Data Unlocked) API client.

DUNL provides CC-licensed reference data via JSON-LD content negotiation:
- Currencies (208 records)
- Ports (301 records)
- Units of Measure (210 records)
- UOM Conversions (635 records)
- Holiday Calendars (~473/year x 7 years)

No API key required. Fully open, Creative Commons licensed.
Content negotiation on /c/ paths with Accept: application/ld+json.
"""

import logging
from typing import Dict, Any, Optional

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class DunlClient(BaseAPIClient):
    """HTTP client for DUNL.org JSON-LD API."""

    SOURCE_NAME = "dunl"
    BASE_URL = "https://dunl.org"

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        config = get_api_config("dunl")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

    def _build_headers(self) -> Dict[str, str]:
        """Request JSON-LD via content negotiation."""
        return {"Accept": "application/ld+json"}

    async def fetch_currencies(self) -> Dict[str, Any]:
        """Fetch all currency definitions."""
        return await self.get("c/currency", resource_id="currencies")

    async def fetch_ports(self) -> Dict[str, Any]:
        """Fetch all port/location definitions."""
        return await self.get("c/port", resource_id="ports")

    async def fetch_uom(self) -> Dict[str, Any]:
        """Fetch all unit-of-measure definitions."""
        return await self.get("c/uom", resource_id="uom")

    async def fetch_uom_conversions(self) -> Dict[str, Any]:
        """Fetch all UOM conversion factors."""
        return await self.get("c/uom-conversion", resource_id="uom_conversions")

    async def fetch_calendar(self, year: int) -> Dict[str, Any]:
        """Fetch holiday calendar for a specific year."""
        return await self.get(f"c/calendar/{year}", resource_id=f"calendar_{year}")

    async def fetch_rss_feed(self) -> str:
        """Fetch RSS feed for dataset change tracking (returns raw XML)."""
        client = await self._get_client()
        response = await client.get(
            f"{self.BASE_URL}/feed",
            headers={"Accept": "application/rss+xml, application/xml, text/xml"},
        )
        response.raise_for_status()
        return response.text
