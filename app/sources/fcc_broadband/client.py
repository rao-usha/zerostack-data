"""
FCC Broadband Map API client with rate limiting and retry logic.

Official FCC Broadband Map API:
https://broadbandmap.fcc.gov/data-download

Rate limits:
- No official documented limit, but be respectful (~60 req/min recommended)
- No API key required (free public API)

Data available:
- Broadband availability by state/county/census block
- Provider information
- Technology types (Fiber, Cable, DSL, Fixed Wireless, Satellite, 5G)
- Advertised speeds
"""

import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class FCCBroadbandClient(BaseAPIClient):
    """
    HTTP client for FCC Broadband Map API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "fcc_broadband"
    BASE_URL = "https://broadbandmap.fcc.gov/api/public"

    # FCC Open Data endpoints (Socrata)
    OPENDATA_BASE = "https://opendata.fcc.gov/resource"
    DATASETS = {
        "fixed_broadband_deployment": "4kuc-phrr",
        "mobile_broadband_deployment": "u3h5-mwgp",
        "broadband_providers": "sgz3-kiqt",
    }

    def __init__(
        self,
        max_concurrency: int = 3,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize FCC Broadband client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("fcc_broadband")

        super().__init__(
            api_key=None,  # No API key required
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=300.0,  # 5min — Socrata aggregation over millions of rows is slow
            connect_timeout=30.0,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers."""
        return {
            "User-Agent": "Nexdata/1.0 (Data Ingestion Service)",
            "Accept": "application/json",
        }

    async def fetch_state_summary(self, state_fips: str) -> Dict[str, Any]:
        """Fetch broadband summary for a state."""
        return await self.get(
            f"map/summary/fixed/state/{state_fips}",
            resource_id=f"state_summary_{state_fips}",
        )

    async def fetch_county_summary(self, county_fips: str) -> Dict[str, Any]:
        """Fetch broadband summary for a county."""
        return await self.get(
            f"map/summary/fixed/county/{county_fips}",
            resource_id=f"county_summary_{county_fips}",
        )

    async def fetch_location_coverage(
        self, latitude: float, longitude: float
    ) -> Dict[str, Any]:
        """Get provider availability at a specific location."""
        params = {"latitude": latitude, "longitude": longitude}
        return await self.get(
            "map/location",
            params=params,
            resource_id=f"location_{latitude}_{longitude}",
        )

    async def fetch_providers_by_state(self, state_fips: str) -> Dict[str, Any]:
        """Fetch all broadband providers serving a state."""
        return await self.get(
            f"map/providers/state/{state_fips}",
            resource_id=f"providers_state_{state_fips}",
        )

    async def fetch_providers_by_county(self, county_fips: str) -> Dict[str, Any]:
        """Fetch all broadband providers serving a county."""
        return await self.get(
            f"map/providers/county/{county_fips}",
            resource_id=f"providers_county_{county_fips}",
        )

    async def fetch_fixed_broadband_data(
        self, state_abbr: Optional[str] = None, limit: int = 50000, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch fixed broadband deployment data from FCC Open Data (raw records)."""
        dataset_id = self.DATASETS["fixed_broadband_deployment"]
        url = f"{self.OPENDATA_BASE}/{dataset_id}.json"

        params = {"$limit": limit, "$offset": offset, "$order": "stateabbr"}

        if state_abbr:
            params["$where"] = f"stateabbr = '{state_abbr.upper()}'"

        return await self.get(url, params=params, resource_id="fixed_broadband")

    async def fetch_state_broadband_aggregated(
        self, state_abbr: str, limit: int = 5000, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Fetch aggregated broadband data for a state using Socrata GROUP BY.

        Returns one row per (provider, technology) with max speeds and block counts.
        Typically ~200-600 rows per state instead of millions of raw records.
        """
        dataset_id = self.DATASETS["fixed_broadband_deployment"]
        url = f"{self.OPENDATA_BASE}/{dataset_id}.json"

        params = {
            "$select": (
                "frn,providername,dbaname,techcode,"
                "max(maxaddown) as max_download,"
                "max(maxadup) as max_upload,"
                "count(*) as block_count"
            ),
            "$where": f"stateabbr='{state_abbr.upper()}'",
            "$group": "frn,providername,dbaname,techcode",
            "$order": "providername,techcode",
            "$limit": str(limit),
            "$offset": str(offset),
        }

        return await self.get(
            url, params=params, resource_id=f"broadband_agg_{state_abbr}"
        )

    async def fetch_mobile_broadband_data(
        self, state_fips: Optional[str] = None, limit: int = 50000, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch mobile broadband deployment data."""
        dataset_id = self.DATASETS["mobile_broadband_deployment"]
        url = f"{self.OPENDATA_BASE}/{dataset_id}.json"

        params = {"$limit": limit, "$offset": offset}
        if state_fips:
            params["$where"] = f"state = '{state_fips}'"

        return await self.get(url, params=params, resource_id="mobile_broadband")

    async def get_available_downloads(self) -> Dict[str, Any]:
        """Get list of available bulk download files from FCC."""
        return await self.get("map/downloads", resource_id="downloads")


# Technology type codes (FCC Form 477)
TECHNOLOGY_CODES = {
    "10": "Asymmetric xDSL",
    "40": "Cable Modem - DOCSIS 3.0",
    "41": "Cable Modem - DOCSIS 3.1",
    "50": "Fiber to the Premises (FTTP)",
    "60": "Satellite",
    "70": "Terrestrial Fixed Wireless",
}

# FCC broadband definition
FCC_BROADBAND_THRESHOLD = {
    "download_mbps": 25,
    "upload_mbps": 3,
}

# US State FIPS codes
STATE_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
    "DC": "11",
}

# All 50 US states + DC
US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
]
