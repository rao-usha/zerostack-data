"""
FBI Crime Data Explorer API client with rate limiting and retry logic.

Official API documentation:
https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi

API Base URL: https://api.usa.gov/crime/fbi/sapi

The FBI Crime Data Explorer provides access to:
- Uniform Crime Reports (UCR) - Summary crime statistics
- National Incident-Based Reporting System (NIBRS) - Detailed incident data
- Hate Crime Statistics
- Law Enforcement Officers Killed and Assaulted (LEOKA)
- Cargo Theft Reports

Rate limits:
- Reasonable use with API key
- API key available free at: https://api.data.gov/signup/
"""
import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class FBICrimeClient(BaseAPIClient):
    """
    HTTP client for FBI Crime Data Explorer API with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "fbi_crime"
    BASE_URL = "https://api.usa.gov/crime/fbi/sapi"

    # Valid offense types
    OFFENSE_TYPES = [
        "aggravated-assault", "arson", "burglary", "homicide",
        "human-trafficking", "larceny", "motor-vehicle-theft",
        "property-crime", "rape", "rape-legacy", "robbery", "violent-crime",
    ]

    # State abbreviations
    STATE_ABBRS = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
        "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
        "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
        "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
        "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    ]

    def __init__(
        self,
        api_key: str,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize FBI Crime API client.

        Args:
            api_key: FBI Crime Data Explorer API key (required)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        if not api_key:
            raise ValueError(
                "FBI Crime Data Explorer API key is required. "
                "Get a free key at: https://api.data.gov/signup/"
            )

        config = get_api_config("fbi_crime")

        super().__init__(
            api_key=api_key,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval()
        )

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers with API key."""
        return {"X-Api-Key": self.api_key}

    # ============================================
    # National Estimates Endpoints
    # ============================================

    async def get_national_estimates(
        self,
        offense: str
    ) -> Dict[str, Any]:
        """Fetch national crime estimates for a specific offense."""
        if offense not in self.OFFENSE_TYPES:
            raise ValueError(f"Invalid offense type: {offense}")

        return await self.get(
            f"api/estimates/national/{offense}/",
            resource_id=f"national_estimates_{offense}"
        )

    async def get_state_estimates(
        self,
        state_abbr: str,
        offense: str
    ) -> Dict[str, Any]:
        """Fetch state-level crime estimates for a specific offense."""
        if offense not in self.OFFENSE_TYPES:
            raise ValueError(f"Invalid offense type: {offense}")
        if state_abbr.upper() not in self.STATE_ABBRS:
            raise ValueError(f"Invalid state abbreviation: {state_abbr}")

        return await self.get(
            f"api/estimates/states/{state_abbr.upper()}/{offense}/",
            resource_id=f"state_estimates_{state_abbr}_{offense}"
        )

    async def get_regional_estimates(
        self,
        region_name: str,
        offense: str
    ) -> Dict[str, Any]:
        """Fetch regional crime estimates."""
        return await self.get(
            f"api/estimates/regions/{region_name}/{offense}/",
            resource_id=f"regional_estimates_{region_name}_{offense}"
        )

    # ============================================
    # Summarized/Agency Data Endpoints
    # ============================================

    async def get_agency_participation(self) -> Dict[str, Any]:
        """Fetch national agency participation data."""
        return await self.get("api/participation/national", resource_id="participation_national")

    async def get_state_participation(self, state_abbr: str) -> Dict[str, Any]:
        """Fetch state agency participation data."""
        return await self.get(
            f"api/participation/states/{state_abbr.upper()}",
            resource_id=f"participation_{state_abbr}"
        )

    async def get_summarized_data(
        self,
        state_abbr: str,
        offense: str,
        since: int,
        until: int
    ) -> Dict[str, Any]:
        """Fetch summarized crime data for a state."""
        if offense not in self.OFFENSE_TYPES:
            raise ValueError(f"Invalid offense type: {offense}")

        return await self.get(
            f"api/summarized/state/{state_abbr.upper()}/{offense}/{since}/{until}",
            resource_id=f"summarized_{state_abbr}_{offense}_{since}_{until}"
        )

    # ============================================
    # NIBRS Data Endpoints
    # ============================================

    async def get_nibrs_offense_data(
        self,
        state_abbr: str,
        variable: str = "count"
    ) -> Dict[str, Any]:
        """Fetch NIBRS offense data for a state."""
        var_encoded = variable.replace("/", "%2F")
        return await self.get(
            f"api/nibrs/offense/states/{state_abbr.upper()}/{var_encoded}",
            resource_id=f"nibrs_offense_{state_abbr}_{variable}"
        )

    async def get_nibrs_victim_data(
        self,
        state_abbr: str,
        variable: str = "count"
    ) -> Dict[str, Any]:
        """Fetch NIBRS victim data for a state."""
        var_encoded = variable.replace("/", "%2F")
        return await self.get(
            f"api/nibrs/victim/states/{state_abbr.upper()}/{var_encoded}",
            resource_id=f"nibrs_victim_{state_abbr}_{variable}"
        )

    async def get_nibrs_offender_data(
        self,
        state_abbr: str,
        variable: str = "count"
    ) -> Dict[str, Any]:
        """Fetch NIBRS offender data for a state."""
        var_encoded = variable.replace("/", "%2F")
        return await self.get(
            f"api/nibrs/offender/states/{state_abbr.upper()}/{var_encoded}",
            resource_id=f"nibrs_offender_{state_abbr}_{variable}"
        )

    # ============================================
    # Hate Crime Data Endpoints
    # ============================================

    async def get_hate_crime_national(self) -> Dict[str, Any]:
        """Fetch national hate crime statistics."""
        return await self.get("api/hate-crime/national", resource_id="hate_crime_national")

    async def get_hate_crime_by_state(self, state_abbr: str) -> Dict[str, Any]:
        """Fetch hate crime statistics for a state."""
        return await self.get(
            f"api/hate-crime/states/{state_abbr.upper()}",
            resource_id=f"hate_crime_{state_abbr}"
        )

    # ============================================
    # LEOKA Endpoints
    # ============================================

    async def get_leoka_national(self) -> Dict[str, Any]:
        """Fetch national LEOKA statistics."""
        return await self.get("api/leoka/national", resource_id="leoka_national")

    async def get_leoka_by_state(self, state_abbr: str) -> Dict[str, Any]:
        """Fetch LEOKA statistics for a state."""
        return await self.get(
            f"api/leoka/states/{state_abbr.upper()}",
            resource_id=f"leoka_{state_abbr}"
        )

    # ============================================
    # Agency Lookup
    # ============================================

    async def lookup_agencies(
        self,
        state_abbr: Optional[str] = None,
        agency_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Lookup law enforcement agencies."""
        params = {}
        if state_abbr:
            params["state"] = state_abbr.upper()
        if agency_name:
            params["name"] = agency_name

        return await self.get("api/agencies", params=params, resource_id="agencies_lookup")

    # ============================================
    # Bulk Data Methods
    # ============================================

    async def get_all_national_estimates(self) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch national estimates for ALL offense types."""
        results = {}

        for offense in self.OFFENSE_TYPES:
            try:
                logger.info(f"Fetching national estimates for {offense}")
                response = await self.get_national_estimates(offense)
                results[offense] = response.get("results", response.get("data", []))
            except Exception as e:
                logger.error(f"Failed to fetch national estimates for {offense}: {e}")
                results[offense] = []

        return results

    async def get_all_state_estimates(self, offense: str) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch state estimates for all states for a specific offense."""
        results = {}

        for state in self.STATE_ABBRS:
            try:
                logger.info(f"Fetching {offense} estimates for {state}")
                response = await self.get_state_estimates(state, offense)
                results[state] = response.get("results", response.get("data", []))
            except Exception as e:
                logger.error(f"Failed to fetch estimates for {state}: {e}")
                results[state] = []

        return results


# Data categories for organizing crime data
CRIME_CATEGORIES = {
    "violent_crime": {
        "offenses": ["violent-crime", "homicide", "rape", "robbery", "aggravated-assault"],
        "description": "Violent crime offenses"
    },
    "property_crime": {
        "offenses": ["property-crime", "burglary", "larceny", "motor-vehicle-theft", "arson"],
        "description": "Property crime offenses"
    },
    "special_crimes": {
        "offenses": ["human-trafficking"],
        "description": "Special category crimes"
    }
}
