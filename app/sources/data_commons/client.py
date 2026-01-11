"""
Google Data Commons API client with rate limiting and retry logic.

Official API documentation:
https://docs.datacommons.org/api/rest/v2

Data Commons provides unified access to public data from 200+ sources including:
- US Census Bureau
- World Bank
- CDC
- FBI
- Bureau of Labor Statistics
- And many more

Rate limits:
- API key REQUIRED for all requests (as of 2025)
- Higher rate limits with registered key
- Conservative default: 10 requests/second

API Key (REQUIRED):
Get at: https://apikeys.datacommons.org
"""
import logging
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class DataCommonsClient(BaseAPIClient):
    """
    HTTP client for Google Data Commons API V2 with bounded concurrency and rate limiting.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "data_commons"
    BASE_URL = "https://api.datacommons.org/v2"

    def __init__(
        self,
        api_key: Optional[str] = None,
        max_concurrency: int = 5,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize Data Commons API client.

        Args:
            api_key: API key for higher rate limits (recommended)
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("data_commons")

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
        headers = {}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        return headers

    # ========== Statistical Variable Methods ==========

    async def get_stat_var_info(self, stat_var: str) -> Dict[str, Any]:
        """Get information about a statistical variable."""
        params = {
            "nodes": stat_var,
            "property": "<-"
        }
        return await self.get("node", params=params, resource_id=f"StatVarInfo:{stat_var}")

    async def get_observation(
        self,
        variable: str,
        entity: str,
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get observation data for a statistical variable at a specific entity."""
        body = {
            "variable": {"dcids": [variable]},
            "entity": {"dcids": [entity]},
            "select": ["variable", "entity", "value", "date"]
        }
        if date:
            body["date"] = date

        return await self.post(
            "observation",
            json_body=body,
            resource_id=f"Observation:{variable}:{entity}"
        )

    async def get_stat_all(
        self,
        places: List[str],
        stat_vars: List[str]
    ) -> Dict[str, Any]:
        """Get statistical data for multiple places and variables."""
        body = {
            "variable": {"dcids": stat_vars},
            "entity": {"dcids": places},
            "select": ["variable", "entity", "value", "date"]
        }

        return await self.post(
            "observation",
            json_body=body,
            resource_id=f"StatAll:{len(places)}places:{len(stat_vars)}vars"
        )

    # ========== Place/Entity Methods ==========

    async def get_places_in(
        self,
        parent_place: str,
        child_type: str
    ) -> Dict[str, Any]:
        """Get child places within a parent place."""
        params = {
            "nodes": parent_place,
            "property": f"<-containedInPlace+{{typeOf:{child_type}}}"
        }

        return await self.get(
            "node",
            params=params,
            resource_id=f"PlacesIn:{parent_place}:{child_type}"
        )

    async def get_place_info(self, place_dcid: str) -> Dict[str, Any]:
        """Get information about a place."""
        params = {
            "nodes": place_dcid,
            "property": "->"
        }

        return await self.get("node", params=params, resource_id=f"PlaceInfo:{place_dcid}")

    async def search_places(
        self,
        query: str,
        type_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Search for places by name."""
        params = {
            "nodes": query,
            "property": "<-description"
        }

        if type_filter:
            params["property"] += f"{{typeOf:{type_filter}}}"

        return await self.get("resolve", params=params, resource_id=f"SearchPlaces:{query}")

    # ========== Time Series Methods ==========

    async def get_stat_series(
        self,
        place: str,
        stat_var: str
    ) -> Dict[str, Any]:
        """Get time series data for a statistical variable."""
        body = {
            "variable": {"dcids": [stat_var]},
            "entity": {"dcids": [place]},
            "select": ["variable", "entity", "value", "date"]
        }

        return await self.post(
            "observation",
            json_body=body,
            resource_id=f"StatSeries:{place}:{stat_var}"
        )

    # ========== Bulk Data Methods ==========

    async def bulk_observations(
        self,
        variables: List[str],
        entities: List[str],
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Bulk fetch observations for multiple variables and entities."""
        body = {
            "variable": {"dcids": variables},
            "entity": {"dcids": entities},
            "select": ["variable", "entity", "value", "date"]
        }

        if date:
            body["date"] = date

        return await self.post(
            "observation",
            json_body=body,
            resource_id=f"BulkObs:{len(variables)}vars:{len(entities)}entities"
        )

    # ========== SPARQL Query Method ==========

    async def sparql_query(self, query: str) -> Dict[str, Any]:
        """Execute a SPARQL query against Data Commons knowledge graph."""
        body = {"sparql": query}
        return await self.post("sparql", json_body=body, resource_id="SPARQL")


# Common Statistical Variables Reference
STATISTICAL_VARIABLES = {
    "Count_Person": "Total population",
    "Count_Person_Male": "Male population",
    "Count_Person_Female": "Female population",
    "Median_Age_Person": "Median age",
    "Count_Household": "Number of households",
    "Median_Income_Person": "Median income",
    "Median_Income_Household": "Median household income",
    "Count_Person_Employed": "Employed persons",
    "UnemploymentRate_Person": "Unemployment rate",
    "Count_CriminalActivities_CombinedCrime": "Total crimes",
}

# Common Place DCIDs
PLACE_DCIDS = {
    "USA": "country/USA",
    "California": "geoId/06",
    "Texas": "geoId/48",
    "New York": "geoId/36",
    "Florida": "geoId/12",
}
