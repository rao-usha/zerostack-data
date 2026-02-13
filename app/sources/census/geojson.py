"""
Fetch GeoJSON boundaries from Census TIGERweb services.

Uses the Census Bureau's TIGERweb REST API for geographic boundaries.
"""
import logging
from typing import Dict, Any, List, Optional
import httpx
import asyncio

logger = logging.getLogger(__name__)


class GeoJSONFetcher:
    """
    Fetch GeoJSON boundaries from Census TIGERweb services.
    
    TIGERweb REST API: https://tigerweb.geo.census.gov/arcgis/rest/services/
    """
    
    # TIGERweb base URLs for different vintages
    BASE_URL = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb"

    # Map geo levels to TIGERweb service names
    GEO_SERVICES = {
        "state": "State_2020",
        "county": "County_2020",
        "tract": "Census_Tracts_2020",
        "zip code tabulation area": "ZCTA_2020"
    }

    # Map geo levels to TIGERweb MapServer layer IDs
    LAYER_IDS = {
        "state": 80,
        "county": 82,
        "tract": 8,
        "zip code tabulation area": 2,
    }

    # FIPS code field names per layer
    FIPS_FIELDS = {
        "state": "STATE",
        "county": "STATE",
        "tract": "STATE",
        "zip code tabulation area": None,
    }

    COUNTY_FIPS_FIELDS = {
        "county": "COUNTY",
        "tract": "COUNTY",
    }
    
    def __init__(self, year: int = 2020):
        """
        Initialize GeoJSON fetcher.
        
        Args:
            year: Vintage year for boundaries (default: 2020)
        """
        self.year = year
    
    def _build_geojson_url(
        self,
        geo_level: str,
        state_fips: Optional[str] = None,
        county_fips: Optional[str] = None
    ) -> str:
        """
        Build TIGERweb URL for GeoJSON fetch.
        
        Note: For now using simplified/smaller GeoJSON sources.
        Production would use full TIGER/Line shapefiles.
        
        Args:
            geo_level: Geographic level (state, county, tract, zip code tabulation area)
            state_fips: State FIPS code for filtering
            county_fips: County FIPS code for filtering
            
        Returns:
            Full URL for GeoJSON fetch
        """
        layer_id = self.LAYER_IDS.get(geo_level)
        if layer_id is None:
            raise ValueError(
                f"Unsupported geo_level '{geo_level}'. "
                f"Supported: {list(self.LAYER_IDS.keys())}"
            )

        # Build WHERE clause for FIPS filtering
        where_parts = []
        fips_field = self.FIPS_FIELDS.get(geo_level)
        if state_fips and fips_field:
            where_parts.append(f"{fips_field}='{state_fips}'")
        county_field = self.COUNTY_FIPS_FIELDS.get(geo_level)
        if county_fips and county_field:
            where_parts.append(f"{county_field}='{county_fips}'")

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"

        url = (
            f"{self.BASE_URL}/tigerWMS_Current/MapServer/{layer_id}/query"
            f"?where={where_clause}"
            f"&outFields=*"
            f"&f=geojson"
            f"&resultRecordCount=1000"
        )
        return url
    
    async def fetch_geojson(
        self,
        geo_level: str,
        state_fips: Optional[str] = None,
        county_fips: Optional[str] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Fetch GeoJSON for a geographic level.
        
        Args:
            geo_level: Geographic level (state, county, tract, zip code tabulation area)
            state_fips: Optional state FIPS filter
            county_fips: Optional county FIPS filter
            max_retries: Maximum retry attempts
            
        Returns:
            GeoJSON FeatureCollection
            
        Raises:
            httpx.HTTPError: On fetch errors
        """
        base_url = self._build_geojson_url(geo_level, state_fips, county_fips)

        logger.info(f"Fetching GeoJSON for {geo_level} level")

        all_features = []
        offset = 0
        page_size = 1000

        async with httpx.AsyncClient(timeout=60.0) as client:
            while True:
                url = f"{base_url}&resultOffset={offset}"

                for attempt in range(max_retries):
                    try:
                        response = await client.get(url)
                        response.raise_for_status()
                        page = response.json()
                        break
                    except (httpx.HTTPError, httpx.TimeoutException) as e:
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.warning(f"GeoJSON fetch failed, retrying in {wait_time}s: {e}")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"Failed to fetch GeoJSON after {max_retries} attempts: {e}")
                            raise
                else:
                    raise Exception("Failed to fetch GeoJSON")

                features = page.get("features", [])
                all_features.extend(features)
                logger.info(f"Fetched {len(features)} features (offset={offset}, total={len(all_features)})")

                if len(features) < page_size:
                    break
                offset += page_size
                await asyncio.sleep(0.5)

        geojson = {
            "type": "FeatureCollection",
            "features": all_features,
        }
        logger.info(f"Fetched GeoJSON with {len(all_features)} total features")
        return geojson
    
    async def fetch_simplified_geojson(
        self,
        geo_level: str,
        state_fips: Optional[str] = None,
        county_fips: Optional[str] = None,
        simplification_tolerance: float = 0.001
    ) -> Dict[str, Any]:
        """
        Fetch simplified GeoJSON (smaller file size, less detail).
        
        Useful for large geographic areas (many tracts, counties, etc).
        
        Args:
            geo_level: Geographic level
            state_fips: Optional state FIPS filter
            county_fips: Optional county FIPS filter
            simplification_tolerance: Simplification tolerance (higher = simpler)
            
        Returns:
            Simplified GeoJSON FeatureCollection
        """
        # For now, just fetch regular GeoJSON
        # Future: implement simplification algorithm or use generalized boundaries
        geojson = await self.fetch_geojson(geo_level, state_fips, county_fips)
        
        # Note: For production, consider using mapshaper or similar for simplification
        # Or use Census generalized boundaries
        
        return geojson

