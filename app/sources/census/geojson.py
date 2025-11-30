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
        # For now, return empty URL - GeoJSON fetch will be skipped
        # TODO: Implement proper TIGERweb integration or use alternative source
        # Alternative: Use Census Cartographic Boundary Files
        # https://www2.census.gov/geo/tiger/GENZ2020/shp/
        raise NotImplementedError(
            "GeoJSON fetching temporarily disabled. "
            "Will implement using Census Cartographic Boundary files."
        )
    
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
        url = self._build_geojson_url(geo_level, state_fips, county_fips)
        
        logger.info(f"Fetching GeoJSON for {geo_level} level")
        
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    geojson = response.json()
                    
                    # Validate it's a FeatureCollection
                    if geojson.get("type") != "FeatureCollection":
                        logger.warning(f"Response is not a FeatureCollection: {geojson}")
                    
                    feature_count = len(geojson.get("features", []))
                    logger.info(f"Fetched GeoJSON with {feature_count} features")
                    
                    return geojson
            
            except (httpx.HTTPError, httpx.TimeoutException) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"GeoJSON fetch failed, retrying in {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch GeoJSON after {max_retries} attempts: {e}")
                    raise
        
        raise Exception("Failed to fetch GeoJSON")
    
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

