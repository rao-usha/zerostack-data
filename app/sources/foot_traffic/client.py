"""
HTTP clients for foot traffic data sources.

Implements rate-limited clients for:
- SafeGraph: Foot traffic patterns API
- Foursquare: Places API for POI enrichment
- Placer.ai: Retail analytics API
- City Open Data: Public pedestrian counters
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx

from app.core.config import get_settings
from app.sources.foot_traffic.metadata import (
    API_RATE_LIMITS,
    SOURCE_CONFIDENCE_LEVELS,
    CITY_PEDESTRIAN_DATA_SOURCES,
)

logger = logging.getLogger(__name__)


@dataclass
class LocationResult:
    """Result from a location search."""
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    latitude: float
    longitude: float
    category: str
    subcategory: Optional[str] = None
    external_id: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    hours: Optional[Dict[str, Any]] = None


@dataclass
class TrafficObservation:
    """A single foot traffic observation."""
    observation_date: date
    observation_period: str
    visit_count: Optional[int] = None
    visitor_count: Optional[int] = None
    visit_count_relative: Optional[int] = None
    median_dwell_minutes: Optional[float] = None
    hourly_traffic: Optional[Dict[str, int]] = None
    daily_traffic: Optional[Dict[str, int]] = None
    visitor_demographics: Optional[Dict[str, Any]] = None
    source_type: str = ""
    confidence: str = "medium"


class BaseFootTrafficClient(ABC):
    """Base class for foot traffic API clients."""
    
    source_name: str = "base"
    default_timeout: int = 30
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: int = 3
    ):
        self.api_key = api_key
        self.timeout = timeout or self.default_timeout
        self.max_retries = max_retries
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._last_request_time: float = 0
        
        # Get rate limits from metadata
        limits = API_RATE_LIMITS.get(self.source_name, {})
        self.requests_per_minute = limits.get("requests_per_minute", 60)
        self.concurrent_requests = limits.get("concurrent_requests", 5)
        self.min_request_interval = 60.0 / self.requests_per_minute
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.concurrent_requests)
        return self._client
    
    async def _rate_limited_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> httpx.Response:
        """Make a rate-limited HTTP request."""
        client = await self._get_client()
        
        async with self._semaphore:
            # Rate limiting
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_request_time
            if elapsed < self.min_request_interval:
                await asyncio.sleep(self.min_request_interval - elapsed)
            
            self._last_request_time = asyncio.get_event_loop().time()
            
            # Retry logic
            last_error = None
            for attempt in range(self.max_retries):
                try:
                    response = await client.request(method, url, **kwargs)
                    response.raise_for_status()
                    return response
                except httpx.HTTPStatusError as e:
                    last_error = e
                    if e.response.status_code == 429:  # Rate limited
                        retry_after = int(e.response.headers.get("Retry-After", 60))
                        logger.warning(f"Rate limited by {self.source_name}, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                    elif e.response.status_code >= 500:  # Server error
                        wait_time = (2 ** attempt) + (asyncio.get_event_loop().time() % 1)
                        logger.warning(f"Server error from {self.source_name}, retry in {wait_time:.1f}s")
                        await asyncio.sleep(wait_time)
                    else:
                        raise
                except httpx.RequestError as e:
                    last_error = e
                    wait_time = (2 ** attempt) + (asyncio.get_event_loop().time() % 1)
                    logger.warning(f"Request error to {self.source_name}: {e}, retry in {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
            
            raise last_error or Exception(f"Failed after {self.max_retries} retries")
    
    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
    
    @abstractmethod
    async def search_locations(
        self,
        query: str,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        radius_meters: int = 5000,
        limit: int = 20
    ) -> List[LocationResult]:
        """Search for locations matching query."""
        pass


class FoursquareClient(BaseFootTrafficClient):
    """
    Foursquare Places API client.
    
    Used for POI discovery and enrichment.
    API Docs: https://developer.foursquare.com/docs/places-api/
    """
    
    source_name = "foursquare"
    base_url = "https://api.foursquare.com/v3"
    
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        settings = get_settings()
        super().__init__(
            api_key=api_key or settings.get_foursquare_api_key(),
            **kwargs
        )
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with API key."""
        if not self.api_key:
            raise ValueError("Foursquare API key is required")
        return {
            "Authorization": self.api_key,
            "Accept": "application/json",
        }
    
    async def search_locations(
        self,
        query: str,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        radius_meters: int = 5000,
        limit: int = 20
    ) -> List[LocationResult]:
        """
        Search for locations using Foursquare Places API.
        
        Args:
            query: Search query (e.g., "Starbucks")
            latitude: Center latitude for geographic search
            longitude: Center longitude for geographic search
            radius_meters: Search radius in meters
            limit: Maximum number of results
            
        Returns:
            List of LocationResult objects
        """
        url = f"{self.base_url}/places/search"
        params = {
            "query": query,
            "limit": min(limit, 50),  # Foursquare max is 50
        }
        
        if latitude and longitude:
            params["ll"] = f"{latitude},{longitude}"
            params["radius"] = radius_meters
        
        response = await self._rate_limited_request(
            "GET", url,
            headers=self._get_headers(),
            params=params
        )
        data = response.json()
        
        results = []
        for place in data.get("results", []):
            location = place.get("location", {})
            categories = place.get("categories", [])
            
            result = LocationResult(
                name=place.get("name", ""),
                address=location.get("address", ""),
                city=location.get("locality", ""),
                state=location.get("region", ""),
                postal_code=location.get("postcode", ""),
                latitude=place.get("geocodes", {}).get("main", {}).get("latitude", 0),
                longitude=place.get("geocodes", {}).get("main", {}).get("longitude", 0),
                category=categories[0].get("name") if categories else "unknown",
                subcategory=categories[1].get("name") if len(categories) > 1 else None,
                external_id=place.get("fsq_id"),
                phone=place.get("tel"),
                website=place.get("website"),
                hours=place.get("hours"),
            )
            results.append(result)
        
        logger.info(f"Foursquare search for '{query}' returned {len(results)} results")
        return results
    
    async def get_place_details(self, fsq_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific place.
        
        Args:
            fsq_id: Foursquare place ID
            
        Returns:
            Place details dictionary
        """
        url = f"{self.base_url}/places/{fsq_id}"
        params = {
            "fields": "name,location,categories,hours,rating,stats,tips,photos,website,tel,price"
        }
        
        response = await self._rate_limited_request(
            "GET", url,
            headers=self._get_headers(),
            params=params
        )
        return response.json()
    
    async def search_by_address(
        self,
        name: str,
        address: str,
        city: str,
        state: str
    ) -> Optional[LocationResult]:
        """
        Search for a specific location by name and address.
        
        Args:
            name: Business name
            address: Street address
            city: City name
            state: State code
            
        Returns:
            LocationResult if found, None otherwise
        """
        query = f"{name} {address} {city} {state}"
        results = await self.search_locations(query, limit=5)
        
        # Find best match
        for result in results:
            if result.city.lower() == city.lower():
                return result
        
        return results[0] if results else None


class SafeGraphClient(BaseFootTrafficClient):
    """
    SafeGraph Patterns API client.
    
    Provides weekly foot traffic data from mobile location signals.
    API Docs: https://docs.safegraph.com/
    """
    
    source_name = "safegraph"
    base_url = "https://api.safegraph.com/v2"
    
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        settings = get_settings()
        super().__init__(
            api_key=api_key or settings.get_safegraph_api_key(),
            **kwargs
        )
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with API key."""
        if not self.api_key:
            raise ValueError("SafeGraph API key is required")
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
    
    async def search_locations(
        self,
        query: str,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        radius_meters: int = 5000,
        limit: int = 20
    ) -> List[LocationResult]:
        """
        Search for locations in SafeGraph Core Places.
        
        Note: This is a simplified implementation. SafeGraph's actual API
        may differ - consult their documentation for exact endpoints.
        """
        url = f"{self.base_url}/places/search"
        params = {
            "query": query,
            "limit": limit,
        }
        
        if latitude and longitude:
            params["ll"] = f"{latitude},{longitude}"
            params["radius"] = radius_meters
        
        try:
            response = await self._rate_limited_request(
                "GET", url,
                headers=self._get_headers(),
                params=params
            )
            data = response.json()
            
            results = []
            for place in data.get("results", []):
                result = LocationResult(
                    name=place.get("location_name", ""),
                    address=place.get("street_address", ""),
                    city=place.get("city", ""),
                    state=place.get("region", ""),
                    postal_code=place.get("postal_code", ""),
                    latitude=place.get("latitude", 0),
                    longitude=place.get("longitude", 0),
                    category=place.get("top_category", "unknown"),
                    subcategory=place.get("sub_category"),
                    external_id=place.get("placekey"),
                )
                results.append(result)
            
            return results
        except Exception as e:
            logger.error(f"SafeGraph search failed: {e}")
            return []
    
    async def get_traffic_patterns(
        self,
        placekey: str,
        start_date: date,
        end_date: date
    ) -> List[TrafficObservation]:
        """
        Get foot traffic patterns for a location.
        
        Args:
            placekey: SafeGraph placekey identifier
            start_date: Start of date range
            end_date: End of date range
            
        Returns:
            List of weekly traffic observations
        """
        url = f"{self.base_url}/patterns"
        params = {
            "placekey": placekey,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        
        try:
            response = await self._rate_limited_request(
                "GET", url,
                headers=self._get_headers(),
                params=params
            )
            data = response.json()
            
            observations = []
            for pattern in data.get("patterns", []):
                obs = TrafficObservation(
                    observation_date=datetime.strptime(
                        pattern.get("date_range_start"), "%Y-%m-%d"
                    ).date(),
                    observation_period="weekly",
                    visit_count=pattern.get("raw_visit_counts"),
                    visitor_count=pattern.get("raw_visitor_counts"),
                    median_dwell_minutes=pattern.get("median_dwell"),
                    hourly_traffic=pattern.get("popularity_by_hour"),
                    daily_traffic=pattern.get("popularity_by_day"),
                    visitor_demographics=pattern.get("visitor_home_cbgs"),
                    source_type="safegraph",
                    confidence="high",
                )
                observations.append(obs)
            
            return observations
        except Exception as e:
            logger.error(f"SafeGraph patterns fetch failed: {e}")
            return []


class CityDataClient(BaseFootTrafficClient):
    """
    Client for city open data pedestrian counters.
    
    Supports multiple cities with public pedestrian count data.
    """
    
    source_name = "city_data"
    
    def __init__(self, city: str, **kwargs):
        super().__init__(**kwargs)
        self.city = city
        self.config = CITY_PEDESTRIAN_DATA_SOURCES.get(city)
        if not self.config:
            raise ValueError(f"City '{city}' not supported. Available: {list(CITY_PEDESTRIAN_DATA_SOURCES.keys())}")
    
    async def search_locations(
        self,
        query: str,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        radius_meters: int = 5000,
        limit: int = 20
    ) -> List[LocationResult]:
        """City data doesn't support location search - use Foursquare instead."""
        return []
    
    async def get_pedestrian_counts(
        self,
        location_name: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 1000
    ) -> List[TrafficObservation]:
        """
        Get pedestrian count data from city open data portal.
        
        Args:
            location_name: Optional filter by location/intersection name
            start_date: Start of date range
            end_date: End of date range
            limit: Maximum number of records
            
        Returns:
            List of traffic observations
        """
        if not self.config.get("endpoint"):
            logger.warning(f"No endpoint configured for {self.city}")
            return []
        
        base_url = self.config["base_url"]
        endpoint = self.config["endpoint"]
        url = f"{base_url}/{endpoint}"
        
        params = {"$limit": limit}
        
        # Build SoQL query filters
        where_clauses = []
        if start_date:
            where_clauses.append(f"date >= '{start_date.isoformat()}'")
        if end_date:
            where_clauses.append(f"date <= '{end_date.isoformat()}'")
        if location_name:
            where_clauses.append(f"location LIKE '%{location_name}%'")
        
        if where_clauses:
            params["$where"] = " AND ".join(where_clauses)
        
        try:
            response = await self._rate_limited_request("GET", url, params=params)
            data = response.json()
            
            observations = []
            for record in data:
                try:
                    obs_date = datetime.strptime(
                        record.get("date", "")[:10], "%Y-%m-%d"
                    ).date()
                    obs = TrafficObservation(
                        observation_date=obs_date,
                        observation_period="daily",
                        visit_count=int(record.get("count", 0)),
                        source_type="city_data",
                        confidence="high",
                    )
                    observations.append(obs)
                except (ValueError, TypeError) as e:
                    logger.debug(f"Skipping invalid record: {e}")
                    continue
            
            return observations
        except Exception as e:
            logger.error(f"City data fetch failed for {self.city}: {e}")
            return []


class FootTrafficClient:
    """
    Unified foot traffic client that combines multiple sources.
    
    Orchestrates data collection from available sources based on configuration.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self._clients: Dict[str, BaseFootTrafficClient] = {}
    
    def _get_foursquare_client(self) -> Optional[FoursquareClient]:
        """Get Foursquare client if API key is configured."""
        if "foursquare" not in self._clients:
            if self.settings.get_foursquare_api_key():
                self._clients["foursquare"] = FoursquareClient()
            else:
                return None
        return self._clients.get("foursquare")
    
    def _get_safegraph_client(self) -> Optional[SafeGraphClient]:
        """Get SafeGraph client if API key is configured."""
        if "safegraph" not in self._clients:
            if self.settings.get_safegraph_api_key():
                self._clients["safegraph"] = SafeGraphClient()
            else:
                return None
        return self._clients.get("safegraph")
    
    def get_available_sources(self) -> List[str]:
        """Get list of available data sources based on configuration."""
        sources = []
        
        if self.settings.get_foursquare_api_key():
            sources.append("foursquare")
        if self.settings.get_safegraph_api_key():
            sources.append("safegraph")
        if self.settings.get_placer_api_key():
            sources.append("placer")
        if self.settings.is_google_scraping_enabled():
            sources.append("google")
        
        # City data is always available (no API key required)
        sources.append("city_data")
        
        return sources
    
    async def discover_locations(
        self,
        brand_name: str,
        city: Optional[str] = None,
        state: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        limit: int = 50
    ) -> List[LocationResult]:
        """
        Discover locations for a brand using available sources.
        
        Args:
            brand_name: Brand name to search for
            city: Optional city filter
            state: Optional state filter
            latitude: Optional center latitude
            longitude: Optional center longitude
            limit: Maximum number of results
            
        Returns:
            List of discovered locations
        """
        all_results = []
        
        # Build search query
        query = brand_name
        if city:
            query = f"{brand_name} {city}"
        if state:
            query = f"{query} {state}"
        
        # Try Foursquare first (best for POI discovery)
        foursquare = self._get_foursquare_client()
        if foursquare:
            try:
                results = await foursquare.search_locations(
                    query=query,
                    latitude=latitude,
                    longitude=longitude,
                    limit=limit
                )
                all_results.extend(results)
                logger.info(f"Foursquare found {len(results)} locations for '{brand_name}'")
            except Exception as e:
                logger.warning(f"Foursquare search failed: {e}")
        
        # Try SafeGraph if available
        safegraph = self._get_safegraph_client()
        if safegraph and len(all_results) < limit:
            try:
                results = await safegraph.search_locations(
                    query=query,
                    latitude=latitude,
                    longitude=longitude,
                    limit=limit - len(all_results)
                )
                all_results.extend(results)
                logger.info(f"SafeGraph found {len(results)} locations for '{brand_name}'")
            except Exception as e:
                logger.warning(f"SafeGraph search failed: {e}")
        
        return all_results[:limit]
    
    async def close(self):
        """Close all client connections."""
        for client in self._clients.values():
            await client.close()
