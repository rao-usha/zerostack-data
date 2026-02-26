"""
BTS NTAD Intermodal Freight Facilities Collector.

Fetches intermodal terminal data from the National Transportation Atlas
Database via the geo.dot.gov ArcGIS REST API.

API: https://geo.dot.gov/server/rest/services/Hosted/
     Intermodal_Freight_Facilities_RailTOFC_TOFC_Locations_BTS/FeatureServer/0/query

No API key required. ArcGIS REST pagination via resultOffset/resultRecordCount.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import IntermodalTerminal
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
    CollectionStatus,
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)

# Possible endpoint names — geo.dot.gov naming is inconsistent
INTERMODAL_ENDPOINTS = [
    "https://geo.dot.gov/server/rest/services/Hosted/Intermodal_Freight_Facilities_RailTOFC_TOFC_Locations_BTS/FeatureServer/0/query",
    "https://geo.dot.gov/server/rest/services/Hosted/Intermodal_Freight_Facilities_Rail_TOFC_COFC_Locations_BTS/FeatureServer/0/query",
    "https://geo.dot.gov/server/rest/services/Hosted/Intermodal_Freight_Facilities/FeatureServer/0/query",
]

# State abbreviation lookup for filtering
STATE_ABBREVIATIONS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}


@register_collector(SiteIntelSource.BTS_NTAD)
class BTSNTADIntermodalCollector(BaseCollector):
    """
    Collector for BTS NTAD intermodal freight terminal data.

    Fetches intermodal (rail TOFC/COFC) terminal locations from
    the National Transportation Atlas Database hosted on geo.dot.gov.
    """

    domain = SiteIntelDomain.TRANSPORT
    source = SiteIntelSource.BTS_NTAD

    default_timeout = 120.0
    rate_limit_delay = 0.3

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)
        self._working_url: Optional[str] = None

    def get_default_base_url(self) -> str:
        return "https://geo.dot.gov/server/rest/services/Hosted"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect intermodal terminal data from BTS NTAD."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting BTS NTAD intermodal terminal data...")

            # Discover working endpoint
            endpoint_url = await self._discover_endpoint()
            if not endpoint_url:
                return self.create_result(
                    status=CollectionStatus.FAILED,
                    error_message="Could not find working intermodal terminals endpoint on geo.dot.gov",
                )

            # Fetch all features with pagination
            all_features = await self._fetch_all_features(endpoint_url, config)
            total_processed = len(all_features)

            if not all_features:
                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=0,
                    processed=0,
                    inserted=0,
                )

            # Transform and dedup by ntad_id
            seen_ids: Dict[str, Dict] = {}
            for feature in all_features:
                transformed = self._transform_terminal(feature)
                if transformed:
                    seen_ids[transformed["ntad_id"]] = transformed

            records = list(seen_ids.values())

            # Filter by states if specified
            if config.states:
                state_set = set(config.states)
                records = [r for r in records if r.get("state") in state_set]

            logger.info(
                f"Transformed {len(records)} intermodal terminal records"
            )

            if records:
                inserted, updated = self.bulk_upsert(
                    IntermodalTerminal,
                    records,
                    unique_columns=["ntad_id"],
                    update_columns=[
                        "name", "operator", "terminal_type", "railroad",
                        "city", "state", "latitude", "longitude",
                        "annual_lifts", "track_miles", "parking_spaces",
                        "has_on_dock_rail", "source", "collected_at",
                    ],
                )
                total_inserted = inserted + updated

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=total_processed,
                processed=len(records),
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"BTS NTAD intermodal collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _discover_endpoint(self) -> Optional[str]:
        """Try known endpoint URLs to find the working one."""
        if self._working_url:
            return self._working_url

        client = await self.get_client()

        for url in INTERMODAL_ENDPOINTS:
            try:
                test_params = {
                    "where": "1=1",
                    "returnCountOnly": "true",
                    "f": "json",
                }
                await self.apply_rate_limit()
                response = await client.get(url, params=test_params)
                if response.status_code == 200:
                    data = response.json()
                    if "count" in data and data["count"] > 0:
                        logger.info(
                            f"Found working endpoint: {url} ({data['count']} features)"
                        )
                        self._working_url = url
                        return url
            except Exception as e:
                logger.debug(f"Endpoint not available: {url} — {e}")
                continue

        # Try service discovery as fallback
        try:
            discovery_url = "https://geo.dot.gov/server/rest/services/Hosted?f=json"
            await self.apply_rate_limit()
            response = await client.get(discovery_url)
            if response.status_code == 200:
                data = response.json()
                services = data.get("services", [])
                for svc in services:
                    name = svc.get("name", "").lower()
                    if "intermodal" in name or "tofc" in name:
                        svc_name = svc.get("name")
                        svc_type = svc.get("type", "FeatureServer")
                        url = f"https://geo.dot.gov/server/rest/services/{svc_name}/{svc_type}/0/query"
                        logger.info(f"Discovered intermodal service: {url}")
                        self._working_url = url
                        return url
        except Exception as e:
            logger.warning(f"Service discovery failed: {e}")

        return None

    async def _fetch_all_features(
        self, url: str, config: CollectionConfig
    ) -> List[Dict]:
        """Fetch all features with ArcGIS pagination."""
        all_features = []
        offset = 0
        page_size = 1000

        while True:
            params = {
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": page_size,
            }

            response = await self._fetch_arcgis(url, params)
            features = response.get("features", [])

            if not features:
                break

            all_features.extend(features)
            logger.info(
                f"Fetched {len(features)} intermodal features (total: {len(all_features)})"
            )

            if len(features) < page_size:
                break

            offset += page_size

        return all_features

    def _transform_terminal(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform ArcGIS feature to IntermodalTerminal record."""
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        # Try various field naming conventions
        terminal_id = (
            attrs.get("NTAD_ID")
            or attrs.get("ntad_id")
            or attrs.get("OBJECTID")
            or attrs.get("objectid")
            or attrs.get("FID")
        )
        if not terminal_id:
            return None

        lat, lng = None, None
        if geometry:
            lng = geometry.get("x")
            lat = geometry.get("y")

        name = (
            attrs.get("FACILITY") or attrs.get("facility")
            or attrs.get("NAME") or attrs.get("name")
            or attrs.get("FAC_NAME") or attrs.get("fac_name")
            or f"Terminal {terminal_id}"
        )

        operator = (
            attrs.get("OPERATOR") or attrs.get("operator")
            or attrs.get("RAILROAD") or attrs.get("railroad")
            or attrs.get("RR") or attrs.get("rr")
        )

        railroad = (
            attrs.get("RAILROAD") or attrs.get("railroad")
            or attrs.get("RR") or attrs.get("rr")
            or attrs.get("RROWNER") or attrs.get("rrowner")
        )

        city = attrs.get("CITY") or attrs.get("city")
        state = attrs.get("STATE") or attrs.get("state") or attrs.get("STFIPS")

        # Normalize state to 2-letter abbreviation
        if state and len(str(state)) > 2:
            state = str(state)[:2].upper()

        terminal_type = (
            attrs.get("TYPE") or attrs.get("type")
            or attrs.get("FAC_TYPE") or attrs.get("fac_type")
            or "ramp"
        )

        return {
            "ntad_id": str(terminal_id),
            "name": name,
            "operator": operator,
            "terminal_type": str(terminal_type).lower() if terminal_type else "ramp",
            "railroad": railroad,
            "city": city,
            "state": state,
            "latitude": lat,
            "longitude": lng,
            "annual_lifts": self._safe_int(
                attrs.get("ANNUAL_LIFTS") or attrs.get("annual_lifts")
                or attrs.get("LIFTS") or attrs.get("lifts")
            ),
            "track_miles": self._safe_float(
                attrs.get("TRACK_MILES") or attrs.get("track_miles")
            ),
            "parking_spaces": self._safe_int(
                attrs.get("PARKING") or attrs.get("parking")
                or attrs.get("SPACES") or attrs.get("spaces")
            ),
            "has_on_dock_rail": None,  # Not typically in NTAD data
            "source": "bts_ntad",
            "collected_at": datetime.utcnow(),
        }

    async def _fetch_arcgis(self, url: str, params: Dict) -> Dict:
        """Fetch from ArcGIS REST endpoint."""
        client = await self.get_client()
        await self.apply_rate_limit()

        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if "error" in data:
                logger.error(f"ArcGIS error: {data['error']}")
                return {"features": []}
            return data
        except Exception as e:
            logger.error(f"ArcGIS request failed: {url} - {e}")
            return {"features": []}

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None or value == "" or value == "-":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None or value == "" or value == "-":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
