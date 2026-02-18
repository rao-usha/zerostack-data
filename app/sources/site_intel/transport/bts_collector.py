"""
BTS Transportation Collector.

Fetches transportation infrastructure data from Bureau of Transportation Statistics:
- Major ports (tonnage data)
- Airports with cargo facilities

Data sources:
- National Transportation Atlas Database (NTAD) via geo.dot.gov Hosted FeatureServer
- BTS Open Data Portal

No API key required - public data via ArcGIS REST services.
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import IntermodalTerminal, Port, Airport
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


@register_collector(SiteIntelSource.BTS)
class BTSTransportCollector(BaseCollector):
    """
    Collector for BTS/NTAD transportation infrastructure data.

    Fetches:
    - Major ports (tonnage, import/export data)
    - Airports (FAA facilities database)
    """

    domain = SiteIntelDomain.TRANSPORT
    source = SiteIntelSource.BTS

    # BTS API configuration
    default_timeout = 120.0
    rate_limit_delay = 0.3

    # Updated NTAD ArcGIS REST endpoints (Hosted FeatureServer)
    PORTS_URL = "https://geo.dot.gov/server/rest/services/Hosted/Major_Ports/FeatureServer/0/query"
    AIRPORTS_URL = "https://geo.dot.gov/server/rest/services/Hosted/Airports_/FeatureServer/0/query"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://geo.dot.gov/server/rest/services/Hosted"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute BTS data collection.

        Collects ports and airports.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            # Collect ports
            logger.info("Collecting BTS port data...")
            ports_result = await self._collect_ports(config)
            total_inserted += ports_result.get("inserted", 0)
            total_processed += ports_result.get("processed", 0)
            if ports_result.get("error"):
                errors.append({"source": "ports", "error": ports_result["error"]})

            # Collect airports
            logger.info("Collecting BTS airport data...")
            airports_result = await self._collect_airports(config)
            total_inserted += airports_result.get("inserted", 0)
            total_processed += airports_result.get("processed", 0)
            if airports_result.get("error"):
                errors.append({"source": "airports", "error": airports_result["error"]})

            status = (
                CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL
            )

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"BTS collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_ports(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect major ports from NTAD Hosted FeatureServer.
        """
        try:
            all_ports = []
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

                response = await self._fetch_arcgis(self.PORTS_URL, params)
                features = response.get("features", [])

                if not features:
                    break

                all_ports.extend(features)
                logger.info(
                    f"Fetched {len(features)} port records (total: {len(all_ports)})"
                )

                if len(features) < page_size:
                    break

                offset += page_size

            # Transform and dedup records
            seen_codes = {}
            for feature in all_ports:
                transformed = self._transform_port(feature)
                if transformed:
                    seen_codes[transformed["port_code"]] = transformed
            records = list(seen_codes.values())

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    Port,
                    records,
                    unique_columns=["port_code"],
                    update_columns=[
                        "name",
                        "city",
                        "state",
                        "latitude",
                        "longitude",
                        "port_type",
                        "has_container_terminal",
                        "has_bulk_terminal",
                        "channel_depth_ft",
                        "collected_at",
                    ],
                )
                return {"processed": len(all_ports), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect ports: {e}", exc_info=True)
            try:
                self.db.rollback()
            except Exception:
                pass
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_port(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform NTAD port feature to database format."""
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        # New hosted service uses lowercase field names
        port_id = attrs.get("port") or attrs.get("PORT") or attrs.get("objectid")
        if not port_id:
            return None

        lat, lng = None, None
        if geometry:
            lng = geometry.get("x")
            lat = geometry.get("y")

        total = self._safe_int(attrs.get("total") or attrs.get("TOTAL")) or 0
        foreign = self._safe_int(attrs.get("foreign_") or attrs.get("FOREIGN")) or 0

        # Parse state from port_name (e.g. "Unalaska Island, AK")
        port_name = (
            attrs.get("port_name") or attrs.get("PORT_NAME") or f"Port {port_id}"
        )
        state = None
        city = None
        if ", " in port_name:
            parts = port_name.rsplit(", ", 1)
            city = parts[0]
            state = parts[1].strip()[:2] if len(parts) > 1 else None

        return {
            "port_code": str(port_id),
            "name": port_name,
            "city": city,
            "state": state,
            "latitude": lat,
            "longitude": lng,
            "port_type": self._determine_port_type(total, foreign),
            "has_container_terminal": foreign > total * 0.3 if total > 0 else None,
            "has_bulk_terminal": total > 1000000,
            "channel_depth_ft": None,
            "source": "bts_ntad",
            "collected_at": datetime.utcnow(),
        }

    def _determine_port_type(self, total: int, foreign: int) -> str:
        """Determine port type from tonnage data."""
        if foreign > total * 0.5:
            return "international"
        elif total > 10000000:  # > 10M tons
            return "major"
        else:
            return "regional"

    async def _collect_airports(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect airports from NTAD Hosted FeatureServer.
        """
        try:
            all_airports = []
            offset = 0
            page_size = 2000

            # Filter for airports only (new service uses lowercase field names)
            state_filter = "fac_type = 'AIRPORT'"
            if config.states:
                state_list = ", ".join(f"'{s}'" for s in config.states)
                state_filter += f" AND state IN ({state_list})"

            while True:
                params = {
                    "where": state_filter,
                    "outFields": "*",
                    "returnGeometry": "true",
                    "outSR": "4326",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                response = await self._fetch_arcgis(self.AIRPORTS_URL, params)
                features = response.get("features", [])

                if not features:
                    break

                all_airports.extend(features)
                logger.info(
                    f"Fetched {len(features)} airport records (total: {len(all_airports)})"
                )

                if len(features) < page_size:
                    break

                offset += page_size

            # Transform and dedup by FAA code
            seen_codes = {}
            for feature in all_airports:
                transformed = self._transform_airport(feature)
                if transformed:
                    seen_codes[transformed["faa_code"]] = transformed
            records = list(seen_codes.values())

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    Airport,
                    records,
                    unique_columns=["faa_code"],
                    update_columns=[
                        "icao_code",
                        "name",
                        "city",
                        "state",
                        "latitude",
                        "longitude",
                        "airport_type",
                        "has_cargo_facility",
                        "longest_runway_ft",
                        "collected_at",
                    ],
                )
                return {"processed": len(all_airports), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect airports: {e}", exc_info=True)
            try:
                self.db.rollback()
            except Exception:
                pass
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_airport(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform NTAD airport feature to database format."""
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        # New hosted service uses lowercase field names
        faa_id = (
            attrs.get("locid")
            or attrs.get("loc_id")
            or attrs.get("LOCID")
            or attrs.get("FAA_ID")
        )
        if not faa_id:
            return None

        lat, lng = None, None
        if geometry:
            lng = geometry.get("x")
            lat = geometry.get("y")

        # Determine if airport has cargo based on hub classification
        hub = (attrs.get("hub") or attrs.get("HUB") or "").upper()
        has_cargo = hub in ("L", "M", "S")  # Large, Medium, Small hub

        return {
            "faa_code": faa_id,
            "icao_code": attrs.get("icao_identifier") or attrs.get("ICAO_ID"),
            "name": (
                attrs.get("fac_name")
                or attrs.get("FULLNAME")
                or attrs.get("airport")
                or f"Airport {faa_id}"
            ),
            "city": attrs.get("city") or attrs.get("CITY"),
            "state": attrs.get("state") or attrs.get("STATE"),
            "latitude": lat,
            "longitude": lng,
            "airport_type": self._determine_airport_type(attrs),
            "has_cargo_facility": has_cargo,
            "longest_runway_ft": None,
            "source": "bts_ntad",
            "collected_at": datetime.utcnow(),
        }

    def _determine_airport_type(self, attrs: Dict[str, Any]) -> str:
        """Determine airport type from hub classification."""
        hub = (attrs.get("hub") or attrs.get("HUB") or "").upper()
        role = (attrs.get("role") or attrs.get("ROLE") or "").lower()

        if hub == "L":
            return "large_hub"
        elif hub == "M":
            return "medium_hub"
        elif hub == "S":
            return "small_hub"
        elif hub == "N":
            return "non_hub"
        elif "reliever" in role:
            return "reliever"
        elif "general" in role:
            return "general_aviation"
        else:
            return "other"

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

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
