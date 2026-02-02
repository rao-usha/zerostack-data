"""
BTS Transportation Collector.

Fetches transportation infrastructure data from Bureau of Transportation Statistics:
- Intermodal terminals (rail/truck)
- Ports and waterways
- Airports with cargo facilities

Data sources:
- National Transportation Atlas Database (NTAD)
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
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)


@register_collector(SiteIntelSource.BTS)
class BTSTransportCollector(BaseCollector):
    """
    Collector for BTS/NTAD transportation infrastructure data.

    Fetches:
    - Intermodal terminals (rail-truck transfer facilities)
    - Major ports
    - Airports with cargo facilities
    """

    domain = SiteIntelDomain.TRANSPORT
    source = SiteIntelSource.BTS

    # BTS API configuration
    default_timeout = 120.0
    rate_limit_delay = 0.3

    # NTAD ArcGIS REST endpoints
    INTERMODAL_URL = "https://geo.dot.gov/server/rest/services/NTAD/Intermodal_Freight_Facilities/MapServer/0/query"
    PORTS_URL = "https://geo.dot.gov/server/rest/services/NTAD/Principal_Ports/MapServer/0/query"
    AIRPORTS_URL = "https://geo.dot.gov/server/rest/services/NTAD/Aviation_Facilities/MapServer/0/query"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://geo.dot.gov/server/rest/services/NTAD"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute BTS data collection.

        Collects intermodal terminals, ports, and airports.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            # Collect intermodal terminals
            logger.info("Collecting BTS intermodal terminal data...")
            intermodal_result = await self._collect_intermodal_terminals(config)
            total_inserted += intermodal_result.get("inserted", 0)
            total_processed += intermodal_result.get("processed", 0)
            if intermodal_result.get("error"):
                errors.append({"source": "intermodal", "error": intermodal_result["error"]})

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

            status = CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL

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

    async def _collect_intermodal_terminals(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect intermodal freight facilities from NTAD.
        """
        try:
            all_terminals = []
            offset = 0
            page_size = 1000

            # Build state filter
            state_filter = ""
            if config.states:
                state_list = ", ".join(f"'{s}'" for s in config.states)
                state_filter = f"STATE IN ({state_list})"

            while True:
                params = {
                    "where": state_filter if state_filter else "1=1",
                    "outFields": "*",
                    "returnGeometry": "true",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                response = await self._fetch_arcgis(self.INTERMODAL_URL, params)
                features = response.get("features", [])

                if not features:
                    break

                all_terminals.extend(features)
                logger.info(f"Fetched {len(features)} intermodal records (total: {len(all_terminals)})")

                if len(features) < page_size:
                    break

                offset += page_size

            # Transform records
            records = []
            for feature in all_terminals:
                transformed = self._transform_intermodal(feature)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    IntermodalTerminal,
                    records,
                    unique_columns=["ntad_id"],
                    update_columns=[
                        "name", "city", "state", "county", "latitude", "longitude",
                        "railroad", "terminal_type", "has_container", "has_trailer",
                        "collected_at"
                    ],
                )
                return {"processed": len(all_terminals), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect intermodal terminals: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_intermodal(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform NTAD intermodal feature to database format."""
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        ntad_id = attrs.get("OBJECTID") or attrs.get("ID")
        if not ntad_id:
            return None

        lat, lng = None, None
        if geometry:
            lng = geometry.get("x")
            lat = geometry.get("y")

        return {
            "ntad_id": str(ntad_id),
            "name": attrs.get("NAME") or attrs.get("FACILITY"),
            "city": attrs.get("CITY"),
            "state": attrs.get("STATE") or attrs.get("STATEABBR"),
            "county": attrs.get("COUNTY"),
            "latitude": lat,
            "longitude": lng,
            "railroad": attrs.get("RAILROAD") or attrs.get("RR"),
            "terminal_type": self._determine_terminal_type(attrs),
            "has_container": attrs.get("CONTAINER") == "Y" or attrs.get("CONTAINER") == 1,
            "has_trailer": attrs.get("TRAILER") == "Y" or attrs.get("TRAILER") == 1,
            "source": "bts_ntad",
            "collected_at": datetime.utcnow(),
        }

    def _determine_terminal_type(self, attrs: Dict[str, Any]) -> str:
        """Determine terminal type from attributes."""
        facility_type = attrs.get("FACTYPE") or attrs.get("TYPE") or ""
        facility_type = facility_type.lower()

        if "rail" in facility_type and "truck" in facility_type:
            return "rail_truck"
        elif "rail" in facility_type:
            return "rail"
        elif "truck" in facility_type:
            return "truck"
        elif "port" in facility_type or "marine" in facility_type:
            return "port"
        else:
            return "intermodal"

    async def _collect_ports(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect principal ports from NTAD.
        """
        try:
            all_ports = []
            offset = 0
            page_size = 1000

            state_filter = ""
            if config.states:
                state_list = ", ".join(f"'{s}'" for s in config.states)
                state_filter = f"STATE_POST IN ({state_list})"

            while True:
                params = {
                    "where": state_filter if state_filter else "1=1",
                    "outFields": "*",
                    "returnGeometry": "true",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                response = await self._fetch_arcgis(self.PORTS_URL, params)
                features = response.get("features", [])

                if not features:
                    break

                all_ports.extend(features)
                logger.info(f"Fetched {len(features)} port records (total: {len(all_ports)})")

                if len(features) < page_size:
                    break

                offset += page_size

            # Transform records
            records = []
            for feature in all_ports:
                transformed = self._transform_port(feature)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    Port,
                    records,
                    unique_columns=["ntad_port_id"],
                    update_columns=[
                        "name", "city", "state", "latitude", "longitude",
                        "port_type", "total_tons", "domestic_tons", "foreign_tons",
                        "imports_tons", "exports_tons", "collected_at"
                    ],
                )
                return {"processed": len(all_ports), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect ports: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_port(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform NTAD port feature to database format."""
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        port_id = attrs.get("PORT_ID") or attrs.get("OBJECTID")
        if not port_id:
            return None

        lat, lng = None, None
        if geometry:
            lng = geometry.get("x")
            lat = geometry.get("y")

        return {
            "ntad_port_id": str(port_id),
            "name": attrs.get("PORT_NAME") or attrs.get("NAME"),
            "city": attrs.get("CITY"),
            "state": attrs.get("STATE_POST") or attrs.get("STATE"),
            "latitude": lat,
            "longitude": lng,
            "port_type": self._determine_port_type(attrs),
            "total_tons": self._safe_int(attrs.get("TOTAL")),
            "domestic_tons": self._safe_int(attrs.get("DOMESTIC")),
            "foreign_tons": self._safe_int(attrs.get("FOREIGN")),
            "imports_tons": self._safe_int(attrs.get("IMPORTS")),
            "exports_tons": self._safe_int(attrs.get("EXPORTS")),
            "source": "bts_ntad",
            "collected_at": datetime.utcnow(),
        }

    def _determine_port_type(self, attrs: Dict[str, Any]) -> str:
        """Determine port type from attributes."""
        total = self._safe_int(attrs.get("TOTAL")) or 0
        foreign = self._safe_int(attrs.get("FOREIGN")) or 0

        if foreign > total * 0.5:
            return "international"
        elif total > 10000000:  # > 10M tons
            return "major"
        else:
            return "regional"

    async def _collect_airports(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect airports from NTAD, focusing on those with cargo facilities.
        """
        try:
            all_airports = []
            offset = 0
            page_size = 2000

            # Focus on larger airports that likely have cargo
            state_filter = "FAC_TYPE = 'AIRPORT'"
            if config.states:
                state_list = ", ".join(f"'{s}'" for s in config.states)
                state_filter += f" AND STATE IN ({state_list})"

            while True:
                params = {
                    "where": state_filter,
                    "outFields": "*",
                    "returnGeometry": "true",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                response = await self._fetch_arcgis(self.AIRPORTS_URL, params)
                features = response.get("features", [])

                if not features:
                    break

                all_airports.extend(features)
                logger.info(f"Fetched {len(features)} airport records (total: {len(all_airports)})")

                if len(features) < page_size:
                    break

                offset += page_size

            # Transform records
            records = []
            for feature in all_airports:
                transformed = self._transform_airport(feature)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    Airport,
                    records,
                    unique_columns=["faa_id"],
                    update_columns=[
                        "icao_id", "name", "city", "state", "county",
                        "latitude", "longitude", "airport_type", "ownership",
                        "has_cargo_facility", "runway_length_ft", "collected_at"
                    ],
                )
                return {"processed": len(all_airports), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect airports: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_airport(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform NTAD airport feature to database format."""
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        faa_id = attrs.get("LOCID") or attrs.get("FAA_ID") or attrs.get("IDENT")
        if not faa_id:
            return None

        lat, lng = None, None
        if geometry:
            lng = geometry.get("x")
            lat = geometry.get("y")

        # Determine if airport has cargo - based on airport class/type
        airport_use = (attrs.get("USE") or "").upper()
        airport_class = (attrs.get("CLASS") or "").upper()
        has_cargo = (
            "CARGO" in airport_use or
            "COMMERCIAL" in airport_class or
            attrs.get("COMMERCIAL") == "Y" or
            (attrs.get("ENPLANEMENTS") or 0) > 10000
        )

        return {
            "faa_id": faa_id,
            "icao_id": attrs.get("ICAO_ID") or attrs.get("ICAO"),
            "name": attrs.get("FULLNAME") or attrs.get("NAME") or attrs.get("ARPT_NAME"),
            "city": attrs.get("CITY"),
            "state": attrs.get("STATE") or attrs.get("STATE_CODE"),
            "county": attrs.get("COUNTY"),
            "latitude": lat,
            "longitude": lng,
            "airport_type": self._determine_airport_type(attrs),
            "ownership": attrs.get("OWNERSHIP") or attrs.get("OWNER_TYPE"),
            "has_cargo_facility": has_cargo,
            "runway_length_ft": self._safe_int(attrs.get("MAX_RUNWAY_LENGTH") or attrs.get("RUNWAY_LENGTH")),
            "source": "bts_ntad",
            "collected_at": datetime.utcnow(),
        }

    def _determine_airport_type(self, attrs: Dict[str, Any]) -> str:
        """Determine airport type from attributes."""
        fac_type = (attrs.get("FAC_TYPE") or "").lower()
        airport_class = (attrs.get("CLASS") or "").lower()

        if "large" in airport_class or "hub" in airport_class:
            return "large_hub"
        elif "medium" in airport_class:
            return "medium_hub"
        elif "small" in airport_class:
            return "small_hub"
        elif "reliever" in airport_class:
            return "reliever"
        elif "general" in fac_type:
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
            return response.json()
        except Exception as e:
            logger.error(f"ArcGIS request failed: {url} - {e}")
            raise

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
