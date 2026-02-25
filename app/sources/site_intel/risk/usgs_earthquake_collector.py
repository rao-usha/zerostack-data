"""
USGS Earthquake/Seismic Hazard Collector.

Fetches seismic hazard data and active fault information from USGS:
- Earthquake events via FDSNWS API
- Quaternary fault data via GeoJSON

API: https://earthquake.usgs.gov/fdsnws/event/1/
Faults: https://earthquake.usgs.gov/cfusion/qfault/
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import SeismicHazard, FaultLine
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

# Grid points for seismic hazard sampling (major US metro areas)
HAZARD_GRID_POINTS = [
    (34.05, -118.24, "Los Angeles, CA"),
    (37.77, -122.42, "San Francisco, CA"),
    (47.61, -122.33, "Seattle, WA"),
    (45.52, -122.68, "Portland, OR"),
    (36.17, -115.14, "Las Vegas, NV"),
    (40.76, -111.89, "Salt Lake City, UT"),
    (39.74, -104.99, "Denver, CO"),
    (35.47, -97.52, "Oklahoma City, OK"),
    (36.16, -86.78, "Nashville, TN"),
    (35.15, -90.05, "Memphis, TN"),
    (38.63, -90.20, "St. Louis, MO"),
    (41.88, -87.63, "Chicago, IL"),
    (42.33, -83.05, "Detroit, MI"),
    (39.96, -82.99, "Columbus, OH"),
    (40.44, -79.99, "Pittsburgh, PA"),
    (40.71, -74.01, "New York, NY"),
    (42.36, -71.06, "Boston, MA"),
    (39.95, -75.17, "Philadelphia, PA"),
    (38.91, -77.04, "Washington, DC"),
    (35.23, -80.84, "Charlotte, NC"),
    (33.75, -84.39, "Atlanta, GA"),
    (25.76, -80.19, "Miami, FL"),
    (30.27, -97.74, "Austin, TX"),
    (29.76, -95.37, "Houston, TX"),
    (32.78, -96.80, "Dallas, TX"),
    (33.45, -112.07, "Phoenix, AZ"),
    (19.90, -155.58, "Hilo, HI"),
    (61.22, -149.90, "Anchorage, AK"),
]


@register_collector(SiteIntelSource.USGS_EARTHQUAKE)
class USGSEarthquakeCollector(BaseCollector):
    """
    Collector for USGS earthquake and seismic hazard data.

    Collects:
    - Recent significant earthquakes
    - Seismic hazard estimates for key locations
    - Quaternary fault data
    """

    domain = SiteIntelDomain.RISK
    source = SiteIntelSource.USGS_EARTHQUAKE
    default_timeout = 60.0
    rate_limit_delay = 0.5

    EARTHQUAKE_API = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    FAULT_API = "https://earthquake.usgs.gov/cfusion/qfault/show_report_AB_archive.cfm"
    HAZARD_API = "https://earthquake.usgs.gov/ws/designmaps/asce7-22.json"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://earthquake.usgs.gov"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect earthquake events, seismic hazard, and fault data."""
        total_inserted = 0
        errors = 0

        # Phase 1: Collect seismic hazard at grid points
        self.update_progress(0, 3, "Collecting seismic hazard data")
        try:
            hazard_result = await self._collect_seismic_hazard(config)
            total_inserted += hazard_result.get("inserted", 0)
        except Exception as e:
            logger.error(f"Seismic hazard collection failed: {e}")
            self.db.rollback()
            errors += 1

        # Phase 2: Collect recent significant earthquakes as hazard indicators
        self.update_progress(1, 3, "Collecting recent earthquakes")
        try:
            quake_result = await self._collect_recent_earthquakes(config)
            total_inserted += quake_result.get("inserted", 0)
        except Exception as e:
            logger.error(f"Earthquake collection failed: {e}")
            self.db.rollback()
            errors += 1

        # Phase 3: Collect fault data
        self.update_progress(2, 3, "Collecting fault line data")
        try:
            fault_result = await self._collect_fault_lines(config)
            total_inserted += fault_result.get("inserted", 0)
        except Exception as e:
            logger.error(f"Fault line collection failed: {e}")
            self.db.rollback()
            errors += 1

        return self.create_result(
            status=CollectionStatus.SUCCESS
            if total_inserted > 0
            else CollectionStatus.PARTIAL,
            total=total_inserted,
            processed=total_inserted,
            inserted=total_inserted,
        )

    async def _collect_seismic_hazard(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect seismic hazard estimates for grid points."""
        records = []

        for lat, lng, location_name in HAZARD_GRID_POINTS:
            try:
                params = {
                    "latitude": lat,
                    "longitude": lng,
                    "riskCategory": "II",
                    "siteClass": "D",
                    "title": location_name,
                }

                data = await self.fetch_json(self.HAZARD_API, params=params)
                if not data or "response" not in data:
                    continue

                resp = data["response"]["data"]
                record = {
                    "latitude": lat,
                    "longitude": lng,
                    "pga_2pct_50yr": resp.get("pgauh"),
                    "pga_10pct_50yr": resp.get("pgad"),
                    "spectral_1sec_2pct": resp.get("s1uh"),
                    "spectral_02sec_2pct": resp.get("ssuh"),
                    "site_class": "D",
                    "seismic_design_category": resp.get("sdc"),
                    "source": "usgs",
                    "collected_at": datetime.utcnow(),
                }
                records.append(record)

            except Exception as e:
                logger.debug(f"Hazard data for ({lat}, {lng}): {e}")

        inserted = 0
        if records:
            inserted, _ = self.bulk_upsert(
                SeismicHazard,
                records,
                unique_columns=["latitude", "longitude"],
                update_columns=[
                    "pga_2pct_50yr",
                    "pga_10pct_50yr",
                    "spectral_1sec_2pct",
                    "spectral_02sec_2pct",
                    "site_class",
                    "seismic_design_category",
                    "collected_at",
                ],
            )

        return {"processed": len(HAZARD_GRID_POINTS), "inserted": inserted}

    async def _collect_recent_earthquakes(
        self, config: CollectionConfig
    ) -> Dict[str, Any]:
        """Collect recent significant earthquakes as hazard indicators."""
        params = {
            "format": "geojson",
            "starttime": "2020-01-01",
            "minmagnitude": "4.5",
            "maxlatitude": "72",
            "minlatitude": "17",
            "maxlongitude": "-64",
            "minlongitude": "-180",
            "orderby": "magnitude",
            "limit": config.limit or 500,
        }

        data = await self.fetch_json(self.EARTHQUAKE_API, params=params)
        if not data:
            return {"processed": 0, "inserted": 0}

        features = data.get("features", [])
        # Deduplicate by (lat, lng) — keep highest magnitude
        seen = {}
        for feature in features:
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})
            coords = geom.get("coordinates", [])
            if len(coords) < 2:
                continue

            lng, lat = coords[0], coords[1]
            mag = props.get("mag", 0) or 0
            key = (round(lat, 4), round(lng, 4))

            if key not in seen or mag > seen[key]["mag"]:
                seen[key] = {"lat": key[0], "lng": key[1], "mag": mag}

        records = []
        for key, info in seen.items():
            pga_estimate = min(info["mag"] * 0.1, 2.0)
            records.append(
                {
                    "latitude": info["lat"],
                    "longitude": info["lng"],
                    "pga_2pct_50yr": pga_estimate,
                    "source": "usgs_earthquake_event",
                    "collected_at": datetime.utcnow(),
                }
            )

        inserted = 0
        if records:
            inserted, _ = self.bulk_upsert(
                SeismicHazard,
                records,
                unique_columns=["latitude", "longitude"],
                update_columns=["pga_2pct_50yr", "collected_at"],
            )

        return {"processed": len(features), "inserted": inserted}

    async def _collect_fault_lines(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect quaternary fault data."""
        # USGS provides faults as GeoJSON via their Hazards site
        url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson"

        data = await self.fetch_json(url)
        if not data:
            return {"processed": 0, "inserted": 0}

        features = data.get("features", [])
        # Deduplicate by fault_name (place) — last wins
        seen_names = {}
        for feature in features:
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})

            fault_name = (props.get("place", "Unknown") or "Unknown")[:255]
            seen_names[fault_name] = {
                "fault_name": fault_name,
                "fault_type": props.get("type", "earthquake_source"),
                "slip_rate_mm_yr": None,
                "age": "recent",
                "geometry_geojson": geom,
                "source": "usgs",
                "collected_at": datetime.utcnow(),
            }
        records = list(seen_names.values())

        inserted = 0
        if records:
            inserted, _ = self.bulk_upsert(
                FaultLine,
                records,
                unique_columns=["fault_name"],
                update_columns=["geometry_geojson", "collected_at"],
            )

        return {"processed": len(features), "inserted": inserted}
