"""
FRA Rail Network Collector.

Fetches rail network data from the NTAD (National Transportation Atlas Database)
via the Bureau of Transportation Statistics ArcGIS REST service.

API: https://geo.dot.gov/server/rest/services/NTAD/Rail_Network/MapServer/0/query
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import RailLine
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

# State FIPS for filtering
STATE_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
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
}


@register_collector(SiteIntelSource.FRA)
class FRARailCollector(BaseCollector):
    """
    Collector for FRA/NTAD rail network data.

    Uses the BTS NTAD ArcGIS REST service to collect
    rail segments including track class, railroad operator,
    max speed, and geometry.
    """

    domain = SiteIntelDomain.TRANSPORT
    source = SiteIntelSource.FRA
    default_timeout = 120.0
    rate_limit_delay = 0.5

    RAIL_NETWORK_URL = (
        "https://geo.dot.gov/server/rest/services/Hosted/"
        "North_American_Class_1_Rail/FeatureServer/0/query"
    )

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://geo.dot.gov/server/rest/services/NTAD"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect rail network data via ArcGIS pagination."""
        self.create_job(config)
        self.start_job()

        all_records = []
        errors = 0
        states = config.states if config.states else list(STATE_FIPS.keys())

        for i, state in enumerate(states):
            fips = STATE_FIPS.get(state)
            if not fips:
                continue

            self.update_progress(i, len(states), f"Collecting rail data for {state}")

            try:
                records = await self._collect_state_rail(state, fips, config)
                all_records.extend(records)
            except Exception as e:
                logger.warning(f"Failed to collect rail for {state}: {e}")
                errors += 1

        # Insert records
        inserted = 0
        if all_records:
            inserted, _ = self.bulk_upsert(
                RailLine,
                all_records,
                unique_columns=["fra_line_id"],
                update_columns=[
                    "railroad",
                    "track_type",
                    "track_class",
                    "max_speed_mph",
                    "annual_tonnage_million",
                    "state",
                    "county",
                    "geometry_geojson",
                    "collected_at",
                ],
            )

        result = self.create_result(
            status=CollectionStatus.SUCCESS
            if inserted > 0
            else CollectionStatus.PARTIAL,
            total=len(all_records),
            processed=len(all_records),
            inserted=inserted,
        )
        self.complete_job(result)
        return result

    async def _collect_state_rail(
        self, state: str, fips: str, config: CollectionConfig
    ) -> List[Dict[str, Any]]:
        """Collect rail segments for a single state using ArcGIS pagination."""
        records = []
        offset = 0
        page_size = 1000

        while True:
            params = {
                "where": f"stfips='{fips}'",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": page_size,
            }

            data = await self.fetch_json(self.RAIL_NETWORK_URL, params=params)
            if not data:
                break

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                attrs = feature.get("attributes", {})
                geom = feature.get("geometry")

                # Build unique ID from railroad + segment (lowercase field names in new API)
                rrowner = (
                    attrs.get("rrowner1")
                    or attrs.get("RROWNER1")
                    or attrs.get("RROWNER")
                    or ""
                )
                objectid = (
                    attrs.get("objectid")
                    or attrs.get("OBJECTID")
                    or attrs.get("FID")
                    or ""
                )
                fra_id = (
                    f"{rrowner}_{state}_{objectid}"
                    if rrowner
                    else f"NTAD_{state}_{objectid}"
                )

                record = {
                    "fra_line_id": fra_id[:50],
                    "railroad": (rrowner or "")[:100],
                    "track_type": self._classify_track(attrs),
                    "track_class": self._safe_int(
                        attrs.get("tracks")
                        or attrs.get("TRACKS")
                        or attrs.get("TRKCLASS")
                    ),
                    "max_speed_mph": None,
                    "annual_tonnage_million": None,
                    "state": state,
                    "county": "",
                    "geometry_geojson": self._arcgis_to_geojson(geom) if geom else None,
                    "source": "fra",
                    "collected_at": datetime.utcnow(),
                }
                records.append(record)

            if len(features) < page_size:
                break
            offset += page_size

        return records

    @staticmethod
    def _classify_track(attrs: Dict) -> str:
        """Classify track type from attributes."""
        net = (attrs.get("net") or attrs.get("NET") or "").upper()
        if "MAIN" in net:
            return "mainline"
        elif "BRANCH" in net or "SPUR" in net:
            return "branch"
        elif "YARD" in net:
            return "yard"
        return "other"

    @staticmethod
    def _safe_int(val) -> Optional[int]:
        if val is None:
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _arcgis_to_geojson(geom: Dict) -> Optional[Dict]:
        """Convert ArcGIS geometry to GeoJSON."""
        if not geom:
            return None
        paths = geom.get("paths")
        if paths:
            return {
                "type": "MultiLineString" if len(paths) > 1 else "LineString",
                "coordinates": paths if len(paths) > 1 else paths[0],
            }
        return None
