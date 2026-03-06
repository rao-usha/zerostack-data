"""
USFWS National Wetlands Inventory (NWI) Collector.

Fetches wetland data from the NWI ArcGIS REST API using server-side
aggregation (outStatistics). Gets total acres per NWI code per state
without downloading millions of individual polygons.

API: https://fwspublicservices.wim.usgs.gov/wetlandsmapservice/rest/services/Wetlands/MapServer/0/query
No API key required.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import Wetland
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

# NWI MapServer layer 0 = Wetlands polygons
NWI_QUERY_URL = (
    "https://fwspublicservices.wim.usgs.gov/wetlandsmapservice"
    "/rest/services/Wetlands/MapServer/0/query"
)

# State bounding boxes (min_lng, min_lat, max_lng, max_lat)
STATE_BBOX = {
    "AL": (-88.47, 30.22, -84.89, 35.01),
    "AK": (-179.15, 51.21, -129.97, 71.39),
    "AZ": (-114.82, 31.33, -109.04, 37.00),
    "AR": (-94.62, 33.00, -89.64, 36.50),
    "CA": (-124.41, 32.53, -114.13, 42.01),
    "CO": (-109.06, 36.99, -102.04, 41.00),
    "CT": (-73.73, 40.95, -71.79, 42.05),
    "DE": (-75.79, 38.45, -75.05, 39.84),
    "FL": (-87.63, 24.52, -80.03, 31.00),
    "GA": (-85.61, 30.36, -80.84, 35.00),
    "HI": (-160.24, 18.91, -154.81, 22.24),
    "ID": (-117.24, 41.99, -111.04, 49.00),
    "IL": (-91.51, 36.97, -87.50, 42.51),
    "IN": (-88.10, 37.77, -84.78, 41.76),
    "IA": (-96.64, 40.38, -90.14, 43.50),
    "KS": (-102.05, 36.99, -94.59, 40.00),
    "KY": (-89.57, 36.50, -81.96, 39.15),
    "LA": (-94.04, 28.93, -89.00, 33.02),
    "ME": (-71.08, 43.06, -66.95, 47.46),
    "MD": (-79.49, 37.91, -75.05, 39.72),
    "MA": (-73.51, 41.24, -69.93, 42.89),
    "MI": (-90.42, 41.70, -82.41, 48.19),
    "MN": (-97.24, 43.50, -89.49, 49.38),
    "MS": (-91.66, 30.17, -88.10, 34.99),
    "MO": (-95.77, 35.99, -89.10, 40.61),
    "MT": (-116.05, 44.36, -104.04, 49.00),
    "NE": (-104.05, 40.00, -95.31, 43.00),
    "NV": (-120.01, 35.00, -114.04, 42.00),
    "NH": (-72.56, 42.70, -71.09, 45.31),
    "NJ": (-75.56, 38.93, -73.89, 41.36),
    "NM": (-109.05, 31.33, -103.00, 37.00),
    "NY": (-79.76, 40.50, -71.86, 45.02),
    "NC": (-84.32, 33.84, -75.46, 36.59),
    "ND": (-104.05, 45.94, -96.55, 49.00),
    "OH": (-84.82, 38.40, -80.52, 41.98),
    "OK": (-103.00, 33.62, -94.43, 37.00),
    "OR": (-124.57, 41.99, -116.46, 46.29),
    "PA": (-80.52, 39.72, -74.69, 42.27),
    "RI": (-71.86, 41.15, -71.12, 42.02),
    "SC": (-83.35, 32.03, -78.54, 35.22),
    "SD": (-104.06, 42.48, -96.44, 45.94),
    "TN": (-90.31, 34.98, -81.65, 36.68),
    "TX": (-106.65, 25.84, -93.51, 36.50),
    "UT": (-114.05, 36.99, -109.04, 42.00),
    "VT": (-73.44, 42.73, -71.50, 45.02),
    "VA": (-83.68, 36.54, -75.24, 39.47),
    "WA": (-124.73, 45.54, -116.92, 49.00),
    "WV": (-82.64, 37.20, -77.72, 40.64),
    "WI": (-92.89, 42.49, -86.25, 47.08),
    "WY": (-111.06, 40.99, -104.05, 45.01),
    "DC": (-77.12, 38.79, -76.91, 38.99),
}

# Cowardin wetland type classification
COWARDIN_TYPES = {
    "E": "Estuarine",
    "L": "Lacustrine",
    "M": "Marine",
    "P": "Palustrine",
    "R": "Riverine",
}


def classify_wetland_type(attribute: str) -> tuple:
    """
    Parse a Cowardin NWI code into wetland type and modifier.

    Example: 'PFO1A' -> ('Palustrine', 'Forested')
    """
    if not attribute:
        return ("Unknown", None)

    system = attribute[0].upper() if attribute else ""
    wetland_type = COWARDIN_TYPES.get(system, "Unknown")

    modifier = None
    if len(attribute) > 1:
        sub = attribute[1:3].upper()
        sub_types = {
            "AB": "Aquatic Bed",
            "EM": "Emergent",
            "FO": "Forested",
            "ML": "Moss-Lichen",
            "OW": "Open Water",
            "RB": "Rocky Bottom",
            "RF": "Reef",
            "RS": "Rocky Shore",
            "SB": "Streambed",
            "SS": "Scrub-Shrub",
            "UB": "Unconsolidated Bottom",
            "US": "Unconsolidated Shore",
        }
        modifier = sub_types.get(sub)

    return (wetland_type, modifier)


@register_collector(SiteIntelSource.USFWS_NWI)
class NWIWetlandCollector(BaseCollector):
    """
    Collector for USFWS National Wetlands Inventory data.

    Uses ArcGIS REST outStatistics to aggregate wetland acres by
    NWI code per state bounding box. One API call per state returns
    ~20-80 aggregated rows instead of millions of individual polygons.
    """

    domain = SiteIntelDomain.RISK
    source = SiteIntelSource.USFWS_NWI

    default_timeout = 300.0
    rate_limit_delay = 0.5

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return NWI_QUERY_URL

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect NWI wetland data aggregated by type per state."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            states = config.states if config.states else list(STATE_BBOX.keys())
            logger.info(f"Collecting NWI wetland data for {len(states)} states...")

            for state in states:
                bbox = STATE_BBOX.get(state)
                if not bbox:
                    continue
                try:
                    result = await self._collect_state(state, bbox)
                    total_inserted += result.get("inserted", 0)
                    total_processed += result.get("processed", 0)
                    if result.get("error"):
                        errors.append(
                            {"source": f"nwi_{state}", "error": result["error"]}
                        )
                except Exception as e:
                    logger.warning(f"NWI collection failed for {state}: {e}")
                    errors.append({"source": f"nwi_{state}", "error": str(e)})

            status = CollectionStatus.SUCCESS
            if errors and total_inserted > 0:
                status = CollectionStatus.PARTIAL
            elif errors and total_inserted == 0:
                status = CollectionStatus.FAILED

            logger.info(
                f"NWI collection complete: {total_processed} processed, "
                f"{total_inserted} inserted, {len(errors)} errors"
            )

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"NWI collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_state(
        self, state: str, bbox: tuple
    ) -> Dict[str, Any]:
        """Fetch aggregated wetland stats for a state using quadrant subdivision."""
        # Skip full-bbox attempt — NWI server consistently times out on state-level
        # aggregations. Go directly to quadrants for all states.
        records = []
        error = None
        quadrants = self._subdivide_bbox(bbox)
        for i, quad in enumerate(quadrants):
            await self.apply_rate_limit()
            quad_records, quad_err = await self._query_bbox(state, quad)
            if quad_records:
                records.extend(quad_records)
            if quad_err:
                error = quad_err
                logger.debug(f"NWI {state} quadrant {i} error: {quad_err}")

        if not records:
            return {"processed": 0, "inserted": 0, "error": error}

        # Deduplicate — same nwi_code may appear in multiple quadrants
        merged: Dict[str, Dict[str, Any]] = {}
        for rec in records:
            key = rec["nwi_code"]
            if key in merged:
                # Sum acres across quadrants
                existing_acres = merged[key].get("acres") or 0
                new_acres = rec.get("acres") or 0
                merged[key]["acres"] = round(existing_acres + new_acres, 2)
            else:
                merged[key] = rec
        records = list(merged.values())

        logger.info(f"NWI {state}: {len(records)} wetland type aggregates")

        try:
            inserted, updated = self.bulk_upsert(
                Wetland,
                records,
                unique_columns=["nwi_code", "state"],
                update_columns=[
                    "wetland_type", "modifier", "acres",
                    "source", "collected_at",
                ],
            )
            return {"processed": len(records), "inserted": inserted + updated}
        except Exception as e:
            logger.warning(f"NWI {state} upsert failed: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return {"processed": len(records), "inserted": 0, "error": str(e)}

    def _subdivide_bbox(self, bbox: tuple) -> List[tuple]:
        """Split a bounding box into 4 quadrants."""
        min_lng, min_lat, max_lng, max_lat = bbox
        mid_lng = (min_lng + max_lng) / 2
        mid_lat = (min_lat + max_lat) / 2
        return [
            (min_lng, min_lat, mid_lng, mid_lat),  # SW
            (mid_lng, min_lat, max_lng, mid_lat),   # SE
            (min_lng, mid_lat, mid_lng, max_lat),   # NW
            (mid_lng, mid_lat, max_lng, max_lat),   # NE
        ]

    async def _query_bbox(
        self, state: str, bbox: tuple
    ) -> tuple:
        """Execute a single outStatistics query for a bbox. Returns (records, error)."""
        import asyncio

        client = await self.get_client()
        await self.apply_rate_limit()

        envelope = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

        out_stats = json.dumps([
            {
                "statisticType": "sum",
                "onStatisticField": "Wetlands.ACRES",
                "outStatisticFieldName": "total_acres",
            },
            {
                "statisticType": "count",
                "onStatisticField": "Wetlands.OBJECTID",
                "outStatisticFieldName": "feature_count",
            },
        ])

        params = {
            "where": "1=1",
            "geometry": envelope,
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "returnGeometry": "false",
            "outStatistics": out_stats,
            "groupByFieldsForStatistics": "Wetlands.WETLAND_TYPE,Wetlands.ATTRIBUTE",
            "f": "json",
        }

        for attempt in range(self.default_retries):
            try:
                response = await client.get(NWI_QUERY_URL, params=params)

                if response.status_code in (500, 502, 503):
                    if attempt < self.default_retries - 1:
                        logger.debug(f"NWI {response.status_code} for {state} (attempt {attempt + 1}), retrying...")
                        await asyncio.sleep(self.rate_limit_delay * (attempt + 1))
                        continue
                    return ([], f"Server error {response.status_code} after {self.default_retries} retries")

                if response.status_code == 504:
                    # Gateway timeout — bbox too large, don't retry
                    return ([], f"Gateway timeout (504) — bbox too large")

                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    msg = data["error"].get("message", "API error")
                    return ([], msg)

                features = data.get("features", [])
                if not features:
                    return ([], None)

                records = []
                for feat in features:
                    attrs = feat.get("attributes", {})
                    nwi_code = attrs.get("Wetlands.ATTRIBUTE", "")
                    wetland_type_raw = attrs.get("Wetlands.WETLAND_TYPE", "")
                    total_acres = attrs.get("total_acres", 0)

                    if not nwi_code:
                        continue

                    wetland_type, modifier = classify_wetland_type(nwi_code)
                    display_type = wetland_type_raw or wetland_type

                    records.append({
                        "nwi_code": str(nwi_code)[:20],
                        "wetland_type": display_type[:100] if display_type else None,
                        "modifier": modifier[:50] if modifier else None,
                        "acres": round(float(total_acres), 2) if total_acres else None,
                        "state": state,
                        "source": "usfws_nwi",
                        "collected_at": datetime.utcnow(),
                    })

                return (records, None)

            except Exception as e:
                if attempt < self.default_retries - 1:
                    await asyncio.sleep(self.rate_limit_delay)
                    continue
                return ([], str(e))
