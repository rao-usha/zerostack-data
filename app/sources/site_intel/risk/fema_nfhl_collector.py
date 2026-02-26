"""
FEMA National Flood Hazard Layer (NFHL) Collector.

Fetches flood zone summary data from FEMA NFHL ArcGIS REST API.
Uses county-level aggregation — counts flood zone polygons per zone type
rather than downloading individual polygon geometries (which would be millions).

API: https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query
Layer 28 = S_FLD_HAZ_AR (flood hazard areas)

No API key required. FEMA site is intermittently unavailable —
collector handles gracefully with retries and partial results.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import FloodZone
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


# State FIPS codes
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56",
}

FIPS_TO_STATE = {v: k for k, v in STATE_FIPS.items()}

# FEMA NFHL flood zone descriptions and risk classification
ZONE_INFO = {
    "A": {"description": "1% annual chance flood (no BFE)", "high_risk": True, "coastal": False},
    "AE": {"description": "1% annual chance flood with BFE", "high_risk": True, "coastal": False},
    "AH": {"description": "1% annual chance shallow flooding (1-3ft)", "high_risk": True, "coastal": False},
    "AO": {"description": "1% annual chance sheet flow (1-3ft)", "high_risk": True, "coastal": False},
    "AR": {"description": "1% annual chance (levee accredited)", "high_risk": True, "coastal": False},
    "A99": {"description": "1% annual chance (federal flood protection)", "high_risk": True, "coastal": False},
    "V": {"description": "Coastal 1% annual chance (no BFE)", "high_risk": True, "coastal": True},
    "VE": {"description": "Coastal 1% annual chance with BFE", "high_risk": True, "coastal": True},
    "X": {"description": "0.2% annual chance or minimal flood hazard", "high_risk": False, "coastal": False},
    "D": {"description": "Undetermined flood hazard", "high_risk": False, "coastal": False},
}

# NFHL MapServer layer for flood hazard areas
NFHL_BASE_URL = "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query"


@register_collector(SiteIntelSource.FEMA_NFHL)
class FEMANFHLFloodCollector(BaseCollector):
    """
    Collector for FEMA NFHL flood zone data.

    Uses county-level statistical queries (outStatistics) to count
    flood zone polygons by zone type per county, rather than
    downloading individual polygon geometries.
    """

    domain = SiteIntelDomain.RISK
    source = SiteIntelSource.FEMA_NFHL

    default_timeout = 120.0
    default_retries = 5  # FEMA can be flaky
    rate_limit_delay = 1.0  # Conservative for FEMA

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect flood zone summary data by state+county."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            states = config.states if config.states else list(STATE_FIPS.keys())
            logger.info(f"Collecting FEMA NFHL flood zone data for {len(states)} states...")

            # First check if FEMA service is available
            if not await self._check_service_available():
                return self.create_result(
                    status=CollectionStatus.FAILED,
                    error_message="FEMA NFHL service unavailable. Try again later.",
                )

            for state in states:
                fips = STATE_FIPS.get(state)
                if not fips:
                    continue

                try:
                    result = await self._collect_state_flood_zones(state, fips)
                    total_inserted += result.get("inserted", 0)
                    total_processed += result.get("processed", 0)
                    if result.get("error"):
                        errors.append(
                            {"source": f"nfhl_{state}", "error": result["error"]}
                        )
                except Exception as e:
                    logger.warning(f"NFHL collection failed for {state}: {e}")
                    errors.append({"source": f"nfhl_{state}", "error": str(e)})

            status = CollectionStatus.SUCCESS
            if errors and total_inserted > 0:
                status = CollectionStatus.PARTIAL
            elif errors and total_inserted == 0:
                status = CollectionStatus.FAILED

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"FEMA NFHL collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _check_service_available(self) -> bool:
        """Check if FEMA NFHL service is responding."""
        try:
            client = await self.get_client()
            response = await client.get(
                NFHL_BASE_URL,
                params={
                    "where": "1=1",
                    "returnCountOnly": "true",
                    "f": "json",
                },
            )
            if response.status_code == 200:
                data = response.json()
                return "count" in data
            return False
        except Exception as e:
            logger.warning(f"FEMA NFHL service check failed: {e}")
            return False

    async def _collect_state_flood_zones(
        self, state: str, state_fips: str
    ) -> Dict[str, Any]:
        """
        Collect flood zone stats for a state using outStatistics.

        Groups by FLD_ZONE and DFIRM_ID (county) to get zone counts
        per county in a single query per state.
        """
        try:
            # Use outStatistics to count zones per county
            params = {
                "where": f"DFIRM_ID LIKE '{state_fips}%'",
                "outStatistics": '[{"statisticType":"count","onStatisticField":"OBJECTID","outStatisticFieldName":"zone_count"}]',
                "groupByFieldsForStatistics": "FLD_ZONE,DFIRM_ID",
                "f": "json",
            }

            await self.apply_rate_limit()
            response_data = await self._fetch_fema(params)
            features = response_data.get("features", [])

            if not features:
                # Fallback: try paginated query for individual records
                logger.info(f"No stats for {state}, trying paginated query...")
                return await self._collect_state_paginated(state, state_fips)

            records = []
            for feature in features:
                attrs = feature.get("attributes", {})
                zone_code = attrs.get("FLD_ZONE", "").strip()
                dfirm_id = attrs.get("DFIRM_ID", "").strip()
                zone_count = attrs.get("zone_count", 0)

                if not zone_code or not dfirm_id:
                    continue

                # Extract county from DFIRM_ID (format: SSCCC or similar)
                county_name = dfirm_id  # Will be FIPS-based ID

                zone_info = ZONE_INFO.get(zone_code, ZONE_INFO.get("X"))

                record = {
                    "zone_code": zone_code,
                    "zone_description": zone_info["description"] if zone_info else f"Flood zone {zone_code}",
                    "is_high_risk": zone_info["high_risk"] if zone_info else False,
                    "is_coastal": zone_info["coastal"] if zone_info else False,
                    "base_flood_elevation_ft": None,
                    "state": state,
                    "county": county_name,
                    "geometry_geojson": {"zone_count": zone_count},
                    "effective_date": None,
                    "source": "fema_nfhl",
                    "collected_at": datetime.utcnow(),
                }
                records.append(record)

            logger.info(f"NFHL {state}: {len(records)} zone/county records")

            if records:
                inserted, updated = self.bulk_upsert(
                    FloodZone,
                    records,
                    unique_columns=["zone_code", "state", "county"],
                    update_columns=[
                        "zone_description", "is_high_risk", "is_coastal",
                        "base_flood_elevation_ft", "geometry_geojson",
                        "effective_date", "source", "collected_at",
                    ],
                )
                return {"processed": len(features), "inserted": inserted + updated}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.warning(f"NFHL stats query failed for {state}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return {"processed": 0, "inserted": 0, "error": str(e)}

    async def _collect_state_paginated(
        self, state: str, state_fips: str
    ) -> Dict[str, Any]:
        """
        Fallback: paginated query counting zones manually.

        If outStatistics isn't supported, fetch records and aggregate
        zone counts in Python.
        """
        try:
            zone_counts: Dict[str, Dict[str, int]] = {}  # {county: {zone: count}}
            offset = 0
            page_size = 2000
            max_pages = 10  # Cap to avoid huge downloads

            for _ in range(max_pages):
                params = {
                    "where": f"DFIRM_ID LIKE '{state_fips}%'",
                    "outFields": "FLD_ZONE,DFIRM_ID",
                    "returnGeometry": "false",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                await self.apply_rate_limit()
                response_data = await self._fetch_fema(params)
                features = response_data.get("features", [])

                if not features:
                    break

                for feature in features:
                    attrs = feature.get("attributes", {})
                    zone = attrs.get("FLD_ZONE", "").strip()
                    county = attrs.get("DFIRM_ID", "").strip()

                    if zone and county:
                        if county not in zone_counts:
                            zone_counts[county] = {}
                        zone_counts[county][zone] = zone_counts[county].get(zone, 0) + 1

                if len(features) < page_size:
                    break

                offset += page_size

            # Build records from aggregated counts
            records = []
            for county, zones in zone_counts.items():
                for zone_code, count in zones.items():
                    zone_info = ZONE_INFO.get(zone_code, ZONE_INFO.get("X"))
                    records.append({
                        "zone_code": zone_code,
                        "zone_description": zone_info["description"] if zone_info else f"Flood zone {zone_code}",
                        "is_high_risk": zone_info["high_risk"] if zone_info else False,
                        "is_coastal": zone_info["coastal"] if zone_info else False,
                        "base_flood_elevation_ft": None,
                        "state": state,
                        "county": county,
                        "geometry_geojson": {"zone_count": count},
                        "effective_date": None,
                        "source": "fema_nfhl",
                        "collected_at": datetime.utcnow(),
                    })

            if records:
                inserted, updated = self.bulk_upsert(
                    FloodZone,
                    records,
                    unique_columns=["zone_code", "state", "county"],
                    update_columns=[
                        "zone_description", "is_high_risk", "is_coastal",
                        "base_flood_elevation_ft", "geometry_geojson",
                        "effective_date", "source", "collected_at",
                    ],
                )
                return {"processed": sum(sum(z.values()) for z in zone_counts.values()), "inserted": inserted + updated}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.warning(f"NFHL paginated query failed for {state}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return {"processed": 0, "inserted": 0, "error": str(e)}

    async def _fetch_fema(self, params: Dict) -> Dict:
        """Fetch from FEMA NFHL endpoint with retry handling."""
        client = await self.get_client()

        for attempt in range(self.default_retries):
            try:
                response = await client.get(NFHL_BASE_URL, params=params)

                if response.status_code == 503:
                    logger.warning(f"FEMA NFHL 503 (attempt {attempt + 1}/{self.default_retries})")
                    await self.apply_rate_limit()
                    continue

                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    error = data["error"]
                    logger.warning(f"FEMA NFHL error: {error}")
                    if error.get("code") in (500, 503):
                        await self.apply_rate_limit()
                        continue
                    return {"features": []}

                return data

            except Exception as e:
                if attempt < self.default_retries - 1:
                    logger.warning(f"FEMA request failed (attempt {attempt + 1}): {e}")
                    await self.apply_rate_limit()
                else:
                    raise

        return {"features": []}
