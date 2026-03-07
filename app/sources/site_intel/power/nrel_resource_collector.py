"""
NREL Solar/Wind Resource Data Collector.

Fetches solar irradiance and wind speed data from NREL APIs at county
centroids. Used to assess renewable energy potential for datacenter
power sourcing.

API: https://developer.nrel.gov/api/solar/solar_resource/v1
API: https://developer.nrel.gov/api/wind-toolkit/v2/wind/wtk-srw-download
Free API key required: https://developer.nrel.gov/signup/
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import RenewableResource
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

# County centroids for major datacenter-relevant states
# (lat, lng, state, county_name, county_fips)
# This is a representative subset — full list would come from Census TIGER
COUNTY_CENTROIDS: List[Dict[str, Any]] = [
    # Virginia — Northern Virginia DC corridor
    {"lat": 39.04, "lng": -77.49, "state": "VA", "county": "Loudoun County", "fips": "51107"},
    {"lat": 38.83, "lng": -77.31, "state": "VA", "county": "Fairfax County", "fips": "51059"},
    {"lat": 38.95, "lng": -77.37, "state": "VA", "county": "Prince William County", "fips": "51153"},
    # Texas — major DC hubs
    {"lat": 32.77, "lng": -96.80, "state": "TX", "county": "Dallas County", "fips": "48113"},
    {"lat": 32.97, "lng": -96.99, "state": "TX", "county": "Denton County", "fips": "48121"},
    {"lat": 30.27, "lng": -97.74, "state": "TX", "county": "Travis County", "fips": "48453"},
    {"lat": 29.76, "lng": -95.37, "state": "TX", "county": "Harris County", "fips": "48201"},
    {"lat": 32.45, "lng": -97.39, "state": "TX", "county": "Tarrant County", "fips": "48439"},
    # Arizona
    {"lat": 33.45, "lng": -112.07, "state": "AZ", "county": "Maricopa County", "fips": "04013"},
    {"lat": 33.42, "lng": -111.94, "state": "AZ", "county": "Pinal County", "fips": "04021"},
    # Oregon
    {"lat": 45.52, "lng": -122.68, "state": "OR", "county": "Multnomah County", "fips": "41051"},
    {"lat": 45.23, "lng": -122.84, "state": "OR", "county": "Washington County", "fips": "41067"},
    # Nevada
    {"lat": 36.17, "lng": -115.14, "state": "NV", "county": "Clark County", "fips": "32003"},
    {"lat": 39.53, "lng": -119.81, "state": "NV", "county": "Washoe County", "fips": "32031"},
    # Georgia
    {"lat": 33.75, "lng": -84.39, "state": "GA", "county": "Fulton County", "fips": "13121"},
    {"lat": 33.79, "lng": -84.33, "state": "GA", "county": "DeKalb County", "fips": "13089"},
    {"lat": 33.94, "lng": -84.52, "state": "GA", "county": "Cobb County", "fips": "13067"},
    # North Carolina
    {"lat": 35.78, "lng": -78.64, "state": "NC", "county": "Wake County", "fips": "37183"},
    {"lat": 35.95, "lng": -78.90, "state": "NC", "county": "Durham County", "fips": "37063"},
    # Ohio
    {"lat": 40.10, "lng": -83.01, "state": "OH", "county": "Franklin County", "fips": "39049"},
    {"lat": 39.99, "lng": -82.99, "state": "OH", "county": "Licking County", "fips": "39089"},
    # Illinois
    {"lat": 41.88, "lng": -87.63, "state": "IL", "county": "Cook County", "fips": "17031"},
    {"lat": 41.75, "lng": -88.15, "state": "IL", "county": "DuPage County", "fips": "17043"},
    # Iowa
    {"lat": 41.60, "lng": -93.61, "state": "IA", "county": "Polk County", "fips": "19153"},
    {"lat": 42.03, "lng": -93.47, "state": "IA", "county": "Story County", "fips": "19169"},
    # Indiana
    {"lat": 39.77, "lng": -86.16, "state": "IN", "county": "Marion County", "fips": "18097"},
    # South Carolina
    {"lat": 34.85, "lng": -82.39, "state": "SC", "county": "Greenville County", "fips": "45045"},
    # Tennessee
    {"lat": 36.16, "lng": -86.78, "state": "TN", "county": "Davidson County", "fips": "47037"},
    # Utah
    {"lat": 40.76, "lng": -111.89, "state": "UT", "county": "Salt Lake County", "fips": "49035"},
    # Colorado
    {"lat": 39.74, "lng": -104.99, "state": "CO", "county": "Denver County", "fips": "08031"},
    {"lat": 39.65, "lng": -104.80, "state": "CO", "county": "Arapahoe County", "fips": "08005"},
    # California
    {"lat": 37.54, "lng": -122.05, "state": "CA", "county": "Santa Clara County", "fips": "06085"},
    {"lat": 37.87, "lng": -122.27, "state": "CA", "county": "Alameda County", "fips": "06001"},
    {"lat": 33.92, "lng": -118.23, "state": "CA", "county": "Los Angeles County", "fips": "06037"},
    {"lat": 37.78, "lng": -122.42, "state": "CA", "county": "San Francisco", "fips": "06075"},
    # New York / New Jersey
    {"lat": 40.71, "lng": -74.01, "state": "NY", "county": "New York County", "fips": "36061"},
    {"lat": 40.74, "lng": -74.17, "state": "NJ", "county": "Essex County", "fips": "34013"},
    # Washington
    {"lat": 47.61, "lng": -122.33, "state": "WA", "county": "King County", "fips": "53033"},
    {"lat": 46.60, "lng": -120.51, "state": "WA", "county": "Yakima County", "fips": "53077"},
    {"lat": 47.25, "lng": -119.85, "state": "WA", "county": "Grant County", "fips": "53025"},
]


@register_collector(SiteIntelSource.NREL_RESOURCE)
class NRELResourceCollector(BaseCollector):
    """
    Collector for NREL solar/wind resource data.

    Queries NREL Solar Resource and Wind Toolkit APIs at county centroids
    to assess renewable energy potential for datacenter power sourcing.
    """

    domain = SiteIntelDomain.POWER
    source = SiteIntelSource.NREL_RESOURCE

    default_timeout = 30.0
    rate_limit_delay = 1.0  # NREL real key: 1000/hr; DEMO_KEY: 30/hr (needs manual override)

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://developer.nrel.gov/api"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect solar/wind resource data at county centroids."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            if not self.api_key:
                # Try to get from config
                from app.core.config import get_settings
                self.api_key = get_settings().get_api_key("nrel") or "DEMO_KEY"

            centroids = COUNTY_CENTROIDS
            if config.states:
                centroids = [c for c in centroids if c["state"] in config.states]

            logger.info(f"Collecting NREL resource data for {len(centroids)} county centroids...")

            results = await self.gather_with_limit(
                [self._collect_point(c) for c in centroids], max_concurrent=4
            )

            all_records = []
            for i, result in enumerate(results):
                centroid = centroids[i]
                total_processed += 1
                if isinstance(result, Exception):
                    logger.warning(f"NREL failed for {centroid['county']}: {result}")
                    errors.append({
                        "source": f"nrel_{centroid['fips']}",
                        "error": str(result),
                    })
                elif result.get("error"):
                    errors.append({
                        "source": f"nrel_{centroid['fips']}",
                        "error": result["error"],
                    })
                elif result.get("record"):
                    all_records.append(result["record"])

            # Single batch upsert instead of 40+ individual calls
            if all_records:
                inserted, updated = self.bulk_upsert(
                    RenewableResource,
                    all_records,
                    unique_columns=["latitude", "longitude"],
                    update_columns=[
                        "resource_type", "state", "county",
                        "ghi_kwh_m2_day", "dni_kwh_m2_day",
                        "wind_speed_100m_ms",
                        "capacity_factor_pct",
                        "source", "collected_at",
                    ],
                )
                total_inserted = inserted + updated

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
            logger.error(f"NREL collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_point(self, centroid: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch solar resource data for a single point. Returns {record: dict} or {error: str}."""
        try:
            url = f"{self.base_url}/solar/solar_resource/v1.json"
            params = {
                "api_key": self.api_key,
                "lat": centroid["lat"],
                "lon": centroid["lng"],
            }

            client = await self.get_client()

            # Retry loop for 429 rate limits (DEMO_KEY = 30 req/hr)
            max_retries = 3
            for attempt in range(max_retries + 1):
                await self.apply_rate_limit()
                response = await client.get(url, params=params)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 120))
                    logger.info(f"NREL 429 for {centroid['county']}, waiting {retry_after}s (attempt {attempt+1})")
                    if attempt < max_retries:
                        await asyncio.sleep(retry_after)
                        continue
                break
            response.raise_for_status()

            data = response.json()
            outputs = data.get("outputs", {})

            avg_ghi = outputs.get("avg_ghi", {})
            avg_dni = outputs.get("avg_dni", {})

            ghi_annual = avg_ghi.get("annual") if isinstance(avg_ghi, dict) else None
            dni_annual = avg_dni.get("annual") if isinstance(avg_dni, dict) else None

            solar_cf = round(ghi_annual / 24 / 1000 * 0.2, 3) if ghi_annual else None
            cap_factor_pct = round(solar_cf * 100, 2) if solar_cf else None

            record = {
                "resource_type": "solar",
                "latitude": centroid["lat"],
                "longitude": centroid["lng"],
                "state": centroid["state"],
                "county": centroid["county"],
                "ghi_kwh_m2_day": round(ghi_annual, 2) if ghi_annual else None,
                "dni_kwh_m2_day": round(dni_annual, 2) if dni_annual else None,
                "wind_speed_100m_ms": None,
                "capacity_factor_pct": cap_factor_pct,
                "source": "nrel",
                "collected_at": datetime.utcnow(),
            }
            return {"record": record}

        except Exception as e:
            logger.warning(f"NREL failed for {centroid['county']}: {e}")
            return {"error": str(e)}
