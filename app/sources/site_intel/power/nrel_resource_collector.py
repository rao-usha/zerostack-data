"""
NREL Solar/Wind Resource Data Collector.

Fetches solar irradiance and wind speed data from NREL APIs at county
centroids. Used to assess renewable energy potential for datacenter
power sourcing.

API: https://developer.nrel.gov/api/solar/solar_resource/v1
Free API key required: https://developer.nrel.gov/signup/
"""

import asyncio
import csv
import io
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

import httpx
from sqlalchemy import text
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

# State abbreviation lookup
STATE_ABBREVS = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}

CENSUS_CENTROID_URL = "https://www2.census.gov/geo/docs/reference/cenpop2020/county/CenPop2020_Mean_CO.txt"


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

    async def _load_centroids(self, config: CollectionConfig) -> List[Dict[str, Any]]:
        """Load county centroids from Census Bureau, skipping already-collected counties."""
        # Check which FIPS we already have
        try:
            result = self.db.execute(text("SELECT DISTINCT CONCAT(latitude::text, '_', longitude::text) FROM renewable_resource"))
            done = {r[0] for r in result.fetchall()}
        except Exception:
            done = set()

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(CENSUS_CENTROID_URL)
            raw = resp.text.lstrip("\ufeff")

        centroids = []
        for row in csv.DictReader(io.StringIO(raw)):
            fips = row.get("STATEFP", "").strip() + row.get("COUNTYFP", "").strip()
            if len(fips) != 5:
                continue
            try:
                lat = float(row.get("LATITUDE", "0").strip().lstrip("+"))
                lng = float(row.get("LONGITUDE", "0").strip().lstrip("+"))
            except ValueError:
                continue
            if lat == 0 or lng == 0:
                continue
            st = STATE_ABBREVS.get(row.get("STNAME", "").strip(), "")
            if not st:
                continue
            if config.states and st not in config.states:
                continue
            key = f"{lat}_{lng}"
            if key in done:
                continue
            centroids.append({
                "lat": lat, "lng": lng, "state": st,
                "county": row.get("COUNAME", "").strip(), "fips": fips,
            })
        return centroids

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect solar/wind resource data at county centroids."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            if not self.api_key:
                from app.core.config import get_settings
                self.api_key = get_settings().get_api_key("nrel") or "DEMO_KEY"

            centroids = await self._load_centroids(config)

            logger.info(f"Collecting NREL resource data for {len(centroids)} county centroids...")

            # Process in chunks of 25 to commit progress and respect rate limits
            chunk_size = 25
            for chunk_start in range(0, len(centroids), chunk_size):
                chunk = centroids[chunk_start:chunk_start + chunk_size]
                # Use 4 concurrent requests for real key, 2 for DEMO_KEY
                max_conc = 2 if self.api_key == "DEMO_KEY" else 4
                results = await self.gather_with_limit(
                    [self._collect_point(c) for c in chunk], max_concurrent=max_conc
                )

                chunk_records = []
                for i, result in enumerate(results):
                    centroid = chunk[i]
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
                        chunk_records.append(result["record"])

                if chunk_records:
                    inserted, updated = self.bulk_upsert(
                        RenewableResource,
                        chunk_records,
                        unique_columns=["latitude", "longitude"],
                        update_columns=[
                            "resource_type", "state", "county",
                            "ghi_kwh_m2_day", "dni_kwh_m2_day",
                            "wind_speed_100m_ms",
                            "capacity_factor_pct",
                            "source", "collected_at",
                        ],
                    )
                    total_inserted += inserted + updated

                logger.info(
                    f"NREL progress: {chunk_start + len(chunk)}/{len(centroids)} "
                    f"counties, {total_inserted} inserted"
                )

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
