"""
USGS 3DEP Elevation Collector.

Fetches county-level elevation statistics using the USGS Elevation Point
Query Service (EPQS). For each county, samples 5 points (centroid + 4
cardinal offsets) to compute min/max/mean elevation.

Data source: https://epqs.nationalmap.gov/v1/json
County centroids: Census Bureau CenPop2020 file
Auth: None required
"""

import asyncio
import csv
import io
import logging
import statistics
from datetime import datetime
from typing import Optional, Dict, Any, List

import httpx
from sqlalchemy.orm import Session

from app.core.models_site_intel import CountyElevation
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

EPQS_URL = "https://epqs.nationalmap.gov/v1/json"
COUNTY_CENTROIDS_URL = (
    "https://www2.census.gov/geo/docs/reference/cenpop2020/county/"
    "CenPop2020_Mean_CO.txt"
)

# Offset in degrees for cardinal sample points (~5 miles)
LAT_OFFSET = 0.072  # ~5 miles
LNG_OFFSET = 0.090  # ~5 miles at ~40° latitude

# Meters to feet conversion
METERS_TO_FEET = 3.28084

# Concurrency settings
MAX_CONCURRENT_COUNTIES = 8
MAX_CONCURRENT_POINTS = 5  # 5 points per county, all independent
BATCH_COMMIT_SIZE = 50


@register_collector(SiteIntelSource.USGS_3DEP)
class USGS3DEPElevationCollector(BaseCollector):
    """
    Collector for county-level elevation data from USGS 3DEP via EPQS.

    Samples 5 points per county (centroid + N/S/E/W offsets) to derive
    min, max, and mean elevation statistics.

    Uses bounded concurrency: 8 counties in-flight, 5 points per county
    queried in parallel. ~20x faster than sequential.
    """

    domain = SiteIntelDomain.RISK
    source = SiteIntelSource.USGS_3DEP

    default_timeout = 30.0
    rate_limit_delay = 0.05  # EPQS has no documented rate limit; 0.05s with 8 concurrent = 0.4s effective

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)
        self._county_centroids: Optional[List[Dict[str, Any]]] = None

    def get_default_base_url(self) -> str:
        return EPQS_URL

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect elevation statistics for counties with concurrent queries."""
        try:
            logger.info("Downloading county centroids from Census Bureau...")
            centroids = await self._fetch_county_centroids()
            if not centroids:
                return self.create_result(
                    status=CollectionStatus.FAILED,
                    error_message="Failed to fetch county centroids",
                )

            if config.states:
                state_set = {s.upper() for s in config.states}
                centroids = [c for c in centroids if c["state"] in state_set]

            total = len(centroids)
            logger.info(f"Processing {total} counties for elevation data (concurrent: {MAX_CONCURRENT_COUNTIES} counties, {MAX_CONCURRENT_POINTS} points)")

            records = []
            errors = []
            sem = asyncio.Semaphore(MAX_CONCURRENT_COUNTIES)

            async def _bounded_county(county):
                async with sem:
                    return await self._collect_county_elevation(county)

            # Process in batches for incremental DB commits
            for batch_start in range(0, total, BATCH_COMMIT_SIZE):
                batch_end = min(batch_start + BATCH_COMMIT_SIZE, total)
                batch_centroids = centroids[batch_start:batch_end]

                tasks = [_bounded_county(c) for c in batch_centroids]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                batch_records = []
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        errors.append({
                            "county": batch_centroids[i].get("county", ""),
                            "state": batch_centroids[i].get("state", ""),
                            "error": str(result),
                        })
                    elif result is not None:
                        batch_records.append(result)

                # Commit this batch immediately
                if batch_records:
                    self.bulk_upsert(
                        CountyElevation,
                        batch_records,
                        unique_columns=["fips_code"],
                        update_columns=[
                            "state", "county",
                            "min_elevation_ft", "max_elevation_ft",
                            "mean_elevation_ft", "elevation_range_ft",
                            "sample_points", "collected_at",
                        ],
                    )
                    records.extend(batch_records)

                processed = batch_end
                logger.info(
                    f"Processed {processed}/{total} counties "
                    f"({len(records)} successful, {len(errors)} errors)"
                )
                self.update_progress(processed, total, "Querying elevation")

            status = CollectionStatus.SUCCESS
            if errors and not records:
                status = CollectionStatus.FAILED
            elif errors:
                status = CollectionStatus.PARTIAL

            return self.create_result(
                status=status,
                total=total,
                processed=total,
                inserted=len(records),
                failed=len(errors),
                errors=errors[:20] if errors else None,
            )

        except Exception as e:
            logger.error(f"Elevation collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _fetch_county_centroids(self) -> List[Dict[str, Any]]:
        """Download county centroids from Census Bureau."""
        if self._county_centroids is not None:
            return self._county_centroids

        async with httpx.AsyncClient(timeout=60.0) as dl_client:
            try:
                response = await dl_client.get(COUNTY_CENTROIDS_URL)
                response.raise_for_status()
                text = response.text
            except Exception as e:
                logger.error(f"Failed to download county centroids: {e}")
                return []

        centroids = []
        if text.startswith("\ufeff"):
            text = text[1:]
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            try:
                state_fips = row.get("STATEFP", "").strip()
                county_fips = row.get("COUNTYFP", "").strip()
                fips_code = state_fips + county_fips
                if len(fips_code) != 5:
                    continue

                lat_str = row.get("LATITUDE", "0").strip().lstrip("+")
                lng_str = row.get("LONGITUDE", "0").strip().lstrip("+")
                lat = float(lat_str)
                lng = float(lng_str)
                if lat == 0 or lng == 0:
                    continue

                state_name = row.get("STNAME", "").strip()
                state_abbr = STATE_NAME_TO_ABBR.get(state_name, "")
                if not state_abbr:
                    continue

                county_name = row.get("COUNAME", "").strip()

                centroids.append({
                    "state": state_abbr,
                    "county": county_name,
                    "fips_code": fips_code,
                    "lat": lat,
                    "lng": lng,
                })
            except (ValueError, KeyError):
                continue

        logger.info(f"Loaded {len(centroids)} county centroids")
        self._county_centroids = centroids
        return centroids

    async def _collect_county_elevation(
        self, county: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Query elevation at 5 sample points concurrently and compute stats."""
        lat = county["lat"]
        lng = county["lng"]

        sample_points = [
            (lat, lng),                        # center
            (lat + LAT_OFFSET, lng),           # north
            (lat - LAT_OFFSET, lng),           # south
            (lat, lng + LNG_OFFSET),           # east
            (lat, lng - LNG_OFFSET),           # west
        ]

        # Query all 5 points concurrently
        tasks = [self._query_elevation(pt_lat, pt_lng) for pt_lat, pt_lng in sample_points]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        elevations = []
        for r in results:
            if isinstance(r, Exception):
                continue
            if r is not None:
                elevations.append(r)

        if not elevations:
            return None

        min_elev = min(elevations)
        max_elev = max(elevations)
        mean_elev = round(statistics.mean(elevations), 2)

        return {
            "state": county["state"],
            "county": county["county"],
            "fips_code": county["fips_code"],
            "min_elevation_ft": round(min_elev, 2),
            "max_elevation_ft": round(max_elev, 2),
            "mean_elevation_ft": mean_elev,
            "elevation_range_ft": round(max_elev - min_elev, 2),
            "sample_points": len(elevations),
            "source": "usgs_3dep",
            "collected_at": datetime.utcnow(),
        }

    async def _query_elevation(self, lat: float, lng: float) -> Optional[float]:
        """Query USGS EPQS for elevation at a point. Returns feet or None."""
        await self.apply_rate_limit()

        client = await self.get_client()
        try:
            params = {
                "x": str(lng),
                "y": str(lat),
                "wkid": "4326",
                "units": "Feet",
                "includeDate": "false",
            }
            response = await client.get(EPQS_URL, params=params)
            response.raise_for_status()
            data = response.json()

            value = data.get("value")
            if value is None:
                return None

            return float(value)

        except Exception as e:
            logger.debug(f"EPQS query failed at ({lat}, {lng}): {e}")
            return None


# State name -> abbreviation mapping for Census data parsing
STATE_NAME_TO_ABBR = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT",
    "Delaware": "DE", "District of Columbia": "DC", "Florida": "FL",
    "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY",
    "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY", "Puerto Rico": "PR",
}
