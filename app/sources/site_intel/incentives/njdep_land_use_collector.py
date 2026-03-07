"""
NJ DEP Land Use / Land Cover (LULC) 2020 Collector.

Fetches statewide land use data from NJDEP ArcGIS REST API using
server-side aggregation (outStatistics) by county bounding box.
Returns acres per land use category per county.

API: https://services1.arcgis.com/QWdNfRs7lkPq4g4Q/arcgis/rest/services/Land_Use_2020/FeatureServer/5
No API key required.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

from sqlalchemy.orm import Session

from app.core.models_site_intel import LandUseParcel
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

LULC_QUERY_URL = (
    "https://services1.arcgis.com/QWdNfRs7lkPq4g4Q/arcgis/rest/services"
    "/Land_Use_2020/FeatureServer/5/query"
)

# NJ county bounding boxes (min_lng, min_lat, max_lng, max_lat)
# and FIPS codes for all 21 NJ counties
NJ_COUNTIES = {
    "Atlantic":     {"fips": "34001", "bbox": (-74.87, 39.29, -74.33, 39.62)},
    "Bergen":       {"fips": "34003", "bbox": (-74.27, 40.81, -73.89, 41.13)},
    "Burlington":   {"fips": "34005", "bbox": (-74.99, 39.62, -74.39, 40.14)},
    "Camden":       {"fips": "34007", "bbox": (-75.13, 39.72, -74.85, 39.97)},
    "Cape May":     {"fips": "34009", "bbox": (-75.10, 38.93, -74.52, 39.29)},
    "Cumberland":   {"fips": "34011", "bbox": (-75.47, 39.07, -74.87, 39.52)},
    "Essex":        {"fips": "34013", "bbox": (-74.38, 40.70, -74.10, 40.89)},
    "Gloucester":   {"fips": "34015", "bbox": (-75.41, 39.52, -74.96, 39.83)},
    "Hudson":       {"fips": "34017", "bbox": (-74.13, 40.63, -74.00, 40.79)},
    "Hunterdon":    {"fips": "34019", "bbox": (-75.09, 40.39, -74.65, 40.72)},
    "Mercer":       {"fips": "34021", "bbox": (-74.89, 40.15, -74.46, 40.42)},
    "Middlesex":    {"fips": "34023", "bbox": (-74.56, 40.30, -74.15, 40.61)},
    "Monmouth":     {"fips": "34025", "bbox": (-74.47, 40.13, -73.93, 40.49)},
    "Morris":       {"fips": "34027", "bbox": (-74.79, 40.71, -74.26, 41.05)},
    "Ocean":        {"fips": "34029", "bbox": (-74.54, 39.49, -74.06, 40.08)},
    "Passaic":      {"fips": "34031", "bbox": (-74.48, 40.87, -74.11, 41.21)},
    "Salem":        {"fips": "34033", "bbox": (-75.56, 39.36, -75.05, 39.70)},
    "Somerset":     {"fips": "34035", "bbox": (-74.79, 40.40, -74.38, 40.66)},
    "Sussex":       {"fips": "34037", "bbox": (-74.90, 41.01, -74.37, 41.36)},
    "Union":        {"fips": "34039", "bbox": (-74.40, 40.57, -74.15, 40.74)},
    "Warren":       {"fips": "34041", "bbox": (-75.20, 40.56, -74.74, 41.01)},
}

# Normalize NJDEP LABEL20 values to standard categories
LAND_USE_CATEGORY_MAP = {
    "INDUSTRIAL": "industrial",
    "INDUSTRIAL AND COMMERCIAL COMPLEXES": "industrial",
    "COMMERCIAL/SERVICES": "commercial",
    "MIXED URBAN OR BUILT-UP LAND": "commercial",
    "RESIDENTIAL, HIGH DENSITY OR MULTIPLE DWELLING": "residential",
    "RESIDENTIAL, SINGLE UNIT, MEDIUM DENSITY": "residential",
    "RESIDENTIAL, SINGLE UNIT, LOW DENSITY": "residential",
    "RESIDENTIAL, RURAL, SINGLE UNIT": "residential",
    "MIXED RESIDENTIAL": "residential",
    "RECREATIONAL LAND": "other",
    "STADIUM, THEATERS, CULTURAL CENTERS AND ZOOS": "other",
    "CEMETERY": "other",
    "ATHLETIC FIELDS (SCHOOLS)": "other",
    "OTHER URBAN OR BUILT-UP LAND": "other",
    "STORMWATER BASIN": "other",
    "TRANSPORTATION/COMMUNICATION/UTILITIES": "infrastructure",
    "MAJOR ROADWAY": "infrastructure",
    "RAILROADS": "infrastructure",
    "AIRPORT FACILITIES": "infrastructure",
    "MILITARY INSTALLATIONS": "infrastructure",
    "NO LONGER MILITARY": "other",
    "UPLAND RIGHTS-OF-WAY DEVELOPED": "infrastructure",
    "UPLAND RIGHTS-OF-WAY UNDEVELOPED": "other",
    "MIXED TRANSPORTATION CORRIDOR OVERLAP AREA": "infrastructure",
    "BRIDGE OVER WATER": "infrastructure",
}


def normalize_land_use_category(label: str) -> str:
    """Normalize a NJDEP LABEL20 to a standard category."""
    if not label:
        return "other"
    upper = label.strip().upper()
    if upper in LAND_USE_CATEGORY_MAP:
        return LAND_USE_CATEGORY_MAP[upper]
    # Infer from TYPE20-level keywords
    if "FOREST" in upper or "DECIDUOUS" in upper or "CONIFEROUS" in upper:
        return "forest"
    if "WETLAND" in upper or "MARSH" in upper or "PHRAGMITES" in upper:
        return "wetlands"
    if "WATER" in upper or "LAKE" in upper or "RIVER" in upper or "TIDAL" in upper or "STREAM" in upper or "LAGOON" in upper:
        return "water"
    if "AGRIC" in upper or "CROP" in upper or "ORCHARD" in upper or "NURSERY" in upper or "PASTURE" in upper or "CONFINED" in upper:
        return "agricultural"
    if "BARREN" in upper or "BARE" in upper or "BEACH" in upper or "DUNE" in upper or "ALTERED" in upper or "TRANSITIONAL" in upper or "EXTRACTIVE" in upper:
        return "barren"
    if "BRUSH" in upper or "SHRUB" in upper or "OLD FIELD" in upper:
        return "forest"
    return "other"


@register_collector(SiteIntelSource.NJDEP_LULC)
class NJDEPLandUseCollector(BaseCollector):
    """
    Collector for NJ DEP Land Use / Land Cover 2020 data.

    Uses ArcGIS REST outStatistics to aggregate land use acres by
    category per county bounding box. 21 queries total (one per county).
    """

    domain = SiteIntelDomain.INCENTIVES
    source = SiteIntelSource.NJDEP_LULC

    default_timeout = 120.0
    rate_limit_delay = 0.5

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return LULC_QUERY_URL

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect NJ land use data aggregated by county."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            counties = list(NJ_COUNTIES.items())
            logger.info(f"Collecting NJDEP LULC data for {len(counties)} NJ counties...")

            for county_name, info in counties:
                try:
                    result = await self._collect_county(
                        county_name, info["fips"], info["bbox"]
                    )
                    total_inserted += result.get("inserted", 0)
                    total_processed += result.get("processed", 0)
                    if result.get("error"):
                        errors.append({
                            "source": f"lulc_{county_name}",
                            "error": result["error"],
                        })
                except Exception as e:
                    logger.warning(f"LULC collection failed for {county_name}: {e}")
                    errors.append({
                        "source": f"lulc_{county_name}",
                        "error": str(e),
                    })

            status = CollectionStatus.SUCCESS
            if errors and total_inserted > 0:
                status = CollectionStatus.PARTIAL
            elif errors and total_inserted == 0:
                status = CollectionStatus.FAILED

            logger.info(
                f"NJDEP LULC collection complete: {total_processed} processed, "
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
            logger.error(f"NJDEP LULC collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_county(
        self, county_name: str, county_fips: str, bbox: Tuple[float, ...]
    ) -> Dict[str, Any]:
        """Fetch aggregated land use stats for one county."""
        import asyncio

        client = await self.get_client()
        await self.apply_rate_limit()

        envelope = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

        out_stats = json.dumps([
            {
                "statisticType": "sum",
                "onStatisticField": "ACRES",
                "outStatisticFieldName": "total_acres",
            },
            {
                "statisticType": "sum",
                "onStatisticField": "ISACRES20",
                "outStatisticFieldName": "impervious_acres",
            },
            {
                "statisticType": "count",
                "onStatisticField": "OBJECTID",
                "outStatisticFieldName": "poly_count",
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
            "groupByFieldsForStatistics": "LABEL20",
            "f": "json",
        }

        for attempt in range(3):
            try:
                response = await client.get(LULC_QUERY_URL, params=params)

                if response.status_code in (500, 502, 503, 504):
                    if attempt < 2:
                        logger.debug(
                            f"LULC {county_name} {response.status_code}, retrying..."
                        )
                        await asyncio.sleep(self.rate_limit_delay * (attempt + 1))
                        continue
                    return {
                        "processed": 0, "inserted": 0,
                        "error": f"Server error {response.status_code}",
                    }

                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    return {
                        "processed": 0, "inserted": 0,
                        "error": data["error"].get("message", "API error"),
                    }

                features = data.get("features", [])
                if not features:
                    return {"processed": 0, "inserted": 0}

                records = []
                for feat in features:
                    attrs = feat.get("attributes", {})
                    label = attrs.get("LABEL20", "")
                    total_acres = attrs.get("total_acres", 0)
                    impervious = attrs.get("impervious_acres", 0)
                    poly_count = attrs.get("poly_count", 0)

                    if not label:
                        continue

                    category = normalize_land_use_category(label)

                    # Use first 20 chars of label as code
                    code = label[:20].strip()

                    records.append({
                        "state": "NJ",
                        "county": county_name,
                        "county_fips": county_fips,
                        "land_use_code": code,
                        "land_use_label": label[:255],
                        "land_use_category": category,
                        "acres": round(float(total_acres), 2) if total_acres else None,
                        "polygon_count": int(poly_count) if poly_count else None,
                        "impervious_acres": round(float(impervious), 2) if impervious else None,
                        "source": "njdep_lulc_2020",
                        "collected_at": datetime.utcnow(),
                    })

                logger.info(
                    f"LULC {county_name} County: {len(records)} land use categories"
                )

                if records:
                    inserted, updated = self.bulk_upsert(
                        LandUseParcel,
                        records,
                        unique_columns=["state", "county", "land_use_code"],
                        update_columns=[
                            "land_use_label", "land_use_category", "acres",
                            "polygon_count", "impervious_acres", "source",
                            "collected_at",
                        ],
                    )
                    return {"processed": len(records), "inserted": inserted + updated}

                return {"processed": 0, "inserted": 0}

            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(self.rate_limit_delay)
                    continue
                return {"processed": 0, "inserted": 0, "error": str(e)}
