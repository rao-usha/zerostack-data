"""
NJ County-Level Zoning Collector.

Fetches actual zoning district polygons from NJ county ArcGIS endpoints.
Currently supports:
- Sussex County (2,478 districts)

Endpoints are public ArcGIS FeatureServers, no API key required.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import ZoningDistrict
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
    CollectionStatus,
)
from app.sources.site_intel.incentives.nza_zoning_collector import infer_uses

logger = logging.getLogger(__name__)

# NJ county zoning ArcGIS endpoints
# Each entry: (url, name_field_for_jurisdiction, max_record_count)
NJ_COUNTY_ENDPOINTS = {
    "Sussex": {
        "url": "https://services.arcgis.com/opPd2BqYeMe7vELn/arcgis/rest/services/Zoning/FeatureServer/0/query",
        "max_records": 1000,
    },
}

# Sussex County zone composite -> standard category mapping
SUSSEX_COMPOSITE_MAP = {
    "Commercial/Business": "commercial",
    "Heavy Industrial": "industrial",
    "Light Industrial": "industrial",
    "Industrial": "industrial",
    "Mixed-Use": "mixed",
    "Multi-family Residential": "residential",
    "Recreation/Tourism": "other",
    "Rural Residential": "residential",
    "Planned Development": "mixed",
    "Public/Institutional": "other",
    "Conservation/Open Space": "other",
}


def _sussex_category(composite: str) -> str:
    """Map Sussex County ZONECOMPOSITE to standard category."""
    if not composite:
        return "other"
    # Check direct mapping first
    for key, cat in SUSSEX_COMPOSITE_MAP.items():
        if key.lower() in composite.lower():
            return cat
    # Fallback heuristics
    c = composite.lower()
    if "residential" in c:
        return "residential"
    if "commercial" in c or "business" in c or "office" in c:
        return "commercial"
    if "industrial" in c or "manufactur" in c:
        return "industrial"
    if "mixed" in c:
        return "mixed"
    if "agricultural" in c or "farm" in c:
        return "agricultural"
    return "other"


class NJCountyZoningCollector(BaseCollector):
    """
    Collector for NJ county-level zoning district data.

    Fetches actual zone district boundaries from county ArcGIS endpoints.
    Not registered in collector registry — called directly from API.
    """

    domain = SiteIntelDomain.INCENTIVES
    source = SiteIntelSource.NJDEP_LULC

    default_timeout = 60.0
    rate_limit_delay = 0.3

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://services.arcgis.com"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect NJ county zoning data."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            for county_name, info in NJ_COUNTY_ENDPOINTS.items():
                try:
                    result = await self._collect_county(county_name, info)
                    total_inserted += result.get("inserted", 0)
                    total_processed += result.get("processed", 0)
                    if result.get("error"):
                        errors.append({
                            "source": f"zoning_{county_name}",
                            "error": result["error"],
                        })
                except Exception as e:
                    logger.warning(f"Zoning collection failed for {county_name}: {e}")
                    errors.append({
                        "source": f"zoning_{county_name}",
                        "error": str(e),
                    })

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
            logger.error(f"NJ zoning collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_county(
        self, county_name: str, info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch all zoning districts from a county endpoint."""
        url = info["url"]
        max_records = info.get("max_records", 1000)
        client = await self.get_client()

        all_features: List[Dict] = []
        offset = 0

        while True:
            await self.apply_rate_limit()

            params = {
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "false",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": max_records,
            }

            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    return {
                        "processed": 0, "inserted": 0,
                        "error": data["error"].get("message", "API error"),
                    }

                features = data.get("features", [])
                if not features:
                    break

                all_features.extend(features)
                logger.info(
                    f"Zoning {county_name}: fetched {len(features)} "
                    f"(total: {len(all_features)})"
                )

                if len(features) < max_records and not data.get("exceededTransferLimit"):
                    break

                offset += max_records

            except Exception as e:
                return {
                    "processed": len(all_features), "inserted": 0,
                    "error": str(e),
                }

        if not all_features:
            return {"processed": 0, "inserted": 0}

        # Transform to zoning_district records
        records = []
        for feat in all_features:
            record = self._transform_sussex(feat, county_name)
            if record:
                records.append(record)

        logger.info(f"Zoning {county_name}: {len(records)} valid districts")

        if records:
            try:
                inserted, updated = self.bulk_upsert(
                    ZoningDistrict,
                    records,
                    unique_columns=["jurisdiction", "state", "zone_code"],
                    update_columns=[
                        "zone_name", "zone_category",
                        "allows_manufacturing", "allows_warehouse",
                        "allows_data_center", "source", "collected_at",
                    ],
                )
                return {"processed": len(all_features), "inserted": inserted + updated}
            except Exception as e:
                logger.warning(f"Zoning {county_name} upsert failed: {e}")
                try:
                    self.db.rollback()
                except Exception:
                    pass
                return {"processed": len(all_features), "inserted": 0, "error": str(e)}

        return {"processed": len(all_features), "inserted": 0}

    def _transform_sussex(
        self, feature: Dict[str, Any], county_name: str
    ) -> Optional[Dict[str, Any]]:
        """Transform a Sussex County zoning feature."""
        attrs = feature.get("attributes", {})

        zone_code = attrs.get("ZONECLASS")
        if not zone_code:
            return None

        zone_desc = attrs.get("ZONEDESC", "")
        composite = attrs.get("ZONECOMPOSITE", "")

        category = _sussex_category(composite)
        uses = infer_uses(category)

        # Use county as jurisdiction since we don't have municipality field
        jurisdiction = f"{county_name} County"

        return {
            "jurisdiction": jurisdiction,
            "state": "NJ",
            "zone_code": str(zone_code)[:50],
            "zone_name": (zone_desc or composite)[:255] if (zone_desc or composite) else None,
            "zone_category": category,
            "allows_manufacturing": uses["allows_manufacturing"],
            "allows_warehouse": uses["allows_warehouse"],
            "allows_data_center": uses["allows_data_center"],
            "max_height_ft": None,
            "max_far": None,
            "min_lot_sqft": None,
            "setback_front_ft": None,
            "setback_side_ft": None,
            "setback_rear_ft": None,
            "parking_ratio": None,
            "geometry_geojson": None,
            "source": "nj_sussex_county_gis",
            "collected_at": datetime.utcnow(),
        }
