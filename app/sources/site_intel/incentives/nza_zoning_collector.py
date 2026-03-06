"""
National Zoning Atlas (NZA) Collector.

Fetches zoning district data from Mercatus Center ZIP archives containing
GeoJSON/CSV per state. Each participating state has a zip with zoning
district polygons and attributes (zone code, category, jurisdiction, etc.).

Data source: https://www.zoningatlas.org/
Downloads: https://www.mercatus.org/state-and-regional-zoning-atlas-datasets
No API key required.

Coverage (as of 2025): CO, HI, MA, MT, NH, TN, TX, VA (~8 states/regions)
"""

import io
import json
import logging
import zipfile
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
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)

MERCATUS_BASE = "https://www.mercatus.org"

# Available state/region downloads from Mercatus Center
# Keys = state abbrev, values = (download path, region label)
NZA_STATE_FILES = {
    "CO": ("/media/document/coloradozoningatlas2025zip", "Denver Area"),
    "HI": ("/sites/default/files/2024-09/hi_zoning_atlas_2023.zip", "Statewide"),
    "MT": ("/sites/default/files/2024-09/mt_zoning_atlas_2023.zip", "Statewide"),
    "NH": ("/sites/default/files/2024-09/nh_zoning_atlas_2023.zip", "Statewide"),
    "TN": ("/sites/default/files/2024-09/tn_zoning_atlas_middletn_2023.zip", "Middle TN"),
    "TX": ("/sites/default/files/2024-10/tx_zoning_atlas_dfw_2024.zip", "DFW"),
    "VA": ("/sites/default/files/2024-10/va_zoning_atlas_hamptonroads_2024.zip", "Hampton Roads"),
}

# Zone category mappings — NZA uses various category labels
# We normalize to: industrial, commercial, residential, mixed, agricultural, other
CATEGORY_MAP = {
    # Standard
    "residential": "residential",
    "single family residential": "residential",
    "multifamily residential": "residential",
    "single-family residential": "residential",
    "multi-family residential": "residential",
    "commercial": "commercial",
    "industrial": "industrial",
    "manufacturing": "industrial",
    "mixed use": "mixed",
    "mixed-use": "mixed",
    "agricultural": "agricultural",
    "rural": "agricultural",
    "open space": "other",
    "overlay": "other",
    "planned development": "mixed",
    "special purpose": "other",
    "institutional": "other",
    # NZA-specific labels (NH, MT, etc.)
    "primarily residential": "residential",
    "nonresidential": "commercial",
    "mixed with residential": "mixed",
    "mixed with nonresidential": "mixed",
    "mixed": "mixed",
    # MT short codes
    "r": "residential",
    "c": "commercial",
    "i": "industrial",
    "m": "mixed",
    "a": "agricultural",
}

# Categories that typically allow these uses
INDUSTRIAL_CATEGORIES = {"industrial", "mixed"}
WAREHOUSE_CATEGORIES = {"industrial", "commercial", "mixed"}
DATACENTER_CATEGORIES = {"industrial", "commercial", "mixed"}


def normalize_category(raw: Optional[str]) -> str:
    """Normalize a zoning category string to a standard value."""
    if not raw:
        return "other"
    key = raw.strip().lower()
    return CATEGORY_MAP.get(key, "other")


def infer_uses(category: str) -> Dict[str, bool]:
    """Infer allowed uses from normalized zone category."""
    return {
        "allows_manufacturing": category in INDUSTRIAL_CATEGORIES,
        "allows_warehouse": category in WAREHOUSE_CATEGORIES,
        "allows_data_center": category in DATACENTER_CATEGORIES,
    }


@register_collector(SiteIntelSource.NATIONAL_ZONING_ATLAS)
class NZAZoningCollector(BaseCollector):
    """
    Collector for National Zoning Atlas zoning district data.

    Downloads ZIP archives from Mercatus Center containing GeoJSON/CSV
    per state, parses features into zoning_district records.
    """

    domain = SiteIntelDomain.INCENTIVES
    source = SiteIntelSource.NATIONAL_ZONING_ATLAS

    default_timeout = 180.0
    rate_limit_delay = 2.0

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return MERCATUS_BASE

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "*/*",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect NZA zoning data for available states."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            # Filter to requested states, or use all available
            if config.states:
                states = [
                    s for s in config.states
                    if s in NZA_STATE_FILES
                ]
                skipped = [s for s in config.states if s not in NZA_STATE_FILES]
                if skipped:
                    logger.info(f"NZA: No data available for states: {skipped}")
            else:
                states = list(NZA_STATE_FILES.keys())

            if not states:
                return self.create_result(
                    status=CollectionStatus.FAILED,
                    error_message="No NZA data available for requested states. "
                    f"Available: {list(NZA_STATE_FILES.keys())}",
                )

            logger.info(f"Collecting NZA zoning data for {len(states)} states: {states}")

            for state in states:
                try:
                    result = await self._collect_state(state)
                    total_inserted += result.get("inserted", 0)
                    total_processed += result.get("processed", 0)
                    if result.get("error"):
                        errors.append(
                            {"source": f"nza_{state}", "error": result["error"]}
                        )
                except Exception as e:
                    logger.warning(f"NZA collection failed for {state}: {e}")
                    errors.append({"source": f"nza_{state}", "error": str(e)})

            status = CollectionStatus.SUCCESS
            if errors and total_inserted > 0:
                status = CollectionStatus.PARTIAL
            elif errors and total_inserted == 0:
                status = CollectionStatus.FAILED

            logger.info(
                f"NZA collection complete: {total_processed} processed, "
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
            logger.error(f"NZA collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_state(self, state: str) -> Dict[str, Any]:
        """Download ZIP, extract GeoJSON, parse into zoning records."""
        entry = NZA_STATE_FILES.get(state)
        if not entry:
            return {"processed": 0, "inserted": 0, "error": f"No file for {state}"}

        path, region = entry
        url = f"{MERCATUS_BASE}{path}"
        client = await self.get_client()

        try:
            await self.apply_rate_limit()
            logger.info(f"NZA: Downloading {state} ({region}) ZIP...")

            response = await client.get(url, timeout=180.0, follow_redirects=True)

            if response.status_code == 404:
                return {"processed": 0, "inserted": 0, "error": f"File not found: {path}"}

            response.raise_for_status()

            # Extract GeoJSON from ZIP
            features = self._extract_geojson_from_zip(response.content, state)

            if features is None:
                return {"processed": 0, "inserted": 0, "error": f"No GeoJSON found in ZIP for {state}"}

            logger.info(f"NZA {state}: {len(features)} features extracted from ZIP")

            records = []
            for feature in features:
                record = self._transform_feature(feature, state)
                if record:
                    records.append(record)

            logger.info(f"NZA {state}: {len(records)} valid zoning records")

            if records:
                inserted, updated = self.bulk_upsert(
                    ZoningDistrict,
                    records,
                    unique_columns=["jurisdiction", "state", "zone_code"],
                    update_columns=[
                        "zone_name", "zone_category",
                        "allows_manufacturing", "allows_warehouse", "allows_data_center",
                        "geometry_geojson", "source", "collected_at",
                    ],
                )
                return {"processed": len(features), "inserted": inserted + updated}

            return {"processed": len(features), "inserted": 0}

        except Exception as e:
            logger.warning(f"NZA {state} download/parse failed: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _extract_geojson_from_zip(
        self, zip_bytes: bytes, state: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Extract GeoJSON features from a ZIP archive."""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                # Find first .geojson or .json file
                geojson_files = [
                    n for n in zf.namelist()
                    if n.lower().endswith((".geojson", ".json"))
                    and not n.startswith("__MACOSX")
                ]

                if not geojson_files:
                    logger.warning(f"NZA {state}: No GeoJSON files in ZIP. Contents: {zf.namelist()[:10]}")
                    return None

                # Use the largest GeoJSON file (likely the main data)
                target = max(geojson_files, key=lambda n: zf.getinfo(n).file_size)
                logger.info(f"NZA {state}: Extracting {target}")

                with zf.open(target) as f:
                    data = json.load(f)

                return data.get("features", [])

        except zipfile.BadZipFile:
            logger.warning(f"NZA {state}: Invalid ZIP file")
            return None
        except json.JSONDecodeError as e:
            logger.warning(f"NZA {state}: Invalid JSON in ZIP: {e}")
            return None

    def _transform_feature(
        self, feature: Dict[str, Any], state: str
    ) -> Optional[Dict[str, Any]]:
        """Transform a GeoJSON feature into a zoning_district record."""
        props = feature.get("properties", {})
        geometry = feature.get("geometry")

        # NZA property names vary wildly per state. Try all known variants.
        # NH: AbbreviatedDistrict, Full District Name, Jurisdiction, Type of Zoning District
        # MT: Z (zone name), T (town), Ty (type code)
        # Others: ZoneCode, zone_code, Zone, etc.
        zone_code = (
            props.get("AbbreviatedDistrict")
            or props.get("Abbreviated District Name")
            or props.get("ZoneCode")
            or props.get("zone_code")
            or props.get("ZONE_CODE")
            or props.get("Zone")
            or props.get("zone")
            or props.get("ZONE")
            or props.get("ZoneDist")
            or props.get("ZONEDIST")
            or props.get("Join")  # MT/NH fallback: "Town:Code"
            or props.get("Z")  # MT: zone name as code
        )

        if not zone_code:
            return None

        zone_name = (
            props.get("Full District Name")
            or props.get("ZoneName")
            or props.get("zone_name")
            or props.get("ZONE_NAME")
            or props.get("ZoneDesc")
            or props.get("ZONEDESC")
            or props.get("Description")
            or props.get("Z")  # MT: zone name
        )

        jurisdiction = (
            props.get("Jurisdiction")
            or props.get("jurisdiction")
            or props.get("JURISDICTION")
            or props.get("Municipality")
            or props.get("municipality")
            or props.get("TOWN")
            or props.get("Town")
            or props.get("T")  # MT: town
            or "Unknown"
        )

        raw_category = (
            props.get("Type of Zoning District")
            or props.get("Type of Nonresidential District")
            or props.get("ZoneCategory")
            or props.get("zone_category")
            or props.get("ZONE_CATEGORY")
            or props.get("Category")
            or props.get("category")
            or props.get("LandUse")
            or props.get("LANDUSE")
            or props.get("Ty")  # MT: type code
        )

        category = normalize_category(raw_category)
        uses = infer_uses(category)

        return {
            "jurisdiction": str(jurisdiction)[:255],
            "state": state,
            "zone_code": str(zone_code)[:50],
            "zone_name": str(zone_name)[:255] if zone_name else None,
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
            "geometry_geojson": geometry,
            "source": "national_zoning_atlas",
            "collected_at": datetime.utcnow(),
        }
