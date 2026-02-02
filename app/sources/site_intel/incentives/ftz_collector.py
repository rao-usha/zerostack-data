"""
FTZ Board Collector.

Fetches Foreign Trade Zone data from FTZ Board:
- Zone locations and boundaries
- Grantee information
- Subzone designations

Data source: https://www.trade.gov/foreign-trade-zones-board
FTZ data via FTZ Board open data.

No API key required - public data.
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import ForeignTradeZone
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)


# State abbreviations
STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
    "WY", "PR", "VI",
]


@register_collector(SiteIntelSource.FTZ_BOARD)
class FTZBoardCollector(BaseCollector):
    """
    Collector for FTZ Board Foreign Trade Zone data.

    Fetches:
    - FTZ zones with grantee information
    - Zone locations and acreage
    """

    domain = SiteIntelDomain.INCENTIVES
    source = SiteIntelSource.FTZ_BOARD

    # FTZ API configuration
    default_timeout = 120.0
    rate_limit_delay = 0.5

    # FTZ Board data endpoint (ArcGIS hosted by HIFLD)
    FTZ_URL = "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Foreign_Trade_Zones/FeatureServer/0/query"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute FTZ data collection.

        Collects Foreign Trade Zone locations.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting FTZ Board data...")
            ftz_result = await self._collect_ftz_zones(config)
            total_inserted += ftz_result.get("inserted", 0)
            total_processed += ftz_result.get("processed", 0)
            if ftz_result.get("error"):
                errors.append({"source": "ftz_zones", "error": ftz_result["error"]})

            status = CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"FTZ collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_ftz_zones(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect Foreign Trade Zones.
        """
        try:
            all_zones = []
            offset = 0
            page_size = 1000

            # Build state filter
            state_filter = ""
            if config.states:
                state_list = ", ".join(f"'{s}'" for s in config.states)
                state_filter = f"STATE IN ({state_list})"

            while True:
                params = {
                    "where": state_filter if state_filter else "1=1",
                    "outFields": "*",
                    "returnGeometry": "true",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                response = await self._fetch_arcgis(self.FTZ_URL, params)
                features = response.get("features", [])

                if not features:
                    break

                all_zones.extend(features)
                logger.info(f"Fetched {len(features)} FTZ records (total: {len(all_zones)})")

                if len(features) < page_size:
                    break

                offset += page_size

            # Transform records
            records = []
            for feature in all_zones:
                transformed = self._transform_ftz_record(feature)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    ForeignTradeZone,
                    records,
                    unique_columns=["ftz_number"],
                    update_columns=[
                        "zone_name", "grantee", "state", "city", "county",
                        "latitude", "longitude", "acreage", "status",
                        "activation_date", "collected_at"
                    ],
                )
                return {"processed": len(all_zones), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect FTZ zones: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_ftz_record(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform FTZ feature to database format."""
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        ftz_number = attrs.get("FTZ_NUMBER") or attrs.get("ZONE_NO") or attrs.get("FTZ")
        if not ftz_number:
            return None

        # Extract coordinates from geometry
        lat, lng = None, None
        if geometry:
            # Could be point or polygon centroid
            if "x" in geometry and "y" in geometry:
                lng = geometry.get("x")
                lat = geometry.get("y")
            elif "rings" in geometry:
                # Polygon - calculate centroid
                rings = geometry.get("rings", [])
                if rings and rings[0]:
                    coords = rings[0]
                    lng = sum(c[0] for c in coords) / len(coords)
                    lat = sum(c[1] for c in coords) / len(coords)

        return {
            "ftz_number": str(ftz_number),
            "zone_name": attrs.get("ZONE_NAME") or attrs.get("NAME"),
            "grantee": attrs.get("GRANTEE") or attrs.get("OPERATOR"),
            "state": attrs.get("STATE"),
            "city": attrs.get("CITY"),
            "county": attrs.get("COUNTY"),
            "latitude": lat,
            "longitude": lng,
            "acreage": self._safe_float(attrs.get("ACREAGE") or attrs.get("ACRES")),
            "status": attrs.get("STATUS") or "active",
            "activation_date": self._parse_date(attrs.get("ACT_DATE") or attrs.get("ACTIVATION_DATE")),
            "source": "ftz_board",
            "collected_at": datetime.utcnow(),
        }

    def _parse_date(self, value: Any) -> Optional[datetime]:
        """Parse date value."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(value / 1000)
            except (ValueError, OSError):
                return None
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def _fetch_arcgis(self, url: str, params: Dict) -> Dict:
        """Fetch from ArcGIS REST endpoint."""
        client = await self.get_client()
        await self.apply_rate_limit()

        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"ArcGIS request failed: {url} - {e}")
            raise
