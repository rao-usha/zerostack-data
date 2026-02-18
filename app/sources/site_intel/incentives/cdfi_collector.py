"""
CDFI Opportunity Zone Collector.

Fetches Opportunity Zone data from CDFI Fund:
- Designated OZ census tracts
- Low-income community status
- Contiguous tract information

Data source: https://www.cdfifund.gov/opportunity-zones
Census tract data via CDFI open data APIs.

No API key required - public data.
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import OpportunityZone
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


# State FIPS to abbreviation mapping
FIPS_TO_STATE = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
    "72": "PR",
    "78": "VI",
}

STATE_TO_FIPS = {v: k for k, v in FIPS_TO_STATE.items()}


@register_collector(SiteIntelSource.CDFI_OZ)
class CDFIOpportunityZoneCollector(BaseCollector):
    """
    Collector for CDFI Fund Opportunity Zone data.

    Fetches:
    - Designated OZ census tracts
    - Low-income community designations
    """

    domain = SiteIntelDomain.INCENTIVES
    source = SiteIntelSource.CDFI_OZ

    # CDFI API configuration
    default_timeout = 120.0
    rate_limit_delay = 0.3

    # HUD Opportunity Zones data endpoint (ArcGIS) - Updated Jan 2026
    # Layer 13 contains designated Opportunity Zones
    OZ_URL = "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/Opportunity_Zones/FeatureServer/13/query"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute CDFI OZ data collection.

        Collects Opportunity Zone census tracts.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting CDFI Opportunity Zone data...")
            oz_result = await self._collect_opportunity_zones(config)
            total_inserted += oz_result.get("inserted", 0)
            total_processed += oz_result.get("processed", 0)
            if oz_result.get("error"):
                errors.append(
                    {"source": "opportunity_zones", "error": oz_result["error"]}
                )

            status = (
                CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL
            )

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"CDFI OZ collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_opportunity_zones(
        self, config: CollectionConfig
    ) -> Dict[str, Any]:
        """
        Collect Opportunity Zone census tracts.
        """
        try:
            all_zones = []
            offset = 0
            page_size = 2000

            # Build state filter using STUSAB field (state abbreviation)
            state_filter = ""
            if config.states:
                # HUD data uses STUSAB for state abbreviation
                state_list = ", ".join(f"'{s}'" for s in config.states)
                state_filter = f"STUSAB IN ({state_list})"

            while True:
                params = {
                    "where": state_filter if state_filter else "1=1",
                    "outFields": "*",
                    "returnGeometry": "false",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                response = await self._fetch_arcgis(self.OZ_URL, params)
                features = response.get("features", [])

                if not features:
                    break

                all_zones.extend(features)
                logger.info(
                    f"Fetched {len(features)} OZ records (total: {len(all_zones)})"
                )

                if len(features) < page_size:
                    break

                offset += page_size

            # Transform records
            records = []
            for feature in all_zones:
                transformed = self._transform_oz_record(feature)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    OpportunityZone,
                    records,
                    unique_columns=["tract_geoid"],
                    update_columns=[
                        "state",
                        "county",
                        "tract_name",
                        "is_low_income",
                        "is_contiguous",
                        "designation_date",
                        "source",
                        "collected_at",
                    ],
                )
                logger.info(f"Inserted/updated {inserted} Opportunity Zone records")
                return {"processed": len(all_zones), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect opportunity zones: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_oz_record(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform HUD/CDFI OZ feature to database format."""
        attrs = feature.get("attributes", {})

        # HUD format uses GEOID10, fallback to other common field names
        geoid = attrs.get("GEOID10") or attrs.get("GEOID") or attrs.get("TRACTCE")
        if not geoid:
            return None

        # Get state - HUD provides STUSAB (state abbreviation) directly
        state = attrs.get("STUSAB")
        if not state:
            # Fallback: extract from GEOID (first 2 digits are state FIPS)
            state_fips = str(geoid)[:2]
            state = FIPS_TO_STATE.get(state_fips)

        # Get county name from STATE_NAME or construct from FIPS
        county_name = attrs.get("COUNTY_NAME") or attrs.get("NAMELSAD")
        if not county_name:
            # County FIPS is digits 3-5 of GEOID
            county_fips = str(geoid)[2:5] if len(str(geoid)) >= 5 else None
            county_name = f"County {county_fips}" if county_fips else None

        # Get tract info
        tract = attrs.get("TRACT") or (
            str(geoid)[5:] if len(str(geoid)) >= 11 else None
        )
        tract_name = (
            attrs.get("NAME") or attrs.get("TRACT_NAME") or f"Tract {tract}"
            if tract
            else None
        )

        # HUD designated OZ data - all tracts in this layer are designated OZs
        # Low-income status is implied by designation (all OZs are either LIC or contiguous)
        lic_type = attrs.get("LIC_TYPE") or attrs.get("TYPE") or ""
        is_low_income = (
            "LIC" in lic_type.upper() if lic_type else True
        )  # Default True for designated OZs
        is_contiguous = "CONTIGUOUS" in lic_type.upper() if lic_type else False

        return {
            "tract_geoid": str(geoid),
            "state": state,
            "county": county_name,
            "tract_name": tract_name,
            "is_low_income": is_low_income,
            "is_contiguous": is_contiguous,
            "designation_date": self._parse_date(
                attrs.get("DESIGNATED") or attrs.get("DESIGNATION_DATE")
            ),
            "source": "hud_oz",
            "collected_at": datetime.utcnow(),
        }

    def _parse_date(self, value: Any) -> Optional[datetime]:
        """Parse date value."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            # Unix timestamp in milliseconds
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
