"""
FEMA Risk Collector.

Fetches natural hazard risk data from FEMA:
- National Risk Index (NRI) - county-level risk scores
- Flood zone data (NFHL) - when available via API

Data sources:
- NRI: https://hazards.fema.gov/nri/
- NFHL: https://www.fema.gov/flood-maps/national-flood-hazard-layer

No API key required - public data.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import NationalRiskIndex
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
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
}

# Reverse lookup
FIPS_TO_STATE = {v: k for k, v in STATE_FIPS.items()}


@register_collector(SiteIntelSource.FEMA)
class FEMARiskCollector(BaseCollector):
    """
    Collector for FEMA natural hazard risk data.

    Fetches:
    - National Risk Index scores by county
    - Risk ratings for various hazard types
    """

    domain = SiteIntelDomain.RISK
    source = SiteIntelSource.FEMA

    # FEMA API configuration
    default_timeout = 120.0
    rate_limit_delay = 0.5

    # NRI ArcGIS REST endpoint
    NRI_URL = "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/National_Risk_Index_Counties/FeatureServer/0/query"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute FEMA data collection.

        Collects National Risk Index data by county.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            # Collect NRI county risk scores
            logger.info("Collecting FEMA National Risk Index data...")
            nri_result = await self._collect_nri_data(config)
            total_inserted += nri_result.get("inserted", 0)
            total_processed += nri_result.get("processed", 0)
            if nri_result.get("error"):
                errors.append({"source": "nri", "error": nri_result["error"]})

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
            logger.error(f"FEMA collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_nri_data(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect National Risk Index data from FEMA.

        NRI provides county-level composite risk scores.
        """
        try:
            all_counties = []
            offset = 0
            page_size = 1000

            # Build state filter using FIPS codes
            state_filter = ""
            if config.states:
                fips_list = []
                for state in config.states:
                    fips = STATE_FIPS.get(state)
                    if fips:
                        fips_list.append(f"STATEFIPS = '{fips}'")
                if fips_list:
                    state_filter = " OR ".join(fips_list)

            while True:
                params = {
                    "where": state_filter if state_filter else "1=1",
                    "outFields": "*",
                    "returnGeometry": "false",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                response = await self._fetch_arcgis(self.NRI_URL, params)
                features = response.get("features", [])

                if not features:
                    break

                all_counties.extend(features)
                logger.info(
                    f"Fetched {len(features)} NRI county records (total: {len(all_counties)})"
                )

                if len(features) < page_size:
                    break

                offset += page_size

            # Transform records, dedup by county_fips (API may return duplicates)
            seen_fips = {}
            for feature in all_counties:
                transformed = self._transform_nri_record(feature)
                if transformed:
                    fips = transformed["county_fips"]
                    seen_fips[fips] = transformed  # last wins
            records = list(seen_fips.values())

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    NationalRiskIndex,
                    records,
                    unique_columns=["county_fips"],
                    update_columns=[
                        "county_name",
                        "state",
                        "risk_score",
                        "risk_rating",
                        "hazard_scores",
                        "earthquake_score",
                        "flood_score",
                        "tornado_score",
                        "hurricane_score",
                        "wildfire_score",
                        "social_vulnerability",
                        "community_resilience",
                        "expected_annual_loss",
                        "collected_at",
                    ],
                )
                return {"processed": len(all_counties), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect NRI data: {e}", exc_info=True)
            try:
                self.db.rollback()
            except Exception:
                pass
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_nri_record(
        self, feature: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Transform FEMA NRI feature to database format."""
        attrs = feature.get("attributes", {})

        county_fips = attrs.get("STCOFIPS") or attrs.get("COUNTYFIPS")
        if not county_fips:
            return None

        state_fips = str(county_fips)[:2]
        state = FIPS_TO_STATE.get(state_fips)

        county_name = attrs.get("COUNTY") or attrs.get("COUNTYNAME")

        # Get overall risk score (composite)
        risk_score = self._safe_float(attrs.get("RISK_SCORE"))
        risk_rating = attrs.get("RISK_RATNG") or attrs.get("RISK_RATING")

        # Extract individual hazard scores
        hazard_scores = self._extract_hazard_scores(attrs)

        return {
            "county_fips": str(county_fips),
            "county_name": county_name,
            "state": state,
            "risk_score": risk_score,
            "risk_rating": risk_rating,
            "hazard_scores": hazard_scores,
            "earthquake_score": self._safe_float(attrs.get("ERQK_RISKS")),
            "flood_score": self._safe_float(attrs.get("RFLD_RISKS")),  # Riverine flood
            "tornado_score": self._safe_float(attrs.get("TRND_RISKS")),
            "hurricane_score": self._safe_float(attrs.get("HRCN_RISKS")),
            "wildfire_score": self._safe_float(attrs.get("WFIR_RISKS")),
            "social_vulnerability": self._safe_float(attrs.get("SOVI_SCORE")),
            "community_resilience": self._safe_float(attrs.get("RESL_SCORE")),
            "expected_annual_loss": self._safe_float(attrs.get("EAL_SCORE")),
            "source": "fema_nri",
            "collected_at": datetime.utcnow(),
        }

    def _extract_hazard_scores(self, attrs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract individual hazard scores from NRI attributes."""
        hazards = {}

        # Map NRI field prefixes to hazard names
        hazard_map = {
            "AVLN": "avalanche",
            "CFLD": "coastal_flood",
            "CWAV": "cold_wave",
            "DRGT": "drought",
            "ERQK": "earthquake",
            "HAIL": "hail",
            "HWAV": "heat_wave",
            "HRCN": "hurricane",
            "ISTM": "ice_storm",
            "LNDS": "landslide",
            "LTNG": "lightning",
            "RFLD": "riverine_flood",
            "SWND": "strong_wind",
            "TRND": "tornado",
            "TSUN": "tsunami",
            "VLCN": "volcanic",
            "WFIR": "wildfire",
            "WNTW": "winter_weather",
        }

        for prefix, name in hazard_map.items():
            # Get risk score for this hazard
            score_key = f"{prefix}_RISKS"  # e.g., ERQK_RISKS
            score = self._safe_float(attrs.get(score_key))
            if score is not None:
                hazards[name] = {
                    "score": score,
                    "rating": attrs.get(f"{prefix}_RISKR"),  # Rating
                }

        return hazards

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

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "" or value == -999:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
