"""
FCC Broadband Availability Collector.

Fetches broadband deployment data from the FCC Broadband Map API
and stores in the broadband_availability table.

API: https://broadbandmap.fcc.gov/api/
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import BroadbandAvailability
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)

# All 50 states + DC + territories
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
    "WY": "56", "PR": "72", "GU": "66", "VI": "78", "AS": "60",
}


@register_collector(SiteIntelSource.FCC)
class FCCBroadbandCollector(BaseCollector):
    """
    Collector for FCC broadband availability data.

    Uses the FCC Broadband Map API to collect fixed broadband
    deployment data by state, including provider, technology,
    and speed information.
    """

    domain = SiteIntelDomain.TELECOM
    source = SiteIntelSource.FCC
    default_timeout = 120.0
    rate_limit_delay = 1.0

    SUMMARY_URL = "https://broadbandmap.fcc.gov/api/public/map/summary/fixed"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://broadbandmap.fcc.gov/api"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect FCC broadband data for configured states."""
        self.create_job(config)
        self.start_job()

        all_records = []
        errors = 0

        states = config.states if config.states else list(STATE_FIPS.keys())[:52]

        for i, state in enumerate(states):
            fips = STATE_FIPS.get(state)
            if not fips:
                continue

            self.update_progress(i, len(states), f"Collecting broadband for {state}")

            try:
                records = await self._collect_state_broadband(state, fips, config)
                all_records.extend(records)
            except Exception as e:
                logger.warning(f"Failed to collect broadband for {state}: {e}")
                errors += 1

        # Insert records
        inserted = 0
        if all_records:
            inserted, _ = self.bulk_upsert(
                BroadbandAvailability,
                all_records,
                unique_columns=["block_geoid", "provider_name", "technology"],
                update_columns=[
                    "state", "county", "latitude", "longitude",
                    "max_download_mbps", "max_upload_mbps",
                    "is_business_service", "collected_at",
                ],
            )

        result = self.create_result(
            status=CollectionStatus.SUCCESS if inserted > 0 else CollectionStatus.PARTIAL,
            total=len(all_records),
            processed=len(all_records),
            inserted=inserted,
        )
        self.complete_job(result)
        return result

    async def _collect_state_broadband(
        self, state: str, fips: str, config: CollectionConfig
    ) -> List[Dict[str, Any]]:
        """Collect broadband data for a single state."""
        records = []

        url = f"{self.SUMMARY_URL}/state/{fips}"
        params = {
            "speed_type": "download",
            "tech_code": "0",
        }

        try:
            data = await self.fetch_json(url, params=params)
            if not data:
                return records

            providers = data if isinstance(data, list) else data.get("data", [])
            if isinstance(providers, dict):
                providers = [providers]

            for provider in providers:
                record = {
                    "state": state,
                    "block_geoid": provider.get("block_geoid", f"{fips}000000000"),
                    "county": provider.get("county_name", ""),
                    "latitude": provider.get("latitude"),
                    "longitude": provider.get("longitude"),
                    "provider_name": provider.get("provider_name", provider.get("holding_company_name", "")),
                    "technology": self._map_tech_code(provider.get("tech_code")),
                    "max_download_mbps": provider.get("max_download_speed"),
                    "max_upload_mbps": provider.get("max_upload_speed"),
                    "is_business_service": provider.get("business_residential_code") == "B",
                    "source": "fcc",
                    "collected_at": datetime.utcnow(),
                }
                if record["provider_name"]:
                    records.append(record)

        except Exception as e:
            logger.warning(f"FCC API error for state {state}: {e}")

        return records

    @staticmethod
    def _map_tech_code(code) -> str:
        """Map FCC technology code to human-readable name."""
        tech_map = {
            "10": "dsl",
            "40": "cable",
            "50": "fiber",
            "60": "satellite",
            "70": "fixed_wireless",
            "71": "licensed_fixed_wireless",
            "72": "unlicensed_fixed_wireless",
        }
        return tech_map.get(str(code), str(code) if code else "unknown")
