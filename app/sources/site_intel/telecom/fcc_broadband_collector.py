"""
FCC Broadband Availability Collector.

Fetches broadband deployment data from the FCC Open Data Socrata API
(Form 477) and stores aggregated provider-level records per state in
the broadband_availability table.

API: https://opendata.fcc.gov/resource/hicn-aujz.json
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import BroadbandAvailability
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

# All 50 states + DC + territories
STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
    "WY", "PR",
]

# State FIPS codes for generating block_geoid
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
    "WY": "56", "PR": "72",
}


@register_collector(SiteIntelSource.FCC)
class FCCBroadbandCollector(BaseCollector):
    """
    Collector for FCC broadband availability data.

    Uses the FCC Open Data Socrata API (Form 477 dataset) to collect
    broadband deployment data aggregated by state + provider + technology,
    including max download/upload speeds and census block coverage counts.
    """

    domain = SiteIntelDomain.TELECOM
    source = SiteIntelSource.FCC
    default_timeout = 120.0
    rate_limit_delay = 1.0

    # FCC Form 477 broadband deployment on Socrata (relative to base_url)
    SOCRATA_ENDPOINT = "/resource/hicn-aujz.json"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://opendata.fcc.gov"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect FCC broadband data for configured states."""
        all_records = []
        errors = 0

        states = config.states if config.states else STATES

        for i, state in enumerate(states):
            self.update_progress(i, len(states), f"Collecting broadband for {state}")

            try:
                records = await self._collect_state_broadband(state)
                all_records.extend(records)
                logger.info(
                    f"FCC broadband {state}: {len(records)} provider-tech combos"
                )
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
                    "state",
                    "max_download_mbps",
                    "max_upload_mbps",
                    "is_business_service",
                    "collected_at",
                ],
            )

        logger.info(
            f"FCC broadband collection complete: {len(all_records)} records, "
            f"{inserted} inserted/updated, {errors} state errors"
        )

        return self.create_result(
            status=CollectionStatus.SUCCESS
            if inserted > 0
            else CollectionStatus.PARTIAL,
            total=len(all_records),
            processed=len(all_records),
            inserted=inserted,
        )

    async def _collect_state_broadband(self, state: str) -> List[Dict[str, Any]]:
        """
        Collect broadband data for a single state using Socrata aggregation.

        Queries the FCC Form 477 dataset grouped by provider + technology,
        returning max speeds and census block coverage counts per provider.
        """
        records = []
        fips = STATE_FIPS.get(state, "00")
        offset = 0
        page_size = 1000

        while True:
            params = {
                "$select": (
                    "stateabbr,providername,techcode,"
                    "max(maxaddown) as max_download,"
                    "max(maxadup) as max_upload,"
                    "count(*) as block_count"
                ),
                "$where": f"stateabbr='{state}'",
                "$group": "stateabbr,providername,techcode",
                "$limit": str(page_size),
                "$offset": str(offset),
                "$order": "providername,techcode",
            }

            try:
                data = await self.fetch_json(self.SOCRATA_ENDPOINT, params=params)
                if not data:
                    break

                for row in data:
                    provider = row.get("providername", "")
                    if not provider:
                        continue

                    tech_code = row.get("techcode", "")
                    record = {
                        "state": state,
                        "block_geoid": f"{fips}000000000",
                        "provider_name": provider,
                        "technology": self._map_tech_code(tech_code),
                        "max_download_mbps": self._safe_int(
                            row.get("max_download")
                        ),
                        "max_upload_mbps": self._safe_int(
                            row.get("max_upload")
                        ),
                        "is_business_service": False,
                        "source": "fcc",
                        "collected_at": datetime.utcnow(),
                    }
                    records.append(record)

                if len(data) < page_size:
                    break
                offset += page_size

            except Exception as e:
                logger.warning(f"FCC Socrata API error for {state}: {e}")
                break

        return records

    @staticmethod
    def _safe_int(value) -> Optional[int]:
        """Safely convert a value to int."""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _map_tech_code(code) -> str:
        """Map FCC technology code to human-readable name."""
        tech_map = {
            "10": "adsl",
            "11": "adsl2",
            "12": "vdsl",
            "20": "sdsl",
            "30": "other_copper",
            "40": "cable_docsis3",
            "41": "cable_docsis31",
            "42": "cable_other",
            "43": "cable_docsis3_1",
            "50": "fiber",
            "60": "satellite",
            "70": "fixed_wireless",
            "71": "licensed_fixed_wireless",
            "72": "unlicensed_fixed_wireless",
            "90": "electric_power_line",
            "0": "all_technologies",
        }
        return tech_map.get(str(code), str(code) if code else "unknown")
