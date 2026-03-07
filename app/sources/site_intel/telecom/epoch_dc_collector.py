"""
Epoch AI Datacenter Locations Collector.

Fetches datacenter facility data from Epoch AI's public dataset.
Contains frontier AI datacenter locations with power capacity, operator,
coordinates, and cost information.

Data source: https://epoch.ai/data/data-centers
CSV: https://epoch.ai/data/data_centers/data_centers.csv
No API key required.
"""

import csv
import io
import logging
import re
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import EpochDatacenter
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

EPOCH_CSV_URL = "https://epoch.ai/data/data_centers/data_centers.csv"

# Two-letter state codes for filtering US facilities from Address field
US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

# Full state names to codes (for Handle-based fallback)
STATE_NAME_TO_CODE = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}

# DMS coordinate pattern: 32°35'25"N or 90°05'35"W
DMS_PATTERN = re.compile(
    r"""(\d+)\s*°\s*(\d+)\s*[''′]\s*(\d+(?:\.\d+)?)\s*[""″]?\s*([NSEW])""",
    re.UNICODE,
)


def parse_dms(dms_str: str) -> Optional[float]:
    """Parse DMS coordinate string like 32°35'25"N to decimal degrees."""
    if not dms_str:
        return None
    match = DMS_PATTERN.search(dms_str.strip())
    if not match:
        return None
    degrees = int(match.group(1))
    minutes = int(match.group(2))
    seconds = float(match.group(3))
    direction = match.group(4)
    decimal = degrees + minutes / 60 + seconds / 3600
    if direction in ("S", "W"):
        decimal = -decimal
    return round(decimal, 7)


def clean_tag(value: str) -> str:
    """Remove Epoch confidence tags like #confident, #speculative, #likely."""
    return re.sub(r"\s*#\w+", "", value).strip()


@register_collector(SiteIntelSource.EPOCH_DC)
class EpochDatacenterCollector(BaseCollector):
    """
    Collector for Epoch AI frontier datacenter location data.

    Downloads the main facilities CSV from epoch.ai. Parses DMS coordinates,
    extracts company/city/state from Address and Handle fields, and filters
    to US-only facilities.
    """

    domain = SiteIntelDomain.TELECOM
    source = SiteIntelSource.EPOCH_DC

    default_timeout = 60.0
    rate_limit_delay = 0.5

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://epoch.ai/data/data_centers"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "text/csv, */*",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect Epoch AI datacenter data."""
        try:
            logger.info("Collecting Epoch AI datacenter data...")

            client = await self.get_client()
            await self.apply_rate_limit()

            response = await client.get(EPOCH_CSV_URL)
            response.raise_for_status()

            csv_text = response.text
            if not csv_text or len(csv_text) < 50:
                return self.create_result(
                    status=CollectionStatus.FAILED,
                    error_message="Empty CSV from Epoch AI",
                )

            reader = csv.DictReader(io.StringIO(csv_text))
            records = []

            for row in reader:
                transformed = self._transform_row(row, config.states)
                if transformed:
                    records.append(transformed)

            logger.info(f"Epoch DC: parsed {len(records)} US datacenter records")

            if records:
                inserted, updated = self.bulk_upsert(
                    EpochDatacenter,
                    records,
                    unique_columns=["epoch_id"],
                    update_columns=[
                        "company", "facility_name", "city", "state",
                        "country", "latitude", "longitude",
                        "power_capacity_mw", "building_area_sqft",
                        "year_opened", "status",
                        "source", "collected_at",
                    ],
                )
                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(records),
                    processed=len(records),
                    inserted=inserted + updated,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=0,
                processed=0,
                inserted=0,
            )

        except Exception as e:
            logger.error(f"Epoch DC collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    def _parse_address(self, address: str, handle: str) -> Dict[str, Optional[str]]:
        """Extract city and state from Address field, with Handle as fallback."""
        state = None
        city = None

        if address:
            # Try to find a 2-letter state code in address
            # Pattern: "City, STATE" or "City, ST ZIP"
            state_match = re.search(
                r",\s*([A-Z]{2})\s*(?:\d{5})?(?:\s|$|,)", address
            )
            if state_match and state_match.group(1) in US_STATE_CODES:
                state = state_match.group(1)

            # Also check for full state name like "Mississippi Madison County"
            if not state:
                addr_lower = address.lower()
                for name, code in STATE_NAME_TO_CODE.items():
                    if name in addr_lower:
                        state = code
                        break

            # Extract city: typically the part before the state
            if state:
                # Try "City, ST" pattern
                city_match = re.search(r"([A-Za-z\s]+),\s*" + state, address)
                if city_match:
                    city = city_match.group(1).strip()
                    # If city looks like a street, try the part after the street
                    if re.match(r"^\d+\s", city):
                        city = None

        # Fallback: parse state from Handle (e.g., "Google Cedar Rapids Iowa")
        if not state and handle:
            handle_lower = handle.lower()
            for name, code in STATE_NAME_TO_CODE.items():
                if handle_lower.endswith(name):
                    state = code
                    break

        return {"city": city, "state": state}

    def _parse_company(self, row: Dict[str, str]) -> Optional[str]:
        """Extract company from Owner field, falling back to Handle."""
        owner = row.get("Owner") or ""
        if owner:
            return clean_tag(owner)[:255] or None

        # Fallback: first word of Handle
        handle = (row.get("Handle") or "").strip()
        if handle:
            first = handle.split()[0]
            # Handle hyphenated like "Anthropic-Amazon"
            return first[:255]
        return None

    def _infer_status(self, row: Dict[str, str]) -> str:
        """Infer status from power capacity."""
        power = self._safe_float(row.get("Current power (MW)"))
        h100 = self._safe_float(row.get("Current H100 equivalents"))
        if power and power > 0:
            return "operational"
        if h100 and h100 > 0:
            return "operational"
        return "under_construction"

    def _transform_row(
        self, row: Dict[str, str], states: Optional[list] = None
    ) -> Optional[Dict[str, Any]]:
        """Transform a facilities CSV row, filtering to US-only."""
        handle = (row.get("Handle") or "").strip()
        title = (row.get("Title") or "").strip()
        address = (row.get("Address") or "").strip()

        if not handle:
            return None

        location = self._parse_address(address, handle)
        state = location.get("state")

        # No state = not a recognized US facility
        if not state:
            return None

        if states and state not in states:
            return None

        epoch_id = handle.replace(" ", "_").lower()[:100]

        return {
            "epoch_id": epoch_id,
            "company": self._parse_company(row),
            "facility_name": (title or handle)[:500],
            "city": (location.get("city") or "")[:100] or None,
            "state": state,
            "country": "US",
            "latitude": parse_dms(row.get("Latitude") or ""),
            "longitude": parse_dms(row.get("Longitude") or ""),
            "power_capacity_mw": self._safe_float(
                row.get("Current power (MW)")
            ),
            "building_area_sqft": None,  # Not in facilities CSV
            "year_opened": None,  # Not in facilities CSV
            "status": self._infer_status(row),
            "source": "epoch_ai",
            "collected_at": datetime.utcnow(),
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None or value == "" or value == "-" or value == "N/A":
            return None
        try:
            return float(str(value).strip().replace(",", ""))
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None or value == "" or value == "-" or value == "N/A":
            return None
        try:
            return int(float(str(value).strip().replace(",", "")))
        except (ValueError, TypeError):
            return None
