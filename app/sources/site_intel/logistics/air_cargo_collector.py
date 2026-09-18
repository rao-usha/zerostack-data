"""
Air Cargo Statistics Collector.

Fetches air cargo data from:
- BTS T-100 Domestic/International Segment Data
- FAA cargo statistics

Data sources:
- Bureau of Transportation Statistics
- FAA Air Cargo Reports

No API key required - public data.
"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import AirCargoStats
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


@register_collector(SiteIntelSource.BTS_CARGO)
class AirCargoCollector(BaseCollector):
    """
    Collector for air cargo statistics from BTS T-100.

    Fetches:
    - Monthly freight tons by airport
    - Domestic vs international cargo
    - Carrier-level breakdown
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.BTS_CARGO

    # BTS API configuration
    default_timeout = 120.0
    rate_limit_delay = 0.5

    # BTS T-100 endpoints
    BTS_BASE = "https://www.transtats.bts.gov/api/GetData"

    # Major US cargo airports (by FAA code)
    MAJOR_CARGO_AIRPORTS = {
        "MEM": {"name": "Memphis International", "city": "Memphis", "state": "TN"},
        "ANC": {
            "name": "Ted Stevens Anchorage International",
            "city": "Anchorage",
            "state": "AK",
        },
        "SDF": {
            "name": "Louisville Muhammad Ali International",
            "city": "Louisville",
            "state": "KY",
        },
        "MIA": {"name": "Miami International", "city": "Miami", "state": "FL"},
        "LAX": {
            "name": "Los Angeles International",
            "city": "Los Angeles",
            "state": "CA",
        },
        "JFK": {
            "name": "John F. Kennedy International",
            "city": "New York",
            "state": "NY",
        },
        "ORD": {"name": "O'Hare International", "city": "Chicago", "state": "IL"},
        "IND": {
            "name": "Indianapolis International",
            "city": "Indianapolis",
            "state": "IN",
        },
        "CVG": {
            "name": "Cincinnati/Northern Kentucky International",
            "city": "Hebron",
            "state": "KY",
        },
        "EWR": {
            "name": "Newark Liberty International",
            "city": "Newark",
            "state": "NJ",
        },
        "DFW": {
            "name": "Dallas/Fort Worth International",
            "city": "Dallas",
            "state": "TX",
        },
        "ATL": {
            "name": "Hartsfield-Jackson Atlanta International",
            "city": "Atlanta",
            "state": "GA",
        },
        "ONT": {"name": "Ontario International", "city": "Ontario", "state": "CA"},
        "OAK": {"name": "Oakland International", "city": "Oakland", "state": "CA"},
        "SFO": {
            "name": "San Francisco International",
            "city": "San Francisco",
            "state": "CA",
        },
        "SEA": {
            "name": "Seattle-Tacoma International",
            "city": "Seattle",
            "state": "WA",
        },
        "PHX": {
            "name": "Phoenix Sky Harbor International",
            "city": "Phoenix",
            "state": "AZ",
        },
        "IAH": {
            "name": "George Bush Intercontinental",
            "city": "Houston",
            "state": "TX",
        },
        "BOS": {"name": "Boston Logan International", "city": "Boston", "state": "MA"},
        "PHL": {
            "name": "Philadelphia International",
            "city": "Philadelphia",
            "state": "PA",
        },
        "RFD": {
            "name": "Chicago Rockford International",
            "city": "Rockford",
            "state": "IL",
        },
        "HSV": {
            "name": "Huntsville International",
            "city": "Huntsville",
            "state": "AL",
        },
        "AFW": {
            "name": "Fort Worth Alliance Airport",
            "city": "Fort Worth",
            "state": "TX",
        },
    }

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return self.BTS_BASE

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute air cargo data collection.

        Collects monthly cargo statistics for major US cargo airports.
        """
        try:
            logger.info("Collecting BTS T-100 air cargo data...")

            all_cargo = []

            # Collect from BTS T-100
            cargo_result = await self._collect_air_cargo(config)
            all_cargo.extend(cargo_result.get("records", []))

            # No sample/random fallback: fabricated rows must never be stored (PLAN_082).
            if not all_cargo:
                raise RuntimeError("No data returned from source; refusing to substitute sample data")

            # Transform and insert records
            records = []
            for record in all_cargo:
                transformed = self._transform_cargo(record)
                if transformed:
                    records.append(transformed)

            logger.info(f"Transformed {len(records)} air cargo records")

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    AirCargoStats,
                    records,
                    unique_columns=["airport_code", "period_year", "period_month"],
                    update_columns=[
                        "airport_name",
                        "freight_tons_enplaned",
                        "freight_tons_deplaned",
                        "freight_tons_total",
                        "freight_domestic",
                        "freight_international",
                        "mail_tons",
                        "carrier_breakdown",
                        "cargo_aircraft_departures",
                        "cargo_aircraft_arrivals",
                        "source",
                        "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(all_cargo),
                    processed=len(all_cargo),
                    inserted=inserted,
                    sample=records[:3] if records else None,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(all_cargo),
                processed=len(all_cargo),
                inserted=0,
            )

        except Exception as e:
            logger.error(f"Air cargo collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_air_cargo(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect air cargo data from BTS T-100.
        """
        try:
            client = await self.get_client()
            all_records = []

            await self.apply_rate_limit()

            try:
                # BTS T-100 data API
                # Note: BTS has specific API structure for T-100 data
                params = {
                    "UserTableName": "T_100_Segment_All_Carrier",
                    "Fields": "ORIGIN,DEST,UNIQUE_CARRIER,FREIGHT,MAIL,MONTH,YEAR",
                    "Filter": f"YEAR GE {config.year or date.today().year - 1}",
                }

                response = await client.get(self.BTS_BASE, params=params)

                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        all_records.extend(data)
                    elif isinstance(data, dict):
                        all_records.extend(data.get("data", []))

                    logger.info(f"Fetched {len(all_records)} records from BTS T-100")

            except Exception as e:
                logger.warning(f"Could not fetch from BTS T-100: {e}")

            return {"records": all_records}

        except Exception as e:
            logger.error(f"Failed to collect air cargo: {e}", exc_info=True)
            return {"records": [], "error": str(e)}

    def _transform_cargo(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw cargo data to database format."""
        airport_code = (
            record.get("airport_code") or record.get("ORIGIN") or record.get("origin")
        )
        if not airport_code:
            return None

        period_year = self._safe_int(record.get("period_year") or record.get("YEAR"))
        period_month = self._safe_int(record.get("period_month") or record.get("MONTH"))

        if not period_year or not period_month:
            return None

        airport_info = self.MAJOR_CARGO_AIRPORTS.get(airport_code, {})

        return {
            "airport_code": airport_code,
            "airport_name": record.get("airport_name") or airport_info.get("name"),
            "period_year": period_year,
            "period_month": period_month,
            "freight_tons_enplaned": self._safe_float(
                record.get("freight_tons_enplaned")
            ),
            "freight_tons_deplaned": self._safe_float(
                record.get("freight_tons_deplaned")
            ),
            "freight_tons_total": self._safe_float(record.get("freight_tons_total")),
            "freight_domestic": self._safe_float(record.get("freight_domestic")),
            "freight_international": self._safe_float(
                record.get("freight_international")
            ),
            "mail_tons": self._safe_float(
                record.get("mail_tons") or record.get("MAIL")
            ),
            "carrier_breakdown": record.get("carrier_breakdown"),
            "cargo_aircraft_departures": self._safe_int(
                record.get("cargo_aircraft_departures")
            ),
            "cargo_aircraft_arrivals": self._safe_int(
                record.get("cargo_aircraft_arrivals")
            ),
            "source": "bts_t100",
            "collected_at": datetime.utcnow(),
        }

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
