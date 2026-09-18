"""
USDA AMS Truck Rate Collector.

Fetches agricultural refrigerated truck rates from USDA AMS:
- Truck rate reports by origin/destination
- Commodity-specific rates
- Weekly updates

Data sources:
- USDA AMS Truck Rate Report via Socrata API
- USDA Open Ag Transport data

No API key required (Socrata app token optional for higher rate limits).
"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import UsdaTruckRate
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


@register_collector(SiteIntelSource.USDA_AMS)
class UsdaTruckCollector(BaseCollector):
    """
    Collector for USDA AMS truck rate data.

    Fetches agricultural refrigerated truck rates from the USDA Agricultural
    Marketing Service. Covers produce hauling rates from major agricultural
    regions to destination cities.
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.USDA_AMS

    # USDA Socrata API configuration
    default_timeout = 60.0
    rate_limit_delay = 0.5

    # USDA AMS Socrata endpoint for truck rate data
    SOCRATA_DOMAIN = "usda.library.cornell.edu"
    DATASET_ID = "mz2r-8a8b"  # USDA AMS Truck Rate Report

    # Alternative: Direct USDA AMS endpoint
    USDA_AMS_BASE = "https://marsapi.ams.usda.gov/services/v1.2"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)
        self.app_token = api_key  # Socrata app token

    def get_default_base_url(self) -> str:
        return f"https://{self.SOCRATA_DOMAIN}/resource"

    def get_default_headers(self) -> Dict[str, str]:
        headers = {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }
        if self.app_token:
            headers["X-App-Token"] = self.app_token
        return headers

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute USDA truck rate collection.

        Fetches truck rates from USDA AMS truck rate reports.
        """
        try:
            logger.info("Collecting USDA AMS truck rate data...")

            all_rates = []

            # Collect from USDA AMS API
            rates_result = await self._collect_truck_rates(config)
            all_rates.extend(rates_result.get("records", []))

            if not all_rates:
                logger.warning("No truck rate data collected")
                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=0,
                    processed=0,
                    inserted=0,
                )

            # Transform and insert records
            records = []
            for rate in all_rates:
                transformed = self._transform_rate(rate)
                if transformed:
                    records.append(transformed)

            logger.info(f"Transformed {len(records)} truck rate records")

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    UsdaTruckRate,
                    records,
                    unique_columns=[
                        "origin_region",
                        "destination_city",
                        "commodity",
                        "report_date",
                    ],
                    update_columns=[
                        "origin_state",
                        "destination_state",
                        "mileage_band",
                        "rate_per_mile",
                        "rate_per_truckload",
                        "fuel_price",
                        "source",
                        "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(all_rates),
                    processed=len(all_rates),
                    inserted=inserted,
                    sample=records[:3] if records else None,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(all_rates),
                processed=len(all_rates),
                inserted=0,
            )

        except Exception as e:
            logger.error(f"USDA truck rate collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_truck_rates(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect truck rates from USDA AMS.

        Uses the USDA Market News API for truck rate data.
        """
        try:
            client = await self.get_client()
            all_records = []

            # Try USDA Market News API first
            # Get truck rate report slugs
            report_slugs = [
                "2706",  # Truck Rate Report - National Summary
                "2709",  # Truck Rate Report - Fruits and Vegetables
            ]

            for slug in report_slugs:
                await self.apply_rate_limit()

                try:
                    # Fetch the report
                    response = await client.get(
                        f"https://marsapi.ams.usda.gov/services/v1.2/reports/{slug}",
                        params={"format": "json"},
                    )

                    if response.status_code == 200:
                        data = response.json()
                        records = self._parse_mars_report(data, slug)
                        all_records.extend(records)
                        logger.info(
                            f"Fetched {len(records)} records from report {slug}"
                        )
                    else:
                        logger.warning(
                            f"Failed to fetch report {slug}: {response.status_code}"
                        )

                except Exception as e:
                    logger.warning(f"Error fetching report {slug}: {e}")
                    continue

            # Fallback: Try Socrata dataset if available
            if not all_records:
                socrata_records = await self._collect_from_socrata(config)
                all_records.extend(socrata_records)

            # No sample/random fallback: fabricated rows must never be stored (PLAN_082).
            if not all_records:
                raise RuntimeError("No data returned from source; refusing to substitute sample data")

            return {"records": all_records}

        except Exception as e:
            logger.error(f"Failed to collect truck rates: {e}", exc_info=True)
            return {"records": [], "error": str(e)}

    def _parse_mars_report(
        self, data: Dict[str, Any], slug: str
    ) -> List[Dict[str, Any]]:
        """Parse USDA MARS API report data."""
        records = []

        # MARS reports have a specific structure
        results = data.get("results", [])
        if not results:
            results = data.get("data", [])

        for item in results:
            record = {
                "origin_region": item.get("origin")
                or item.get("origin_name")
                or "Unknown",
                "origin_state": item.get("origin_state"),
                "destination_city": item.get("destination")
                or item.get("destination_name")
                or "Unknown",
                "destination_state": item.get("destination_state"),
                "commodity": item.get("commodity")
                or item.get("commodity_name")
                or "General Produce",
                "rate_per_mile": self._parse_rate(
                    item.get("rate_per_mile") or item.get("low_price")
                ),
                "rate_per_truckload": self._parse_rate(
                    item.get("rate") or item.get("avg_price")
                ),
                "report_date": item.get("report_date") or item.get("date"),
                "mileage": item.get("mileage") or item.get("distance"),
            }

            if record["origin_region"] and record["destination_city"]:
                records.append(record)

        return records

    async def _collect_from_socrata(
        self, config: CollectionConfig
    ) -> List[Dict[str, Any]]:
        """Collect from Socrata API as fallback."""
        try:
            client = await self.get_client()

            params = {
                "$limit": 5000,
                "$order": "report_date DESC",
            }

            # Add date filter if specified
            if config.start_date:
                params["$where"] = (
                    f"report_date >= '{config.start_date.strftime('%Y-%m-%d')}'"
                )

            response = await client.get(
                f"/{self.DATASET_ID}.json",
                params=params,
            )

            if response.status_code == 200:
                return response.json()

            return []

        except Exception as e:
            logger.warning(f"Socrata fallback failed: {e}")
            return []

    def _transform_rate(self, rate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw rate data to database format."""
        origin = rate.get("origin_region") or rate.get("origin")
        dest = rate.get("destination_city") or rate.get("destination")

        if not origin or not dest:
            return None

        # Parse report date
        report_date = rate.get("report_date")
        if isinstance(report_date, str):
            try:
                report_date = datetime.strptime(report_date[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                report_date = date.today()
        elif not isinstance(report_date, date):
            report_date = date.today()

        # Determine mileage band if not provided
        mileage_band = rate.get("mileage_band")
        if not mileage_band and rate.get("mileage"):
            mileage = int(rate["mileage"])
            if mileage < 200:
                mileage_band = "local"
            elif mileage < 500:
                mileage_band = "short"
            elif mileage < 1000:
                mileage_band = "medium"
            else:
                mileage_band = "long"

        return {
            "origin_region": origin,
            "origin_state": rate.get("origin_state"),
            "destination_city": dest,
            "destination_state": rate.get("destination_state"),
            "commodity": rate.get("commodity") or "General Produce",
            "mileage_band": mileage_band,
            "rate_per_mile": self._parse_rate(rate.get("rate_per_mile")),
            "rate_per_truckload": self._parse_rate(rate.get("rate_per_truckload")),
            "fuel_price": self._parse_rate(rate.get("fuel_price")),
            "report_date": report_date,
            "source": "usda_ams",
            "collected_at": datetime.utcnow(),
        }

    def _parse_rate(self, value: Any) -> Optional[float]:
        """Parse rate value to float."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                # Remove currency symbols and commas
                cleaned = value.replace("$", "").replace(",", "").strip()
                return float(cleaned)
            except (ValueError, TypeError):
                return None
        return None
