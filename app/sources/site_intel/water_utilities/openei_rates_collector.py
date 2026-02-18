"""
OpenEI Utility Rate Database (URDB) Collector.

Fetches electric utility rate information from OpenEI:
- Utility rate schedules
- Energy rates by customer class
- Demand charges and time-of-use rates

Data source: https://openei.org/wiki/Utility_Rate_Database
No API key required for basic access.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import UtilityRate
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


# Customer class mapping
CUSTOMER_CLASS_MAP = {
    "Residential": "residential",
    "General": "commercial",
    "Commercial": "commercial",
    "Industrial": "industrial",
    "Agricultural": "agricultural",
    "Lighting": "lighting",
}


@register_collector(SiteIntelSource.OPENEI_URDB)
class OpenEIRatesCollector(BaseCollector):
    """
    Collector for OpenEI Utility Rate Database.

    Fetches:
    - Utility rate schedules by state
    - Energy rates and demand charges
    - Time-of-use period definitions
    """

    domain = SiteIntelDomain.WATER_UTILITIES
    source = SiteIntelSource.OPENEI_URDB

    default_timeout = 60.0
    rate_limit_delay = 1.0

    # OpenEI URDB API
    OPENEI_API_URL = "https://api.openei.org/utility_rates"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://api.openei.org"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Execute OpenEI utility rates collection."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting OpenEI utility rate data...")

            # Collect rates
            rates_result = await self._collect_utility_rates(config)
            total_inserted += rates_result.get("inserted", 0)
            total_processed += rates_result.get("processed", 0)
            if rates_result.get("error"):
                errors.append(
                    {"source": "utility_rates", "error": rates_result["error"]}
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
            logger.error(f"OpenEI rates collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_utility_rates(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect utility rates from OpenEI URDB."""
        try:
            client = await self.get_client()
            all_rates = []

            # Determine states to query
            states = config.states if config.states else ["TX", "CA", "OH", "PA", "IL"]

            for state in states:
                await self.apply_rate_limit()

                params = {
                    "version": "7",
                    "format": "json",
                    "detail": "full",
                    "getpage": state,
                    "limit": 100,
                }

                # Add API key if configured
                if self.api_key:
                    params["api_key"] = self.api_key

                try:
                    response = await client.get(self.OPENEI_API_URL, params=params)

                    if response.status_code != 200:
                        logger.warning(
                            f"OpenEI API returned {response.status_code} for {state}"
                        )
                        continue

                    data = response.json()
                    items = data.get("items", [])

                    if items:
                        for item in items:
                            item["_state"] = state  # Track state
                        all_rates.extend(items)
                        logger.info(f"Retrieved {len(items)} utility rates for {state}")

                except Exception as e:
                    logger.warning(f"Failed to fetch rates for {state}: {e}")
                    continue

            if not all_rates:
                return {"processed": 0, "inserted": 0}

            # Transform records
            records = []
            for rate in all_rates:
                transformed = self._transform_rate(rate)
                if transformed:
                    # Filter by customer class if specified
                    if config.options and config.options.get("customer_class"):
                        if (
                            transformed.get("customer_class")
                            != config.options["customer_class"]
                        ):
                            continue
                    records.append(transformed)

            if records:
                inserted, _ = self.bulk_upsert(
                    UtilityRate,
                    records,
                    unique_columns=["rate_schedule_id"],
                    update_columns=[
                        "utility_id",
                        "utility_name",
                        "state",
                        "service_territory",
                        "rate_schedule_name",
                        "customer_class",
                        "sector",
                        "energy_rate_kwh",
                        "demand_charge_kw",
                        "fixed_monthly_charge",
                        "has_time_of_use",
                        "has_demand_charges",
                        "has_net_metering",
                        "effective_date",
                        "description",
                        "source",
                        "source_url",
                        "collected_at",
                    ],
                )
                logger.info(f"Inserted/updated {inserted} utility rates")
                return {"processed": len(all_rates), "inserted": inserted}

            return {"processed": len(all_rates), "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect utility rates: {e}")
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_rate(self, rate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform OpenEI rate data to database format."""
        label = rate.get("label") or rate.get("name")
        utility = rate.get("utility")

        if not label or not utility:
            return None

        # Generate unique ID
        rate_id = rate.get("label") or f"{utility}_{label}".replace(" ", "_")[:100]

        # Map customer class
        sector = rate.get("sector", "")
        customer_class = CUSTOMER_CLASS_MAP.get(
            sector, sector.lower() if sector else "commercial"
        )

        # Extract rates - OpenEI uses complex rate structures
        energy_rate = None
        demand_charge = None
        fixed_charge = None

        # Try to get flat rate first
        if rate.get("flatdemandunit"):
            energy_rate = self._parse_float(rate.get("flatdemandunit"))

        # Look for energyratestructure
        ers = rate.get("energyratestructure", [])
        if ers and len(ers) > 0 and len(ers[0]) > 0:
            first_tier = ers[0][0] if isinstance(ers[0], list) else ers[0]
            if isinstance(first_tier, dict):
                energy_rate = energy_rate or self._parse_float(first_tier.get("rate"))

        # Demand charges
        drs = rate.get("demandratestructure", [])
        if drs and len(drs) > 0 and len(drs[0]) > 0:
            first_tier = drs[0][0] if isinstance(drs[0], list) else drs[0]
            if isinstance(first_tier, dict):
                demand_charge = self._parse_float(first_tier.get("rate"))

        # Fixed charges
        fixed_charge = self._parse_float(
            rate.get("fixedmonthlycharge") or rate.get("minmonthlycharge")
        )

        # Time of use
        has_tou = bool(rate.get("tou", False)) or bool(
            rate.get("energyweekdayschedule")
        )

        # Parse effective date
        effective_date = None
        if rate.get("startdate"):
            try:
                effective_date = datetime.strptime(
                    str(rate["startdate"])[:10], "%Y-%m-%d"
                ).date()
            except (ValueError, TypeError):
                pass

        return {
            "rate_schedule_id": rate_id,
            "utility_id": str(rate.get("eiaid", utility))[:50],
            "utility_name": utility,
            "state": rate.get("_state") or self._extract_state(rate),
            "service_territory": rate.get("service_type"),
            "rate_schedule_name": label,
            "customer_class": customer_class,
            "sector": sector,
            "energy_rate_kwh": energy_rate,
            "demand_charge_kw": demand_charge,
            "fixed_monthly_charge": fixed_charge,
            "has_time_of_use": has_tou,
            "has_demand_charges": demand_charge is not None and demand_charge > 0,
            "has_net_metering": bool(rate.get("usenetmetering")),
            "effective_date": effective_date,
            "description": rate.get("description"),
            "source": "openei",
            "source_url": rate.get("uri"),
            "collected_at": datetime.utcnow(),
        }

    def _extract_state(self, rate: Dict[str, Any]) -> Optional[str]:
        """Extract state from rate data."""
        # Try various fields
        for field in ["state", "address"]:
            value = rate.get(field)
            if value and len(value) == 2 and value.isalpha():
                return value.upper()
        return None

    def _parse_float(self, value: Any) -> Optional[float]:
        """Parse float value."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
