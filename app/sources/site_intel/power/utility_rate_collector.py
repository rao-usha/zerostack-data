"""
Utility Rate Collector — Proprietary datacenter power cost dataset.

Combines two data sources into a unified utility rate database:
1. EIA API v2 — Utility-level average rates by sector (all US utilities)
2. OpenEI URDB — Detailed tariff structures with demand charges, TOU rates

Why this is proprietary:
- Nobody else combines utility-level EIA rates with OpenEI tariff details
- Demand charges ($/kW) are typically 30-50% of a DC's electricity bill
  but aren't in any standard dataset
- Time-of-use rate structures affect DC power procurement strategy
- Service territory mapping enables county-level rate estimation

No API key required for EIA (we have one) or OpenEI (DEMO_KEY works
for ratesforutility queries at 30 req/hr).
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Set

from sqlalchemy import text
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

# All 50 states + DC
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

# Customer class mapping from OpenEI sectors
CUSTOMER_CLASS_MAP = {
    "Residential": "residential",
    "General": "commercial",
    "Commercial": "commercial",
    "Industrial": "industrial",
    "Agricultural": "agricultural",
    "Lighting": "lighting",
}

# EIA retail sales API
EIA_RETAIL_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"

# OpenEI URDB API
OPENEI_API_URL = "https://api.openei.org/utility_rates"


@register_collector(SiteIntelSource.OPENEI_URDB)
class UtilityRateCollector(BaseCollector):
    """
    Comprehensive utility rate collector for datacenter power cost analysis.

    Phase 1: EIA retail sales — pulls utility-level average rates for
             commercial and industrial sectors across all states.
    Phase 2: OpenEI URDB — enriches with detailed tariff structures
             (demand charges, TOU, tiers) for major DC-market utilities.
    """

    domain = SiteIntelDomain.POWER
    source = SiteIntelSource.OPENEI_URDB

    default_timeout = 60.0
    rate_limit_delay = 1.0  # NREL key = 1000 req/hr; DEMO_KEY = 30 req/hr

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)
        self._eia_key = None
        self._openei_key = None

    def get_default_base_url(self) -> str:
        return "https://api.openei.org"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    def _load_api_keys(self):
        """Load API keys from settings."""
        from app.core.config import get_settings
        settings = get_settings()
        self._eia_key = getattr(settings, "eia_api_key", None)
        self._openei_key = self.api_key or getattr(settings, "nrel_api_key", None) or getattr(settings, "openei_api_key", None) or "DEMO_KEY"

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Execute full utility rate collection pipeline."""
        self._load_api_keys()

        total_inserted = 0
        total_processed = 0
        errors = []

        states = config.states if config.states else ALL_STATES

        try:
            # Phase 1: EIA utility-level rates (fast, structured)
            logger.info(f"Phase 1: Collecting EIA utility rates for {len(states)} states...")
            eia_result = await self._collect_eia_rates(states)
            total_inserted += eia_result.get("inserted", 0)
            total_processed += eia_result.get("processed", 0)
            if eia_result.get("error"):
                errors.append({"source": "eia_rates", "error": eia_result["error"]})
            logger.info(f"Phase 1 complete: {eia_result.get('inserted', 0)} EIA rates")

            # Phase 2: OpenEI detailed tariffs for major utilities
            logger.info("Phase 2: Collecting OpenEI detailed tariff data...")
            openei_result = await self._collect_openei_rates(states)
            total_inserted += openei_result.get("inserted", 0)
            total_processed += openei_result.get("processed", 0)
            if openei_result.get("error"):
                errors.append({"source": "openei_tariffs", "error": openei_result["error"]})
            logger.info(f"Phase 2 complete: {openei_result.get('inserted', 0)} OpenEI tariffs")

            status = CollectionStatus.SUCCESS
            if errors and total_inserted > 0:
                status = CollectionStatus.PARTIAL
            elif errors and total_inserted == 0:
                status = CollectionStatus.FAILED

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"Utility rate collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    # =========================================================================
    # Phase 1: EIA Utility-Level Rates
    # =========================================================================

    async def _collect_eia_rates(self, states: List[str]) -> Dict[str, Any]:
        """
        Collect utility-level electricity rates from EIA retail sales API.

        Returns average $/kWh for commercial and industrial sectors
        by utility and state. This gives us ~3,000+ utility-level rates.
        """
        if not self._eia_key:
            return {"processed": 0, "inserted": 0, "error": "No EIA API key configured"}

        try:
            client = await self.get_client()
            all_records = []

            # EIA API supports facets for filtering. We want:
            # - sectorid: COM (commercial) and IND (industrial)
            # - Most recent year available
            # We query per state to stay within API limits
            for state in states:
                try:
                    params = {
                        "api_key": self._eia_key,
                        "frequency": "annual",
                        "data[0]": "price",
                        "data[1]": "customers",
                        "data[2]": "revenue",
                        "data[3]": "sales",
                        "facets[sectorid][]": ["COM", "IND"],
                        "facets[stateid][]": state,
                        "sort[0][column]": "period",
                        "sort[0][direction]": "desc",
                        "length": 100,
                    }

                    response = await client.get(EIA_RETAIL_URL, params=params)

                    if response.status_code != 200:
                        logger.warning(f"EIA API {response.status_code} for {state}")
                        continue

                    data = response.json()
                    rows = data.get("response", {}).get("data", [])

                    for row in rows:
                        record = self._transform_eia_rate(row, state)
                        if record:
                            all_records.append(record)

                except Exception as e:
                    logger.warning(f"EIA rate fetch failed for {state}: {e}")
                    continue

            if not all_records:
                return {"processed": 0, "inserted": 0}

            # Upsert to utility_rate table
            inserted, updated = self.bulk_upsert(
                UtilityRate,
                all_records,
                unique_columns=["rate_schedule_id"],
                update_columns=[
                    "utility_id", "utility_name", "state",
                    "rate_schedule_name", "customer_class", "sector",
                    "energy_rate_kwh",
                    "source", "source_url", "collected_at",
                ],
            )

            return {"processed": len(all_records), "inserted": inserted + updated}

        except Exception as e:
            logger.error(f"EIA rate collection failed: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_eia_rate(self, row: Dict[str, Any], state: str) -> Optional[Dict[str, Any]]:
        """Transform an EIA retail sales row into a UtilityRate record."""
        period = row.get("period")
        sector_id = row.get("sectorid", "")
        state_id = row.get("stateid", state)
        price = row.get("price")

        if not price or not period:
            return None

        # Map EIA sector to customer class
        sector_map = {"COM": "commercial", "IND": "industrial"}
        customer_class = sector_map.get(sector_id)
        if not customer_class:
            return None

        # EIA price is in cents/kWh, convert to $/kWh
        try:
            energy_rate = round(float(price) / 100.0, 6)
        except (ValueError, TypeError):
            return None

        # Build unique rate schedule ID
        rate_id = f"eia_{state_id}_{sector_id}_{period}"

        sector_name = row.get("sectorName", sector_id)
        state_name = row.get("stateDescription", state_id)

        return {
            "rate_schedule_id": rate_id[:100],
            "utility_id": f"eia_state_{state_id}",
            "utility_name": f"{state_name} Average ({sector_name})",
            "state": state_id,
            "service_territory": None,
            "rate_schedule_name": f"{state_name} {sector_name} Average Rate {period}",
            "customer_class": customer_class,
            "sector": sector_name,
            "energy_rate_kwh": energy_rate,
            "demand_charge_kw": None,
            "fixed_monthly_charge": None,
            "minimum_charge": None,
            "energy_tiers": None,
            "demand_tiers": None,
            "has_time_of_use": False,
            "tou_periods": None,
            "has_demand_charges": False,
            "has_net_metering": False,
            "power_factor_adjustment": False,
            "min_power_factor": None,
            "effective_date": datetime(int(period), 1, 1).date() if period.isdigit() else None,
            "end_date": None,
            "approved_date": None,
            "description": f"EIA state average {customer_class} rate for {state_name}, {period}",
            "source": "eia",
            "source_url": f"https://www.eia.gov/electricity/data/state/",
            "collected_at": datetime.utcnow(),
        }

    # =========================================================================
    # Phase 2: OpenEI Detailed Tariffs
    # =========================================================================

    async def _collect_openei_rates(self, states: List[str]) -> Dict[str, Any]:
        """
        Collect detailed tariff structures from OpenEI URDB.

        Strategy: Query by EIA utility ID for the largest utilities in each
        target state. This avoids the 500-item cap on unfiltered queries
        and gets us the most impactful rates first.
        """
        try:
            # Get top utilities per state from our EIA electricity_price data
            utility_ids = await self._get_target_utility_ids(states)

            if not utility_ids:
                # Fallback: pull the general feed
                logger.info("No utility IDs found, using general OpenEI query")
                return await self._collect_openei_general()

            logger.info(f"Querying OpenEI for {len(utility_ids)} utilities...")

            client = await self.get_client()
            all_records = []
            errors_count = 0

            # Process utilities in batches to respect rate limits
            for i, (eia_id, utility_info) in enumerate(utility_ids.items()):
                try:
                    await self.apply_rate_limit()

                    params = {
                        "version": "7",
                        "format": "json",
                        "api_key": self._openei_key,
                        "eia": eia_id,
                        "detail": "full",
                    }

                    response = await client.get(OPENEI_API_URL, params=params)

                    if response.status_code == 429:
                        logger.warning(f"OpenEI rate limited at utility {i+1}/{len(utility_ids)}")
                        # Save what we have so far
                        break

                    if response.status_code != 200:
                        logger.warning(f"OpenEI {response.status_code} for utility {eia_id}")
                        errors_count += 1
                        continue

                    data = response.json()

                    if "error" in data:
                        logger.warning(f"OpenEI error for {eia_id}: {data['error']}")
                        if data["error"].get("code") == "OVER_RATE_LIMIT":
                            break
                        errors_count += 1
                        continue

                    items = data.get("items", [])
                    state = utility_info.get("state")

                    for item in items:
                        record = self._transform_openei_rate(item, state)
                        if record:
                            all_records.append(record)

                    if items:
                        logger.info(
                            f"OpenEI [{i+1}/{len(utility_ids)}] "
                            f"{utility_info.get('name','?')}: {len(items)} tariffs"
                        )

                except Exception as e:
                    logger.warning(f"OpenEI failed for utility {eia_id}: {e}")
                    errors_count += 1
                    continue

            if not all_records:
                return {
                    "processed": 0, "inserted": 0,
                    "error": f"No OpenEI records ({errors_count} errors)" if errors_count else None,
                }

            # Upsert
            inserted, updated = self.bulk_upsert(
                UtilityRate,
                all_records,
                unique_columns=["rate_schedule_id"],
                update_columns=[
                    "utility_id", "utility_name", "state",
                    "service_territory", "rate_schedule_name",
                    "customer_class", "sector",
                    "energy_rate_kwh", "demand_charge_kw",
                    "fixed_monthly_charge",
                    "energy_tiers", "demand_tiers",
                    "has_time_of_use", "has_demand_charges",
                    "has_net_metering", "effective_date",
                    "description", "source", "source_url",
                    "collected_at",
                ],
            )

            return {"processed": len(all_records), "inserted": inserted + updated}

        except Exception as e:
            logger.error(f"OpenEI collection failed: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    async def _get_target_utility_ids(self, states: List[str]) -> Dict[str, Dict]:
        """
        Get EIA utility IDs for the largest utilities in target states.

        Uses our existing electricity_price data to find utilities
        with the most customers (= most market coverage).
        """
        utility_ids = {}

        try:
            # Check if we have utility-level EIA data
            result = self.db.execute(text("""
                SELECT DISTINCT geography_id, geography_name,
                       MAX(customer_count) as max_customers
                FROM electricity_price
                WHERE geography_type = 'utility'
                  AND sector IN ('commercial', 'industrial')
                  AND customer_count > 0
                GROUP BY geography_id, geography_name
                ORDER BY max_customers DESC
                LIMIT 200
            """))
            rows = result.fetchall()

            if rows:
                for row in rows:
                    eia_id = str(row[0])
                    utility_ids[eia_id] = {
                        "name": row[1],
                        "customers": row[2],
                        "state": None,  # Will be filled by OpenEI
                    }
                return utility_ids
        except Exception as e:
            logger.debug(f"No utility-level EIA data: {e}")

        # Fallback: use well-known large utilities in DC markets
        # These are the top utilities serving major datacenter hubs
        dc_market_utilities = {
            # Virginia (Ashburn = largest DC market)
            "4922": {"name": "Dominion Energy Virginia", "state": "VA"},
            # Texas (Dallas, San Antonio, Houston)
            "44372": {"name": "Oncor Electric Delivery", "state": "TX"},
            "4110": {"name": "CenterPoint Energy Houston", "state": "TX"},
            "859": {"name": "Austin Energy", "state": "TX"},
            # Oregon/Washington (Portland, Seattle)
            "14354": {"name": "Portland General Electric", "state": "OR"},
            "15500": {"name": "Puget Sound Energy", "state": "WA"},
            # Illinois (Chicago)
            "4110": {"name": "ComEd", "state": "IL"},
            # Georgia (Atlanta)
            "7140": {"name": "Georgia Power", "state": "GA"},
            # Arizona (Phoenix)
            "803": {"name": "Arizona Public Service", "state": "AZ"},
            # Ohio (Columbus)
            "776": {"name": "AEP Ohio", "state": "OH"},
            # California (Silicon Valley, LA)
            "14328": {"name": "Pacific Gas & Electric", "state": "CA"},
            "17609": {"name": "Southern California Edison", "state": "CA"},
            "17534": {"name": "San Diego Gas & Electric", "state": "CA"},
            # New Jersey (data center corridor)
            "15477": {"name": "PSE&G", "state": "NJ"},
            # New York
            "4226": {"name": "Con Edison", "state": "NY"},
            # North Carolina (Charlotte, RTP)
            "5416": {"name": "Duke Energy Carolinas", "state": "NC"},
            # Nevada (Las Vegas = Switch)
            "13407": {"name": "NV Energy", "state": "NV"},
            # Iowa (Meta, Google, Microsoft DCs)
            "12345": {"name": "MidAmerican Energy", "state": "IA"},
            # Colorado (Denver)
            "15466": {"name": "Public Service Co of Colorado", "state": "CO"},
            # Florida
            "6452": {"name": "Florida Power & Light", "state": "FL"},
            # Indiana
            "9324": {"name": "Indianapolis Power & Light", "state": "IN"},
            # Minnesota (Minneapolis)
            "13781": {"name": "Northern States Power", "state": "MN"},
            # Utah (Salt Lake)
            "14354": {"name": "PacifiCorp", "state": "UT"},
            # Tennessee (Nashville)
            "18642": {"name": "Tennessee Valley Authority", "state": "TN"},
            # Maryland
            "924": {"name": "Baltimore Gas & Electric", "state": "MD"},
            # Massachusetts
            "13433": {"name": "National Grid", "state": "MA"},
            # Missouri (Kansas City)
            "12685": {"name": "Kansas City Power & Light", "state": "MO"},
            # Kansas
            "20856": {"name": "Westar Energy", "state": "KS"},
            # Kentucky
            "11249": {"name": "Louisville Gas & Electric", "state": "KY"},
            # Pennsylvania
            "14711": {"name": "PECO Energy", "state": "PA"},
            # South Carolina
            "17543": {"name": "SC Electric & Gas", "state": "SC"},
        }

        # Filter to requested states
        for eia_id, info in dc_market_utilities.items():
            if info["state"] in states:
                utility_ids[eia_id] = info

        return utility_ids

    async def _collect_openei_general(self) -> Dict[str, Any]:
        """Fallback: collect rates from OpenEI general query."""
        try:
            client = await self.get_client()
            await self.apply_rate_limit()

            params = {
                "version": "7",
                "format": "json",
                "api_key": self._openei_key,
                "limit": 500,
                "detail": "full",
            }

            response = await client.get(OPENEI_API_URL, params=params)
            if response.status_code != 200:
                return {"processed": 0, "inserted": 0, "error": f"OpenEI HTTP {response.status_code}"}

            data = response.json()
            if "error" in data:
                return {"processed": 0, "inserted": 0, "error": str(data["error"])}

            items = data.get("items", [])
            records = []
            for item in items:
                record = self._transform_openei_rate(item, None)
                if record:
                    records.append(record)

            if records:
                inserted, updated = self.bulk_upsert(
                    UtilityRate,
                    records,
                    unique_columns=["rate_schedule_id"],
                    update_columns=[
                        "utility_id", "utility_name", "state",
                        "rate_schedule_name", "customer_class", "sector",
                        "energy_rate_kwh", "demand_charge_kw",
                        "fixed_monthly_charge",
                        "energy_tiers", "demand_tiers",
                        "has_time_of_use", "has_demand_charges",
                        "has_net_metering", "effective_date",
                        "source", "source_url", "collected_at",
                    ],
                )
                return {"processed": len(items), "inserted": inserted + updated}

            return {"processed": len(items), "inserted": 0}

        except Exception as e:
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_openei_rate(
        self, rate: Dict[str, Any], state: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Transform OpenEI URDB rate schedule into a UtilityRate record."""
        label = rate.get("label")
        utility = rate.get("utility")

        if not label or not utility:
            return None

        # Map customer class
        sector = rate.get("sector", "")
        customer_class = CUSTOMER_CLASS_MAP.get(
            sector, sector.lower() if sector else "commercial"
        )

        # Extract energy rate from rate structure
        energy_rate = None
        energy_tiers = None
        ers = rate.get("energyratestructure", [])
        if ers:
            tiers = []
            for period in ers:
                if isinstance(period, list):
                    period_tiers = []
                    for tier in period:
                        if isinstance(tier, dict):
                            period_tiers.append({
                                "rate": tier.get("rate"),
                                "max_kwh": tier.get("max"),
                                "adj": tier.get("adj"),
                            })
                    tiers.append(period_tiers)
            if tiers:
                energy_tiers = json.dumps(tiers)
                # Use first tier of first period as base rate
                first = tiers[0][0] if tiers[0] else {}
                energy_rate = self._parse_float(first.get("rate"))

        # Extract demand charge from rate structure
        demand_charge = None
        demand_tiers = None
        drs = rate.get("demandratestructure", [])
        if drs:
            tiers = []
            for period in drs:
                if isinstance(period, list):
                    period_tiers = []
                    for tier in period:
                        if isinstance(tier, dict):
                            period_tiers.append({
                                "rate": tier.get("rate"),
                                "max_kw": tier.get("max"),
                                "adj": tier.get("adj"),
                            })
                    tiers.append(period_tiers)
            if tiers:
                demand_tiers = json.dumps(tiers)
                first = tiers[0][0] if tiers[0] else {}
                demand_charge = self._parse_float(first.get("rate"))

        # Also check flat demand charge
        if demand_charge is None:
            demand_charge = self._parse_float(rate.get("flatdemandunit"))

        # Fixed charges
        fixed_charge = self._parse_float(
            rate.get("fixedmonthlycharge") or rate.get("minmonthlycharge")
        )
        min_charge = self._parse_float(rate.get("minmonthlycharge"))

        # Time of use
        has_tou = bool(rate.get("tou", False)) or bool(rate.get("energyweekdayschedule"))
        tou_periods = None
        if has_tou and rate.get("energyweekdayschedule"):
            tou_periods = json.dumps({
                "weekday": rate.get("energyweekdayschedule"),
                "weekend": rate.get("energyweekendschedule"),
            })

        # Parse effective date
        effective_date = None
        if rate.get("startdate"):
            try:
                ts = rate["startdate"]
                if isinstance(ts, (int, float)):
                    effective_date = datetime.fromtimestamp(ts).date()
                else:
                    effective_date = datetime.strptime(
                        str(ts)[:10], "%Y-%m-%d"
                    ).date()
            except (ValueError, TypeError, OSError):
                pass

        # Determine state
        rate_state = state
        if not rate_state:
            # Try to extract from utility name or address
            for field in ["state", "address"]:
                value = rate.get(field)
                if value and isinstance(value, str) and len(value) == 2 and value.isalpha():
                    rate_state = value.upper()
                    break

        eia_id = rate.get("eiaid")

        return {
            "rate_schedule_id": label[:100],
            "utility_id": str(eia_id)[:50] if eia_id else None,
            "utility_name": utility[:255] if utility else None,
            "state": rate_state,
            "service_territory": rate.get("service_type"),
            "rate_schedule_name": (rate.get("name") or label)[:500],
            "customer_class": customer_class,
            "sector": sector[:30] if sector else None,
            "energy_rate_kwh": energy_rate,
            "demand_charge_kw": demand_charge,
            "fixed_monthly_charge": fixed_charge,
            "minimum_charge": min_charge,
            "energy_tiers": energy_tiers,
            "demand_tiers": demand_tiers,
            "has_time_of_use": has_tou,
            "tou_periods": tou_periods,
            "has_demand_charges": demand_charge is not None and demand_charge > 0,
            "has_net_metering": bool(rate.get("usenetmetering")),
            "power_factor_adjustment": bool(rate.get("voltageminimum") or rate.get("voltagemax")),
            "min_power_factor": self._parse_float(rate.get("voltageminimum")),
            "effective_date": effective_date,
            "end_date": None,
            "approved_date": None,
            "description": rate.get("description"),
            "source": "openei",
            "source_url": rate.get("uri"),
            "collected_at": datetime.utcnow(),
        }

    def _parse_float(self, value: Any) -> Optional[float]:
        """Parse float value safely."""
        if value is None or value == "" or value == "-":
            return None
        try:
            v = float(value)
            return round(v, 6)
        except (ValueError, TypeError):
            return None
