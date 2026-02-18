"""
EIA Electricity Data Collector.

Fetches electricity data from EIA:
- State/utility electricity prices
- Consumption by sector
- Generation mix

Data source: https://www.eia.gov/opendata/
API key required (free registration).
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

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
from app.core.config import get_settings

logger = logging.getLogger(__name__)


# State FIPS to abbreviation for price data
STATE_ABBR = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}


@register_collector(SiteIntelSource.EIA_ELECTRICITY)
class EIAElectricityCollector(BaseCollector):
    """
    Collector for EIA electricity price and consumption data.

    Fetches:
    - Average electricity prices by state and sector
    - Monthly price trends
    - Utility-level pricing
    """

    domain = SiteIntelDomain.WATER_UTILITIES
    source = SiteIntelSource.EIA_ELECTRICITY

    default_timeout = 60.0
    rate_limit_delay = 0.5

    # EIA API endpoint
    EIA_API_URL = "https://api.eia.gov/v2"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)
        if not self.api_key:
            settings = get_settings()
            self.api_key = getattr(settings, "eia_api_key", None)

    def get_default_base_url(self) -> str:
        return "https://api.eia.gov/v2"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Execute EIA electricity data collection."""
        if not self.api_key:
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message="EIA_API_KEY not configured. Get free key at https://www.eia.gov/opendata/register.php",
            )

        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting EIA electricity price data...")

            # Collect state average prices
            prices_result = await self._collect_state_prices(config)
            total_inserted += prices_result.get("inserted", 0)
            total_processed += prices_result.get("processed", 0)
            if prices_result.get("error"):
                errors.append(
                    {"source": "state_prices", "error": prices_result["error"]}
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
            logger.error(f"EIA electricity collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_state_prices(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect state-level electricity prices from EIA."""
        try:
            client = await self.get_client()
            await self.apply_rate_limit()

            # EIA electricity prices by state and sector
            url = f"{self.EIA_API_URL}/electricity/retail-sales/data"
            params = {
                "api_key": self.api_key,
                "frequency": "monthly",
                "data[0]": "price",
                "facets[sectorid][]": [
                    "IND",
                    "COM",
                    "RES",
                ],  # Industrial, Commercial, Residential
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 500,
            }

            # Filter by states if specified
            if config.states:
                for state in config.states:
                    params[f"facets[stateid][]"] = state

            response = await client.get(url, params=params)

            if response.status_code != 200:
                logger.warning(f"EIA API returned {response.status_code}")
                return {"processed": 0, "inserted": 0}

            data = response.json()
            price_data = data.get("response", {}).get("data", [])

            if not price_data:
                return {"processed": 0, "inserted": 0}

            # Group by state and sector to create rate records
            rates_by_key = {}
            for row in price_data:
                state = row.get("stateid")
                sector = row.get("sectorid")
                period = row.get("period")

                if not state or not sector:
                    continue

                key = f"EIA_{state}_{sector}_{period}"

                if key not in rates_by_key:
                    rates_by_key[key] = self._transform_price_to_rate(row)

            records = list(rates_by_key.values())
            records = [r for r in records if r is not None]

            if records:
                inserted, _ = self.bulk_upsert(
                    UtilityRate,
                    records,
                    unique_columns=["rate_schedule_id"],
                    update_columns=[
                        "utility_id",
                        "utility_name",
                        "state",
                        "rate_schedule_name",
                        "customer_class",
                        "energy_rate_kwh",
                        "effective_date",
                        "source",
                        "collected_at",
                    ],
                )
                logger.info(f"Inserted/updated {inserted} state electricity prices")
                return {"processed": len(price_data), "inserted": inserted}

            return {"processed": len(price_data), "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect state prices: {e}")
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_price_to_rate(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform EIA price data to utility rate format."""
        state = row.get("stateid")
        sector = row.get("sectorid")
        period = row.get("period")
        price = row.get("price")

        if not state or not sector or price is None:
            return None

        # Map sector to customer class
        sector_map = {
            "IND": "industrial",
            "COM": "commercial",
            "RES": "residential",
            "ALL": "all",
        }
        customer_class = sector_map.get(sector, sector.lower())

        # State name
        state_name = STATE_ABBR.get(state, state)

        # Parse period to date
        effective_date = None
        if period:
            try:
                # Period format: "2024-01"
                effective_date = datetime.strptime(f"{period}-01", "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

        # Price is in cents/kWh, convert to $/kWh
        energy_rate = self._parse_float(price)
        if energy_rate:
            energy_rate = energy_rate / 100.0  # cents to dollars

        return {
            "rate_schedule_id": f"EIA_{state}_{sector}_{period}",
            "utility_id": f"EIA_{state}",
            "utility_name": f"{state_name} State Average",
            "state": state,
            "rate_schedule_name": f"{state_name} {customer_class.title()} Average Rate ({period})",
            "customer_class": customer_class,
            "energy_rate_kwh": energy_rate,
            "has_time_of_use": False,
            "has_demand_charges": False,
            "effective_date": effective_date,
            "source": "eia",
            "collected_at": datetime.utcnow(),
        }

    def _parse_float(self, value: Any) -> Optional[float]:
        """Parse float value."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
