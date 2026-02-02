"""
EIA Power Plant Collector.

Fetches power plant data from EIA API v2:
- Generator capacity and fuel types
- Plant locations and operators
- Grid region assignments

API Documentation: https://www.eia.gov/opendata/documentation.php
Electricity data: https://api.eia.gov/v2/electricity/

Requires EIA_API_KEY environment variable.
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.models_site_intel import PowerPlant, ElectricityPrice
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)


@register_collector(SiteIntelSource.EIA)
class EIAPowerCollector(BaseCollector):
    """
    Collector for EIA electricity and power plant data.

    Fetches:
    - Power plants with capacity, fuel type, location
    - Electricity prices by state and sector
    """

    domain = SiteIntelDomain.POWER
    source = SiteIntelSource.EIA

    # EIA API configuration
    default_timeout = 60.0
    rate_limit_delay = 0.5  # EIA allows 5000 req/hour

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)
        if not self.api_key:
            settings = get_settings()
            self.api_key = getattr(settings, 'eia_api_key', None)

    def get_default_base_url(self) -> str:
        return "https://api.eia.gov/v2"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute EIA data collection.

        Collects power plants and electricity prices.
        """
        if not self.api_key:
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message="EIA_API_KEY not configured. Get free key at https://www.eia.gov/opendata/register.php"
            )

        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            # Collect power plants
            logger.info("Collecting EIA power plant data...")
            plants_result = await self._collect_power_plants(config)
            total_inserted += plants_result.get("inserted", 0)
            total_processed += plants_result.get("processed", 0)
            if plants_result.get("error"):
                errors.append({"source": "power_plants", "error": plants_result["error"]})

            # Collect electricity prices
            logger.info("Collecting EIA electricity price data...")
            prices_result = await self._collect_electricity_prices(config)
            total_inserted += prices_result.get("inserted", 0)
            total_processed += prices_result.get("processed", 0)
            if prices_result.get("error"):
                errors.append({"source": "electricity_prices", "error": prices_result["error"]})

            status = CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"EIA collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_power_plants(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect power plant data from EIA.

        Uses electricity/operating-generator-capacity endpoint.
        """
        try:
            all_plants = []
            offset = 0
            page_size = 5000

            while True:
                params = {
                    "api_key": self.api_key,
                    "frequency": "annual",
                    "data[0]": "nameplate-capacity-mw",
                    "data[1]": "summer-capacity-mw",
                    "data[2]": "winter-capacity-mw",
                    "sort[0][column]": "plantid",
                    "sort[0][direction]": "asc",
                    "offset": offset,
                    "length": page_size,
                }

                # Filter by states if specified
                if config.states:
                    for i, state in enumerate(config.states):
                        params[f"facets[stateid][{i}]"] = state

                response = await self.fetch_json(
                    "/electricity/operating-generator-capacity/data/",
                    params=params,
                )

                data = response.get("response", {}).get("data", [])
                if not data:
                    break

                all_plants.extend(data)
                logger.info(f"Fetched {len(data)} power plant records (total: {len(all_plants)})")

                if len(data) < page_size:
                    break

                offset += page_size
                self.update_progress(len(all_plants), len(all_plants) + page_size, "Fetching power plants")

            # Transform and deduplicate by plant
            plants_by_id = {}
            for record in all_plants:
                plant_id = record.get("plantid")
                if not plant_id:
                    continue

                # Keep the record with highest capacity for each plant
                if plant_id not in plants_by_id:
                    plants_by_id[plant_id] = self._transform_plant_record(record)
                else:
                    # Aggregate capacity
                    existing = plants_by_id[plant_id]
                    new_capacity = float(record.get("nameplate-capacity-mw") or 0)
                    existing["nameplate_capacity_mw"] = (existing.get("nameplate_capacity_mw") or 0) + new_capacity

            # Insert into database
            records = list(plants_by_id.values())
            if records:
                inserted, _ = self.bulk_upsert(
                    PowerPlant,
                    records,
                    unique_columns=["eia_plant_id"],
                    update_columns=[
                        "name", "operator_name", "state", "county", "latitude", "longitude",
                        "primary_fuel", "nameplate_capacity_mw", "summer_capacity_mw",
                        "winter_capacity_mw", "grid_region", "balancing_authority", "collected_at"
                    ],
                )
                return {"processed": len(all_plants), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect power plants: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_plant_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform EIA plant record to database format."""
        return {
            "eia_plant_id": str(record.get("plantid")),
            "name": record.get("plantName") or record.get("plant_name"),
            "operator_name": record.get("entityName") or record.get("entity_name"),
            "state": record.get("stateid") or record.get("state"),
            "county": record.get("county"),
            "latitude": self._safe_float(record.get("latitude")),
            "longitude": self._safe_float(record.get("longitude")),
            "primary_fuel": self._map_fuel_type(record.get("energy_source_code") or record.get("technology")),
            "nameplate_capacity_mw": self._safe_float(record.get("nameplate-capacity-mw")),
            "summer_capacity_mw": self._safe_float(record.get("summer-capacity-mw")),
            "winter_capacity_mw": self._safe_float(record.get("winter-capacity-mw")),
            "grid_region": record.get("balancing_authority_code"),
            "balancing_authority": record.get("balancing_authority_name"),
            "nerc_region": record.get("nerc_region"),
            "source": "eia",
            "collected_at": datetime.utcnow(),
        }

    async def _collect_electricity_prices(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect electricity prices by state and sector.

        Uses electricity/retail-sales endpoint.
        """
        try:
            all_prices = []
            offset = 0
            page_size = 5000

            # Get recent years
            current_year = datetime.now().year
            start_year = current_year - 5

            while True:
                params = {
                    "api_key": self.api_key,
                    "frequency": "annual",
                    "data[0]": "price",
                    "data[1]": "sales",
                    "data[2]": "revenue",
                    "data[3]": "customers",
                    "start": str(start_year),
                    "end": str(current_year),
                    "sort[0][column]": "period",
                    "sort[0][direction]": "desc",
                    "offset": offset,
                    "length": page_size,
                }

                # Filter by states if specified
                if config.states:
                    for i, state in enumerate(config.states):
                        params[f"facets[stateid][{i}]"] = state

                response = await self.fetch_json(
                    "/electricity/retail-sales/data/",
                    params=params,
                )

                data = response.get("response", {}).get("data", [])
                if not data:
                    break

                all_prices.extend(data)
                logger.info(f"Fetched {len(data)} electricity price records (total: {len(all_prices)})")

                if len(data) < page_size:
                    break

                offset += page_size

            # Transform records
            records = []
            for record in all_prices:
                transformed = self._transform_price_record(record)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    ElectricityPrice,
                    records,
                    unique_columns=["geography_type", "geography_id", "period_year", "period_month", "sector"],
                )
                return {"processed": len(all_prices), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect electricity prices: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_price_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform EIA price record to database format."""
        state_id = record.get("stateid")
        if not state_id:
            return None

        period = record.get("period")
        year = int(period[:4]) if period and len(period) >= 4 else None
        month = int(period[5:7]) if period and len(period) >= 7 else None

        sector_map = {
            "RES": "residential",
            "COM": "commercial",
            "IND": "industrial",
            "TRA": "transportation",
            "OTH": "other",
            "ALL": "all",
        }

        sector_code = record.get("sectorid", "ALL")
        sector = sector_map.get(sector_code, sector_code.lower() if sector_code else "all")

        return {
            "geography_type": "state",
            "geography_id": state_id,
            "geography_name": record.get("stateDescription"),
            "period_year": year,
            "period_month": month,
            "sector": sector,
            "avg_price_cents_kwh": self._safe_float(record.get("price")),
            "total_sales_mwh": self._safe_int(record.get("sales")),
            "total_revenue_thousand": self._safe_int(record.get("revenue")),
            "customer_count": self._safe_int(record.get("customers")),
            "source": "eia",
            "collected_at": datetime.utcnow(),
        }

    def _map_fuel_type(self, code: Optional[str]) -> Optional[str]:
        """Map EIA fuel codes to readable names."""
        if not code:
            return None

        fuel_map = {
            "NG": "natural_gas",
            "SUB": "coal",
            "BIT": "coal",
            "LIG": "coal",
            "COL": "coal",
            "SUN": "solar",
            "WND": "wind",
            "NUC": "nuclear",
            "WAT": "hydro",
            "GEO": "geothermal",
            "BIO": "biomass",
            "OTH": "other",
            "PET": "petroleum",
            "DFO": "petroleum",
            "RFO": "petroleum",
            "Solar Photovoltaic": "solar",
            "Onshore Wind Turbine": "wind",
            "Natural Gas": "natural_gas",
            "Conventional Hydroelectric": "hydro",
            "Nuclear": "nuclear",
        }

        return fuel_map.get(code, code.lower().replace(" ", "_") if code else None)

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
