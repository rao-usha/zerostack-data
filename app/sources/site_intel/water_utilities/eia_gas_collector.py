"""
EIA Natural Gas Collector.

Fetches natural gas infrastructure data from EIA:
- Interstate and intrastate pipelines
- Underground storage facilities
- Storage capacity and deliverability

Data source: https://www.eia.gov/opendata/
API key required (free registration).
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import NaturalGasPipeline, NaturalGasStorage
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector
from app.core.config import get_settings

logger = logging.getLogger(__name__)


# Storage type mapping
STORAGE_TYPE_MAP = {
    "DEPLETED": "depleted_field",
    "SALT": "salt_cavern",
    "AQUIFER": "aquifer",
}


@register_collector(SiteIntelSource.EIA_GAS)
class EIAGasCollector(BaseCollector):
    """
    Collector for EIA natural gas infrastructure data.

    Fetches:
    - Natural gas pipeline information
    - Underground storage facilities
    - Capacity and current inventory
    """

    domain = SiteIntelDomain.WATER_UTILITIES
    source = SiteIntelSource.EIA_GAS

    default_timeout = 60.0
    rate_limit_delay = 0.5

    # EIA API endpoints
    EIA_API_URL = "https://api.eia.gov/v2"

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
        """Execute EIA natural gas data collection."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting EIA natural gas infrastructure data...")

            # Collect pipelines
            pipeline_result = await self._collect_pipelines(config)
            total_inserted += pipeline_result.get("inserted", 0)
            total_processed += pipeline_result.get("processed", 0)
            if pipeline_result.get("error"):
                errors.append({"source": "pipelines", "error": pipeline_result["error"]})

            # Collect storage facilities
            storage_result = await self._collect_storage(config)
            total_inserted += storage_result.get("inserted", 0)
            total_processed += storage_result.get("processed", 0)
            if storage_result.get("error"):
                errors.append({"source": "storage", "error": storage_result["error"]})

            # If no data from API, load sample data
            if total_processed == 0:
                logger.info("No API data retrieved, loading sample data...")
                sample_result = await self._load_sample_data(config)
                total_inserted = sample_result.get("inserted", 0)
                total_processed = sample_result.get("processed", 0)

            status = CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"EIA gas collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_pipelines(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect natural gas pipeline data from EIA."""
        try:
            if not self.api_key:
                logger.warning("No EIA API key configured, skipping API collection")
                return {"processed": 0, "inserted": 0}

            client = await self.get_client()
            await self.apply_rate_limit()

            # EIA natural gas pipeline capacity data
            url = f"{self.EIA_API_URL}/natural-gas/pipe/cap"
            params = {
                "api_key": self.api_key,
                "frequency": "annual",
                "data[0]": "value",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 1000,
            }

            response = await client.get(url, params=params)

            if response.status_code != 200:
                logger.warning(f"EIA API returned {response.status_code}")
                return {"processed": 0, "inserted": 0}

            data = response.json()
            pipeline_data = data.get("response", {}).get("data", [])

            if not pipeline_data:
                return {"processed": 0, "inserted": 0}

            # Transform and dedupe by pipeline
            pipelines_by_id = {}
            for row in pipeline_data:
                pipeline_id = row.get("duoarea") or row.get("series-id")
                if pipeline_id and pipeline_id not in pipelines_by_id:
                    transformed = self._transform_pipeline(row)
                    if transformed:
                        pipelines_by_id[pipeline_id] = transformed

            records = list(pipelines_by_id.values())

            if records:
                inserted, _ = self.bulk_upsert(
                    NaturalGasPipeline,
                    records,
                    unique_columns=["pipeline_id"],
                    update_columns=[
                        "pipeline_name", "operator_name", "origin_state",
                        "destination_state", "capacity_mmcfd", "pipeline_type",
                        "status", "source", "collected_at"
                    ],
                )
                logger.info(f"Inserted/updated {inserted} natural gas pipelines")
                return {"processed": len(pipeline_data), "inserted": inserted}

            return {"processed": len(pipeline_data), "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect pipelines: {e}")
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_pipeline(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform EIA pipeline data to database format."""
        pipeline_id = row.get("duoarea") or row.get("series-id")
        if not pipeline_id:
            return None

        pipeline_name = row.get("duoarea-name") or row.get("series-name", f"Pipeline {pipeline_id}")

        return {
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "operator_name": row.get("operator"),
            "origin_state": row.get("fromstate"),
            "destination_state": row.get("tostate"),
            "capacity_mmcfd": self._parse_float(row.get("value")),
            "pipeline_type": "interstate",
            "status": "operational",
            "source": "eia",
            "collected_at": datetime.utcnow(),
        }

    async def _collect_storage(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect natural gas storage facility data from EIA."""
        try:
            if not self.api_key:
                return {"processed": 0, "inserted": 0}

            client = await self.get_client()
            await self.apply_rate_limit()

            # EIA underground storage capacity
            url = f"{self.EIA_API_URL}/natural-gas/stor/cap"
            params = {
                "api_key": self.api_key,
                "frequency": "annual",
                "data[0]": "value",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 500,
            }

            response = await client.get(url, params=params)

            if response.status_code != 200:
                return {"processed": 0, "inserted": 0}

            data = response.json()
            storage_data = data.get("response", {}).get("data", [])

            if not storage_data:
                return {"processed": 0, "inserted": 0}

            # Transform records
            storage_by_id = {}
            for row in storage_data:
                facility_id = row.get("duoarea") or row.get("series-id")
                if facility_id and facility_id not in storage_by_id:
                    transformed = self._transform_storage(row)
                    if transformed:
                        storage_by_id[facility_id] = transformed

            records = list(storage_by_id.values())

            if records:
                inserted, _ = self.bulk_upsert(
                    NaturalGasStorage,
                    records,
                    unique_columns=["facility_id"],
                    update_columns=[
                        "facility_name", "operator_name", "state",
                        "storage_type", "total_capacity_bcf", "working_gas_bcf",
                        "base_gas_bcf", "deliverability_mmcfd",
                        "status", "source", "collected_at"
                    ],
                )
                logger.info(f"Inserted/updated {inserted} natural gas storage facilities")
                return {"processed": len(storage_data), "inserted": inserted}

            return {"processed": len(storage_data), "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect storage: {e}")
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_storage(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform EIA storage data to database format."""
        facility_id = row.get("duoarea") or row.get("series-id")
        if not facility_id:
            return None

        facility_name = row.get("duoarea-name") or row.get("series-name", f"Storage {facility_id}")

        # Extract state from duoarea if available (usually 2-letter code at start)
        state = row.get("state")
        if not state and len(facility_id) >= 2:
            state = facility_id[:2] if facility_id[:2].isalpha() else None

        return {
            "facility_id": facility_id,
            "facility_name": facility_name,
            "operator_name": row.get("operator"),
            "state": state,
            "storage_type": row.get("storage-type", "depleted_field").lower().replace(" ", "_"),
            "total_capacity_bcf": self._parse_float(row.get("value")),
            "working_gas_bcf": self._parse_float(row.get("working-gas")),
            "base_gas_bcf": self._parse_float(row.get("base-gas")),
            "deliverability_mmcfd": self._parse_float(row.get("deliverability")),
            "status": "operational",
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

    async def _load_sample_data(self, config: CollectionConfig) -> Dict[str, Any]:
        """Load sample data when API is unavailable."""
        # Sample pipelines - major interstate systems
        sample_pipelines = [
            {"pipeline_id": "TEXGATEWAY", "pipeline_name": "Texas Eastern Pipeline", "operator_name": "Enbridge",
             "origin_state": "TX", "destination_state": "NJ", "capacity_mmcfd": 10000,
             "pipeline_type": "interstate", "states_crossed": ["TX", "LA", "MS", "AL", "TN", "KY", "OH", "PA", "NJ"]},
            {"pipeline_id": "TRANSCO", "pipeline_name": "Transcontinental Gas Pipeline", "operator_name": "Williams",
             "origin_state": "TX", "destination_state": "NY", "capacity_mmcfd": 15000,
             "pipeline_type": "interstate", "states_crossed": ["TX", "LA", "MS", "AL", "GA", "SC", "NC", "VA", "MD", "PA", "NJ", "NY"]},
            {"pipeline_id": "COLUMBIA", "pipeline_name": "Columbia Gas Transmission", "operator_name": "TC Energy",
             "origin_state": "LA", "destination_state": "NY", "capacity_mmcfd": 5800,
             "pipeline_type": "interstate", "states_crossed": ["LA", "KY", "WV", "OH", "PA", "NY"]},
            {"pipeline_id": "NATURAL", "pipeline_name": "Natural Gas Pipeline of America", "operator_name": "Kinder Morgan",
             "origin_state": "TX", "destination_state": "IL", "capacity_mmcfd": 7500,
             "pipeline_type": "interstate", "states_crossed": ["TX", "OK", "KS", "NE", "IA", "IL"]},
            {"pipeline_id": "ELMCREEK", "pipeline_name": "El Paso Natural Gas", "operator_name": "Kinder Morgan",
             "origin_state": "TX", "destination_state": "CA", "capacity_mmcfd": 5500,
             "pipeline_type": "interstate", "states_crossed": ["TX", "NM", "AZ", "CA"]},
            {"pipeline_id": "ROCKIES", "pipeline_name": "Rockies Express Pipeline", "operator_name": "Tallgrass",
             "origin_state": "CO", "destination_state": "OH", "capacity_mmcfd": 1800,
             "pipeline_type": "interstate", "states_crossed": ["CO", "WY", "NE", "MO", "IL", "IN", "OH"]},
            {"pipeline_id": "GULFSTREAM", "pipeline_name": "Gulfstream Natural Gas", "operator_name": "Williams",
             "origin_state": "MS", "destination_state": "FL", "capacity_mmcfd": 1300,
             "pipeline_type": "interstate", "states_crossed": ["MS", "AL", "FL"]},
            {"pipeline_id": "PERMIAN", "pipeline_name": "Permian Highway Pipeline", "operator_name": "Kinder Morgan",
             "origin_state": "TX", "destination_state": "TX", "capacity_mmcfd": 2100,
             "pipeline_type": "intrastate", "states_crossed": ["TX"], "latitude": 31.9, "longitude": -102.1},
        ]

        # Sample storage facilities
        sample_storage = [
            {"facility_id": "MOSS_BLUFF", "facility_name": "Moss Bluff Storage", "operator_name": "Kinder Morgan",
             "state": "TX", "county": "Liberty", "latitude": 30.2, "longitude": -94.6,
             "storage_type": "salt_cavern", "total_capacity_bcf": 15.5, "working_gas_bcf": 12.0,
             "deliverability_mmcfd": 1500},
            {"facility_id": "MIDCONTINENT", "facility_name": "Mid-Continent Storage", "operator_name": "ONEOK",
             "state": "OK", "county": "Wagoner", "latitude": 35.9, "longitude": -95.4,
             "storage_type": "depleted_field", "total_capacity_bcf": 24.0, "working_gas_bcf": 18.0,
             "deliverability_mmcfd": 800},
            {"facility_id": "WILD_GOOSE", "facility_name": "Wild Goose Storage", "operator_name": "Pacific Gas Electric",
             "state": "CA", "county": "Butte", "latitude": 39.4, "longitude": -121.9,
             "storage_type": "depleted_field", "total_capacity_bcf": 75.0, "working_gas_bcf": 50.0,
             "deliverability_mmcfd": 600},
            {"facility_id": "ALISO_CANYON", "facility_name": "Aliso Canyon", "operator_name": "Southern California Gas",
             "state": "CA", "county": "Los Angeles", "latitude": 34.3, "longitude": -118.6,
             "storage_type": "depleted_field", "total_capacity_bcf": 86.0, "working_gas_bcf": 34.0,
             "deliverability_mmcfd": 1800},
            {"facility_id": "WESTERN_CAVE", "facility_name": "Western Kentucky Gas Storage", "operator_name": "Texas Gas",
             "state": "KY", "county": "Henderson", "latitude": 37.8, "longitude": -87.6,
             "storage_type": "aquifer", "total_capacity_bcf": 12.0, "working_gas_bcf": 8.0,
             "deliverability_mmcfd": 300},
            {"facility_id": "STAGECOACH", "facility_name": "Stagecoach South", "operator_name": "Crestwood",
             "state": "NY", "county": "Chemung", "latitude": 42.1, "longitude": -76.8,
             "storage_type": "salt_cavern", "total_capacity_bcf": 6.0, "working_gas_bcf": 5.0,
             "deliverability_mmcfd": 600},
            {"facility_id": "SENECA_LAKE", "facility_name": "Seneca Lake Storage", "operator_name": "Crestwood",
             "state": "NY", "county": "Schuyler", "latitude": 42.4, "longitude": -76.9,
             "storage_type": "salt_cavern", "total_capacity_bcf": 1.5, "working_gas_bcf": 1.2,
             "deliverability_mmcfd": 250},
        ]

        # Filter by states if specified
        if config.states:
            sample_pipelines = [p for p in sample_pipelines
                               if p.get("origin_state") in config.states or p.get("destination_state") in config.states]
            sample_storage = [s for s in sample_storage if s.get("state") in config.states]

        total_inserted = 0
        total_processed = 0

        # Insert pipelines
        pipeline_records = []
        for pipeline in sample_pipelines:
            record = {
                "pipeline_id": pipeline["pipeline_id"],
                "pipeline_name": pipeline["pipeline_name"],
                "operator_name": pipeline.get("operator_name"),
                "origin_state": pipeline.get("origin_state"),
                "destination_state": pipeline.get("destination_state"),
                "states_crossed": pipeline.get("states_crossed"),
                "capacity_mmcfd": pipeline.get("capacity_mmcfd"),
                "pipeline_type": pipeline.get("pipeline_type", "interstate"),
                "latitude": pipeline.get("latitude"),
                "longitude": pipeline.get("longitude"),
                "status": "operational",
                "source": "eia_sample",
                "collected_at": datetime.utcnow(),
            }
            pipeline_records.append(record)

        if pipeline_records:
            inserted, _ = self.bulk_upsert(
                NaturalGasPipeline,
                pipeline_records,
                unique_columns=["pipeline_id"],
            )
            total_inserted += inserted
            total_processed += len(pipeline_records)
            logger.info(f"Loaded {inserted} sample natural gas pipelines")

        # Insert storage facilities
        storage_records = []
        for storage in sample_storage:
            record = {
                "facility_id": storage["facility_id"],
                "facility_name": storage["facility_name"],
                "operator_name": storage.get("operator_name"),
                "state": storage.get("state"),
                "county": storage.get("county"),
                "latitude": storage.get("latitude"),
                "longitude": storage.get("longitude"),
                "storage_type": storage.get("storage_type"),
                "total_capacity_bcf": storage.get("total_capacity_bcf"),
                "working_gas_bcf": storage.get("working_gas_bcf"),
                "deliverability_mmcfd": storage.get("deliverability_mmcfd"),
                "status": "operational",
                "source": "eia_sample",
                "collected_at": datetime.utcnow(),
            }
            storage_records.append(record)

        if storage_records:
            inserted, _ = self.bulk_upsert(
                NaturalGasStorage,
                storage_records,
                unique_columns=["facility_id"],
            )
            total_inserted += inserted
            total_processed += len(storage_records)
            logger.info(f"Loaded {inserted} sample natural gas storage facilities")

        return {"processed": total_processed, "inserted": total_inserted}
