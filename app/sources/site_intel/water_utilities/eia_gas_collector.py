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
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
    CollectionStatus,
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
            self.api_key = getattr(settings, "eia_api_key", None)

    def get_default_base_url(self) -> str:
        return "https://api.eia.gov/v2"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Execute EIA natural gas data collection."""
        if not self.api_key:
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message="EIA_API_KEY not configured. Get free key at https://www.eia.gov/opendata/register.php",
            )

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
                errors.append(
                    {"source": "pipelines", "error": pipeline_result["error"]}
                )

            # Collect storage facilities
            storage_result = await self._collect_storage(config)
            total_inserted += storage_result.get("inserted", 0)
            total_processed += storage_result.get("processed", 0)
            if storage_result.get("error"):
                errors.append({"source": "storage", "error": storage_result["error"]})

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
            logger.error(f"EIA gas collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_pipelines(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect natural gas pipeline data from EIA."""
        try:
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
                        "pipeline_name",
                        "operator_name",
                        "origin_state",
                        "destination_state",
                        "capacity_mmcfd",
                        "pipeline_type",
                        "status",
                        "source",
                        "collected_at",
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

        pipeline_name = row.get("duoarea-name") or row.get(
            "series-name", f"Pipeline {pipeline_id}"
        )

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
                        "facility_name",
                        "operator_name",
                        "state",
                        "storage_type",
                        "total_capacity_bcf",
                        "working_gas_bcf",
                        "base_gas_bcf",
                        "deliverability_mmcfd",
                        "status",
                        "source",
                        "collected_at",
                    ],
                )
                logger.info(
                    f"Inserted/updated {inserted} natural gas storage facilities"
                )
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

        facility_name = row.get("duoarea-name") or row.get(
            "series-name", f"Storage {facility_id}"
        )

        # Extract state from duoarea if available (usually 2-letter code at start)
        state = row.get("state")
        if not state and len(facility_id) >= 2:
            state = facility_id[:2] if facility_id[:2].isalpha() else None

        return {
            "facility_id": facility_id,
            "facility_name": facility_name,
            "operator_name": row.get("operator"),
            "state": state,
            "storage_type": row.get("storage-type", "depleted_field")
            .lower()
            .replace(" ", "_"),
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
