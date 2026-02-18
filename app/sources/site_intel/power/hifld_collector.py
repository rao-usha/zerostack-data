"""
HIFLD Infrastructure Collector.

Fetches infrastructure data from HIFLD ArcGIS REST API:
- Electrical substations
- Transmission lines (future)

HIFLD Data: https://hifld-geoplatform.opendata.arcgis.com/
ArcGIS REST API: Query endpoint with JSON output.

No API key required - public data.
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import Substation
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


@register_collector(SiteIntelSource.HIFLD)
class HIFLDInfraCollector(BaseCollector):
    """
    Collector for HIFLD infrastructure data.

    Fetches:
    - Electrical substations with voltage, owner, location
    """

    domain = SiteIntelDomain.POWER
    source = SiteIntelSource.HIFLD

    # HIFLD API configuration
    default_timeout = 120.0  # Large dataset, may be slow
    rate_limit_delay = 0.2

    # HIFLD ArcGIS REST endpoints
    SUBSTATIONS_URL = "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Electric_Substations/FeatureServer/0/query"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute HIFLD data collection.

        Collects electrical substations.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            # Collect substations from API
            logger.info("Collecting HIFLD substation data...")
            substations_result = await self._collect_substations(config)
            total_inserted += substations_result.get("inserted", 0)
            total_processed += substations_result.get("processed", 0)
            if substations_result.get("error"):
                errors.append(
                    {"source": "substations", "error": substations_result["error"]}
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
            logger.error(f"HIFLD collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_substations(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect electrical substations from HIFLD.

        Uses ArcGIS REST API with pagination via resultOffset.
        """
        try:
            all_substations = []
            offset = 0
            page_size = 2000  # ArcGIS default max is often 2000

            # Build state filter if specified
            state_filter = ""
            if config.states:
                state_list = ", ".join(f"'{s}'" for s in config.states)
                state_filter = f"STATE IN ({state_list})"

            while True:
                params = {
                    "where": state_filter if state_filter else "1=1",
                    "outFields": "*",
                    "returnGeometry": "true",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                response = await self.fetch_json(
                    self.SUBSTATIONS_URL,
                    params=params,
                    use_base_url=False,  # Use full URL
                )

                features = response.get("features", [])
                if not features:
                    break

                all_substations.extend(features)
                logger.info(
                    f"Fetched {len(features)} substation records (total: {len(all_substations)})"
                )

                # Check if we've reached the end
                if len(features) < page_size:
                    break

                offset += page_size
                self.update_progress(
                    len(all_substations),
                    len(all_substations) + page_size,
                    "Fetching substations",
                )

            # Transform records
            records = []
            for feature in all_substations:
                transformed = self._transform_substation(feature)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    Substation,
                    records,
                    unique_columns=["hifld_id"],
                    update_columns=[
                        "name",
                        "state",
                        "county",
                        "city",
                        "latitude",
                        "longitude",
                        "max_voltage_kv",
                        "min_voltage_kv",
                        "owner",
                        "substation_type",
                        "status",
                        "collected_at",
                    ],
                )
                return {"processed": len(all_substations), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect substations: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_substation(
        self, feature: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Transform HIFLD substation feature to database format."""
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        # Skip if no ID
        hifld_id = attrs.get("ID") or attrs.get("OBJECTID")
        if not hifld_id:
            return None

        # Extract coordinates
        lat = None
        lng = None
        if geometry:
            lng = geometry.get("x")
            lat = geometry.get("y")

        # Parse voltage - HIFLD uses various field names
        max_voltage = self._parse_voltage(
            attrs.get("MAX_VOLT") or attrs.get("VOLTAGE") or attrs.get("MAX_VOLTAGE")
        )
        min_voltage = self._parse_voltage(
            attrs.get("MIN_VOLT") or attrs.get("MIN_VOLTAGE")
        )

        # Determine substation type
        sub_type = attrs.get("TYPE") or attrs.get("SUBTYPE")
        if sub_type:
            sub_type = sub_type.lower()
        else:
            # Infer from voltage
            if max_voltage and max_voltage >= 230:
                sub_type = "transmission"
            elif max_voltage and max_voltage >= 69:
                sub_type = "subtransmission"
            else:
                sub_type = "distribution"

        return {
            "hifld_id": str(hifld_id),
            "name": attrs.get("NAME")
            or attrs.get("SUBSTATIO")
            or f"Substation {hifld_id}",
            "state": attrs.get("STATE") or attrs.get("STATEABBR"),
            "county": attrs.get("COUNTY"),
            "city": attrs.get("CITY"),
            "latitude": lat,
            "longitude": lng,
            "max_voltage_kv": max_voltage,
            "min_voltage_kv": min_voltage,
            "owner": attrs.get("OWNER")
            or attrs.get("UTILITY")
            or attrs.get("OPERATOR"),
            "substation_type": sub_type,
            "status": attrs.get("STATUS") or "operational",
            "source": "hifld",
            "collected_at": datetime.utcnow(),
        }

    def _parse_voltage(self, value: Any) -> Optional[float]:
        """Parse voltage value to float kV."""
        if value is None or value == "" or value == -999:
            return None
        try:
            voltage = float(value)
            # Some datasets use volts, some use kV
            if voltage > 1000:
                return voltage / 1000  # Convert V to kV
            return voltage
        except (ValueError, TypeError):
            return None

    async def fetch_json(
        self,
        url: str,
        params: Optional[Dict] = None,
        use_base_url: bool = True,
        **kwargs,
    ) -> Dict:
        """
        Fetch JSON from URL, with option to skip base URL.

        Override to support full URLs for HIFLD.
        """
        if use_base_url:
            return await super().fetch_json(url, params, **kwargs)

        # Use full URL directly
        client = await self.get_client()

        await self.apply_rate_limit()

        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Request failed: {url} - {e}")
            raise
