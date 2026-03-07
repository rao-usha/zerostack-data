"""
HIFLD Infrastructure Collector.

Fetches infrastructure data from HIFLD ArcGIS REST API:
- Electrical substations
- Transmission lines

HIFLD Data: https://hifld-geoplatform.opendata.arcgis.com/
ArcGIS REST API: Query endpoint with JSON output.

No API key required - public data.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import Substation, TransmissionLine
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
    - Transmission lines with voltage, type, owner
    """

    domain = SiteIntelDomain.POWER
    source = SiteIntelSource.HIFLD

    # HIFLD API configuration
    default_timeout = 120.0  # Large dataset, may be slow
    rate_limit_delay = 0.1  # ArcGIS mirror, low traffic

    # HIFLD ArcGIS REST endpoints (Rutgers mirror — primary HIFLD URL was decommissioned)
    SUBSTATIONS_URL = "https://oceandata.rad.rutgers.edu/arcgis/rest/services/RenewableEnergy/HIFLD_Electric_SubstationsTransmissionLines/MapServer/0/query"
    TRANSMISSION_URL = "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Electric_Power_Transmission_Lines/FeatureServer/0/query"

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

        Collects electrical substations and transmission lines.
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

            # Collect transmission lines
            logger.info("Collecting HIFLD transmission line data...")
            lines_result = await self._collect_transmission_lines(config)
            total_inserted += lines_result.get("inserted", 0)
            total_processed += lines_result.get("processed", 0)
            if lines_result.get("error"):
                errors.append(
                    {"source": "transmission_lines", "error": lines_result["error"]}
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
            page_size = 1000  # Rutgers mirror caps at 1000 per request

            # Build state filter if specified
            state_filter = ""
            if config.states:
                state_list = ", ".join(f"'{s}'" for s in config.states)
                state_filter = f"STATE IN ({state_list})"

            while True:
                params = {
                    "where": state_filter if state_filter else "1=1",
                    "outFields": "*",
                    "returnGeometry": "false",
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
                if len(features) < page_size and not response.get("exceededTransferLimit"):
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

        # Use LATITUDE/LONGITUDE attributes (geometry is Web Mercator, not WGS84)
        lat = self._safe_float(attrs.get("LATITUDE"))
        lng = self._safe_float(attrs.get("LONGITUDE"))

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

    async def _collect_transmission_lines(
        self, config: CollectionConfig
    ) -> Dict[str, Any]:
        """
        Collect transmission lines from HIFLD ArcGIS FeatureServer.

        Uses pagination via resultOffset.
        Note: Transmission lines have no STATE field — they span state boundaries.
        State filtering is not supported; all lines are collected.
        """
        try:
            all_lines = []
            offset = 0
            page_size = 2000

            while True:
                params = {
                    "where": "1=1",
                    "outFields": "OBJECTID_1,OBJECTID,ID,OWNER,VOLTAGE,VOLT_CLASS,Shape__Length,TYPE,STATUS",
                    "returnGeometry": "false",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                }

                response = await self.fetch_json(
                    self.TRANSMISSION_URL,
                    params=params,
                    use_base_url=False,
                )

                features = response.get("features", [])
                if not features:
                    break

                all_lines.extend(features)
                logger.info(
                    f"Fetched {len(features)} transmission line records "
                    f"(total: {len(all_lines)})"
                )

                if len(features) < page_size and not response.get(
                    "exceededTransferLimit"
                ):
                    break

                offset += page_size
                self.update_progress(
                    len(all_lines),
                    len(all_lines) + page_size,
                    "Fetching transmission lines",
                )

            # Transform records
            records = []
            for feature in all_lines:
                transformed = self._transform_transmission_line(feature)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    TransmissionLine,
                    records,
                    unique_columns=["hifld_id"],
                    update_columns=[
                        "name",
                        "state",
                        "owner",
                        "voltage_kv",
                        "voltage_class",
                        "num_circuits",
                        "line_type",
                        "sub_type",
                        "length_miles",
                        "status",
                        "collected_at",
                    ],
                )
                return {"processed": len(all_lines), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(
                f"Failed to collect transmission lines: {e}", exc_info=True
            )
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_transmission_line(
        self, feature: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Transform HIFLD transmission line feature to database format."""
        attrs = feature.get("attributes", {})

        # ID field, fallback to OBJECTID_1 or OBJECTID
        hifld_id = attrs.get("ID") or attrs.get("OBJECTID_1") or attrs.get("OBJECTID")
        if not hifld_id:
            return None

        voltage = self._parse_voltage(attrs.get("VOLTAGE"))
        volt_class = attrs.get("VOLT_CLASS") or ""

        # Shape__Length is in meters (Web Mercator); convert to miles
        shape_length = self._safe_float(
            attrs.get("Shape__Length") or attrs.get("SHAPE__Len")
        )
        length_miles = None
        if shape_length and shape_length > 0:
            length_miles = round(shape_length * 0.000621371, 3)

        # TYPE field contains combined info like "AC; OVERHEAD"
        type_raw = attrs.get("TYPE") or ""
        line_type = None
        sub_type = None
        if type_raw:
            parts = [p.strip().upper() for p in type_raw.split(";")]
            for part in parts:
                if part in ("AC", "DC"):
                    line_type = part
                elif part in ("OVERHEAD", "UNDERGROUND", "SUBMARINE"):
                    sub_type = part.lower()
            if not line_type:
                line_type = "AC"  # Default

        # Status
        status = attrs.get("STATUS") or "IN SERVICE"
        if isinstance(status, str):
            status = status.strip().lower()

        owner = attrs.get("OWNER")

        return {
            "hifld_id": str(hifld_id),
            "name": f"Line {hifld_id}",
            "state": None,  # Lines span states; no single state attribute
            "owner": owner,
            "voltage_kv": voltage,
            "voltage_class": volt_class if volt_class else None,
            "num_circuits": None,
            "line_type": line_type,
            "sub_type": sub_type,
            "length_miles": length_miles,
            "status": status,
            "source": "hifld",
            "collected_at": datetime.utcnow(),
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "" or value == -999:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

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
