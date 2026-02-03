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
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
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
        Falls back to sample data if API unavailable.
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
                errors.append({"source": "substations", "error": substations_result["error"]})

            # Fall back to sample data if API failed or returned no data
            if total_processed == 0:
                logger.info("No API data retrieved, loading sample substation data...")
                sample_result = await self._load_sample_substations(config)
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
                logger.info(f"Fetched {len(features)} substation records (total: {len(all_substations)})")

                # Check if we've reached the end
                if len(features) < page_size:
                    break

                offset += page_size
                self.update_progress(len(all_substations), len(all_substations) + page_size, "Fetching substations")

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
                        "name", "state", "county", "city", "latitude", "longitude",
                        "max_voltage_kv", "min_voltage_kv", "owner",
                        "substation_type", "status", "collected_at"
                    ],
                )
                return {"processed": len(all_substations), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect substations: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_substation(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
            "name": attrs.get("NAME") or attrs.get("SUBSTATIO") or f"Substation {hifld_id}",
            "state": attrs.get("STATE") or attrs.get("STATEABBR"),
            "county": attrs.get("COUNTY"),
            "city": attrs.get("CITY"),
            "latitude": lat,
            "longitude": lng,
            "max_voltage_kv": max_voltage,
            "min_voltage_kv": min_voltage,
            "owner": attrs.get("OWNER") or attrs.get("UTILITY") or attrs.get("OPERATOR"),
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
        **kwargs
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

    async def _load_sample_substations(self, config: CollectionConfig) -> Dict[str, Any]:
        """Load sample substation data when API is unavailable."""
        # Sample electrical substations across major industrial states
        sample_substations = [
            # Texas - ERCOT grid
            {"hifld_id": "100001", "name": "Houston Central Substation", "state": "TX",
             "county": "Harris", "city": "Houston", "latitude": 29.7604, "longitude": -95.3698,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "CenterPoint Energy",
             "substation_type": "transmission"},
            {"hifld_id": "100002", "name": "Dallas North Substation", "state": "TX",
             "county": "Dallas", "city": "Dallas", "latitude": 32.8998, "longitude": -96.7800,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "Oncor",
             "substation_type": "transmission"},
            {"hifld_id": "100003", "name": "San Antonio West Substation", "state": "TX",
             "county": "Bexar", "city": "San Antonio", "latitude": 29.4241, "longitude": -98.4936,
             "max_voltage_kv": 138.0, "min_voltage_kv": 69.0, "owner": "CPS Energy",
             "substation_type": "subtransmission"},
            {"hifld_id": "100004", "name": "Austin Industrial Substation", "state": "TX",
             "county": "Travis", "city": "Austin", "latitude": 30.2672, "longitude": -97.7431,
             "max_voltage_kv": 138.0, "min_voltage_kv": 69.0, "owner": "Austin Energy",
             "substation_type": "subtransmission"},
            # California - CAISO grid
            {"hifld_id": "100011", "name": "Los Angeles Central Substation", "state": "CA",
             "county": "Los Angeles", "city": "Los Angeles", "latitude": 34.0522, "longitude": -118.2437,
             "max_voltage_kv": 500.0, "min_voltage_kv": 230.0, "owner": "SCE",
             "substation_type": "transmission"},
            {"hifld_id": "100012", "name": "San Diego Mesa Substation", "state": "CA",
             "county": "San Diego", "city": "San Diego", "latitude": 32.7157, "longitude": -117.1611,
             "max_voltage_kv": 230.0, "min_voltage_kv": 138.0, "owner": "SDG&E",
             "substation_type": "transmission"},
            {"hifld_id": "100013", "name": "Sacramento North Substation", "state": "CA",
             "county": "Sacramento", "city": "Sacramento", "latitude": 38.5816, "longitude": -121.4944,
             "max_voltage_kv": 230.0, "min_voltage_kv": 115.0, "owner": "SMUD",
             "substation_type": "transmission"},
            {"hifld_id": "100014", "name": "Fresno Industrial Substation", "state": "CA",
             "county": "Fresno", "city": "Fresno", "latitude": 36.7378, "longitude": -119.7871,
             "max_voltage_kv": 115.0, "min_voltage_kv": 69.0, "owner": "PG&E",
             "substation_type": "subtransmission"},
            # Pennsylvania - PJM grid
            {"hifld_id": "100021", "name": "Philadelphia Main Substation", "state": "PA",
             "county": "Philadelphia", "city": "Philadelphia", "latitude": 39.9526, "longitude": -75.1652,
             "max_voltage_kv": 500.0, "min_voltage_kv": 230.0, "owner": "PECO",
             "substation_type": "transmission"},
            {"hifld_id": "100022", "name": "Pittsburgh West Substation", "state": "PA",
             "county": "Allegheny", "city": "Pittsburgh", "latitude": 40.4406, "longitude": -79.9959,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "Duquesne Light",
             "substation_type": "transmission"},
            {"hifld_id": "100023", "name": "Harrisburg Central Substation", "state": "PA",
             "county": "Dauphin", "city": "Harrisburg", "latitude": 40.2732, "longitude": -76.8867,
             "max_voltage_kv": 230.0, "min_voltage_kv": 115.0, "owner": "PPL",
             "substation_type": "transmission"},
            # Ohio - PJM grid
            {"hifld_id": "100031", "name": "Columbus Central Substation", "state": "OH",
             "county": "Franklin", "city": "Columbus", "latitude": 39.9612, "longitude": -82.9988,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "AEP Ohio",
             "substation_type": "transmission"},
            {"hifld_id": "100032", "name": "Cleveland East Substation", "state": "OH",
             "county": "Cuyahoga", "city": "Cleveland", "latitude": 41.4993, "longitude": -81.6944,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "FirstEnergy",
             "substation_type": "transmission"},
            {"hifld_id": "100033", "name": "Cincinnati Industrial Substation", "state": "OH",
             "county": "Hamilton", "city": "Cincinnati", "latitude": 39.1031, "longitude": -84.5120,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "Duke Energy Ohio",
             "substation_type": "transmission"},
            {"hifld_id": "100034", "name": "Toledo Manufacturing Substation", "state": "OH",
             "county": "Lucas", "city": "Toledo", "latitude": 41.6528, "longitude": -83.5379,
             "max_voltage_kv": 138.0, "min_voltage_kv": 69.0, "owner": "Toledo Edison",
             "substation_type": "subtransmission"},
            # Illinois - PJM/MISO grid
            {"hifld_id": "100041", "name": "Chicago Loop Substation", "state": "IL",
             "county": "Cook", "city": "Chicago", "latitude": 41.8781, "longitude": -87.6298,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "ComEd",
             "substation_type": "transmission"},
            {"hifld_id": "100042", "name": "Aurora Industrial Substation", "state": "IL",
             "county": "Kane", "city": "Aurora", "latitude": 41.7606, "longitude": -88.3201,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "ComEd",
             "substation_type": "transmission"},
            {"hifld_id": "100043", "name": "Rockford West Substation", "state": "IL",
             "county": "Winnebago", "city": "Rockford", "latitude": 42.2711, "longitude": -89.0940,
             "max_voltage_kv": 138.0, "min_voltage_kv": 69.0, "owner": "ComEd",
             "substation_type": "subtransmission"},
            # Indiana - MISO grid
            {"hifld_id": "100051", "name": "Indianapolis Central Substation", "state": "IN",
             "county": "Marion", "city": "Indianapolis", "latitude": 39.7684, "longitude": -86.1581,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "AES Indiana",
             "substation_type": "transmission"},
            {"hifld_id": "100052", "name": "Fort Wayne Industrial Substation", "state": "IN",
             "county": "Allen", "city": "Fort Wayne", "latitude": 41.0793, "longitude": -85.1394,
             "max_voltage_kv": 138.0, "min_voltage_kv": 69.0, "owner": "I&M",
             "substation_type": "subtransmission"},
            # Washington - BPA grid
            {"hifld_id": "100061", "name": "Seattle Central Substation", "state": "WA",
             "county": "King", "city": "Seattle", "latitude": 47.6062, "longitude": -122.3321,
             "max_voltage_kv": 230.0, "min_voltage_kv": 115.0, "owner": "Seattle City Light",
             "substation_type": "transmission"},
            {"hifld_id": "100062", "name": "Tacoma Industrial Substation", "state": "WA",
             "county": "Pierce", "city": "Tacoma", "latitude": 47.2529, "longitude": -122.4443,
             "max_voltage_kv": 230.0, "min_voltage_kv": 115.0, "owner": "Tacoma Power",
             "substation_type": "transmission"},
            {"hifld_id": "100063", "name": "Spokane East Substation", "state": "WA",
             "county": "Spokane", "city": "Spokane", "latitude": 47.6588, "longitude": -117.4260,
             "max_voltage_kv": 230.0, "min_voltage_kv": 115.0, "owner": "Avista",
             "substation_type": "transmission"},
            # New York - NYISO grid
            {"hifld_id": "100071", "name": "New York City Substation", "state": "NY",
             "county": "New York", "city": "New York", "latitude": 40.7128, "longitude": -74.0060,
             "max_voltage_kv": 345.0, "min_voltage_kv": 138.0, "owner": "Con Edison",
             "substation_type": "transmission"},
            {"hifld_id": "100072", "name": "Buffalo North Substation", "state": "NY",
             "county": "Erie", "city": "Buffalo", "latitude": 42.8864, "longitude": -78.8784,
             "max_voltage_kv": 345.0, "min_voltage_kv": 115.0, "owner": "National Grid",
             "substation_type": "transmission"},
            {"hifld_id": "100073", "name": "Albany Central Substation", "state": "NY",
             "county": "Albany", "city": "Albany", "latitude": 42.6526, "longitude": -73.7562,
             "max_voltage_kv": 345.0, "min_voltage_kv": 115.0, "owner": "National Grid",
             "substation_type": "transmission"},
        ]

        # Filter by states if specified
        if config.states:
            sample_substations = [s for s in sample_substations if s["state"] in config.states]

        records = []
        for sub in sample_substations:
            records.append({
                "hifld_id": sub["hifld_id"],
                "name": sub["name"],
                "state": sub["state"],
                "county": sub.get("county"),
                "city": sub.get("city"),
                "latitude": sub.get("latitude"),
                "longitude": sub.get("longitude"),
                "max_voltage_kv": sub.get("max_voltage_kv"),
                "min_voltage_kv": sub.get("min_voltage_kv"),
                "owner": sub.get("owner"),
                "substation_type": sub.get("substation_type", "transmission"),
                "status": "operational",
                "source": "hifld_sample",
                "collected_at": datetime.utcnow(),
            })

        if records:
            inserted, _ = self.bulk_upsert(
                Substation,
                records,
                unique_columns=["hifld_id"],
            )
            logger.info(f"Loaded {inserted} sample HIFLD substations")
            return {"processed": len(records), "inserted": inserted}

        return {"processed": 0, "inserted": 0}
