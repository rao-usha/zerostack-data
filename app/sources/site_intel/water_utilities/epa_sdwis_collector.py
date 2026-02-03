"""
EPA SDWIS (Safe Drinking Water Information System) Collector.

Fetches public water system data from EPA Envirofacts:
- Public water system information
- Population served and service connections
- Water source types
- Compliance violations

Data source: https://enviro.epa.gov/envirofacts/
No API key required - public access.
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import PublicWaterSystem, WaterSystemViolation
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)


# PWS type codes
PWS_TYPE_MAP = {
    "CWS": "community",  # Community Water System
    "TNCWS": "transient",  # Transient Non-Community
    "NTNCWS": "non_transient",  # Non-Transient Non-Community
}

# Primary source codes
SOURCE_TYPE_MAP = {
    "GW": "groundwater",
    "GU": "purchased_ground",
    "SW": "surface_water",
    "SWP": "purchased_surface",
}


@register_collector(SiteIntelSource.EPA_SDWIS)
class EPASDWISCollector(BaseCollector):
    """
    Collector for EPA SDWIS public water system data.

    Fetches:
    - Public water system registry
    - Population served and infrastructure
    - Water quality violations
    """

    domain = SiteIntelDomain.WATER_UTILITIES
    source = SiteIntelSource.EPA_SDWIS

    default_timeout = 120.0
    rate_limit_delay = 1.0  # Be respectful to EPA servers

    # EPA Envirofacts SDWIS API
    ENVIROFACTS_URL = "https://enviro.epa.gov/enviro/efservice"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://enviro.epa.gov/enviro/efservice"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Execute EPA SDWIS data collection."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting EPA SDWIS public water system data...")

            # Collect water systems
            pws_result = await self._collect_water_systems(config)
            total_inserted += pws_result.get("inserted", 0)
            total_processed += pws_result.get("processed", 0)
            if pws_result.get("error"):
                errors.append({"source": "water_systems", "error": pws_result["error"]})

            # Collect violations (if we got water systems)
            if total_processed > 0:
                violation_result = await self._collect_violations(config)
                total_inserted += violation_result.get("inserted", 0)
                total_processed += violation_result.get("processed", 0)
                if violation_result.get("error"):
                    errors.append({"source": "violations", "error": violation_result["error"]})

            status = CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"EPA SDWIS collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_water_systems(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect public water systems from EPA SDWIS."""
        try:
            client = await self.get_client()
            all_systems = []

            # Determine states to query
            states = config.states if config.states else ["TX", "CA", "OH", "PA", "IL"]

            for state in states:
                await self.apply_rate_limit()

                # EPA Envirofacts URL format: /table/column/value/rows/start:end/format
                url = f"{self.ENVIROFACTS_URL}/SDWIS_WATER_SYSTEM/PRIMACY_AGENCY_CODE/{state}/JSON"

                try:
                    response = await client.get(url)

                    if response.status_code != 200:
                        logger.warning(f"EPA API returned {response.status_code} for {state}")
                        continue

                    data = response.json()
                    if data:
                        all_systems.extend(data)
                        logger.info(f"Retrieved {len(data)} water systems for {state}")

                except Exception as e:
                    logger.warning(f"Failed to fetch water systems for {state}: {e}")
                    continue

            if not all_systems:
                return {"processed": 0, "inserted": 0}

            # Transform records
            records = []
            for system in all_systems:
                transformed = self._transform_water_system(system)
                if transformed:
                    # Filter by minimum population if specified
                    if config.options and config.options.get("min_population"):
                        if (transformed.get("population_served") or 0) < config.options["min_population"]:
                            continue
                    records.append(transformed)

            if records:
                inserted, _ = self.bulk_upsert(
                    PublicWaterSystem,
                    records,
                    unique_columns=["pwsid"],
                    update_columns=[
                        "pws_name", "pws_type", "state", "county", "city", "zip_code",
                        "population_served", "service_connections", "service_area_type",
                        "primary_source_code", "primary_source_name", "source_water_protection",
                        "is_active", "compliance_status", "source", "collected_at"
                    ],
                )
                logger.info(f"Inserted/updated {inserted} public water systems")
                return {"processed": len(all_systems), "inserted": inserted}

            return {"processed": len(all_systems), "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect water systems: {e}")
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_water_system(self, system: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform EPA SDWIS water system to database format."""
        pwsid = system.get("PWSID")
        if not pwsid:
            return None

        # Map PWS type
        pws_type_code = system.get("PWS_TYPE_CODE", "")
        pws_type = PWS_TYPE_MAP.get(pws_type_code, pws_type_code.lower() if pws_type_code else None)

        # Map source type
        source_code = system.get("PRIMARY_SOURCE_CODE", "")
        source_name = SOURCE_TYPE_MAP.get(source_code, source_code)

        # Activity status
        activity_code = system.get("PWS_ACTIVITY_CODE", "")
        is_active = activity_code.upper() == "A"

        return {
            "pwsid": pwsid,
            "pws_name": system.get("PWS_NAME"),
            "pws_type": pws_type,
            "state": system.get("PRIMACY_AGENCY_CODE"),
            "county": system.get("COUNTY_SERVED"),
            "city": system.get("CITY_SERVED"),
            "zip_code": system.get("ZIP_CODE"),
            "population_served": self._parse_int(system.get("POPULATION_SERVED_COUNT")),
            "service_connections": self._parse_int(system.get("SERVICE_CONNECTIONS_COUNT")),
            "service_area_type": system.get("SERVICE_AREA_TYPE_CODE"),
            "primary_source_code": source_code,
            "primary_source_name": source_name,
            "source_water_protection": system.get("SOURCE_WATER_PROTECTION_CODE") == "Y",
            "is_active": is_active,
            "compliance_status": "compliant" if is_active else "inactive",
            "source": "epa_sdwis",
            "collected_at": datetime.utcnow(),
        }

    async def _collect_violations(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect water system violations from EPA SDWIS."""
        try:
            client = await self.get_client()
            all_violations = []

            states = config.states if config.states else ["TX", "CA", "OH", "PA", "IL"]

            for state in states:
                await self.apply_rate_limit()

                url = f"{self.ENVIROFACTS_URL}/SDWIS_VIOLATION/PRIMACY_AGENCY_CODE/{state}/JSON"

                try:
                    response = await client.get(url)

                    if response.status_code != 200:
                        continue

                    data = response.json()
                    if data:
                        all_violations.extend(data[:1000])  # Limit per state
                        logger.info(f"Retrieved {min(len(data), 1000)} violations for {state}")

                except Exception as e:
                    logger.warning(f"Failed to fetch violations for {state}: {e}")
                    continue

            if not all_violations:
                return {"processed": 0, "inserted": 0}

            records = []
            for violation in all_violations:
                transformed = self._transform_violation(violation)
                if transformed:
                    records.append(transformed)

            if records:
                inserted, _ = self.bulk_upsert(
                    WaterSystemViolation,
                    records,
                    unique_columns=["violation_id"],
                    update_columns=[
                        "pwsid", "violation_type", "contaminant_code", "contaminant_name",
                        "contaminant_group", "violation_date", "compliance_period",
                        "is_health_based", "severity_level", "enforcement_action",
                        "returned_to_compliance", "returned_to_compliance_date",
                        "source", "collected_at"
                    ],
                )
                logger.info(f"Inserted/updated {inserted} violations")
                return {"processed": len(all_violations), "inserted": inserted}

            return {"processed": len(all_violations), "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect violations: {e}")
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_violation(self, violation: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform EPA SDWIS violation to database format."""
        pwsid = violation.get("PWSID")
        violation_id = violation.get("VIOLATION_ID")

        if not pwsid or not violation_id:
            return None

        # Parse violation date
        viol_date = violation.get("COMPL_PER_BEGIN_DATE")
        violation_date = None
        if viol_date:
            try:
                violation_date = datetime.strptime(str(viol_date)[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                violation_date = datetime.utcnow().date()

        # Determine if health-based
        is_health_based = violation.get("IS_HEALTH_BASED_IND", "N") == "Y"

        return {
            "pwsid": pwsid,
            "violation_id": f"{pwsid}_{violation_id}",
            "violation_type": violation.get("VIOLATION_CATEGORY_CODE"),
            "contaminant_code": violation.get("CONTAMINANT_CODE"),
            "contaminant_name": violation.get("CONTAMINANT_NAME"),
            "contaminant_group": violation.get("CONTAMINANT_GROUP_CODE"),
            "violation_date": violation_date,
            "compliance_period": violation.get("COMPLIANCE_PERIOD"),
            "is_health_based": is_health_based,
            "severity_level": violation.get("SEVERITY_IND_CNT"),
            "enforcement_action": violation.get("ENFORCEMENT_ACTION_TYPE"),
            "returned_to_compliance": violation.get("RTC_DATE") is not None,
            "returned_to_compliance_date": self._parse_date(violation.get("RTC_DATE")),
            "source": "epa_sdwis",
            "collected_at": datetime.utcnow(),
        }

    def _parse_int(self, value: Any) -> Optional[int]:
        """Parse integer value."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _parse_date(self, value: Any) -> Optional[datetime]:
        """Parse date value."""
        if value is None or value == "":
            return None
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
