"""
FMCSA Motor Carrier Collector.

Fetches motor carrier data from FMCSA:
- Carrier registry (SAFER Web Services)
- SMS safety scores
- Inspection and crash data

Data sources:
- FMCSA SAFER Web Services (public)
- FMCSA SMS (Safety Measurement System)

No API key required - public data.
"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import MotorCarrier, CarrierSafety
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


@register_collector(SiteIntelSource.FMCSA)
class FMCSACollector(BaseCollector):
    """
    Collector for FMCSA motor carrier and safety data.

    Fetches:
    - Motor carrier registry (company info, DOT numbers)
    - Safety ratings and BASIC scores
    - Inspection and crash statistics
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.FMCSA

    # FMCSA API configuration
    default_timeout = 60.0
    rate_limit_delay = 0.5

    # FMCSA endpoints
    SAFER_BASE = "https://safer.fmcsa.dot.gov/query.asp"
    WEBSERVICES_BASE = "https://mobile.fmcsa.dot.gov/qc/services"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return self.WEBSERVICES_BASE

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute FMCSA data collection.

        Collects motor carrier registry and safety data.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            # Collect motor carriers
            logger.info("Collecting FMCSA motor carrier data...")
            carrier_result = await self._collect_carriers(config)
            total_inserted += carrier_result.get("inserted", 0)
            total_processed += carrier_result.get("processed", 0)
            if carrier_result.get("error"):
                errors.append({"source": "carriers", "error": carrier_result["error"]})

            # Collect safety data
            logger.info("Collecting FMCSA safety data...")
            safety_result = await self._collect_safety_data(config)
            total_inserted += safety_result.get("inserted", 0)
            total_processed += safety_result.get("processed", 0)
            if safety_result.get("error"):
                errors.append({"source": "safety", "error": safety_result["error"]})

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
            logger.error(f"FMCSA collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_carriers(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect motor carrier data.

        Uses FMCSA web services to fetch carrier information.
        For full data, would need FMCSA MCMIS data file access.
        """
        try:
            client = await self.get_client()
            all_carriers = []

            # FMCSA QC services provide carrier lookup
            # For bulk collection, we need to query by state
            states = config.states or [
                "CA",
                "TX",
                "FL",
                "IL",
                "PA",
                "OH",
                "NY",
                "GA",
                "NC",
                "MI",
            ]

            for state in states:
                await self.apply_rate_limit()

                try:
                    # Query carriers by state
                    # Note: FMCSA public web services are limited
                    # Full data requires MCMIS snapshot
                    carriers = await self._fetch_carriers_by_state(client, state)
                    all_carriers.extend(carriers)
                    logger.info(f"Fetched {len(carriers)} carriers for state {state}")

                except Exception as e:
                    logger.warning(f"Error fetching carriers for {state}: {e}")
                    continue

            # No sample fallback: fabricated rows must never be stored (PLAN_082).
            if not all_carriers:
                raise RuntimeError("No carriers returned from FMCSA; refusing to substitute sample data")

            # Transform and insert
            records = []
            for carrier in all_carriers:
                transformed = self._transform_carrier(carrier)
                if transformed:
                    records.append(transformed)

            if records:
                inserted, _ = self.bulk_upsert(
                    MotorCarrier,
                    records,
                    unique_columns=["dot_number"],
                    update_columns=[
                        "mc_number",
                        "legal_name",
                        "dba_name",
                        "physical_address",
                        "physical_city",
                        "physical_state",
                        "physical_zip",
                        "power_units",
                        "drivers",
                        "mcs150_date",
                        "mcs150_mileage",
                        "carrier_operation",
                        "cargo_carried",
                        "operation_classification",
                        "is_active",
                        "collected_at",
                    ],
                )
                return {"processed": len(all_carriers), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect carriers: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    async def _fetch_carriers_by_state(
        self,
        client,
        state: str,
    ) -> List[Dict[str, Any]]:
        """Fetch carriers for a specific state."""
        try:
            # FMCSA QC services endpoint
            params = {
                "searchType": "ANY",
                "stateAbbr": state,
                "size": 500,
            }

            response = await client.get(
                f"{self.WEBSERVICES_BASE}/carriers",
                params=params,
            )

            if response.status_code == 200:
                data = response.json()
                return data.get("content", [])

            return []

        except Exception as e:
            logger.warning(f"FMCSA API request failed for {state}: {e}")
            return []

    def _transform_carrier(self, carrier: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw carrier data to database format."""
        dot_number = carrier.get("dot_number") or carrier.get("dotNumber")
        if not dot_number:
            return None

        legal_name = (
            carrier.get("legal_name") or carrier.get("legalName") or carrier.get("name")
        )
        if not legal_name:
            return None

        # Parse MCS-150 date
        mcs150_date = carrier.get("mcs150_date") or carrier.get("mcs150FormDate")
        if isinstance(mcs150_date, str):
            try:
                mcs150_date = datetime.strptime(mcs150_date[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                mcs150_date = None

        return {
            "dot_number": str(dot_number),
            "mc_number": carrier.get("mc_number") or carrier.get("mcNumber"),
            "legal_name": legal_name,
            "dba_name": carrier.get("dba_name") or carrier.get("dbaName"),
            "physical_address": carrier.get("physical_address")
            or carrier.get("phyStreet"),
            "physical_city": carrier.get("physical_city") or carrier.get("phyCity"),
            "physical_state": carrier.get("physical_state") or carrier.get("phyState"),
            "physical_zip": carrier.get("physical_zip") or carrier.get("phyZip"),
            "mailing_address": carrier.get("mailing_address")
            or carrier.get("mailingStreet"),
            "mailing_city": carrier.get("mailing_city") or carrier.get("mailingCity"),
            "mailing_state": carrier.get("mailing_state")
            or carrier.get("mailingState"),
            "mailing_zip": carrier.get("mailing_zip") or carrier.get("mailingZip"),
            "telephone": carrier.get("telephone") or carrier.get("phone"),
            "email": carrier.get("email") or carrier.get("emailAddress"),
            "power_units": self._safe_int(
                carrier.get("power_units") or carrier.get("totalPowerUnits")
            ),
            "drivers": self._safe_int(
                carrier.get("drivers") or carrier.get("totalDrivers")
            ),
            "mcs150_date": mcs150_date,
            "mcs150_mileage": self._safe_int(
                carrier.get("mcs150_mileage") or carrier.get("mcs150Mileage")
            ),
            "carrier_operation": carrier.get("carrier_operation")
            or carrier.get("carrierOperation")
            or "interstate",
            "cargo_carried": carrier.get("cargo_carried")
            or carrier.get("cargoCarried"),
            "operation_classification": carrier.get("operation_classification")
            or carrier.get("operationClassification"),
            "is_active": carrier.get("is_active", True),
            "out_of_service_date": None,
            "source": "fmcsa",
            "collected_at": datetime.utcnow(),
        }

    async def _collect_safety_data(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect safety data for carriers.

        Uses FMCSA SMS data for safety scores and inspection history.
        """
        try:
            # Get DOT numbers from recently collected carriers

            dot_numbers = (
                self.db.query(MotorCarrier.dot_number)
                .order_by(MotorCarrier.power_units.desc().nullslast())
                .limit(100)
                .all()
            )

            dot_list = [d[0] for d in dot_numbers]

            all_safety = []

            for dot_number in dot_list:
                await self.apply_rate_limit()

                try:
                    safety_data = await self._fetch_safety_for_carrier(dot_number)
                    if safety_data:
                        all_safety.append(safety_data)

                except Exception as e:
                    logger.warning(f"Error fetching safety for DOT {dot_number}: {e}")
                    continue

            # No sample fallback: fabricated rows must never be stored (PLAN_082).
            if not all_safety and dot_list:
                raise RuntimeError("No safety data returned from FMCSA; refusing to substitute sample data")

            # Transform and insert
            records = []
            for safety in all_safety:
                transformed = self._transform_safety(safety)
                if transformed:
                    records.append(transformed)

            if records:
                inserted, _ = self.bulk_upsert(
                    CarrierSafety,
                    records,
                    unique_columns=["dot_number", "inspection_date"],
                    update_columns=[
                        "safety_rating",
                        "rating_date",
                        "unsafe_driving_score",
                        "hours_of_service_score",
                        "driver_fitness_score",
                        "controlled_substances_score",
                        "vehicle_maintenance_score",
                        "hazmat_compliance_score",
                        "crash_indicator_score",
                        "vehicle_oos_rate",
                        "driver_oos_rate",
                        "total_inspections",
                        "total_violations",
                        "total_crashes",
                        "fatal_crashes",
                        "injury_crashes",
                        "tow_crashes",
                        "collected_at",
                    ],
                )
                return {"processed": len(all_safety), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect safety data: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    async def _fetch_safety_for_carrier(
        self, dot_number: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch safety data for a specific carrier."""
        try:
            client = await self.get_client()

            response = await client.get(
                f"{self.WEBSERVICES_BASE}/carriers/{dot_number}/safetyRating",
            )

            if response.status_code == 200:
                return response.json()

            return None

        except Exception as e:
            logger.warning(f"FMCSA safety API request failed for {dot_number}: {e}")
            return None

    def _transform_safety(self, safety: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw safety data to database format."""
        dot_number = safety.get("dot_number") or safety.get("dotNumber")
        if not dot_number:
            return None

        # Parse dates
        rating_date = safety.get("rating_date") or safety.get("ratingDate")
        if isinstance(rating_date, str):
            try:
                rating_date = datetime.strptime(rating_date[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                rating_date = None
        elif not isinstance(rating_date, date):
            rating_date = None

        inspection_date = (
            safety.get("inspection_date")
            or safety.get("inspectionDate")
            or date.today()
        )
        if isinstance(inspection_date, str):
            try:
                inspection_date = datetime.strptime(
                    inspection_date[:10], "%Y-%m-%d"
                ).date()
            except (ValueError, TypeError):
                inspection_date = date.today()

        return {
            "dot_number": str(dot_number),
            "safety_rating": safety.get("safety_rating") or safety.get("safetyRating"),
            "rating_date": rating_date,
            "unsafe_driving_score": self._safe_float(
                safety.get("unsafe_driving_score") or safety.get("unsafeDriving")
            ),
            "hours_of_service_score": self._safe_float(
                safety.get("hours_of_service_score") or safety.get("hosCompliance")
            ),
            "driver_fitness_score": self._safe_float(
                safety.get("driver_fitness_score") or safety.get("driverFitness")
            ),
            "controlled_substances_score": self._safe_float(
                safety.get("controlled_substances_score")
                or safety.get("controlledSubstances")
            ),
            "vehicle_maintenance_score": self._safe_float(
                safety.get("vehicle_maintenance_score")
                or safety.get("vehicleMaintenance")
            ),
            "hazmat_compliance_score": self._safe_float(
                safety.get("hazmat_compliance_score") or safety.get("hazmatCompliance")
            ),
            "crash_indicator_score": self._safe_float(
                safety.get("crash_indicator_score") or safety.get("crashIndicator")
            ),
            "vehicle_oos_rate": self._safe_float(
                safety.get("vehicle_oos_rate") or safety.get("vehicleOosRate")
            ),
            "driver_oos_rate": self._safe_float(
                safety.get("driver_oos_rate") or safety.get("driverOosRate")
            ),
            "total_inspections": self._safe_int(
                safety.get("total_inspections") or safety.get("totalInspections")
            ),
            "total_violations": self._safe_int(
                safety.get("total_violations") or safety.get("totalViolations")
            ),
            "total_crashes": self._safe_int(
                safety.get("total_crashes") or safety.get("totalCrashes")
            ),
            "fatal_crashes": self._safe_int(
                safety.get("fatal_crashes") or safety.get("fatalCrashes")
            ),
            "injury_crashes": self._safe_int(
                safety.get("injury_crashes") or safety.get("injuryCrashes")
            ),
            "tow_crashes": self._safe_int(
                safety.get("tow_crashes") or safety.get("towCrashes")
            ),
            "inspection_date": inspection_date,
            "source": "fmcsa",
            "collected_at": datetime.utcnow(),
        }

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
