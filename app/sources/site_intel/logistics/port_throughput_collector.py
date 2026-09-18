"""
Port Throughput Collector.

Fetches port throughput and performance data from:
- BTS Port Performance Statistics
- USACE Waterborne Commerce Statistics Center (WCSC)
- Individual port authority data

Data sources:
- BTS National Transportation Statistics
- USACE WCSC tonnage data

No API key required - public data.
"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import PortThroughputMonthly
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


# Note: Using BTS source since we're extending BTS transport data
@register_collector(SiteIntelSource.USACE)
class PortThroughputCollector(BaseCollector):
    """
    Collector for port throughput and performance metrics.

    Fetches:
    - Monthly TEU (container) throughput
    - Vessel calls and turnaround times
    - Tonnage data by port
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.USACE

    # API configuration
    default_timeout = 120.0
    rate_limit_delay = 0.5

    # USACE WCSC endpoints
    WCSC_BASE = "https://publibrary.planusace.us"

    # BTS port performance endpoint
    BTS_PORT_BASE = "https://data.bts.gov/resource"

    # Major US container ports (UN/LOCODE)
    MAJOR_PORTS = {
        "USLAX": {"name": "Los Angeles", "state": "CA"},
        "USLGB": {"name": "Long Beach", "state": "CA"},
        "USNYC": {"name": "New York/New Jersey", "state": "NY"},
        "USSAV": {"name": "Savannah", "state": "GA"},
        "USHOU": {"name": "Houston", "state": "TX"},
        "USSEA": {"name": "Seattle", "state": "WA"},
        "USTIW": {"name": "Tacoma", "state": "WA"},
        "USNWK": {"name": "Newark", "state": "NJ"},
        "USORF": {"name": "Norfolk", "state": "VA"},
        "USCHA": {"name": "Charleston", "state": "SC"},
        "USJAX": {"name": "Jacksonville", "state": "FL"},
        "USBAL": {"name": "Baltimore", "state": "MD"},
        "USOAK": {"name": "Oakland", "state": "CA"},
        "USMIA": {"name": "Miami", "state": "FL"},
        "USPHF": {"name": "Philadelphia", "state": "PA"},
        "USMOB": {"name": "Mobile", "state": "AL"},
        "USNOL": {"name": "New Orleans", "state": "LA"},
        "USBOS": {"name": "Boston", "state": "MA"},
        "USPDX": {"name": "Portland", "state": "OR"},
        "USDET": {"name": "Detroit", "state": "MI"},
    }

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return self.BTS_PORT_BASE

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute port throughput data collection.

        Collects monthly throughput metrics for major US ports.
        """
        try:
            logger.info("Collecting port throughput data...")

            all_throughput = []

            # Collect from BTS/WCSC
            throughput_result = await self._collect_throughput(config)
            all_throughput.extend(throughput_result.get("records", []))

            # No sample/random fallback: fabricated rows must never be stored (PLAN_082).
            if not all_throughput:
                raise RuntimeError("No data returned from source; refusing to substitute sample data")

            # Transform and insert records
            records = []
            for record in all_throughput:
                transformed = self._transform_throughput(record)
                if transformed:
                    records.append(transformed)

            logger.info(f"Transformed {len(records)} port throughput records")

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    PortThroughputMonthly,
                    records,
                    unique_columns=["port_code", "period_year", "period_month"],
                    update_columns=[
                        "port_name",
                        "teu_loaded_import",
                        "teu_loaded_export",
                        "teu_empty_import",
                        "teu_empty_export",
                        "teu_total",
                        "container_vessel_calls",
                        "avg_berthing_hours",
                        "avg_vessel_turnaround_hours",
                        "tonnage_import",
                        "tonnage_export",
                        "tonnage_total",
                        "bulk_tonnage",
                        "breakbulk_tonnage",
                        "roro_units",
                        "source",
                        "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(all_throughput),
                    processed=len(all_throughput),
                    inserted=inserted,
                    sample=records[:3] if records else None,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(all_throughput),
                processed=len(all_throughput),
                inserted=0,
            )

        except Exception as e:
            logger.error(f"Port throughput collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_throughput(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect port throughput data from BTS and USACE.
        """
        try:
            client = await self.get_client()
            all_records = []

            # Try BTS port performance data
            await self.apply_rate_limit()

            try:
                # BTS has port performance datasets
                # Example: Port Performance Freight Statistics dataset
                response = await client.get(
                    f"{self.BTS_PORT_BASE}/port-performance.json",
                    params={
                        "$limit": 5000,
                        "$order": "year DESC, month DESC",
                    },
                )

                if response.status_code == 200:
                    data = response.json()
                    all_records.extend(data)
                    logger.info(f"Fetched {len(data)} records from BTS port data")

            except Exception as e:
                logger.warning(f"Could not fetch from BTS: {e}")

            # Try USACE WCSC data
            try:
                await self.apply_rate_limit()

                # USACE provides waterborne commerce data
                response = await client.get(
                    f"{self.WCSC_BASE}/wcsc/data/ports",
                    params={"format": "json"},
                )

                if response.status_code == 200:
                    data = response.json()
                    wcsc_records = (
                        data.get("data", []) if isinstance(data, dict) else data
                    )
                    all_records.extend(wcsc_records)
                    logger.info(f"Fetched {len(wcsc_records)} records from USACE WCSC")

            except Exception as e:
                logger.warning(f"Could not fetch from USACE: {e}")

            return {"records": all_records}

        except Exception as e:
            logger.error(f"Failed to collect throughput: {e}", exc_info=True)
            return {"records": [], "error": str(e)}

    def _transform_throughput(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw throughput data to database format."""
        port_code = record.get("port_code") or record.get("portCode")
        if not port_code:
            return None

        period_year = self._safe_int(record.get("period_year") or record.get("year"))
        period_month = self._safe_int(record.get("period_month") or record.get("month"))

        if not period_year or not period_month:
            return None

        port_info = self.MAJOR_PORTS.get(port_code, {})

        return {
            "port_code": port_code,
            "port_name": record.get("port_name") or port_info.get("name"),
            "period_year": period_year,
            "period_month": period_month,
            "teu_loaded_import": self._safe_int(record.get("teu_loaded_import")),
            "teu_loaded_export": self._safe_int(record.get("teu_loaded_export")),
            "teu_empty_import": self._safe_int(record.get("teu_empty_import")),
            "teu_empty_export": self._safe_int(record.get("teu_empty_export")),
            "teu_total": self._safe_int(record.get("teu_total")),
            "container_vessel_calls": self._safe_int(
                record.get("container_vessel_calls")
            ),
            "avg_berthing_hours": self._safe_float(record.get("avg_berthing_hours")),
            "avg_vessel_turnaround_hours": self._safe_float(
                record.get("avg_vessel_turnaround_hours")
            ),
            "tonnage_import": self._safe_int(record.get("tonnage_import")),
            "tonnage_export": self._safe_int(record.get("tonnage_export")),
            "tonnage_total": self._safe_int(record.get("tonnage_total")),
            "bulk_tonnage": self._safe_int(record.get("bulk_tonnage")),
            "breakbulk_tonnage": self._safe_int(record.get("breakbulk_tonnage")),
            "roro_units": self._safe_int(record.get("roro_units")),
            "source": "bts_usace",
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
