"""
EPA Envirofacts Collector — Toxic Release Inventory (TRI) Facilities.

Fetches industrial facility data from EPA Envirofacts:
- TRI_FACILITY: Active industrial facilities that handle toxic chemicals
- Includes location, parent company, industry sector

Data source: https://data.epa.gov/efservice/
No API key required — public access.

TRI is preferred over FRS (Facility Registry Service) because:
- FRS is a meta-registry with 100K+ facilities per state, many lacking coordinates
- TRI targets active industrial facilities relevant to environmental due diligence
- Manageable volume (~2-5K facilities per major state)
"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import EnvironmentalFacility
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


@register_collector(SiteIntelSource.EPA_ENVIROFACTS)
class EPAEnvirofactsCollector(BaseCollector):
    """
    Collector for EPA Envirofacts TRI facility data.

    Fetches Toxic Release Inventory facilities and maps them
    to the environmental_facility table for proximity queries.
    """

    domain = SiteIntelDomain.RISK
    source = SiteIntelSource.EPA_ENVIROFACTS

    default_timeout = 120.0
    rate_limit_delay = 1.0

    ENVIROFACTS_URL = "https://data.epa.gov/efservice"
    PAGE_SIZE = 5000

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return self.ENVIROFACTS_URL

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Execute EPA Envirofacts TRI facility collection."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting EPA Envirofacts TRI facility data...")

            result = await self._collect_tri_facilities(config)
            total_inserted += result.get("inserted", 0)
            total_processed += result.get("processed", 0)
            if result.get("error"):
                errors.append({"source": "tri_facilities", "error": result["error"]})

            status = CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"EPA Envirofacts collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_tri_facilities(self, config: CollectionConfig) -> Dict[str, Any]:
        """Collect TRI facilities from EPA Envirofacts with pagination."""
        try:
            client = await self.get_client()
            all_facilities = []

            states = config.states if config.states else US_STATES

            for state_idx, state in enumerate(states):
                offset = 0
                state_count = 0

                while True:
                    await self.apply_rate_limit()

                    # TRI uses STATE_ABBR (not STATE_CODE)
                    url = (
                        f"{self.ENVIROFACTS_URL}/TRI_FACILITY/STATE_ABBR/{state}"
                        f"/rows/{offset}:{offset + self.PAGE_SIZE}/JSON"
                    )

                    try:
                        response = await client.get(url)

                        if response.status_code != 200:
                            logger.warning(
                                f"EPA Envirofacts returned {response.status_code} for TRI/{state}"
                            )
                            break

                        data = response.json()
                        if not data:
                            break

                        all_facilities.extend(data)
                        state_count += len(data)

                        if len(data) < self.PAGE_SIZE:
                            break
                        offset += self.PAGE_SIZE

                    except Exception as e:
                        logger.warning(
                            f"Failed to fetch TRI facilities for {state} at offset {offset}: {e}"
                        )
                        break

                if state_count > 0:
                    logger.info(f"Retrieved {state_count} TRI facilities for {state}")

                self.update_progress(
                    processed=state_idx + 1,
                    total=len(states),
                    current_step=f"Fetched TRI facilities for {state} ({state_count} records)",
                )

            if not all_facilities:
                return {"processed": 0, "inserted": 0}

            # Transform, filter, and dedup by epa_id (API may return duplicates across pages)
            seen_ids = {}
            for facility in all_facilities:
                transformed = self._transform_tri_facility(facility)
                if transformed:
                    epa_id = transformed["epa_id"]
                    seen_ids[epa_id] = transformed  # last wins
            records = list(seen_ids.values())

            if records:
                inserted, _ = self.bulk_upsert(
                    EnvironmentalFacility,
                    records,
                    unique_columns=["epa_id"],
                    update_columns=[
                        "facility_name", "facility_type", "address", "city",
                        "state", "zip", "latitude", "longitude", "permits",
                        "is_superfund", "is_brownfield", "source", "collected_at",
                    ],
                )
                logger.info(f"Inserted/updated {inserted} environmental facilities from TRI")
                return {"processed": len(all_facilities), "inserted": inserted}

            return {"processed": len(all_facilities), "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect TRI facilities: {e}", exc_info=True)
            try:
                self.db.rollback()
            except Exception:
                pass
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_tri_facility(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform a TRI_FACILITY record to environmental_facility schema."""
        # API returns lowercase field names
        facility_id = record.get("tri_facility_id")
        if not facility_id:
            return None

        # Skip closed facilities
        if str(record.get("closed_ind", "")).upper() == "Y":
            return None

        facility_name = record.get("facility_name")
        if not facility_name:
            return None

        lat = self._parse_float(record.get("latitude"))
        lng = self._parse_float(record.get("longitude"))

        return {
            "epa_id": str(facility_id).strip(),
            "facility_name": facility_name,
            "facility_type": record.get("industry_sector_code"),
            "address": record.get("street_address"),
            "city": record.get("city_name"),
            "state": record.get("state_abbr"),
            "zip": record.get("zip_code"),
            "latitude": lat,
            "longitude": lng,
            "permits": ["TRI"],
            "violations_5yr": None,
            "is_superfund": False,
            "is_brownfield": False,
            "source": "epa_tri",
            "collected_at": datetime.utcnow(),
        }

    def _parse_float(self, value) -> Optional[float]:
        """Safely parse a float value."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
