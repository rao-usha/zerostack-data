"""
EPA ACRES (Assessment, Cleanup and Redevelopment Exchange System) Collector.

Fetches brownfield site data from EPA's FRS (Facility Registry Service) API,
filtering to ACRES program facilities. Contains sites that have been assessed
and/or cleaned up through the Brownfields program.

Data source: https://data.epa.gov/efservice/FRS_PROGRAM_FACILITY
No API key required.

Note: The original ACRES_SITE_INFORMATION table was removed from the
Envirofacts API. Brownfield data is now available through the FRS API
by filtering on PGM_SYS_ACRNM=ACRES.
"""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import BrownfieldSite
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

# Paginate in batches of 500 rows (EPA API can 500 on larger pages)
PAGE_SIZE = 500

# States we care about for datacenter site selection
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
]


@register_collector(SiteIntelSource.EPA_ACRES)
class EPAACRESCollector(BaseCollector):
    """
    Collector for EPA ACRES brownfield site data.

    Fetches assessed/remediated brownfield sites from EPA's FRS API,
    filtering to the ACRES program. Returns XML which is parsed into
    brownfield_site records.
    """

    domain = SiteIntelDomain.RISK
    source = SiteIntelSource.EPA_ACRES

    default_timeout = 120.0
    rate_limit_delay = 0.5  # Gov API, moderate throughput

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://data.epa.gov/efservice"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/xml, text/xml, */*",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect EPA ACRES brownfield site data via FRS API."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            states = config.states if config.states else US_STATES
            logger.info(f"Collecting EPA ACRES data for {len(states)} states...")

            results = await self.gather_with_limit(
                [self._collect_state(s) for s in states], max_concurrent=4
            )

            for i, result in enumerate(results):
                state = states[i]
                if isinstance(result, Exception):
                    logger.warning(f"Failed to collect ACRES for {state}: {result}")
                    errors.append({"source": f"acres_{state}", "error": str(result)})
                    continue
                total_inserted += result.get("inserted", 0)
                total_processed += result.get("processed", 0)
                if result.get("error"):
                    errors.append(
                        {"source": f"acres_{state}", "error": result["error"]}
                    )

            status = CollectionStatus.SUCCESS
            if errors and total_inserted > 0:
                status = CollectionStatus.PARTIAL
            elif errors and total_inserted == 0:
                status = CollectionStatus.FAILED

            logger.info(
                f"ACRES collection complete: {total_processed} processed, "
                f"{total_inserted} inserted, {len(errors)} errors"
            )

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"ACRES collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_state(self, state: str) -> Dict[str, Any]:
        """Fetch brownfield data for a single state via FRS API with pagination."""
        client = await self.get_client()
        all_records: List[Dict[str, Any]] = []
        offset = 0
        page_errors = 0

        while True:
            try:
                await self.apply_rate_limit()
                end = offset + PAGE_SIZE - 1
                url = (
                    f"{self.base_url}/FRS_PROGRAM_FACILITY"
                    f"/PGM_SYS_ACRNM/ACRES/STATE_CODE/{state}"
                    f"/rows/{offset}:{end}"
                )

                response = await client.get(url)
                if response.status_code == 404:
                    break
                response.raise_for_status()

                xml_text = response.text
                if not xml_text or len(xml_text) < 50:
                    break

                records = self._parse_xml(xml_text, state)
                if not records:
                    break

                all_records.extend(records)
                logger.debug(
                    f"ACRES {state}: page at offset {offset} returned {len(records)} sites"
                )

                if len(records) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

            except Exception as e:
                page_errors += 1
                logger.warning(f"ACRES {state} page at offset {offset} failed: {e}")
                # Don't discard already-collected records; just stop paginating
                break

        logger.info(f"ACRES {state}: parsed {len(all_records)} brownfield sites")

        if all_records:
            try:
                inserted, updated = self.bulk_upsert(
                    BrownfieldSite,
                    all_records,
                    unique_columns=["acres_id"],
                    update_columns=[
                        "site_name", "address", "city", "state", "county",
                        "zip_code", "latitude", "longitude",
                        "source", "collected_at",
                    ],
                )
                result = {"processed": len(all_records), "inserted": inserted + updated}
                if page_errors:
                    result["error"] = f"{page_errors} page(s) failed during pagination"
                return result
            except Exception as e:
                logger.warning(f"ACRES {state} upsert failed: {e}")
                try:
                    self.db.rollback()
                except Exception:
                    pass
                return {"processed": len(all_records), "inserted": 0, "error": str(e)}

        error = f"{page_errors} page(s) failed" if page_errors else None
        return {"processed": 0, "inserted": 0, "error": error}

    def _parse_xml(self, xml_text: str, state: str) -> List[Dict[str, Any]]:
        """Parse FRS XML response into brownfield site records."""
        records = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.warning(f"Failed to parse XML for {state}: {e}")
            return records

        for facility in root.findall("frs_program_facility"):
            record = self._transform_facility(facility, state)
            if record:
                records.append(record)

        return records

    def _get_text(self, element: ET.Element, tag: str) -> Optional[str]:
        """Get text from an XML child element, returning None for 'None' or missing."""
        child = element.find(tag)
        if child is None or child.text is None or child.text.strip() == "None":
            return None
        return child.text.strip()

    def _transform_facility(
        self, facility: ET.Element, state: str
    ) -> Optional[Dict[str, Any]]:
        """Transform an FRS XML facility element to a brownfield_site record."""
        pgm_sys_id = self._get_text(facility, "PGM_SYS_ID")
        registry_id = self._get_text(facility, "REGISTRY_ID")

        # Use PGM_SYS_ID as primary identifier, fall back to REGISTRY_ID
        acres_id = pgm_sys_id or registry_id
        if not acres_id:
            return None

        site_name = self._get_text(facility, "PRIMARY_NAME")
        address = self._get_text(facility, "LOCATION_ADDRESS")
        city = self._get_text(facility, "CITY_NAME")
        county = self._get_text(facility, "COUNTY_NAME")
        zip_code = self._get_text(facility, "POSTAL_CODE")

        return {
            "acres_id": str(acres_id)[:50],
            "site_name": site_name[:500] if site_name else None,
            "address": address[:500] if address else None,
            "city": city[:100] if city else None,
            "state": state,
            "county": county[:100] if county else None,
            "zip_code": zip_code[:10] if zip_code else None,
            "latitude": None,  # FRS_PROGRAM_FACILITY doesn't include coords
            "longitude": None,
            "acreage": None,
            "cleanup_status": None,
            "contaminant_types": None,
            "land_use_prior": None,
            "land_use_current": None,
            "assessment_date": None,
            "cleanup_completion_date": None,
            "source": "epa_acres",
            "collected_at": datetime.utcnow(),
        }
