"""
3PL Company Enrichment Collector - Phase 2: SEC EDGAR.

Pulls HQ address and employee count from SEC EDGAR for ~18 publicly traded 3PLs.
Uses the SEC company submissions and XBRL company facts APIs.

Rate limit: 10 req/sec per SEC fair access policy.
User-Agent must include company/contact info per SEC guidelines.
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import ThreePLCompany
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)

# Map 3PL company names (as in DB) to SEC CIK numbers
# CIK numbers are zero-padded to 10 digits for the API
PUBLIC_3PL_CIK_MAP = {
    "C.H. Robinson Worldwide": {"cik": "1043277", "ticker": "CHRW"},
    "XPO": {"cik": "1166003", "ticker": "XPO"},
    "GXO Logistics": {"cik": "1852633", "ticker": "GXO"},
    "J.B. Hunt Transport Services": {"cik": "728535", "ticker": "JBHT"},
    "Ryder System": {"cik": "85961", "ticker": "R"},
    "Schneider National": {"cik": "1692819", "ticker": "SNDR"},
    "Werner Enterprises": {"cik": "793074", "ticker": "WERN"},
    "Landstar System": {"cik": "853816", "ticker": "LSTR"},
    "Knight-Swift Transportation": {"cik": "1492691", "ticker": "KNX"},
    "Old Dominion Freight Line": {"cik": "878927", "ticker": "ODFL"},
    "Saia": {"cik": "866374", "ticker": "SAIA"},
    "Hub Group": {"cik": "940390", "ticker": "HUBG"},
    "Forward Air": {"cik": "912728", "ticker": "FWRD"},
    "Echo Global Logistics": {"cik": "1426945", "ticker": "ECHO"},
    "Radiant Logistics": {"cik": "1420302", "ticker": "RLGT"},
    "ArcBest": {"cik": "817655", "ticker": "ARCB"},
    "Heartland Express": {"cik": "799233", "ticker": "HTLD"},
    "RXO": {"cik": "1900907", "ticker": "RXO"},
}


@register_collector(SiteIntelSource.THREE_PL_SEC)
class ThreePLSECEnrichmentCollector(BaseCollector):
    """
    Enrichment collector that pulls authoritative data from SEC EDGAR.

    For each public 3PL company:
    1. Fetches company submission data for HQ address
    2. Fetches XBRL company facts for employee count and revenue
    3. Uses null_preserving_upsert to enrich existing records
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.THREE_PL_SEC

    rate_limit_delay = 0.12  # ~8 req/sec, under SEC 10 req/sec limit

    def get_default_base_url(self) -> str:
        return "https://data.sec.gov"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0 (research@nexdata.io)",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Fetch SEC EDGAR data for public 3PL companies."""
        try:
            logger.info(f"Enriching {len(PUBLIC_3PL_CIK_MAP)} public 3PL companies from SEC EDGAR...")

            records = []
            errors = []
            processed = 0

            for company_name, sec_info in PUBLIC_3PL_CIK_MAP.items():
                processed += 1
                try:
                    record = await self._fetch_company_data(company_name, sec_info)
                    if record:
                        records.append(record)
                        logger.debug(f"Enriched {company_name} from SEC ({sec_info['ticker']})")

                    self.update_progress(
                        processed=processed,
                        total=len(PUBLIC_3PL_CIK_MAP),
                        current_step=f"Fetching {company_name}",
                    )

                except Exception as e:
                    logger.warning(f"Failed to fetch SEC data for {company_name}: {e}")
                    errors.append({
                        "company": company_name,
                        "ticker": sec_info.get("ticker"),
                        "error": str(e),
                    })

            logger.info(f"Fetched SEC data for {len(records)}/{len(PUBLIC_3PL_CIK_MAP)} companies")

            if records:
                inserted, updated = self.null_preserving_upsert(
                    ThreePLCompany,
                    records,
                    unique_columns=["company_name"],
                    update_columns=[
                        "headquarters_city", "headquarters_state",
                        "employee_count",
                        "source", "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL,
                    total=len(PUBLIC_3PL_CIK_MAP),
                    processed=processed,
                    inserted=inserted,
                    updated=updated,
                    failed=len(errors),
                    errors=errors if errors else None,
                    sample=records[:3],
                )

            return self.create_result(
                status=CollectionStatus.FAILED if errors else CollectionStatus.SUCCESS,
                total=len(PUBLIC_3PL_CIK_MAP),
                processed=processed,
                failed=len(errors),
                errors=errors if errors else None,
                error_message="No SEC data retrieved" if not records else None,
            )

        except Exception as e:
            logger.error(f"SEC enrichment failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )
        finally:
            await self.close_client()

    async def _fetch_company_data(
        self, company_name: str, sec_info: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """Fetch company data from SEC EDGAR APIs."""
        cik = sec_info["cik"].zfill(10)
        record = {"company_name": company_name}

        # 1. Fetch submissions for HQ address
        try:
            await self.apply_rate_limit()
            submissions = await self.fetch_json(f"/submissions/CIK{cik}.json")

            addresses = submissions.get("addresses", {})
            business_addr = addresses.get("business", {})

            city = business_addr.get("city")
            state = business_addr.get("stateOrCountry")

            if city:
                record["headquarters_city"] = city.title()
            if state and len(state) == 2:
                record["headquarters_state"] = state.upper()

        except Exception as e:
            logger.debug(f"Submissions fetch failed for {company_name}: {e}")

        # 2. Fetch XBRL facts for employee count
        try:
            await self.apply_rate_limit()
            facts = await self.fetch_json(f"/api/xbrl/companyfacts/CIK{cik}.json")

            employee_count = self._extract_employee_count(facts)
            if employee_count:
                record["employee_count"] = employee_count

        except Exception as e:
            logger.debug(f"XBRL facts fetch failed for {company_name}: {e}")

        # Only return if we got at least some data beyond the name
        if len(record) > 1:
            record["source"] = "sec_edgar"
            record["collected_at"] = datetime.utcnow()
            return record

        return None

    def _extract_employee_count(self, facts: Dict[str, Any]) -> Optional[int]:
        """Extract the most recent employee count from XBRL facts."""
        try:
            dei_facts = facts.get("facts", {}).get("dei", {})
            employee_fact = dei_facts.get("EntityNumberOfEmployees", {})
            units = employee_fact.get("units", {})

            # Employee count is typically reported as a pure number
            values = units.get("pure", []) or units.get("number", [])

            if not values:
                return None

            # Get the most recent 10-K filing (annual report)
            annual_values = [
                v for v in values
                if v.get("form") in ("10-K", "10-K/A")
            ]

            if annual_values:
                # Sort by end date descending
                annual_values.sort(key=lambda v: v.get("end", ""), reverse=True)
                val = annual_values[0].get("val")
                if val and isinstance(val, (int, float)) and val > 0:
                    return int(val)

            # Fallback: any form with employee data
            if values:
                values.sort(key=lambda v: v.get("end", ""), reverse=True)
                val = values[0].get("val")
                if val and isinstance(val, (int, float)) and val > 0:
                    return int(val)

        except Exception as e:
            logger.debug(f"Employee count extraction failed: {e}")

        return None
