"""
3PL Company Enrichment Collector - Phase 1: Curated Seed Data.

Loads curated enrichment data for 100 Transport Topics Top 100 3PL companies.
Uses null-preserving upsert so existing data (revenue, rank) is not overwritten.
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import ThreePLCompany
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector
from app.sources.site_intel.logistics.three_pl_seed_data import THREE_PL_SEED_DATA

logger = logging.getLogger(__name__)


@register_collector(SiteIntelSource.THREE_PL_ENRICHMENT)
class ThreePLEnrichmentCollector(BaseCollector):
    """
    Enrichment collector that loads curated seed data for 3PL companies.

    Uses null_preserving_upsert so that:
    - Existing revenue/rank data from Transport Topics scraping is preserved
    - New fields (HQ, employees, services, etc.) are populated
    - Null seed values don't overwrite existing non-null data
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.THREE_PL_ENRICHMENT

    def get_default_base_url(self) -> str:
        return ""  # No external API needed

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Load curated seed data and enrich existing 3PL records."""
        try:
            logger.info(f"Loading curated seed data for {len(THREE_PL_SEED_DATA)} 3PL companies...")

            records = []
            errors = []

            for entry in THREE_PL_SEED_DATA:
                try:
                    record = self._transform_seed_entry(entry)
                    if record:
                        records.append(record)
                except Exception as e:
                    errors.append({
                        "company": entry.get("company_name", "unknown"),
                        "error": str(e),
                    })

            logger.info(f"Prepared {len(records)} enrichment records, {len(errors)} errors")

            if records:
                inserted, updated = self.null_preserving_upsert(
                    ThreePLCompany,
                    records,
                    unique_columns=["company_name"],
                    update_columns=[
                        "headquarters_city", "headquarters_state", "website",
                        "employee_count", "facility_count",
                        "services", "industries_served",
                        "has_cold_chain", "has_hazmat", "has_ecommerce_fulfillment",
                        "has_cross_dock", "is_asset_based", "is_non_asset",
                        "regions_served", "states_coverage",
                        "source", "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(THREE_PL_SEED_DATA),
                    processed=len(records),
                    inserted=inserted,
                    updated=updated,
                    failed=len(errors),
                    errors=errors if errors else None,
                    sample=records[:3],
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(THREE_PL_SEED_DATA),
                processed=0,
                inserted=0,
            )

        except Exception as e:
            logger.error(f"Seed data enrichment failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    def _transform_seed_entry(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform a seed data entry to database record format."""
        company_name = entry.get("company_name")
        if not company_name:
            return None

        return {
            "company_name": company_name,
            "headquarters_city": entry.get("headquarters_city"),
            "headquarters_state": entry.get("headquarters_state"),
            "website": entry.get("website"),
            "employee_count": entry.get("employee_count"),
            "facility_count": entry.get("facility_count"),
            "services": entry.get("services"),
            "industries_served": entry.get("industries_served"),
            "has_cold_chain": entry.get("has_cold_chain"),
            "has_hazmat": entry.get("has_hazmat"),
            "has_ecommerce_fulfillment": entry.get("has_ecommerce_fulfillment"),
            "has_cross_dock": entry.get("has_cross_dock"),
            "is_asset_based": entry.get("is_asset_based"),
            "is_non_asset": entry.get("is_non_asset"),
            "regions_served": entry.get("regions_served"),
            "states_coverage": entry.get("states_coverage"),
            "source": "three_pl_enrichment",
            "collected_at": datetime.utcnow(),
        }
