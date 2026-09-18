"""
Good Jobs First Subsidy Tracker Collector.

Fetches disclosed incentive deals from Good Jobs First Subsidy Tracker:
- Company subsidy awards
- Job creation commitments
- Investment amounts
- Program types

Data source: https://subsidytracker.goodjobsfirst.org/

IMPORTANT: Good Jobs First requires a subscription ($25/month) for bulk data access.
Their API returns 403 Forbidden for anonymous requests.

Options for data access:
1. Subscribe at https://subsidytracker.goodjobsfirst.org/plans ($25/month or $250/year)
2. Contact kasia@goodjobsfirst.org for research/bulk access
3. Use the built-in sample seed data (major deals only)

This collector uses built-in seed data of major disclosed deals.
For full dataset, subscribe and import CSV manually.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import IncentiveDeal
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


# Sample major deals compiled from public Good Jobs First disclosures
# Source: https://goodjobsfirst.org/megadeals (January 2026)
# Note: This is a sample dataset. For complete data, subscribe to GJF.
@register_collector(SiteIntelSource.GOOD_JOBS_FIRST)
class GoodJobsFirstCollector(BaseCollector):
    """
    Collector for Good Jobs First Subsidy Tracker data.

    IMPORTANT: The GJF API requires a subscription ($25/month).
    This collector uses built-in seed data of major disclosed deals.

    For full dataset access:
    1. Subscribe at https://subsidytracker.goodjobsfirst.org/plans
    2. Download CSV and use import_from_csv() method
    3. Or contact kasia@goodjobsfirst.org for research access

    Fetches:
    - Disclosed corporate subsidy deals
    - Job commitments and investment amounts
    """

    domain = SiteIntelDomain.INCENTIVES
    source = SiteIntelSource.GOOD_JOBS_FIRST

    # Configuration
    default_timeout = 60.0
    rate_limit_delay = 0.5

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://subsidytracker.goodjobsfirst.org"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0 (Research)",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute Good Jobs First data collection.

        The built-in "seed" deal list was removed (PLAN_082): rows must come
        from a real source. Use import_from_csv() with a Subsidy Tracker export.
        """
        message = (
            "Good Jobs First API collection not implemented: built-in seed data "
            "removed; use import_from_csv() with a Subsidy Tracker export"
        )
        logger.warning(message)
        return self.create_result(
            status=CollectionStatus.FAILED,
            error_message=message,
        )

    def _transform_deal_record(
        self, record: Dict[str, Any], index: int
    ) -> Optional[Dict[str, Any]]:
        """Transform Good Jobs First deal record to database format."""
        company = record.get("company_name")
        state = record.get("state")
        year = record.get("year")

        if not company or not state:
            return None

        # Generate gjf_id from record data (company_state_year)
        gjf_id = f"{company[:30].replace(' ', '_')}_{state}_{year or 'NA'}"

        return {
            "gjf_id": gjf_id,
            "company_name": company,
            "parent_company": record.get("parent_company"),
            "state": state,
            "city": record.get("city"),
            "county": record.get("county"),
            "year": year,
            "subsidy_type": record.get("subsidy_type"),
            "program_name": record.get("program_name"),
            "subsidy_value": record.get("subsidy_value"),
            "jobs_announced": record.get("jobs_announced"),
            "investment_announced": record.get("investment_announced"),
            "industry": record.get("industry"),
            "source": "gjf_seed",
            "collected_at": datetime.utcnow(),
        }

    async def import_from_csv(
        self, csv_path: str, config: CollectionConfig
    ) -> Dict[str, Any]:
        """
        Import subsidy deals from a Good Jobs First CSV download.

        Use this after subscribing to GJF and downloading their data.

        Args:
            csv_path: Path to the downloaded CSV file
            config: Collection configuration

        Returns:
            Dict with processed and inserted counts
        """
        import csv

        try:
            records = []
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    # Filter by state if specified
                    if config.states and row.get("state") not in config.states:
                        continue

                    company = row.get("company") or row.get("recipient")
                    state = row.get("state")
                    year = self._safe_int(row.get("year") or row.get("fiscal_year"))

                    if not company or not state:
                        continue

                    # Generate gjf_id or use existing
                    gjf_id = row.get("id") or row.get("subsidy_id")
                    if not gjf_id:
                        gjf_id = (
                            f"{company[:30].replace(' ', '_')}_{state}_{year or 'NA'}"
                        )

                    record = {
                        "gjf_id": str(gjf_id),
                        "company_name": company,
                        "parent_company": row.get("parent_company")
                        or row.get("parent"),
                        "state": state,
                        "city": row.get("city"),
                        "county": row.get("county"),
                        "year": year,
                        "subsidy_type": row.get("subsidy_type") or row.get("type"),
                        "program_name": row.get("program") or row.get("program_name"),
                        "subsidy_value": self._safe_int(
                            row.get("subsidy") or row.get("value")
                        ),
                        "jobs_announced": self._safe_int(row.get("jobs")),
                        "investment_announced": self._safe_int(row.get("investment")),
                        "industry": row.get("industry") or row.get("naics_desc"),
                        "source": "gjf_csv",
                        "collected_at": datetime.utcnow(),
                    }
                    records.append(record)

            if records:
                inserted, _ = self.bulk_upsert(
                    IncentiveDeal,
                    records,
                    unique_columns=["gjf_id"],
                    update_columns=[
                        "company_name",
                        "parent_company",
                        "state",
                        "city",
                        "county",
                        "year",
                        "subsidy_type",
                        "program_name",
                        "subsidy_value",
                        "jobs_announced",
                        "investment_announced",
                        "industry",
                        "source",
                        "collected_at",
                    ],
                )
                logger.info(f"Imported {inserted} records from CSV")
                return {"processed": len(records), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to import from CSV: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "" or value == "N/A":
            return None
        try:
            # Handle currency strings like "$1,000,000"
            if isinstance(value, str):
                value = value.replace("$", "").replace(",", "").strip()
            return int(float(value))
        except (ValueError, TypeError):
            return None
