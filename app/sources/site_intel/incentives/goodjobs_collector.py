"""
Good Jobs First Subsidy Tracker Collector.

Fetches disclosed incentive deals from Good Jobs First Subsidy Tracker:
- Company subsidy awards
- Job creation commitments
- Investment amounts
- Program types

Data source: https://subsidytracker.goodjobsfirst.org/
Provides bulk data access for research purposes.

No API key required - public data.
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import IncentiveDeal
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)


@register_collector(SiteIntelSource.GOOD_JOBS_FIRST)
class GoodJobsFirstCollector(BaseCollector):
    """
    Collector for Good Jobs First Subsidy Tracker data.

    Fetches:
    - Disclosed corporate subsidy deals
    - Job commitments and investment amounts
    """

    domain = SiteIntelDomain.INCENTIVES
    source = SiteIntelSource.GOOD_JOBS_FIRST

    # GJF API configuration
    default_timeout = 180.0  # Large dataset
    rate_limit_delay = 1.0

    # Good Jobs First data endpoint
    # Note: GJF provides data via their API or bulk download
    GJF_API_URL = "https://subsidytracker.goodjobsfirst.org/api/subsidies"

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://subsidytracker.goodjobsfirst.org/api"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0 (Research)",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute Good Jobs First data collection.

        Collects disclosed subsidy deals.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting Good Jobs First subsidy data...")
            deals_result = await self._collect_subsidy_deals(config)
            total_inserted += deals_result.get("inserted", 0)
            total_processed += deals_result.get("processed", 0)
            if deals_result.get("error"):
                errors.append({"source": "subsidy_deals", "error": deals_result["error"]})

            status = CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"Good Jobs First collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_subsidy_deals(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect subsidy deals from Good Jobs First.

        Note: GJF may require specific API access or provide bulk downloads.
        This collector attempts the API approach.
        """
        try:
            all_deals = []
            page = 1
            page_size = 100

            # Determine states to collect
            states = config.states if config.states else None

            while True:
                params = {
                    "page": page,
                    "per_page": page_size,
                }

                # Add state filter if specified
                if states:
                    # GJF API may use different parameter names
                    params["state"] = ",".join(states)

                try:
                    response = await self.fetch_json("/subsidies", params=params)
                except Exception as e:
                    # If API fails, try alternative approach or log
                    logger.warning(f"GJF API request failed: {e}")
                    # Return partial results or try fallback
                    break

                # Parse response - structure may vary
                data = response if isinstance(response, list) else response.get("data", [])

                if not data:
                    break

                all_deals.extend(data)
                logger.info(f"Fetched {len(data)} subsidy records (total: {len(all_deals)})")

                if len(data) < page_size:
                    break

                page += 1

                # Safety limit
                if page > 100:
                    logger.warning("Hit page limit, stopping collection")
                    break

            # Transform records
            records = []
            for deal in all_deals:
                transformed = self._transform_deal_record(deal)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    IncentiveDeal,
                    records,
                    unique_columns=["gjf_id"],
                    update_columns=[
                        "company_name", "parent_company", "state", "city", "county",
                        "year", "subsidy_type", "program_name", "subsidy_value",
                        "jobs_announced", "investment_announced", "industry",
                        "collected_at"
                    ],
                )
                return {"processed": len(all_deals), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect subsidy deals: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_deal_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform Good Jobs First deal record to database format."""
        # GJF record ID
        gjf_id = record.get("id") or record.get("subsidy_id")
        if not gjf_id:
            # Generate from key fields
            company = record.get("company") or record.get("recipient")
            state = record.get("state")
            year = record.get("year") or record.get("fiscal_year")
            if company and state:
                gjf_id = f"{company[:20]}_{state}_{year or 'NA'}"
            else:
                return None

        return {
            "gjf_id": str(gjf_id),
            "company_name": record.get("company") or record.get("recipient") or record.get("subsidiary"),
            "parent_company": record.get("parent_company") or record.get("parent"),
            "state": record.get("state"),
            "city": record.get("city") or record.get("location"),
            "county": record.get("county"),
            "year": self._safe_int(record.get("year") or record.get("fiscal_year")),
            "subsidy_type": record.get("subsidy_type") or record.get("type"),
            "program_name": record.get("program") or record.get("program_name"),
            "subsidy_value": self._safe_int(record.get("subsidy") or record.get("value") or record.get("amount")),
            "jobs_announced": self._safe_int(record.get("jobs") or record.get("jobs_created")),
            "investment_announced": self._safe_int(record.get("investment") or record.get("capital_investment")),
            "industry": record.get("industry") or record.get("sector") or record.get("naics_desc"),
            "source": "good_jobs_first",
            "collected_at": datetime.utcnow(),
        }

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
