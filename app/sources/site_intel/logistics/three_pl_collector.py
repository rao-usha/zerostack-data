"""
3PL Company Collector.

Fetches 3PL (Third-Party Logistics) company data from:
- Transport Topics Top 100 Logistics Companies
- Armstrong & Associates rankings (when available)
- SEC filings for public companies

Data sources:
- Transport Topics website (scraping)
- SEC EDGAR API
- Company websites

No API key required for most public data.
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

logger = logging.getLogger(__name__)


@register_collector(SiteIntelSource.TRANSPORT_TOPICS)
class ThreePLCollector(BaseCollector):
    """
    Collector for 3PL company directory data.

    Fetches:
    - Company profiles and rankings
    - Revenue and employee counts
    - Services offered and geographic coverage
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.TRANSPORT_TOPICS

    # Transport Topics configuration
    default_timeout = 60.0
    rate_limit_delay = 1.5

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://www.ttnews.com"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (compatible; Nexdata-SiteIntel/1.0)",
            "Accept": "application/json, text/html",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute 3PL company data collection.

        Collects company profiles from Transport Topics and other sources.
        """
        try:
            logger.info("Collecting 3PL company data...")

            # Collect from Transport Topics
            companies_result = await self._collect_companies(config)
            all_companies = companies_result.get("records", [])

            if not all_companies:
                logger.warning("No 3PL company data retrieved from sources")
                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=0,
                    processed=0,
                    inserted=0,
                )

            # Transform and insert records
            records = []
            for company in all_companies:
                transformed = self._transform_company(company)
                if transformed:
                    records.append(transformed)

            logger.info(f"Transformed {len(records)} 3PL company records")

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    ThreePLCompany,
                    records,
                    unique_columns=["company_name"],
                    update_columns=[
                        "parent_company", "headquarters_city", "headquarters_state",
                        "headquarters_country", "website", "annual_revenue_million",
                        "revenue_year", "employee_count", "facility_count",
                        "services", "industries_served", "regions_served", "states_coverage",
                        "countries_coverage", "armstrong_rank", "transport_topics_rank",
                        "has_cold_chain", "has_hazmat", "has_ecommerce_fulfillment",
                        "has_cross_dock", "is_asset_based", "is_non_asset",
                        "source", "collected_at"
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(all_companies),
                    processed=len(all_companies),
                    inserted=inserted,
                    sample=records[:3] if records else None,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(all_companies),
                processed=len(all_companies),
                inserted=0,
            )

        except Exception as e:
            logger.error(f"3PL company collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_companies(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect 3PL company data from Transport Topics and other sources.

        TODO: Implement actual scraping with Playwright for JS-rendered content.
        Transport Topics Top 100: https://www.ttnews.com/top100/logistics
        """
        try:
            client = await self.get_client()
            all_records = []

            await self.apply_rate_limit()

            try:
                # Attempt to fetch Transport Topics Top 100 page
                # Note: This page requires JS rendering - needs Playwright implementation
                response = await client.get("/top-100-logistics-companies")

                if response.status_code == 200:
                    # TODO: Parse HTML for company data
                    # This requires implementing HTML parsing or using Playwright
                    # for JS-rendered content
                    logger.info("Transport Topics page fetched - parsing not yet implemented")
                else:
                    logger.warning(f"Transport Topics returned {response.status_code}")

            except Exception as e:
                logger.warning(f"Could not fetch from Transport Topics: {e}")

            return {"records": all_records}

        except Exception as e:
            logger.error(f"Failed to collect companies: {e}", exc_info=True)
            return {"records": [], "error": str(e)}

    def _transform_company(self, company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw company data to database format."""
        company_name = company.get("company_name")
        if not company_name:
            return None

        return {
            "company_name": company_name,
            "parent_company": company.get("parent_company"),
            "headquarters_city": company.get("headquarters_city"),
            "headquarters_state": company.get("headquarters_state"),
            "headquarters_country": company.get("headquarters_country", "USA"),
            "website": company.get("website"),
            "annual_revenue_million": self._safe_float(company.get("annual_revenue_million")),
            "revenue_year": self._safe_int(company.get("revenue_year")),
            "employee_count": self._safe_int(company.get("employee_count")),
            "facility_count": self._safe_int(company.get("facility_count")),
            "services": company.get("services"),
            "industries_served": company.get("industries_served"),
            "regions_served": company.get("regions_served"),
            "states_coverage": company.get("states_coverage"),
            "countries_coverage": company.get("countries_coverage"),
            "armstrong_rank": self._safe_int(company.get("armstrong_rank")),
            "transport_topics_rank": self._safe_int(company.get("transport_topics_rank")),
            "has_cold_chain": company.get("has_cold_chain"),
            "has_hazmat": company.get("has_hazmat"),
            "has_ecommerce_fulfillment": company.get("has_ecommerce_fulfillment"),
            "has_cross_dock": company.get("has_cross_dock"),
            "is_asset_based": company.get("is_asset_based"),
            "is_non_asset": company.get("is_non_asset"),
            "source": "transport_topics",
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
