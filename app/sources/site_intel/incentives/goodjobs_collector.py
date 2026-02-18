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
from typing import Optional, List, Dict, Any

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
GJF_SEED_DATA = [
    # Major Technology Deals
    {
        "company_name": "Tesla",
        "parent_company": "Tesla Inc",
        "state": "TX",
        "city": "Austin",
        "year": 2020,
        "subsidy_type": "tax_rebate",
        "program_name": "Texas Enterprise Fund",
        "subsidy_value": 64700000,
        "jobs_announced": 5000,
        "investment_announced": 1100000000,
        "industry": "Electric Vehicles",
    },
    {
        "company_name": "Tesla",
        "parent_company": "Tesla Inc",
        "state": "NV",
        "city": "Sparks",
        "year": 2014,
        "subsidy_type": "tax_abatement",
        "program_name": "Nevada Tax Abatement",
        "subsidy_value": 1300000000,
        "jobs_announced": 6500,
        "investment_announced": 5000000000,
        "industry": "Battery Manufacturing",
    },
    {
        "company_name": "Samsung Austin Semiconductor",
        "parent_company": "Samsung Electronics",
        "state": "TX",
        "city": "Taylor",
        "year": 2021,
        "subsidy_type": "tax_abatement",
        "program_name": "Texas Chapter 313",
        "subsidy_value": 954000000,
        "jobs_announced": 2000,
        "investment_announced": 17000000000,
        "industry": "Semiconductor Manufacturing",
    },
    {
        "company_name": "Intel",
        "parent_company": "Intel Corporation",
        "state": "OH",
        "city": "New Albany",
        "year": 2022,
        "subsidy_type": "tax_credit",
        "program_name": "Ohio Job Creation Tax Credit",
        "subsidy_value": 2020000000,
        "jobs_announced": 3000,
        "investment_announced": 20000000000,
        "industry": "Semiconductor Manufacturing",
    },
    {
        "company_name": "Micron Technology",
        "parent_company": "Micron Technology Inc",
        "state": "NY",
        "city": "Clay",
        "year": 2022,
        "subsidy_type": "tax_credit",
        "program_name": "Green CHIPS",
        "subsidy_value": 5500000000,
        "jobs_announced": 9000,
        "investment_announced": 100000000000,
        "industry": "Semiconductor Manufacturing",
    },
    {
        "company_name": "TSMC Arizona",
        "parent_company": "Taiwan Semiconductor",
        "state": "AZ",
        "city": "Phoenix",
        "year": 2020,
        "subsidy_type": "tax_rebate",
        "program_name": "Arizona Commerce Authority",
        "subsidy_value": 205000000,
        "jobs_announced": 1600,
        "investment_announced": 12000000000,
        "industry": "Semiconductor Manufacturing",
    },
    # Major Automotive Deals
    {
        "company_name": "Ford Motor Company",
        "parent_company": "Ford Motor Company",
        "state": "TN",
        "city": "Stanton",
        "year": 2021,
        "subsidy_type": "tax_credit",
        "program_name": "Tennessee FastTrack",
        "subsidy_value": 884000000,
        "jobs_announced": 6000,
        "investment_announced": 5600000000,
        "industry": "Electric Vehicles",
    },
    {
        "company_name": "General Motors",
        "parent_company": "General Motors",
        "state": "MI",
        "city": "Lansing",
        "year": 2021,
        "subsidy_type": "tax_abatement",
        "program_name": "Michigan Business Tax Credit",
        "subsidy_value": 824000000,
        "jobs_announced": 4000,
        "investment_announced": 7000000000,
        "industry": "Electric Vehicles",
    },
    {
        "company_name": "Rivian",
        "parent_company": "Rivian Automotive",
        "state": "GA",
        "city": "Social Circle",
        "year": 2021,
        "subsidy_type": "tax_credit",
        "program_name": "Georgia EDGE",
        "subsidy_value": 1500000000,
        "jobs_announced": 7500,
        "investment_announced": 5000000000,
        "industry": "Electric Vehicles",
    },
    {
        "company_name": "SK Battery America",
        "parent_company": "SK Innovation",
        "state": "GA",
        "city": "Commerce",
        "year": 2019,
        "subsidy_type": "tax_credit",
        "program_name": "Georgia Quick Start",
        "subsidy_value": 302000000,
        "jobs_announced": 2600,
        "investment_announced": 2600000000,
        "industry": "Battery Manufacturing",
    },
    {
        "company_name": "Volkswagen",
        "parent_company": "Volkswagen AG",
        "state": "TN",
        "city": "Chattanooga",
        "year": 2008,
        "subsidy_type": "tax_abatement",
        "program_name": "Tennessee Tax Incentives",
        "subsidy_value": 577000000,
        "jobs_announced": 2000,
        "investment_announced": 1000000000,
        "industry": "Automotive Manufacturing",
    },
    {
        "company_name": "Toyota",
        "parent_company": "Toyota Motor Corporation",
        "state": "KY",
        "city": "Georgetown",
        "year": 2017,
        "subsidy_type": "tax_credit",
        "program_name": "Kentucky Business Investment",
        "subsidy_value": 147000000,
        "jobs_announced": 700,
        "investment_announced": 1330000000,
        "industry": "Automotive Manufacturing",
    },
    {
        "company_name": "Hyundai Motor Group",
        "parent_company": "Hyundai Motor Company",
        "state": "GA",
        "city": "Bryan County",
        "year": 2022,
        "subsidy_type": "tax_credit",
        "program_name": "Georgia EDGE",
        "subsidy_value": 1800000000,
        "jobs_announced": 8100,
        "investment_announced": 5540000000,
        "industry": "Electric Vehicles",
    },
    # Major Data Center Deals
    {
        "company_name": "Amazon Web Services",
        "parent_company": "Amazon.com Inc",
        "state": "VA",
        "city": "Ashburn",
        "year": 2019,
        "subsidy_type": "tax_credit",
        "program_name": "Virginia Data Center Incentive",
        "subsidy_value": 550000000,
        "jobs_announced": 1500,
        "investment_announced": 35000000000,
        "industry": "Data Centers",
    },
    {
        "company_name": "Microsoft",
        "parent_company": "Microsoft Corporation",
        "state": "VA",
        "city": "Mecklenburg County",
        "year": 2020,
        "subsidy_type": "tax_abatement",
        "program_name": "Virginia Data Center Incentive",
        "subsidy_value": 118000000,
        "jobs_announced": 50,
        "investment_announced": 2000000000,
        "industry": "Data Centers",
    },
    {
        "company_name": "Google",
        "parent_company": "Alphabet Inc",
        "state": "VA",
        "city": "Loudoun County",
        "year": 2018,
        "subsidy_type": "tax_credit",
        "program_name": "Virginia Data Center Incentive",
        "subsidy_value": 125000000,
        "jobs_announced": 75,
        "investment_announced": 600000000,
        "industry": "Data Centers",
    },
    {
        "company_name": "Meta Platforms",
        "parent_company": "Meta Platforms Inc",
        "state": "TX",
        "city": "Temple",
        "year": 2022,
        "subsidy_type": "tax_abatement",
        "program_name": "Texas Chapter 313",
        "subsidy_value": 147000000,
        "jobs_announced": 100,
        "investment_announced": 800000000,
        "industry": "Data Centers",
    },
    {
        "company_name": "Apple",
        "parent_company": "Apple Inc",
        "state": "TX",
        "city": "Austin",
        "year": 2018,
        "subsidy_type": "property_tax",
        "program_name": "Texas Chapter 313",
        "subsidy_value": 290000000,
        "jobs_announced": 5000,
        "investment_announced": 1000000000,
        "industry": "Technology",
    },
    # Major Manufacturing Deals
    {
        "company_name": "Foxconn",
        "parent_company": "Hon Hai Precision Industry",
        "state": "WI",
        "city": "Mount Pleasant",
        "year": 2017,
        "subsidy_type": "tax_credit",
        "program_name": "Wisconsin WEDC",
        "subsidy_value": 4500000000,
        "jobs_announced": 13000,
        "investment_announced": 10000000000,
        "industry": "Electronics Manufacturing",
    },
    {
        "company_name": "Boeing",
        "parent_company": "Boeing Company",
        "state": "SC",
        "city": "North Charleston",
        "year": 2009,
        "subsidy_type": "tax_credit",
        "program_name": "South Carolina Incentives",
        "subsidy_value": 900000000,
        "jobs_announced": 3800,
        "investment_announced": 750000000,
        "industry": "Aerospace",
    },
    {
        "company_name": "Boeing",
        "parent_company": "Boeing Company",
        "state": "WA",
        "city": "Everett",
        "year": 2013,
        "subsidy_type": "tax_break",
        "program_name": "Washington B&O Tax Incentive",
        "subsidy_value": 8700000000,
        "jobs_announced": 0,
        "investment_announced": 0,
        "industry": "Aerospace",
    },
    {
        "company_name": "LG Energy Solution",
        "parent_company": "LG Chem",
        "state": "MI",
        "city": "Holland",
        "year": 2019,
        "subsidy_type": "tax_credit",
        "program_name": "Michigan MEGA",
        "subsidy_value": 350000000,
        "jobs_announced": 1200,
        "investment_announced": 2500000000,
        "industry": "Battery Manufacturing",
    },
    {
        "company_name": "Panasonic Energy",
        "parent_company": "Panasonic Corporation",
        "state": "KS",
        "city": "De Soto",
        "year": 2022,
        "subsidy_type": "tax_rebate",
        "program_name": "Kansas PEAK",
        "subsidy_value": 829000000,
        "jobs_announced": 4000,
        "investment_announced": 4000000000,
        "industry": "Battery Manufacturing",
    },
    # Major Distribution/Logistics Deals
    {
        "company_name": "Amazon",
        "parent_company": "Amazon.com Inc",
        "state": "VA",
        "city": "Arlington",
        "year": 2018,
        "subsidy_type": "tax_credit",
        "program_name": "Virginia HQ2 Package",
        "subsidy_value": 750000000,
        "jobs_announced": 25000,
        "investment_announced": 2500000000,
        "industry": "Technology/HQ",
    },
    {
        "company_name": "Amazon",
        "parent_company": "Amazon.com Inc",
        "state": "NY",
        "city": "New York",
        "year": 2018,
        "subsidy_type": "tax_credit",
        "program_name": "NYC HQ2 Package",
        "subsidy_value": 3000000000,
        "jobs_announced": 25000,
        "investment_announced": 2500000000,
        "industry": "Technology/HQ",
    },
    # Major Pharmaceutical/Biotech Deals
    {
        "company_name": "Eli Lilly",
        "parent_company": "Eli Lilly and Company",
        "state": "IN",
        "city": "Lebanon",
        "year": 2022,
        "subsidy_type": "tax_credit",
        "program_name": "Indiana EDGE",
        "subsidy_value": 451000000,
        "jobs_announced": 500,
        "investment_announced": 2100000000,
        "industry": "Pharmaceutical",
    },
    {
        "company_name": "Pfizer",
        "parent_company": "Pfizer Inc",
        "state": "NC",
        "city": "Sanford",
        "year": 2022,
        "subsidy_type": "tax_credit",
        "program_name": "Job Development Investment Grant",
        "subsidy_value": 315000000,
        "jobs_announced": 250,
        "investment_announced": 5000000000,
        "industry": "Pharmaceutical",
    },
    # Energy Deals
    {
        "company_name": "Solugen",
        "parent_company": "Solugen Inc",
        "state": "TX",
        "city": "Marshall",
        "year": 2023,
        "subsidy_type": "tax_abatement",
        "program_name": "Texas Chapter 313",
        "subsidy_value": 42000000,
        "jobs_announced": 50,
        "investment_announced": 1100000000,
        "industry": "Bio-Manufacturing",
    },
]


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

        Uses built-in seed data of major disclosed deals.
        For full dataset, subscribe to GJF and import CSV.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting Good Jobs First subsidy data from seed dataset...")
            logger.info(
                "Note: For complete data, subscribe at subsidytracker.goodjobsfirst.org/plans"
            )

            deals_result = await self._collect_from_seed(config)
            total_inserted += deals_result.get("inserted", 0)
            total_processed += deals_result.get("processed", 0)
            if deals_result.get("error"):
                errors.append(
                    {"source": "subsidy_deals", "error": deals_result["error"]}
                )

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
            logger.error(f"Good Jobs First collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_from_seed(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect subsidy deals from built-in seed data.
        """
        try:
            # Filter by state if specified
            deals = GJF_SEED_DATA
            if config.states:
                deals = [d for d in deals if d.get("state") in config.states]

            logger.info(f"Processing {len(deals)} subsidy deals from seed data")

            # Transform records
            records = []
            for i, deal in enumerate(deals):
                transformed = self._transform_deal_record(deal, i)
                if transformed:
                    records.append(transformed)

            # Insert into database
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
                logger.info(f"Inserted/updated {inserted} subsidy deal records")
                return {"processed": len(deals), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect subsidy deals: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

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
