"""
SEC Form D Ingestion Service.

Stores Form D filings in PostgreSQL.
"""

import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.sec_form_d.client import FormDClient
from app.sources.sec_form_d.parser import FormDParser

logger = logging.getLogger(__name__)


class FormDIngestionService:
    """
    Service for ingesting and storing Form D filings.
    """

    def __init__(self, db: Session):
        self.db = db
        self.client = FormDClient()
        self.parser = FormDParser()
        self._ensure_tables()

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS form_d_filings (
            id SERIAL PRIMARY KEY,
            accession_number VARCHAR(25) NOT NULL UNIQUE,
            cik VARCHAR(10) NOT NULL,
            submission_type VARCHAR(10) NOT NULL,
            filed_at TIMESTAMP NOT NULL,
            issuer_name VARCHAR(500) NOT NULL,
            issuer_street VARCHAR(500),
            issuer_city VARCHAR(100),
            issuer_state VARCHAR(10),
            issuer_zip VARCHAR(20),
            issuer_phone VARCHAR(50),
            entity_type VARCHAR(50),
            jurisdiction VARCHAR(100),
            year_of_incorporation INTEGER,
            industry_group VARCHAR(100),
            revenue_range VARCHAR(50),
            related_persons JSONB,
            federal_exemptions JSONB,
            date_of_first_sale DATE,
            more_than_one_year BOOLEAN,
            is_equity BOOLEAN,
            is_debt BOOLEAN,
            is_option BOOLEAN,
            is_security_to_be_acquired BOOLEAN,
            is_pooled_investment_fund BOOLEAN,
            is_business_combination BOOLEAN,
            minimum_investment BIGINT,
            total_offering_amount BIGINT,
            total_amount_sold BIGINT,
            total_remaining BIGINT,
            total_number_already_invested INTEGER,
            accredited_investors INTEGER,
            non_accredited_investors INTEGER,
            sales_compensation JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_form_d_cik ON form_d_filings(cik);
        CREATE INDEX IF NOT EXISTS idx_form_d_filed_at ON form_d_filings(filed_at);
        CREATE INDEX IF NOT EXISTS idx_form_d_issuer_name ON form_d_filings(issuer_name);
        CREATE INDEX IF NOT EXISTS idx_form_d_industry ON form_d_filings(industry_group);
        CREATE INDEX IF NOT EXISTS idx_form_d_exemptions ON form_d_filings USING GIN(federal_exemptions);
        """
        try:
            self.db.execute(text(create_table_sql))
            self.db.commit()
            logger.info("Form D tables ready")
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    async def ingest_company_filings(self, cik: str) -> Dict[str, Any]:
        """
        Ingest all Form D filings for a company.

        Args:
            cik: Company CIK

        Returns:
            Ingestion result summary
        """
        result = {
            "cik": cik,
            "filings_found": 0,
            "filings_ingested": 0,
            "filings_skipped": 0,
            "errors": [],
        }

        try:
            # Get Form D filings from submissions
            filings = await self.client.get_form_d_filings_from_submissions(cik)
            result["filings_found"] = len(filings)

            for filing in filings:
                try:
                    # Check if already exists
                    if self._filing_exists(filing["accession_number"]):
                        result["filings_skipped"] += 1
                        continue

                    # Get and parse XML
                    xml_content = await self.client.get_filing_xml(
                        cik, filing["accession_number"], filing.get("primary_document")
                    )
                    if not xml_content:
                        result["errors"].append(
                            f"Failed to fetch XML for {filing['accession_number']}"
                        )
                        continue

                    parsed = self.parser.parse(xml_content)
                    if not parsed:
                        result["errors"].append(
                            f"Failed to parse XML for {filing['accession_number']}"
                        )
                        continue

                    # Store in database
                    self._store_filing(
                        accession_number=filing["accession_number"],
                        cik=cik,
                        filed_at=filing["filing_date"],
                        parsed_data=parsed,
                    )
                    result["filings_ingested"] += 1

                except Exception as e:
                    result["errors"].append(
                        f"Error processing {filing.get('accession_number')}: {str(e)}"
                    )

        except Exception as e:
            result["errors"].append(f"Failed to fetch submissions: {str(e)}")

        return result

    def _filing_exists(self, accession_number: str) -> bool:
        """Check if filing already exists in database."""
        query = text(
            "SELECT 1 FROM form_d_filings WHERE accession_number = :acc LIMIT 1"
        )
        result = self.db.execute(query, {"acc": accession_number})
        return result.fetchone() is not None

    def _store_filing(
        self, accession_number: str, cik: str, filed_at: str, parsed_data: Dict
    ):
        """Store a parsed Form D filing in the database."""
        issuer = parsed_data.get("issuer", {})
        offering = parsed_data.get("offering", {})
        investors = parsed_data.get("investors", {})

        # Parse date
        if filed_at:
            try:
                if isinstance(filed_at, str):
                    filed_at_dt = datetime.strptime(filed_at, "%Y-%m-%d")
                else:
                    filed_at_dt = filed_at
            except ValueError:
                filed_at_dt = datetime.now()
        else:
            filed_at_dt = datetime.now()

        # Parse date of first sale
        first_sale = offering.get("date_of_first_sale")
        first_sale_date = None
        if first_sale:
            try:
                first_sale_date = datetime.strptime(first_sale, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

        insert_sql = text("""
            INSERT INTO form_d_filings (
                accession_number, cik, submission_type, filed_at,
                issuer_name, issuer_street, issuer_city, issuer_state, issuer_zip, issuer_phone,
                entity_type, jurisdiction, year_of_incorporation,
                industry_group, revenue_range, related_persons, federal_exemptions,
                date_of_first_sale, more_than_one_year,
                is_equity, is_debt, is_option, is_security_to_be_acquired,
                is_pooled_investment_fund, is_business_combination,
                minimum_investment, total_offering_amount, total_amount_sold, total_remaining,
                total_number_already_invested, accredited_investors, non_accredited_investors,
                sales_compensation
            ) VALUES (
                :accession_number, :cik, :submission_type, :filed_at,
                :issuer_name, :issuer_street, :issuer_city, :issuer_state, :issuer_zip, :issuer_phone,
                :entity_type, :jurisdiction, :year_of_incorporation,
                :industry_group, :revenue_range, :related_persons, :federal_exemptions,
                :date_of_first_sale, :more_than_one_year,
                :is_equity, :is_debt, :is_option, :is_security_to_be_acquired,
                :is_pooled_investment_fund, :is_business_combination,
                :minimum_investment, :total_offering_amount, :total_amount_sold, :total_remaining,
                :total_number_already_invested, :accredited_investors, :non_accredited_investors,
                :sales_compensation
            )
            ON CONFLICT (accession_number) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP
        """)

        self.db.execute(
            insert_sql,
            {
                "accession_number": accession_number,
                "cik": cik,
                "submission_type": parsed_data.get("submission_type", "D"),
                "filed_at": filed_at_dt,
                "issuer_name": issuer.get("name", "Unknown"),
                "issuer_street": issuer.get("street"),
                "issuer_city": issuer.get("city"),
                "issuer_state": issuer.get("state"),
                "issuer_zip": issuer.get("zip"),
                "issuer_phone": issuer.get("phone"),
                "entity_type": issuer.get("entity_type"),
                "jurisdiction": issuer.get("jurisdiction"),
                "year_of_incorporation": issuer.get("year_incorporated"),
                "industry_group": offering.get("industry_group"),
                "revenue_range": offering.get("revenue_range"),
                "related_persons": json.dumps(parsed_data.get("related_persons", [])),
                "federal_exemptions": json.dumps(offering.get("exemptions", [])),
                "date_of_first_sale": first_sale_date,
                "more_than_one_year": offering.get("more_than_one_year", False),
                "is_equity": offering.get("is_equity", False),
                "is_debt": offering.get("is_debt", False),
                "is_option": offering.get("is_option", False),
                "is_security_to_be_acquired": offering.get(
                    "is_security_to_be_acquired", False
                ),
                "is_pooled_investment_fund": offering.get(
                    "is_pooled_investment_fund", False
                ),
                "is_business_combination": offering.get(
                    "is_business_combination", False
                ),
                "minimum_investment": offering.get("minimum_investment"),
                "total_offering_amount": offering.get("total_offering_amount"),
                "total_amount_sold": offering.get("total_amount_sold"),
                "total_remaining": offering.get("total_remaining"),
                "total_number_already_invested": investors.get("total"),
                "accredited_investors": investors.get("accredited"),
                "non_accredited_investors": investors.get("non_accredited"),
                "sales_compensation": json.dumps(
                    parsed_data.get("sales_compensation", [])
                ),
            },
        )
        self.db.commit()

    def search_filings(
        self,
        issuer_name: Optional[str] = None,
        industry: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exemption: Optional[str] = None,
        min_amount: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Search Form D filings in database.

        Returns:
            Search results with filings and total count
        """
        conditions = ["1=1"]
        params = {"limit": limit, "offset": offset}

        if issuer_name:
            conditions.append("issuer_name ILIKE :issuer_name")
            params["issuer_name"] = f"%{issuer_name}%"

        if industry:
            conditions.append("industry_group ILIKE :industry")
            params["industry"] = f"%{industry}%"

        if start_date:
            conditions.append("filed_at >= :start_date")
            params["start_date"] = start_date

        if end_date:
            conditions.append("filed_at <= :end_date")
            params["end_date"] = end_date

        if exemption:
            conditions.append("federal_exemptions @> :exemption::jsonb")
            params["exemption"] = json.dumps([exemption])

        if min_amount:
            conditions.append("total_offering_amount >= :min_amount")
            params["min_amount"] = min_amount

        where_clause = " AND ".join(conditions)

        # Get total count
        count_query = text(f"SELECT COUNT(*) FROM form_d_filings WHERE {where_clause}")
        total = self.db.execute(count_query, params).scalar()

        # Get filings
        query = text(f"""
            SELECT accession_number, cik, submission_type, filed_at,
                   issuer_name, issuer_city, issuer_state,
                   industry_group, federal_exemptions,
                   total_offering_amount, total_amount_sold
            FROM form_d_filings
            WHERE {where_clause}
            ORDER BY filed_at DESC
            LIMIT :limit OFFSET :offset
        """)

        result = self.db.execute(query, params)
        filings = []
        for row in result.mappings():
            filings.append(
                {
                    "accession_number": row["accession_number"],
                    "cik": row["cik"],
                    "submission_type": row["submission_type"],
                    "filed_at": row["filed_at"].isoformat()
                    if row["filed_at"]
                    else None,
                    "issuer_name": row["issuer_name"],
                    "location": f"{row['issuer_city']}, {row['issuer_state']}"
                    if row["issuer_city"]
                    else row["issuer_state"],
                    "industry_group": row["industry_group"],
                    "exemptions": json.loads(row["federal_exemptions"])
                    if row["federal_exemptions"]
                    else [],
                    "total_offering_amount": row["total_offering_amount"],
                    "total_amount_sold": row["total_amount_sold"],
                }
            )

        return {"total": total, "limit": limit, "offset": offset, "filings": filings}

    def get_filing(self, accession_number: str) -> Optional[Dict]:
        """Get a specific filing by accession number."""
        query = text("""
            SELECT * FROM form_d_filings WHERE accession_number = :acc
        """)
        result = self.db.execute(query, {"acc": accession_number})
        row = result.mappings().fetchone()

        if not row:
            return None

        return {
            "accession_number": row["accession_number"],
            "cik": row["cik"],
            "submission_type": row["submission_type"],
            "filed_at": row["filed_at"].isoformat() if row["filed_at"] else None,
            "issuer": {
                "name": row["issuer_name"],
                "address": {
                    "street": row["issuer_street"],
                    "city": row["issuer_city"],
                    "state": row["issuer_state"],
                    "zip": row["issuer_zip"],
                },
                "phone": row["issuer_phone"],
                "entity_type": row["entity_type"],
                "jurisdiction": row["jurisdiction"],
                "year_incorporated": row["year_of_incorporation"],
            },
            "industry": row["industry_group"],
            "revenue_range": row["revenue_range"],
            "offering": {
                "exemptions": json.loads(row["federal_exemptions"])
                if row["federal_exemptions"]
                else [],
                "date_of_first_sale": row["date_of_first_sale"].isoformat()
                if row["date_of_first_sale"]
                else None,
                "more_than_one_year": row["more_than_one_year"],
                "securities": {
                    "is_equity": row["is_equity"],
                    "is_debt": row["is_debt"],
                    "is_option": row["is_option"],
                    "is_pooled_fund": row["is_pooled_investment_fund"],
                },
                "amounts": {
                    "total_offering": row["total_offering_amount"],
                    "amount_sold": row["total_amount_sold"],
                    "remaining": row["total_remaining"],
                    "minimum_investment": row["minimum_investment"],
                },
            },
            "investors": {
                "total": row["total_number_already_invested"],
                "accredited": row["accredited_investors"],
                "non_accredited": row["non_accredited_investors"],
            },
            "related_persons": json.loads(row["related_persons"])
            if row["related_persons"]
            else [],
            "sales_compensation": json.loads(row["sales_compensation"])
            if row["sales_compensation"]
            else [],
        }

    def get_filings_by_cik(self, cik: str) -> List[Dict]:
        """Get all filings for a specific CIK."""
        query = text("""
            SELECT accession_number, submission_type, filed_at,
                   issuer_name, industry_group, federal_exemptions,
                   total_offering_amount, total_amount_sold
            FROM form_d_filings
            WHERE cik = :cik
            ORDER BY filed_at DESC
        """)
        result = self.db.execute(query, {"cik": cik.zfill(10)})

        filings = []
        for row in result.mappings():
            filings.append(
                {
                    "accession_number": row["accession_number"],
                    "submission_type": row["submission_type"],
                    "filed_at": row["filed_at"].isoformat()
                    if row["filed_at"]
                    else None,
                    "issuer_name": row["issuer_name"],
                    "industry_group": row["industry_group"],
                    "exemptions": json.loads(row["federal_exemptions"])
                    if row["federal_exemptions"]
                    else [],
                    "total_offering_amount": row["total_offering_amount"],
                    "total_amount_sold": row["total_amount_sold"],
                }
            )

        return filings

    def get_stats(self) -> Dict[str, Any]:
        """Get aggregate statistics for Form D filings."""
        stats_query = text("""
            SELECT
                COUNT(*) as total_filings,
                COUNT(DISTINCT cik) as unique_issuers,
                SUM(COALESCE(total_offering_amount, 0)) as total_offering_volume,
                SUM(COALESCE(total_amount_sold, 0)) as total_sold_volume,
                COUNT(CASE WHEN is_pooled_investment_fund THEN 1 END) as fund_filings,
                MIN(filed_at) as earliest_filing,
                MAX(filed_at) as latest_filing
            FROM form_d_filings
        """)
        result = self.db.execute(stats_query).mappings().fetchone()

        # Industry breakdown
        industry_query = text("""
            SELECT industry_group, COUNT(*) as count
            FROM form_d_filings
            WHERE industry_group IS NOT NULL
            GROUP BY industry_group
            ORDER BY count DESC
            LIMIT 10
        """)
        industries = self.db.execute(industry_query).mappings().fetchall()

        # Exemption breakdown
        exemption_query = text("""
            SELECT elem as exemption, COUNT(*) as count
            FROM form_d_filings, jsonb_array_elements_text(federal_exemptions) as elem
            GROUP BY elem
            ORDER BY count DESC
        """)
        exemptions = self.db.execute(exemption_query).mappings().fetchall()

        return {
            "total_filings": result["total_filings"],
            "unique_issuers": result["unique_issuers"],
            "total_offering_volume": result["total_offering_volume"],
            "total_sold_volume": result["total_sold_volume"],
            "fund_filings": result["fund_filings"],
            "date_range": {
                "earliest": result["earliest_filing"].isoformat()
                if result["earliest_filing"]
                else None,
                "latest": result["latest_filing"].isoformat()
                if result["latest_filing"]
                else None,
            },
            "by_industry": [
                {"industry": r["industry_group"], "count": r["count"]}
                for r in industries
            ],
            "by_exemption": [
                {"exemption": r["exemption"], "count": r["count"]} for r in exemptions
            ],
        }
