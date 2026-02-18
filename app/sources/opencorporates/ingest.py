"""
OpenCorporates Ingestor.

Searches OpenCorporates API for tracked companies and stores
results in oc_companies, oc_officers, and oc_filings tables.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.models import IngestionJob, JobStatus
from app.core.batch_operations import batch_insert
from app.sources.opencorporates.client import OpenCorporatesClient

logger = logging.getLogger(__name__)

# Table DDL
CREATE_OC_COMPANIES = """
CREATE TABLE IF NOT EXISTS oc_companies (
    id SERIAL PRIMARY KEY,
    company_number VARCHAR(100),
    jurisdiction_code VARCHAR(20),
    name VARCHAR(500) NOT NULL,
    incorporation_date DATE,
    dissolution_date DATE,
    company_type VARCHAR(200),
    current_status VARCHAR(100),
    registered_address TEXT,
    registry_url TEXT,
    opencorporates_url TEXT,
    agent_name VARCHAR(500),
    source VARCHAR(200),
    retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_number, jurisdiction_code)
);
CREATE INDEX IF NOT EXISTS idx_oc_companies_name ON oc_companies (name);
CREATE INDEX IF NOT EXISTS idx_oc_companies_jurisdiction ON oc_companies (jurisdiction_code);
"""

CREATE_OC_OFFICERS = """
CREATE TABLE IF NOT EXISTS oc_officers (
    id SERIAL PRIMARY KEY,
    company_number VARCHAR(100),
    jurisdiction_code VARCHAR(20),
    name VARCHAR(500) NOT NULL,
    position VARCHAR(200),
    start_date DATE,
    end_date DATE,
    nationality VARCHAR(100),
    occupation VARCHAR(200),
    opencorporates_url TEXT,
    retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_oc_officers_name ON oc_officers (name);
CREATE INDEX IF NOT EXISTS idx_oc_officers_company ON oc_officers (company_number, jurisdiction_code);
"""

CREATE_OC_FILINGS = """
CREATE TABLE IF NOT EXISTS oc_filings (
    id SERIAL PRIMARY KEY,
    company_number VARCHAR(100),
    jurisdiction_code VARCHAR(20),
    title VARCHAR(500),
    filing_type VARCHAR(200),
    date DATE,
    description TEXT,
    url TEXT,
    opencorporates_url TEXT,
    retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_oc_filings_company ON oc_filings (company_number, jurisdiction_code);
"""

OC_COMPANY_COLUMNS = [
    "company_number",
    "jurisdiction_code",
    "name",
    "incorporation_date",
    "dissolution_date",
    "company_type",
    "current_status",
    "registered_address",
    "registry_url",
    "opencorporates_url",
    "agent_name",
    "source",
]

OC_OFFICER_COLUMNS = [
    "company_number",
    "jurisdiction_code",
    "name",
    "position",
    "start_date",
    "end_date",
    "nationality",
    "occupation",
    "opencorporates_url",
]

OC_FILING_COLUMNS = [
    "company_number",
    "jurisdiction_code",
    "title",
    "filing_type",
    "date",
    "description",
    "url",
    "opencorporates_url",
]


class OpenCorporatesIngestor:
    """
    Ingestor that searches OpenCorporates for tracked companies,
    fetches company details, officers, and filings.
    """

    SOURCE_NAME = "opencorporates"

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self):
        for ddl in (CREATE_OC_COMPANIES, CREATE_OC_OFFICERS, CREATE_OC_FILINGS):
            for statement in ddl.strip().split(";"):
                statement = statement.strip()
                if statement:
                    self.db.execute(text(statement))
        self.db.commit()

    async def run(
        self,
        job_id: int,
        company_names: Optional[List[str]] = None,
        jurisdiction: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Run OpenCorporates ingestion.

        Args:
            job_id: Ingestion job ID
            company_names: Companies to search for. If None, uses tracked companies.
            jurisdiction: Jurisdiction filter (e.g., 'us_de')
            limit: Max companies to process

        Returns:
            Dict with ingestion results
        """
        start_time = datetime.utcnow()

        job = self.db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = start_time
            self.db.commit()

        try:
            # Get company names to search
            if not company_names:
                company_names = self._get_tracked_companies()

            if limit:
                company_names = company_names[:limit]

            client = OpenCorporatesClient()
            total_companies = 0
            total_officers = 0
            total_filings = 0
            errors = []

            for name in company_names:
                try:
                    # Search for company
                    results = client.search_companies(
                        name, jurisdiction=jurisdiction, per_page=5
                    )
                    companies = results.get("companies", [])
                    if not companies:
                        continue

                    for company_wrapper in companies:
                        company = (
                            company_wrapper if isinstance(company_wrapper, dict) else {}
                        )
                        if "company" in company:
                            company = company["company"]
                        parsed = client._parse_company(company)

                        cn = parsed.get("company_number")
                        jc = parsed.get("jurisdiction_code")
                        if not cn or not jc:
                            continue

                        # Flatten address
                        addr = parsed.get("registered_address")
                        if isinstance(addr, dict):
                            parsed["registered_address"] = addr.get("full_address", "")
                        elif addr is None:
                            parsed["registered_address"] = None

                        # Ensure all columns present
                        row = {c: parsed.get(c) for c in OC_COMPANY_COLUMNS}
                        batch_insert(
                            self.db,
                            "oc_companies",
                            [row],
                            OC_COMPANY_COLUMNS,
                            conflict_columns=["company_number", "jurisdiction_code"],
                            update_columns=[
                                c
                                for c in OC_COMPANY_COLUMNS
                                if c not in ("company_number", "jurisdiction_code")
                            ],
                        )
                        total_companies += 1

                        # Fetch officers
                        try:
                            officer_data = client.get_company_officers(jc, cn)
                            officers = officer_data.get("officers", [])
                            officer_rows = []
                            for o in officers:
                                off = o.get("officer", o) if isinstance(o, dict) else {}
                                parsed_o = client._parse_officer(off)
                                parsed_o["company_number"] = cn
                                parsed_o["jurisdiction_code"] = jc
                                officer_rows.append(
                                    {c: parsed_o.get(c) for c in OC_OFFICER_COLUMNS}
                                )

                            if officer_rows:
                                batch_insert(
                                    self.db,
                                    "oc_officers",
                                    officer_rows,
                                    OC_OFFICER_COLUMNS,
                                )
                                total_officers += len(officer_rows)
                        except Exception as e:
                            logger.debug(f"Officers for {cn}: {e}")

                        # Fetch filings
                        try:
                            filing_data = client.get_company_filings(jc, cn)
                            filings = filing_data.get("filings", [])
                            filing_rows = []
                            for f in filings:
                                fil = f.get("filing", f) if isinstance(f, dict) else {}
                                parsed_f = client._parse_filing(fil)
                                parsed_f["company_number"] = cn
                                parsed_f["jurisdiction_code"] = jc
                                filing_rows.append(
                                    {c: parsed_f.get(c) for c in OC_FILING_COLUMNS}
                                )

                            if filing_rows:
                                batch_insert(
                                    self.db,
                                    "oc_filings",
                                    filing_rows,
                                    OC_FILING_COLUMNS,
                                )
                                total_filings += len(filing_rows)
                        except Exception as e:
                            logger.debug(f"Filings for {cn}: {e}")

                except Exception as e:
                    errors.append(f"{name}: {e}")
                    logger.warning(f"OpenCorporates search failed for '{name}': {e}")

            client.close()

            duration = (datetime.utcnow() - start_time).total_seconds()
            total = total_companies + total_officers + total_filings

            if job:
                job.status = JobStatus.SUCCESS if total > 0 else JobStatus.FAILED
                if total == 0:
                    job.error_message = "No records found"
                job.completed_at = datetime.utcnow()
                job.rows_inserted = total
                self.db.commit()

            logger.info(
                f"OpenCorporates ingestion: {total_companies} companies, "
                f"{total_officers} officers, {total_filings} filings in {duration:.1f}s"
            )

            return {
                "companies": total_companies,
                "officers": total_officers,
                "filings": total_filings,
                "total_rows": total,
                "errors": errors,
                "duration_seconds": duration,
            }

        except Exception as e:
            logger.error(f"OpenCorporates ingestion failed: {e}", exc_info=True)
            if job:
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.completed_at = datetime.utcnow()
                self.db.commit()
            raise

    def _get_tracked_companies(self) -> List[str]:
        """Get company names from industrial_companies table."""
        try:
            result = self.db.execute(
                text("SELECT DISTINCT company_name FROM industrial_companies LIMIT 100")
            )
            return [row[0] for row in result.fetchall() if row[0]]
        except Exception:
            return []
