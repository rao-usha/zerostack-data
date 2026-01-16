"""
Company Data Enrichment Engine.

Enriches portfolio company data with financials, funding,
employee counts, and industry classification.
"""
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.enrichment.sec_edgar import SECEdgarClient

logger = logging.getLogger(__name__)


# Industry classification mapping (simplified)
INDUSTRY_KEYWORDS = {
    "technology": {
        "keywords": ["software", "saas", "cloud", "tech", "data", "ai", "ml", "platform"],
        "sector": "Technology",
        "sic": "7372",
        "naics": "541512",
    },
    "fintech": {
        "keywords": ["fintech", "payment", "banking", "financial", "lending", "insurance"],
        "sector": "Financial Services",
        "sic": "6199",
        "naics": "522320",
    },
    "healthcare": {
        "keywords": ["health", "medical", "biotech", "pharma", "drug", "therapeutic"],
        "sector": "Healthcare",
        "sic": "8011",
        "naics": "621111",
    },
    "ecommerce": {
        "keywords": ["ecommerce", "retail", "commerce", "marketplace", "shop"],
        "sector": "Consumer",
        "sic": "5961",
        "naics": "454110",
    },
    "energy": {
        "keywords": ["energy", "solar", "wind", "renewable", "clean", "power", "grid"],
        "sector": "Energy",
        "sic": "4911",
        "naics": "221111",
    },
}


class CompanyEnrichmentEngine:
    """
    Company data enrichment engine.

    Aggregates data from multiple sources to enrich company records.
    """

    def __init__(self, db: Session):
        self.db = db
        self.sec_client = SECEdgarClient()

    def _ensure_tables(self) -> None:
        """Ensure enrichment tables exist."""
        create_enrichment = text("""
            CREATE TABLE IF NOT EXISTS company_enrichment (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                sec_cik VARCHAR(20),
                sec_ticker VARCHAR(20),
                latest_revenue BIGINT,
                latest_assets BIGINT,
                latest_net_income BIGINT,
                latest_filing_date DATE,
                total_funding VARCHAR(100),
                last_funding_round VARCHAR(50),
                last_funding_amount VARCHAR(100),
                last_funding_date DATE,
                valuation BIGINT,
                valuation_date DATE,
                employee_count INTEGER,
                employee_count_date DATE,
                employee_growth_yoy FLOAT,
                industry VARCHAR(100),
                sector VARCHAR(100),
                sic_code VARCHAR(10),
                naics_code VARCHAR(10),
                company_status VARCHAR(50) DEFAULT 'active',
                status_date DATE,
                acquirer_name VARCHAR(255),
                ipo_date DATE,
                stock_symbol VARCHAR(20),
                enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                enrichment_source VARCHAR(255),
                confidence_score FLOAT DEFAULT 0.0,
                UNIQUE(company_name)
            )
        """)

        create_jobs = text("""
            CREATE TABLE IF NOT EXISTS enrichment_jobs (
                id SERIAL PRIMARY KEY,
                job_type VARCHAR(50) NOT NULL,
                company_name VARCHAR(255),
                status VARCHAR(20) DEFAULT 'pending',
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                results JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        try:
            self.db.execute(create_enrichment)
            self.db.execute(create_jobs)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def create_job(self, company_name: str, job_type: str = "single") -> int:
        """Create an enrichment job record."""
        self._ensure_tables()

        query = text("""
            INSERT INTO enrichment_jobs (job_type, company_name, status, created_at)
            VALUES (:job_type, :company_name, 'pending', NOW())
            RETURNING id
        """)
        result = self.db.execute(query, {
            "job_type": job_type,
            "company_name": company_name,
        })
        self.db.commit()
        row = result.fetchone()
        return row[0] if row else 0

    def update_job(
        self,
        job_id: int,
        status: str,
        results: Optional[Dict] = None,
        error: Optional[str] = None,
    ) -> None:
        """Update job status."""
        # Serialize results dict to JSON string for JSONB column
        results_json = json.dumps(results) if results else None

        if status == "running":
            query = text("""
                UPDATE enrichment_jobs
                SET status = :status, started_at = NOW()
                WHERE id = :job_id
            """)
        elif status in ("completed", "failed"):
            query = text("""
                UPDATE enrichment_jobs
                SET status = :status, completed_at = NOW(),
                    results = CAST(:results AS jsonb), error_message = :error
                WHERE id = :job_id
            """)
        else:
            query = text("""
                UPDATE enrichment_jobs
                SET status = :status
                WHERE id = :job_id
            """)

        self.db.execute(query, {
            "job_id": job_id,
            "status": status,
            "results": results_json,
            "error": error,
        })
        self.db.commit()

    def get_job_status(self, company_name: str) -> Optional[Dict]:
        """Get latest job status for a company."""
        self._ensure_tables()

        query = text("""
            SELECT id, job_type, company_name, status,
                   started_at, completed_at, error_message, results
            FROM enrichment_jobs
            WHERE company_name = :company_name
            ORDER BY created_at DESC
            LIMIT 1
        """)
        result = self.db.execute(query, {"company_name": company_name})
        row = result.mappings().fetchone()

        if not row:
            return None

        return {
            "job_id": row["id"],
            "job_type": row["job_type"],
            "company_name": row["company_name"],
            "status": row["status"],
            "started_at": row["started_at"].isoformat() if row["started_at"] else None,
            "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
            "error_message": row["error_message"],
            "results": row["results"],
        }

    async def enrich_from_sec(self, company_name: str) -> Dict:
        """
        Enrich company with SEC EDGAR data.

        Args:
            company_name: Company name to search

        Returns:
            Dict with SEC data or error
        """
        return await self.sec_client.enrich_company(company_name)

    def enrich_funding(self, company_name: str) -> Dict:
        """
        Enrich company with funding data.

        Note: This is a placeholder. In production, integrate with
        Crunchbase, PitchBook, or similar APIs.

        Args:
            company_name: Company name

        Returns:
            Dict with funding data (placeholder)
        """
        # Check if we have any funding data in portfolio_companies
        try:
            query = text("""
                SELECT company_name, investment_amount_usd, investment_date
                FROM portfolio_companies
                WHERE LOWER(company_name) LIKE LOWER(:pattern)
                ORDER BY investment_date DESC NULLS LAST
                LIMIT 1
            """)
            result = self.db.execute(query, {"pattern": f"%{company_name}%"})
            row = result.mappings().fetchone()

            if row and row.get("investment_amount_usd"):
                # Try to parse amount from string (may contain "$", "M", etc.)
                amount_str = row["investment_amount_usd"]
                return {
                    "source": "portfolio_records",
                    "found": True,
                    "total_funding": None,  # Would aggregate all rounds
                    "last_funding_amount": amount_str,
                    "last_funding_date": row["investment_date"].isoformat() if row.get("investment_date") else None,
                }
        except Exception as e:
            logger.warning(f"Error querying funding data: {e}")

        return {
            "source": "placeholder",
            "found": False,
            "note": "Funding data requires Crunchbase/PitchBook integration",
        }

    def enrich_employees(self, company_name: str) -> Dict:
        """
        Enrich company with employee data.

        Note: This is a placeholder. In production, integrate with
        LinkedIn or similar APIs.

        Args:
            company_name: Company name

        Returns:
            Dict with employee data (placeholder)
        """
        return {
            "source": "placeholder",
            "found": False,
            "note": "Employee data requires LinkedIn integration",
        }

    def classify_industry(self, company_name: str, description: Optional[str] = None) -> Dict:
        """
        Classify company industry based on name and description.

        Args:
            company_name: Company name
            description: Optional company description

        Returns:
            Dict with industry classification
        """
        text_to_analyze = f"{company_name} {description or ''}".lower()

        best_match = None
        best_score = 0

        for industry, data in INDUSTRY_KEYWORDS.items():
            score = sum(1 for kw in data["keywords"] if kw in text_to_analyze)
            if score > best_score:
                best_score = score
                best_match = industry

        if best_match:
            data = INDUSTRY_KEYWORDS[best_match]
            return {
                "found": True,
                "industry": best_match.replace("_", " ").title(),
                "sector": data["sector"],
                "sic_code": data["sic"],
                "naics_code": data["naics"],
                "confidence": min(best_score * 0.25, 1.0),
            }

        return {
            "found": False,
            "industry": "Other",
            "sector": "Other",
            "sic_code": None,
            "naics_code": None,
            "confidence": 0.0,
        }

    def check_company_status(self, company_name: str) -> Dict:
        """
        Check company status (active, acquired, IPO, etc).

        Note: This is a placeholder. Would integrate with news/SEC APIs.

        Args:
            company_name: Company name

        Returns:
            Dict with status information
        """
        # Check if SEC found a ticker (indicates public company)
        # This would be called after SEC enrichment in real flow

        return {
            "source": "placeholder",
            "status": "active",
            "ipo_date": None,
            "acquirer": None,
            "note": "Status checking requires additional integrations",
        }

    async def enrich_company(self, company_name: str) -> Dict:
        """
        Run full enrichment for a company.

        Args:
            company_name: Company name to enrich

        Returns:
            Dict with all enrichment results
        """
        self._ensure_tables()

        # Create job
        job_id = self.create_job(company_name)
        self.update_job(job_id, "running")

        results = {
            "company_name": company_name,
            "job_id": job_id,
            "sec_edgar": None,
            "funding": None,
            "employees": None,
            "classification": None,
            "status": None,
        }

        try:
            # SEC EDGAR enrichment
            sec_data = await self.enrich_from_sec(company_name)
            results["sec_edgar"] = sec_data

            # Funding data
            funding_data = self.enrich_funding(company_name)
            results["funding"] = funding_data

            # Employee data
            employee_data = self.enrich_employees(company_name)
            results["employees"] = employee_data

            # Industry classification
            classification = self.classify_industry(company_name)
            results["classification"] = classification

            # Company status
            status = self.check_company_status(company_name)
            results["status"] = status

            # Calculate confidence score
            confidence = 0.0
            if sec_data.get("found"):
                confidence += 0.4
            if funding_data.get("found"):
                confidence += 0.2
            if employee_data.get("found"):
                confidence += 0.2
            if classification.get("found"):
                confidence += 0.2

            # Save enrichment data
            self._save_enrichment(company_name, results, confidence)

            self.update_job(job_id, "completed", results={
                "sec_edgar": "success" if sec_data.get("found") else "not_found",
                "funding": "success" if funding_data.get("found") else "placeholder",
                "employees": "placeholder",
                "classification": "success" if classification.get("found") else "no_match",
                "status": "placeholder",
            })

            results["confidence_score"] = confidence
            return results

        except Exception as e:
            logger.error(f"Enrichment error for {company_name}: {e}")
            self.update_job(job_id, "failed", error=str(e))
            raise

    def _save_enrichment(self, company_name: str, results: Dict, confidence: float) -> None:
        """Save enrichment results to database."""
        sec = results.get("sec_edgar", {})
        funding = results.get("funding", {})
        employees = results.get("employees", {})
        classification = results.get("classification", {})
        status = results.get("status", {})

        # Upsert enrichment record
        query = text("""
            INSERT INTO company_enrichment (
                company_name, sec_cik, sec_ticker,
                latest_revenue, latest_assets, latest_net_income, latest_filing_date,
                total_funding, last_funding_amount, last_funding_date,
                employee_count, employee_count_date, employee_growth_yoy,
                industry, sector, sic_code, naics_code,
                company_status, enriched_at, confidence_score
            ) VALUES (
                :company_name, :sec_cik, :sec_ticker,
                :revenue, :assets, :net_income, :filing_date,
                :total_funding, :last_funding, :funding_date,
                :employees, :emp_date, :emp_growth,
                :industry, :sector, :sic, :naics,
                :status, NOW(), :confidence
            )
            ON CONFLICT (company_name) DO UPDATE SET
                sec_cik = EXCLUDED.sec_cik,
                sec_ticker = EXCLUDED.sec_ticker,
                latest_revenue = EXCLUDED.latest_revenue,
                latest_assets = EXCLUDED.latest_assets,
                latest_net_income = EXCLUDED.latest_net_income,
                latest_filing_date = EXCLUDED.latest_filing_date,
                total_funding = EXCLUDED.total_funding,
                last_funding_amount = EXCLUDED.last_funding_amount,
                last_funding_date = EXCLUDED.last_funding_date,
                employee_count = EXCLUDED.employee_count,
                employee_count_date = EXCLUDED.employee_count_date,
                employee_growth_yoy = EXCLUDED.employee_growth_yoy,
                industry = EXCLUDED.industry,
                sector = EXCLUDED.sector,
                sic_code = EXCLUDED.sic_code,
                naics_code = EXCLUDED.naics_code,
                company_status = EXCLUDED.company_status,
                enriched_at = NOW(),
                confidence_score = EXCLUDED.confidence_score
        """)

        filing_date = None
        if sec.get("filing_date"):
            try:
                filing_date = datetime.strptime(sec["filing_date"], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

        funding_date = None
        if funding.get("last_funding_date"):
            try:
                funding_date = datetime.strptime(funding["last_funding_date"], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

        self.db.execute(query, {
            "company_name": company_name,
            "sec_cik": sec.get("cik"),
            "sec_ticker": sec.get("ticker"),
            "revenue": sec.get("revenue"),
            "assets": sec.get("assets"),
            "net_income": sec.get("net_income"),
            "filing_date": filing_date,
            "total_funding": funding.get("total_funding"),
            "last_funding": funding.get("last_funding_amount"),
            "funding_date": funding_date,
            "employees": employees.get("count"),
            "emp_date": None,
            "emp_growth": employees.get("growth_yoy"),
            "industry": classification.get("industry"),
            "sector": classification.get("sector"),
            "sic": classification.get("sic_code"),
            "naics": classification.get("naics_code"),
            "status": status.get("status", "active"),
            "confidence": confidence,
        })
        self.db.commit()

    def get_enriched_company(self, company_name: str) -> Optional[Dict]:
        """Get enriched data for a company."""
        self._ensure_tables()

        query = text("""
            SELECT * FROM company_enrichment
            WHERE LOWER(company_name) = LOWER(:name)
        """)
        result = self.db.execute(query, {"name": company_name})
        row = result.mappings().fetchone()

        if not row:
            return None

        return {
            "company_name": row["company_name"],
            "financials": {
                "sec_cik": row["sec_cik"],
                "ticker": row["sec_ticker"],
                "revenue": row["latest_revenue"],
                "assets": row["latest_assets"],
                "net_income": row["latest_net_income"],
                "filing_date": row["latest_filing_date"].isoformat() if row["latest_filing_date"] else None,
            },
            "funding": {
                "total_funding": row["total_funding"],
                "last_amount": row["last_funding_amount"],
                "last_date": row["last_funding_date"].isoformat() if row["last_funding_date"] else None,
                "valuation": row["valuation"],
            },
            "employees": {
                "count": row["employee_count"],
                "date": row["employee_count_date"].isoformat() if row["employee_count_date"] else None,
                "growth_yoy": row["employee_growth_yoy"],
            },
            "classification": {
                "industry": row["industry"],
                "sector": row["sector"],
                "sic_code": row["sic_code"],
                "naics_code": row["naics_code"],
            },
            "status": {
                "current": row["company_status"],
                "acquirer": row["acquirer_name"],
                "ipo_date": row["ipo_date"].isoformat() if row["ipo_date"] else None,
                "stock_symbol": row["stock_symbol"],
            },
            "enriched_at": row["enriched_at"].isoformat() if row["enriched_at"] else None,
            "confidence_score": row["confidence_score"],
        }

    def list_enriched_companies(
        self,
        limit: int = 50,
        offset: int = 0,
        min_confidence: float = 0.0,
    ) -> List[Dict]:
        """List all enriched companies."""
        self._ensure_tables()

        query = text("""
            SELECT company_name, industry, sector, company_status,
                   confidence_score, enriched_at
            FROM company_enrichment
            WHERE confidence_score >= :min_confidence
            ORDER BY enriched_at DESC
            LIMIT :limit OFFSET :offset
        """)
        result = self.db.execute(query, {
            "min_confidence": min_confidence,
            "limit": limit,
            "offset": offset,
        })

        return [
            {
                "company_name": row["company_name"],
                "industry": row["industry"],
                "sector": row["sector"],
                "status": row["company_status"],
                "confidence_score": row["confidence_score"],
                "enriched_at": row["enriched_at"].isoformat() if row["enriched_at"] else None,
            }
            for row in result.mappings()
        ]

    async def batch_enrich(self, company_names: List[str]) -> Dict:
        """
        Run batch enrichment for multiple companies.

        Args:
            company_names: List of company names

        Returns:
            Dict with batch results summary
        """
        results = {
            "total": len(company_names),
            "completed": 0,
            "failed": 0,
            "companies": [],
        }

        for name in company_names:
            try:
                enrichment = await self.enrich_company(name)
                results["completed"] += 1
                results["companies"].append({
                    "company_name": name,
                    "status": "completed",
                    "confidence": enrichment.get("confidence_score", 0),
                })
            except Exception as e:
                results["failed"] += 1
                results["companies"].append({
                    "company_name": name,
                    "status": "failed",
                    "error": str(e),
                })

        return results
