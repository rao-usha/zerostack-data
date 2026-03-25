"""
SEC Proxy Compensation Agent.

Parses the Summary Compensation Table from DEF 14A filings and populates:
- company_people.base_salary_usd
- company_people.total_compensation_usd
- company_people.equity_awards_usd
- company_people.compensation_year

Also parses Form 4 filings and creates InsiderTransaction records.
Uses existing FilingFetcher (already fetches DEF 14A for SECAgent).
"""

import logging
import re
from typing import List, Optional
from datetime import date

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.filing_fetcher import FilingFetcher
from app.agentic.llm_client import LLMClient

logger = logging.getLogger(__name__)

COMP_TABLE_PROMPT = """
Extract the Summary Compensation Table from this SEC proxy filing excerpt.
Return a JSON array where each element is one executive row:
{
  "name": "Full Name",
  "title": "Chief Executive Officer",
  "year": 2024,
  "salary_usd": 850000,
  "bonus_usd": 200000,
  "stock_awards_usd": 1500000,
  "option_awards_usd": 500000,
  "non_equity_incentive_usd": 300000,
  "total_comp_usd": 3350000
}
Use null for missing fields. Only include named executives from the table — do not invent data.
"""

FORM4_PROMPT = """
Extract insider transactions from this SEC Form 4 filing.
Return JSON:
{
  "reporting_person": "Full Name",
  "transactions": [
    {
      "transaction_date": "2024-03-15",
      "transaction_type": "sell",
      "shares": 10000,
      "price_per_share": 42.50,
      "total_value_usd": 425000,
      "shares_owned_after": 85000,
      "is_10b5_plan": false
    }
  ]
}
transaction_type: buy, sell, option_exercise, gift, grant
"""


class ProxyCompAgent(BaseCollector):
    """Parses DEF 14A comp tables and Form 4 filings."""

    def __init__(self):
        super().__init__(source_type="sec_edgar")
        self.fetcher = FilingFetcher()
        self.llm     = LLMClient()

    async def close(self):
        await super().close()
        await self.fetcher.close()

    async def collect_comp(self, company_id: int, cik: str, company_name: str, db) -> dict:
        """Parse DEF 14A and update company_people compensation fields."""
        from app.core.people_models import CompanyPerson, Person

        filings = await self.fetcher.get_company_filings(
            cik=cik, filing_types=["DEF 14A"], limit=3
        )
        if not filings:
            return {"status": "no_filings", "company_id": company_id}

        updated = 0
        for filing in filings:
            text = await self.fetcher.get_filing_content(filing)
            if not text:
                continue

            comp_section = self._extract_comp_section(text)
            if not comp_section:
                continue

            try:
                response = await self.llm.complete(
                    prompt=f"{COMP_TABLE_PROMPT}\n\n---\n\n{comp_section[:8000]}"
                )
                comp_rows = response.parse_json()
            except Exception as e:
                logger.error(f"Comp extraction failed: {e}")
                continue

            for row in (comp_rows or []):
                name = row.get("name", "")
                year = row.get("year")
                if not name or not year:
                    continue
                cp = (
                    db.query(CompanyPerson)
                    .join(Person, CompanyPerson.person_id == Person.id)
                    .filter(
                        CompanyPerson.company_id == company_id,
                        CompanyPerson.is_current == True,
                        Person.full_name.ilike(f"%{name.split()[-1]}%"),
                    )
                    .first()
                )
                if cp:
                    cp.base_salary_usd        = row.get("salary_usd")
                    cp.total_compensation_usd  = row.get("total_comp_usd")
                    cp.equity_awards_usd       = (row.get("stock_awards_usd") or 0) + (row.get("option_awards_usd") or 0)
                    cp.compensation_year       = year
                    updated += 1

            db.commit()
            if updated > 0:
                break

        return {"status": "ok", "company_id": company_id, "executives_updated": updated}

    async def collect_form4(self, company_id: int, cik: str, company_name: str,
                            db, days_back: int = 730) -> dict:
        """Fetch and parse Form 4 filings, store InsiderTransaction records."""
        from app.core.people_models import InsiderTransaction, Person
        from datetime import datetime, timedelta

        filings = await self.fetcher.get_company_filings(
            cik=cik, filing_types=["4"], limit=50
        )

        # Filter by date if needed
        if days_back:
            cutoff = (datetime.utcnow() - timedelta(days=days_back)).date()
            filings = [f for f in filings if f.filing_date >= cutoff]

        created = 0
        for filing in filings:
            text = await self.fetcher.get_filing_content(filing)
            if not text:
                continue
            try:
                response = await self.llm.complete(
                    prompt=f"{FORM4_PROMPT}\n\n---\n\n{text[:4000]}"
                )
                data = response.parse_json()
            except Exception as e:
                logger.warning(f"Form 4 parse failed: {e}")
                continue

            person_name = data.get("reporting_person", "")
            person = db.query(Person).filter(
                Person.full_name.ilike(f"%{person_name.split()[-1]}%")
            ).first() if person_name else None

            for txn in (data.get("transactions") or []):
                try:
                    txn_date = date.fromisoformat(txn["transaction_date"])
                except (KeyError, ValueError):
                    continue
                existing = db.query(InsiderTransaction).filter(
                    InsiderTransaction.person_id == (person.id if person else None),
                    InsiderTransaction.company_id == company_id,
                    InsiderTransaction.transaction_date == txn_date,
                    InsiderTransaction.shares == txn.get("shares"),
                ).first()
                if not existing:
                    db.add(InsiderTransaction(
                        person_id=person.id if person else None,
                        company_id=company_id,
                        company_name=company_name,
                        transaction_date=txn_date,
                        transaction_type=txn.get("transaction_type"),
                        shares=txn.get("shares"),
                        price_per_share=txn.get("price_per_share"),
                        total_value_usd=txn.get("total_value_usd"),
                        shares_owned_after=txn.get("shares_owned_after"),
                        is_10b5_plan=txn.get("is_10b5_plan", False),
                        form4_url=filing.filing_url,
                        filed_at=filing.filing_date,
                    ))
                    created += 1

        db.commit()
        return {"status": "ok", "company_id": company_id, "transactions_created": created}

    def _extract_comp_section(self, text: str) -> Optional[str]:
        """Find Summary Compensation Table section in proxy text."""
        markers = [
            "summary compensation table",
            "executive compensation table",
            "named executive officer compensation",
        ]
        text_lower = text.lower()
        for marker in markers:
            idx = text_lower.find(marker)
            if idx != -1:
                return text[max(0, idx - 100): idx + 10000]
        return None
