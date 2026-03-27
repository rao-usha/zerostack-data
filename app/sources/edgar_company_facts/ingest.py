"""
EDGAR Company Facts Ingestor — persists public company quarterly financials.
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public_company_financials (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    cik VARCHAR(20),
    company_name VARCHAR(255),
    period_end_date DATE NOT NULL,
    fiscal_period VARCHAR(10),
    revenue_usd NUMERIC,
    gross_profit_usd NUMERIC,
    net_income_usd NUMERIC,
    ebitda_usd NUMERIC,
    eps_basic NUMERIC,
    eps_diluted NUMERIC,
    data_source VARCHAR(50) DEFAULT 'edgar_xbrl',
    filed_at DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(ticker, period_end_date, fiscal_period)
);
CREATE INDEX IF NOT EXISTS idx_pcf_ticker ON public_company_financials(ticker);
CREATE INDEX IF NOT EXISTS idx_pcf_period ON public_company_financials(period_end_date);
"""

UPSERT_SQL = """
INSERT INTO public_company_financials
    (ticker, cik, company_name, period_end_date, fiscal_period,
     revenue_usd, gross_profit_usd, net_income_usd, ebitda_usd,
     eps_basic, data_source, filed_at, updated_at)
VALUES
    (%(ticker)s, %(cik)s, %(company_name)s, %(period_end_date)s, %(fiscal_period)s,
     %(revenue_usd)s, %(gross_profit_usd)s, %(net_income_usd)s, %(ebitda_usd)s,
     %(eps_basic)s, %(data_source)s, %(filed_at)s, NOW())
ON CONFLICT (ticker, period_end_date, fiscal_period)
DO UPDATE SET
    revenue_usd = COALESCE(EXCLUDED.revenue_usd, public_company_financials.revenue_usd),
    gross_profit_usd = COALESCE(EXCLUDED.gross_profit_usd, public_company_financials.gross_profit_usd),
    net_income_usd = COALESCE(EXCLUDED.net_income_usd, public_company_financials.net_income_usd),
    eps_basic = COALESCE(EXCLUDED.eps_basic, public_company_financials.eps_basic),
    filed_at = COALESCE(EXCLUDED.filed_at, public_company_financials.filed_at),
    updated_at = NOW();
"""


class EDGARCompanyFactsIngestor:
    """Persists EDGAR company facts to the database."""

    def __init__(self, db_conn):
        self.db = db_conn

    def ensure_table(self):
        with self.db.cursor() as cur:
            cur.execute(TABLE_DDL)
        self.db.commit()

    def upsert_records(self, records: list[dict]) -> dict:
        """Upsert a list of financial records. Returns {inserted, updated, skipped}."""
        self.ensure_table()
        inserted = 0
        errors = 0

        with self.db.cursor() as cur:
            for rec in records:
                # Skip records with no useful data
                if not any([rec.get("revenue_usd"), rec.get("net_income_usd")]):
                    continue
                try:
                    cur.execute(UPSERT_SQL, rec)
                    inserted += 1
                except Exception as e:
                    logger.error(f"Upsert error for {rec.get('ticker')} {rec.get('period_end_date')}: {e}")
                    errors += 1

        self.db.commit()
        return {"inserted": inserted, "errors": errors}
