"""
SEC-derived analytical views.

`public_company_financials` is a denormalized annual-financials view that
v1 synthetic generators and downstream PE scorers query. It joins
sec_income_statement with derived EBITDA (operating_income + D&A from
sec_financial_facts where available).
"""

import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


CREATE_PUBLIC_COMPANY_FINANCIALS_VIEW_SQL = """
DROP VIEW IF EXISTS public_company_financials;

CREATE VIEW public_company_financials AS
SELECT
    s.cik,
    s.company_name,
    s.ticker,
    s.fiscal_period,
    s.fiscal_year,
    s.period_end_date,
    s.period_start_date,
    s.filing_date,
    s.form_type,
    s.revenues AS revenue_usd,
    s.gross_profit AS gross_profit_usd,
    s.operating_income AS operating_income_usd,
    (COALESCE(s.operating_income, 0) + COALESCE(da.da_value, 0)) AS ebitda_usd,
    s.net_income AS net_income_usd,
    s.cost_of_revenue AS cost_of_revenue_usd,
    s.operating_expenses AS operating_expenses_usd,
    s.income_tax_expense AS income_tax_expense_usd,
    s.interest_expense AS interest_expense_usd,
    s.accession_number,
    -- PLAN_062 rev_02 Step 1b: NAICS-2 from sec_company_metadata for TabDDPM conditioning
    m.sic_code,
    m.sic_description,
    m.naics_2,
    m.state_of_incorporation,
    m.business_state
FROM sec_income_statement s
LEFT JOIN (
    SELECT
        cik,
        fiscal_year,
        fiscal_period,
        SUM(value) AS da_value
    FROM sec_financial_facts
    WHERE fact_name IN (
        'DepreciationDepletionAndAmortization',
        'DepreciationAndAmortization',
        'Depreciation'
    )
    AND unit = 'USD'
    GROUP BY cik, fiscal_year, fiscal_period
) da
    ON da.cik = s.cik
    AND da.fiscal_year = s.fiscal_year
    AND da.fiscal_period = s.fiscal_period
LEFT JOIN sec_company_metadata m
    ON m.cik = s.cik
WHERE s.revenues IS NOT NULL
  AND s.revenues > 0;
"""


def create_public_company_financials_view(engine: Engine) -> None:
    """Create or refresh the public_company_financials view. Idempotent."""
    try:
        with engine.begin() as conn:
            conn.execute(text(CREATE_PUBLIC_COMPANY_FINANCIALS_VIEW_SQL))
        logger.info("public_company_financials view created/refreshed")
    except Exception as exc:
        logger.error("Failed to create public_company_financials view: %s", exc)
        raise
