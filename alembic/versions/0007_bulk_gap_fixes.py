"""Bulk loader gap fixes: shared 8-K key + stale financial facts (SPEC_115)

Revision ID: 0007_bulk_gap_fixes
Revises: 0006_xbrl_period_keys
Create Date: 2026-09-18

- sec_8k_index was keyed on accession_number alone, so an 8-K filed for several
  companies kept only one of them. Re-key on (accession_number, cik).
- sec_financial_facts still holds rows labelled with the FILING's fy/fp (the D6
  bug fixed in SPEC_113). Quarantine them; the sec_companyfacts loader now
  repopulates the concepts the public_company_financials view reads.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0007_bulk_gap_fixes"
down_revision: Union[str, None] = "0006_xbrl_period_keys"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    "CREATE SCHEMA IF NOT EXISTS quarantine",
    # --- stale facts (pre-SPEC_113 period labels) ---------------------------
    """
    CREATE TABLE IF NOT EXISTS quarantine.sec_financial_facts_pre_period_fix
    AS SELECT * FROM public.sec_financial_facts WHERE false
    """,
    """
    INSERT INTO quarantine.sec_financial_facts_pre_period_fix
    SELECT * FROM public.sec_financial_facts
    """,
    "DELETE FROM public.sec_financial_facts",
    # --- one row per (8-K, company) -----------------------------------------
    "ALTER TABLE public.sec_8k_index DROP CONSTRAINT IF EXISTS sec_8k_index_pkey",
    """
    ALTER TABLE public.sec_8k_index
    ADD CONSTRAINT sec_8k_index_pkey PRIMARY KEY (accession_number, cik)
    """,
]

DOWNGRADE_SQL = [
    "ALTER TABLE public.sec_8k_index DROP CONSTRAINT IF EXISTS sec_8k_index_pkey",
    """
    DELETE FROM public.sec_8k_index a
    USING public.sec_8k_index b
    WHERE a.accession_number = b.accession_number AND a.cik > b.cik
    """,
    """
    ALTER TABLE public.sec_8k_index
    ADD CONSTRAINT sec_8k_index_pkey PRIMARY KEY (accession_number)
    """,
    "DELETE FROM public.sec_financial_facts",
    """
    INSERT INTO public.sec_financial_facts
    SELECT * FROM quarantine.sec_financial_facts_pre_period_fix
    """,
    "DROP TABLE IF EXISTS quarantine.sec_financial_facts_pre_period_fix",
]


def _run(statements) -> None:
    conn = op.get_bind()
    from sqlalchemy import text

    if not conn.execute(text("SELECT to_regclass('public.sec_8k_index')")).scalar():
        # Fresh database: the bulk loader creates the table with the right key
        statements = [s for s in statements if "sec_8k_index" not in s]
    if not conn.execute(text("SELECT to_regclass('public.sec_financial_facts')")).scalar():
        statements = [s for s in statements if "sec_financial_facts" not in s]
    for stmt in statements:
        conn.execute(text(stmt))


def upgrade() -> None:
    _run(UPGRADE_SQL)


def downgrade() -> None:
    _run(DOWNGRADE_SQL)
