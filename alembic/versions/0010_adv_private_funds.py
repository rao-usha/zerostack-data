"""Form ADV Schedule D 7.B.(1) private funds (SPEC_118)

Revision ID: 0010_adv_private_funds
Revises: 0009_pe_mart_keys
Create Date: 2026-09-20

Stored at filing grain, not fund grain: each monthly ADV release carries only
that month's filings, so loading the 2025-01..2026-08 window in any order must
produce the same answer, and a restated release must not be able to regress a
fund to an older filing. The current-state mart is derived from these rows.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0010_adv_private_funds"
down_revision: Union[str, None] = "0009_pe_mart_keys"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    # --- filings: the only place a CRD appears (7B1 itself carries none) ----
    """
    CREATE TABLE IF NOT EXISTS public.sec_adv_filings (
        filing_id BIGINT PRIMARY KEY,
        crd_number TEXT NOT NULL,
        adviser_type TEXT NOT NULL,
        sec_number TEXT,
        legal_name TEXT,
        form_version TEXT,
        filed_at TIMESTAMP,
        source_release_key TEXT,
        loaded_at TIMESTAMP DEFAULT NOW()
    )
    """,
    # the mart picks a fund's winning filing by newest filed_at per adviser
    "CREATE INDEX IF NOT EXISTS ix_sec_adv_filings_crd_filed "
    "ON public.sec_adv_filings (crd_number, filed_at DESC)",
    # --- 7B1 rows, one per fund per filing ---------------------------------
    """
    CREATE TABLE IF NOT EXISTS public.sec_adv_private_fund_filings (
        filing_id BIGINT NOT NULL,
        private_fund_id TEXT NOT NULL,
        adviser_type TEXT NOT NULL,
        fund_name TEXT,
        fund_name_core TEXT,
        reference_id TEXT,
        org_state TEXT,
        org_country TEXT,
        excl_3c1 BOOLEAN,
        excl_3c7 BOOLEAN,
        is_master_fund BOOLEAN,
        is_feeder_fund BOOLEAN,
        master_fund_name TEXT,
        master_fund_id TEXT,
        is_fund_of_funds BOOLEAN,
        invests_in_self_or_related BOOLEAN,
        invests_in_securities BOOLEAN,
        fund_type TEXT,
        fund_type_other TEXT,
        gross_asset_value NUMERIC,
        minimum_investment NUMERIC,
        owners INTEGER,
        pct_owned_you_or_related NUMERIC(6, 3),
        pct_owned_funds NUMERIC(6, 3),
        sales_limited BOOLEAN,
        pct_owned_non_us NUMERIC(6, 3),
        has_subadviser BOOLEAN,
        other_ias_advise BOOLEAN,
        clients_solicited BOOLEAN,
        pct_invested NUMERIC(6, 3),
        exempt_from_registration BOOLEAN,
        annual_audit BOOLEAN,
        gaap BOOLEAN,
        fs_distributed BOOLEAN,
        unqualified_opinion TEXT,
        prime_brokers BOOLEAN,
        custodians BOOLEAN,
        administrator BOOLEAN,
        pct_assets_valued NUMERIC(6, 3),
        marketing BOOLEAN,
        source_release_key TEXT,
        loaded_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (filing_id, private_fund_id)
    )
    """,
    # fund_type is deliberately unconstrained: an SEC vocabulary addition
    # must widen the data, not fail the load.
    "CREATE INDEX IF NOT EXISTS ix_sec_adv_pff_fund_id "
    "ON public.sec_adv_private_fund_filings (private_fund_id)",
    "CREATE INDEX IF NOT EXISTS ix_sec_adv_pff_name_core "
    "ON public.sec_adv_private_fund_filings (fund_name_core) WHERE fund_name_core IS NOT NULL",
    # --- current state, rebuilt by the mart (never written by the loader) ---
    """
    CREATE TABLE IF NOT EXISTS public.sec_adv_private_funds (
        crd_number TEXT NOT NULL,
        private_fund_id TEXT NOT NULL,
        filing_id BIGINT,
        adviser_type TEXT,
        as_of_date DATE NOT NULL,
        fund_name TEXT,
        fund_name_core TEXT,
        fund_type TEXT,
        fund_type_other TEXT,
        gross_asset_value NUMERIC,
        is_master_fund BOOLEAN,
        is_feeder_fund BOOLEAN,
        master_fund_id TEXT,
        master_fund_name TEXT,
        org_state TEXT,
        org_country TEXT,
        excl_3c1 BOOLEAN,
        excl_3c7 BOOLEAN,
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (crd_number, private_fund_id)
    )
    """,
    # the attribution index looks up by fund id, then by core name
    "CREATE INDEX IF NOT EXISTS ix_sec_adv_pf_fund_id "
    "ON public.sec_adv_private_funds (private_fund_id)",
    "CREATE INDEX IF NOT EXISTS ix_sec_adv_pf_name_core "
    "ON public.sec_adv_private_funds (fund_name_core) WHERE fund_name_core IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_sec_adv_pf_pe_types "
    "ON public.sec_adv_private_funds (fund_type) "
    "WHERE fund_type IN ('Private Equity Fund', 'Venture Capital Fund')",
    # --- pe_funds: how a fund got its firm, and its ADV identity -----------
    "ALTER TABLE pe_funds ADD COLUMN IF NOT EXISTS firm_link_method VARCHAR(32)",
    "ALTER TABLE pe_funds ADD COLUMN IF NOT EXISTS sec_fund_id VARCHAR(20)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pe_funds_sec_fund_id "
    "ON pe_funds (sec_fund_id) WHERE sec_fund_id IS NOT NULL",
]

DOWNGRADE_SQL = [
    "DROP INDEX IF EXISTS uq_pe_funds_sec_fund_id",
    "ALTER TABLE pe_funds DROP COLUMN IF EXISTS sec_fund_id",
    "ALTER TABLE pe_funds DROP COLUMN IF EXISTS firm_link_method",
    "DROP TABLE IF EXISTS public.sec_adv_private_funds",
    "DROP TABLE IF EXISTS public.sec_adv_private_fund_filings",
    "DROP TABLE IF EXISTS public.sec_adv_filings",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
