"""Target tables for Phase 1 SEC bulk sources (SPEC_108-112)

Revision ID: 0005_bulk_source_tables
Revises: 0004_bulk_framework
Create Date: 2026-09-16

Applies each listed BulkSource's ddl() (all CREATE ... IF NOT EXISTS).
Sources also call ensure_ddl() at load time, so this migration only makes the
schema exist ahead of the first load. `sec_companyfacts` targets existing
statement tables and is handled by 0006.
"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

revision: str = "0005_bulk_source_tables"
down_revision: Union[str, None] = "0004_bulk_framework"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SOURCES = [
    "sec_form_d",
    "sec_13f",
    "sec_insider",
    "sec_edgar_submissions",
    "sec_adv_roster",
    "sec_iapd_feed",
]

# New tables only (never the pre-existing form_d_filings / sec_form_adv).
NEW_TABLES = [
    "form_d_issuers", "form_d_related_persons", "form_d_offerings", "form_d_signatures",
    "sec_13f_filings", "sec_13f_holdings", "sec_13f_other_managers",
    "sec_insider_filings", "sec_insider_owners", "sec_insider_transactions", "sec_insider_footnotes",
    "sec_filers", "sec_filer_former_names", "sec_8k_index",
    "sec_adv_roster_snapshots", "sec_adv_feed_firm_state",
]


def upgrade() -> None:
    from app.ingest.bulk.registry import get_source

    conn = op.get_bind()
    for name in SOURCES:
        source = get_source(name)
        for stmt in source.ddl():
            conn.execute(text(stmt))


def downgrade() -> None:
    for table in reversed(NEW_TABLES):
        op.execute(f'DROP TABLE IF EXISTS public."{table}"')
