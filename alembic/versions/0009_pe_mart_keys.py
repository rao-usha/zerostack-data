"""PE mart keys: natural-key upserts for SEC-derived firms and funds (SPEC_117)

Revision ID: 0009_pe_mart_keys
Revises: 0008_entity_master
Create Date: 2026-09-20

pe_firms has no unique constraint at all today (not even on name), so an
upsert by CRD/CIK would silently fan out duplicates. pe_funds.firm_id is
NOT NULL, which would force inventing a parent firm for the ~85% of Form D
funds that cannot be attributed to a manager.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0009_pe_mart_keys"
down_revision: Union[str, None] = "0008_entity_master"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    # natural keys for the SEC-derived rows (partial: hand-entered rows have neither)
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pe_firms_crd_number ON pe_firms (crd_number) "
    "WHERE crd_number IS NOT NULL",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pe_firms_cik ON pe_firms (cik) WHERE cik IS NOT NULL",
    # the Form D issuer CIK is the fund vehicle's natural key
    "ALTER TABLE pe_funds ADD COLUMN IF NOT EXISTS cik VARCHAR(10)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pe_funds_cik ON pe_funds (cik) WHERE cik IS NOT NULL",
    # a fund with no attributable GP is loaded unlinked rather than given a fake parent
    "ALTER TABLE pe_funds ALTER COLUMN firm_id DROP NOT NULL",
]

DOWNGRADE_SQL = [
    "DROP INDEX IF EXISTS uq_pe_firms_crd_number",
    "DROP INDEX IF EXISTS uq_pe_firms_cik",
    "DROP INDEX IF EXISTS uq_pe_funds_cik",
    "DELETE FROM pe_funds WHERE firm_id IS NULL",
    "ALTER TABLE pe_funds ALTER COLUMN firm_id SET NOT NULL",
    "ALTER TABLE pe_funds DROP COLUMN IF EXISTS cik",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
