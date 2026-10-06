"""ATS board links: rejected boards, retracted postings, EIN on the link (SPEC_156)

Revision ID: 0021_ats_link_precision
Revises: 0020_ats_targets_discovery
Create Date: 2026-10-06

ats_board.status / verification gain 'rejected': a board whose firm link was retracted (a false link
found by review or by re-verification under a stricter rule). The row is kept with its previous link
and evidence under discovery_evidence.retracted; the firm keys are cleared, so nothing joins to it.

ats_posting.status gains 'retracted': the postings of a rejected (or never-linked candidate) board.
Kept as raw rows, never read as open or closed roles.

ats_board.ein: the linked target's EIN, so the SPEC_148 `atsboard` feed can attach an EIN-only
firm's careers domain by EIN (37 of 119 domains had no CIK on 2026-10-06).

Idempotent (IF NOT EXISTS; the CHECKs are dropped and re-created). No table rewrite.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0021_ats_link_precision"
down_revision: Union[str, None] = "0020_ats_targets_discovery"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    # plain strings, not f-strings: the column dictionary harvests static DDL
    "ALTER TABLE ats_board ADD COLUMN IF NOT EXISTS ein TEXT",
    "ALTER TABLE ats_board DROP CONSTRAINT IF EXISTS ck_ats_board_status",
    "ALTER TABLE ats_board ADD CONSTRAINT ck_ats_board_status "
    "CHECK (status IN ('active', 'unverified', 'not_found', 'refused', 'error', 'candidate', 'rejected'))",
    "ALTER TABLE ats_board DROP CONSTRAINT IF EXISTS ck_ats_board_verification",
    "ALTER TABLE ats_board ADD CONSTRAINT ck_ats_board_verification "
    "CHECK (verification IS NULL OR verification IN ('verified', 'candidate', 'rejected'))",
    # VARCHAR(8) -> (16): 'retracted' is 9 characters (a length increase is metadata-only, no rewrite)
    "ALTER TABLE ats_posting ALTER COLUMN status TYPE VARCHAR(16)",
    "ALTER TABLE ats_posting DROP CONSTRAINT IF EXISTS ck_ats_posting_status",
    "ALTER TABLE ats_posting ADD CONSTRAINT ck_ats_posting_status "
    "CHECK (status IN ('open', 'closed', 'retracted'))",
]

DOWNGRADE_SQL = [
    "ALTER TABLE ats_posting DROP CONSTRAINT IF EXISTS ck_ats_posting_status",
    "UPDATE ats_posting SET status = 'closed' WHERE status = 'retracted'",
    "ALTER TABLE ats_posting ADD CONSTRAINT ck_ats_posting_status CHECK (status IN ('open', 'closed'))",
    "ALTER TABLE ats_posting ALTER COLUMN status TYPE VARCHAR(8)",
    "ALTER TABLE ats_board DROP CONSTRAINT IF EXISTS ck_ats_board_verification",
    "ALTER TABLE ats_board DROP CONSTRAINT IF EXISTS ck_ats_board_status",
    "UPDATE ats_board SET status = 'candidate', verification = 'candidate' WHERE status = 'rejected'",
    "ALTER TABLE ats_board ADD CONSTRAINT ck_ats_board_status "
    "CHECK (status IN ('active', 'unverified', 'not_found', 'refused', 'error', 'candidate'))",
    "ALTER TABLE ats_board ADD CONSTRAINT ck_ats_board_verification "
    "CHECK (verification IS NULL OR verification IN ('verified', 'candidate'))",
    "ALTER TABLE ats_board DROP COLUMN IF EXISTS ein",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
