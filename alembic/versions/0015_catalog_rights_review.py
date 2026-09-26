"""Catalog rights review audit trail: catalog_rights_review (SPEC_142)

Revision ID: 0015_catalog_rights_review
Revises: 0014_dataset_status
Create Date: 2026-09-26

One row per human rights decision on a catalog dataset (PLAN_088 §3
"SPEC_135", decision 8): ``confirm_current`` (the current rights block is
right), ``accept_proposal`` (apply the cited proposal in rights.py) or
``reject``. Each row pins the ``rights_hash`` of the block the reviewer saw
and a JSON snapshot of it, the reviewer and the time.

The table is an audit trail, not a switch: ``reviewed`` comes from the
committed ``app/catalog/rights_reviewed.py`` hash (code truth). Rows are
append-only: triggers refuse UPDATE, DELETE and TRUNCATE, so a sign-off cannot be
edited after the fact (a correction is a new row).

A new table: no lock on any existing table. Every statement is idempotent
(IF NOT EXISTS / CREATE OR REPLACE / DROP TRIGGER IF EXISTS), so a rerun is a
no-op. There is no ORM model (like core.mart_build): create_all never
creates a trigger-less copy.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0015_catalog_rights_review"
down_revision: Union[str, None] = "0014_dataset_status"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DECISIONS = ("confirm_current", "accept_proposal", "reject")

UPGRADE_SQL = [
    f"""
    CREATE TABLE IF NOT EXISTS catalog_rights_review (
        id BIGSERIAL PRIMARY KEY,
        dataset_key VARCHAR(64) NOT NULL,
        decision VARCHAR(24) NOT NULL,
        rights_hash CHAR(64) NOT NULL,
        rights_snapshot JSONB NOT NULL,
        proposal_hash CHAR(64),
        proposal_snapshot JSONB,
        reviewer VARCHAR(255) NOT NULL,
        reviewer_user_id INTEGER,
        api_key_id INTEGER,
        note TEXT NOT NULL,
        reviewed_at TIMESTAMP NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_catalog_rights_review_decision
            CHECK (decision IN ({", ".join(repr(d) for d in DECISIONS)})),
        CONSTRAINT ck_catalog_rights_review_proposal
            CHECK ((decision = 'accept_proposal') = (proposal_hash IS NOT NULL)),
        CONSTRAINT ck_catalog_rights_review_note CHECK (length(btrim(note)) >= 10)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_catalog_rights_review_key_at "
    "ON catalog_rights_review (dataset_key, reviewed_at DESC, id DESC)",
    """
    CREATE OR REPLACE FUNCTION catalog_rights_review_append_only() RETURNS trigger AS $$
    BEGIN
        RAISE EXCEPTION 'catalog_rights_review is append-only (SPEC_142): record a new decision instead';
    END;
    $$ LANGUAGE plpgsql
    """,
    "DROP TRIGGER IF EXISTS trg_catalog_rights_review_append_only ON catalog_rights_review",
    """
    CREATE TRIGGER trg_catalog_rights_review_append_only
        BEFORE UPDATE OR DELETE ON catalog_rights_review
        FOR EACH ROW EXECUTE FUNCTION catalog_rights_review_append_only()
    """,
    "DROP TRIGGER IF EXISTS trg_catalog_rights_review_no_truncate ON catalog_rights_review",
    """
    CREATE TRIGGER trg_catalog_rights_review_no_truncate
        BEFORE TRUNCATE ON catalog_rights_review
        FOR EACH STATEMENT EXECUTE FUNCTION catalog_rights_review_append_only()
    """,
]

DOWNGRADE_SQL = [
    "DROP TABLE IF EXISTS catalog_rights_review",
    "DROP FUNCTION IF EXISTS catalog_rights_review_append_only()",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
