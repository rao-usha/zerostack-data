"""Entity master weak tier: core.weak_match (SPEC_147)

Revision ID: 0016_entity_weak_match
Revises: 0015_catalog_rights_review
Create Date: 2026-09-30

Flagged name+state candidates for Form 5500 sponsors (and any later weak-tier
source). A row is EVIDENCE, never a merge: the resolver stays identifier-only.
`status` is corroborates | candidate | ambiguous | conflict; `conflicts` lists
each contradiction (ein_conflict, strong_match_elsewhere) with its values.

A new table: no lock on any existing table. Every statement is idempotent
(IF NOT EXISTS), so a rerun is a no-op. No ORM model (like core.mart_build).
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0016_entity_weak_match"
down_revision: Union[str, None] = "0015_catalog_rights_review"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    "CREATE SCHEMA IF NOT EXISTS core",
    # a plain string, not an f-string: the column dictionary harvests static DDL
    """
    CREATE TABLE IF NOT EXISTS core.weak_match (
        record_key TEXT NOT NULL,
        candidate_record_key TEXT NOT NULL,
        candidate_entity_id BIGINT,
        tier VARCHAR(16) NOT NULL,
        status VARCHAR(16) NOT NULL,
        conflicts JSONB NOT NULL DEFAULT '[]'::jsonb,
        matched_on JSONB,
        resolver_version VARCHAR(16),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (record_key, candidate_record_key),
        CONSTRAINT ck_core_weak_match_status
            CHECK (status IN ('corroborates', 'candidate', 'ambiguous', 'conflict'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_core_weak_match_entity ON core.weak_match (candidate_entity_id) "
    "WHERE candidate_entity_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_core_weak_match_status ON core.weak_match (status)",
]

DOWNGRADE_SQL = [
    "DROP TABLE IF EXISTS core.weak_match",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
