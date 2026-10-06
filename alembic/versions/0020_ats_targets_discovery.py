"""ATS board discovery over the PE targets universe (SPEC_155)

Revision ID: 0020_ats_targets_discovery
Revises: 0019_gleif
Create Date: 2026-10-05

ats_board gains the firm link a target discovery writes (target_id = workbench.targets_universe
target id), how the board was verified (verification verified|candidate, verification_score, a
ranking aid, verified_at) and the careers domain read off a verified board with its evidence
(a single-family SPEC_148 claim). Status 'candidate' = a board that exists and names the firm
weakly: NO firm link (core_entity_id / cik / target_id stay NULL), the proposed firm(s) are in
discovery_evidence.

ats_discovery_run: one row per discovery run (params, status, per-site checkpoint = the last
target_id done, metrics refreshed as the run goes).

ats_discovery_attempt: one row per (target_id, ats_type, board_token) ever asked: the resumable
ledger. Final outcomes (fetched with a verdict, not_found) are never asked again; transient
ones (timeouts, 5xx, Retry-After) are retried by the next run. Rejected pairs live ONLY here.

Columns are added nullable without defaults and the two tables are new: no table rewrite.
Idempotent (IF NOT EXISTS; the status CHECK is dropped and re-created).
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0020_ats_targets_discovery"
down_revision: Union[str, None] = "0019_gleif"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    # plain strings, not f-strings: the column dictionary harvests static DDL
    "ALTER TABLE ats_board ADD COLUMN IF NOT EXISTS target_id BIGINT",
    "ALTER TABLE ats_board ADD COLUMN IF NOT EXISTS verification VARCHAR(16)",
    "ALTER TABLE ats_board ADD COLUMN IF NOT EXISTS verification_score NUMERIC(4, 3)",
    "ALTER TABLE ats_board ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP",
    "ALTER TABLE ats_board ADD COLUMN IF NOT EXISTS careers_domain TEXT",
    "ALTER TABLE ats_board ADD COLUMN IF NOT EXISTS careers_domain_evidence JSONB",
    "ALTER TABLE ats_board DROP CONSTRAINT IF EXISTS ck_ats_board_status",
    "ALTER TABLE ats_board ADD CONSTRAINT ck_ats_board_status "
    "CHECK (status IN ('active', 'unverified', 'not_found', 'refused', 'error', 'candidate'))",
    "ALTER TABLE ats_board DROP CONSTRAINT IF EXISTS ck_ats_board_verification",
    "ALTER TABLE ats_board ADD CONSTRAINT ck_ats_board_verification "
    "CHECK (verification IS NULL OR verification IN ('verified', 'candidate'))",
    "CREATE INDEX IF NOT EXISTS ix_ats_board_target ON ats_board (target_id) WHERE target_id IS NOT NULL",
    """
    CREATE TABLE IF NOT EXISTS ats_discovery_run (
        run_id SERIAL PRIMARY KEY,
        started_at TIMESTAMP NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMP,
        heartbeat_at TIMESTAMP,
        status VARCHAR(24) NOT NULL,
        params JSONB,
        checkpoint JSONB NOT NULL DEFAULT '{}'::jsonb,
        metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
        CONSTRAINT ck_ats_discovery_run_status
            CHECK (status IN ('running', 'done', 'paused', 'budget_exhausted', 'failed'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ats_discovery_attempt (
        id BIGSERIAL PRIMARY KEY,
        run_id INTEGER REFERENCES ats_discovery_run (run_id),
        target_id BIGINT NOT NULL,
        core_entity_id BIGINT,
        ats_type VARCHAR(16) NOT NULL,
        board_token TEXT NOT NULL,
        variant VARCHAR(16),
        outcome VARCHAR(40) NOT NULL,
        http_status INTEGER,
        requests SMALLINT NOT NULL DEFAULT 0,
        verdict VARCHAR(16),
        reason VARCHAR(40),
        score NUMERIC(4, 3),
        board_id INTEGER,
        postings INTEGER,
        evidence JSONB,
        error TEXT,
        attempted_at TIMESTAMP NOT NULL,
        CONSTRAINT uq_ats_discovery_attempt UNIQUE (target_id, ats_type, board_token),
        CONSTRAINT ck_ats_discovery_attempt_verdict
            CHECK (verdict IS NULL OR verdict IN ('verified', 'candidate', 'rejected'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_ats_discovery_attempt_token ON ats_discovery_attempt (ats_type, board_token)",
    "CREATE INDEX IF NOT EXISTS ix_ats_discovery_attempt_verdict ON ats_discovery_attempt (verdict) "
    "WHERE verdict IS NOT NULL",
]

DOWNGRADE_SQL = [
    "DROP TABLE IF EXISTS ats_discovery_attempt",
    "DROP TABLE IF EXISTS ats_discovery_run",
    "DROP INDEX IF EXISTS ix_ats_board_target",
    "ALTER TABLE ats_board DROP CONSTRAINT IF EXISTS ck_ats_board_verification",
    "ALTER TABLE ats_board DROP CONSTRAINT IF EXISTS ck_ats_board_status",
    "UPDATE ats_board SET status = 'unverified' WHERE status = 'candidate'",
    "ALTER TABLE ats_board ADD CONSTRAINT ck_ats_board_status "
    "CHECK (status IN ('active', 'unverified', 'not_found', 'refused', 'error'))",
    "ALTER TABLE ats_board DROP COLUMN IF EXISTS careers_domain_evidence",
    "ALTER TABLE ats_board DROP COLUMN IF EXISTS careers_domain",
    "ALTER TABLE ats_board DROP COLUMN IF EXISTS verified_at",
    "ALTER TABLE ats_board DROP COLUMN IF EXISTS verification_score",
    "ALTER TABLE ats_board DROP COLUMN IF EXISTS verification",
    "ALTER TABLE ats_board DROP COLUMN IF EXISTS target_id",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
