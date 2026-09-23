"""Mart build ledger: core.mart_build (SPEC_126a)

Revision ID: 0013_mart_build
Revises: 0012_access_lockdown
Create Date: 2026-09-23

One row per guarded `pe_mart_build` / `entity_resolve` run: the input releases
it consumed, the code version, per-stage counts, the ship-gate results and the
outcome (`running` -> `success` | `failed` | `refused`). Mirrors
`core.resolve_run`, but for every mart and including the runs that never
built (refused on stale or failed inputs).
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0013_mart_build"
down_revision: Union[str, None] = "0012_access_lockdown"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    "CREATE SCHEMA IF NOT EXISTS core",
    """
    CREATE TABLE IF NOT EXISTS core.mart_build (
        id BIGSERIAL PRIMARY KEY,
        mart VARCHAR(64) NOT NULL,
        status VARCHAR(16) NOT NULL DEFAULT 'running',
        dry_run BOOLEAN NOT NULL DEFAULT FALSE,
        started_at TIMESTAMP NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMP,
        code_version VARCHAR(64),
        inputs JSONB,
        stage_counts JSONB,
        gate_results JSONB,
        overrides JSONB,
        refusal_reason TEXT,
        error TEXT,
        ingestion_job_id INTEGER,
        job_queue_id INTEGER,
        CONSTRAINT ck_mart_build_status
            CHECK (status IN ('running', 'success', 'failed', 'refused'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_core_mart_build_mart_started "
    "ON core.mart_build (mart, started_at DESC)",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS core.mart_build")
