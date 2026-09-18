"""Bulk ingestion framework: raw/stg schemas + raw.source_release (SPEC_107)

Revision ID: 0004_bulk_framework
Revises: 0003_quarantine_bad_data
Create Date: 2026-09-16
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0004_bulk_framework"
down_revision: Union[str, None] = "0003_quarantine_bad_data"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SOURCE_RELEASE_DDL = [
    "CREATE SCHEMA IF NOT EXISTS raw",
    "CREATE SCHEMA IF NOT EXISTS stg",
    """
    CREATE TABLE IF NOT EXISTS raw.source_release (
        id BIGSERIAL PRIMARY KEY,
        source VARCHAR(64) NOT NULL,
        release_key VARCHAR(200) NOT NULL,
        url TEXT NOT NULL,
        status VARCHAR(16) NOT NULL DEFAULT 'discovered',
        local_path TEXT,
        bytes BIGINT,
        sha256 CHAR(64),
        etag TEXT,
        last_modified TEXT,
        rows_loaded JSONB,
        parser_version VARCHAR(32),
        error TEXT,
        discovered_at TIMESTAMP NOT NULL DEFAULT NOW(),
        fetched_at TIMESTAMP,
        loaded_at TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_source_release UNIQUE (source, release_key),
        CONSTRAINT ck_source_release_status CHECK (status IN ('discovered', 'fetched', 'staged', 'loaded', 'failed'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_source_release_status ON raw.source_release (source, status)",
]


def upgrade() -> None:
    for stmt in SOURCE_RELEASE_DDL:
        op.execute(stmt)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS raw.source_release")
    op.execute("DROP SCHEMA IF EXISTS stg CASCADE")
    op.execute("DROP SCHEMA IF EXISTS raw")
