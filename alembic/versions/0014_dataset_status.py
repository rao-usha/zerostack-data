"""Dataset status API: audit actor + ingestion_jobs.dataset_key (SPEC_124)

Revision ID: 0014_dataset_status
Revises: 0013_mart_build
Create Date: 2026-09-23

- `collection_audit_log.actor`: who triggered a run. The admin-only
  `POST /datasets/{key}/run` records the principal here; the column was
  missing, so the audit log could say what ran but not who ran it.
- `ingestion_jobs.dataset_key` (+ index): the catalog dataset a job
  produced (PLAN_085 "project dataset_key"). The discriminator used to live
  only in `config["dataset"]`, so job history could not be grouped by
  dataset. New jobs get it from an `IngestionJob` before_insert listener;
  history is resolved at read time, so there is no backfill.

Both tables are created by `create_all`, which runs *after* migrations at
startup, so on a fresh database they do not exist yet: every statement is
guarded by `to_regclass`. On the populated live tables, adding a nullable
column without a default is a catalog-only change (no rewrite), and
`ADD COLUMN IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` make a rerun a
no-op. ingestion_jobs is a few thousand rows, so a plain index build is fine.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0014_dataset_status"
down_revision = "0013_mart_build"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    """
    DO $$
    BEGIN
        IF to_regclass('public.collection_audit_log') IS NOT NULL THEN
            ALTER TABLE collection_audit_log ADD COLUMN IF NOT EXISTS actor VARCHAR(255);
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF to_regclass('public.ingestion_jobs') IS NOT NULL THEN
            ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS dataset_key VARCHAR(64);
            CREATE INDEX IF NOT EXISTS ix_ingestion_jobs_dataset_key
                ON ingestion_jobs (dataset_key);
        END IF;
    END $$;
    """,
]

DOWNGRADE_SQL = [
    "DROP INDEX IF EXISTS ix_ingestion_jobs_dataset_key",
    """
    DO $$
    BEGIN
        IF to_regclass('public.ingestion_jobs') IS NOT NULL THEN
            ALTER TABLE ingestion_jobs DROP COLUMN IF EXISTS dataset_key;
        END IF;
        IF to_regclass('public.collection_audit_log') IS NOT NULL THEN
            ALTER TABLE collection_audit_log DROP COLUMN IF EXISTS actor;
        END IF;
    END $$;
    """,
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
