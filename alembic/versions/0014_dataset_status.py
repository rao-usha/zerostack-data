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

The *lock* is not catalog-only: ALTER TABLE takes ACCESS EXCLUSIVE and waits
behind any open transaction on ingestion_jobs (a worker mid-ingest), while
every later query on the table waits behind it. So every attempt runs under
a short lock_timeout and is retried (see LOCK_TIMEOUT). Deploying with the
workers stopped avoids the wait entirely.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0014_dataset_status"
down_revision = "0013_mart_build"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Lock wait per attempt and number of attempts. ALTER TABLE needs an ACCESS
# EXCLUSIVE lock: on the hot ingestion_jobs table it can queue behind a
# worker's open transaction, and every later reader then queues behind the
# ALTER. A short lock_timeout gives up instead; each failed attempt is rolled
# back to its subtransaction (releasing every lock it took), sleeps and
# retries. After the last attempt the migration fails, and startup refuses to
# serve with the columns missing (app.core.migrate.verify_mapped_columns).
LOCK_TIMEOUT = "3s"
LOCK_ATTEMPTS = 10
RETRY_SLEEP_S = 2

def upgrade_sql(lock_timeout: str = LOCK_TIMEOUT, attempts: int = LOCK_ATTEMPTS,
                sleep_s: float = RETRY_SLEEP_S) -> str:
    return f"""
    DO $$
    DECLARE
        attempt INT := 0;
    BEGIN
        LOOP
            BEGIN
                PERFORM set_config('lock_timeout', '{lock_timeout}', true);
                IF to_regclass('public.ingestion_jobs') IS NOT NULL THEN
                    ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS dataset_key VARCHAR(64);
                    CREATE INDEX IF NOT EXISTS ix_ingestion_jobs_dataset_key
                        ON ingestion_jobs (dataset_key);
                END IF;
                IF to_regclass('public.collection_audit_log') IS NOT NULL THEN
                    ALTER TABLE collection_audit_log ADD COLUMN IF NOT EXISTS actor VARCHAR(255);
                END IF;
                EXIT;
            EXCEPTION WHEN lock_not_available THEN
                attempt := attempt + 1;
                IF attempt >= {int(attempts)} THEN
                    RAISE;
                END IF;
                RAISE NOTICE '0014: lock wait timed out (attempt %), retrying', attempt;
                PERFORM pg_sleep({float(sleep_s)});
            END;
        END LOOP;
        PERFORM set_config('lock_timeout', '0', true);
    END $$;
    """


UPGRADE_SQL = [upgrade_sql()]

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
