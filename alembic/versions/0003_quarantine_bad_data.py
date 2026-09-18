"""Quarantine fabricated, demo and misclassified rows (SPEC_105, PLAN_082)

Revision ID: 0003_quarantine_bad_data
Revises: 0002_worker_heartbeats
Create Date: 2026-09-16

- Moves verified-bad rows into the `quarantine` / `demo` schemas (reversible).
- Marks zombie ingestion_jobs (pending/blocked/running with no worker since
  2026-04-16) as failed, backing up their previous status.
- Removes plaintext API keys from ingestion_jobs.config (irreversible by design).

Run `python scripts/quarantine_dry_run.py` first to review the moves.
"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

revision: str = "0003_quarantine_bad_data"
down_revision: Union[str, None] = "0002_worker_heartbeats"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

ZOMBIE_REASON = "zombie: no worker since 2026-04-16 (PLAN_082)"


def upgrade() -> None:
    from app.core.quarantine import apply

    conn = op.get_bind()
    result = apply(conn)
    print(f"[0003] quarantined {result['total_rows']} rows")

    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS quarantine.ingestion_jobs_status_backup (
                id INTEGER PRIMARY KEY,
                status TEXT,
                error_message TEXT,
                completed_at TIMESTAMP
            )
            """
        )
    )
    conn.execute(
        text(
            """
            INSERT INTO quarantine.ingestion_jobs_status_backup (id, status, error_message, completed_at)
            SELECT id, status::text, error_message, completed_at
            FROM ingestion_jobs
            WHERE status::text IN ('pending', 'blocked', 'running')
              AND created_at < '2026-09-16'
            ON CONFLICT (id) DO NOTHING
            """
        )
    )
    zombies = conn.execute(
        text(
            """
            UPDATE ingestion_jobs
            SET status = 'failed', error_message = :reason, completed_at = NOW()
            WHERE id IN (SELECT id FROM quarantine.ingestion_jobs_status_backup)
              AND status::text IN ('pending', 'blocked', 'running')
            """
        ),
        {"reason": ZOMBIE_REASON},
    ).rowcount
    print(f"[0003] failed {zombies} zombie ingestion_jobs")

    scrubbed = conn.execute(
        text(
            """
            UPDATE ingestion_jobs
            SET config = (config::jsonb - 'api_key')::json
            WHERE config IS NOT NULL AND config::jsonb ? 'api_key'
            """
        )
    ).rowcount
    print(f"[0003] scrubbed api_key from {scrubbed} ingestion_jobs.config rows")


def downgrade() -> None:
    from app.core.quarantine import revert

    conn = op.get_bind()
    restored = revert(conn)
    print(f"[0003] restored {restored} rows")

    if conn.execute(text("SELECT to_regclass('quarantine.ingestion_jobs_status_backup')")).scalar():
        conn.execute(
            text(
                """
                UPDATE ingestion_jobs j
                SET status = b.status, error_message = b.error_message, completed_at = b.completed_at
                FROM quarantine.ingestion_jobs_status_backup b
                WHERE j.id = b.id AND j.error_message = :reason
                """
            ),
            {"reason": ZOMBIE_REASON},
        )
        conn.execute(text("DROP TABLE quarantine.ingestion_jobs_status_backup"))
