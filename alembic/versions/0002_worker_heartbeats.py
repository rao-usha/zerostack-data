"""worker_heartbeats table (D25, SPEC_106)

Revision ID: 0002_worker_heartbeats
Revises: 0001_pe_firm_people_role_type
Create Date: 2026-09-16

Idle workers never touch job_queue.heartbeat_at, so there was no way to tell
whether any worker is alive before launching a batch.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0002_worker_heartbeats"
down_revision: Union[str, None] = "0001_pe_firm_people_role_type"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS worker_heartbeats (
            worker_id VARCHAR(100) PRIMARY KEY,
            hostname VARCHAR(255),
            started_at TIMESTAMP NOT NULL DEFAULT NOW(),
            last_seen_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_worker_heartbeats_last_seen_at "
        "ON worker_heartbeats (last_seen_at)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS worker_heartbeats")
