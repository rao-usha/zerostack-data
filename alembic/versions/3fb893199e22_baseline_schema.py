"""baseline schema

Revision ID: 3fb893199e22
Revises:
Create Date: 2026-02-22 01:21:28.707926

This is the baseline migration. It represents all tables that exist in the
codebase as of this commit. Existing databases should be stamped with:

    alembic stamp head

New databases use create_all() as a fallback (see app/main.py lifespan),
then get stamped automatically. Future migrations build on this baseline.
"""
from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = '3fb893199e22'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Baseline — all tables already exist via create_all() or prior deployment.
    # This migration is intentionally empty; it serves as the starting point
    # for Alembic version tracking.
    pass


def downgrade() -> None:
    # Cannot downgrade past baseline.
    pass
