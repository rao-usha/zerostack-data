"""pe_firm_people.role_type (D1, SPEC_103)

Revision ID: 0001_pe_firm_people_role_type
Revises: 3fb893199e22
Create Date: 2026-09-16

The ORM model declares PEFirmPeople.role_type but create_all() never adds
columns to existing tables, so every query on PEFirmPeople failed.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0001_pe_firm_people_role_type"
down_revision: Union[str, None] = "3fb893199e22"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS role_type VARCHAR(50)")


def downgrade() -> None:
    op.execute("ALTER TABLE pe_firm_people DROP COLUMN IF EXISTS role_type")
