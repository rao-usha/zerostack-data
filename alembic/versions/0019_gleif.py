"""GLEIF LEI records: gleif_lei_record, gleif_fetch (SPEC_154)

Revision ID: 0019_gleif
Revises: 0018_ats_boards
Create Date: 2026-10-03

gleif_lei_record: one row per LEI (ISO 17442) read from the GLEIF API (CC0): legal name, legal
and headquarters address, jurisdiction, registration authority ids, legal form, entity and
registration status and dates, the managing LOU, the golden copy publish date of the run that
last saw it. The third-party mapping fields GLEIF serves (S&P CIQ ids, OpenCorporates ids, BIC)
are not stored.

gleif_fetch: one row per applied run (outcome complete | partial | refused | error | running).
max(golden_copy_publish_date) of complete runs is the dataset's declared clock (catalog dataset
``gleif_lei_records``).

New tables only: no lock on any existing table. Idempotent (IF NOT EXISTS).
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0019_gleif"
down_revision: Union[str, None] = "0018_ats_boards"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    # plain strings, not f-strings: the column dictionary harvests static DDL
    """
    CREATE TABLE IF NOT EXISTS gleif_fetch (
        id SERIAL PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        country VARCHAR(2) NOT NULL,
        outcome VARCHAR(16) NOT NULL,
        pages INTEGER,
        records INTEGER,
        requests INTEGER,
        api_total INTEGER,
        golden_copy_publish_date TIMESTAMPTZ,
        user_agent TEXT NOT NULL,
        terms_citation TEXT,
        error TEXT,
        report JSONB,
        CONSTRAINT ck_gleif_fetch_outcome
            CHECK (outcome IN ('running', 'complete', 'partial', 'refused', 'error'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gleif_lei_record (
        lei VARCHAR(20) PRIMARY KEY,
        legal_name TEXT,
        jurisdiction VARCHAR(16),
        state VARCHAR(2),
        legal_city TEXT,
        legal_region VARCHAR(16),
        legal_state VARCHAR(2),
        legal_postal VARCHAR(32),
        legal_country VARCHAR(2),
        hq_city TEXT,
        hq_region VARCHAR(16),
        hq_state VARCHAR(2),
        hq_postal VARCHAR(32),
        hq_country VARCHAR(2),
        registered_at VARCHAR(16),
        registered_as TEXT,
        legal_form_id VARCHAR(16),
        category VARCHAR(32),
        entity_status VARCHAR(16),
        registration_status VARCHAR(32),
        initial_registration_date TIMESTAMPTZ,
        last_update_date TIMESTAMPTZ,
        next_renewal_date TIMESTAMPTZ,
        managing_lou VARCHAR(20),
        corroboration_level VARCHAR(32),
        golden_copy_publish_date TIMESTAMPTZ,
        last_seen_run_id INTEGER,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_gleif_lei_record_state ON gleif_lei_record (state)",
    "CREATE INDEX IF NOT EXISTS ix_gleif_lei_record_status ON gleif_lei_record (registration_status)",
]

DOWNGRADE_SQL = [
    "DROP TABLE IF EXISTS gleif_lei_record",
    "DROP TABLE IF EXISTS gleif_fetch",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
