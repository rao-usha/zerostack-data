"""ATS job boards: ats_board, ats_posting, ats_board_fetch (SPEC_151)

Revision ID: 0018_ats_boards
Revises: 0017_domain_link
Create Date: 2026-10-02

ats_board: one row per public job board (Greenhouse / Lever / Ashby token) with its company
link (core entity id, CIK, industrial_companies id -- plain columns, no foreign key into
other domains), how the token was found (ats_config | seed | domain_slug | name_slug), the
verification evidence and the terms review it ran under.

ats_posting: one row per posting per board, first_seen_at / last_seen_at / closed_at /
status kept by the collector (hiring velocity, team growth), pay with its provenance
(source structured|text, the raw snippet, a confidence, the parser version). Description
text is stored with e-mail addresses and phone numbers redacted.

ats_board_fetch: append-only, one row per fetch: outcome, open/new/closed/reopened counts,
bytes, sha256, user agent, terms citation. max(fetched_at) of fetched rows is the lane's
declared clock (catalog dataset ``ats_boards``).

New tables only: no lock on any existing table. Idempotent (IF NOT EXISTS).
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0018_ats_boards"
down_revision: Union[str, None] = "0017_domain_link"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    # plain strings, not f-strings: the column dictionary harvests static DDL
    """
    CREATE TABLE IF NOT EXISTS ats_board (
        id SERIAL PRIMARY KEY,
        ats_type VARCHAR(16) NOT NULL,
        board_token TEXT NOT NULL,
        company_name TEXT,
        core_entity_id BIGINT,
        cik TEXT,
        industrial_company_id INTEGER,
        discovery_basis VARCHAR(16) NOT NULL,
        discovery_evidence JSONB,
        status VARCHAR(16) NOT NULL,
        terms_citation TEXT,
        first_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
        last_fetched_at TIMESTAMP,
        last_outcome VARCHAR(40),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_ats_board UNIQUE (ats_type, board_token),
        CONSTRAINT ck_ats_board_type CHECK (ats_type IN ('greenhouse', 'lever', 'ashby')),
        CONSTRAINT ck_ats_board_status
            CHECK (status IN ('active', 'unverified', 'not_found', 'refused', 'error')),
        CONSTRAINT ck_ats_board_basis
            CHECK (discovery_basis IN ('ats_config', 'seed', 'domain_slug', 'name_slug'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_ats_board_entity ON ats_board (core_entity_id) "
    "WHERE core_entity_id IS NOT NULL",
    """
    CREATE TABLE IF NOT EXISTS ats_posting (
        id BIGSERIAL PRIMARY KEY,
        board_id INTEGER NOT NULL REFERENCES ats_board (id),
        external_id TEXT NOT NULL,
        title TEXT NOT NULL,
        department TEXT,
        team TEXT,
        location TEXT,
        locations_all JSONB NOT NULL DEFAULT '[]'::jsonb,
        employment_type TEXT,
        workplace_type TEXT,
        posted_at TIMESTAMP,
        source_updated_at TIMESTAMP,
        source_url TEXT,
        description_text TEXT,
        description_sha256 TEXT,
        pay_min NUMERIC,
        pay_max NUMERIC,
        pay_currency VARCHAR(3),
        pay_interval VARCHAR(8),
        pay_kind VARCHAR(16),
        pay_source VARCHAR(16),
        pay_snippet TEXT,
        pay_confidence VARCHAR(8),
        pay_parser VARCHAR(16),
        status VARCHAR(8) NOT NULL DEFAULT 'open',
        first_seen_at TIMESTAMP NOT NULL,
        last_seen_at TIMESTAMP NOT NULL,
        closed_at TIMESTAMP,
        CONSTRAINT uq_ats_posting UNIQUE (board_id, external_id),
        CONSTRAINT ck_ats_posting_status CHECK (status IN ('open', 'closed'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_ats_posting_board_status ON ats_posting (board_id, status)",
    "CREATE INDEX IF NOT EXISTS ix_ats_posting_first_seen ON ats_posting (first_seen_at)",
    """
    CREATE TABLE IF NOT EXISTS ats_board_fetch (
        id BIGSERIAL PRIMARY KEY,
        board_id INTEGER NOT NULL REFERENCES ats_board (id),
        fetched_at TIMESTAMP NOT NULL,
        url TEXT NOT NULL,
        outcome VARCHAR(40) NOT NULL,
        http_status INTEGER,
        bytes INTEGER,
        body_sha256 TEXT,
        postings_seen INTEGER,
        open_count INTEGER,
        new_count INTEGER,
        closed_count INTEGER,
        reopened_count INTEGER,
        pay_count INTEGER,
        user_agent TEXT NOT NULL,
        terms_citation TEXT,
        error TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_ats_board_fetch_board ON ats_board_fetch (board_id, fetched_at)",
]

DOWNGRADE_SQL = [
    "DROP TABLE IF EXISTS ats_board_fetch",
    "DROP TABLE IF EXISTS ats_posting",
    "DROP TABLE IF EXISTS ats_board",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
