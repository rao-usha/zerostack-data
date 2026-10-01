"""Entity master web domains: core.domain_link, core.domain_probe (SPEC_148)

Revision ID: 0017_domain_link
Revises: 0016_entity_weak_match
Create Date: 2026-09-30

core.domain_link: one row per (registrable domain, subject) the resolver derived.
A subject is `ent:<entity_id>` or a lone source record_key. `status` is
strong (one subject, >= 2 independent source families) | weak (one family) |
conflict (several subjects, or the claim could not be attached) | alias (the
domain redirects to `alias_of`; `evidence` holds the probe). `claims` lists every
record that named the domain, with its source, family and how it was attached
(member / key / name / redirect:<domain> / probe). A domain is never a merge key.

core.domain_probe: append-only evidence from the domain probe collector
(app/entities/domain_probe.py): one row per request outcome, with the hops, the
user agent and the terms review it ran under.

New tables only: no lock on any existing table. Idempotent (IF NOT EXISTS).
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0017_domain_link"
down_revision: Union[str, None] = "0016_entity_weak_match"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPGRADE_SQL = [
    "CREATE SCHEMA IF NOT EXISTS core",
    # plain strings, not f-strings: the column dictionary harvests static DDL
    """
    CREATE TABLE IF NOT EXISTS core.domain_link (
        domain TEXT NOT NULL,
        subject TEXT NOT NULL,
        entity_id BIGINT,
        record_key TEXT,
        status VARCHAR(16) NOT NULL,
        families TEXT[] NOT NULL DEFAULT '{}',
        sources TEXT[] NOT NULL DEFAULT '{}',
        claims JSONB NOT NULL DEFAULT '[]'::jsonb,
        claim_count INTEGER NOT NULL DEFAULT 0,
        conflicts JSONB NOT NULL DEFAULT '[]'::jsonb,
        alias_of TEXT,
        evidence JSONB,
        resolver_version VARCHAR(16),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (domain, subject),
        CONSTRAINT ck_core_domain_link_status
            CHECK (status IN ('strong', 'weak', 'conflict', 'alias'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_core_domain_link_entity ON core.domain_link (entity_id) "
    "WHERE entity_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_core_domain_link_status ON core.domain_link (status)",
    """
    CREATE TABLE IF NOT EXISTS core.domain_probe (
        domain TEXT NOT NULL,
        probed_at TIMESTAMP NOT NULL,
        url TEXT NOT NULL,
        outcome VARCHAR(32) NOT NULL,
        http_status INTEGER,
        redirect_to TEXT,
        final_url TEXT,
        hops JSONB NOT NULL DEFAULT '[]'::jsonb,
        names_checked JSONB NOT NULL DEFAULT '[]'::jsonb,
        names_found JSONB NOT NULL DEFAULT '[]'::jsonb,
        retry_after_seconds DOUBLE PRECISION,
        user_agent TEXT NOT NULL,
        terms_citation TEXT,
        error TEXT,
        PRIMARY KEY (domain, probed_at)
    )
    """,
]

DOWNGRADE_SQL = [
    "DROP TABLE IF EXISTS core.domain_probe",
    "DROP TABLE IF EXISTS core.domain_link",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
