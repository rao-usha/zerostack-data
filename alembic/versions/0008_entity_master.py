"""Entity master: core schema for identifier-only resolution (SPEC_116)

Revision ID: 0008_entity_master
Revises: 0007_bulk_gap_fixes
Create Date: 2026-09-18

Gives the loaded SEC data a join layer: source records carrying strong
identifiers (EIN/CIK/CRD/LEI/UEI), the entities they resolve into, and the
CIK<->CRD bridge built from 13F cover pages.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0008_entity_master"
down_revision: Union[str, None] = "0007_bulk_gap_fixes"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

CORE_DDL = [
    "CREATE SCHEMA IF NOT EXISTS core",
    # --- staging: one row per source record -------------------------------
    """
    CREATE TABLE IF NOT EXISTS core.source_record (
        record_key TEXT PRIMARY KEY,
        source VARCHAR(32) NOT NULL,
        native_id TEXT,
        legal_name TEXT,
        name_norm TEXT,
        name_norm_version VARCHAR(8),
        ein TEXT,
        cik TEXT,
        crd TEXT,
        lei TEXT,
        uei TEXT,
        state_entity_id TEXT,
        state VARCHAR(2),
        zip5 VARCHAR(5),
        domain TEXT,
        observed_at TIMESTAMP,
        loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_core_source_record_source ON core.source_record (source)",
    "CREATE INDEX IF NOT EXISTS ix_core_source_record_cik ON core.source_record (cik) WHERE cik IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_core_source_record_crd ON core.source_record (crd) WHERE crd IS NOT NULL",
    # --- resolved entities -------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core.entity (
        entity_id BIGSERIAL PRIMARY KEY,
        entity_type VARCHAR(16) NOT NULL DEFAULT 'org',
        canonical_name TEXT,
        ein TEXT,
        cik TEXT,
        crd TEXT,
        lei TEXT,
        uei TEXT,
        state_entity_id TEXT,
        canonical_state VARCHAR(2),
        canonical_zip5 VARCHAR(5),
        canonical_domain TEXT,
        member_count INTEGER NOT NULL DEFAULT 0,
        strong_key_count INTEGER NOT NULL DEFAULT 0,
        match_tier VARCHAR(16),
        field_conflicts JSONB,
        last_members JSONB,
        resolver_version VARCHAR(16),
        dissolved_at TIMESTAMP,
        superseded_by BIGINT,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_core_entity_cik ON core.entity (cik) WHERE cik IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_core_entity_crd ON core.entity (crd) WHERE crd IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_core_entity_live ON core.entity (entity_id) WHERE dissolved_at IS NULL",
    # --- membership: which record belongs to which entity ------------------
    """
    CREATE TABLE IF NOT EXISTS core.membership (
        record_key TEXT PRIMARY KEY REFERENCES core.source_record (record_key) ON DELETE CASCADE,
        entity_id BIGINT NOT NULL,
        match_tier VARCHAR(16) NOT NULL,
        match_method VARCHAR(32) NOT NULL,
        features JSONB,
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_core_membership_entity ON core.membership (entity_id)",
    # --- identifier index (the lookup surface) -----------------------------
    """
    CREATE TABLE IF NOT EXISTS core.identifier (
        id_type VARCHAR(16) NOT NULL,
        id_value TEXT NOT NULL,
        entity_id BIGINT NOT NULL,
        sources TEXT[],
        first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
        last_seen TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (id_type, id_value)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_core_identifier_entity ON core.identifier (entity_id)",
    # --- names seen for an entity -----------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core.alias (
        entity_id BIGINT NOT NULL,
        name_norm TEXT NOT NULL,
        name TEXT,
        source VARCHAR(32) NOT NULL,
        PRIMARY KEY (entity_id, name_norm, source)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_core_alias_name_norm ON core.alias (name_norm)",
    # --- human overrides ---------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core.key_veto (
        key_type VARCHAR(24) NOT NULL,
        key_value TEXT NOT NULL,
        record_key TEXT NOT NULL DEFAULT '*',
        reason TEXT NOT NULL,
        actor TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        revoked_at TIMESTAMP,
        PRIMARY KEY (key_type, key_value, record_key)
    )
    """,
    # --- append-only history + run ledger ----------------------------------
    """
    CREATE TABLE IF NOT EXISTS core.entity_merge (
        id BIGSERIAL PRIMARY KEY,
        absorbed_entity_id BIGINT NOT NULL,
        survivor_entity_id BIGINT NOT NULL,
        cause JSONB,
        merged_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.resolve_run (
        run_at TIMESTAMP PRIMARY KEY,
        resolver_version VARCHAR(16) NOT NULL,
        dry_run BOOLEAN NOT NULL DEFAULT FALSE,
        duration_seconds NUMERIC(10, 2),
        metrics JSONB NOT NULL
    )
    """,
    # --- CIK <-> CRD bridge -------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS core.cik_crd_bridge (
        cik TEXT PRIMARY KEY,
        crd TEXT NOT NULL,
        tier VARCHAR(24) NOT NULL,
        tier_rank SMALLINT NOT NULL,
        matched_on TEXT,
        evidence JSONB NOT NULL,
        crd_cik_count INTEGER NOT NULL DEFAULT 1,
        builder_version VARCHAR(16) NOT NULL,
        built_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_core_bridge_crd ON core.cik_crd_bridge (crd)",
    """
    CREATE TABLE IF NOT EXISTS core.cik_crd_bridge_refused (
        cik TEXT PRIMARY KEY,
        reason VARCHAR(32) NOT NULL,
        tier VARCHAR(24),
        candidates JSONB,
        built_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,
]


def upgrade() -> None:
    for stmt in CORE_DDL:
        op.execute(stmt)


def downgrade() -> None:
    op.execute("DROP SCHEMA IF EXISTS core CASCADE")
