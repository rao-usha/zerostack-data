"""SPEC_119: GP people from Form D related persons.

Adds the identity key and evidence columns the people mart needs, plus the
unique indexes a set-based merge requires -- `pe_people` had none but `id` and
`linkedin_url`, and `pe_firm_people` had none on `(firm_id, person_id)`.

`uq_pe_people_source_key` is PARTIAL so the 2,551 legacy rows (all
`source_key` NULL) are untouched and need no backfill; `merge_staging` repeats
the predicate through `conflict_where`, the same way `pe_funds.cik` works.

`uq_pe_firm_people_firm_person` is FULL, not partial: the pair should be
unique for every writer of that table, not just this one. Verified safe before
writing -- the upgrade asserts it rather than trusting the check.
"""

from alembic import op

revision = "0011_pe_people_sec"
down_revision = "0010_adv_private_funds"
branch_labels = None
depends_on = None


UPGRADE_SQL = [
    # --- identity + provenance -------------------------------------------
    "ALTER TABLE pe_people ADD COLUMN IF NOT EXISTS source_key TEXT",
    # pe_people already carries data_sources JSON and quarantine.py already
    # matches on CAST(data_sources AS TEXT) LIKE for demo_people/sec_adv_firms,
    # so no second provenance column is introduced here.
    "ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS data_source TEXT",
    "ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS person_link_method VARCHAR(32)",
    "ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS firm_link_method VARCHAR(32)",
    "ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS address_confirmation VARCHAR(16)",
    "ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS fund_count INTEGER",
    "ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS filing_count INTEGER",
    "ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS first_seen DATE",
    "ALTER TABLE pe_firm_people ADD COLUMN IF NOT EXISTS last_seen DATE",
    # --- the firm-level flag ---------------------------------------------
    "ALTER TABLE pe_firms ADD COLUMN IF NOT EXISTS is_spv_platform BOOLEAN",
    # --- merge keys -------------------------------------------------------
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pe_people_source_key "
    "ON pe_people (source_key) WHERE source_key IS NOT NULL",
    # Fail loudly rather than build a broken index: a duplicate pair here
    # would mean another writer has been creating them and this constraint
    # would silently change that writer's behaviour.
    """
    DO $$
    DECLARE dupes INTEGER;
    BEGIN
        SELECT count(*) INTO dupes FROM (
            SELECT firm_id, person_id FROM pe_firm_people
            GROUP BY 1, 2 HAVING count(*) > 1) d;
        IF dupes > 0 THEN
            RAISE EXCEPTION
                'pe_firm_people has % duplicate (firm_id, person_id) pairs; '
                'dedupe before adding the unique index', dupes;
        END IF;
    END $$;
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pe_firm_people_firm_person "
    "ON pe_firm_people (firm_id, person_id)",
    "CREATE INDEX IF NOT EXISTS ix_pe_firm_people_link_method "
    "ON pe_firm_people (person_link_method)",
]

DOWNGRADE_SQL = [
    "DELETE FROM pe_firm_people WHERE data_source = 'SEC Form D'",
    "DELETE FROM pe_people WHERE source_key LIKE 'secformd:%'",
    "DROP INDEX IF EXISTS ix_pe_firm_people_link_method",
    "DROP INDEX IF EXISTS uq_pe_firm_people_firm_person",
    "DROP INDEX IF EXISTS uq_pe_people_source_key",
    "ALTER TABLE pe_firms DROP COLUMN IF EXISTS is_spv_platform",
    "ALTER TABLE pe_firm_people DROP COLUMN IF EXISTS last_seen",
    "ALTER TABLE pe_firm_people DROP COLUMN IF EXISTS first_seen",
    "ALTER TABLE pe_firm_people DROP COLUMN IF EXISTS filing_count",
    "ALTER TABLE pe_firm_people DROP COLUMN IF EXISTS fund_count",
    "ALTER TABLE pe_firm_people DROP COLUMN IF EXISTS address_confirmation",
    "ALTER TABLE pe_firm_people DROP COLUMN IF EXISTS firm_link_method",
    "ALTER TABLE pe_firm_people DROP COLUMN IF EXISTS person_link_method",
    "ALTER TABLE pe_firm_people DROP COLUMN IF EXISTS data_source",
    "ALTER TABLE pe_people DROP COLUMN IF EXISTS source_key",
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
