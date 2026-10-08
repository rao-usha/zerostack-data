"""cms_medicare_utilization: data_year + unique key, duplicates removed (SPEC_162)

Revision ID: 0022_cms_utilization_year_key
Revises: 0021_ats_link_precision
Create Date: 2026-10-08

The ingest fetched the DKAN *series* id 92396110-2aed-4d63-a6a2-5d6207d46a29, which always serves
the latest release, and stored no year. Live (2026-10-08) the table holds two data years, told apart
by load date (each spot-checked against the pinned per-year DKAN ids):

- loads before 2026-05-21 (the day data.cms.gov published the 2024 release on the series) = 2023;
- later loads = 2024.

Steps, all inside one DO block under a short lock_timeout with retries (0014 pattern):

1. ADD COLUMN IF NOT EXISTS data_year INTEGER (catalog-only, no rewrite);
2. backfill NULL data_year by load date;
3. delete duplicates per (data_year, rndrng_npi, hcpcs_cd, place_of_srvc), NULL-safe, keeping the
   latest ingestion_timestamp (tie: highest id). Live: 40,054 -> 29,189 rows;
4. SET NOT NULL; add uq_cms_medicare_utilization_year_key unless it exists.

Guarded by to_regclass: the table is created by the ingest, not create_all, so a fresh database has
nothing to migrate. Re-running is a no-op. The table is ~40k rows: the dedupe and the unique index
build take well under a second; the lock_timeout only protects against a concurrent ingest.

Downgrade drops the constraint and the column; deleted duplicate rows are not restored.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0022_cms_utilization_year_key"
down_revision: Union[str, None] = "0021_ats_link_precision"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "cms_medicare_utilization"
CONSTRAINT = "uq_cms_medicare_utilization_year_key"
KEY = ("data_year", "rndrng_npi", "hcpcs_cd", "place_of_srvc")
# The day the DKAN series switched from the 2023 to the 2024 release (data.cms.gov data.json,
# "modified" of the 2024 release). No load happened between 2026-02-25 and 2026-09-25.
SERIES_SWITCH_DATE = "2026-05-21"
YEAR_BEFORE_SWITCH = 2023
YEAR_AFTER_SWITCH = 2024

LOCK_TIMEOUT = "3s"
LOCK_ATTEMPTS = 10
RETRY_SLEEP_S = 2


def upgrade_sql(lock_timeout: str = LOCK_TIMEOUT, attempts: int = LOCK_ATTEMPTS,
                sleep_s: float = RETRY_SLEEP_S) -> str:
    key_list = ", ".join(KEY)
    return f"""
    DO $$
    DECLARE
        attempt INT := 0;
        deleted BIGINT;
    BEGIN
        IF to_regclass('public.{TABLE}') IS NULL THEN
            RETURN;
        END IF;
        LOOP
            BEGIN
                PERFORM set_config('lock_timeout', '{lock_timeout}', true);
                LOCK TABLE {TABLE} IN SHARE ROW EXCLUSIVE MODE;
                ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS data_year INTEGER;
                UPDATE {TABLE}
                   SET data_year = CASE WHEN ingestion_timestamp < DATE '{SERIES_SWITCH_DATE}'
                                        THEN {YEAR_BEFORE_SWITCH} ELSE {YEAR_AFTER_SWITCH} END
                 WHERE data_year IS NULL;
                -- PARTITION BY groups NULLs together (NULL-safe key); one hash/sort pass, not a
                -- self-join
                DELETE FROM {TABLE}
                 WHERE id IN (
                     SELECT id FROM (
                         SELECT id, row_number() OVER (
                                    PARTITION BY {key_list}
                                    ORDER BY ingestion_timestamp DESC NULLS LAST, id DESC) AS rn
                           FROM {TABLE}) ranked
                      WHERE rn > 1);
                GET DIAGNOSTICS deleted = ROW_COUNT;
                RAISE NOTICE '0022: % duplicate rows deleted', deleted;
                ALTER TABLE {TABLE} ALTER COLUMN data_year SET NOT NULL;
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{CONSTRAINT}') THEN
                    ALTER TABLE {TABLE} ADD CONSTRAINT {CONSTRAINT} UNIQUE ({key_list});
                END IF;
                EXIT;
            EXCEPTION WHEN lock_not_available THEN
                attempt := attempt + 1;
                IF attempt >= {int(attempts)} THEN
                    RAISE;
                END IF;
                RAISE NOTICE '0022: lock wait timed out (attempt %), retrying', attempt;
                PERFORM pg_sleep({float(sleep_s)});
            END;
        END LOOP;
        PERFORM set_config('lock_timeout', '0', true);
    END $$;
    """


def downgrade_sql() -> str:
    return f"""
    DO $$
    BEGIN
        IF to_regclass('public.{TABLE}') IS NULL THEN
            RETURN;
        END IF;
        PERFORM set_config('lock_timeout', '{LOCK_TIMEOUT}', true);
        ALTER TABLE {TABLE} DROP CONSTRAINT IF EXISTS {CONSTRAINT};
        ALTER TABLE {TABLE} DROP COLUMN IF EXISTS data_year;
        PERFORM set_config('lock_timeout', '0', true);
    END $$;
    """


def upgrade() -> None:
    op.execute(upgrade_sql())


def downgrade() -> None:
    op.execute(downgrade_sql())
