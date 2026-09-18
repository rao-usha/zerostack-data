"""XBRL statement tables keyed on their own period (SPEC_113)

The old unique keys (cik, period_end_date, fiscal_year, fiscal_period) used the
FILING's fy/fp, so comparatives and YTD durations landed in the wrong rows.
Every existing row is mis-keyed: upgrade moves them to
quarantine.<table>_pre_period_fix and installs the new keys:

    sec_income_statement     (cik, period_end_date, period_start_date)
    sec_cash_flow_statement  (cik, period_end_date, period_start_date)
    sec_balance_sheet        (cik, period_end_date)

Idempotent: rows are only quarantined while the OLD key is still present, and
tables that don't exist yet (fresh DB; create_all runs after migrations) are
skipped.

Revision ID: 0006_xbrl_period_keys
Revises: 0005_bulk_source_tables
Create Date: 2026-09-16
"""
from typing import List, Sequence, Tuple, Union

from alembic import op

revision: str = "0006_xbrl_period_keys"
down_revision: Union[str, None] = "0005_bulk_source_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# (table, (old constraint name, old columns), (new constraint name, new columns))
KEYS: List[Tuple[str, Tuple[str, str], Tuple[str, str]]] = [
    (
        "sec_income_statement",
        ("uq_sec_income_cik_period", "cik, period_end_date, fiscal_year, fiscal_period"),
        ("uq_sec_income_cik_period_bounds", "cik, period_end_date, period_start_date"),
    ),
    (
        "sec_balance_sheet",
        ("uq_sec_balance_cik_period", "cik, period_end_date, fiscal_year, fiscal_period"),
        ("uq_sec_balance_cik_period_end", "cik, period_end_date"),
    ),
    (
        "sec_cash_flow_statement",
        ("uq_sec_cashflow_cik_period", "cik, period_end_date, fiscal_year, fiscal_period"),
        ("uq_sec_cashflow_cik_period_bounds", "cik, period_end_date, period_start_date"),
    ),
]

_HAS_START = {"sec_income_statement", "sec_cash_flow_statement"}


def _upgrade_table(table: str, old: Tuple[str, str], new: Tuple[str, str]) -> str:
    old_name, _ = old
    new_name, new_cols = new
    add_start = (
        f"ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS period_start_date DATE;"
        if table in _HAS_START else ""
    )
    not_null = (
        f"ALTER TABLE public.{table} ALTER COLUMN period_start_date SET NOT NULL;"
        if table in _HAS_START else ""
    )
    # Identifiers are module constants (no user input).
    return f"""
DO $mig$
BEGIN
    IF to_regclass('public.{table}') IS NULL THEN
        RETURN;
    END IF;
    {add_start}
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conrelid = 'public.{table}'::regclass AND conname = '{old_name}')
       OR to_regclass('public.{old_name}') IS NOT NULL THEN
        IF to_regclass('quarantine.{table}_pre_period_fix') IS NULL THEN
            CREATE TABLE quarantine.{table}_pre_period_fix AS SELECT * FROM public.{table};
        ELSE
            INSERT INTO quarantine.{table}_pre_period_fix SELECT * FROM public.{table};
        END IF;
        DELETE FROM public.{table};
    END IF;
    ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS {old_name};
    DROP INDEX IF EXISTS public.{old_name};
    {not_null}
    ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS {new_name};
    DROP INDEX IF EXISTS public.{new_name};
    ALTER TABLE public.{table} ADD CONSTRAINT {new_name} UNIQUE ({new_cols});
END
$mig$
"""


def _downgrade_table(table: str, old: Tuple[str, str], new: Tuple[str, str]) -> str:
    old_name, old_cols = old
    new_name, _ = new
    drop_not_null = (
        f"ALTER TABLE public.{table} ALTER COLUMN period_start_date DROP NOT NULL;"
        if table in _HAS_START else ""
    )
    return f"""
DO $mig$
BEGIN
    IF to_regclass('public.{table}') IS NULL THEN
        RETURN;
    END IF;
    ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS {new_name};
    DROP INDEX IF EXISTS public.{new_name};
    IF to_regclass('quarantine.{table}_post_period_fix') IS NULL THEN
        CREATE TABLE quarantine.{table}_post_period_fix AS SELECT * FROM public.{table};
    ELSE
        INSERT INTO quarantine.{table}_post_period_fix SELECT * FROM public.{table};
    END IF;
    DELETE FROM public.{table};
    {drop_not_null}
    IF to_regclass('quarantine.{table}_pre_period_fix') IS NOT NULL THEN
        INSERT INTO public.{table} SELECT * FROM quarantine.{table}_pre_period_fix;
        DROP TABLE quarantine.{table}_pre_period_fix;
    END IF;
    ALTER TABLE public.{table} DROP CONSTRAINT IF EXISTS {old_name};
    ALTER TABLE public.{table} ADD CONSTRAINT {old_name} UNIQUE ({old_cols});
END
$mig$
"""


UPGRADE_SQL: List[str] = ["CREATE SCHEMA IF NOT EXISTS quarantine"] + [
    _upgrade_table(t, old, new) for t, old, new in KEYS
]
DOWNGRADE_SQL: List[str] = ["CREATE SCHEMA IF NOT EXISTS quarantine"] + [
    _downgrade_table(t, old, new) for t, old, new in KEYS
]


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)
