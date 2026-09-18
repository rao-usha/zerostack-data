# SPEC 105 — Quarantine fabricated, demo and misclassified rows

**Status:** Draft
**Task type:** model
**Date:** 2026-09-16
**Test file:** tests/test_spec_105_quarantine_bad_data.py
**Plan:** docs/plans/PLAN_082_pe_rebuild_phase0_phase1.md (Phase 0)

## Goal

Move the verified-bad rows out of the live tables, reversibly, so analytics and APIs stop serving them. The row counts and predicates were checked against the cloud DB on 2026-09-16. Bad rows go to schema `quarantine` and demo rows to schema `demo`. Nothing is hard-deleted. The same migration fixes zombie `ingestion_jobs` and scrubs the plaintext API key. Alembic also starts running at startup.

## Acceptance Criteria

- [ ] `app/core/quarantine.py` provides:
  - an ordered list of `QuarantineRule(name, table, predicate, target_schema, expected, reason)`
  - `apply(conn, rules, dry_run)`: captures all root ids first, then moves each rule's rows. Rows that depend on them through a foreign key are moved first (NO ACTION/RESTRICT/CASCADE children). SET NULL references are backed up.
  - `revert(conn)`: restores everything in reverse manifest order, including SET NULL values.
- [ ] Guard: abort (raise) if any root count is more than 5% above `expected`, or if the total rows moved exceed `MAX_TOTAL_ROWS`.
- [ ] Every moved row keeps all its columns plus `_q_seq`, `_q_rule`, `_q_reason` and `_q_at`. `quarantine.manifest` records `(seq, rule, schema, table, rows)`.
- [ ] Alembic `0003_quarantine_bad_data`:
  - `upgrade` applies the rules
  - it backs up the zombie `ingestion_jobs` statuses to `quarantine.ingestion_jobs_status_backup`, then marks them failed
  - it removes the `api_key` field from `ingestion_jobs.config` (irreversible by design)
  - `downgrade` reverts the moves and the zombie statuses
- [ ] `scripts/quarantine_dry_run.py` runs `apply` inside a transaction, prints a per-rule and per-table report, then rolls back.
- [ ] `app/core/migrate.py` `run_migrations()` runs `alembic upgrade head` under `pg_advisory_lock`. The API lifespan and the worker `main()` call it before `create_tables()`. A failure is logged and does not block startup.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_rules_are_well_formed | Unique names, valid identifiers, schema in {quarantine, demo}, expected > 0 |
| T2 | test_guard_rejects_count_over_tolerance | Raises when live > expected × 1.05 |
| T3 | test_guard_allows_zero_and_exact | 0 (already applied) and exact counts pass |
| T4 | test_apply_moves_rows_and_children_pg | (Postgres) root and FK children move; unrelated rows stay |
| T5 | test_apply_backs_up_set_null_refs_pg | (Postgres) a SET NULL reference is restored by revert |
| T6 | test_revert_restores_everything_pg | (Postgres) row counts and values equal the originals after revert |
| T7 | test_apply_is_idempotent_pg | (Postgres) a second apply moves 0 rows |
| T8 | test_migration_chain | 0003 revises 0002 |
| T9 | test_run_migrations_uses_advisory_lock | `run_migrations` takes and releases a pg advisory lock around `command.upgrade` |

Postgres tests use `TEST_PG_URL` and are skipped when it isn't set. They run against a throwaway container.

## Rubric Checklist

_No model rubric found in memory/rubrics; generic:_
- [ ] Reversible (downgrade tested)
- [ ] Identifiers validated through `safe_sql.qi`; values parameterized
- [ ] Dry run reviewed before running against the cloud DB

## Design Notes

- Root rule ids are captured into temp tables before any move, because some predicates reference rows that an earlier rule moves (8-K filer companies are found through the 8-K deals).
- The FK graph is read from `pg_constraint` at run time, so new child tables are handled automatically.
- Target tables use `CREATE TABLE IF NOT EXISTS <schema>.<t> (LIKE public.<t>)`, without defaults or indexes, so they don't depend on public sequences.

## Files to Create/Modify

| File | Action |
|------|--------|
| app/core/quarantine.py | Create |
| alembic/versions/0003_quarantine_bad_data.py | Create |
| scripts/quarantine_dry_run.py | Create |
| app/core/migrate.py | Create |
| app/main.py, app/worker/main.py | Call run_migrations at startup |

## Feedback History

_No corrections yet._
