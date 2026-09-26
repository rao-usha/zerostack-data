# SPEC 144 — Per-dataset quality and usage signals

**Status:** Implemented (branch spec-144)
**Task type:** service
**Date:** 2026-09-26
**Plan:** PLAN_088 §3 "SPEC_138" (renumbered to SPEC_144), wave A; evidence `docs/plans/PLAN_088_evidence.json` (`lens:lineage-quality`)
**Builds on:** SPEC_123 (catalog, `resolve_tables`), SPEC_124 (`dataset_status.build_status` verdicts, `ingestion_jobs.dataset_key` from 0014), SPEC_125 (`frontend/status.html`), SPEC_126a (`run_guarded`), SPEC_127 (`is_exportable`, `require_admin_for_writes`)
**Test file:** tests/test_spec_144_catalog_quality_usage.py

## Problem (measured 2026-09-25/26, read-only)

- The DQ framework selects tables with `DatasetRegistry.ingested()`: 147 of 263
  registry rows are excluded, including every SEC bulk table, every PE mart and
  all of `core.*`. Nothing in the bulk runner, the marts or the entity build
  calls `profile_table`.
- 107 of 110 profiles are from 2026-03-11 (no scheduler calls `profile_all_tables`).
- The row-count-drop check reads the "previous" snapshot after the gate has
  just written a new one, so it compares a snapshot with itself.
- The profiling advisory lock key is Python `hash()`, randomised per process,
  and the unlock may run on a different pooled connection.
- Freshness decays to 0 after 168 h whatever the cadence, and is keyed on
  `ingestion_jobs.source` (split jobs never match).
- No catalog entry says anything about quality or who reads it.
- 3,719 of 3,850 `ingestion_jobs` rows have no `dataset_key` (3,396 resolvable now).

## Scope

1. **Catalog-driven DQ selector** (`app/catalog/quality.py::dq_targets`): the
   base tables (`relkind r/p`, never views) that catalog specs resolve to
   (`resolve_tables`), plus the ingested registry rows (legacy tables not in the
   catalog). Used by `profile_all_tables`, `compute_daily_snapshots`,
   `evaluate_all_rules` and `_analyze_missing_coverage`.
2. **Quality gate** (`jobs._run_quality_gate`): tables come from
   `job.dataset_key` (or `dataset_key_for_job(source, config)`) → spec tables
   that exist (at most `GATE_MAX_TABLES`); the registry lookup is the fallback.
   Every table is gated on its own (one failure does not skip the others).
3. **Bug fixes**
   - the previous profile row count is read *before* profiling and passed to
     `check_row_count_delta(previous_count=...)`;
   - the lock is `pg_try_advisory_lock(144, hashtext(table))` on a dedicated
     autocommit connection (same key in every process; unlocked on the same
     connection);
   - freshness comes from the SPEC_124 verdict (`FRESHNESS_BY_STATUS`), which
     already folds in cadence and `slo_lag_hours`; the 168 h decay remains only
     for legacy registry tables with no catalog dataset.
4. **Huge-table guards** in `profile_table`: every statement under
   `SET LOCAL statement_timeout` (`PROFILE_STATEMENT_TIMEOUT_MS`); schema-qualified
   tables (`core.entity`); planner estimate instead of `count(*)` above
   `EXACT_COUNT_MAX_ROWS`; `BERNOULLI(10)` above 1M rows, `SYSTEM(1)` above 20M.
   `evaluate_all_rules` skips catalog-only tables above `RULE_SCAN_MAX_ROWS`.
5. **Post-load hook** (`quality.post_load(engine, producer)`), advisory, never
   raises: called by the bulk runner after a run that loaded releases and by
   `run_guarded` after a successful non-dry-run build (covers `pe_mart_build`
   and `entity_resolve`). Profiles the producer's dataset tables up to
   `POST_LOAD_MAX_ROWS`; larger tables are left to the scheduler.
   `CATALOG_POST_LOAD_PROFILE=0` disables it.
6. **Scheduled profiling** (`profile_stale_catalog_tables`, APScheduler
   `system_catalog_profiling` at 01:00, registered alongside the daily
   snapshots so `main.py` is untouched): profiles each catalog table whose
   profile is older than its cadence, skips tables whose row estimate is
   unchanged since the last profile (until 4× cadence), bounded by
   `max_tables` and a deadline.
7. **Row-count history without a migration**: `dq_quality_snapshots.row_count`
   is now the live row estimate (`pg_stat_user_tables.n_live_tup`, then
   `reltuples`, then the latest profile) taken daily for every catalog table.
   PLAN_088's `catalog_table_stats` table (migration 0018) is not built; this
   wave allows no migration for SPEC_144.
8. **Quality block on `GET /catalog/{key}`** (`quality`): `live_state` (was `data_state`; see Review fixes 4)
   (`phantom | empty | populated | defective | seed_contaminated`, with reasons),
   latest DQ score and components, rule pass/fail (latest result per rule and
   table in 7 days, failing rule names), open anomalies, profile age and staleness
   (older than 2× cadence), null % on primary-key / key columns, all-NULL columns,
   seed rows, 30-day row trend, metadata completeness score. Cached 60 s; every
   statement under a 3 s timeout; failure → `quality: null`.
   - `empty` uses an exact `count(*)` at or below 1M estimated rows (the planner
     said ~982 for an empty `container_freight_index`).
   - `defective`: a primary-key or curated key column (`KEY_COLUMNS`,
     PLAN_088 §1.3) is 100 % NULL in the latest profile, or ≥ 90 % of the
     non-bookkeeping columns are.
   - `seed_contaminated`: rows whose `source` matches `%sample%`, `%\_seed` or
     `nrel_reference` (scanned only at or below 5M estimated rows). Flag only:
     nothing is deleted or moved (user decision).
9. **Usage** (`app/catalog/usage_build.py` → `app/catalog/usage.json`): a static
   scan of string literals in `app/api/v1`, `app/services`, `app/graphql`,
   `app/reports`, `app/marts`, `app/entities` (catalog modules excluded) for
   `FROM`/`JOIN` table references (kept only when the name is a table or view
   declared in code) and ORM model class references. Keyed by table, so it does
   not depend on the catalog. At read time `pg_depend` adds view → base table
   edges (cached 10 min). `consumers` on `GET /catalog/{key}`; `GET
   /catalog/usage` lists every dataset's consumers and the tables views read
   that no spec declares (e.g. `sec_company_metadata`). Regenerate with
   `python -m app.catalog.usage_build`; a drift test fails when stale.
10. **`dataset_key` backfill** (approved, PLAN_088 open decision 10):
    `app/catalog/backfill_dataset_key.py` (`python -m app.catalog.backfill_dataset_key
    [--apply]`) and `POST /catalog/admin/backfill-dataset-key?apply=` (admin).
    Resolves with `job_keys.dataset_key_for_job`, updates only NULL rows in
    batches of 500 under `lock_timeout` with retries, never overwrites; returns
    a count report (resolved per dataset, ambiguous, unresolved per source).
    Dry run against live on 2026-09-26: 3,719 NULL → 3,396 resolvable,
    1 ambiguous (`job:pe_mart_build`), 322 unresolved (missing aliases:
    SPEC_143).
11. **`GET /catalog/row-trends?days=30`** and a row-count sparkline in
    `frontend/status.html` (inline SVG built from numbers only; the tooltip is
    escaped).

## Out of scope

`catalog_table_stats` (0018), alias additions to `job_keys.py` (SPEC_143),
lineage graph (SPEC_143), column-doc percentage source (SPEC_137; the
completeness check reads an optional `column_doc_pct`), making DQ failures fail
jobs, deleting or moving seed/fabricated rows.

## Tests (`tests/test_spec_144_catalog_quality_usage.py`)

- selector includes `form_d_filings`, `pe_firms`, `core.entity`, never views
- row delta uses the prior snapshot; lock blocks across connections; no `hash(`
- freshness follows the verdict map
- `live_state` fixtures: `eia_steo` phantom, `fbi_crime_*` empty,
  `usaspending_awards` defective, `substation` (si_grid_infrastructure) seed_contaminated
- completeness score deterministic
- `usage.json` drift gate; view dependencies; `public_company_financials` consumers
- backfill: dry run counts, apply, idempotent rerun, never overwrites
- quality block / row-trends / usage endpoints; status.html sparkline helper under node

## Review fixes (spec-144-fix)

1. **Gate table choice** (`quality.gate_tables_for_job`): the gate checks the
   table the job loaded. Order: the source's registry row updated since the job
   started, tables the config names (`table`, `table_name`, `tables`), else the
   dataset's tables whose name tokens best match the config values and the
   source suffix (`acs5`+`2023`+`B01001` → `acs5_2023_b01001`; `dataset=oes` →
   `bls_oes`; `dunl:ports` → `dunl_ports`). Only when nothing specific is known:
   the latest-updated registry table, then the catalog order. Max 6.
2. **Gate off the event loop**: `_run_quality_gate` runs the blocking work with
   `asyncio.to_thread` (the session is handed over; the caller awaits), so the
   worker heartbeat keeps running. Tables above `GATE_PROFILE_MAX_ROWS`
   (= `POST_LOAD_MAX_ROWS`, 2M) are not profiled or counted by the gate; the row
   delta uses the planner estimate and the scheduler profiles them.
3. **Usage map**: SQL references also count when the name is a table a spec
   declares or matches a spec's `table_patterns` (generic-ingestor tables such as
   `fdic_bank_financials`, `bea_regional`, `acs5_*`); `app/core` is scanned
   (kind `core_service`; model modules, `schemas.py`, `database.py`,
   `migrate.py` excluded); `consumers_for` follows views over views.
4. **One vocabulary**: the block's field is `live_state` (`LIVE_STATES`), not
   `data_state` (SPEC_141 owns `DatasetSpec.data_state`). `verified_state` is
   SPEC_141's `data_state` when present, else `CURATED_DATA_STATES` (the values
   SPEC_141 sets). A verified `fabricated | seeded | sample_mixed | placeholder |
   demo` makes the live state `seed_contaminated`; `key_columns_null` makes it
   `defective`. `flags` lists every finding (verified state, `key_columns_null`,
   `all_null_columns`, `seed_rows`), so defective does not hide seeded.
5. **Seed markers**: `source`, `data_source` and `origin` columns;
   `%sample%`, `%\_seed`, `nrel_reference`, `demo_seeder`, `gjf_expanded`. A
   skipped (>5M rows) or failed scan is a reason. `quality_flags` (static) is on
   every `GET /catalog` entry and the detail, and status.html shows them as
   escaped chips next to the dataset name.
6. **Shared tables** (SPEC_141 `row_filters`, read with `getattr` until it
   merges): a filtered table gets filtered counts (None when not exact), filtered
   seed scans, and no whole-table profile facts, score, rules or row trend;
   `row_trends` leaves such datasets out; `dq_targets` marks `shared_by` and
   `row_filtered`.
7. **Raw values**: profiles of tables whose dataset has `pii_class != 'none'` or
   `redistribution = 'restricted'` store no `top_values`
   (`top_values_withheld: true`), nor do name/email/phone/linkedin/address columns
   anywhere; the policy fails closed. `GET /data-quality/profiles/{t}/columns`
   strips `top_values` for non-admins on those tables and columns (older profiles).
8. **Cheap GET**: a plain `GET /catalog/{key}` quality block runs no count or
   seed scan: rows come from the detail's `live` counts (or the estimate), seed
   rows from the latest profile (the profiler stores `seed_rows` on the marker
   column). `refresh=true` (admin) scans. The block says `measured: cached|scan`.

Not done here: applying the `dataset_key` backfill on live (writes are out of
bounds for this session; run `python -m app.catalog.backfill_dataset_key
--apply` after SPEC_143's aliases); `catalog_table_stats` (no migration this
wave); `column_doc_pct` wiring (SPEC_137); flags on each source's own data
routers (dozens of routers other specs own).
