# SPEC 129 — CI and publish guards

**Status:** Draft
**Task type:** bug_fix
**Date:** 2026-09-23
**Plan:** PLAN_086 ("Now: stop silent loss and exposure")
**Review:** `docs/reviews/2026-09-23_daas_platform_review.md` §5 findings 9, 11, 17, 19, 21 and §5 minor list
**Test file:** tests/test_spec_129_ci_and_publish_guards.py

## Goal

Stop the ways data can vanish, or look wrong, without anyone being told:

1. **CI has not protected anything.** It runs Python 3.10 (Docker is 3.11),
   never sets `TEST_PG_URL` so all 15 PG-backed spec test files skip, never
   runs a migration, has no secret scan, and its "smoke test" is `sleep 5`.
2. **A 13F zip without INFOTABLE empties `sec_13f_holdings`** while the release
   is marked `loaded`. `iter_tsv` yields nothing for a missing member, 0 rows
   merge, then the prune deletes every older release in the same transaction.
   Header drift is just as silent: a renamed column loads as NULL.
3. **`pe_mart_build` with `dry_run=true` writes** `pe_firms` and
   `sec_adv_private_funds` (only the funds and people stages honour it).
4. **Value errors in the PE API:** `total` ignores the `strategy` filter, and
   `float(x) if x else None` reports a real 0 as null.

## Acceptance Criteria

- [ ] CI runs on Python 3.11, sets `TEST_PG_URL` to the service Postgres, and
      brings a blank database to `alembic` head before tests.
- [ ] CI has a blocking secret-scan job that scans only the push / PR commit
      range, so the known (rotated) historical leak cannot fail every build.
- [ ] The Docker smoke step checks something real (the image imports the app).
- [ ] A 13F release missing a required member or header **fails** (release
      `failed`, transaction rolled back, holdings untouched).
- [ ] Form D: a required member missing a required header fails the release.
- [ ] Publish guard: a destructive replace aborts when the new row count is 0
      or drops more than 50% versus what is published now, unless overridden.
      Applied to the 13F holdings prune, the ADV private-funds mart delete and
      the IAPD feed edition prune.
- [ ] `dry_run=true` leaves every mart table byte-for-byte unchanged and still
      reports what would change for each stage.
- [ ] The `pe_mart_build` job summary counts links across **all** tiers.
- [ ] `GET /pe/firms/?strategy=` returns a `total` that honours the filter.
- [ ] No `float(x) if x else None` remains in `app/api/v1/pe_*.py`.

## Design

### Publish guard (`app/core/copy_loader.py`)

```python
class PublishGuardError(RuntimeError)
def check_publish(target, new_rows, current_rows, max_drop=0.5, override=None) -> None
```

- `new_rows == 0` -> raise (an empty replacement is never a real release).
- `current_rows > 0 and new_rows < current_rows * (1 - max_drop)` -> raise.
- `override` defaults to env `BULK_PUBLISH_GUARD_OVERRIDE`: `1`/`all`, or a
  comma list of target names (`sec_13f_holdings`). An operator who knows a
  release is legitimately smaller sets it for one run.

The guard raises inside the release transaction, so `run_source` rolls the
whole release back and records `status='failed'` with the message. Nothing
publishes.

Where it is applied — every path that deletes published rows because a new
batch replaced them:

| Path | new_rows | current_rows |
|---|---|---|
| 13F holdings prune (`sec_13f/source.py`) | holdings rows that survive the prune | holdings rows before this release merged |
| ADV private funds mart (`marts/adv_private_funds.py`) | staged candidates | rows in `sec_adv_private_funds` |
| IAPD feed prune (`sec_form_adv/iapd_feed.py`) | rows of the new edition | rows before the load |

Not guarded, deliberately: the 8-K index date-window prune (age-based, not a
replacement) and the insider per-accession child cleanup (scoped to accessions
the new release restates).

For 13F the comparison is "what will be published" vs "what was published":
with one holdings release kept, a new quarter replaces the old one, so the
right denominator is the count before the merge (old quarter), not after (old
+ new, which would make every normal release look like a 50% drop).

### Parse guards

- 13F `iter_tsv`: module tables `REQUIRED_MEMBERS` and `REQUIRED_HEADERS`,
  looked up by member name, so every caller is guarded. A missing required
  member, or a present member whose header lacks a required column, raises
  `ValueError`. Required members: SUBMISSION, COVERPAGE, INFOTABLE (read only
  when holdings load). SUMMARYPAGE, SIGNATURE, OTHERMANAGER, OTHERMANAGER2
  stay optional (SPEC_109 tests load a zip without them), but if present
  their key headers must be there. Required headers = keys + what downstream
  reads, not every column (FIGI, CRDNUMBER are absent in older data sets).
- Form D `iter_tsv`: `REQUIRED_HEADERS` by member, checked against the exact
  DictReader field names (the iterators read exact keys). Missing members
  already raised; optional members (RECIPIENTS, SIGNATURES) stay optional.
  `TESTORLIVE` is required: without it every submission would be skipped as
  non-LIVE.

### dry_run (`app/worker/executors/pe_marts.py`)

Every stage runs on **one** connection inside **one** transaction that is
rolled back at the end. Firms and ADV private funds really write inside it, so
their counts are exactly what a real run would change, and the funds/people
stages see the would-be firm ids — then all of it is discarded. The job
message is prefixed `DRY RUN`. Side effects that survive a rollback: the
`pe_firms.id` sequence advances, and firm rows are row-locked for the duration.

### Job summary

`linked` = sum of every `linked_*` key in the funds stats (adv_exact,
adv_family, adv_platform, name_core, related_person), not two of five.

### pe_firms

One `WHERE` list + params built once, used by both the page query and the
count. Zero coercion: `float(x) if x is not None else None` (mechanical
rewrite across `pe_*.py`; computed locals checked by hand for divide-by-zero).

### CI

- `scripts/ci/bootstrap_db.py`: `create_tables()` then `alembic upgrade head`.
  The baseline migration is empty (it assumes `create_all` ran), so upgrade
  alone cannot run on a blank DB. Verified locally on a fresh `qtest_129_mig`.
  Found and fixed on the way: 0003 (quarantine) crashed on tables that do not
  exist yet (ingestor-created); `quarantine.apply` now skips absent tables.
- `alembic check` runs non-blocking: it currently reports 7 indexes that the
  migrations create but the models do not declare (follow-up, needs model
  edits).
- gitleaks: pinned binary (v8.21.2, no action licence needed), full-history
  checkout, `gitleaks git --log-opts=<base>..<head>` over only the commits the
  push/PR adds; blocking. `.gitleaks.toml` extends the default rules and
  allowlists the three known historical leak commits (a1483fc, 0360e3c,
  cda25ea — all to be rotated) plus local-dev placeholder credentials.
- Test DBs: `DATABASE_URL` -> `nexdata_test` (bootstrapped), `TEST_PG_URL` ->
  a separate `nexdata_spec` (spec tests drop/create their own tables).
- Docker smoke: import `app.main` (asserts >100 routes) and `app.worker.main`
  inside the built image, instead of `sleep 5`.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | `test_13f_missing_infotable_raises` | synthetic zip without INFOTABLE -> ValueError |
| T2 | `test_13f_header_drift_raises` | INFOTABLE lacking CUSIP -> ValueError |
| T3 | `test_13f_valid_zip_parses` | guard does not break a good zip |
| T4 | `test_form_d_header_drift_raises` | ISSUERS lacking ENTITYNAME -> ValueError |
| T5 | `test_form_d_optional_member_absent_ok` | no RECIPIENTS.tsv still fine |
| T6 | `test_check_publish_*` | zero, >50% drop, override env, first load |
| T7 | `test_13f_load_without_infotable_keeps_holdings` (PG) | release fails, holdings untouched |
| T8 | `test_13f_prune_guard_blocks_large_drop` (PG) | tiny release cannot replace a big one |
| T9 | `test_13f_normal_quarter_rollover_passes` (PG) | same-size replacement prunes |
| T10 | `test_adv_private_funds_guard` (PG) | empty staging cannot wipe the mart |
| T11 | `test_dry_run_leaves_tables_unchanged` (PG) | pe_firms / adv funds / pe_funds unchanged, stats reported |
| T12 | `test_summary_counts_all_tiers` | message sums every linked_* |
| T13 | `test_pe_firms_total_honours_strategy` | count query gets the strategy predicate |
| T14 | `test_zero_is_not_null` | aum 0 -> 0.0 |
| T15 | `test_no_truthy_float_coercion_left` | grep of pe_*.py |
| T16 | `test_ci_workflow_contract` | py3.11, TEST_PG_URL, bootstrap, gitleaks, no sleep smoke |
| T17 | `test_quarantine_skips_absent_tables` (PG) | blank DB can migrate past 0003 |

## Files to Create/Modify

| File | Action |
|------|--------|
| .github/workflows/ci.yml | Modify |
| .gitleaks.toml | Create |
| scripts/ci/bootstrap_db.py | Create |
| app/core/copy_loader.py | Modify (publish guard) |
| app/core/quarantine.py | Modify (skip absent tables) |
| app/ingest/bulk/sec_13f/parse.py, source.py | Modify |
| app/ingest/bulk/sec_form_d/parse.py | Modify |
| app/ingest/bulk/sec_form_adv/iapd_feed.py | Modify |
| app/marts/adv_private_funds.py | Modify |
| app/worker/executors/pe_marts.py | Modify |
| app/api/v1/pe_firms.py, pe_*.py | Modify |
| tests/test_spec_129_ci_and_publish_guards.py | Create |

## Out of scope

- `merge_staging` overwriting with NULL (review §5 minor) — separate spec.
- Mart build ledger / tolerance vs previous build (review finding 17, M).
- Renaming `final_close_usd_millions` / `vintage_year` semantics (finding 19).
- Declaring the migration-created indexes on the models so `alembic check` passes.

## Feedback History

_No corrections yet._
