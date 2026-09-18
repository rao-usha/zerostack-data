# SPEC 113 — XBRL period-key fix + companyfacts bulk loader (`sec_companyfacts`)

**Status:** Draft
**Task type:** bug_fix + service
**Date:** 2026-09-16
**Test file:** tests/test_spec_113_xbrl_period_keys.py
**Plan:** docs/plans/PLAN_082_pe_rebuild_phase0_phase1.md (Phase 1, D6)

## Problem (D6)

`app/sources/sec/xbrl_parser.py` grouped companyfacts facts by the FILING's `fy`/`fp`.
A FY2024 10-K carries FY2023/FY2022 comparatives that are all tagged `fy=2024`, and a
Q2/Q3 10-Q mixes 3-month and YTD durations. The result: values land in the wrong period.
For example, an Apple row labeled FY2025 held FY2023 revenue (383,285M), and 23,490 of
26,389 FY rows had a period_end before their fiscal year. `_find_fact_value` ignored the
period entirely.

## Fix

### Grouping rules (parser)
- Each fact is grouped by its **own** period: `(start, end)` for durations and `end` for instants.
- Duration bucket = `(end - start).days`:
  - quarter: 80–100 days
  - H1 (YTD): 170–190 days
  - 9M (YTD): 260–285 days
  - annual: 350–380 days
  - anything else: `D<days>`
- **Income statement and cash flow rows:** annual and discrete-quarter buckets only. YTD H1/9M rows are never stored as quarters.
- **Balance sheet rows:** instants (no `start`) only.
- **Value choice:** for each line item, the first candidate concept (in mapping order) that has a fact for exactly that period. Among duplicates of that concept/unit/period, the fact with the latest `filed` date wins, with ties broken by accession number. This handles restatements.
- **Fiscal labels** (derived, not part of any key):
  - A filing's document period end is the max `end` over its us-gaap duration facts of 80 days or longer. Durations are used so that subsequent-event instants and dei cover dates don't skew it.
  - When a filing's document period end equals the fact's own period end, that filing reports this period as its current period. Its `fy` becomes `fiscal_year`, and the earliest such filing (the original) supplies `accession_number`, `form_type` and `filing_date`.
  - `fiscal_period`:
    - annual: `FY`
    - quarter: the filing's `fp` when it is Q1–Q3; `Q4` when the filing is a 10-K (`fp=FY`)
    - instants: the filing's `fp`
  - **Fallback** when no filing reports the period as current:
    - `fiscal_year` is `end.year`, or `end.year - 1` when the end falls on Jan 1–7 (52/53-week years).
    - A quarter's number is counted back from the company's next annual period end within 370 days. With no such annual period, the calendar quarter is used.
    - `accession_number`/`form_type`/`filing_date` come from the latest-filed fact in the row.
- `sec_financial_facts` records are deduped to one per `(namespace, fact_name, unit, start, end)` (latest filed). Their `fiscal_year`/`fiscal_period` are the derived labels, with `H1`/`9M`/`D<days>` for non-standard durations.
- `min_period_end` (optional) drops every period ending before the cutoff.

### Keys
| Table | Old unique key | New unique key |
|---|---|---|
| sec_income_statement | uq_sec_income_cik_period (cik, period_end_date, fiscal_year, fiscal_period) | uq_sec_income_cik_period_bounds (cik, period_end_date, period_start_date), with period_start_date NOT NULL |
| sec_balance_sheet | uq_sec_balance_cik_period (cik, period_end_date, fiscal_year, fiscal_period) | uq_sec_balance_cik_period_end (cik, period_end_date) |
| sec_cash_flow_statement | uq_sec_cashflow_cik_period (cik, period_end_date, fiscal_year, fiscal_period) | uq_sec_cashflow_cik_period_bounds (cik, period_end_date, period_start_date), with period_start_date NOT NULL |
| sec_financial_facts | (cik, fact_name, period_end_date, fiscal_year, fiscal_period, unit) | **unchanged** (see below) |

**sec_financial_facts has the same flaw.** Its key contains the filing's fy/fp, so:
- the same fact from three filings produced three rows;
- the 3-month and 6-month facts of one 10-Q (same end, fy, fp, unit) overwrote each other.

The key is kept, and the flaw is fixed in the parser instead:
- facts are deduped per own period;
- `fiscal_period` is duration-aware (FY/Q1–Q4/H1/9M/D<days>), which makes the existing key unique per own period.

This avoids a NULL-in-unique-key problem (instants have no start; PG14 has no NULLS NOT DISTINCT). No migration touches this potentially very large table. Rows written before the fix still carry the old labels. **Open item:** truncate and re-ingest `sec_financial_facts`.

### Migration `0006_xbrl_period_keys` (down_revision `0005_bulk_source_tables`)

**Upgrade**, for each of the three tables, skipped when the table does not exist:
1. `CREATE SCHEMA IF NOT EXISTS quarantine`.
2. Add `period_start_date` if it is missing (income and cash flow only).
3. If the old unique constraint or index is still present:
   - copy all rows to `quarantine.<table>_pre_period_fix` (`CREATE TABLE IF NOT EXISTS … AS SELECT`, then `INSERT … SELECT` if the table already existed);
   - `DELETE` the rows from public (every row is mis-keyed).
4. `DROP CONSTRAINT IF EXISTS` / `DROP INDEX IF EXISTS` the old name.
5. Set `period_start_date` NOT NULL (income and cash flow), and add the new unique constraint. This is idempotent: the constraint is dropped first if it exists.

**Downgrade**:
1. Drop the new constraint.
2. Move the current rows to `quarantine.<table>_post_period_fix` and delete them.
3. Drop NOT NULL.
4. Restore the rows from `quarantine.<table>_pre_period_fix`.
5. Re-add the old constraint.

### Remove `app/sources/edgar_company_facts/`
The package upserted into `public_company_financials`, which is now a VIEW over `sec_income_statement` (app/sources/sec/views.py), and it duplicated the XBRL pipeline. Remove the package and its only reference, `POST /macro/collect/edgar-facts` in `app/api/v1/macro_cascade.py`. main.py and jobs.py have no references.

### Bulk source `sec_companyfacts`
- `discover()` returns one release, `snapshot:YYYY-MM-DD` (UTC today), for `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip`.
- `load()`:
  1. `ensure_ddl` (CREATE TABLE IF NOT EXISTS from the ORM models), then verify that the new unique constraints exist (fail loudly if migration 0006 has not run).
  2. Iterate the zip members `CIK##########.json` one at a time.
  3. Run `xbrl_parser.build_financial_statements(..., min_period_end=release_date - 3 years)` (statements only, no raw facts).
  4. Buffer up to N companies' rows, COPY them into `stg.sec_companyfacts_<table>`, and merge with the new keys.
  5. Drop staging at the end.
- Numeric values that overflow the column precision are nulled rather than failing the release.
- Memory is bounded by one company JSON plus one batch.

## Acceptance Criteria
- [ ] Apple FY2023 revenue 383,285M lands on the row with period_end 2023-09-30, start 2022-09-25, fiscal_year 2023, FY. No FY2024/FY2025 row holds it.
- [ ] A 10-Q with 3-month and 6-month durations produces only the discrete quarter row.
- [ ] A restated fact (later `filed`) wins.
- [ ] The 3-year cutoff drops older periods.
- [ ] Balance sheet rows are instants keyed on period_end only.
- [ ] Migration chain `0006_xbrl_period_keys` → `0005_bulk_source_tables`. Upgrade SQL on PG moves old rows to quarantine and creates the new unique key. Downgrade restores them.
- [ ] `ingest_xbrl` / `bulk_ingest_orchestrator` conflict columns match the new keys.
- [ ] `sec_companyfacts` loads a tiny fixture zip twice; the second run inserts nothing new (idempotent).
- [ ] `edgar_company_facts` removed; `import app.main` still works.

## Test Cases
| ID | Test | Verifies |
|----|------|----------|
| T1 | test_apple_fy2023_revenue_on_own_period | Own-period grouping + labels from originating filing |
| T2 | test_quarter_vs_ytd_only_discrete_quarter | 3M kept, 6M not stored as quarter |
| T3 | test_restated_fact_latest_filed_wins | Restatement preference |
| T4 | test_three_year_cutoff | min_period_end filter |
| T5 | test_balance_sheet_instants | Instant grouping, one row per end |
| T6 | test_financial_facts_deduped_and_duration_labels | Facts dedupe + H1 label |
| T7 | test_model_unique_keys | ORM constraints |
| T8 | test_ingest_conflict_columns | ingest_xbrl + orchestrator keys |
| T9 | test_migration_0006_chain | revision / down_revision |
| T10 | test_migration_0006_sql_pg | (PG) old-key tables → quarantine + new key; downgrade restores |
| T11 | test_bulk_discover_snapshot | One release snapshot:YYYY-MM-DD |
| T12 | test_bulk_load_fixture_zip_twice_pg | (PG) idempotent load |
| T13 | test_edgar_company_facts_removed | Package gone, no references |

## Rubric
- Parameterized SQL only; COPY for bulk.
- The parser does not duplicate logic: the bulk source calls `xbrl_parser`.
- No real DB or real zip download in tests.
