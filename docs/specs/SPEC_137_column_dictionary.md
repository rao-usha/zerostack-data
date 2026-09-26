# SPEC_137 — Column dictionary, `/catalog/{key}/schema`, masked sample, join keys

**Type:** service + api_endpoint
**Date:** 2026-09-26
**Plan:** PLAN_088 §3 "SPEC_137" (wave A, parallel with SPEC_141 truth pass and SPEC_144 quality/usage)
**Depends on:** SPEC_123 catalog (`app/catalog/*`), SPEC_127 auth (`require_admin_for_writes`)

## Problem (measured, PLAN_088 §1.9 and the dictionary lens)

- Column truth is split across 7 sources in 5 formats and nothing gathers it:
  2 of 10,089 live columns carry a PG comment; 612 of 3,334 model columns have an
  inline `#` comment; `metadata.py` description dicts (fdic 184, cms 66, nppes 30 ...);
  DDL `--` comments (realestate); three shapes of bulk-loader DDL; the
  `census_variable_metadata` table (196 rows); `data_profile_columns` stats.
- Identifiers are stored inconsistently. CIK: varchar/text/integer/bigint, and
  `core.identifier` holds it unpadded — a naive join of 1,000 identifier CIKs to
  `sec_filers` matched 0; with `lpad(...,10,'0')` it matched 1,000.
- Column-level PII is invisible: `sec_13f` is `pii_class='none'` while
  `sec_13f_filings.signature_name/phone` name a natural person.
- The only data preview (`/export` preview) is admin-only, public-schema only,
  runs an unbounded `COUNT(*)` and masks nothing.

## Design

### Modules (all new unless stated)

| File | Role |
|---|---|
| `app/catalog/identifiers.py` | Closed `SEMANTIC_TYPES`: canonical format, `normalize_sql` template, join specificity. `normalize_expr(type, col_sql)`. |
| `app/catalog/columns.py` | `ColumnSpec` (frozen, validated at construction: unknown `semantic_type`, `pii`, `source` or `confidence` raises), the glossary rules (`GLOSSARY`), `classify(name, pg_type)`, the PII-name lint regex, masking helpers. |
| `app/catalog/columns_curated.py` | Hand-written / upstream text keyed `table.column` (SEC Form D, 13F, insider, EDGAR submissions, ADV, PE marts, core entity tables, people). `source='upstream'` rows carry an `upstream_url`. |
| `app/catalog/dictionary_build.py` | Offline generator: `python -m app.catalog.dictionary_build [--check] [--coverage]`. Writes `app/catalog/columns.generated.json` (sorted, diffable, `dictionary_hash`). |
| `app/catalog/dictionary.py` | Runtime: loads the JSON, merges live facts (information_schema types, PG comments, census labels, profile stats), per-table/per-dataset description coverage, join-key index, column search. |
| `app/catalog/schema_live.py` | Live facts (`pg_attribute`, `pg_index`, `reltuples`, census labels, profile stats), `dataset_schema()`, `build_sample()` with masking and the 10-min cache. |
| `app/catalog/mirror.py` (edit) | `sync_column_comments(engine, dry_run)` — `COMMENT ON COLUMN` sync. |
| `app/api/v1/catalog_schema.py` | Routes (below). |
| `app/main.py` (edit) | One `include_router` line, placed **before** `catalog.router` so `/catalog/columns` and `/catalog/joins` are not swallowed by `/catalog/{key}`. The `catalog` OpenAPI tag already exists; no new tag. |

### Sources and precedence (highest first)

Description, unit and example are resolved independently of `semantic_type`/`pii`.

1. **curated** / **upstream** — `columns_curated.py`.
2. **census** — `census_variable_metadata.label` (live, runtime only; reported as `source='upstream'`).
3. **bulk** — `COMMENT ON COLUMN` statements inside a `BulkSource.ddl()`.
4. **metadata** — `app/sources/<pkg>/metadata.py` description dicts (any module-level dict of
   `column -> {"description": ...}`, keys lower-cased; applied to every table of a dataset whose
   source package is `<pkg>`) and `-- comments` inside static `CREATE TABLE` DDL in `app/`/`alembic/`.
5. **model** — `Column(comment=...)`, then the inline `# comment` on the column's statement
   (AST + tokenize over `tables.MODEL_MODULES`). A comment starting `e.g.` becomes `example`.
6. **pg_comment** — the live `col_description` (runtime only).
7. **glossary** — ~80 name rules (`id`, `*_at`, `source_release_key`, cik, crd, lei, cusip, figi,
   npi, ein, uei, duns, naics, sic, fips, zip, lat/lon, state, country, accession, ticker,
   series_id, `*_usd`, `*_pct`, email/phone/fax/name/street/birth ...). Glossary rules also assign
   `semantic_type`, `unit` and column `pii`.

Types/nullable: live `information_schema` at request time; offline from the model, the bulk DDL
or the static DDL (in that order). Offline generation never touches a database, so the drift
test can regenerate it in CI.

### Semantic types (closed)

`cik, crd, lei, cusip, figi, npi, ein, uei, duns, naics, sic, fips_state, fips_county, zip5,
iso_country, us_state, accession_number, ticker, series_id, latitude, longitude, period_date,
amount_usd, pct`. Join keys are the ones with a `normalize_sql`, except `us_state` (excluded:
too coarse). Examples: cik `lpad(ltrim({col}::text,'0'),10,'0')`; crd `ltrim({col}::text,'0')`;
fips_county `lpad({col}::text,5,'0')`; naics `left(regexp_replace({col}::text,'\D','','g'),6)`.
Per user decision (PLAN_088 open decision 9) there is **no data migration** of `core.identifier`;
`normalize_sql` is the supported way to join.

### Column PII

- Glossary: email / phone / fax / first|middle|last_name / signature / name_of_signer / street /
  address lines → `business_contact`; birth / dob / ssn / home_* → `personal`. Curated rows
  override (e.g. `people.email` → `personal`).
- Lint (`PII_NAME_RE`, from PLAN_088): a column whose name matches
  `email|phone|fax|first_name|last_name|signature|street|birth` must have `pii != 'none'`, unless
  its type is boolean/date/timestamp/numeric (e.g. `signature_date`, `has_email`).
- Dataset `pii_class` must be ≥ the maximum column PII of its tables. Mismatches are listed in
  the test as `PENDING_PII_RAISES` (`sec_13f`, raised by SPEC_141; `si_public_water_systems`, new
  finding); the test asserts the offender set is a subset, so it can only shrink.

### Routes (`app/api/v1/catalog_schema.py`, user-read; mounted with `_auth`)

- `GET /catalog/{key}/schema?table=` — per resolved table: `exists`, `row_estimate`
  (`reltuples`), declared `primary_key` (tables[0]) and derived unique keys (`pg_index`),
  columns (name, type, nullable, description, unit, example, semantic_type, normalize_sql, pii,
  source, confidence, null_pct, distinct_count), description coverage per table and dataset.
  Examples come from the latest `data_profile_columns.stats.top_values` only for `pii='none'`.
  Tables resolved from patterns are capped at 25 (`tables_truncated`); `table=` selects one.
  Header `ETag: W/"<dictionary_hash>"`.
- `GET /catalog/{key}/sample?table=&limit<=20` — `SET TRANSACTION READ ONLY`,
  `SET LOCAL statement_timeout=3000`, no count; `ORDER BY` the natural key;
  `TABLESAMPLE SYSTEM` when `reltuples > 1M`; cached 10 min.
  - only **documented** columns (description present) and never `export_policy` sensitive columns;
    `hidden_columns` counts the rest;
  - `export_policy.is_exportable(table, columns)` must pass (403 for everyone otherwise);
  - non-admin: 403 when any writer of the table declares `redistribution='restricted'`
    (or a future `storage='forbidden'`);
  - non-admin masking: `personal` → NULL; `business_contact` → partial (email `j***@domain`,
    phone `***1234`, other text first letter + `***`); in a dataset whose `pii_class='personal'`
    every PII column is treated as `personal`; JSON/array/geometry columns → NULL unless their
    text is curated/upstream (they can embed names);
  - in any table with an `email_confidence` column (`people`), `email`/`work_email` where the
    confidence is `inferred` or `guessed` → **always NULL**, admins included (done in SQL);
  - admin callers get unmasked values (except the inferred-email rule);
  - header `X-Dataset-Attribution` (attribution or license).
- `GET /catalog/joins?semantic_type=` — join-key index: semantic_type → columns
  `{dataset, table, column, pg_type, normalize_sql}`.
- `GET /catalog/{key}/joins` — other datasets sharing an identifier, ranked by specificity
  (cik/crd/npi/lei 10 ... zip 2), each with ready-to-run join SQL using `normalize_sql` on both sides.
- `GET /catalog/columns?q=&semantic_type=&pii=&limit=` — search across datasets.
- `GET /catalog/columns/coverage` — description coverage per dataset (offline dictionary).
- `POST /catalog/columns/comments/sync?dry_run=true` — admin only (the router's
  `require_admin_for_writes` plus an explicit `require_admin`).

### COMMENT ON sync — decision

Admin endpoint + CLI (`python -m app.catalog.dictionary_build --sync-comments`), **not startup**:
the dictionary only changes on deploy, a sync touches up to ~10k columns, and `COMMENT` takes a
`SHARE UPDATE EXCLUSIVE` lock (conflicts with VACUUM/ANALYZE/index builds). Per table: one
transaction under `SET LOCAL lock_timeout='2s'`, `statement_timeout='10s'`; a table that fails
is skipped and reported. Only comments from curated/upstream/bulk/metadata/model text are written
(glossary guesses are not). Existing non-empty comments are **adopted, never overwritten**
(the two 13F comments). Idempotent: a second run writes nothing. Never deletes a comment.

### No migration

No `catalog_column` table: the generated JSON is the store (plan: "prefer code + generated file"),
so no Alembic revision in this spec. A DB view/table mirror can be added later if a BI tool needs it.

## Tests (`tests/test_spec_137_column_dictionary.py`)

Unit (offline):
1. Offline regeneration equals the checked-in `columns.generated.json` (drift gate) and the hash matches.
2. `ColumnSpec` rejects an unknown `semantic_type`, `pii`, `source`, `confidence`.
3. Every semantic type with a `normalize_sql` renders valid-looking SQL with `{col}` substituted;
   cik normalises `'25743'`, `25743`, `'0000025743'` to the same value (checked in PG).
4. Glossary: cik/crd/naics/zip/lat/lon/fips/email/phone classify as expected.
5. PII-name lint over every dictionary column.
6. Dataset `pii_class` ≥ max column PII, offenders ⊆ `PENDING_SPEC_141`; `sec_13f` is caught.
7. Masking helpers (email, phone, text, personal, json).
8. Join index: `sec_13f` and `sec_form_d` share cik; `/catalog/{key}/joins` ranks cik above zip; join SQL uses both normalize expressions.
9. Route order: `catalog_schema` is included before `catalog` in `app/main.py`; `/catalog/joins`
   and `/catalog/columns` do not hit the `/{key}` 404.
10. Coverage: overall offline description coverage ≥ 56% (tracked floor) and the PE/entity pack ≥ 80% (gate).

PostgreSQL (embedded, `TEST_PG_URL`):
11. `/schema` returns live types, `exists:false` for a missing declared table, derived unique key, profile null_pct, census label.
12. `/sample`: ≤20 rows, non-admin masks `personal` and partially masks `business_contact`, admin unmasked,
    inferred emails NULL for both, undocumented columns hidden, 403 on a restricted dataset for
    non-admin, 403 for everyone on an export-denied table, `limit=21` → 422, `X-Dataset-Attribution` set.
13. CIK join fixture: unpadded identifier CIKs join 1,000/1,000 through `normalize_sql` (naive join 0).
14. `sync_column_comments`: writes missing comments, adopts an existing one, second run writes 0, dry run writes nothing.

## Results (2026-09-26)

Description coverage, measured with `coverage_report()` (offline) and by merging the live
`pg_attribute` column list of every resolved catalog base table (read-only, `nexdata-api-1`):

| Scope | Columns | Described | % | % without glossary |
|---|---|---|---|---|
| Offline dictionary (197 tables the code declares) | 2,753 | 1,825 | 66.3 | 29.5 |
| **Live, all 252 resolved catalog tables** | 4,893 | 2,959 | **60.5** | 27.2 |
| Live, excluding `acs5_*` | 4,217 | 2,690 | 63.8 | |
| Live, `acs5_*` (census labels exist for 196 variables) | 676 | 269 | 39.8 | |
| **PE/entity pack (14 specs)** | 679 | 679 | **100.0** | 66.1 |

PLAN_088 estimated ~56% from existing text + glossary; the live figure is 60.5%. Lowest live
datasets: `cftc_cot` 10%, `treasury_auctions` 13%, `courtlistener_dockets` 21%, `fema_disaster_declarations`
23%, `irs_soi` 24% — the next curated/upstream batch (Treasury `meta.labels`, CFTC field guide).

Live checks (read-only): the CIK join `core.identifier` → `sec_filers` through `normalize_sql`
matched 1,000/1,000 (2.9 s: expression join, no index); a 20-row `TABLESAMPLE SYSTEM` sample of
`sec_13f_holdings` (reltuples 3.83M) ordered by its key took 0.07 s; live column comments: 2.

New PII finding (not fixed here, `datasets.py` belongs to SPEC_141): `si_public_water_systems` is
`pii_class='none'` but `public_water_system.admin_contact_phone` is a contact phone. `sec_13f`
(none → business_contact) is raised by SPEC_141. Both are listed in `PENDING_PII_RAISES`.

## Out of scope

Alembic-only DDL; normalising `core.identifier` on write; a `catalog_column` DB table; upstream
Treasury `meta.labels` harvesting (needs a live API call — follow-up).
