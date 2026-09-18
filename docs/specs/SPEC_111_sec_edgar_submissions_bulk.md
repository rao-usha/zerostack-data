# SPEC 111 — EDGAR submissions bulk loader (`sec_edgar_submissions`) + D18 SEC DDL fix

**Status:** Implemented (tests passing)
**Task type:** collector
**Date:** 2026-09-16
**Test file:** tests/test_spec_111_sec_edgar_submissions_bulk.py
**Depends on:** SPEC_107 (BulkSource framework)

## Goal

1. Load SEC's nightly `submissions.zip` into three tables: filer master data, former names, and a 3-year 8-K index. Use the SPEC_107 bulk framework: stream the file, COPY it into staging, then merge.
2. Fix D18. `app/sources/sec/metadata.py` emits MySQL-style `INDEX idx (col)` clauses inside `CREATE TABLE`. That is invalid Postgres, so the 10-K/10-Q/8-K tables could never be created.

## Source facts (verified 2026-09-16 against data.sec.gov for Apple, Carlyle, Berkshire, Buffett and Citadel Advisors)

- The zip holds one `CIK##########.json` per filer, plus overflow `CIK##########-submissions-NNN.json` files. Overflow files are bare column arrays with no filer header. **We skip them.**
- Top-level fields:
  - `cik` (10-digit string), `entityType`, `sic`, `sicDescription`, `ownerOrg`
  - `insiderTransactionForOwnerExists` / `insiderTransactionForIssuerExists` (0/1)
  - `name`, `tickers[]`, `exchanges[]`, `ein`, `lei`, `description`, `website`, `investorWebsite`, `category`, `fiscalYearEnd`
  - `stateOfIncorporation`, `addresses.{business,mailing}` (`street1`, `street2`, `city`, `stateOrCountry`, `zipCode`, `stateOrCountryDescription`, `isForeignLocation`, `country`, `countryCode`)
  - `phone`, `flags`, `formerNames[{name, from, to}]` (ISO timestamps)
  - `filings.recent` (parallel arrays)
- EDGAR writes `''` for "not populated". Individuals and advisers carry the EIN sentinel `000000000`, and foreign filers use `999999999`.
- `filings.recent` holds at least one year or 1000 filings. For heavy filers (big banks with many 424B2 filings) it can reach back less than 3 years. Their older 8-Ks sit only in overflow files, which we skip. This gap is documented and accepted.

## Tables

- `public.sec_filers`
  - key `cik TEXT` (10-digit zero-padded, same as the existing `sec_*` tables)
  - `name`, `entity_type`, `sic`, `sic_description`, `owner_org`, `category`
  - `ein` (cleaned; sentinel values become NULL), `ein_raw`
  - `lei`, `state_of_incorporation`, `fiscal_year_end`
  - `tickers TEXT[]`, `exchanges TEXT[]`
  - `website`, `investor_website`, `phone` (raw), `phone10`
  - `biz_`/`mail_` + `street1`, `street2`, `city`, `state_or_country`, `zip`, `zip5`, `state2`
  - `biz_country`, `biz_is_foreign`
  - `insider_transaction_for_owner_exists BOOLEAN`, `insider_transaction_for_issuer_exists BOOLEAN`, `flags`
  - `former_name_count`, `recent_filing_count`, `latest_filing_date`, `earliest_recent_filing_date`, `latest_form`
  - `source_release_key`, `loaded_at`
- `public.sec_filer_former_names`
  - key `(cik, name, from_date)`, plus `to_date`, `source_release_key`, `loaded_at`
  - Rows with no `from` date use the sentinel `1900-01-01`, because key columns can't be NULL.
- `public.sec_8k_index`
  - key `accession_number`
  - `cik`, `form`, `filing_date`, `report_date`, `acceptance_datetime TIMESTAMPTZ`, `items TEXT`, `primary_document`, `primary_doc_description`, `size BIGINT`, `file_number`, `film_number`, `source_release_key`, `loaded_at`
  - Indexes on `(cik, filing_date)` and `filing_date`. `items` gets no index: lookups are substring matches, which a btree can't serve.
- Other indexes: `sec_filers` on `ein`, `sic`, `lower(name)` and GIN `tickers`; `sec_filer_former_names` on `lower(name)`.
- On each load, `sec_8k_index` rows filed before the cutoff are pruned.

## Acceptance Criteria

- [x] `discover()` returns exactly one `Release("snapshot:<UTC today>", https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip)` and makes no HTTP calls.
- [x] `load()` streams members one at a time (`infolist()` + `json.load` per member), with bounded memory.
  - One pass writes three staging tables at once. Filers go through COPY as a generator. Former-name and 8-K rows go to spooled temp CSVs that are copied after the pass.
  - Merge on the key, drop staging, and return `{table: inserted+updated}`.
- [x] Overflow members, non-JSON members and unparseable JSON are skipped and counted, not fatal.
- [x] Only `8-K` and `8-K/A` rows with `filing_date >= snapshot_date - 3 years` go into `sec_8k_index`.
- [x] Normalizers: `clean_ein` (nulls all-same-digit sentinels), `state2`, `zip5`, `phone10`, `s()` (`''` becomes None).
- [x] Loading the same fixture twice gives the same row counts (idempotent). The second run updates rows and inserts none.
- [x] Registered as `sec_edgar_submissions` in the bulk registry.
- [x] D18: `generate_create_table_sql` returns valid Postgres with no `INDEX` clause inside CREATE TABLE. Indexes come from `generate_create_index_sql` as `CREATE INDEX IF NOT EXISTS`, and the caller in `sec/ingest.py` runs them.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_discover_single_snapshot_release | release key `snapshot:YYYY-MM-DD`, URL, no HTTP |
| T2 | test_normalizers | EIN sentinel/dash, state2, zip5, phone10 |
| T3 | test_parse_filer_fields | filer row fields from Apple-shaped JSON |
| T4 | test_parse_former_names | dates parsed, blank names dropped |
| T5 | test_parse_8k_cutoff_and_items | only 8-K/8-K/A within 3 years; items kept |
| T6 | test_iter_members_skips_overflow | overflow + non-JSON + bad JSON skipped |
| T7 | test_registered | registry has the source |
| T8 | test_load_fixture_zip_twice_pg | (PG) counts, idempotent reload |
| T9 | test_d18_ddl_has_no_inline_index | no `INDEX ` inside CREATE TABLE |
| T10 | test_d18_ddl_executes_on_pg | (PG) CREATE TABLE + indexes run twice |

## Volume estimate

- About 950k filer JSONs, giving roughly 950k `sec_filers` rows, around 0.6–0.8 GB with indexes.
- Former names: about 400–600k rows (~60 MB).
- 8-K + 8-K/A: about 70–80k per year, so roughly 220–250k rows over 3 years (~60 MB).
- Decompressing the zip is the main cost, since members are parsed one at a time. Expect a 15–30 minute load.
