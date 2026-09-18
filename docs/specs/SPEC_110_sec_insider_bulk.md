# SPEC 110 — SEC Insider Transactions data sets bulk loader (`sec_insider`)

**Status:** Draft
**Task type:** collector
**Date:** 2026-09-16
**Test file:** tests/test_spec_110_sec_insider_bulk.py
**Depends on:** SPEC_107 (BulkSource framework)

## Goal

Load the SEC "Insider Transactions Data Sets" (Forms 3/4/5) into Postgres. SEC publishes these every quarter as `YYYYqN_form345.zip` at
https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets. Each zip holds tab-delimited files. This spec adds a `BulkSource` named `sec_insider` that finds, fetches and loads the quarterly releases into new `public.sec_insider_*` tables. By default it loads the latest 12 quarters.

The existing `public.insider_transactions` table, used by the people pipeline, is **not** touched.

## Real format (verified 2026-09-16 against 2025q4 and 2023q3)

- Index page links: `/files/structureddata/data/insider-transactions-data-sets/2025q4_form345.zip`. The newest link, 2026q2, is under `/files/datastandardsinnovation/data/...`, so URLs are taken from the hrefs and never built by hand. The index lists 2006q1–2026q2.
- Zip members: `SUBMISSION.tsv`, `REPORTINGOWNER.tsv`, `NONDERIV_TRANS.tsv`, `NONDERIV_HOLDING.tsv`, `DERIV_TRANS.tsv`, `DERIV_HOLDING.tsv`, `FOOTNOTES.tsv`, `OWNER_SIGNATURE.tsv`, `FORM_345_metadata.json`, `FORM_345_readme.htm`.
- Files are tab-delimited UTF-8 with a header row. Text holding quotes is CSV-quoted (`"..""..."`); FOOTNOTES is an example.
- Dates look like `DD-MON-YYYY` (`31-DEC-2025`). Numbers look like `14706.0` or are blank.
- Flag columns (`EQUITY_SWAP_INVOLVED`, `AFF10B5ONE`, `NO_SECURITIES_OWNED`, ...) mix `0`/`1` with `true`/`false`.
- `RPTOWNER_RELATIONSHIP` is a comma list of `Director`, `Officer`, `TenPercentOwner` and `Other`.
- Header quirks:
  - DERIV_TRANS spells its column `EXCERCISE_DATE`; DERIV_HOLDING uses `EXERCISE_DATE`.
  - The equity-swap footnote column in the real TSVs is `EQUITY_SWAP_TRANS_CD_FN`, but the metadata json says `EQUITY_SWAP_INVOLVED_FN`.
  - SUBMISSION has no CONTACT_* columns, although the metadata lists them.
- Natural keys are unique within a file:
  - SUBMISSION: `ACCESSION_NUMBER`
  - REPORTINGOWNER: (`ACCESSION_NUMBER`, `RPTOWNERCIK`)
  - each trans/holding table: (`ACCESSION_NUMBER`, `<TABLE>_SK`)
  - FOOTNOTES: (`ACCESSION_NUMBER`, `FOOTNOTE_ID`)
- No accession number appears in two quarters (checked for 2023q3 vs 2025q4).
- Volume: about 8 MB zipped and 50 MB unzipped per quarter. Rows in 2025q4:
  - 36.4k submissions
  - 39.6k owners
  - 59.7k non-derivative transactions + 17.5k non-derivative holdings
  - 20.4k derivative transactions + 8.2k derivative holdings
  - 88.9k footnotes

## Acceptance Criteria

- [ ] `app/ingest/bulk/sec_insider/` registers a `BulkSource` named `sec_insider` through `@register_bulk_source`.
- [ ] `discover(http, since)`:
  - [ ] makes one `get_text` call to the index page and parses every `YYYYqN_form345.zip` href (relative or absolute) into `Release(release_key="YYYYqN", url=absolute)`.
  - [ ] returns releases oldest first, with no duplicates.
  - [ ] with `since=None`, returns the latest 12 quarters listed.
  - [ ] with `since`, returns every quarter whose end date is on or after `since`.
- [ ] `parse.py` is pure (no DB):
  - [ ] `parse_sec_date`: `DD-MON-YYYY` (any case) or ISO → `date`; blank or invalid → `None`.
  - [ ] `parse_number`: → `Decimal`; blank, invalid, NaN or Inf → `None`.
  - [ ] `parse_flag`: `1/0/true/false/Y/N` → `bool`; anything else → `None`.
  - [ ] `parse_relationship`: → director / officer / ten_percent_owner / other booleans.
  - [ ] `iter_filings`, `iter_owners`, `iter_transactions` and `iter_footnotes` stream zip members with `csv.DictReader(delimiter='\t')` and yield tuples in the target column order. Memory stays bounded.
  - [ ] Header lookup ignores case, and missing columns become NULL. Both `EXCERCISE_DATE` and `EXERCISE_DATE` are accepted.
  - [ ] Text is stripped, NUL characters are removed, and empty text → NULL.
  - [ ] The `*_FN` footnote-reference columns go into one `footnote_refs` JSON object (NULL when empty).
- [ ] `load(conn, release, path)`:
  - [ ] calls `ensure_ddl` first.
  - [ ] for each target: COPY into `stg.sec_insider_<t>_<release>`, then `merge_staging` on the natural key, then drop the staging table.
  - [ ] for child tables (owners, transactions, footnotes), deletes rows for accessions in this release that are no longer present, so a republished quarter replaces its children.
  - [ ] returns `{table: rows_merged}`.
  - [ ] loading the same file twice gives identical row counts (idempotent).
- [ ] Tables (DDL is `CREATE TABLE/INDEX IF NOT EXISTS`). Each has `source_release_key TEXT` and `loaded_at TIMESTAMP DEFAULT NOW()`:
  - `public.sec_insider_filings` (PK `accession_number`)
  - `public.sec_insider_owners` (PK `accession_number, rptowner_cik`)
  - `public.sec_insider_transactions` (PK `accession_number, table_type, trans_sk`). `table_type` is one of `nonderiv_trans`, `deriv_trans`, `nonderiv_holding` or `deriv_holding`; the derivative-only columns are nullable.
  - `public.sec_insider_footnotes` (PK `accession_number, footnote_id`)
  - Indexes: `issuer_cik`, `issuer_trading_symbol`, `filing_date` (filings); `rptowner_cik` (owners); `trans_date`, `trans_code` (transactions).

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_discover_parses_index_default_window | Relative and absolute hrefs, mixed paths, dedupe, oldest first, latest 12 by default |
| T2 | test_discover_since_filters_by_quarter_end | `since=2025-05-01` keeps 2025q2 and later |
| T3 | test_parse_sec_date | `01-JAN-2024`, lowercase, ISO, blank, garbage, `31-FEB-2024` |
| T4 | test_parse_number_and_flag | `14706.0`, `-1.5`, blank, `abc`, `NaN`; flags `0/1/true/false/blank` |
| T5 | test_parse_relationship | `Director,Officer,TenPercentOwner`, `Other`, blank |
| T6 | test_iter_filings_and_owners_fixture_zip | Real-header fixture zip → filing and owner tuples (dates, flags, CIK, relationship) |
| T7 | test_iter_transactions_covers_all_four_tables | nonderiv/deriv trans and holding rows, `EXCERCISE_DATE` quirk, footnote_refs, blank numbers |
| T8 | test_iter_footnotes_quoted_text | CSV-quoted footnote text with embedded quotes and tabs |
| T9 | test_ddl_and_registration | Source registered; DDL names all tables, keys and indexes |
| T10 | test_load_fixture_twice_idempotent_pg | (PG) load → counts; second load → same counts, no duplicates; values typed correctly |
| T11 | test_republish_removes_stale_children_pg | (PG) reload with one transaction removed → stale row deleted |

## Design Notes

- The release key is `YYYYqN`, and the URL always comes from the index page because SEC moved at least one quarter to a different directory.
- Default window: the latest 12 listed quarters, about 3 years and roughly 100 MB of zips (cost constraint). Pass `since` for more.
- One unified transactions table with nullable derivative columns, as specified. `trans_sk` is SEC's surrogate key from `<TABLE>_SK`.
- CIKs are kept as TEXT zero-padded to 10 digits (canonical EDGAR form).
- Staging table names carry the release key, so two releases loading at once cannot collide.
- `OWNER_SIGNATURE.tsv` is not loaded (out of scope).
- The CSV reader uses the default quoting (`"`), which matches the SEC export. `csv.field_size_limit` is raised for long REMARKS and footnotes.
- Stale-children delete: `DELETE FROM target t USING (SELECT DISTINCT accession_number FROM stg) s WHERE t.accession_number = s.accession_number AND NOT EXISTS (matching key in stg)`. This runs only for accessions in the release, so other quarters are untouched.
- Estimated size for 12 quarters: about 440k filings, 480k owners, 1.3M transaction/holding rows and 1.05M footnotes, roughly 1.2–1.9 GB in Postgres with indexes. Measured on 2025q4 in a disposable DB: 160 MB after two loads (including update bloat), and about 14 s per load.

## Files

- `docs/specs/SPEC_110_sec_insider_bulk.md`
- `tests/test_spec_110_sec_insider_bulk.py`
- `app/ingest/bulk/sec_insider/__init__.py`
- `app/ingest/bulk/sec_insider/source.py`
- `app/ingest/bulk/sec_insider/parse.py`
