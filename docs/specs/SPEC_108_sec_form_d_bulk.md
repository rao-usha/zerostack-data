# SPEC 108 — SEC Form D data sets bulk loader (`sec_form_d`)

**Status:** Draft
**Task type:** collector
**Date:** 2026-09-16
**Test file:** tests/test_spec_108_sec_form_d_bulk.py
**Depends on:** SPEC_107 (BulkSource framework)

## Goal

Load the SEC quarterly Form D structured data sets (`YYYYqN_d.zip`, tab-separated
FORMDSUBMISSION / ISSUERS / OFFERING / RELATEDPERSONS / RECIPIENTS / SIGNATURES) through the
SPEC_107 framework, replacing per-filing XML scraping. The loader keeps the last 12 quarters by default
because the Cloud SQL disk is small. Everything is loaded; nothing is filtered by universe.

## Acceptance Criteria

- [ ] `SecFormDDataSets(BulkSource)` is registered as `sec_form_d`. Importing `app.ingest.bulk.sec_form_d` registers it.
- [ ] **Discovery**
  - [ ] Scrapes the index page with `SecHttp.get_text`, takes hrefs matching `(\d{4})q(\d)_d(_N)?.zip`, and resolves relative URLs against `https://www.sec.gov`. The path moved at 2026q2 (structureddata → datastandardsinnovation) and some old files carry a `_0` suffix.
  - [ ] One release per quarter, `release_key` = `YYYYqN`, oldest first.
  - [ ] When `since` is None, returns the newest 12 quarters.
  - [ ] When `since` is given, returns quarters whose quarter-end date is on or after `since`.
  - [ ] Raises if no hrefs are found, so a broken page never passes silently as "nothing new".
- [ ] **Parsing** (`parse.py`, pure, no DB): streams zip members through `TextIOWrapper` and `csv.DictReader(delimiter='\t', QUOTE_NONE)`, one generator of typed tuples per table.
  - [ ] Blank values become NULL, and whitespace (including NBSP) is normalized.
  - [ ] Amounts become ints. `Indefinite` becomes NULL. Values out of range for BIGINT/INT become NULL.
  - [ ] `DD-MON-YYYY` and ISO dates are parsed.
  - [ ] `true`/`false` become booleans.
  - [ ] Only `TESTORLIVE = LIVE` submissions are loaded. Child rows of TEST submissions are skipped.
  - [ ] Every issuer is loaded, not only the primary one. Related persons carry `relationships` as TEXT[].
- [ ] **Targets**
  - [ ] `public.form_d_filings`: the existing DDL is reproduced with `CREATE TABLE IF NOT EXISTS` and is not altered. One row per LIVE accession that has a primary issuer, built in SQL from staging:
    - issuer fields from the primary issuer
    - offering fields
    - `related_persons` JSONB (XML-path shape `{first_name,last_name,relationship[]}`)
    - `federal_exemptions` JSONB (XML-path labels such as `Rule 506(b)`)
    - `sales_compensation` JSONB from RECIPIENTS
  - [ ] New `public.form_d_offerings` (the main PE signal), keyed by `accession_number`:
    - `file_num` (trimmed, from FORMDSUBMISSION), `is_amendment`, `previous_accession_number`
    - `industry_group_type`, `investment_fund_type`, `is_40_act`, `revenue_range`, `aggregate_net_asset_value_range`
    - `federal_exemptions_items_list` TEXT[] of raw codes
    - `is_business_combination_transaction`, `duration_of_offering_more_than_one_year`
    - `date_of_first_sale`, `yet_to_occur`
    - `total_offering_amount` NUMERIC (NULL when Indefinite) plus `is_indefinite`
    - `total_amount_sold`, `total_remaining`, `has_non_accredited_investors`, `total_number_already_invested`
    - `sales_commission_amount`, `finders_fees_amount`, `gross_proceeds_used_amount`, `minimum_investment_accepted` (all NUMERIC)
    - Indexes on `investment_fund_type` and `date_of_first_sale`.
  - [ ] New `public.form_d_signatures`, keyed by `(accession_number, signature_seq)`: `issuer_name`, `signature_name`, `name_of_signer`, `signature_title`, `signature_date`.
  - [ ] New `public.form_d_issuers`, keyed by `(accession_number, issuer_seq)`.
  - [ ] New `public.form_d_related_persons`, keyed by `(accession_number, related_person_seq)`.
  - [ ] All new tables have `source_release_key TEXT` and `loaded_at TIMESTAMP DEFAULT NOW()`.
- [ ] **Load**: COPY into `stg.sec_form_d_*` staging, then `merge_staging`, then drop the staging tables, all in the caller's transaction. Returns `{table: inserted + updated}`. Re-loading the same release inserts 0 rows.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_discover_parses_hrefs_and_window | Relative and absolute hrefs, `_0` suffix, dedupe, oldest first, default 12-quarter window |
| T2 | test_discover_since_filter | `since` keeps quarters ending on or after the date |
| T3 | test_discover_raises_when_no_links | An empty index raises |
| T4 | test_value_parsers | Amounts (int and exact NUMERIC), Indefinite flag, dates, bools, ints, NBSP, blank → None |
| T5 | test_parse_zip_real_headers | Real header names; full offering fields (fund type, amendment, fees, raw exemption codes); signatures; multiple issuers and related persons; TEST filings skipped; blanks → NULL |
| T6 | test_registered | `get_source('sec_form_d')` works; `ddl()` contains the existing `form_d_filings` DDL |
| T7 | test_load_idempotent_pg | (PG) Loads the fixture zip into all five tables with correct values; a second load inserts 0 |
| T8 | test_run_source_flow_pg | (PG) `run_source` with a mocked HTTP layer discovers, fetches and loads, then skips on a rerun |

## Design Notes

- Real format was verified against 2026q2 (3.8 MB zip, 22 MB unzipped) and 2023q3 (3.1 MB):
  - Headers are identical across both. Files are UTF-8 without a BOM, with no CR characters and no quoting.
  - `FILE_NUM` is right-padded with spaces. `FILING_DATE` is `DD-MON-YYYY`. `SALE_DATE` is ISO.
  - Amount fields may be `Indefinite`. `RECIPIENTS` uses the literal `None` for missing CRD numbers.
  - `ISSUER_SEQ_KEY` / `RELATEDPERSON_SEQ_KEY` start at 101.
- Volume: about 13–17k filings/offerings, 13–17k issuers and signatures, and 44–55k related persons per quarter. Loading 2023q3 + 2026q2 used about 128 MB across the five tables (after one reload). 12 quarters ≈ 0.7–0.8 GB.
- Offering amounts are staged as exact NUMERIC. `form_d_filings` gets them cast to BIGINT (NULL if out of range).
- `form_d_filings.cik` keeps SEC's 10-digit zero-padded CIK.
- Security-type checkboxes (`is_equity` and similar) that are blank become `false`, matching the XML path's defaults.
- `accredited_investors` stays NULL: Form D does not report it, and it is never imputed.
- `industry_group` stores SEC's raw value.
- Merging overwrites every mapped column of `form_d_filings` (bulk data is authoritative) and sets `updated_at = NOW()`. `created_at` is preserved.

## Files

- `app/ingest/bulk/sec_form_d/__init__.py`
- `app/ingest/bulk/sec_form_d/source.py`
- `app/ingest/bulk/sec_form_d/parse.py`
- `tests/test_spec_108_sec_form_d_bulk.py`
