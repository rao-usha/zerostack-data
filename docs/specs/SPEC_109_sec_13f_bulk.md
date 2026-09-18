# SPEC 109 — SEC Form 13F data sets bulk loader (`sec_13f`)

**Status:** Draft
**Task type:** collector
**Date:** 2026-09-16
**Test file:** tests/test_spec_109_sec_13f_bulk.py
**Depends on:** SPEC_107 (BulkSource framework)

## Goal

Load the SEC Form 13F data sets into Postgres through the SPEC_107 bulk framework. The data sets are the quarterly ZIPs at
https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets.

The loader writes three tables:
- **filings:** SUBMISSION, COVERPAGE, SUMMARYPAGE and SIGNATURE joined into one row per accession.
- **holdings:** the INFOTABLE positions.
- **other managers:** OTHERMANAGER and OTHERMANAGER2.

Cloud SQL disk is small, so the loader limits what it keeps:
- filings for the last 3 years of data sets
- holdings for only the newest 2 data sets

## Acceptance Criteria

- [ ] `app/ingest/bulk/sec_13f/` registers the bulk source `sec_13f` (`@register_bulk_source`), and the package `__init__` imports `source`.
- [ ] **`discover(http, since)`**
  - [ ] Reads the index page once and collects every `*_form13f(_N).zip` href. Both naming schemes are supported: `01jun2026-31aug2026_form13f.zip` (date range) and `2023q4_form13f.zip` (calendar quarter).
  - [ ] Hrefs are resolved against `https://www.sec.gov`.
  - [ ] Each release gets `meta.start_date` and `meta.end_date`.
  - [ ] When a date range was re-uploaded (`_N` suffix), only the lexically last basename is kept.
  - [ ] Releases are returned oldest first.
  - [ ] When `since` is None, only releases whose `end_date >= today - 3 years` are kept. Otherwise the filter is `end_date >= since`.
  - [ ] The newest `HOLDINGS_RELEASES = 2` releases **of all releases listed** (override with env `BULK_13F_HOLDINGS_RELEASES`) get `meta.load_holdings = True`. Every release carries `meta.holdings_keep_keys`, the release keys of those newest N.
  - [ ] If the page lists no data-set hrefs, it raises: the layout has drifted, and that must not look like "nothing new".
- [ ] **Parsing (`parse.py`, no DB)**
  - [ ] Zip members are streamed via `zipfile.open` + `TextIOWrapper` + `csv.reader(delimiter='\t', quoting=QUOTE_NONE)`.
  - [ ] Members are matched by basename, case-insensitive.
  - [ ] Headers are upper-cased and stripped, with any BOM removed.
  - [ ] `csv.field_size_limit` is raised.
  - [ ] Fields the SEC wrapped in quotes (`"a ""b"""`) are unwrapped.
  - [ ] Blanks become NULL.
  - [ ] Dates are parsed from `DD-MON-YYYY` (ISO is also accepted).
  - [ ] Integers and numerics are parsed; unparseable values become NULL.
  - [ ] Y/N flags become booleans.
  - [ ] A missing optional member (SIGNATURE, SUMMARYPAGE, OTHERMANAGER*) yields nothing.
  - [ ] A missing SUBMISSION raises.
- [ ] **`load(conn, release, path)`**
  - [ ] Calls `ensure_ddl`, then streams rows into `stg.sec_13f_*` with COPY and merges them into the targets.
  - [ ] Returns `{sec_13f_filings, sec_13f_other_managers[, sec_13f_holdings]}` row counts (inserted + updated).
  - [ ] Idempotent: loading the same zip twice leaves the same row counts.
  - [ ] INFOTABLE is read **only** when `meta.load_holdings` is true.
  - [ ] After loading holdings, it prunes holdings rows whose `source_release_key` is not in `meta.holdings_keep_keys`. `BULK_13F_PRUNE_HOLDINGS=0` disables pruning.
- [ ] **VALUE units:** the readme says "Starting on January 3, 2023, market value is reported rounded to the nearest dollar. Previously, market value was reported in thousands." `value` and `table_value_total` are stored **in dollars**. For filings with `filing_date < 2023-01-03` the loader multiplies by 1000, and `value_multiplier` records the factor applied. Every data set in the 3-year window is already in dollars (factor 1).

## Target tables

`public.sec_13f_filings` — PK `accession_number`
- **Submission:** cik, submission_type, filing_date, period_of_report
- **Cover page:** report_calendar_or_quarter, is_amendment, amendment_no, amendment_type, conf_denied_expired, date_denied_expired, date_reported, reason_for_non_confidentiality, filing_manager_name, filing_manager_street1, filing_manager_street2, filing_manager_city, filing_manager_state_or_country, filing_manager_zipcode, report_type, form13f_file_number, crd_number, sec_file_number, provide_info_for_instruction5, additional_information
- **Summary page:** other_included_managers_count, table_entry_total, table_value_total (dollars), is_confidential_omitted
- **Signature:** signature_name, signature_title, signature_phone, signature, signature_city, signature_state_or_country, signature_date
- **Load metadata:** value_multiplier, source_release_key, loaded_at
- **Indexes:** cik, period_of_report, crd_number, filing_manager_name

`public.sec_13f_holdings` — PK `(accession_number, infotable_sk)`
- **Columns:** name_of_issuer, title_of_class, cusip, figi, value (NUMERIC, dollars), ssh_prnamt, ssh_prnamt_type, put_call, investment_discretion, other_manager, voting_auth_sole, voting_auth_shared, voting_auth_none, source_release_key, loaded_at
- **Indexes:** cusip, source_release_key

`public.sec_13f_other_managers` — PK `(accession_number, list_type, sequence_number)`
- **Columns:** cik, form13f_file_number, crd_number, sec_file_number, name, source_release_key, loaded_at
- **Indexes:** cik, crd_number
- **`list_type`:**
  - `'cover'` comes from OTHERMANAGER, the "other managers reporting for this manager" list. Its `sequence_number` is `OTHERMANAGER_SK`.
  - `'summary'` comes from OTHERMANAGER2, the "other included managers" list. Its `sequence_number` is `SEQUENCENUMBER`.
  - The two lists mean different things, so `list_type` is part of the key.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_parse_index_hrefs_both_schemes | Date-range and `YYYYqN` hrefs parse to (key, url, start, end); non-13F links ignored; `_N` re-upload wins |
| T2 | test_select_releases_window_and_holdings_flag | Default 3-year window, `since` filter, oldest-first order, newest-2 `load_holdings`, env override |
| T3 | test_discover_uses_http_and_raises_on_empty | discover() calls get_text once; raises when no hrefs |
| T4 | test_value_helpers | Dates (DD-MON-YYYY/ISO/blank), ints, numerics, Y/N, quote unwrapping |
| T5 | test_iter_tsv_real_headers_and_limits | Real header set, tab delimiting, blanks → None, BOM, quoted fields, >131KB field |
| T6 | test_iter_filings_joins_members | SUBMISSION+COVERPAGE+SUMMARYPAGE+SIGNATURE joined; pre-2023 multiplier; missing optional members |
| T7 | test_iter_holdings_and_other_managers | INFOTABLE rows typed + value normalised; OTHERMANAGER/OTHERMANAGER2 list_type rows |
| T8 | test_registry_has_sec_13f | `get_source("sec_13f")` works |
| T9 | test_load_idempotent_with_and_without_holdings_pg | (PG) fixture zip loaded without holdings → no holdings rows; with holdings twice → same counts; values correct |
| T10 | test_prune_old_holdings_pg | (PG) loading newer release prunes holdings of a release outside keep keys |

## Design Notes

- **Format checks** (real data set `01jun2026-31aug2026_form13f.zip`, downloaded 2026-09-16):
  - The zip is 96 MB (100,719,015 bytes). It holds 7 TSVs plus `FORM13F_metadata.json` and `FORM13F_readme.htm`.
  - Row counts: SUBMISSION, COVERPAGE and SIGNATURE 11,852 each; SUMMARYPAGE 10,036; OTHERMANAGER 4,848; OTHERMANAGER2 3,890.
  - INFOTABLE has 3,830,274 rows (397 MB uncompressed) and no duplicate `(ACCESSION_NUMBER, INFOTABLE_SK)` keys.
  - OTHERMANAGER2 has 2 duplicate `(ACCESSION_NUMBER, SEQUENCENUMBER)` keys. The merge keeps the last row.
  - Line endings are LF. Every field containing `"` is fully wrapped in quotes with inner quotes doubled, and no field contains an embedded tab or newline. The loader therefore parses with `QUOTE_NONE` and unwraps quoted fields itself: a stray quote can't swallow rows, and wrapped values still come out clean.
- **Measured volume:** the real data set was loaded into the disposable test DB in 188 s (one transaction).
  - `sec_13f_holdings`: 3,830,274 rows, 745 MB heap + 243 MB indexes, about **1.0 GB per data set**. The default 2 data sets come to **about 2 GB**.
  - While a data set loads, `stg.sec_13f_holdings` (about 0.7 GB) and the sort for DISTINCT ON take extra space. Pruned rows stay as dead tuples until vacuum. Peak use can briefly reach about 3.5 GB.
  - `sec_13f_filings`: 11,852 rows, about 6 MB per data set. The 3-year window (about 12 data sets) comes to about 140k rows and 70 MB.
  - `sec_13f_other_managers`: 8,735 rows, about 2.4 MB per data set (about 30 MB over 3 years).
  - The raw zips (~80–100 MB each, about 1.1 GB for 3 years) stay under `bulk_raw_dir`.
  - Sanity check: SUM(value) for the data set is about $84T, which confirms dollars, not thousands.
- **Index naming:** 2013q2–2023q4 use `YYYYqN`. From 2024 the files use date ranges (`01jan2024-29feb2024` was a 2-month transition set). The newest link is under `/files/datastandardsinnovation/...`; older ones are under `/files/structureddata/...`. The loader never builds URLs from a template; it always scrapes the hrefs.
- **Release key:** the zip basename without `.zip`, e.g. `01jun2026-31aug2026_form13f` or `2023q4_form13f`.
- **Pruning:** holdings for older data sets are deleted in the same transaction that loads a newer holdings set, which keeps disk use near 2 data sets. Postgres reuses the freed space after (auto)vacuum; it isn't returned to the OS.
- **Filings join:** the 12k filing-level rows per data set are joined in memory. INFOTABLE is only ever streamed.
- **Not ported from workbench:** the `fin_universe` CRD match and the universe filter. Unlike workbench, positions are kept, not folded into counts.
- **Not handled:** 13F-NT filings have no holdings. `table_entry_total` is kept as reported.

## Files

- `app/ingest/bulk/sec_13f/__init__.py`
- `app/ingest/bulk/sec_13f/parse.py` — index + TSV parsing (pure)
- `app/ingest/bulk/sec_13f/source.py` — `Sec13FDataSets(BulkSource)`
- `tests/test_spec_109_sec_13f_bulk.py`
