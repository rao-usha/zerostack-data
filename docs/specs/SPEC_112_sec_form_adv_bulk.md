# SPEC 112 — Form ADV bulk loaders (SEC monthly adviser roster + IAPD compilation feed)

**Status:** Draft
**Task type:** collector
**Date:** 2026-09-16
**Test file:** tests/test_spec_112_sec_form_adv_bulk.py
**Plan:** docs/plans/PLAN_082_pe_rebuild_phase0_phase1.md (Phase 1, ADV)
**Framework:** SPEC_107 (`BulkSource`, `SecHttp`, `copy_loader`)

## Goal

Load the SEC's full published Form ADV Part 1A adviser population through two bulk sources.

1. **`sec_adv_roster`** covers the monthly "Information About Registered Investment Advisers and Exempt Reporting Advisers" files on www.sec.gov.
   - There is one zip per month for each population: RIA (`ia…zip`) and ERA (`ia…-exempt…zip`).
   - Each release is stored as a snapshot.
   - The newest roster of each type is also upserted into `public.sec_form_adv` (NULL-preserving).
2. **`sec_iapd_feed`** covers the daily IAPD compilation feed (`IA_FIRM_SEC_Feed_MM_DD_YYYY.xml.gz`) on reports.adviserinfo.sec.gov.
   - One snapshot is stored per edition.

This is ported from the wildcard-workbench connectors `adv_monthly_roster.py` and `iapd_compilation_feed.py`, with these changes:
- The universe filters are removed, so every firm is loaded.
- The finalize differ, timing signals and events are removed.
- ERA files are included.

## Real format findings (measured 2026-09-16)

| Item | Finding |
|---|---|
| Listing page | `https://www.sec.gov/data-research/sec-markets-data/information-about-registered-investment-advisers-exempt-reporting-advisers`, 422 KB HTML. Anchors carry `download` and text like `Registered Investment Advisers, September 2026` / `Exempt Investment Advisers, February 2026`. |
| Filenames seen | `ia051023exempt.zip`, `ia060319-1.zip`, `ia100118_.zip`, `ia09012026-registered.zip`, `ia09012026-exempt.zip`, `ia08032026_1.zip`, `ia08032026-exempt_1.zip`, `ia060126_0.zip`, `ia06012026-exempt_0.zip`, `ia020226-exemptzip.zip`, `ia122025.zip` (MMYYYY!), `ia-no-data-110125.pdf` |
| Directories | Both `/files/investment/data/other/information-about-…/` and `/files/investment/data/information-about-…/` |
| Formats | The page lists 341 zips, 58 xlsx and 4 pdf, going back to 2006. **Only December 2025 and later zips hold CSV** (10 months per type today). Jun 2023 - Sep 2025 are bare `.xlsx`, and Oct and Nov 2025 are "no data" PDFs. **Zips from 2006 - May 2023 wrap an `.xlsx`** (`ia040423.zip`, 13.6 MB, contains `ia040423.xlsx`). All of these are skipped in discover (`CSV_ERA_START = 2025-12-01`). |
| RIA zip (Sep 2026) | 5.33 MB zip → 42.6 MB CSV, **17,149 rows × 448 cols** |
| ERA zip (Sep 2026) | 0.86 MB zip → 6.6 MB CSV, **6,686 rows × 171 cols** (no Item 5 / 8 / 9 columns) |
| CSV encoding | **Not UTF-8**: cp1252 bytes (C9, A0, C4). No BOM, CRLF line endings. Each member is scanned once per file: strict UTF-8 (`utf-8-sig`) if the whole file is valid, otherwise cp1252. Per-byte mixing is unsafe because cp1252 E-acute+NBSP (C9 A0) is valid UTF-8. |
| Numbers | Money looks like `'           628,902,725.00'` and `'                      .00'`. Counts look like `'               1,548'` and `' 18'`. Some cells hold text such as `' More than 500'` and `'Fewer than 5 clients'`, which become NULL. |
| Dates | `MM/DD/YYYY` |
| IAPD manifest | 273 bytes: `{"files":[{"name":"IA_FIRM_SEC_Feed_09_16_2026.xml.gz","size":"79 MB","date":"09/16/2026"}, IA_FIRM_STATE_Feed…, IA_INDVL_Feed….xml.zip]}`. The `size` field is the uncompressed size. |
| IAPD SEC feed | 7.30 MB gz → 82.5 MB XML, ISO-8859-1. `<IAPDFirmSECReport GenOn><Firms><Firm>` holds 23,904 firms (17,200 `Registered` + 6,704 `ERA`), all with CRDs. S3 `x-amz-expiration` is about 8 days. |
| Firm element | `Info@{SECRgnCD,FirmCrdNb,SECNb,BusNm,LegalNm,UmbrRgstn}`, `MainAddr@{Strt1,Strt2,City,State,Cntry,PostlCd,PhNb,FaxNb}`, `MailingAddr@…`, `Rgstn@{FirmType,St,Dt}`, `NoticeFiled/States@{RgltrCd,St,Dt}`, `Filing@{Dt,FormVrsn}`, `FormInfo/Part1A/Item1/WebAddrs/WebAddr`, `Item5A@TtlEmp`, `Item5F@{Q5F2A,Q5F2B,Q5F2C,Q5F2D,Q5F2E,Q5F2F}`, … `Item11H`. Dates are ISO. |

## Acceptance Criteria

### `sec_adv_roster` (`app/ingest/bulk/sec_form_adv/roster.py`)
- [ ] `discover` scrapes the listing page's anchors and classifies each file.
  - [ ] **Type**: from the anchor text ("Registered…" → `ria`, "Exempt…" → `era`), falling back to `exempt` in the filename.
  - [ ] **Roster date**: from the filename (`MMDDYYYY`, `MMDDYY`, or `MMYYYY` → 1st of the month), cross-checked against the month in the anchor text.
- [ ] Only `.zip` files are releases. xlsx, csv and pdf files are counted and logged as skipped.
- [ ] Same type and month with several files (`_N` re-uploads) → keep the highest suffix.
- [ ] Default window (since=None) is the last **36 calendar months** per type, measured back from the newest listed zip. With `since`, return every CSV-era zip with roster_date >= since. Pre-December-2025 zips (xlsx inside) are never releases. Results are oldest first.
- [ ] Release key is `ria:YYYY-MM-DD` / `era:YYYY-MM-DD`. Meta holds `adviser_type`, `roster_date`, `filename`, `newest_roster_date` (per type).
- [ ] `load` unzips the CSV member and checks the required headers for that type. Missing headers raise loudly.
- [ ] It streams the rows through `copy_rows` into `stg.sec_adv_roster`, then merges into `public.sec_adv_roster_snapshots` on `(crd_number, roster_date, adviser_type)`.
- [ ] If the release is the newest roster of its type (roster_date ≥ max(db max for that type, meta.newest_roster_date)), upsert every CRD into `public.sec_form_adv` with `SET col = COALESCE(EXCLUDED.col, sec_form_adv.col)`.
- [ ] `ddl()` creates the snapshot table and its indexes, plus `sec_form_adv` using the same DDL as `app/sources/sec/formadv_metadata.py`. It is CREATE IF NOT EXISTS only, never ALTER.

### `sec_iapd_feed` (`app/ingest/bulk/sec_form_adv/iapd_feed.py`)
- [ ] `discover` reads the manifest and returns one release, `edition:YYYY-MM-DD`, for `IA_FIRM_SEC_Feed_*.xml.gz`. If the edition is older than `since`, it returns nothing.
- [ ] A changed manifest shape, or a missing SEC feed entry, raises.
- [ ] `load` reads the gzip with `ET.iterparse`. After each `<Firm>` it clears the parent container, so memory stays bounded.
- [ ] It merges into `public.sec_adv_feed_firm_state` on `(crd_number, edition_date)`.

### Parsing (`parse.py`)
- [ ] Money/number parser handles padded, comma-grouped values and `.00`, and returns None for text.
- [ ] Y/N → bool. `MM/DD/YYYY` and ISO dates → date.
- [ ] Per-file UTF-8/cp1252 encoding detection.
- [ ] Notice-filed states → sorted state-code list.
- [ ] `raw` JSONB holds only non-empty, unmapped columns or attributes.

## Tables

`public.sec_adv_roster_snapshots` has PK `(crd_number, roster_date, adviser_type)` and indexes on `crd_number` and `roster_date`.
- **Identity:** crd_number, roster_date, adviser_type (`ria`/`era`), sec_region, sec_number, firm_type, legal_name, business_name.
- **Main office:** main_office_street1/2, city, state, country, postal_code, phone, fax, website.
- **Status:** sec_status, sec_status_effective_date, latest_filing_date, form_version, notice_filed_states TEXT[], other_office_count.
- **Employees and clients:** employees_total, employees_investment_advisory, clients_count, clients_individuals, clients_hnw, clients_pooled_vehicles.
- **AUM and accounts:** aum_discretionary, aum_non_discretionary, aum_total, accounts_discretionary, accounts_non_discretionary, accounts_total.
- **Custody:** custody_client_cash_securities, custody_related_person, custody_amount.
- **Private funds and disclosures:** private_fund_count, private_fund_gross_assets, has_disciplinary_disclosure, drp_flags JSONB, drp_disclosure_total.
- **Load metadata:** raw JSONB, source_release_key, loaded_at.

`public.sec_adv_feed_firm_state` has PK `(crd_number, edition_date)` and indexes on `crd_number` and `edition_date`.
- **Identity:** crd_number, edition_date, sec_region, sec_number, firm_type, business_name, legal_name.
- **Registration and filing:** registration_status, registration_date, filing_date, form_version.
- **Main office:** main_office_city, main_office_state, main_office_country, website.
- **Size:** employees_total, aum_discretionary, aum_non_discretionary, aum_total, accounts_total.
- **Other:** notice_filed_states TEXT[], raw JSONB, source_release_key, loaded_at.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_roster_listing_classification | Real anchor shapes → correct type/date/suffix. xlsx and pdf skipped. MMYYYY handled. Re-upload dedupe. |
| T2 | test_roster_discover_window_and_since | 36 calendar months per type by default; `since` filter; pre-CSV-era zip and xlsx excluded; oldest first; `newest_roster_date` meta |
| T3 | test_parse_numbers_dates_flags | Padded money, `.00`, counts with commas, text → None, Y/N, dates, notice states |
| T4 | test_roster_csv_parse_real_headers | RIA and ERA CSV fixtures with real headers, cp1252 bytes, BOM variant, raw excludes mapped/empty, drp_flags |
| T5 | test_roster_missing_header_raises | Header drift raises |
| T6 | test_iapd_manifest_discover | Manifest parsing, release key, `since`, shape-change raises |
| T7 | test_iapd_iterparse_fixture | Small gz fixture → rows with mapped fields and raw |
| T8 | test_roster_load_idempotent_and_form_adv_null_preserving_pg | (PG) Load twice with the same counts. sec_form_adv upsert keeps pre-existing values where the roster is NULL. A non-newest release doesn't touch sec_form_adv. |
| T9 | test_iapd_load_idempotent_pg | (PG) Load twice with the same counts |
| T10 | test_sources_registered | Both names in the registry |

## Estimates

- **Roster (measured):** a real Sep 2026 load was RIA 17,149 + ERA 6,686 = 23,835 rows and 53 MB total relation size (avg raw 1.5 KB RIA / 0.8 KB ERA). Load time was 7.0 s + 1.3 s, with 66 MB peak RSS. Today's 20 releases come to ~240k rows and ~0.55 GB. At 36 months ~860k rows and ~1.9 GB. sec_form_adv is ~24k rows and 13 MB.
- **IAPD (measured):** 23,904 rows per edition and 40 MB (avg raw 1.3 KB). Load took 6.5 s with 67 MB peak RSS. Daily loads come to ~14 GB/yr, so a retention policy is needed (open issue).

## Open issues

- xlsx-era rosters (2006 - Nov 2025, both bare and zip-wrapped xlsx) aren't loaded. That would need openpyxl plus header mapping, and old layouts may differ.
- The IAPD S3 objects persist for about 8 days, but the manifest names only the latest edition. Missed days aren't backfilled.
- If a firm is in both the newest RIA and newest ERA roster (a transition month), whichever loads last wins in sec_form_adv.
