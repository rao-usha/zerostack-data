# SPEC 118 — Form ADV Schedule D private funds loader + attribution upgrade

**Status:** Draft
**Task type:** collector
**Date:** 2026-09-20
**Test file:** tests/test_spec_118_adv_schedule_d_bulk.py
**Plan:** docs/plans/PLAN_084_adv_schedule_d.md

## Goal

Only 5,101 of 39,148 `pe_funds` rows (13%) name a manager, because a Form D fund vehicle shares no identifier with its GP. Form ADV Schedule D 7.B.(1) is the one public source that lists, per adviser, every private fund it manages. Load it and use it to attribute funds.

**Formats verified against the real August 2026 file on 2026-09-20** (not assumed):
- Manifest `https://reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json` → `advFilingData[<year>].files[]` with `fileName`, `size`, `uploadedOn`.
- Download `https://reports.adviserinfo.sec.gov/reports/foia/advFilingData/{year}/{fileName}`; `ADV_Filing_Data_20260801_20260831.zip` is 7.2 MB with 101 members. **Data runs through 2026-08 — the "ends Dec 2024" claim in the earlier review is wrong** (that is the sec.gov FOIA archive, a different host).
- `IA_Schedule_D_7B1_*.csv` = 18,218 rows, `ERA_Schedule_D_7B1_*.csv` = 876 rows, both **38 columns**: FilingID, Fund Name, Fund ID, ReferenceID, State, Country, 3(c)(1) Exclusion, 3(c)(7) Exclusion, Master Fund, Feeder Fund, Master Fund Name, Master Fund ID, Fund of Funds, Fund Invested Self or Related, Fund Invested in Securities, Fund Type, Fund Type Other, Gross Asset Value, Minimum Investment, Owners, %Owned You or Related, %Owned Funds, Sales Limited, %Owned Non-US, Subadviser, Other IAs Advise, Clients Solicited, Percentage Invested, Exempt from Registration, Annual Audit, GAAP, FS Distributed, Unqualified Opinion, Prime Brokers, Custodians, Administrator, % Assets Valued, Marketing.
- `Fund ID` is `805-` + **ten** digits (e.g. `805-9253414470`), not seven.
- **7B1 carries no CRD.** CRD is `1E1` in `IA_ADV_Base_A_*.csv` / `ERA_ADV_Base_*.csv`, joined on `FilingID`.
- Members are **UTF-8** on this host (the older FOIA archive is cp1252), so encoding is detected per file, never assumed.
- Each monthly file holds only that month's filings (Aug: 2,386 IA filings), so coverage comes from loading the whole 2025-01…2026-08 window; March is the big one (annual amendment deadline).

## Acceptance Criteria

- [ ] **Filing-grain, append-only tables** (migration `0010_adv_private_funds`): `sec_adv_filings` (filing_id PK → crd_number, adviser_type, sec_number, legal_name, filed_at) and `sec_adv_private_fund_filings` (PK `(filing_id, private_fund_id)`, all 38 fields typed). Filing grain is what makes the loader order-independent: monthly releases can load in any order, and a restated release cannot regress a row to an older filing.
- [ ] **Current-state mart** `sec_adv_private_funds` (PK `(crd_number, private_fund_id)`), recomputed from scratch each run via `DISTINCT ON … ORDER BY filed_at DESC`.
- [ ] **Loader** `sec_adv_schedule_d`: discovers every `advFilingData` release from the manifest (oldest first), release key `adv1:{period}:{uploadedOn}` so an SEC re-upload is seen as new work; loads filings before funds and **raises if a fund row's filing_id is unknown** rather than writing an unattributable fund.
- [ ] **Attribution tiers** in `app/marts/links.py`, ranked above `name_core`: `adv_exact` (core matches exactly one ADV fund whose CRD maps to one firm), `adv_family` (several ADV rows, one CRD family). Refuse: multi-family hits, ≤2-token cores, master-fund-name-only hits, and overwriting an existing different `firm_id`.
- [ ] `pe_funds_sec.build()` stays the **sole writer** of `firm_id`; it records `firm_link_method` per row.
- [ ] **Ship gate:** run the attribution in report mode first. Proceed only if the new tiers reproduce ≥95% of the 5,101 existing links and add ≥8,000 net new ones; report the numbers before writing.
- [ ] Monthly schedule entry `bulk:sec_adv_schedule_d` at `0 8 4 * *`.

## Test Cases

T1 discover: count, oldest-first, exact URLs, key shape · T2 discover raises on manifest drift · T3 key changes when only `uploadedOn` changes · T4 7B1 parse: 38 headers, 10-digit fund id intact, GAV → Decimal · T5 tri-state blanks → None, free-text opinion survives · T6 both cp1252 and UTF-8 members round-trip · T7 header drift raises naming the missing headers · T8 missing 7B1 member raises listing members · T9 unknown Fund Type passes through · T10 IA and ERA differ only by adviser_type · T11 source registers in BULK_SOURCES · T12 (pg) load twice → second is all zeros, staging dropped · T13 (pg) order independence: Aug→Mar equals Mar→Aug · T14 (pg) unknown filing_id raises, no partial rows · T15 index drops multi-family cores, keeps sibling CRDs · T16 tier refusals (short core, master-name-only, multi-family) · T17 pe_funds_sec records firm_link_method and never downgrades an existing link.

## Rubric Checklist

- [ ] Formats verified against real files, not assumed
- [ ] Idempotent and order-independent (proven by test)
- [ ] Refusals counted, never silent
- [ ] Single writer for `firm_id`
- [ ] Ship gate measured and reported before writing

## Files to Create/Modify

| File | Action |
|------|--------|
| alembic/versions/0010_adv_private_funds.py | Create |
| app/ingest/bulk/sec_adv_schedule_d/{__init__,parse,source}.py | Create |
| app/marts/adv_private_funds.py | Create |
| app/marts/links.py, app/marts/pe_funds_sec.py | Modify (tiers, firm_link_method) |
| app/core/pe_models.py | Modify (ORM drift: firm_id nullable, cik, sec_fund_id, firm_link_method) |
| app/core/scheduler_service.py | Modify (schedule entry) |
| tests/test_spec_118_adv_schedule_d_bulk.py | Create |

## Feedback History

_No corrections yet._

## Revisions

- **[Revision 01](PLAN_084_adv_schedule_d_rev_01.md)** (2026-09-20) — adversarial
  review before the first production write. Two defects in the filing-platform
  rule (collision metric counted CRDs sharing a stem rather than distinct
  adviser names; displacement discarded 234 of 320 links by re-pointing at
  advisers with no `pe_firms` row) and two pre-existing `name_core` weaknesses
  the rule promotes (89 mid-token cuts, 1,079 links on single-token cores).
