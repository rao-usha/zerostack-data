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
- [x] **Ship gate:** run the attribution in report mode first. Proceed only if the new tiers reproduce ≥95% of the 5,094 existing links and add ≥8,000 net new ones; report the numbers before writing.
  - **Measured 2026-09-20: 96.19% reproduced (4,900/5,094), 15,389 net new. Both pass.**
  - The gate first read 94.56% and the rule says a failure means the resolver has
    a bug. It did. Investigating the 277 disagreements showed every one came from
    an ADV tier and none from a name tier changing its mind — but 84 of them were
    AngelList's filing entity taking funds like "Singh Capital Rolling Fund - D1"
    away from Singh Capital. That is the bug the gate caught: see the filing
    platform tier below. With it the shortfall closed on its own.
- [ ] Monthly schedule entry `bulk:sec_adv_schedule_d` at `0 8 4 * *`.

## Test Cases

T1 discover: count, oldest-first, exact URLs, key shape · T2 discover raises on manifest drift · T3 key changes when only `uploadedOn` changes · T4 7B1 parse: 38 headers, 10-digit fund id intact, GAV → Decimal · T5 tri-state blanks → None, free-text opinion survives · T6 both cp1252 and UTF-8 members round-trip · T7 header drift raises naming the missing headers · T8 missing 7B1 member raises listing members · T9 unknown Fund Type passes through · T10 IA and ERA differ only by adviser_type · T11 source registers in BULK_SOURCES · T12 (pg) load twice → second is all zeros, staging dropped · T13 (pg) order independence: Aug→Mar equals Mar→Aug · T14 (pg) unknown filing_id raises, no partial rows · T15 index drops multi-family cores, keeps sibling CRDs · T16 tier refusals (short core, master-name-only, multi-family) · T17 pe_funds_sec records firm_link_method and never downgrades an existing link · T18-T24 mart idempotence, restatement, dry-run, master-name refusal · **T25** platform advisers identified by name collisions, and the threshold is a cliff · **T26** a platform's claim yields to the sponsor the fund is named after, and survives alone as `adv_platform` · **T27** ordinary ADV claims are untouched (StepStone Real Assets keeps its own fund) · **T28** every `DateSubmitted` spelling the SEC has shipped parses.

## Rubric Checklist

- [ ] Formats verified against real files, not assumed
- [ ] Idempotent and order-independent (proven by test)
- [ ] Refusals counted, never silent
- [ ] Single writer for `firm_id`
- [x] Ship gate measured and reported before writing

## Filing platforms (added 2026-09-20, after the gate failed)

An adviser of record is not always a sponsor. AngelList's **PLATFORM ADVISOR,
LLC** (CRD 167700) reports **22,329** private funds on its own Schedule D; it
files them, it does not raise them. Taking its claim at face value handed it
6,435 of 39,141 fund vehicles — 31% of the whole attribution — and displaced
real sponsors from funds that carry their names.

**What does not work.** Fund count: Apollo reports 880 and Ares 720, both real.
Name *affinity* (the share of an adviser's funds carrying its own name): KKR
scores 0.000, because its funds are named "KKR" while its adviser record reads
"Kohlberg Kravis Roberts & Co. L.P." — the same score as AngelList.

**What does.** Count how many **other** advisers' names appear on a filer's
fund list. A GP never names its competitors; a platform names its clients.
Measured over every adviser reporting ≥50 funds:

| adviser | funds reported | other advisers named |
|---|---|---|
| PLATFORM ADVISOR, LLC | 22,329 | **105** |
| GOLDMAN SACHS ASSET MANAGEMENT | 1,358 | **101** |
| SALI FUND SERVICES | 254 | **85** |
| ICAPITAL ADVISORS, LLC | 1,195 | **64** |
| *— gap —* | | |
| MORGAN STANLEY AIP GP LP | 107 | 21 |
| Ares / Apollo / StepStone / KKR / Carlyle | 720 / 880 / 418 / 665 / 337 | 7 / 6 / 6 / 3 / 0 |

`PLATFORM_MIN_COLLISIONS = 25` sits inside a 21→64 gap, so the threshold is a
cliff rather than a knob: any value in that range selects the same four.

**Behaviour.** When a platform is the claimant, the adviser the fund's *name*
identifies wins (tier `name_core`, counted as `platform_displaced`). Only when
the name points nowhere else does the platform keep the link, under a new
weakest tier **`adv_platform`** — the claim is true and is recorded as such, so
a consumer wanting real GPs can exclude that tier rather than lose the row.

Effect on the live build: 320 funds returned to their named sponsor,
disagreements with existing links fell 277 → 191, and reproduction rose
94.56% → 96.25%.

**Live tier counts (39,141 PE/VC Form D fund vehicles):**

| tier | funds |
|---|---|
| `adv_exact` | 11,352 |
| `adv_family` | 638 |
| `name_core` | 1,263 |
| `related_person` | 540 |
| `adv_platform` | 6,652 |
| unlinked | 18,658 |
| **attributed** | **20,445 (52.2%)** |

Excluding `adv_platform`, 13,793 funds (35.2%) are attributed to an adviser
that plausibly sponsors them, against 5,094 before this spec.

A displacement only happens when the sponsor can actually carry the link:
83 funds move, and 233 whose named sponsor has no `pe_firms` row keep the
platform's claim under `adv_platform` rather than losing their manager
(counted as `platform_sponsor_unresolvable`). See
[PLAN_084 Revision 01](../plans/PLAN_084_adv_schedule_d_rev_01.md).

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
