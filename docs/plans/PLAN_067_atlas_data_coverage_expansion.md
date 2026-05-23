# PLAN 067 — Atlas Data Coverage Expansion

**Status:** Draft — awaiting approval
**Date:** 2026-05-22
**Phase:** 3a (parallel — see `ATLAS_PROGRAM_ROADMAP.md`)
**Builds on:** the PLAN_066 §4 data-completeness review
**Gating:** none — additive. Does NOT block PLAN_066. Upgrades layers in place.

---

## 1 · Why this plan

The PLAN_066 §4 audit graded every Atlas data source against verified
coverage. Six items came back 🟡 / 🔴 / ⏸ — meaning a layer the product
*wants* either can't be drawn at the promised grain, is a thin slice, or is
missing entirely. PLAN_066 ships the map with the ✅ layers and is honest
about the rest. **This plan turns the not-✅ items into ✅** so the map gets
more complete and more honest over time.

Principle: **the map never promises data it doesn't have.** Today that means
omitting/demoting layers. PLAN_067 is how we earn them back — with real
ingest, verified the same way (§4-style coverage check must return ✅).

---

## 2 · The gap inventory (from PLAN_066 §4)

| Gap | Current state | Target | Spec |
|---|---|---|---|
| **County wealth/demand** | `acs5_2023_b19013` keyed by ZCTA (ZIP), `state_fips` empty — not county-joinable | County-grain median income + demand | SPEC_070 |
| **Federal money** | `usaspending_awards` — 10,190 rows, dates all null — a thin, dateless stub | Real awards, dated, county-geocoded — enough for a layer + a Recent Activity source | SPEC_071 |
| **County broadband** | `fcc_broadband_coverage` — `geography_type` is `state` only | County-grain broadband availability | SPEC_072 |
| **Industry over time** | `census_cbp` 2021 only, ~89% counties; `census_business_patterns` 2022 only | Multi-year CBP, full county coverage | SPEC_073 |
| **SEC in Recent Activity** | `sec_company_metadata` has no filing-date column | A filings table with `filing_date`, type, CIK, location | SPEC_074 |

---

## 3 · Specs

Each spec is **pre-flight → ingest → verify**. The verify step re-runs the
PLAN_066 §4 coverage probe and must return ✅ for the spec to close.

### SPEC_070 — ACS → county wealth crosswalk
**Goal:** county-grain median household income + demand signal.
- **Pre-flight:** the Census publishes ZCTA→county relationship files
  (`zcta_county_rel`). Confirm format + currency.
- **Build:** ingest the ZCTA→county crosswalk into a reference table;
  optionally also ingest ACS B19013 *at the county summary level directly*
  from the Census API (cleaner than crosswalking ZIP up) — pre-flight decides
  which is more reliable.
- **Verify:** county-grain median income exists for ≥3,000 counties.
- **Unlocks:** PLAN_066's deferred ACS county-wealth layer; richens the Local
  Wealth layer beyond IRS SOI alone.

### SPEC_071 — USAspending real ingest
**Goal:** a real, dated, geocoded federal-awards dataset.
- **Pre-flight:** the current 10k-row stub is unusable. Scope the real ingest
  deliberately — USAspending is millions of awards/year. Target a **bounded
  window** (last 2–3 fiscal years), prime + sub awards, with
  `action_date` / period dates and `place_of_performance` geocoded to county.
  Use the USAspending bulk-download or API.
- **Build:** ingestor following `BaseSourceIngestor`; county-geocode the
  place-of-performance; keep award date, amount, NAICS, agency.
- **Verify:** awards span the window with non-null dates; county coverage
  measured; row count realistic (hundreds of thousands+, not 10k).
- **Risk:** volume on cloud `db-f1-micro`. Mitigate — bound the window,
  aggregate to county×NAICS×month at ingest if raw rows are too heavy.
- **Unlocks:** the Federal Money layer (cut from PLAN_066 v1) **and** a real
  Recent Activity source (SPEC_067 currently leans on FEMA alone).

### SPEC_072 — FCC county broadband
**Goal:** county-grain broadband availability.
- **Pre-flight:** the FCC Broadband Data Collection (BDC) publishes coverage
  at far finer grain than the state rollup we currently hold. Confirm the
  county/block availability of the BDC public files.
- **Build:** re-ingest FCC broadband at county grain — providers,
  technologies, max speeds per county.
- **Verify:** county rows for ≥3,000 counties.
- **Unlocks:** the Connectivity layer as a county choropleth (PLAN_066 ships
  it state-only).

### SPEC_073 — Multi-year CBP + county fill
**Goal:** an industry layer that has a time dimension and full county coverage.
- **Pre-flight:** Census CBP is published annually; we hold one year. Confirm
  the multi-year availability + the county-level files.
- **Build:** ingest CBP for a span of years (e.g. last 5) at county × NAICS;
  fill the ~11% county gap.
- **Verify:** ≥5 years present; ≥3,000 counties.
- **Unlocks:** the Industry Density layer at county grain + the PLAN_066
  v2.1 time-scrubber (animate the industry base over time).

### SPEC_074 — SEC filings table with dates
**Goal:** SEC as an honest Recent Activity source.
- **Pre-flight:** `sec_company_metadata` has no filing dates. Check whether a
  SEC submissions/filings table already exists in the codebase (the project
  has SEC ingest); if so, confirm it has `filing_date` + form type + CIK.
- **Build:** if absent, an ingestor for the SEC EDGAR submissions feed —
  filing date, form type (S-1, 8-K, 10-K…), CIK, filer location.
- **Verify:** recent filings present with dates.
- **Unlocks:** SEC pins in the Recent Activity layer (new S-1s, material 8-Ks).

---

## 4 · Sequencing

No hard internal order — each spec is independent. Priority by **layer
value × effort**:

```
SPEC_071 (USAspending)  — highest value (unlocks a layer + activity source), highest effort
SPEC_070 (ACS county)   — high value (county wealth), low effort
SPEC_072 (FCC county)   — medium value, medium effort
SPEC_074 (SEC filings)  — medium value (activity richness), low-medium effort
SPEC_073 (multi-year CBP) — enables a v2.1 feature, do last
```

Recommend SPEC_070 first (cheap win, county wealth) then SPEC_071 (the big one).

---

## 5 · Non-goals
- No new data *domains* — this plan only completes sources Atlas already uses.
  New domains are a separate decision.
- No scraping / no non-public data — Census, FCC, USAspending, SEC EDGAR are
  all official open bulk/API sources.
- No blocking PLAN_066 — the map ships first; layers upgrade as backfills land.

## 6 · Definition of done
Each spec closes only when its §4-style coverage probe returns ✅ and the
corresponding PLAN_066 layer is re-rated from 🟡/🔴/⏸ to ✅ in PLAN_066 §5.1.
