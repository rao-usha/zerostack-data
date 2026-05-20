# PLAN 065 — Nexdata Sector × Market Intelligence Pack

**Status:** Draft (v2) — awaiting approval
**Date:** 2026-05-16
**Supersedes:** PLAN_064 (paused). This is a rewrite of PLAN_065 v1 (the
"diligence memo" framing) — the wedge changed from target-evaluation to
landscape-discovery after user feedback.
**Approx. effort:** 30 calendar days; ~3 working sessions of code + ongoing service delivery.
**Goal:** First paid revenue inside 30 days via a productized **sector × geography market-intelligence pack** that uses public-data we have verified is populated.

---

## 1 · Strategic frame

### 1.1 Why this, not "diligence memo"

A target-diligence memo competes with the work the PE associate already does
on the deal in front of them. The buyer's reaction is "we already cover
that internally." The wedge has to be something they *can't* assemble from
internal capacity.

**Pivoting to a landscape-discovery artifact** — a sector × geography map —
flips the use case from "evaluate the deal in front of me" to "find me deals
I'm not yet looking at." Now the buyer is the deal sourcer, not the deal
analyst, and the question we answer is structural ("where is the herd of
metal fabricators dense and growing") rather than target-specific ("is this
particular target a good buy"). The deal sourcer / investment thesis owner
has budget and no internal substitute for what we can produce.

### 1.2 The wedge

> **Nexdata Sector × Market Intelligence Pack** — a 24–48-hour, decision-grade
> map of the operating landscape for a chosen NAICS sector × MSA / state /
> region, assembled from governed public sources (Census CBP, ACS, IRS SOI,
> SEC, FDIC, FRED, BEA, BLS, Treasury, FEMA, EPA, FCC, BTS, USDA, USAspending,
> CFTC). Includes: structural establishment density, demand context, operating
> environment, risk profile, infrastructure proximity, federal dollar flow,
> trade exposure, visible public-co operators in scope, and a best-effort
> curated list of named private operators pulled from regulated-facility data.

Positioning copy: **"The market before the company."**

### 1.3 ICP shortlist (reordered to fit landscape-discovery)

1. **Search funders** — pre-acquisition thesis sharpening. Highest fit; biggest
   pain (can't justify Bain LEK; budget is a rounding error).
2. **Independent sponsors** doing LMM-industrial deals — sector-thesis-led.
3. **Lower-middle-market PE associates / VPs** at sub-$500M-AUM funds — same
   pattern.
4. **Operating partners / advisory firms** building expansion theses for
   portfolio cos.
5. **Industrial operators** evaluating market entry / expansion (CapEx side).
6. **(Demoted from PLAN_065 v1)** Lenders — target-evaluation-flavored, less
   natural fit for a landscape product.

Explicit non-targets for 30 days: corporate strategy teams, mega-funds, banks
doing vendor onboarding, anyone requiring SOC2 / RFP / procurement cycles.

### 1.4 Pricing ladder

| SKU | Price | Output | Trigger to upsell |
|---|---|---|---|
| Single Sector × Market Map | $2,500 | 1 sector × 1 geo map | After 1 — propose pilot |
| 3-Map Pilot | $7,500 | 3 sector × geo combos + comparable view | After pilot — propose retainer |
| Sector-Thesis Retainer | $10K–$15K / mo | Weekly refresh of a portfolio of sector × geo combos; new comps and signals | After 2 mo — propose annual |
| Future SaaS / self-serve | $20K–$60K / yr | Once the report template is fully automated | — |

Stripe Payment Links cover the first two SKUs. Retainer = invoice.

### 1.5 What we are explicitly NOT promising

| Claim | Why we can't |
|---|---|
| Comprehensive private-company directory | We do not host one. Named privates are best-effort curation from regulated-facility data + manual research per order. |
| Management-team / org-chart depth | `people`, `pe_people`, `lp_*` tables empty locally; unverified on cloud |
| LinkedIn / social-graph scraping | Out of project constraints |
| Real-time SLA / SSO / SOC2 | Out of 30-day scope |
| Custom programmatic API access to underlying data | DB and report-gen are the product; API access is a later upsell |
| Coverage of every sector at equal depth | The named-privates section is strongest for industrial / healthcare / govcon sectors where regulated-facility data exists. We will say so plainly in the report. |

---

## 2 · Verified data coverage → map section design

Coverage matrix from 2026-05-16 sync (post local→cloud push). Cloud is the
authoritative source. Each section maps to ≥1 populated table; sections
without data are skip-on-empty.

### 2.1 Structural density (the core differentiator)

| Section | Source | Cloud rows | Notes |
|---|---|---|---|
| **Establishment density** by NAICS × MSA | `census_cbp` (county × NAICS-6, rolled up to NAICS-4) | 7,160 county-NAICS-6 rows | **Audit-confirmed (SPEC_060 run 2026-05-17):** 359/393 MSAs (91%) have ≥90% county coverage. SPEC_061 §3 truncates NAICS-6 → NAICS-4 and SUMs across MSA constituent counties. |
| Establishment density (state fallback) | `census_business_patterns` | 63,369 | Used for the 7 MSAs with <50% county coverage in `census_cbp`, OR when the buyer picks a state instead of an MSA. Full NAICS-2 → 6 depth. |

### 2.2 Demand / wealth / labor context

| Section | Source | Cloud rows |
|---|---|---|
| Local wealth (zip-level median income) | `acs5_2023_b19013` | 33,772 |
| Tax-return wealth (zip) | `irs_soi_zip_income` | 331,168 |
| County income | `irs_soi_county_income` | 100,544 |
| Net-migration $$ (county-to-county) | `irs_soi_migration` | 447,240 |
| Business income (zip) | `irs_soi_business_income` | 110,398 |

### 2.3 Operating environment

| Section | Source | Cloud rows |
|---|---|---|
| Regional banking depth + health | `fdic_bank_financials` (deposit/loan/capital ratios by county aggregation) | 1.67M |
| Bank failures + watchlist context | `fdic_failed_banks` | 3,626 |
| Macro rates + inflation | `fred_interest_rates`, `fred_economic_indicators`, `fred_industrial_production`, `bls_cpi` | 130k+ |
| Labor market | `bls_jolts`, `bls_ces_employment` (skip if NAICS-mismatched per schema-drift) | 1.3k + 2.5k |
| Treasury context | `treasury_daily_balance`, `treasury_monthly_statement` | 220k+ + 9.9k |

### 2.4 Risk profile

| Section | Source | Cloud rows |
|---|---|---|
| Disaster declaration history (county-level) | `fema_disaster_declarations` | 51,093 |
| FEMA federal $$ flow (PA/HMA projects) | `fema_pa_projects` + `fema_hma_projects` | 100k + 52k |
| Forward-looking risk index (county) | `national_risk_index` | 3,564 |
| Flood zones | `flood_zone` | 9,107 |
| Environmental violations / enforcement | `epa_echo_facilities` | 1.07M (sector-rich) |
| Brownfield / wetland / water-system constraints | `brownfield_site` + `wetland` + `public_water_system` | 45k + 40k + 277k |

### 2.5 Infrastructure proximity (industrial wedge specific)

| Section | Source | Cloud rows |
|---|---|---|
| Power generation + transmission + substations | `power_plant` + `transmission_line` + `substation` | 14k + 52k + 8.7k |
| Rail + airports | `rail_line` + `airport` | 71k + 13k |
| Data centers | `data_center_facility` | 1.4k |
| Broadband availability | `fcc_broadband_coverage` + `broadband_availability` | 10k + 10.5k |

### 2.6 Trade + federal $$ exposure

| Section | Source | Cloud rows |
|---|---|---|
| State-level export economy | `us_trade_exports_state` | 546,095 |
| Trade by HS code (proxy for sector trade exposure) | `us_trade_exports_hs` + `us_trade_imports_hs` | 26k + 19k |
| Border crossings | `bts_border_crossing` | 45k |
| Federal contract awards | `usaspending_awards` | 10k |

### 2.7 Public-co operators visible in sector × geo

| Section | Source | Cloud rows |
|---|---|---|
| Public-co operators (SEC SIC code matched to NAICS, HQ state matched to geo) | `sec_company_metadata` + `sec_*_statement` | 2.3k cos / 97k statements |

### 2.8 Named-private operators (best-effort, the new SPEC_065 work)

The honest answer: we don't host a private-company database. We assemble
named privates per-order from public regulated-facility datasets we DO host,
intersected with the chosen NAICS × MSA. Coverage varies by sector.

| Source | Cloud rows | Best for |
|---|---|---|
| `epa_echo_facilities` | 1,068,232 | **Industrial / manufacturing / chemicals / oil-gas / waste / utilities.** EPA carries facility name, address, NAICS, ownership. Strongest signal for our headline sectors. |
| `nppes_providers` | 39,817 | Healthcare-services (medspa, clinics, urgent care, etc.) |
| `usaspending_awards` | 10,190 | Govcon / federal-contractor exposure |
| `cms_hospitals` | 5,426 | Hospital / healthcare facility operators |
| `cms_drug_pricing` + `cms_medicare_utilization` | 71k + 21k | Healthcare-services market structure |

For sectors not regulated by those agencies (consumer services, professional
services, etc.), the named-privates section is shallower and the report
states so explicitly. We do NOT scrape Yelp / Yellow Pages / LinkedIn / state
SoS for v1 — too many T&C surfaces, too easy to break, and the regulated
data already covers the highest-margin industrial sectors.

### 2.9 Sections by NAICS-readiness

The plan assumes we can map between NAICS (chosen by buyer) and SIC
(what SEC reports use) + the NAICS codes our regulated-facility tables
already carry. This needs a 1-time bootstrap in pre-flight (§5 SPEC_060):
load the Census NAICS 2022 dictionary + NAICS↔SIC crosswalk into a small
reference table. ~2k rows total.

---

## 3 · 30-day commercialization plan

### Week 1 (May 16–22) — **Make the artifact real, end-to-end**

- Pick **1 sector × 1 MSA** you'd actually buy ("metal fabrication × Houston-Sugar-Land-Baytown MSA", or "industrial laundry × DFW", or whatever your network knows). Hand-assemble the map in markdown using the §2 sources directly via SQL on cloud. ~8 hours. This is the artifact spec; everything else exists to reproduce it at scale.
- Bootstrap NAICS dictionary + MSA dictionary + NAICS↔SIC crosswalk as a static JSON the intake UI loads.
- Verify Census CBP NAICS grain on cloud (SPEC_060 pre-flight): confirm we have CBP rows at the NAICS-4/5/6-digit × county grain we need, or surface gaps.
- Stand up Stripe Payment Links for $2,500 / $7,500.
- Identify 30 named ICPs — bias to search funders + indep sponsors first (highest fit). Use names from Searchfunder, HoldCo Conference, Capital Camp, Stanford / HBS search-fund alumni lists, and your own network. **No cold LinkedIn prospecting yet.**

### Week 2 (May 23–29) — **Productize the template, generate 5 reference samples**

- Build SPEC_061 (report template) following the hand-assembled artifact from Week 1.
- Build SPEC_062 (named-private operators curator service) — query EPA/NPPES/USAspending by NAICS × geo.
- Generate 5 reference maps across **3 NAICS sectors** (1 industrial, 1 healthcare-services, 1 govcon-adjacent) and **3 geographies** (1 dense metro, 1 secondary metro, 1 regional). Sanitize and publish 1 as a public sample.
- Build SPEC_064 intake page + SPEC_063 orders API + SPEC_065 admin queue in parallel-ish (SPEC_063 has to land before SPEC_064 can submit).

### Week 3 (May 30 – Jun 5) — **Sell**

- Founder-led outreach: 30 ICPs receive a personalized note with the sample link. Each message names a sector × geo you think THEY'd want, not a generic pitch.
- Goal: 1 paid map by end of week. Stretch: 3 + 1 pilot conversation.
- Track every reply (positive / negative / silent) in the admin queue notes field. Use top objections to revise positioning every Friday.
- Manual delivery: every paid map produced in 24–48 hours, founder-curated.

### Week 4 (Jun 6 – Jun 12) — **Convert + decide**

- Goal: 1 happy customer → 1 paid pilot ($7,500). Or 3+ single maps.
- Day-30 gate:
  - **≥1 paid + ≥1 pilot in flight** → plan PLAN_066 (productize manual curation, build named-privates auto-curator, retainer infrastructure).
  - **0 paid, ≥3 substantive replies** → ICP / messaging tweak — but don't abandon.
  - **0 paid, 0 replies after 30 sends** → ICP or wedge re-evaluation.

### Outreach copy template

> Subject: Sector × market map of `<NAICS sector> × <MSA>` — for your `<DEAL THESIS>` work
>
> Hi `<NAME>`, I've built a public-data sector × geography intelligence pack
> for LMM industrial / healthcare / govcon search and sourcing work. It pulls
> Census, ACS, IRS SOI, SEC, FDIC, FRED, FEMA, EPA, FCC, BTS, and others
> into a single map of the operating landscape — structural density,
> demand, banking depth, regulatory risk, infrastructure proximity, federal
> $$ flow, and a curated list of named private operators where regulated-
> facility data permits.
>
> I built one for `<RELEVANT SECTOR × GEO>` as a sample — link below. If you
> have a sector you're sharpening a thesis on, I'll build a map for you for
> $2,500, payable on delivery, 48-hour turnaround.
>
> Sample: `<URL>`  ·  Order: `<STRIPE LINK>`
>
> Pilot option: 3 maps + a comparable view for $7,500.
>
> — Alex

---

## 4 · Implementation plan

Seven specs, numbered sequentially. Each independently testable.

### SPEC_060 — Pre-flight: NAICS / MSA / SIC taxonomies + Census CBP grain audit

**Files**
- `app/services/diligence/__init__.py` (new package)
- `app/services/diligence/taxonomies.py` — loads NAICS 2022 + MSA + NAICS↔SIC crosswalk from `data/reference/` JSON files
- `data/reference/naics_2022.json`, `data/reference/msa.json`, `data/reference/naics_sic_crosswalk.json` (sourced from Census + OMB — all open data)
- `scripts/audit_cbp_coverage.py` — one-off script: for each top-50 MSA × NAICS-4 in our target sectors, report `census_business_patterns` row count + completeness on cloud

**Tests** — `tests/test_spec_060_taxonomies.py`
- T1: NAICS lookup roundtrips (code → label → code) for all 4/5/6-digit codes
- T2: MSA dictionary includes all 384 official MSAs with constituent FIPS counties
- T3: NAICS↔SIC crosswalk produces ≥1 SIC for every NAICS-4 that has at least 1 SEC company in `sec_company_metadata`
- T4: invalid NAICS or MSA code → raises `ValueError` (not silently returns None)

**Acceptance**
- Taxonomy module loads in <100ms; lookups O(1).
- `audit_cbp_coverage.py` produces a CSV showing populated vs empty NAICS × MSA cells. If CBP is too sparse at NAICS-4 × MSA, that's the gate — we drop to NAICS-2/3 × state and adjust SPEC_064 intake UX before building it.

### SPEC_061 — Market Intelligence Pack report template

**Files**
- `app/reports/templates/market_intelligence_pack.py` (modeled on `pe_deal_memo.py`)
- `app/reports/builder.py` — one-line registration

**Inputs**
- `naics_code` (validated against SPEC_060 taxonomy; grain decided by SPEC_060 audit)
- `geography` (MSA code, or `{state}`, or `{county_fips_list}` — `geography_mode` selects)
- `geography_mode` ∈ `msa | state | multi_county`
- `client_note` (free text; appears in cover)

**Sections** (each skip-on-empty)
1. Cover + executive summary
2. Sector + geography definition (NAICS label, MSA / counties, population)
3. Structural density (Census CBP — establishment count + payroll + employment). Implementation note: `census_cbp` is county × NAICS-6; the template truncates NAICS-6 to NAICS-4 and SUMs across the buyer's MSA's constituent counties. Falls back to `census_business_patterns` (state grain, full NAICS depth) for MSAs with <50% county coverage — surface a "limited county-grain data for this MSA" callout when this happens.
4. Demand context (ACS B19013 + IRS SOI county / zip wealth)
5. Net-migration economy (IRS SOI migration $$ in/out)
6. Operating environment (FDIC regional banking depth + FRED macro + BLS labor)
7. Risk profile (FEMA declarations + NRI + flood + EPA enforcement intensity)
8. Infrastructure proximity (power + transmission + rail + airport + broadband)
9. Trade exposure (us_trade_exports_state + HS-code sector trade)
10. Federal $$ flow (USAspending awards by NAICS × geo)
11. Public-co operators visible in scope (SEC via NAICS↔SIC crosswalk)
12. Named private operators (best-effort) — output of SPEC_062
13. Diligence questions / what to look for next
14. Source appendix + provenance

**Tests** — `tests/test_spec_061_market_intelligence_pack.py`
- T1: every section method tolerates empty query results
- T2: section ordering deterministic across runs
- T3: provenance footer cites ≥5 distinct sources for a realistic input
- T4: registers correctly in `ReportBuilder.templates`
- T5: sample input (`naics=3323, msa=26420`) renders non-empty doc with all primary sections

**Acceptance** — manually run on 5 distinct (NAICS, geography) pairs across 3 sectors; visual quality at or above `pe_deal_memo`.

### SPEC_062 — Named-private operators curator service

**Files**
- `app/services/diligence/named_operators.py`

**Logic**
- NAICS in EPA-regulated industries (manufacturing / chemicals / oil-gas / utilities / waste) → `epa_echo_facilities`
- NAICS in healthcare → `nppes_providers` + `cms_hospitals`
- NAICS in federal-contractor-dense → `usaspending_awards`
- Other NAICS → empty + explanatory note flag

**Tests** — `tests/test_spec_062_named_operators.py`
- T1: industrial NAICS → ECHO-sourced operators
- T2: healthcare NAICS → NPPES + CMS-sourced operators
- T3: unregulated NAICS → empty + flag
- T4: geographic filter honored (returns only operators in the chosen geo)

**Acceptance** — 5 NAICS × MSA pairs (one per major sector category) return defensible lists (or honestly empty).

### SPEC_063 — Diligence orders model + intake API

**Files**
- `app/services/diligence/orders.py`
- `app/api/v1/diligence.py` (new public router, no auth required for intake)
- `app/core/config.py` — 3 new settings: `stripe_payment_link_url_2500`, `stripe_payment_link_url_7500`, `diligence_notify_email`

**Schema** (idempotent migration in `_ensure_tables`)

```sql
CREATE TABLE IF NOT EXISTS diligence_orders (
    id              SERIAL PRIMARY KEY,
    requested_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    contact_name    TEXT NOT NULL,
    contact_email   TEXT NOT NULL,
    contact_org     TEXT,
    naics_code      TEXT,
    msa_code        TEXT,
    geography_mode  TEXT,
    geography_note  TEXT,
    client_note     TEXT,
    sku             TEXT NOT NULL,  -- 'single_map' | 'pilot_3_maps' | 'retainer'
    status          TEXT NOT NULL DEFAULT 'requested',
    stripe_session_id TEXT,
    paid_at         TIMESTAMP,
    report_ids      TEXT[],
    delivered_at    TIMESTAMP,
    notes           TEXT,
    source          TEXT
);
CREATE INDEX IF NOT EXISTS diligence_orders_status_idx ON diligence_orders(status);
```

**Endpoints**
- `POST /api/v1/diligence/request` — public. Validates NAICS + MSA. Creates order. Returns the matching Stripe Payment Link URL. Fires best-effort email to `diligence_notify_email`.
- `GET /api/v1/diligence/skus` — public. Pricing rendered from config.
- `GET /api/v1/diligence/taxonomies` — public. Returns NAICS + MSA dicts for the intake UI.

**Tests** — `tests/test_spec_063_diligence_orders.py`
- T1: intake creates row with `status='requested'`
- T2: invalid SKU → 422
- T3: response `payment_url` matches configured Stripe link for that SKU
- T4: missing required fields → 422 with correct field names
- T5: notify-email failure doesn't fail the intake
- T6: invalid NAICS → 422
- T7: invalid MSA → 422

### SPEC_064 — `frontend/diligence.html` intake page

**Files**
- `frontend/diligence.html` — single self-contained file. Loads taxonomies from `/api/v1/diligence/taxonomies`. NAICS picker = 2-level select (sector → industry). MSA picker = searchable autocomplete.

**Sections**
1. Hero + positioning copy
2. "What you get" — section bullet list with example fragments
3. Live sample link (one of Week 2's reference maps)
4. Pricing block (live from `/skus`)
5. Intake form (NAICS picker, MSA picker, geography_mode toggle, client_note, contact_*, source dropdown)
6. Submit → success card with Stripe link + "what happens next" timeline

**Tests** — static page, no skeleton test file (SPEC_058 convention). E2e verification is the acceptance.

### SPEC_065 — Admin queue + tier gating

**Files**
- `app/api/v1/auth.py` — `require_admin` dependency
- `app/api/v1/diligence.py` — `/admin/orders/*` sub-routes (list, detail, patch)
- `frontend/diligence-admin.html` — kanban-by-status, filter bar, drill-in

**Tests** — `tests/test_spec_065_diligence_admin.py`
- T1: non-admin JWT → 403 on every admin endpoint
- T2: admin JWT → 200 + correct payload shape
- T3: status transitions enforced (`requested → scoped → paid → generating → delivered → closed`)

**Acceptance** — `UPDATE users SET tier='admin' WHERE email='alexiusmichael@gmail.com'`; admin sees full queue end-to-end.

### SPEC_066 — CTA rewire across existing playground reports

**Files**
- `app/core/config.py` — update defaults for `playground_cta_platform_url` and `playground_cta_run_url` to point at `/diligence.html` instead of `/playground.html#platform`.

**Tests** — update SPEC_059 assertions for the new default URLs.

**Acceptance** — newly-generated playground reports route CTA to the diligence intake page. Old shared reports keep working (no re-rendering of saved HTML).

---

## 5 · Out-of-scope (explicit deferrals)

- Stripe webhook / auto-paid-flip. PATCH manually until volume warrants.
- Auto-generate-on-paid. Founder-curated for v1.
- Yelp / Yellow Pages / LinkedIn / state SoS scraping for named privates. Regulated-facility data only.
- CRM, dashboards, multi-currency, contracts beyond Payment Links, multi-user collab.
- PE / family-office / org-chart depth.
- Programmatic API access to underlying data.

---

## 6 · Risks + mitigations

| Risk | Mitigation |
|---|---|
| Census CBP NAICS grain insufficient for our intake spec | SPEC_060 audit IS the gate; if CBP is too sparse at NAICS-4 × MSA, we fall back to NAICS-2/3 × state and adjust intake taxonomy accordingly |
| Named-privates section is thin for unregulated sectors | Report template makes this loud and honest; we steer ICP outreach toward sectors where the named list is dense (industrial, healthcare, govcon) for the first 30 days |
| 30-IPs outreach lands flat | Tighten ICP cut to search funders only (highest fit); rerun the sample for one of their named sectors |
| Founder bottleneck on manual delivery | Template + named-operators auto-query do most of the heavy lifting; manual = the cover page + executive summary + Section 13 (diligence questions). ~3 hours per map by report 5. |
| Buyer wants names of private operators we can't supply | Quote the named-operators add-on as a separate $1,500 deep-curation SKU (Week-3+ if requested) |
| Cloud DB connectivity outage during demo | Pre-render samples; demo from cached HTML, not live generation |

---

## 7 · Commits expected

One commit per spec (6 total). Plan doc itself committed separately so the
strategy step is reviewable independent of the code.

---

## 8 · What success looks like at day 30

- **Minimum:** 1 published sample map, intake page live, admin queue working, 30 named ICP outreaches sent, ≥3 substantive replies. 0 paid is acceptable only with a clear next-step plan.
- **Target:** 1–3 paid single maps ($2.5K–$7.5K booked).
- **Stretch:** 1 paid pilot ($7,500) + 1 retainer conversation in late-stage.

Day-30 outcome decides PLAN_066: "productize manual curation + scale outreach" vs "re-evaluate wedge."

---

## Revisions

### rev_01 — 2026-05-20 — Pivot to Nexdata Atlas (report-first monetization rejected)

The report-first commercial framing of this plan was rejected by the user.
The engineering (SPEC_061 report template, SPEC_062 orders API, SPEC_063
intake page) is **kept and repositioned** — the report becomes an
export/deep-dive renderer, the orders API becomes a concierge fallback, and
the `$2,500 / $7,500` pricing leaves all first-touch surfaces. The new
primary product is **Nexdata Atlas** — an interactive public-data
exploration engine (query → insight cards → connections → provenance →
share/fork → telemetry).

See **`docs/plans/PLAN_065_industrial_diligence_pack_rev_01.md`** for the
full What Was Wrong / What Was Fixed / Lessons Learned, and
**`docs/specs/SPEC_064_atlas_data_explorer.md`** for the Atlas build spec.

This plan (rev_00, everything above this Revisions section) is retained as
historical record. It is **no longer the active commercial plan.**
