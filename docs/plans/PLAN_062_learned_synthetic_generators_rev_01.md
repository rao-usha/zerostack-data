# PLAN_062 — Revision 01: Data Prerequisites Are Not Met

**Date:** 2026-05-11
**Triggered by:** Prerequisite verification queries against the live database (2026-05-10 session)

---

## Revision 01

Insert a new **Week 1: Data Prerequisites** into PLAN_062, shifting all subsequent weeks by one. Total plan length becomes 5 weeks instead of 4. Architecture, methods, acceptance criteria, and runtime contract unchanged.

The new Week 1 ships two workstreams in parallel: (W1.A) SEC EDGAR XBRL bulk ingest to populate the training data for A1, and (W1.B) FRED expansion + 50-year backfill to populate the training data for A2. A third workstream (W1.C) migrates the schemas the v1 generators reference so they finally run on real fitted distributions instead of hardcoded sector priors.

---

## What Was Wrong

PLAN_062 (v0) assumed the following prerequisites were met. Verification queries against the live DB show none of them are:

### Finding 1 — `public_company_financials` doesn't exist
The v1 generator `app/services/synthetic/private_company_financials.py` queries `FROM public_company_financials` at line 244. That table does not exist in the database. The v1 generator silently catches the failure, returns `peer_count: 0`, and falls back to hardcoded `SECTOR_PRIORS`. **The v1 generator has never fitted a real distribution. The "v1 fallback" we planned to keep is sector-prior random sampling, not the Gaussian-copula-on-peers we documented.**

### Finding 2 — empty SEC tables exist instead
`sec_income_statement`, `sec_balance_sheet`, `sec_cash_flow_statement`, `sec_financial_facts`, `pe_company_financials` — all five exist with **0 rows**. PLAN_053 Phase C1 specced "SEC EDGAR XBRL bulk ingest" as a future workstream but it was never shipped. The empty tables are leftover schema scaffolding from PLAN_053.

### Finding 3 — `fred_observations` doesn't exist
The v1 generator `macro_scenarios.py` queries `FROM fred_observations` at lines 121 and 146. That table does not exist either. The only FRED table is `fred_interest_rates`. v1 macro generator returns `training_history_months: 0` and runs entirely on default O-U fallback parameters.

### Finding 4 — FRED coverage is 5-of-8 short and 50 years too shallow
`fred_interest_rates` contains 7 interest-rate series only: DFF, DGS10, DGS2, DGS30, DGS3MO, DGS5, DPRIME. The 5 macro series PLAN_062 needs (UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO) are absent entirely. Coverage on the 3 series we share (DFF, DGS10, DGS2) is daily-frequency from 2016-04 to 2026-05 (~10 years), not the 60-year monthly history the plan assumed.

### Finding 5 — only FDIC bank financials are populated
`fdic_bank_financials` has 1.67M rows of real data. This is out of scope for the "general private company TabDDPM training" goal but useful to know — it would support a sector-specific (banking) TabDDPM if we ever wanted that, and is a reusable data asset for the Underwrite peril SKU.

---

## What Was Fixed

A new Week 1 inserted ahead of the original training week. Subsequent weeks renumbered. Total plan: 5 weeks.

### Week 1 — Data Prerequisites (NEW)

Three concurrent workstreams. Estimated 5–7 working days.

**W1.A — SEC EDGAR XBRL bulk ingest (2 days)**
- Targets: S&P 500 + Russell 2000 ≈ 2,500 companies × 5–10 years annual filings = 12,500–25,000 company-years
- XBRL concept tags to extract per company-year: `us-gaap:Revenues`, `us-gaap:CostOfRevenue`, `us-gaap:GrossProfit`, `us-gaap:OperatingIncomeLoss`, `us-gaap:NetIncomeLoss`, `us-gaap:Assets`, `us-gaap:Liabilities`, `us-gaap:StockholdersEquity`, `dei:EntityCommonStockSharesOutstanding`, plus NAICS code from EDGAR submission metadata
- Use existing `app/sources/sec/` Company Facts API client (EDGAR rate limit 10 req/sec — already enforced)
- Bottleneck: XBRL parsing, not network. Sequential per-company processing acceptable
- Output: populate `public_company_financials` (creating it — see W1.C) directly with the columns v1 generator expects (`revenue_usd`, `gross_profit_usd`, `ebitda_usd`, `net_income_usd`, `fiscal_period`, `period_end_date`, `company_name`)
- Acceptance: ≥12,500 rows across ≥1,500 distinct CIKs; ≥5 years of history; non-null fields on ≥80% of rows

**W1.B — FRED expansion + 50-year backfill (2 days)**
- Add 5 new series to ingest config: UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO
- Existing `app/sources/fred/` client supports any series_id — config-only addition
- Backfill all 12 series (5 new + 7 existing) to 1965 or as far back as FRED returns each (some series start later: UMCSENT starts 1952, DCOILWTICO starts 1986)
- Aggregate daily values to monthly (mean of daily values within month, or end-of-month spot value depending on series semantics — use mean for rates, end-of-month for indices)
- Acceptance: 12 series in `fred_observations`; ≥720 monthly observations for series that historically allow it (DFF, UNRATE, CPIAUCSL); MIN observation_date ≤ 1985 for 8 of 12 series

**W1.C — Schema migration + v1 generator validation (1 day)**
- Create `public_company_financials` table matching v1 generator's expected schema:
  ```sql
  CREATE TABLE public_company_financials (
      cik VARCHAR(10) NOT NULL,
      company_name TEXT NOT NULL,
      fiscal_period VARCHAR(4) NOT NULL,  -- 'FY', 'Q1', 'Q2', 'Q3', 'Q4'
      period_end_date DATE NOT NULL,
      revenue_usd NUMERIC,
      gross_profit_usd NUMERIC,
      ebitda_usd NUMERIC,
      net_income_usd NUMERIC,
      total_assets_usd NUMERIC,
      total_liabilities_usd NUMERIC,
      total_equity_usd NUMERIC,
      naics_code VARCHAR(8),
      state_fips VARCHAR(2),
      employee_count INTEGER,
      ingestion_job_id INTEGER REFERENCES ingestion_jobs(id),
      PRIMARY KEY (cik, fiscal_period, period_end_date)
  );
  ```
- Create `fred_observations` table (generalized from `fred_interest_rates`):
  ```sql
  CREATE TABLE fred_observations (
      series_id TEXT NOT NULL,
      date DATE NOT NULL,
      value NUMERIC,
      realtime_start DATE,
      realtime_end DATE,
      ingested_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (series_id, date)
  );
  -- Copy existing fred_interest_rates rows into fred_observations
  -- Keep fred_interest_rates as a backward-compat view for existing scorers
  ```
- Decision: keep `fred_interest_rates` as a VIEW over `fred_observations` so existing scorers that reference it don't break.
- Verify v1 generators against real data:
  - `curl /api/v1/synthetic/private-financials -d '{"sector":"industrials","n":5}'` should return `peer_count > 0` (not 0)
  - `curl /api/v1/synthetic/macro-scenarios -d '{"series":["DFF","UNRATE"]}'` should return `training_history_months > 60`
- Acceptance: both v1 generators report `methodology` matching peer-fitted / FRED-calibrated state, not fallback mode

### Renumbered weeks 2-5

| Old | New | Workstream |
|---|---|---|
| Week 1 | **Week 2** | Scaffolding: vendor TabDDPM, training infra, deps, smoke tests |
| Week 2 | **Week 3** | Train both models concurrent on the 5070 |
| Week 3 | **Week 4** | Validation extensions + integration |
| Week 4 | **Week 5** | Downstream wiring + demo |

All other content (architecture, methods, acceptance criteria, stop-and-checkpoint triggers, references) unchanged.

---

## Lessons Learned

1. **Verify table existence before writing a plan that depends on the data.** The original PLAN_062 was written from research-doc assumptions (PLAN_052 mentioned `public_company_financials`; PLAN_053 specced its ingest) without checking that the ingest had actually shipped. A 5-minute `SELECT COUNT(*) FROM public_company_financials;` query at plan-draft time would have surfaced this. Add to default plan-writing checklist for any task that depends on database state: run row-count queries against every table the plan names.

2. **Silent fallbacks mask broken state.** Both v1 generators catch the missing-table exception and fall back gracefully. The endpoints return valid-looking JSON. The `methodology` and provenance fields claim "gaussian_copula_from_peers" and "mean_reverting_correlated_random_walk" even when `peer_count: 0` and `training_history_months: 0`. **The methodology field should report the actual mode used, not the intended mode.** This is a small but real correctness bug in the v1 generators — fix opportunistically during W1.C while we're touching that code.

3. **PLAN_053 left scaffolding without ingest.** The empty `sec_income_statement` and friends are leftover schema from a plan whose Phase C never shipped. When a plan specs scaffolding + ingest as separate phases, **check the actual git commits for the ingest phase, not just the planning doc.** PLAN_053's status note doesn't make clear which phases shipped.

4. **"Reuse existing infrastructure" claims need verification.** PLAN_062 said "use existing `app/sources/sec/` Company Facts API." That client does exist and works — but no one ever scheduled the bulk ingest job to populate the financial tables. There's a difference between "ingestor code exists" and "data has been ingested."

5. **Verification before committing autonomous-mode budget.** The "build until done" autonomy model in PLAN_062 had stop-and-checkpoint triggers including "prereq blocker → pause." Good triggers won't save us from wasted GPU time if we hit them in the middle of training rather than at the start. Run all prereq checks before declaring "go" on any autonomous run.

---

## Memory + workflow updates

These should propagate to future plans, not just PLAN_062:

1. **Plan-writing checklist addition:** any plan that depends on database state must verify table existence and population (row count) before the plan is approved. Add to `/plan-task` skill or equivalent.

2. **Memory entry — feedback:** "Verify table existence before writing data-dependent plans" rule with this incident as the *why*.

3. **v1 generator opportunistic fix (W1.C):** the `methodology` field in synthetic generator responses should report actual mode, not intended mode. File-level TODO acceptable; not a separate plan.

---

## What this revision does NOT change

- Method choice (TabDDPM for A1, Diffusion-TS for A2)
- Acceptance criteria (KS p > 0.05 in 8/10 sectors; Frobenius < 0.15 tabular / < 0.10 time series; kurtosis ±0.3; ACF lag-12 ±0.05; discriminative score < 0.60; ML utility within 5%)
- GPU plan (RTX 5070 concurrent training, ~24hr wall clock for both models)
- Runtime contract (`LearnedSyntheticGenerator` ABC, model singletons, provenance carry-through)
- v1 fallback strategy (`algorithm="parametric"` retained after W1.C makes v1 actually fit on data for the first time)
- Stop-and-checkpoint triggers

---

## Pointer

Updated main plan: [PLAN_062_learned_synthetic_generators.md](PLAN_062_learned_synthetic_generators.md) (new Week 1 inserted; subsequent weeks renumbered; Revisions section appended at bottom)

Research foundation unchanged: [LEARNED_SYNTHETIC_GENERATORS_RESEARCH.md](../strategy/LEARNED_SYNTHETIC_GENERATORS_RESEARCH.md)
