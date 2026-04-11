# PLAN_053 — Synthetic Seed, Claims Intelligence & Source Ingestion Strategy

## Problem

All 8 signal chains are architecturally complete, but 3 scorers are at **0% confidence** because their input tables are empty. The platform has 47 registered API sources — the bottleneck isn't missing sources, it's that we haven't systematically ingested them or generated synthetic fills for domains where real public data doesn't exist.

Additionally, **claims data** — the transactional record of violations, complaints, enforcement actions, and healthcare utilization — is the most statistically robust signal available for PE diligence. We already have 6 claims sources implemented but most are un-ingested or unwired to scorers.

### Scorer Confidence (Current State)

| # | Scorer | Confidence | Bottleneck |
|---|--------|-----------|------------|
| 1 | Deal Environment | 70% | EIA/BEA/CFTC partially wired |
| 2 | Company Diligence | 0% | 7/8 source tables empty |
| 3 | GP Pipeline | 0% | LP fund universe = 0 rows |
| 4 | Exec Signal | 5% | Job postings = 0 rows |
| 5 | Unified Site | 85% | Good shape |
| 6 | Portfolio Stress | Demo-only | Macro scenarios not wired |
| 7 | Healthcare Practice | 60% | CMS claims/quality data unwired |
| 8 | LP-GP Graph | 0% | LP-GP relationships = 0 rows |

### Claims Data Inventory

We already have **6 claims sources implemented** — the gap is ingestion and scorer wiring, not new code:

| Source | Table | Data Type | Records | Wired to Scorer? |
|--------|-------|-----------|---------|-----------------|
| **OSHA** | `osha_inspections`, `osha_violations` | Workplace injury/violation claims | Needs ingest | Yes → Diligence (15%) |
| **EPA ECHO** | `epa_echo_facilities` | Environmental enforcement claims | ~1M facilities | Yes → Diligence (15%) |
| **CourtListener** | `courtlistener_dockets` | Bankruptcy/litigation claims | Needs ingest | Yes → Diligence (10%) |
| **CMS Drug Pricing** | `cms_drug_pricing` | Medicare Part D claims (spend, utilization) | Needs ingest | **No** |
| **CMS HCRIS** | `cms_hospital_cost_reports` | Hospital cost/utilization claims | Needs ingest | **No** |
| **FEMA NFIP** | `fema_nfip_claims` | Flood insurance claims by geography | Needs ingest | **No** |

**Missing but high-value:**

| Source | Type | PE Use Case | Public API? |
|--------|------|-------------|------------|
| **CFPB** | Consumer financial complaints | Fintech/lending/banking targets | Yes — 1M+ records, free |
| **CPSC** | Product safety recalls/complaints | Consumer goods, manufacturing | Yes — free API |
| **NHTSA** | Auto defect/recall claims | Auto suppliers, fleet operators | Yes — free API |

### Why NOT Kaggle

Researched pulling Kaggle datasets as seeds. Conclusion: **wrong approach.**
- M5 (only dataset integrated) is frozen 2011-2016, 3 Walmart stores — stale and non-representative
- Every domain Kaggle could cover (macro, financials, labor, real estate) is already covered by our existing 47 sources with current, authoritative, auditable data
- Kaggle adds integration debt (custom parsers, one-time snapshots, no refresh) for inferior data
- Keep existing Kaggle client infra, but don't expand it

---

## Phase 0 — Data Provenance System (Foundation — Do First)

All subsequent phases depend on this. Without provenance tracking, synthetic data silently pollutes real data and the UI can't distinguish them.

### Design Decision: Job-Level Provenance (Option C)

We tag provenance at the **ingestion job** level, not per-row. Every record already has a `job_id` FK back to `ingestion_jobs`. We add one column there.

**Why not per-row?** Adding `data_origin` to 60+ tables requires schema migration on every table, storage overhead on every row, and maintenance burden on every INSERT. Job-level gives us per-batch granularity at near-zero cost.

### 0.1 Schema Change

Add `data_origin` column to `ingestion_jobs`:

```sql
ALTER TABLE ingestion_jobs
ADD COLUMN data_origin VARCHAR(16) NOT NULL DEFAULT 'real'
CHECK (data_origin IN ('real', 'synthetic'));
```

- All existing jobs default to `'real'`
- Synthetic generators set `data_origin = 'synthetic'` when creating their job
- Seed scripts that create ingestion jobs also set `'synthetic'`

**Files:**
- MODIFY: `app/core/models.py` — add `data_origin` column to `IngestionJob` model
- MODIFY: `app/core/ingest_base.py` — accept `data_origin` param in job creation

### 0.2 Synthetic Source Registration

Register synthetic generators as proper sources in the source registry so they appear in the Data Sources view:

```python
# source_registry.py
"synthetic_job_postings": {
    "category": "synthetic",
    "display_name": "Synthetic Job Postings",
    "origin": "synthetic",
    ...
},
"synthetic_lp_gp": {
    "category": "synthetic",
    "display_name": "Synthetic LP-GP Universe",
    "origin": "synthetic",
    ...
},
```

Add `origin` field to every source registry entry:
- Existing sources (FRED, BLS, SEC, etc.) get `"origin": "real"`
- Synthetic generators get `"origin": "synthetic"`

**Files:**
- MODIFY: `app/core/source_registry.py` — add `origin` field to all entries + new synthetic source entries

### 0.3 Scorer Provenance Reporting

Every scorer response should include a provenance breakdown so users know what they're looking at:

```json
{
  "composite_score": 67,
  "grade": "B",
  "provenance": {
    "real_factors": 3,
    "synthetic_factors": 2,
    "total_factors": 5,
    "real_pct": 60,
    "detail": {
      "environmental_risk": "real",
      "safety_risk": "real",
      "growth_momentum": "synthetic",
      "legal_exposure": "real",
      "innovation_capacity": "synthetic"
    }
  }
}
```

**Approach:**
- Each scorer factor query joins back to `ingestion_jobs.data_origin` to determine provenance
- Aggregate into `provenance` object in response
- Helper function in a shared util so all 8 scorers use the same pattern

**Files:**
- NEW: `app/services/provenance.py` — shared helper: `get_data_origin(db, table_name, filter_col, filter_val) → "real" | "synthetic" | "mixed"`
- MODIFY: All 8 scorer services — add provenance to response

### 0.4 UI Provenance Indicators

The frontend must visually distinguish real from synthetic data:

- **Sources view:** Synthetic sources get a distinct badge/tag (e.g., "SYNTHETIC" pill in a different color). They appear alongside real sources but are clearly marked.
- **Scorer output:** Show the provenance breakdown — e.g., "3/5 factors from real data" with a visual indicator (progress bar, pie, or simple label).
- **Data tables/exports:** Any table view or CSV export should indicate whether the underlying data is real or synthetic (badge on table header, column in export).
- **Filter toggle:** "Show real only / synthetic only / all" filter on Sources view and data tables.

**Files:**
- MODIFY: Frontend source dashboard (when applicable)
- API already provides provenance in scorer responses + source registry `origin` field

---

## Phase A — Unblock Dead Scorers (Synthetic Generation)

These domains have no public data source — synthetic generation is the only option.

**Important:** All synthetic generators MUST create their ingestion jobs with `data_origin='synthetic'`. This is enforced by Phase 0.

### A1. Synthetic Job Postings Generator

**Unblocks:** Chain 4 (Exec Signal), Chain 2 (Company Diligence growth factor)

**What:** Generate realistic job postings for the ~70 seeded industrial companies + 100 PE portfolio companies.

**Approach:**
- Sector-aware role distributions (tech = 40% engineering, healthcare = 30% clinical, etc.)
- Seniority mix: ~5% C-suite/VP, ~15% Director, ~30% Manager, ~50% Individual Contributor
- Title generation from sector × seniority templates (e.g., "VP of Operations", "Senior Data Engineer")
- Posted date distribution: 70% last 30 days, 20% 30-60 days, 10% 60-90 days
- Company size → headcount → open role ratio (typically 5-15% of headcount)
- Location drawn from company HQ state + random secondary offices

**Output:** ~10,000 synthetic job postings across 120+ companies

**Target table:** `job_postings` (already exists — used by exec_signal_scorer)

**Files:**
- NEW: `app/services/synthetic/job_postings.py`
- NEW: `app/api/v1/synthetic.py` (add endpoint, or extend existing)
- NEW: `tests/test_synthetic_job_postings.py`

---

### A2. Synthetic LP-GP Universe Generator

**Unblocks:** Chain 3 (GP Pipeline), Chain 8 (LP-GP Graph)

**What:** Generate a realistic LP fund universe and LP→GP commitment relationships.

**Approach:**
- LP types: public pension (40%), endowment (20%), insurance (15%), sovereign wealth (10%), family office (10%), fund-of-funds (5%)
- LP AUM: log-normal by type (pensions $50-500B, endowments $5-50B, FOs $1-10B)
- LP names: Real institution names from public Form 990 / CAFR data (CalPERS, Harvard Endowment, etc.)
- GP relationships: Power-law distribution — mega-GPs (Blackstone, Apollo, KKR) get 15-25 LPs, mid-market GPs get 3-8
- Commitment sizing: 1-5% of LP AUM per commitment, larger % for smaller LPs
- Vintage years: 2015-2025, with re-up probability based on fund performance quartile
- Fund strategy tags: buyout, growth, venture, credit, real estate, infrastructure

**Output:** ~500 LP funds, ~1,000 LP-GP relationships, ~200 fund commitments

**Target tables:** `lp_fund`, `lp_gp_relationship`, `pe_fund` (extend existing)

**Files:**
- NEW: `app/services/synthetic/lp_gp_universe.py`
- NEW: `tests/test_synthetic_lp_gp.py`

---

### A3. Wire Macro Scenarios → Portfolio Stress Scorer

**Unblocks:** Chain 6 (Portfolio Stress) — currently uses live FRED only, no scenario projection

**What:** Connect the macro scenario generator (PLAN_052) to portfolio_stress_scorer so stress scores can be projected forward under different macro regimes.

**Approach:**
- Add `scenario_id` optional param to stress scorer
- If provided, pull macro inputs from synthetic scenario instead of live FRED
- Return stress scores under base, adverse, and severe scenarios
- Add `/pe/stress/{firm_id}/scenarios` endpoint

**Files:**
- MODIFY: `app/services/portfolio_stress_scorer.py`
- MODIFY: `app/api/v1/pe_benchmarks.py`

---

## Phase B — Claims Intelligence Layer

Claims data is the most statistically robust diligence signal — high N, mandatory reporting, predictive of costs and regulatory outcomes. This phase ingests existing claims sources and wires them into scorers.

### B1. OSHA + EPA ECHO + CourtListener Ingestion (Company Diligence)

**Feeds:** Chain 2 (Company Diligence) — currently 0% on Safety, Environmental, and Legal factors

**What:** Trigger existing ingestion pipelines for 100+ target companies. All three sources are already implemented with clients + ingestors.

**Approach:**
- **OSHA** (`app/sources/osha/`): Bulk CSV ingest of inspections + violations. ~13M inspection records nationally. Scorer queries penalty totals, serious violation counts.
- **EPA ECHO** (`app/sources/epa_echo/`): Facility search by company name → violations, penalties, compliance history. ~1M facilities already in table but may need refresh/expansion.
- **CourtListener** (`app/sources/courtlistener/`): Currently bankruptcy-only. Expand query to include civil litigation (product liability, employment, IP disputes) for richer Legal Exposure signal.
- **USPTO** (`app/sources/uspto/`): Patent search by assignee → patent counts, tech classes. Innovation signal for diligence.

**Statistical significance:** OSHA inspections are mandatory for workplaces with 10+ employees. EPA enforcement is comprehensive for permitted facilities. Both have large N and consistent reporting — not sample-based.

**Target confidence lift:** Company Diligence 0% → 40-50%

---

### B2. CMS Medicare Claims Intelligence (Healthcare Scorer)

**Feeds:** Chain 7 (Healthcare Practice) — strongest unexploited claims asset

**What:** Wire CMS claims data into healthcare_practice_scorer to enable provider-level comparison of acquisition targets.

**Why this matters for PE:** Healthcare roll-ups are the #1 PE playbook right now. CMS publishes provider-level data with millions of claims aggregated — it directly answers "is this practice efficient, high-volume, and well-reimbursed?"

**Approach:**

1. **Ingest CMS Provider Utilization** (existing `app/sources/cms/`):
   - Medicare Part B claims by provider (NPI): services rendered, beneficiaries, total charges, Medicare payments
   - Enables: cost-per-claim benchmarking, volume ranking, payer efficiency
   - ~10M rows nationally, filterable by provider type + geography

2. **Ingest CMS Quality Scores** (existing or extend CMS source):
   - Hospital Compare: star ratings, readmission rates, patient experience (HCAHPS)
   - Physician Compare: quality measures, group practice scores
   - Enables: quality-adjusted valuation (high-quality practices command premium multiples)

3. **Ingest CMS Drug Spending** (already in `cms_drug_pricing` table):
   - Wire Part D claims (total claims, spend per beneficiary, brand vs generic mix) into healthcare scorer
   - Enables: pharmaceutical cost exposure assessment for practices

4. **Wire into healthcare_practice_scorer:**
   - New factor: **Claims Efficiency** (cost per claim vs. specialty median)
   - New factor: **Volume & Scale** (total claims / beneficiaries as market share proxy)
   - New factor: **Quality Score** (CMS star rating, readmission rate)
   - Adjust existing factor weights to accommodate claims-based factors

**Statistical significance:** CMS claims are the gold standard — mandatory reporting for all Medicare providers, millions of records, audited by OIG. This is not survey data; it's the actual transaction record.

**Target confidence lift:** Healthcare Practice 60% → 80-85%

---

### B3. FEMA Flood Claims → Site Scorer

**Feeds:** Chain 5 (Unified Site) — adds empirical loss data to flood risk assessment

**What:** Wire `fema_nfip_claims` into unified_site_scorer's Climate Risk factor.

**Approach:**
- Aggregate claims by ZIP/census tract: total claims count, average payout, loss ratio
- Areas with high historical claim density are empirically riskier than FEMA flood zone designation alone
- Add as a sub-factor under Climate Risk (currently uses NRI + flood zones + seismic)

**Statistical significance:** 2.5M+ NFIP claims since 1978. Geographic concentration makes this a strong spatial signal.

**Target confidence lift:** Unified Site 85% → 88% (marginal but adds empirical ground truth)

---

### B4. CFPB Consumer Complaints (New Source — High-Value Addition)

**Feeds:** Chain 2 (Company Diligence) — new factor for financial services targets

**What:** Add CFPB as a new data source. Free public API, 1M+ complaint records with company-level attribution.

**Approach:**
- NEW: `app/sources/cfpb/client.py` — CFPB Complaint Database API (REST, no auth required)
- NEW: `app/sources/cfpb/ingest.py` — ingest complaints by company name
- NEW: `app/sources/cfpb/metadata.py` — schema: complaint ID, company, product, issue, resolution, dates
- Table: `cfpb_complaints` — PK on complaint_id
- Wire into company_diligence_scorer as optional **Consumer Risk** factor:
  - Complaint volume per $1B revenue (normalized)
  - Resolution rate (% closed with relief vs. closed without)
  - Timely response rate
  - Trending: complaint velocity (increasing = deteriorating)

**PE relevance:** Fintech, specialty lending, insurance, banking, debt collection, mortgage servicers. Complaint spikes are leading indicators of enforcement actions (CFPB fines, consent orders).

**Statistical significance:** 1M+ complaints, company-attributed, with structured product/issue taxonomy. Resolution outcomes tracked. High N for any company with >$1B consumer exposure.

---

## Phase C — Real Data Calibration

### C1. SEC EDGAR XBRL Bulk Ingest

**Calibrates:** `private_company_financials` synthetic generator (replaces hardcoded sector priors)

**What:** Ingest 5,000+ company-years of financial data from SEC Company Facts API.

**Approach:**
- Target: S&P 500 + Russell 2000 companies (broad sector coverage)
- Fields: Revenue, EBITDA, Net Income, Total Assets, Total Debt, Employees
- Years: 2020-2025 (5 years × 1,000 companies = 5,000 records)
- Use existing `app/sources/sec/` client — already supports Company Facts API

**Target table:** `public_company_financials`

---

### C2. FRED Historical Backfill

**Calibrates:** Macro scenario generator (better mean-reversion parameters)

**What:** Ensure 10+ years of history for all 8 macro series used by `macro_scenarios.py`.

**Series:** DFF, DGS10, DGS2, UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO

**Approach:**
- Check current row counts per series in `fred_observations`
- Trigger FRED ingest for any series with < 10 years of monthly data
- Existing FRED client handles this — just need to call with correct series IDs

---

## Phase D — Expand Demo Coverage

### D1. Expand PE Demo Seeder

**What:** Scale from 3 firms / 48 portfolio companies to all 100 seeded PE firms / 500 companies.

**Approach:**
- Extend `app/sources/pe/demo_seeder.py` with parameterized company generation
- Use `private_company_financials` generator (now calibrated from Phase C) for realistic P&L
- Assign companies to PE firms based on sector + fund strategy alignment

---

### D2. E2E Integration Tests

**What:** Verify each scorer produces meaningful output with seeded/synthetic + claims data.

**Approach:**
- For each of the 8 chains: seed data → call scorer → assert non-zero confidence + valid score range
- Test claims-based factors: verify OSHA/EPA/CMS/CFPB data flows through to scorer output
- Test synthetic generators: verify output distributions match sector priors
- Test scenario wiring: stress scorer with synthetic macro scenario vs live FRED → both produce valid output

---

## Execution Order

```
Phase 0 (Day 1-2) — Data provenance foundation
  0.1 Add data_origin to ingestion_jobs    → schema change, one column
  0.2 Register synthetic sources           → source registry entries
  0.3 Scorer provenance reporting          → provenance helper + wire to scorers
  0.4 UI provenance indicators             → badges, filters, breakdown display

Phase A (Week 1) — Unblock dead scorers with synthetic data
  A1. Synthetic job postings           → exec_signal 5% → ~60%    [origin: synthetic]
  A2. Synthetic LP-GP universe         → gp_pipeline 0% → ~70%    [origin: synthetic]
  A3. Wire macro → stress scorer       → portfolio_stress → scenario-capable

Phase B (Week 2-3) — Claims intelligence layer
  B1. OSHA + EPA + CourtListener ingest → company_diligence 0% → ~40%  [origin: real]
  B2. CMS Medicare claims → healthcare  → healthcare_practice 60% → ~85% [origin: real]
  B3. FEMA flood claims → site scorer   → unified_site 85% → ~88%  [origin: real]
  B4. CFPB new source (if fintech/     → company_diligence adds consumer  [origin: real]
      lending targets in scope)           risk factor

Phase C (Week 3-4) — Real data calibration
  C1. SEC EDGAR XBRL bulk ingest       → synthetic financials calibrated  [origin: real]
  C2. FRED historical backfill         → macro scenarios calibrated       [origin: real]

Phase D (Week 4-5) — Demo expansion + verification
  D1. Expand PE demo seeder            → 48 → 500 portfolio companies    [origin: synthetic]
  D2. E2E integration tests            → all 8 chains verified, provenance correct
```

## Expected Outcome

| Scorer | Before | After | Key Claims Signal Added |
|--------|--------|-------|------------------------|
| Deal Environment | 70% | 80% | — |
| Company Diligence | 0% | 50-60% | OSHA violations, EPA penalties, CourtListener litigation, CFPB complaints |
| GP Pipeline | 0% | 70% | — (synthetic LP-GP) |
| Exec Signal | 5% | 60% | — (synthetic job postings) |
| Unified Site | 85% | 88% | FEMA NFIP historical loss claims |
| Portfolio Stress | Demo-only | Scenario-capable | — (macro scenario wiring) |
| Healthcare Practice | 60% | 85% | CMS claims efficiency, volume, quality scores |
| LP-GP Graph | 0% | 70% | — (synthetic LP-GP) |

## Claims Data Statistical Properties

Why claims data is the strongest diligence signal:

| Source | Records (National) | Reporting | Predictive Of |
|--------|-------------------|-----------|---------------|
| **OSHA** | ~13M inspections | Mandatory (10+ employees) | Insurance costs, labor relations, operational quality |
| **EPA ECHO** | ~1M facilities | Mandatory (permitted) | Remediation liability, regulatory risk, ESG exposure |
| **CourtListener** | ~100M dockets | Public record | Financial distress (Ch 11), management quality, legal exposure |
| **CMS Medicare** | ~1B claims/yr | Mandatory (all providers) | Practice efficiency, volume, quality, reimbursement risk |
| **FEMA NFIP** | ~2.5M claims | Mandatory (insured) | Geographic flood loss frequency, severity |
| **CFPB** | ~1M complaints | Voluntary (consumer-filed) | Regulatory enforcement risk, consumer satisfaction |

Key advantage: Claims data is **not survey-based or sampled** — it's the actual transaction/event record. Large N means you can make statistically significant comparisons between companies, geographies, and time periods.

---

## How to Think About Synthetic Data in Nexdata

### The Mental Model

Nexdata has two kinds of data, and you should always know which you're looking at:

| | Real Data | Synthetic Data |
|-|-----------|---------------|
| **Source** | Government APIs (FRED, SEC, BLS, EPA, OSHA, CMS, etc.) | Our own generators (job postings, LP-GP universe, macro scenarios, private financials) |
| **Purpose** | Ground truth — actual filings, inspections, claims, economic series | Fill gaps where no public data exists (private company internals, LP commitments) OR demo/stress-test the platform |
| **Trust level** | High — auditable, mandatory reporting, large N | Directional — realistic distributions but fabricated records |
| **Badge** | No badge (default) | Purple "SYNTHETIC" badge |
| **When to use** | Always preferred. Real data is the goal. | When real data doesn't exist for that domain, or for scenario analysis |
| **When to replace** | Never — real data is never overwritten by synthetic | When real data becomes available for that domain, phase out synthetic |

### The Key Principle

**Synthetic data is scaffolding, not foundation.** It exists to make the platform functional while we systematically ingest real data from our 47+ registered sources. As real data flows in (OSHA inspections, CMS claims, SEC filings), it should progressively replace synthetic fills. The provenance system ensures you always know what's what.

### User Clickstream: Navigating Synthetic Data

#### 1. Sources View → See what's real vs synthetic

```
Open index.html → Sources tab (defaults to Explore view)
```

- All 51 sources are listed by category
- **4 synthetic sources** show a purple "SYNTHETIC" badge:
  - Synthetic Macro Scenarios (macro_economic)
  - Synthetic Private Company Financials (pe_intel)
  - Synthetic Job Postings (alt_data)
  - Synthetic LP-GP Universe (pe_intel)
- Use the **Origin filter** (top-right): `[All] [Real] [Synthetic]`
  - "Real" → shows only the 47 government/public API sources
  - "Synthetic" → shows only the 4 synthetic generators
  - "All" → everything

#### 2. Generate Synthetic Data → Seed the platform

```
POST /api/v1/synthetic/job-postings      → 16K job postings for 200 companies
POST /api/v1/synthetic/lp-gp-universe    → 500 LPs + 4K+ GP relationships
POST /api/v1/synthetic/macro-scenarios   → N correlated macro paths
POST /api/v1/synthetic/private-financials → sector-aware company P&Ls
```

- Each generator creates an ingestion job tagged `data_origin='synthetic'`
- Data appears in the same tables as real data (e.g., `job_postings`, `lp_fund`)
- But the ingestion job tracks provenance — you can always trace back

#### 3. Scorer Output → Understand what's driving the score

```
Signal Chains Dashboard → pick a scorer panel
```

- **Company Diligence** → "Sources Matched" shows which sources had data + provenance bar (green = real, purple = synthetic)
- **GP Pipeline** → detail panel shows "SYNTHETIC DATA" badge (LP-GP data is synthetic)
- **Exec Signal** → leaderboard header shows "SYNTHETIC DATA" badge (job postings are synthetic)
- **Portfolio Stress** → scenario endpoint lets you override macro values with synthetic scenarios

**Rule of thumb:** If a scorer shows a mix of real + synthetic, the real factors are the trustworthy ones. Synthetic factors show the platform's capability but should be validated against real data before making decisions.

#### 4. Scenario Analysis → Stress test with synthetic macro overrides

```
POST /api/v1/pe/stress/{firm_id}/scenarios
Body: { "fed_funds_rate": 7.0, "cpi_yoy_pct": 6.0, "energy_cost_yoy_pct": 25.0 }
```

- Override live FRED macro values with "what-if" scenarios
- Response shows `scenario_applied` so you know what was overridden
- Compare base case (live data) vs adverse scenario to quantify portfolio sensitivity

#### 5. Provenance Audit → Verify what's in the database

```
Query: SELECT data_origin, COUNT(*) FROM ingestion_jobs GROUP BY data_origin
```

- Shows exact count of real vs synthetic ingestion jobs
- Every synthetic record traces back to a job with `data_origin='synthetic'`
- Source registry API (`GET /api/v1/sources`) returns `origin` field per source

### Progressive Replacement Path

As real data is ingested, synthetic data becomes less necessary:

```
Phase A (NOW)     — Synthetic fills: job postings, LP-GP, macro scenarios
Phase B (Next)    — Real claims data: OSHA, EPA, CMS Medicare, CFPB
Phase C           — Real calibration: SEC EDGAR XBRL, FRED backfill
Phase D           — Expanded demo: 500 real-ish portfolio companies

End state: Synthetic data used ONLY for:
  1. Scenario analysis (what-if macro projections)
  2. Private company financials (no public source exists)
  3. Demo/training environments
```

When real job postings arrive (from ATS integrations), the synthetic ones can be purged:
```sql
DELETE FROM job_postings WHERE ats_type = 'synthetic';
-- The ingestion job remains for audit trail
```

---

## Verification

### Provenance
- Query `ingestion_jobs` → confirm all synthetic jobs have `data_origin = 'synthetic'`, all real ingest jobs have `'real'`
- Call each scorer → confirm response includes `provenance` object with correct real/synthetic breakdown
- Source registry API → confirm synthetic sources appear with `origin: "synthetic"` badge
- UI: synthetic sources visually distinct in Sources view; scorer output shows provenance breakdown

### Scorer Confidence
- Run company_diligence_scorer on 5+ companies → confirm OSHA/EPA/legal factors return non-null
- Run healthcare_practice_scorer on 5+ providers → confirm CMS claims factors populated
- Run unified_site_scorer on 3+ locations → confirm FEMA claims sub-factor present
- Run exec_signal_scorer → confirm job posting factors populated (provenance: synthetic)
- Run gp_pipeline_scorer → confirm LP-GP data populated (provenance: synthetic)

### Data Integrity
- `curl http://localhost:8001/api/v1/data-coverage` → verify record counts per claims table
- Run synthetic generators → verify output row counts match spec
- `pytest tests/ -v` → all new + existing tests pass
- Spot check: compare scorer output with vs without claims data — claims should meaningfully differentiate companies that look similar on financial metrics alone
- Verify no synthetic data leaks into "real only" filtered views
