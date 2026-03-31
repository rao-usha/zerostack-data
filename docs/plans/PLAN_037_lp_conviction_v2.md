# PLAN 037 — LP Conviction 2.0

**Date:** 2026-03-27
**Status:** Draft — awaiting approval
**Goal:** Make LP conviction scoring actually work with real data. Fix broken collectors, expand coverage, complete the pipeline from collection → normalization → conviction scoring.

---

## Problem Statement

PLAN_034 built the full LP conviction infrastructure (tables, scorer, API, collectors) but real data collection returns 0 records:

- All 5 pension IR URLs → 404 (portals restructured)
- Form 990 Schedule D parser → 0 records (assumes line-by-line text, actual filings use HTML tables)
- Form D collector → not wired into the agent at all
- CAFR parser → never called for public pensions (the best data source is completely unused)
- Conviction scorer → never called after collection (FundLPTrackerAgent.run() doesn't call it)
- `lp_fund` table has no tier classification → lp_quality_score always 0

The result: `lp_gp_commitments` is empty, all conviction scores are 0.

---

## What Already Exists (Do NOT Rebuild)

| Asset | Location | Notes |
|---|---|---|
| `LpGpCommitment`, `LpGpRelationship`, `PEFundConvictionScore` | `app/core/models.py`, `app/core/pe_models.py` | Tables are correct, keep as-is |
| `FundConvictionScorer` | `app/services/pe_fund_conviction_scorer.py` | 6-signal scorer works correctly |
| `FundLPTrackerAgent` | `app/agents/fund_lp_tracker_agent.py` | Async→sync fix already applied |
| `CafrParser` | `app/sources/lp_collection/cafr_parser.py` | PDF extraction + LLM works; `extract_pe_portfolio_schedule()` exists |
| `SECFormDCollector` | `app/sources/lp_collection/sec_form_d_collector.py` | Works; gives oversubscription + investor count signals |
| `pe_conviction.py` router | `app/api/v1/pe_conviction.py` | 5 endpoints exist |
| `LpCollectionOrchestrator` | `app/sources/lp_collection/runner.py` | Full orchestration framework |
| `FuzzyMatcher` | `app/agentic/fuzzy_matcher.py` | Entity resolution |
| `LLMClient` | `app/agentic/llm_client.py` | Used for CAFR extraction |

---

## Root Cause Analysis

### Why pension IR pages return 404
Public pension PE pages are NOT stable HTML tables — they're mostly:
1. **Quarterly IC meeting documents** (PDF, posted at `/board/meetings/YYYY/MMM`)
2. **Annual investment reports** embedded in the CAFR PDF appendix
3. **Ad-hoc disclosure pages** that restructure frequently

The `pension_ir_scraper.py` hardcoded "investments/private-equity" paths that no longer exist.

### Why Form 990 parsing returns 0
Modern EDGAR Form 990 filings are HTML documents with `<table>` tags. The current parser regex assumes `"FundName    BookValue"` line-by-line text. Schedule D is always in an HTML table with `<tr>/<td>` structure.

### Why CAFR is the gold standard (and unused)
Every US public pension fund is legally required to publish a Comprehensive Annual Financial Report. The PE portfolio schedule (often called "Schedule of Investments" or "Appendix A/B") is always in the CAFR. The `cafr_parser.py` already has `extract_pe_portfolio_schedule()` but:
- It's never called by `FundLPTrackerAgent`
- It requires knowing the CAFR PDF URL upfront (no URL discovery logic for pensions)

---

## New Files

### 1. `app/sources/lp_collection/pension_cafr_collector.py` *(replaces pension_ir_scraper)*

**Strategy:** For each target pension, use a two-step approach:
1. **Find the CAFR/Annual Report URL** — using pension-specific search patterns against the pension's known IR root URL and Google SERP as fallback
2. **Extract PE portfolio** — pass to `cafr_parser.extract_pe_portfolio_schedule()`

**10 target pensions with known stable IR roots:**

| LP | IR Root | CAFR Pattern |
|---|---|---|
| CalPERS | calpers.ca.gov | `/docs/forms-publications/comprehensive-annual-financial-report-{year}.pdf` |
| CalSTRS | calstrs.com | `/sites/main/files/file-attachments/cafr_{year}.pdf` |
| NY Common | osc.ny.gov | `/files/pdf/pension/{year}cafr.pdf` |
| Oregon PERS | oregon.gov/pers | `/documents/PERS-CAFR-{year}.pdf` |
| Washington WSIB | sib.wa.gov | `/publications/annual_reports/{year}-annual-report.pdf` |
| Texas TRS | trs.texas.gov | `/TRS_Documents/comprehensive_annual_financial_report_{year}.pdf` |
| NJ Pension | njtreasury.gov | `/doi/annualrpts/njdpb-annual-report-{year}.pdf` |
| Ohio STRS | strsoh.org | `/assets/files/publications/cafr-{year}.pdf` |
| Pennsylvania PSERS | psers.pa.gov | `/Publications/FinancialReports/PSERS-CAFR-{year}.pdf` |
| Illinois TRS | trs.illinois.gov | `/Downloader/{year}AFRCOMBINED.pdf` |

**URL resolution logic:**
1. Try most recent year (2024) → prior year (2023) → prior-prior (2022)
2. HTTP HEAD request to verify the PDF exists before downloading
3. If all fail, fall back to DuckDuckGo search: `"{pension_name}" CAFR {year} "private equity" filetype:pdf`

**Output:** Same schema as existing `pension_ir_scraper` output (lp_name, gp_name, fund_name, vintage_year, commitment_amount_usd, data_source='cafr')

---

### 2. `app/sources/lp_collection/form_990_html_parser.py` *(replaces naive regex in form_990_pe_extractor.py)*

**Problem:** Schedule D in EDGAR Form 990 is always an HTML table. Current regex parser misses it entirely.

**Strategy:** Use BeautifulSoup (already available via htmlparser) to parse Schedule D tables.

**Algorithm:**
1. Parse filing HTML with `html.parser`
2. Find `<table>` elements that contain Schedule D signals: `td` text matching `"Part XIV"`, `"Other Assets"`, `"Investment"`, `"Fund"`
3. For each row in the table, extract:
   - Investment name (first column)
   - Investment type (second column) — filter for "PE", "VC", "Partnership", "Private Equity"
   - Book value (last numeric column)
4. For each matching row, infer GP name (first 1-3 words before "Fund"/"Partners"/"Capital")

**Expand endowment targets from 5 → 12:**

| Endowment | EIN | Est. PE Allocation |
|---|---|---|
| Harvard Management Co. | 04-2103580 | ~35% |
| Yale University | 06-0646973 | ~40% |
| Stanford University | 94-1156365 | ~35% |
| MIT Investment Mgmt | 04-2103594 | ~30% |
| Princeton University | 21-0634501 | ~40% |
| Duke University | 56-0532129 | ~30% (ADD) |
| University of Michigan | 38-6006309 | ~25% (ADD) |
| University of Virginia | 54-0506458 | ~35% (ADD) |
| MacArthur Foundation | 23-7093598 | ~20% (ADD) |
| Ford Foundation | 13-1684331 | ~25% (ADD) |
| Rockefeller Foundation | 13-1659629 | ~20% (ADD) |
| Wellcome Trust (US ops) | 13-3948776 | ~30% (ADD) |

**Output:** Richer than current — includes `book_value_usd`, `lp_name`, `gp_name`, `fund_name` (no vintage yet — that's a known limitation of Form 990)

---

### 3. `app/sources/lp_collection/lp_tier_classifier.py` *(new utility)*

**Purpose:** Classify each `LpFund` record into a tier for the conviction scorer's `lp_quality_score` signal. This is the only signal currently always scoring 0.

**Tier definitions:**

| Tier | Score | LP Types | Examples |
|---|---|---|---|
| 1 — Sovereign/Endowment | 10 | sovereign_wealth, endowment | Yale, GIC, ADIA, CalPERS (by AUM) |
| 2 — Large Public Pension | 7 | public_pension with AUM > $50B | CalSTRS, NY Common, OTPP |
| 3 — Mid Public Pension/Foundation | 5 | public_pension < $50B, foundation | PSERS, MacArthur |
| 4 — Insurance/Corp Pension | 3 | corporate_pension, insurance | Prudential, MetLife |
| 5 — Family Office/HNW | 1 | family_office, hnw | Various |

**New column on `lp_fund`:** `lp_tier` (Integer, 1-5, nullable)

**Implementation:**
- `classify_lp_tier(lp: LpFund) -> int` — heuristic from `lp_type` + `aum_usd_billions`
- `classify_all_lps(db)` — run on all `lp_fund` rows; update `lp_tier` in bulk
- Called during collection pipeline and available as `POST /pe/conviction/classify-lps`

---

## Modified Files

### `app/agents/fund_lp_tracker_agent.py`

**Add 3 things:**

1. **CAFR as primary source:** Call `PensionCafrCollector` instead of (or in addition to) `PensionIRScraper`
2. **Form D integration:** After collecting commitment data, call `SECFormDCollector` for each distinct GP name found, use the results to enrich `LpGpCommitment` records with `oversubscription_ratio`
3. **Conviction scoring at end of pipeline:** After `_rebuild_relationships()`, call `FundConvictionScorer.score_from_data()` for each `pe_fund` record that has matching `LpGpRelationship` data, persist to `PEFundConvictionScore`

```python
# New end-of-pipeline call (after _rebuild_relationships)
def _score_affected_funds(self, gp_names: list[str]) -> int:
    """Score all PE funds whose GP has new LP relationship data."""
    # Find pe_firms matching collected GP names
    # For each firm, find their pe_funds records
    # Gather signals from lp_gp_relationships
    # Call FundConvictionScorer.score_from_data()
    # Persist PEFundConvictionScore
```

**Add GP name deduplication:**
- Before persisting, normalize GP names via `FuzzyMatcher` against existing `pe_firms.name` values
- Cache normalized names to avoid re-matching same GP 50 times

---

### `app/sources/lp_collection/form_990_pe_extractor.py`

**Replace naive regex parsing with HTML-aware parser from `form_990_html_parser.py`.**

Keep the EDGAR search logic (it works). Only replace the `_parse_schedule_d()` method.

---

### `app/sources/lp_collection/pension_ir_scraper.py`

**Replace hardcoded IR page URLs with CAFR document URLs** (delegate to `PensionCafrCollector`). Keep as a thin wrapper that calls the new collector.

Alternatively: deprecate `pension_ir_scraper.py` entirely; `FundLPTrackerAgent` calls `PensionCafrCollector` directly.

---

### `app/api/v1/pe_conviction.py`

**Add 3 new endpoints:**

```
POST /pe/conviction/classify-lps              Run LP tier classification
GET  /pe/conviction/lp-commitments            Browse raw LpGpCommitment records (filtered by LP or GP)
GET  /pe/conviction/coverage                  Show data coverage stats (LPs with data, vintages covered, sources)
```

**Coverage endpoint returns:**
```json
{
  "total_lp_commitments": 847,
  "unique_gps": 234,
  "unique_lps": 12,
  "vintages_covered": "2008-2024",
  "by_source": {
    "cafr": 612,
    "form_990": 235,
    "form_d": 0
  },
  "lps_with_data": [
    {"name": "CalPERS", "commitment_count": 145, "earliest_vintage": 2008, "latest_vintage": 2024},
    ...
  ]
}
```

---

### `app/core/models.py`

**Add `lp_tier` column to `LpFund`:**
```python
lp_tier = Column(Integer, nullable=True)  # 1=sovereign, 2=large_pension, 3=mid, 4=corp, 5=family_office
```

Requires `ALTER TABLE lp_fund ADD COLUMN IF NOT EXISTS lp_tier INTEGER`.

---

## Database Changes

| Change | Type | How |
|---|---|---|
| `lp_fund.lp_tier INTEGER` | New column | `ALTER TABLE lp_fund ADD COLUMN IF NOT EXISTS lp_tier INTEGER` |
| No new tables | — | Existing tables are correct |

---

## Conviction Scoring — End-to-End With Real Data

After this plan, for a PE fund like KKR Americas Fund XII (2019 vintage):

| Signal | Data Source | Expected |
|---|---|---|
| LP Quality | CAFR → LpGpCommitment → lp_fund.lp_tier | CalPERS (tier=2), CalSTRS (tier=2), Yale (tier=1) → lp_quality_score ≈ 75 |
| Re-up Rate | LpGpRelationship.total_vintages_committed / available vintages | KKR Fund VII, IX, X, XI, XII = 5 re-ups → reup_rate_pct = 100% |
| Oversubscription | Form D → LpGpCommitment → pe_funds | Fund XII: $10B target, $12.5B raised → 1.25x → score ≈ 69 |
| LP Diversity | LpGpCommitment count per fund | ~35 LP names → lp_diversity_score ≈ 80 |
| Time to Close | Form D date_of_first_sale + CAFR as_of_date | ~9 months → score ≈ 70 |
| GP Commitment | Manual / pe_funds table | Usually 1-3% → gp_commitment_score ≈ 40 |

**Expected conviction score for a blue-chip fund:** 70-80 (Grade B)

---

## Execution Order

```
Step 1: Schema change
  - ALTER TABLE lp_fund ADD COLUMN IF NOT EXISTS lp_tier INTEGER
  - Run manually or via API restart (add to database.py startup logic)

Step 2: New collectors
  A. Write pension_cafr_collector.py (CAFR URL finder + extract_pe_portfolio_schedule call)
  B. Write form_990_html_parser.py (BeautifulSoup Schedule D parser)
  C. Write lp_tier_classifier.py (LP tier logic)

Step 3: Modify existing
  A. Update fund_lp_tracker_agent.py:
     - Replace pension_ir_scraper with pension_cafr_collector
     - Add Form D enrichment step
     - Add _score_affected_funds() at end of run()
  B. Update form_990_pe_extractor.py: replace _parse_schedule_d() with HTML parser
  C. Update pe_conviction.py: add 3 new endpoints

Step 4: Integration
  A. Add ALTER TABLE to database.py startup (check-and-add pattern)
  B. Restart API
  C. POST /pe/conviction/classify-lps → tier all existing lp_fund rows
  D. POST /pe/conviction/collect → run full pipeline

Step 5: Verify
  A. GET /pe/conviction/coverage → shows LP commitment count > 0
  B. POST /pe/conviction/score/1 → returns conviction score with sub-scores > 0
  C. SELECT * FROM lp_gp_commitments LIMIT 10 → shows real CalPERS data
  D. SELECT * FROM pe_fund_conviction_scores LIMIT 5 → shows scored funds
```

---

## Verification Checklist

1. `pytest tests/ -v --ignore=tests/integration/` — all unit tests pass
2. `POST /pe/conviction/collect` completes without errors; logs show CAFR PDF downloaded
3. `GET /pe/conviction/coverage` → `total_lp_commitments > 50`
4. `SELECT * FROM lp_gp_commitments LIMIT 10` → shows real fund names + commitment amounts
5. `POST /pe/conviction/score/1` → conviction_score between 0 and 100, grade not null
6. `GET /pe/conviction/lp-base/1` → shows LP names with tier values populated
7. `GET /pe/conviction/signals` → at least 1 fund with conviction grade populated
8. CAFR PDF download succeeds for at least 3 of 10 target pensions

---

## Known Limitations / Risks

| Risk | Mitigation |
|---|---|
| CAFR PDF URLs change year-to-year | Try 2024 → 2023 → 2022; log which year was found |
| CAFR PE appendix layout varies by pension | LLM extraction handles layout variance; validate `manager_name` present |
| Form 990 Schedule D parsing still incomplete (no vintage year) | Accept as-is; flag data_completeness accordingly |
| Form D gives no individual LP commitments | Use only for oversubscription enrichment, not LP-level data |
| Large CAFR PDFs (200+ pages) | Use existing MAX_PAGES_TO_EXTRACT = 100; PE schedule is usually in last 30% |
| LLM hallucination on PDF extraction | Validate fund names against `pe_firms` table; require `manager_name` present |
| Public pension portals may block automated access | Use 3s rate limiting; accept partial collection |
