# PLAN 034 — Agentic PE Org Intelligence + LP Fund Conviction

**Date:** 2026-03-25
**Status:** Approved
**Goal:** Two parallel tracks:
1. **Track A — PE Org Intelligence**: Classify IC members vs operating partners vs deal team; PE firm org snapshots
2. **Track B — LP Conviction Tracking**: Collect LP→GP fund commitments across vintages; compute fund conviction scores

---

## Context

We have mature PE firm people collection (BioExtractor + LLM) but no structure distinguishing IC members from operating partners from deal team — all sit flat in `pe_firm_people`. On the LP side, `LpManagerOrVehicleExposure` stores manager names as text with no FK, no vintage tracking, and no commitment amounts — making it impossible to compute re-up rates or fund conviction scores. This plan closes both gaps.

**What "LP conviction in a GP" means operationally:**
- An LP commits to Fund I → re-ups for Fund II, III, IV with growing commitment size = maximum conviction
- Multiple tier-1 LPs (Yale, CalPERS, GIC) in the same fund = validation signal
- Fund closed above target in <12 months = demand exceeded supply
- These signals are derivable from publicly available data (CAFR, Form D, pension IR pages, Form 990)

---

## What Already Exists (Do NOT Rebuild)

| Asset | Location |
|---|---|
| `PEFirmPeople`, `PEPerson`, `PEPersonExperience`, `PEPersonEducation` | `app/core/pe_models.py` |
| `BioExtractor` (website team page scraper + LLM) | `app/sources/pe_collection/people_collectors/bio_extractor.py` |
| `OrgChartBuilder` (4-pass hierarchy builder) | `app/sources/people_collection/org_chart_builder.py` |
| `LpFund`, `LpStrategySnapshot`, `LpManagerOrVehicleExposure` | `app/core/models.py` |
| `cafr_parser.py` (CAFR PDF extraction with LLM) | `app/sources/lp_collection/cafr_parser.py` |
| `LpCollectionOrchestrator` | `app/sources/lp_collection/runner.py` |
| `ExitReadinessScorer` (scoring pattern to reuse) | `app/core/pe_exit_scoring.py` |
| `LLMClient` | `app/agentic/llm_client.py` |
| `FuzzyMatcher` entity resolution | `app/agentic/fuzzy_matcher.py` |

---

## New Database Tables

### Track A — PE Org (`app/core/pe_models.py`)

**New column on `PEFirmPeople`:**
- `role_type`: Enum — `'investment_team'`, `'operating_partner'`, `'advisory_board'`, `'lpac_member'`, `'ir_fundraising'`, `'finance_ops'`

**New table `PEInvestmentCommittee`:**
```python
id, firm_id (FK), person_id (FK)
role: str          # 'voting_member', 'observer', 'chair'
is_current: bool
start_date, end_date
```

**New table `PEFirmOrgSnapshot`:**
```python
id, firm_id (FK), snapshot_date
org_json (JSONB)                  # full hierarchy
ic_member_count, op_partner_count
investment_team_count, total_headcount
changes_from_prior (JSONB)        # diff vs previous snapshot
```

### Track B — LP Conviction

**New table `LpGpCommitment` (`app/core/models.py`):**
```python
id, lp_id (FK to lp_fund)
gp_name: str             # canonical GP name
gp_firm_id (FK nullable) # linked to pe_firms if resolved
fund_name: str           # "KKR Americas Fund XII"
fund_vintage: int        # 2019
commitment_amount_usd: Numeric
commitment_date: Date
capital_called_pct: float
status: str              # 'active', 'harvesting', 'exited'
data_source: str         # 'cafr', 'pension_ir', 'form_990', 'form_d'
source_url: str
as_of_date: Date
```

**New table `LpGpRelationship` (`app/core/models.py`):**
```python
id, lp_id (FK), gp_name, gp_firm_id (FK nullable)
first_vintage: int, last_vintage: int
total_vintages_committed: int        # re-up count (key conviction signal)
total_committed_usd: Numeric
avg_commitment_usd: Numeric
commitment_trend: str    # 'growing', 'stable', 'declining', 'new'
last_updated: DateTime
```

**New table `PEFundConvictionScore` (`app/core/pe_models.py`):**
```python
id, fund_id (FK to pe_funds), scored_at
conviction_score: float  # 0-100
conviction_grade: str    # A/B/C/D/F
lp_quality_score: float          # 25% weight
reup_rate_score: float           # 25% weight
oversubscription_score: float    # 20% weight
lp_diversity_score: float        # 15% weight
time_to_close_score: float       # 10% weight
gp_commitment_score: float       # 5% weight
# Raw signals
lp_count, repeat_lp_count, tier1_lp_count: int
oversubscription_ratio: float    # final_close / target_size
days_to_final_close: int
reup_rate_pct: float
data_completeness: float         # 0-1 confidence
```

---

## New Files

### Track A — PE Org Intelligence

| File | Purpose |
|---|---|
| `app/services/pe_org_classifier.py` | Classify each PEFirmPeople member into `role_type` using title heuristics + LLM; build IC membership records |
| `app/services/pe_org_snapshot.py` | Compute diff vs prior snapshot; detect IC departures, new OP additions; store to `pe_firm_org_snapshots` |

**Modify:**
- `app/sources/pe_collection/people_collectors/bio_extractor.py` — add `role_type` to LLM prompt; emit on every extracted person
- `app/api/v1/pe_firms.py` — add `GET /pe/firms/{id}/org-intelligence` and `GET /pe/firms/{id}/org-snapshot/latest`

### Track B — LP Commitment Collectors

| File | Purpose |
|---|---|
| `app/sources/lp_collection/pension_ir_scraper.py` | Scrape ~10 public pensions: CalPERS, CalSTRS, NY Common, Oregon, Washington, NJ, Texas TRS, Ohio STRS |
| `app/sources/lp_collection/sec_form_d_collector.py` | EDGAR Form D: fund name, total raised, amount sold, date of first sale, investor count → oversubscription + time-to-close signals |
| `app/sources/lp_collection/form_990_pe_extractor.py` | Form 990 Schedule D for Ivy endowments + large foundations; PE fund names + book values |

**Modify:**
- `app/sources/lp_collection/cafr_parser.py` — add `extract_pe_portfolio_schedule()` to parse PE Portfolio appendix tables

### Track B — Conviction Analytics

| File | Purpose |
|---|---|
| `app/services/pe_fund_conviction_scorer.py` | 6-signal conviction scoring (mirrors `pe_exit_scoring.py`); returns score + narrative |
| `app/agents/fund_lp_tracker_agent.py` | Agentic orchestrator: collect → normalize GP names via FuzzyMatcher → build LpGpRelationships → score |
| `app/api/v1/pe_conviction.py` | New router: conviction score CRUD + LP base composition + market signals |

**Modify:**
- `app/main.py` — register `pe_conviction` router

---

## Conviction Scoring Weights

| Signal | Weight | Data Source |
|--------|--------|-------------|
| LP Quality | 25% | LP tier × tier-1 LP count (sovereign/endowment=10, pension=7, foundation=5) |
| Re-up Rate | 25% | `LpGpRelationship.total_vintages_committed` / available fund vintages |
| Oversubscription | 20% | `pe_funds.final_close_usd / target_size_usd` |
| LP Diversity | 15% | LP count + Herfindahl concentration |
| Time to Close | 10% | `first_close_date → final_close_date` in days |
| GP Commitment | 5% | GP's own % of fund (alignment signal) |

---

## Execution Order

```
Agent A (Track A — PE Org):
  1. Add role_type col + PEInvestmentCommittee + PEFirmOrgSnapshot to pe_models.py
  2. Write pe_org_classifier.py + pe_org_snapshot.py
  3. Modify bio_extractor.py (role_type in LLM prompt)
  4. Add org endpoints to pe_firms.py

Agent B (Track B — LP Data + Scoring):
  1. Add LpGpCommitment + LpGpRelationship to models.py
  2. Add PEFundConvictionScore to pe_models.py
  3. Write pension_ir_scraper.py + sec_form_d_collector.py + form_990_pe_extractor.py
  4. Extend cafr_parser.py with extract_pe_portfolio_schedule()
  5. Write pe_fund_conviction_scorer.py + fund_lp_tracker_agent.py
  6. Write pe_conviction.py router

Master (Integration):
  1. Register pe_conviction router in main.py
  2. Restart API; verify all endpoints 200/404
  3. Smoke test: run pension_ir_scraper on CalPERS
  4. Smoke test: compute conviction score for seeded PE fund
  5. Commit
```

---

## Verification

1. `pytest tests/ -v --ignore=tests/integration/` — all unit tests pass
2. `curl http://localhost:8001/pe/firms/1/org-intelligence` — IC roster + op partner count
3. `curl -X POST http://localhost:8001/pe/conviction/collect` — triggers LP collection job
4. `curl -X POST http://localhost:8001/pe/conviction/score/1` — returns conviction score 0-100
5. `SELECT * FROM lp_gp_commitments LIMIT 10` — shows CalPERS commitment records
6. `SELECT * FROM pe_investment_committees LIMIT 10` — shows IC composition
