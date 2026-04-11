# PLAN_041 — Agentic Pipeline Eval Builder

**Status:** Draft — awaiting approval
**Date:** 2026-03-29
**Depends on:** PLAN_038 (PeopleQAService), PLAN_039 (DQ Unification)

---

## Goal

Build an evaluation framework **bound directly to every agentic API and agent class** in the system. Each eval suite is attached to a concrete agent or API endpoint and evaluates the actual output it produces. Covers all 23 agent-based endpoints across 10 domains.

Evals are fully editable after creation — suites, cases, assertion params, and tiers can be updated at any time through the UI or API without reseeding from scratch.

---

## What this is NOT

| | DQ Checks (PLAN_038/039) | Unit Tests | Eval Builder (this plan) |
|---|---|---|---|
| Purpose | Is stored data healthy? | Does the code work? | Does the agent produce good output? |
| Ground truth | Rule heuristics | Code assertions | Human-curated expected outputs |
| Runs on | Every ingestion | Every commit | On-demand / weekly scheduled |
| Subject | DB tables | Functions | Agent output or API response |

---

## Full Inventory — All 23 Agent Endpoints

Grouped by domain with their eval binding, recommended mode, and rollout priority.

### Priority 1 — Core Product (client-facing, demo-critical, regressions hurt immediately)

| # | Endpoint | Agent Class | Output Type | Eval Mode | Key Assertions |
|---|---|---|---|---|---|
| 1 | `POST /people-jobs/deep-collect/{id}` | `DeepCollectionOrchestrator` | `DeepCollectionResult` | `agent_output` | ceo_exists, headcount_range, org_depth_range, no_duplicate_ceo |
| 2 | `POST /people-jobs/recursive-collect/{id}` | `RecursiveCollector` | `CollectionResult` | `agent_output` | headcount_range, org_depth_range, subsidiary_count_range |
| 3 | `POST /pe/collection/collect` | `PECollectionOrchestrator` | job status + DB records | `db_snapshot` | deal_count_range, has_deal_with_status, firm_people_count |
| 4 | `POST /ai-reports/generate` | `ReportWriterAgent` | HTML/JSON report | `report_output` | report_section_present, report_word_count, report_no_empty_tables |
| 5 | `POST /datacenter-sites/{fips}/thesis` | `LLMClient` direct | thesis text | `api_response` | response_status_200, response_word_count_range, thesis_mentions_county |

### Priority 2 — Demo & Research Workflows (used in client demos or regular runs)

| # | Endpoint | Agent Class | Output Type | Eval Mode | Key Assertions |
|---|---|---|---|---|---|
| 6 | `POST /people-jobs/test/{id}` | `PeopleCollectionOrchestrator` | diagnostics dict | `api_response` | response_status_200, extraction_count_range, no_extraction_errors |
| 7 | `POST /people/companies/{id}/collect-comp` | `ProxyCompAgent` | comp + form4 dicts | `api_response` | response_field_present(salary), response_field_present(equity) |
| 8 | `POST /agents/deep-research` | `DeepResearchAgent` | investment analysis | `api_response` | response_status_200, response_field_present(thesis), response_field_present(risks) |
| 9 | `POST /agents/research/company` | `CompanyResearchAgent` | company profile | `api_response` | response_status_200, response_field_present(summary), response_time_ms |
| 10 | `POST /diligence/start` | `DueDiligenceAgent` | DD memo + risk score | `api_response` | response_field_present(risk_score), response_field_range(risk_score, 0, 100) |
| 11 | `POST /pe/conviction/score/{fund_id}` | `FundConvictionScorer` | conviction score | `api_response` | response_field_range(score, 0, 100), response_field_present(grade) |

### Priority 3 — Background Pipelines (async enrichment, not yet primary client-facing)

| # | Endpoint | Agent Class | Output Type | Eval Mode | Key Assertions |
|---|---|---|---|---|---|
| 12 | `POST /agents/research/batch` | `CompanyResearchAgent` (batched) | list of profiles | `api_response` | response_list_length(min:1), response_status_200 |
| 13 | `POST /competitive/analyze` | `CompetitiveIntelAgent` | competitor matrix | `api_response` | response_field_present(competitors), response_list_length(competitors, min:1) |
| 14 | `POST /market/scan/trigger` | `MarketScannerAgent` | signals list | `api_response` | response_status_200, response_field_present(signals) |
| 15 | `POST /diligence/start` (batch) | `DueDiligenceAgent` | DD results | `api_response` | response_status_200 |
| 16 | `POST /hunter/start` | `DataHunterAgent` | gap fills | `api_response` | response_status_200, response_field_present(filled_count) |
| 17 | `POST /anomalies/scan` | `AnomalyDetectorAgent` | anomaly list | `api_response` | response_status_200, response_field_present(anomalies) |
| 18 | `POST /monitors/news/process` | `NewsMonitor` | matches + alerts | `api_response` | response_status_200, response_field_present(matches_created) |
| 19 | `POST /lp-collection/collect` | `LpCollectionOrchestrator` | LP records | `db_snapshot` | enrichment_coverage_pct(website, 50%), lp_count_range |
| 20 | `POST /fo-collection/collect` | `FoCollectionOrchestrator` | FO records | `db_snapshot` | enrichment_coverage_pct(website, 50%) |
| 21 | `POST /datacenter-sites/score-counties` | `CountyRegulatoryScorer` | site scores | `db_snapshot` | score_count_range, score_field_range(composite, 0, 100) |
| 22 | `POST /people-reports/management-assessment` | Report builder | HTML report | `report_output` | report_section_present(Org Chart), report_section_present(Leadership), report_data_cells_pct |
| 23 | `POST /macro/simulate` | `MacroSensitivityAgent` | scenario results | `api_response` | response_status_200, response_field_present(scenarios) |

---

## Three Binding Modes

### Mode 1 — `agent_output`
Calls the Python agent class directly (in isolation), captures its structured output (`CollectionResult`, `DeepCollectionResult`, `ExtractedPerson` list) **before any DB write**, and evaluates it against the cases.

Best for: deep-collect, recursive-collect — where you want to evaluate raw extraction quality independent of DB state.

### Mode 2 — `api_response`
Makes a live HTTP call to the running API endpoint, evaluates the JSON response. No agent code is invoked directly — it goes through the full stack.

Best for: most Priority 2 and 3 endpoints — tests the full pipeline including auth, serialization, caching, and error handling.

### Mode 3 — `report_output`
Calls a report generator endpoint, receives HTML or JSON, parses it for structural completeness and data quality.

Best for: `ReportWriterAgent`, `management-assessment`, `pe_deal_memo` — where output is a document not a structured JSON.

### Mode 4 — `db_snapshot` (zero agent invocation)
Reads current DB state for an entity, evaluates against expected values. No API call, no agent invocation — fast and free.

Best for: PE collection, LP/FO collection, datacenter scoring — where the output accumulates in DB over time rather than returning in a single response.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Eval Suite                                                          │
│  name: "Deep Collect — Industrial Companies"                         │
│  binding_type: "agent"                                               │
│  binding_target: "DeepCollectionOrchestrator"                        │
│  eval_mode: "agent_output"                                           │
│  priority: 1                                                         │
│  schedule_cron: "0 9 * * 1"  (weekly Monday)                        │
│                                                                      │
│  Eval Cases (editable any time)                                      │
│  ├── T1: CEO must be found                                           │
│  ├── T1: No extraction errors                                        │
│  ├── T2: Headcount 5–200                                             │
│  ├── T2: Org depth ≥ 2                                               │
│  └── T3: LLM judge — hierarchy makes sense                          │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Eval Runner — dispatches by eval_mode                               │
│  agent_output  → AgentOutputCapture(agent_class, entity_id)         │
│  api_response  → APIResponseCapture(endpoint, method, params)       │
│  report_output → ReportOutputCapture(endpoint, template)            │
│  db_snapshot   → DBSnapshotCapture(entity_type, entity_id)          │
│                                                                      │
│  → runs each case through EvalScorer                                 │
│  → detects regressions vs rolling 5-run average                     │
│  → persists EvalRun + EvalResult rows                                │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Eval Dashboard                                                      │
│  Suite list (all 23+) | Score trend | Regression flags | Edit cases  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Data Models

New file: `app/core/eval_models.py`

### `eval_suites`
```python
id, name, description
binding_type    # "agent" | "api" | "report" | "db"
binding_target  # Python class path or API endpoint path
eval_mode       # "agent_output" | "api_response" | "report_output" | "db_snapshot"
priority        # 1 | 2 | 3  (drives dashboard ordering + schedule frequency)
schedule_cron   # e.g. "0 9 * * 1"
is_active, created_at, updated_at
```

### `eval_cases`
```python
id, suite_id (FK), name, description
entity_type     # "company" | "pe_firm" | "three_pl" | "report" | "api_response" | "lp" | "county"
entity_id       # e.g. industrial_companies.id (nullable for api_response mode)
entity_name     # denormalized for display
assertion_type  # see Assertion Types table
assertion_params  (JSON)       # editable
tier            # 1 (hard) | 2 (soft) | 3 (LLM judge)
weight          # default 1.0
regression_threshold_pct  # default 15.0
previous_params (JSON)    # saved before each edit
edited_at, edit_reason    # audit trail
is_active, created_at, updated_at
```

### `eval_runs`
```python
id, suite_id (FK)
status          # running | completed | failed
triggered_by    # manual | schedule | api
triggered_at, completed_at
composite_score, tier1_pass_rate, tier2_avg_score, tier3_avg_score
is_regression, regression_details (JSON)
captured_output (JSON)   # raw agent/API output for replay and debug
llm_cost_usd
cases_total, cases_passed, cases_failed
errors (JSON)
```

### `eval_results`
```python
id, run_id (FK), case_id (FK)
passed (bool), score (0–100), partial_credit (bool)
actual_value (JSON), expected_value (JSON), failure_reason
llm_judge_prompt, llm_judge_response, llm_judge_score, llm_judge_reasoning
evaluated_at
```

---

## Assertion Types

### People / Org Chart

| assertion_type | Tier | Params | Checks |
|---|---|---|---|
| `ceo_exists` | 1 | — | management_level=1, not board |
| `no_duplicate_ceo` | 1 | — | ≤1 active CEO title |
| `person_exists` | 1 | `{full_name, fuzzy_threshold:0.85}` | Fuzzy name match |
| `no_extraction_errors` | 1 | `{max_errors:0}` | CollectionResult.errors empty |
| `headcount_range` | 2 | `{min, max}` | Total people extracted |
| `has_person_with_title` | 2 | `{title_contains, min_count}` | Title match count |
| `person_has_title` | 2 | `{full_name, expected_title}` | Named person title |
| `org_depth_range` | 2 | `{min_depth, max_depth}` | Chart max_depth |
| `confidence_threshold` | 2 | `{min_avg_confidence}` | Avg confidence score |
| `confidence_distribution` | 2 | `{min_high_pct:0.5}` | % high-confidence people |
| `source_pages_found` | 2 | `{min_pages}` | Pages successfully scraped |
| `dept_coverage` | 2 | `{required_depts:[...]}` | Required dept names present |
| `llm_hierarchy_quality` | 3 | `{prompt_context}` | LLM rates hierarchy 1–10 |
| `llm_people_plausibility` | 3 | — | LLM: "do these people plausibly run this co?" |

### API Response

| assertion_type | Tier | Params | Checks |
|---|---|---|---|
| `response_status_200` | 1 | — | HTTP 200 |
| `response_field_present` | 1 | `{field_path}` | JSON field non-null |
| `response_no_error_key` | 1 | — | Response has no `"error"` or `"detail"` key |
| `response_field_range` | 2 | `{field_path, min, max}` | Numeric field in range |
| `response_list_length` | 2 | `{field_path, min, max}` | Array length in range |
| `response_time_ms` | 2 | `{max_ms}` | Latency within budget |
| `response_word_count_range` | 2 | `{field_path, min, max}` | Word count of a text field |

### Report Generator

| assertion_type | Tier | Params | Checks |
|---|---|---|---|
| `report_section_present` | 1 | `{section_name}` | Section heading exists in HTML |
| `report_no_empty_tables` | 1 | — | No all-N/A or all-zero tables |
| `report_word_count` | 2 | `{min, max}` | Narrative word count |
| `report_data_cells_pct` | 2 | `{min_pct}` | % data cells with real values |
| `thesis_mentions_entity` | 2 | `{entity_name}` | Entity name appears in output text |
| `llm_report_quality` | 3 | `{report_type, prompt_context}` | LLM rates coherence/accuracy |

### PE / Conviction / 3PL / LP

| assertion_type | Tier | Params | Checks |
|---|---|---|---|
| `deal_count_range` | 2 | `{min, max}` | PE deal count |
| `has_deal_with_status` | 1 | `{status}` | Deal with status exists |
| `enrichment_coverage_pct` | 2 | `{field, min_pct}` | % records with field populated |
| `score_field_range` | 2 | `{field, min, max}` | Numeric score in valid range |
| `lp_count_range` | 2 | `{min, max}` | LP record count |

---

## Editing Eval Cases

Cases are editable at any time — non-destructive:

1. `PATCH /api/v1/evals/cases/{case_id}` — update `assertion_params`, `tier`, `regression_threshold_pct`, `edit_reason`
2. Previous params saved to `eval_cases.previous_params` before overwrite — visible in the dashboard edit panel
3. Past `eval_results` are **never retroactively changed** — they preserve the trend line
4. `POST /api/v1/evals/cases/{case_id}/dry-run` — test a param change against latest captured output without creating a run record

---

## API Endpoints

New router: `app/api/v1/evals.py`

```
# Suite CRUD
GET    /api/v1/evals/suites                            List all suites (grouped by priority)
POST   /api/v1/evals/suites                            Create suite
GET    /api/v1/evals/suites/{suite_id}                 Suite detail + cases
PATCH  /api/v1/evals/suites/{suite_id}                 Edit suite
DELETE /api/v1/evals/suites/{suite_id}                 Deactivate

# Case CRUD
POST   /api/v1/evals/suites/{suite_id}/cases           Add case
GET    /api/v1/evals/cases/{case_id}                   Case detail
PATCH  /api/v1/evals/cases/{case_id}                   Edit case (params, tier, threshold)
DELETE /api/v1/evals/cases/{case_id}                   Deactivate
POST   /api/v1/evals/cases/{case_id}/dry-run           Test case without recording run

# Running evals
POST   /api/v1/evals/run/{suite_id}                    Trigger eval run (async bg task)
POST   /api/v1/evals/run-priority/{priority}           Run all suites at a given priority (1|2|3)
GET    /api/v1/evals/runs                              Recent runs across all suites
GET    /api/v1/evals/runs/{run_id}                     Run detail + per-case results
GET    /api/v1/evals/suites/{suite_id}/history         Run history for trend chart

# Regression & health
GET    /api/v1/evals/regressions                       All regression-flagged runs
GET    /api/v1/evals/health                            One-line health per suite (for status bar)

# Seeding
POST   /api/v1/evals/suites/{suite_id}/seed-from-db    Auto-generate cases from current DB state
```

New endpoint `run-priority/{priority}` lets you run all Priority 1 suites with one call — useful for a weekly cron that runs P1 daily, P2 weekly, P3 monthly.

---

## Runner Architecture (`app/services/eval_runner.py`)

```python
class EvalRunner:
    def run_suite(self, suite_id: int, db: Session) -> EvalRun:
        suite = db.get(EvalSuite, suite_id)
        output = self._capture_output(suite)
        results = [EvalScorer.score(c, output, db) for c in suite.active_cases]
        regressions = self._detect_regressions(suite_id, results, db)
        return self._persist(suite, output, results, regressions, db)

    def _capture_output(self, suite) -> CapturedOutput:
        match suite.eval_mode:
            case "agent_output":  return AgentOutputCapture(suite).capture()
            case "api_response":  return APIResponseCapture(suite).capture()
            case "report_output": return ReportOutputCapture(suite).capture()
            case "db_snapshot":   return DBSnapshotCapture(suite).capture()
```

`CapturedOutput` stores: `raw` (the agent/API output), `entity_id`, `capture_time`, `cost_usd`. The scorer receives this object rather than querying the DB directly — enabling offline replay of a previous capture without re-running the agent.

---

## Frontend Dashboard (`frontend/eval-dashboard.html`)

Dark theme, consistent with `pe-demo.html`. Six sections:

**1. Stats bar** — Active Suites | P1 Health | Regressions (30d) | Avg T1 Pass Rate | Total LLM Cost (30d)

**2. Suite list** — Table of all suites grouped by priority (P1 / P2 / P3). Columns: Name | Domain | Mode | Last Score | Last Run | Schedule | Status chip. "Run All P1" button at top.

**3. Suite detail** (click to expand) — Shows binding target, schedule, last captured output summary. "Edit Suite" and "Run Now" buttons.

**4. Case table (editable)** — Columns: Name | Entity | Assertion | Tier | Last Score | Status chip. Each row has an Edit button that opens an inline form with:
- Structured param inputs (not raw JSON) — number inputs for ranges, text for names, dropdown for tier
- Regression threshold slider
- Edit reason field
- "Previous params" accordion

**5. Score trend chart** — Chart.js line, last 10 runs. Reference line at 80. Regression points in red.

**6. Run history** — Date | Score | T1 Pass % | Cases | Regression flag | LLM cost. Expandable rows showing per-case verdicts with actual vs expected values.

---

## Seeding Presets

`POST /api/v1/evals/suites/{id}/seed-from-db`

Query params: `entity_type`, `entity_id`. Auto-generates baseline cases from current DB state with buffers.

| entity_type | Auto-generated cases |
|---|---|
| `company` | ceo_exists (T1), no_duplicate_ceo (T1), headcount_range ±30% (T2), org_depth_range ±1 (T2), person_exists per top exec (T2) |
| `pe_firm` | deal_count_range ±30% (T2), has_deal_with_status=closed (T1), firm_people_count_range (T2) |
| `three_pl` | enrichment_coverage_pct(website, 60%) (T2), enrichment_coverage_pct(hq_city, 50%) (T2) |
| `lp` | lp_count_range ±20% (T2), enrichment_coverage_pct(aum, 40%) (T2) |
| `report` | report_section_present per section in last report (T1), report_data_cells_pct 70% (T2), report_word_count ≥500 (T2) |
| `api_response` | response_status_200 (T1), response_field_present per top-level key in last response (T1) |

---

## Regression Detection

After each run, per-case scores compared to rolling average of last 5 completed runs:
- **Tier 1 regression:** previously-passing case now fails — always flagged regardless of threshold
- **Tier 2/3 regression:** score drops >15% from rolling average (configurable per-case)
- Requires minimum 2 prior runs before regression can fire

```json
[{"case_id": 3, "case_name": "Prudential headcount", "prev_avg": 85.0, "current": 40.0, "drop_pct": 52.9}]
```

---

## Schedule Strategy

| Priority | Suites | Suggested Cadence |
|---|---|---|
| P1 (5 suites) | deep-collect, pe-collect, ai-reports, datacenter-thesis, recursive-collect | Daily at 9am |
| P2 (6 suites) | test-collect, proxy-comp, deep-research, company-research, diligence, conviction | Weekly Monday |
| P3 (12 suites) | competitive, market-scan, hunter, anomalies, news-monitor, lp, fo, datacenter-score, management-report, macro, batch-research | Monthly 1st |

---

## Implementation Phases

| Phase | What | Files | Est. |
|-------|------|-------|------|
| 1 | DB models | `app/core/eval_models.py` | 0.5d |
| 2 | Scorer — all assertion types | `app/services/eval_scorer.py` | 1.5d |
| 3 | Runner + db_snapshot + api_response capture | `app/services/eval_runner.py` | 1d |
| 4 | API endpoints (full CRUD + run-priority) | `app/api/v1/evals.py`, `app/main.py` | 1d |
| 5 | Seed presets for all entity types | extend `app/api/v1/evals.py` | 0.5d |
| 6 | agent_output capture mode | extend `eval_runner.py` | 0.5d |
| 7 | report_output capture mode | extend `eval_runner.py` | 0.5d |
| 8 | LLM judge (Tier 3) | extend `eval_scorer.py` | 0.5d |
| 9 | Frontend dashboard + case editor | `frontend/eval-dashboard.html` | 1.5d |

**Total: ~8 days.** Phases 1–5 cover all 23 endpoints with db_snapshot + api_response modes (zero agent invocation cost). Phases 6–9 add live capture, LLM judging, and the full UI.

---

## Critical Files

| File | Action | Description |
|------|--------|-------------|
| `app/core/eval_models.py` | CREATE | EvalSuite, EvalCase, EvalRun, EvalResult |
| `app/services/eval_scorer.py` | CREATE | All assertion type scoring functions + LLM judge |
| `app/services/eval_runner.py` | CREATE | EvalRunner + 4 capture modes + regression detection |
| `app/api/v1/evals.py` | CREATE | Full CRUD + run-priority + dry-run + seed endpoints |
| `app/main.py` | MODIFY | Import eval_models, register evals router + OpenAPI tag |
| `frontend/eval-dashboard.html` | CREATE | Dashboard with editable case table, trend charts, run history |
| `frontend/index.html` | MODIFY | Add Eval Builder gallery card |

**No changes to any existing agent code** — the runner wraps agents externally.
