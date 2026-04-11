# PLAN_043 — Eval System Improvements (Post-Launch)

## Status: Draft
**Date:** 2026-03-29
**Prerequisite:** PLAN_041 / SPEC_039 eval builder complete (all 9 phases done)

---

## What Was Accomplished (PLAN_041 recap)

All 9 phases shipped:

| Phase | What | Status |
|-------|------|--------|
| 1 | DB models (EvalSuite, EvalCase, EvalRun, EvalResult) | ✅ |
| 2 | 26 rule-based assertion scorers across 5 domains | ✅ |
| 3 | EvalRunner — 4 capture modes, regression detection, composite scoring | ✅ |
| 4 | Full CRUD API — 14 endpoints (suites, cases, run, dry-run, seed) | ✅ |
| 5 | Seed script — 23 suites × 51 cases for all agent endpoints | ✅ |
| 6 | LLM judge — generic `llm_judge` Tier 3 scorer with OpenAI gpt-4o-mini | ✅ |
| 7 | APScheduler — P1 daily 05:00, P2 weekly Mon 05:30, P3 monthly 06:00 | ✅ |
| 8 | Eval dashboard — dark theme, suite list, detail panel, run history, edit | ✅ |
| 9 | Tests — 26 passing (full spec coverage) | ✅ |

---

## Gaps Identified by Audit

### Critical (blocking production value)

**1. Regressions are silent.**
The system *detects* regressions (sets `EvalRun.is_regression=True`) but nothing *acts* on them. Nobody gets notified when a nightly P1 run regresses. The detection logic is wasted without a delivery mechanism.

**2. Tier 3 judges are generic, not domain-specific.**
`llm_judge` uses a free-form `criteria` param. For common agent output types (org chart hierarchy quality, people plausibility, report coherence) this means every suite author must re-invent the same rubric. Three domain-specific Tier 3 types with baked-in prompts would make suite authoring much faster and evaluations more consistent.

### High (user-facing gaps)

**3. No eval health summary.**
To answer "how are my evals doing overall?" you have to query individual suites and stitch results together. A single `/evals/health` endpoint returning suite-level scores + regression counts would power dashboards, alerts, and spot-checks without N API calls.

**4. Case creation accepts invalid assertion types.**
`POST /evals/suites/{suite_id}/cases` accepts any `assertion_type` string. If a typo or renamed type is used, the case is created successfully but silently scores `0` on every run with `failure_reason: "Unknown assertion_type 'xxx'"`. Validation at create time would catch this immediately.

**5. Missing test coverage (5 cases).**
Current: 26 tests. Missing spec cases: `person_exists_fuzzy`, `person_exists_no_match`, `enrichment_coverage_pct`, `dry_run_no_eval_run_created`, `eval_run_persists_captured_output`. Easy to add, needed for full spec compliance.

### Medium (operational improvements)

**6. Seeding only covers 3 entity types.**
`seed-from-db` generates baseline cases for `company`, `pe_firm`, and `three_pl`. Missing: `lp`, `family_office`, and `county` (datacenter). Limits automated bootstrap coverage for those domains.

**7. No per-case dry-run.**
Currently dry-run operates at the suite level (capture output, no DB write). To tune a single case's `assertion_params` you have to run the full suite. A per-case dry-run that takes `assertion_params` in the request body would speed up iteration.

---

## Proposed Improvements

### Phase A — Regression Alerting (`app/services/eval_runner.py` + `app/api/v1/evals.py`)

Add a lightweight webhook-based alert system:

- Read env var `EVAL_REGRESSION_WEBHOOK_URL` (optional; silent if unset)
- After any run where `is_regression=True`, POST a JSON payload to the webhook URL:
  ```json
  {
    "suite_name": "Deep Collect: Industrial Companies",
    "run_id": 42,
    "composite_score": 0.0,
    "regressions": [
      {"case_name": "ceo_exists", "tier": 1, "prev_avg": 100.0, "current": 0.0}
    ],
    "triggered_by": "schedule",
    "triggered_at": "2026-03-30T05:00:12Z"
  }
  ```
- Webhook fires synchronously at end of `run_suite()` (httpx, 5s timeout, fire-and-forget on failure)
- Also expose `GET /evals/regressions/recent?limit=20` — last N runs with `is_regression=True`, sorted by triggered_at desc

**Files:** `eval_runner.py` (2 additions), `evals.py` (1 new endpoint)

---

### Phase B — Domain-Specific Tier 3 Judges (`app/services/eval_scorer.py`)

Add three specialized LLM assertion types that encapsulate domain knowledge in their prompts:

| Type | Rubric | Pass threshold |
|------|--------|----------------|
| `llm_org_chart_quality` | Evaluates: hierarchy plausibility (CEO→VP→Director), no circular reporting, department groupings make business sense, title consistency | 70 |
| `llm_people_plausibility` | Evaluates: names sound real, titles match seniority, no obvious extraction artifacts ("DIRECTOR DIRECTOR"), no placeholder text | 70 |
| `llm_report_coherence` | Evaluates: narrative flows logically, data cited supports conclusions, no contradictions, no boilerplate filler, executive summary matches body | 70 |

Each scorer builds its own multi-point rubric prompt. `assertion_params` can override `pass_threshold` and add `focus_areas` (extra guidance). All three reuse the same `_call_llm_judge_sync()` helper extracted from `_score_llm_judge`.

**Files:** `eval_scorer.py` (~80 lines), 3 entries added to `_SCORERS`

---

### Phase C — Eval Health Summary (`app/api/v1/evals.py`)

**`GET /evals/health`**
Returns one object per active suite, joining the latest completed run. Optionally filters `domain=` or `priority=`.

```json
{
  "summary": {
    "total_suites": 23,
    "suites_with_data": 18,
    "suites_with_regressions": 2,
    "avg_composite": 74.3,
    "last_run_at": "2026-03-29T22:10:00Z"
  },
  "suites": [
    {
      "suite_id": 1,
      "suite_name": "Deep Collect: Industrial Companies",
      "priority": 1,
      "domain": "people",
      "last_run_id": 42,
      "last_run_at": "2026-03-29T22:10:00Z",
      "composite_score": 87.5,
      "is_regression": false,
      "cases_passed": 4,
      "cases_failed": 0
    }
  ]
}
```

Single DB query (LEFT JOIN EvalRun subquery for latest run per suite). No N+1.

**Files:** `evals.py` (~50 lines)

---

### Phase D — Validation + Test Gaps

**Case creation validation** (`evals.py`):
- `POST /evals/suites/{suite_id}/cases`: validate `assertion_type in EvalScorer.SUPPORTED_TYPES`
- Return `422 Unprocessable Entity` with message: `"Unknown assertion_type 'xxx'. Supported: ceo_exists, headcount_range, ..."`

**Missing tests** (`tests/test_spec_039_eval-builder.py`):
- `test_scorer_person_exists_fuzzy` — "Andrew Sullivan" matches "Andrew F. Sullivan" at ≥0.85
- `test_scorer_person_exists_no_match` — "John Smith" does not match "Jane Doe"
- `test_scorer_enrichment_coverage_pct` — ThreePL coverage: pass at 80%, fail at 30%
- `test_dry_run_no_eval_run_created` — POST /evals/dry-run/{id} → no EvalRun row in DB
- `test_eval_run_persists_captured_output` — run completes → EvalRun.captured_output is non-null

**Files:** `evals.py` (3 lines), `tests/test_spec_039_eval-builder.py` (5 new tests)

---

### Phase E — Seeding & Per-Case Dry-Run (optional, lower priority)

**Seed-from-DB for LP/FO/County:**
- `_baseline_lp_cases()`: `lp_count_range` case reading LP table count
- `_baseline_fo_cases()`: headcount check for family office people
- `_baseline_county_cases()`: datacenter score threshold check

**Per-case dry-run endpoint:**
- `POST /evals/suites/{suite_id}/cases/{case_id}/dry-run`
  Body: `{"assertion_params": {...}}` — optional override
  Returns: scorer result without persisting to DB
  Useful for: "what would my new params produce before I commit the edit?"

---

## Prioritization

| Phase | Effort | Value | Ship order |
|-------|--------|-------|------------|
| A — Regression alerting | Small (< 1h) | High — makes detection actionable | 1st |
| B — Domain LLM judges | Medium (2h) | High — Tier 3 actually useful | 2nd |
| C — Health summary | Small (1h) | High — dashboard stats bar | 3rd |
| D — Validation + tests | Small (1h) | Medium — correctness + coverage | 4th |
| E — Seeding + per-case dry-run | Medium (2h) | Medium — operational | 5th |

---

## Critical Files

| File | Action |
|------|--------|
| `app/services/eval_runner.py` | Add webhook dispatch after regression detected |
| `app/services/eval_scorer.py` | Add 3 domain-specific Tier 3 types, extract `_call_llm_judge_sync` helper |
| `app/api/v1/evals.py` | Add `/evals/health`, `/evals/regressions/recent`, per-case dry-run, assertion_type validation |
| `tests/test_spec_039_eval-builder.py` | Add 5 missing test cases |

**No changes to:** `app/core/eval_models.py` (models are complete), `app/main.py`, `scripts/seed_eval_suites.py` (Phase E only if pursued), `frontend/eval-dashboard.html` (JS is complete)

---

## What This Is NOT

- No new DB tables or schema changes
- No changes to the composite score formula or regression thresholds
- No frontend rebuild — dashboard already has full JS implementation
- No changes to existing tests (26 tests stay green)
- No LLM cost increases beyond what Tier 3 cases already incur

---

## Verification

```bash
# After Phase A — regression webhook fires
curl -X POST http://localhost:8001/api/v1/evals/regressions/recent | python -m json.tool

# After Phase B — new LLM judge types registered
curl http://localhost:8001/api/v1/evals/suites | python -m json.tool
# Create a case with assertion_type=llm_org_chart_quality and run it

# After Phase C — health summary
curl http://localhost:8001/api/v1/evals/health | python -m json.tool

# After Phase D — validation rejects unknown type
curl -X POST http://localhost:8001/api/v1/evals/suites/1/cases \
  -H 'Content-Type: application/json' \
  -d '{"name":"test","assertion_type":"bad_type","tier":1}'
# → 422 Unprocessable Entity

# Run all tests
pytest tests/test_spec_039_eval-builder.py -v
# → 31 passed
```
