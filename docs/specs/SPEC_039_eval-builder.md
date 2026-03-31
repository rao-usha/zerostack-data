# SPEC 039 — Eval Builder: Agentic Pipeline Evaluation Framework

**Status:** Draft
**Task type:** service
**Date:** 2026-03-29
**Plan:** PLAN_041_eval_builder.md
**Test file:** tests/test_spec_039_eval-builder.py

## Goal

Build an evaluation framework bound directly to all 23 agentic APIs and agent classes in Nexdata. Each eval suite attaches to a specific agent or endpoint and evaluates its output against human-curated assertions. Evals are fully editable after creation. Covers all domains: People/Org, PE, Reports, Research, DD, Competitive, LP/FO, Datacenter, Macro.

## Acceptance Criteria

- [ ] `eval_suites`, `eval_cases`, `eval_runs`, `eval_results` tables created in DB on startup
- [ ] `EvalScorer` implements all 25 rule-based assertion types (people, api, report, PE/3PL/LP)
- [ ] `EvalRunner` dispatches to 4 capture modes: `db_snapshot`, `api_response`, `agent_output`, `report_output`
- [ ] Regression detection fires for Tier 1 failures and Tier 2/3 drops >15% from 5-run rolling avg
- [ ] Full CRUD API: suites (GET/POST/PATCH/DELETE), cases (GET/POST/PATCH/DELETE), dry-run
- [ ] `POST /evals/run/{suite_id}` triggers async eval run and returns run_id
- [ ] `POST /evals/run-priority/{1|2|3}` runs all active suites at that priority
- [ ] `POST /evals/suites/{id}/seed-from-db` auto-generates cases for all 6 entity types
- [ ] `PATCH /evals/cases/{id}` saves previous_params before overwrite; past results unchanged
- [ ] LLM judge (Tier 3): `llm_hierarchy_quality`, `llm_people_plausibility`, `llm_report_quality`
- [ ] APScheduler jobs registered: P1 daily, P2 weekly, P3 monthly
- [ ] 23 eval suites seeded (one per agent endpoint), all active
- [ ] `frontend/eval-dashboard.html` renders suite list, editable case table, score trend, run history
- [ ] Gallery card added to `frontend/index.html`

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_scorer_ceo_exists_pass | ceo_exists returns score=100 when management_level=1 person present |
| T2 | test_scorer_ceo_exists_fail | ceo_exists returns score=0, passed=False when no CEO in company_people |
| T3 | test_scorer_headcount_range_partial | headcount=15 with min=20 returns partial credit, not 0 |
| T4 | test_scorer_headcount_range_pass | headcount within range returns score=100 |
| T5 | test_scorer_person_exists_fuzzy | "Andrew Sullivan" fuzzy-matches "Andrew J. Sullivan" at 0.85 threshold |
| T6 | test_scorer_person_exists_no_match | Unknown name returns passed=False |
| T7 | test_scorer_no_duplicate_ceo_pass | Single CEO returns score=100 |
| T8 | test_scorer_no_duplicate_ceo_fail | Two active CEOs returns score=0 |
| T9 | test_scorer_response_status_200_pass | CapturedOutput with status=200 returns passed=True |
| T10 | test_scorer_response_status_200_fail | CapturedOutput with status=500 returns passed=False |
| T11 | test_scorer_response_field_present_nested | field_path="data.ceo.name" traverses nested JSON |
| T12 | test_scorer_response_field_range_partial | value=45 with min=50 returns partial credit |
| T13 | test_scorer_report_section_present_pass | HTML with "Executive Summary" heading returns passed=True |
| T14 | test_scorer_report_section_present_fail | HTML without section returns passed=False |
| T15 | test_scorer_enrichment_coverage_pct | 60/100 records with website → 60% coverage, passes min_pct=50 |
| T16 | test_regression_tier1_always_flags | Tier 1 case that passed before now fails → is_regression=True regardless of threshold |
| T17 | test_regression_tier2_threshold | Tier 2 score drops 20% (above default 15%) → is_regression=True |
| T18 | test_regression_tier2_under_threshold | Tier 2 score drops 10% (below 15%) → is_regression=False |
| T19 | test_regression_requires_two_prior_runs | Only 1 prior run → regression detection skipped, is_regression=False |
| T20 | test_seed_from_db_company | seed-from-db for company creates ceo_exists, no_duplicate_ceo, headcount_range cases |
| T21 | test_patch_case_saves_previous_params | PATCH case params → previous_params = old params, new params applied |
| T22 | test_dry_run_no_eval_run_created | dry-run returns ScorerResult without creating EvalRun row in DB |
| T23 | test_run_priority_only_active_suites | run-priority/1 skips inactive suites |
| T24 | test_eval_run_persists_captured_output | completed EvalRun has captured_output JSON populated |
| T25 | test_tier1_fail_zeros_composite | all_tier1_pass=False → composite_score=0 regardless of T2/T3 scores |

## Rubric Checklist

- [ ] Clear single responsibility (EvalScorer scores, EvalRunner orchestrates, API exposes)
- [ ] Async methods where I/O is involved (runner uses async for API capture mode)
- [ ] Uses dependency injection for DB sessions
- [ ] All DB operations use parameterized queries (SQLAlchemy ORM only, no raw SQL)
- [ ] Error handling: capture failures never crash the eval run — log and continue to next case
- [ ] Logging with structured context: suite_id, run_id, case counts, scores, regression flags
- [ ] Tests cover happy path, error cases, and boundary conditions (see T1–T25 above)
- [ ] No changes to any existing agent code — runner wraps externally

## Design Notes

### Composite score formula
```
tier1_all_pass = all(r.passed for r in tier1_results)
composite = 0 if not tier1_all_pass else round(
    0.50 * avg(tier1_scores) +
    0.30 * avg(tier2_scores) +
    0.20 * avg(tier3_scores)
)
```

### CapturedOutput dataclass
```python
@dataclass
class CapturedOutput:
    mode: str                    # "db_snapshot" | "api_response" | "agent_output" | "report_output"
    entity_id: int | None
    raw: dict | list | str       # the captured data
    capture_time: datetime
    latency_ms: float | None     # for api_response mode
    cost_usd: float              # LLM cost if agent was called
    error: str | None            # if capture failed
```

### Partial credit formula (Tier 2 range assertions)
```
score = 100 * min(actual / expected_min, 1.0)   # if actual < min
score = 100                                       # if min <= actual <= max
score = 100 * min(expected_max / actual, 1.0)    # if actual > max
```

### Regression detection (rolling window)
```python
prior_scores = [last 5 completed runs for this case]
if len(prior_scores) < 2: return  # not enough history
avg = mean(prior_scores)
drop_pct = (avg - current_score) / avg * 100
if tier == 1 and current_score < 100 and any(s == 100 for s in prior_scores):
    flag_regression()
elif drop_pct > case.regression_threshold_pct:
    flag_regression()
```

### Field path traversal (response_field_present)
```python
def _get_nested(data, path):  # path = "data.ceo.name"
    for key in path.split("."):
        if isinstance(data, dict): data = data.get(key)
        else: return None
    return data
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/core/eval_models.py` | Create | EvalSuite, EvalCase, EvalRun, EvalResult SQLAlchemy models |
| `app/services/eval_scorer.py` | Create | All 25 assertion types + LLM judge harness |
| `app/services/eval_runner.py` | Create | EvalRunner + 4 CapturedOutput capture classes + regression |
| `app/api/v1/evals.py` | Create | Full CRUD + run/run-priority/dry-run/seed-from-db endpoints |
| `app/main.py` | Modify | Import eval_models, register evals router + OpenAPI tag |
| `frontend/eval-dashboard.html` | Create | Dashboard UI with editable case table |
| `frontend/index.html` | Modify | Add Eval Builder gallery card |

## Feedback History

_No corrections yet._
