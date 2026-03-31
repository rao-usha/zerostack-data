# SPEC 040 — Eval System Improvements

**Status:** Draft
**Task type:** service
**Date:** 2026-03-29
**Test file:** tests/test_spec_040_eval-improvements.py

## Goal

Extend the eval builder (SPEC_039) with five improvements identified after launch: regression alerting via webhook, three domain-specific Tier 3 LLM judges, an eval health summary endpoint, assertion type validation at case creation, and 5 missing test cases from SPEC_039 plus a per-case dry-run endpoint and LP/FO/county seeding.

## Acceptance Criteria

- [ ] `EVAL_REGRESSION_WEBHOOK_URL` env var triggers POST after any regression run
- [ ] `GET /evals/regressions/recent` returns last N runs with `is_regression=True`
- [ ] `llm_org_chart_quality`, `llm_people_plausibility`, `llm_report_coherence` added to `_SCORERS`
- [ ] All three new judges share a common `_call_llm_judge_sync()` helper
- [ ] `GET /evals/health` returns per-suite latest score in a single DB query (no N+1)
- [ ] `POST /evals/suites/{id}/cases` returns 422 for unknown `assertion_type`
- [ ] 5 previously-missing tests now pass: person_exists_fuzzy, person_exists_no_match, enrichment_coverage_pct, dry_run_no_eval_run_created, eval_run_persists_captured_output
- [ ] `POST /evals/suites/{suite_id}/cases/{case_id}/dry-run` returns scorer result without persisting
- [ ] `_baseline_lp_cases()` seeding added to seed-from-db
- [ ] All 31+ tests pass

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_webhook_fires_on_regression | webhook URL called when is_regression=True |
| T2 | test_webhook_silent_when_unset | no error when EVAL_REGRESSION_WEBHOOK_URL unset |
| T3 | test_webhook_silent_on_success | webhook NOT called when no regression |
| T4 | test_llm_org_chart_quality_pass | org chart scorer returns score ≥ 70 with good data |
| T5 | test_llm_org_chart_quality_no_key | returns failure gracefully when OPENAI_API_KEY unset |
| T6 | test_llm_people_plausibility_pass | people plausibility scorer returns result |
| T7 | test_llm_report_coherence_pass | report coherence scorer returns result |
| T8 | test_health_summary_no_runs | suites with no runs show null scores |
| T9 | test_health_summary_with_runs | returns per-suite composite scores |
| T10 | test_case_create_invalid_assertion_type | 422 for unknown assertion_type |
| T11 | test_case_create_valid_assertion_type | 201 for known assertion_type |
| T12 | test_scorer_person_exists_fuzzy | "Andrew Sullivan" matches "Andrew F. Sullivan" |
| T13 | test_scorer_person_exists_no_match | "John Smith" does not match "Jane Doe" |
| T14 | test_scorer_enrichment_coverage_pct | 80% coverage passes, 30% fails |
| T15 | test_dry_run_no_eval_run_created | dry-run creates no EvalRun row |
| T16 | test_eval_run_persists_captured_output | EvalRun.captured_output is non-null after run |
| T17 | test_per_case_dry_run | scorer result returned, no DB write |

## Rubric Checklist

- [x] Clear single responsibility (one domain concern per service)
- [x] Uses dependency injection for DB sessions and external clients
- [x] All DB operations use parameterized queries
- [x] Error handling follows the error hierarchy
- [x] Logging with structured context
- [x] Has corresponding test file with mocked dependencies
- [x] Tests cover happy path, error cases, and boundary conditions

## Design Notes

### Phase A — Webhook Alerting
- Add `_fire_regression_webhook(run: EvalRun, suite: EvalSuite, regressions: list) -> None` to `eval_runner.py`
- Called synchronously after `run.is_regression = True`; catches all exceptions (never crashes a run)
- Uses `httpx.Client(timeout=5.0).post(url, json=payload)`
- `GET /evals/regressions/recent` — simple query on `EvalRun WHERE is_regression=True ORDER BY triggered_at DESC`

### Phase B — Domain LLM Judges
- Extract `_call_llm_judge_sync(prompt: str, pass_threshold: float) -> ScorerResult` from `_score_llm_judge`
- `llm_org_chart_quality`: prompt evaluates hierarchy plausibility, no circular reporting, dept groupings
- `llm_people_plausibility`: prompt evaluates real names, title consistency, no extraction artifacts
- `llm_report_coherence`: prompt evaluates narrative flow, data-backed conclusions, no boilerplate
- Each scorer builds domain-specific prompt then delegates to `_call_llm_judge_sync`

### Phase C — Health Summary
```python
# Single query via subquery for latest run per suite
subq = (
    db.query(EvalRun.suite_id, func.max(EvalRun.id).label("max_id"))
    .filter(EvalRun.status == "completed")
    .group_by(EvalRun.suite_id)
    .subquery()
)
```
- Left join EvalSuite → subquery → EvalRun to get latest run data
- Aggregate summary stats (avg composite, regression count) in Python

### Phase D — Validation
- In `POST /evals/suites/{id}/cases`: `if assertion_type not in EvalScorer.SUPPORTED_TYPES: raise HTTPException(422, ...)`
- Test: POST with `assertion_type="bad_type"` → 422

### Phase E — Per-Case Dry-Run + LP Seeding
- `POST /evals/suites/{suite_id}/cases/{case_id}/dry-run`
  - Load case, optionally merge `assertion_params` override from body
  - `_capture_output(suite, case.entity_id, db)` → `EvalScorer.score(case, output, db)`
  - Return `{case_id, assertion_type, passed, score, actual_value, expected_value, failure_reason}`
- LP seeding: `_baseline_lp_cases(suite, db)` → `lp_count_range` case reading LP count

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/services/eval_runner.py` | Modify | Add `_fire_regression_webhook()` |
| `app/services/eval_scorer.py` | Modify | Extract `_call_llm_judge_sync`, add 3 domain judges |
| `app/api/v1/evals.py` | Modify | Add `/health`, `/regressions/recent`, case dry-run, assertion_type validation |
| `tests/test_spec_040_eval-improvements.py` | Create | 17 new tests |
| `tests/test_spec_039_eval-builder.py` | Modify | Add 5 missing test cases |

## Feedback History

_No corrections yet._
