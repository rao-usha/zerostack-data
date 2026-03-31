# SPEC 030 — People Data QA Dashboard

**Status:** Draft
**Task type:** service
**Date:** 2026-03-27
**Test file:** tests/test_spec_030_people-qa-dashboard.py

## Goal

Build a rule-based QA service (`PeopleQAService`) that scores each company's people/org-chart data quality using 9 checks (no_ceo, duplicate title, stale snapshot, etc.), expose it via 3 API endpoints, and surface results in a `qa-dashboard.html` review UI with a merge-candidate approval workflow. No LLM calls — all checks run purely from DB queries.

## Acceptance Criteria

- [ ] `PeopleQAService.run_company(company_id, db)` returns `{company_id, issues, health_score}` with all 9 checks evaluated
- [ ] `PeopleQAService.run_all(db)` returns all companies with org charts, sorted by health_score ascending (worst first)
- [ ] `_health_score()` computes `100 - (20×ERRORs) - (10×WARNINGs) - (5×INFOs)`, clamped 0–100
- [ ] `GET /people-analytics/qa-report` returns company list with issues and health scores
- [ ] `GET /people-analytics/qa/merge-candidates?status=pending` returns enriched candidate cards
- [ ] `POST /people-analytics/qa/merge-candidates/{id}/resolve` accepts `approve`/`reject`, updates DB, returns `{status, action}`
- [ ] `approve` action sets `Person.canonical_id` on the lower-confidence record
- [ ] `qa-dashboard.html` loads QA report on page load, shows company health table with expandable issues
- [ ] Merge candidate review section shows side-by-side person cards with Approve/Reject buttons
- [ ] Gallery `index.html` links to QA dashboard

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_health_score_all_pass | Company with 0 issues → score 100 |
| T2 | test_health_score_errors_reduce | 1 ERROR → score 80, 2 ERRORs → score 60 |
| T3 | test_health_score_clamped | Many issues → score never < 0 |
| T4 | test_check_no_ceo_fires | Org chart with no management_level=1 non-board exec → no_ceo ERROR |
| T5 | test_check_no_ceo_passes | CEO present → no_ceo returns None |
| T6 | test_check_stale_snapshot | snapshot_date > 90 days ago → stale_snapshot WARNING |
| T7 | test_check_low_headcount | total_people=3 → low_headcount WARNING |
| T8 | test_check_no_org_chart | Company with no OrgChartSnapshot → no_org_chart ERROR |
| T9 | test_run_company_aggregates | run_company returns correct issue list and health_score |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Async methods where I/O is involved
- [ ] Uses dependency injection for DB sessions and external clients
- [ ] All DB operations use parameterized queries
- [ ] Uses `null_preserving_upsert()` for enrichment workflows
- [ ] Error handling follows the error hierarchy (`RetryableError`, `FatalError`, etc.)
- [ ] Logging with structured context (source, operation, record counts)
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

### PeopleQAService — check function contract

Each check returns `dict | None`:
- `None` = check passed
- `{"check": str, "severity": "ERROR"|"WARNING"|"INFO", "message": str, "count": int}` = issue found

```python
class PeopleQAService:
    def run_company(self, company_id: int, db: Session) -> dict:
        snapshot = db.query(OrgChartSnapshot)...first()
        company_people = db.query(CompanyPerson).filter(...is_current=True).all()
        issues = []
        for check_fn in self._checks:
            result = check_fn(company_id, snapshot, company_people, db)
            if result:
                issues.append(result)
        return {
            "company_id": company_id,
            "issues": issues,
            "health_score": self._health_score(issues),
        }

    def _health_score(self, issues: list) -> int:
        score = 100
        for i in issues:
            if i["severity"] == "ERROR": score -= 20
            elif i["severity"] == "WARNING": score -= 10
            else: score -= 5
        return max(0, score)
```

### Checks list

| Check | Trigger condition |
|-------|------------------|
| `_check_no_org_chart` | `snapshot is None` |
| `_check_no_ceo` | no CompanyPerson with `management_level=1` and `is_board_member=False` |
| `_check_duplicate_ceo_title` | count of current CPs with "CEO"/"Chief Executive" in title > 1 |
| `_check_stale_snapshot` | `snapshot.snapshot_date < today - 90 days` |
| `_check_low_headcount` | `snapshot.total_people < 5` |
| `_check_board_misclassified` | CP with senior title AND `is_board_member=True` |
| `_check_depth_anomaly` | `snapshot.max_depth > 10` |
| `_check_pending_dedup` | PeopleMergeCandidate with status='pending' AND company_id in shared_company_ids |
| `_check_low_confidence` | >30% of current CPs have `confidence='low'` |

### Merge resolve — approve logic

When action=`approve`:
1. Find both Person records
2. Pick winner = higher `confidence_score` (or lower person_id if tied)
3. Set `loser.canonical_id = winner.id`, `loser.is_canonical = False`
4. Set `candidate.status = 'approved'`, `candidate.canonical_person_id = winner.id`

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/services/people_qa_service.py` | Create | QA check engine with 9 checks |
| `app/api/v1/people_analytics.py` | Modify | Add qa-report, merge-candidates, resolve endpoints |
| `frontend/d3/qa-dashboard.html` | Create | Company health table + merge review UI |
| `frontend/index.html` | Modify | Add QA Dashboard link in gallery |

## Feedback History

_No corrections yet._
