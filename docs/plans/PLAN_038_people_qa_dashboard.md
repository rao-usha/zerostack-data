# PLAN_038 — People Data QA Dashboard (Layer 1 + 2)

## Context

After wiring D3 visualizations to real data, we identified data quality issues: duplicate people in org charts, missing CEOs, stale snapshots, unreviewed merge candidates sitting in `PeopleMergeCandidate` with status=`pending`. A comprehensive DQ framework already exists (`DataQualityRule`, `DataQualityResult`, `DataQualityService`) but is not wired to people/org-chart-specific checks. Layer 3 (LLM verification) is explicitly out of scope for this plan.

**Goal:** Build a rule-based QA service that scores each company's people data quality, surface issues in an actionable dashboard, and provide a merge-candidate review UI.

---

## Existing Infrastructure to Reuse

| What | Where |
|------|-------|
| `DataQualityService.get_overall_stats()` | `app/services/data_quality_service.py` |
| `PeopleMergeCandidate` model (pending/approved/rejected/auto_merged) | `app/core/people_models.py` lines 574–618 |
| `OrgChartSnapshot` (company_id, snapshot_date, chart_data, total_people, max_depth) | `app/core/people_models.py` lines 394–444 |
| `CompanyPerson` (management_level, is_board_member, title, confidence, is_current) | `app/core/people_models.py` lines 209–285 |
| `IndustrialCompany` (id, name, leadership_last_updated, status) | `app/core/people_models.py` lines 126–207 |
| `Person` (canonical_id, is_canonical, confidence_score, last_verified_date) | `app/core/people_models.py` lines 56–124 |
| `get_db` dependency, `Session` pattern | `app/core/database.py` |
| Router registration pattern | `app/main.py` |

---

## Phase 1 — QA Service (`app/services/people_qa_service.py`) NEW FILE

### Checks implemented (all run from DB — no LLM):

| Check name | Severity | What it detects |
|------------|----------|-----------------|
| `no_org_chart` | ERROR | Company has no `OrgChartSnapshot` at all |
| `no_ceo` | ERROR | Org chart has no exec with `management_level=1` and `is_board_member=false` |
| `duplicate_ceo_title` | WARNING | >1 current `CompanyPerson` with "CEO" or "Chief Executive" in title |
| `stale_snapshot` | WARNING | Most recent `snapshot_date` > 90 days old |
| `low_headcount` | WARNING | `total_people < 5` (likely incomplete collection) |
| `board_misclassified` | WARNING | `CompanyPerson` with "CEO"/"CFO"/"President" title AND `is_board_member=true` |
| `depth_anomaly` | WARNING | `max_depth > 10` (likely circular chain or bad data) |
| `pending_dedup` | INFO | `PeopleMergeCandidate` rows with `status='pending'` linked to this company via `shared_company_ids` |
| `low_confidence` | INFO | >30% of `CompanyPerson` rows at this company have `confidence='low'` |

### Service class:

```python
class PeopleQAService:
    CHECKS = [...]  # ordered list of check functions

    def run_company(self, company_id: int, db: Session) -> dict:
        """Run all checks for one company. Returns {company_id, issues, health_score}."""

    def run_all(self, db: Session, limit: int = None) -> list[dict]:
        """Run checks for all companies with org charts. Returns list sorted by health_score asc."""

    def _health_score(self, issues: list[dict]) -> int:
        """100 - (20×ERRORs) - (10×WARNINGs) - (5×INFOs), clamped to 0–100."""
```

Each check function signature: `def _check_xxx(company_id, snapshot, company_people, db) -> dict | None`
Returns `{check, severity, message, count}` or `None` if check passes.

---

## Phase 2 — API Endpoints (`app/api/v1/people_analytics.py`)

### New endpoints:

**`GET /people-analytics/qa-report`**
- Query params: `limit=50` (top N companies by worst health score first)
- Calls `PeopleQAService().run_all(db, limit=limit)`
- Returns:
```json
[
  {
    "company_id": 142,
    "company_name": "Prudential Financial",
    "health_score": 75,
    "total_people": 36,
    "snapshot_date": "2026-02-09",
    "issues": [
      {"check": "stale_snapshot", "severity": "WARNING", "message": "Snapshot is 47 days old", "count": 1},
      {"check": "pending_dedup", "severity": "INFO", "message": "3 merge candidates pending review", "count": 3}
    ]
  }
]
```

**`GET /people-analytics/qa/merge-candidates`**
- Query params: `status=pending` (default), `limit=50`
- Joins `PeopleMergeCandidate` → `Person` (×2) → `CompanyPerson` (×2) for name/title/company
- Returns side-by-side person cards for review UI:
```json
[
  {
    "id": 12,
    "similarity_score": 0.87,
    "match_type": "name_fuzzy",
    "status": "pending",
    "person_a": {"id": 629, "name": "Andrew Sullivan", "title": "CEO", "company": "Prudential", "sources": ["website"]},
    "person_b": {"id": 639, "name": "Andrew F. Sullivan", "title": "CEO Prudential Financial", "company": "Prudential", "sources": ["sec_proxy"]}
  }
]
```

**`POST /people-analytics/qa/merge-candidates/{candidate_id}/resolve`**
- Body: `{"action": "approve" | "reject"}`
- Updates `PeopleMergeCandidate.status` → `approved` or `rejected`
- If `approved`: sets `Person.canonical_id` on the lower-confidence record → points to the better record
- Returns: `{"status": "ok", "action": "approved"}`

---

## Phase 3 — QA Dashboard (`frontend/d3/qa-dashboard.html`) NEW FILE

### Layout (dark theme, consistent with other D3 pages):

```
┌─────────────────── Header ──────────────────────────────────────┐
│ ← Gallery   People Data QA                  [Run QA] button     │
├──────── Stats Bar ──────────────────────────────────────────────┤
│  6 companies  │  XX open issues  │  YY pending merges  │  ZZ%  │
│  with data    │                  │                     │avg health│
├────────────────────────────────────────────────────────────────-┤
│ Company Health Table (sortable)    │ Issue Detail Panel         │
│                                    │                            │
│ ● Cummins         94  ✓ 0 issues  │ [click row to expand]     │
│ ⚠ Prudential      75  2 issues    │ - stale_snapshot WARNING   │
│ ✗ JPMorgan        45  3 issues    │ - pending_dedup INFO ×3    │
│                                    │                            │
├────────────────────────────────────────────────────────────────-┤
│ Merge Candidate Review                                           │
│ ┌──── Person A ────┐  similarity  ┌──── Person B ────┐         │
│ │ Andrew Sullivan  │    0.87      │ Andrew F. Sullivan│         │
│ │ CEO, Prudential  │  name_fuzzy  │ CEO, Prudential   │         │
│ │ Source: website  │              │ Source: sec_proxy │         │
│ └──────────────────┘              └──────────────────┘         │
│            [✓ Approve — Same Person]  [✗ Reject — Different]   │
└────────────────────────────────────────────────────────────────-┘
```

### Data flow:
1. On load: `GET /people-analytics/qa-report?limit=50` → populate company health table
2. On company row click: expand issues inline
3. On tab switch to "Merge Review": `GET /people-analytics/qa/merge-candidates?status=pending` → render cards
4. Approve/Reject: `POST /people-analytics/qa/merge-candidates/{id}/resolve` → update card status
5. "Run QA" button → re-fetches qa-report (QA runs synchronously for now, <2s for 6 companies)

---

## Phase 4 — Register & Wire

- Import `PeopleQAService` in `app/api/v1/people_analytics.py`
- Add link to `frontend/index.html` gallery for QA Dashboard page

---

## Critical Files

| File | Action |
|------|--------|
| `app/services/people_qa_service.py` | **CREATE** — QA check engine |
| `app/api/v1/people_analytics.py` | **MODIFY** — add 3 new endpoints |
| `frontend/d3/qa-dashboard.html` | **CREATE** — QA review UI |
| `frontend/index.html` | **MODIFY** — add QA Dashboard link |

**No changes to:** `app/main.py` (router already registered), `app/core/people_models.py` (all models exist)

---

## Verification

```bash
# 1. QA report — all companies scored
curl http://localhost:8001/api/v1/people-analytics/qa-report | python -m json.tool

# 2. Merge candidates
curl http://localhost:8001/api/v1/people-analytics/qa/merge-candidates?status=pending | python -m json.tool

# 3. Resolve a candidate
curl -X POST http://localhost:8001/api/v1/people-analytics/qa/merge-candidates/1/resolve \
  -H 'Content-Type: application/json' -d '{"action":"reject"}'

# 4. Visual review
open http://localhost:3001/d3/qa-dashboard.html
```

## Implementation Order

1. `app/services/people_qa_service.py` — QA engine (testable standalone)
2. API endpoints in `people_analytics.py`
3. Restart + verify endpoints curl
4. `frontend/d3/qa-dashboard.html` — dashboard UI
5. `frontend/index.html` — add gallery link
