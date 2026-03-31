# PLAN_039 — Data Quality Unification

**Status:** Draft — awaiting approval
**Date:** 2026-03-27
**Depends on:** PLAN_038 (PeopleQAService complete)

---

## Problem

The DQ audit revealed four structural issues that will compound as we add more datasets:

| Issue | Impact |
|-------|--------|
| `DataQualityValidator` in `app/core/data_quality.py` (362 lines) is defined but never instantiated anywhere | Dead code shipping to prod, confusion for maintainers |
| Two classes named `DataQualityService` — one generic rules engine (`app/core/data_quality_service.py`), one people-specific (`app/services/data_quality_service.py`) | Import ambiguity; two developers will use the wrong one |
| Quality score computed 3 different ways across the codebase | Dashboard numbers disagree with API numbers disagree with DB numbers |
| Rules engine (`DataQualityRule` / `DataQualityResult`) never auto-runs after job completion | DQ data goes stale silently; ops team can't trust it |
| PE Intelligence, Site Intelligence, 3PL — zero DQ integration | We have no systematic way to know if PE or Site Intel data is good |

---

## Goal

A single, consistent DQ framework that:
1. Shares one quality score formula across all datasets
2. Has one clear entry point per dataset (no naming confusion)
3. Auto-runs after any ingestion job completes
4. Covers People, PE Intelligence, Site Intelligence, and 3PL

---

## Non-Goals

- LLM-based verification (PLAN_038 Layer 3, explicitly deferred)
- Automated merge / resolution of detected issues (human review via QA dashboard)
- Schema migrations or new DB tables

---

## Existing Infrastructure

| File | Status | Role |
|------|--------|------|
| `app/core/data_quality.py` | **Dead** — delete | DataQualityValidator never used |
| `app/core/data_quality_service.py` | **Keep, rename** → `dq_engine.py` | Generic rules engine (DataQualityService class) |
| `app/services/data_quality_service.py` | **Keep, rename** → `people_dq_service.py` | People-specific DQ (confusingly also DataQualityService) |
| `app/services/people_qa_service.py` | **Existing** (PLAN_038) | 9-check QA engine for people data |
| `app/core/people_models.py` | **Existing** | DataQualityRule, DataQualityResult, PeopleMergeCandidate |
| `app/core/pe_models.py` | **Existing** | PEDeal, PEFirm, PEPortfolioCompany |
| `app/core/models_site_intel.py` | **Existing** | SiteIntelCollectionJob |
| `app/core/models.py` | **Existing** | IngestionJob (lifecycle hook point) |

---

## Phase 1 — Dead Code Removal + Naming Fix

**Files touched:** `app/core/data_quality.py` (delete), rename two files, update imports

### Steps

1. **Delete** `app/core/data_quality.py` — `DataQualityValidator` is defined but imported 0 times. Confirm with `grep -r "DataQualityValidator" app/` before deleting.

2. **Rename** `app/core/data_quality_service.py` → `app/core/dq_engine.py`
   - Class stays `DataQualityEngine` (rename from `DataQualityService`)
   - Update import in `app/api/v1/people_analytics.py` and anywhere else referenced

3. **Rename** `app/services/data_quality_service.py` → `app/services/people_dq_service.py`
   - Class stays `PeopleDQService` (rename from `DataQualityService`)
   - Update import in `app/api/v1/people_analytics.py`

4. **Consolidate** `PeopleDQService` + `PeopleQAService` — they both score people data quality but use different schemas. Options:
   - Option A: `PeopleQAService` absorbs `PeopleDQService` checks (preferred — QAService is newer, cleaner)
   - Option B: Keep separate, add adapter so both feed the same `health_score` formula

   **Decision: Option A.** `PeopleQAService` becomes the single entry point for people DQ.

---

## Phase 2 — Standardize Quality Score Formula

**Files touched:** `app/services/people_qa_service.py`, new `app/core/dq_base.py`

### Unified formula (4 dimensions, same across all datasets)

```
quality_score = (
    0.35 × completeness_score   # required fields present
  + 0.25 × freshness_score      # data age vs threshold
  + 0.25 × validity_score       # structural / rule checks pass
  + 0.15 × consistency_score    # cross-field coherence
)
× 100 → clamped 0–100
```

### Base class

```python
# app/core/dq_base.py
class BaseQualityProvider(ABC):
    dataset: str              # "people", "pe", "site_intel", "three_pl"

    @abstractmethod
    def run(self, entity_id: int, db: Session) -> QualityReport:
        """Run all checks for one entity. Returns QualityReport."""

    @abstractmethod
    def run_all(self, db: Session, limit: int | None = None) -> list[QualityReport]:
        """Run checks across all entities. Sorted worst-first."""

@dataclass
class QualityReport:
    entity_id: int
    entity_name: str
    dataset: str
    quality_score: int          # 0–100
    completeness: int           # 0–100 dimension sub-scores
    freshness: int
    validity: int
    consistency: int
    issues: list[QualityIssue]
    checked_at: datetime

@dataclass
class QualityIssue:
    check: str
    severity: Literal["ERROR", "WARNING", "INFO"]
    message: str
    count: int
```

### PeopleQAService refactor

Map existing checks to dimensions:
- `completeness`: no_ceo, low_headcount, no_org_chart, low_confidence
- `freshness`: stale_snapshot
- `validity`: duplicate_ceo_title, depth_anomaly, board_misclassified
- `consistency`: pending_dedup (name collision / dedup backlog)

---

## Phase 3 — Wire Rules Engine to Job Lifecycle

**Files touched:** `app/core/models.py` (or job completion handler), `app/core/dq_engine.py`

Currently `DataQualityEngine` (nee DataQualityService) stores `DataQualityRule` and `DataQualityResult` in DB but is never triggered automatically.

### Change

Add a `post_job_completion` hook in the job runner that triggers the relevant `BaseQualityProvider.run()` for the affected entity after any ingestion job reaches `success`:

```python
# In worker job completion path:
def _on_job_success(job: IngestionJob, db: Session):
    provider = _get_dq_provider(job.source)
    if provider and job.entity_id:
        report = provider.run(job.entity_id, db)
        _persist_quality_report(report, db)
```

Mapping `job.source` → `BaseQualityProvider`:
- `people_*` → `PeopleQualityProvider`
- `pe_*` → `PEQualityProvider`
- `site_intel_*` → `SiteIntelQualityProvider`
- `three_pl_*` → `ThreePLQualityProvider`

---

## Phase 4 — Extend DQ to PE Intelligence, Site Intel, 3PL

**Files touched:** new `app/services/pe_dq_service.py`, `site_intel_dq_service.py`, `three_pl_dq_service.py`

### PE Intelligence checks

| Check | Severity | What |
|-------|----------|------|
| `no_deal_date` | WARNING | `PEDeal.deal_date` is null |
| `no_deal_amount` | INFO | `PEDeal.deal_amount` is null (often private) |
| `stale_news` | WARNING | No `PEFirmNews` in last 90 days for active firm |
| `low_portfolio_coverage` | WARNING | <3 portfolio companies for a firm with >$500M AUM |
| `missing_lp_data` | INFO | `PEFund` has no `PEFundInvestment` rows |
| `duplicate_firm_name` | ERROR | Two `PEFirm` rows with same normalized name |

### Site Intelligence checks

| Check | Severity | What |
|-------|----------|------|
| `no_score` | ERROR | `SiteIntelCollectionJob` completed but no score recorded |
| `stale_job` | WARNING | Last successful job >60 days old for tracked site |
| `low_coverage` | WARNING | <3 datacenter suitability factors scored |
| `score_outlier` | WARNING | Score >2σ from mean for same market |

### 3PL checks

| Check | Severity | What |
|-------|----------|------|
| `no_website` | WARNING | `three_pl_company.website` is null |
| `no_employees` | INFO | `employees` is null |
| `stale_enrichment` | WARNING | `updated_at` > 90 days |
| `no_hq` | WARNING | `hq_city` + `hq_state` both null |

---

## Critical Files

| File | Action |
|------|--------|
| `app/core/data_quality.py` | **DELETE** (dead code) |
| `app/core/data_quality_service.py` | **RENAME** → `dq_engine.py`, rename class → `DataQualityEngine` |
| `app/services/data_quality_service.py` | **RENAME** → `people_dq_service.py`, rename class → `PeopleDQService` |
| `app/core/dq_base.py` | **CREATE** — `BaseQualityProvider`, `QualityReport`, `QualityIssue` |
| `app/services/people_qa_service.py` | **MODIFY** — extend BaseQualityProvider, adopt 4-dimension score |
| `app/services/pe_dq_service.py` | **CREATE** — PE Intelligence DQ checks |
| `app/services/site_intel_dq_service.py` | **CREATE** — Site Intel DQ checks |
| `app/services/three_pl_dq_service.py` | **CREATE** — 3PL DQ checks |
| `app/api/v1/people_analytics.py` | **MODIFY** — update import paths |
| `app/worker/` or job completion handler | **MODIFY** — wire `_on_job_success` hook |

---

## Implementation Order

1. Phase 1: Delete dead code + rename files (safe, low-risk refactor)
2. Phase 2: Create `dq_base.py`, update `PeopleQAService` to implement `BaseQualityProvider`
3. Phase 3: Wire job completion hook (test with a single source first)
4. Phase 4: Add PE, Site Intel, 3PL providers (each independently testable)

---

## Verification

```bash
# Phase 1 — no broken imports
grep -r "DataQualityValidator\|data_quality_service" app/ | grep -v "__pycache__"

# Phase 2 — people QA still works
curl http://localhost:8001/api/v1/people-analytics/qa-report | python -m json.tool

# Phase 3 — DQ auto-runs after job
# Trigger any people ingestion job, then:
curl http://localhost:8001/api/v1/people-analytics/qa-report | python -m json.tool
# health_score should have updated timestamp

# Phase 4 — new DQ endpoints
curl http://localhost:8001/api/v1/pe-analytics/qa-report | python -m json.tool
curl http://localhost:8001/api/v1/site-intel/qa-report | python -m json.tool
```

---

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| Rename breaks import somewhere we missed | `grep -r` before rename; CI catches at restart |
| Phase 3 hook adds latency to job completion | Run DQ async in background task, not in critical path |
| PE/Site Intel checks surface noisy false positives | Tune thresholds after first run; start with INFO severity |
