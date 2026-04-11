# SPEC 043 — Data Provenance System

**Status:** Draft
**Task type:** service
**Date:** 2026-04-02
**Test file:** tests/test_spec_043_data_provenance.py

## Goal

Add job-level provenance tracking so the system always knows whether data is real or synthetic. One column on `ingestion_jobs`, a provenance helper for scorers, and synthetic source registration in the source registry. This is the foundation for PLAN_053 Phase 0.

## Acceptance Criteria

- [ ] `ingestion_jobs` table has `data_origin` column (`real` | `synthetic`, default `real`)
- [ ] `IngestionJob` SQLAlchemy model includes `data_origin` field
- [ ] Existing jobs default to `real` — no data loss or breakage
- [ ] `BaseSourceIngestor` (or job creation helpers) accept `data_origin` param
- [ ] Source registry entries include `origin` field (`real` | `synthetic`)
- [ ] Synthetic generators (macro_scenarios, private_company_financials) registered as synthetic sources
- [ ] Provenance helper `get_provenance_for_table(db, table, filter)` returns origin breakdown
- [ ] Scorer responses include `provenance` object with real/synthetic factor counts
- [ ] Source registry API endpoint returns `origin` field per source

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_ingestion_job_data_origin_default | New IngestionJob defaults to `data_origin='real'` |
| T2 | test_ingestion_job_synthetic_origin | Can create job with `data_origin='synthetic'` |
| T3 | test_data_origin_validation | Rejects invalid values (not real/synthetic) |
| T4 | test_provenance_helper_real_only | Returns `real` when all matching jobs are real |
| T5 | test_provenance_helper_synthetic_only | Returns `synthetic` when all matching jobs are synthetic |
| T6 | test_provenance_helper_mixed | Returns `mixed` when jobs have both origins |
| T7 | test_provenance_helper_no_data | Returns `unknown` when no matching data exists |
| T8 | test_source_registry_origin_field | All sources have `origin` field, synthetics marked correctly |
| T9 | test_scorer_provenance_in_response | Scorer response includes provenance breakdown |

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

### Schema change
```python
# app/core/models.py — IngestionJob
data_origin = Column(String(16), nullable=False, default="real")
```
No CHECK constraint in SQLAlchemy (Postgres will enforce via app logic). Default `'real'` means existing rows are safe.

### Provenance helper
```python
# app/services/provenance.py
async def get_provenance_for_table(
    db: Session,
    table_name: str,
    filter_col: str,
    filter_val: str,
) -> str:
    """Returns 'real', 'synthetic', 'mixed', or 'unknown'."""

def build_scorer_provenance(
    factor_origins: dict[str, str],
) -> dict:
    """Build provenance summary from {factor_name: origin} mapping."""
```

### Source registry
Add `"origin": "real"` to all existing entries. Add new entries for synthetic sources with `"origin": "synthetic"`.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/core/models.py` | Modify | Add `data_origin` column to `IngestionJob` |
| `app/services/provenance.py` | Create | Provenance helper functions |
| `app/core/source_registry.py` | Modify | Add `origin` field + synthetic source entries |
| `tests/test_spec_043_data_provenance.py` | Create | All test cases |

## Feedback History

_No corrections yet._
