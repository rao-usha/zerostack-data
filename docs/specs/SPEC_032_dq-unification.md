# SPEC 032 — DQ Framework Unification

**Status:** Draft
**Task type:** service
**Date:** 2026-03-27
**Test file:** tests/test_spec_032_dq_unification.py

## Goal

Unify the fragmented DQ infrastructure: remove dead code, resolve naming conflicts, standardize the quality score formula across all datasets, wire DQ to run automatically after job completion, and extend DQ coverage to PE, Site Intel, and 3PL datasets.

## Acceptance Criteria

- [ ] `app/core/data_quality.py` deleted (DataQualityValidator was never instantiated)
- [ ] `app/services/data_quality_service.py` class renamed to `PeopleDQService` and file to `people_dq_service.py`
- [ ] All imports updated — no references to old paths remain
- [ ] `app/core/dq_base.py` created with `BaseQualityProvider`, `QualityReport`, `QualityIssue`
- [ ] `PeopleQAService` implements `BaseQualityProvider` with 4-dimension score formula
- [ ] `dq_post_ingestion_hook.py` extended to call domain-specific `BaseQualityProvider`
- [ ] `pe_dq_service.py`, `site_intel_dq_service.py`, `three_pl_dq_service.py` created
- [ ] `/api/v1/people-analytics/qa-report` still returns valid data after changes

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_base_quality_provider_abstract | Cannot instantiate BaseQualityProvider directly |
| T2 | test_quality_report_score_formula | 4-dimension score formula computes correctly |
| T3 | test_people_qa_service_implements_base | PeopleQAService.run() returns QualityReport |
| T4 | test_pe_dq_no_deal_date_check | PE check flags deals with null deal_date |
| T5 | test_three_pl_no_website_check | 3PL check flags companies with null website |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/core/data_quality.py` | Delete | Dead code — DataQualityValidator never used |
| `app/services/people_dq_service.py` | Create | Renamed from data_quality_service.py, class → PeopleDQService |
| `app/services/data_quality_service.py` | Modify | Re-export stub for backward compat |
| `app/api/v1/people_data_quality.py` | Modify | Update import to PeopleDQService |
| `app/core/dq_base.py` | Create | BaseQualityProvider, QualityReport, QualityIssue |
| `app/services/people_qa_service.py` | Modify | Implement BaseQualityProvider, 4-dimension score |
| `app/core/dq_post_ingestion_hook.py` | Modify | Call domain providers after job success |
| `app/services/pe_dq_service.py` | Create | PE Intelligence DQ (6 checks) |
| `app/services/site_intel_dq_service.py` | Create | Site Intel DQ (4 checks) |
| `app/services/three_pl_dq_service.py` | Create | 3PL DQ (4 checks) |
