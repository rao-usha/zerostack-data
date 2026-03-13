# SPEC 021 — Source Health API Endpoints

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-03-13
**Test file:** tests/test_spec_021_source_health_api.py

## Goal

Expose source health scoring via REST API endpoints so the frontend and ops dashboards can display per-source health, aggregate platform health, and collection recommendations.

## Acceptance Criteria

- [ ] `GET /api/v1/source-health` — returns all source health scores sorted worst-first
- [ ] `GET /api/v1/source-health/summary` — aggregate platform health with tier counts
- [ ] `GET /api/v1/source-health/{source}` — detailed health breakdown for one source
- [ ] `POST /api/v1/source-health/{source}/refresh` — re-calculate and return updated health
- [ ] Router registered in main.py under Collection Management
- [ ] All endpoints use auth dependencies

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_list_all_health | GET /source-health returns list sorted by score |
| T2 | test_health_summary | GET /source-health/summary has tier counts |
| T3 | test_source_detail | GET /source-health/{source} returns full breakdown |
| T4 | test_refresh_source | POST /source-health/{source}/refresh triggers recalculation |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/api/v1/source_health.py | Create | API endpoints |
| app/main.py | Modify | Register router |
| tests/test_spec_021_source_health_api.py | Create | Tests |

## Feedback History

_No corrections yet._
