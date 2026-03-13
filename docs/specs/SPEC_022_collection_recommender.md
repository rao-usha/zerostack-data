# SPEC 022 — Collection Recommendations Engine

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_022_collection_recommender.py

## Goal

Build a collection recommendation engine that analyzes source health scores, freshness gaps, and historical patterns to generate prioritized collection recommendations. Enables smart scheduling and proactive data freshness management.

## Acceptance Criteria

- [ ] `generate_recommendations(db)` returns prioritized list of collection actions
- [ ] Each recommendation includes source, priority (1-10), reason, and action type
- [ ] Action types: collect_now, schedule_retry, investigate, disable
- [ ] `get_optimal_collection_plan(db, max_concurrent)` returns batched collection plan
- [ ] `get_collection_history_stats(db, source_key)` returns historical stats per source
- [ ] Recommendations factor in health score, time since last success, failure patterns
- [ ] All DB queries use parameterized SQL

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_recommendations_stale_source | Stale source gets collect_now recommendation |
| T2 | test_recommendations_failing_source | Repeatedly failing source gets investigate recommendation |
| T3 | test_recommendations_healthy_source | Healthy source gets no urgent recommendations |
| T4 | test_optimal_plan_respects_concurrency | Plan respects max_concurrent limit |
| T5 | test_collection_history_stats | Returns correct success rate and avg rows |
| T6 | test_recommendations_disabled_source | Disabled source gets no collect_now recommendation |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/collection_recommender.py | Create | Recommendation engine |
| tests/test_spec_022_collection_recommender.py | Create | Tests |

## Feedback History

_No corrections yet._
