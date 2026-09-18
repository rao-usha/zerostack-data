# SPEC 103 — PE/People silent data-loss hotfixes (D1–D5, D22)

**Status:** Draft
**Task type:** bug_fix
**Date:** 2026-09-16
**Test file:** tests/test_spec_103_pe_silent_data_loss_hotfixes.py
**Plan:** docs/plans/PLAN_082_pe_rebuild_phase0_phase1.md (Phase 0)

## Goal

Fix six confirmed defects from the 2026-09-16 PE collector review (docs/reviews/2026-09-16_pe_collector_review.md §3). Each one loses or corrupts data without raising an error: people/PE writes fail, leadership changes disappear, and SEC requests are throttled 80× below the intended limit.

## Acceptance Criteria

- [ ] **D1** `pe_firm_people.role_type` is added to existing DBs by an Alembic migration (down_revision = baseline `3fb893199e22`) and by `_apply_schema_migrations` in `app/core/database.py`.
- [ ] **D2** `PEPersister._dispatch_item` isolates each item in a SAVEPOINT. When one item fails, items already flushed in the same phase survive, and a failed item doesn't count toward `persisted`/`updated`.
- [ ] **D3** `_store_changes` stores leadership changes whose `change_type` is a plain string (because of `use_enum_values=True`).
- [ ] **D4** Dedup re-points `company_people.reports_to_id` from a deleted duplicate role to the surviving role. A failed auto-merge rolls back only its own savepoint, so the scan can still queue a review candidate and commit. Both orchestrator post-storage dedup `except` blocks roll back the session.
- [ ] **D5** Concurrent units don't share one Session:
  - `PeopleCollectionOrchestrator.collect_batch` runs each company without the provided session.
  - `LpCollectionOrchestrator` runs each LP on its own session.
  - `BioParserService.parse_all` gives each person its own session.
  - Every per-task session is closed.
- [ ] **D22** The SEC registry limit is 480 req/min, and the in-memory `sec` bucket is 8 req/s.
- [ ] All existing unit tests still pass.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_d1_alembic_migration_adds_role_type | The migration file exists, chains from the baseline, and its upgrade SQL adds `role_type` |
| T2 | test_d1_runtime_alter_list_has_role_type | `_apply_schema_migrations` includes the `pe_firm_people.role_type` ALTER |
| T3 | test_d2_failing_item_does_not_roll_back_earlier_items | A good item is persisted even when a later item's handler adds a row and then raises |
| T4 | test_d2_failed_item_not_counted_as_persisted | Stats show persisted=1, failed=1 |
| T5 | test_d3_store_changes_with_string_change_type | A `leadership_changes` row is created |
| T6 | test_d4_reassign_references_repoints_reports_to | A direct report ends up pointing at the surviving role |
| T7 | test_d4_failed_auto_merge_keeps_session_usable | After an IntegrityError inside `_auto_merge`, the scan still creates a candidate and commits |
| T8 | test_d5_collect_batch_does_not_share_provided_session | Each `collect_company` call inside `collect_batch` sees `_provided_session is None` |
| T9 | test_d5_bio_parse_all_uses_session_per_person | `parse_person` receives a distinct session per person, and each one is closed |
| T10 | test_d5_lp_runner_uses_session_per_lp | `_collect_single_lp` runs on a distinct session per LP, and each one is closed |
| T11 | test_d22_sec_rate_limits | Registry allows 480/min; limiter allows 8.0 rps |

## Rubric Checklist

_No bug_fix rubric exists in memory/rubrics; generic checklist:_
- [ ] Root cause fixed, not just the symptom
- [ ] A regression test fails before the fix and passes after
- [ ] No behavior change outside the defect
- [ ] Errors are logged, not swallowed silently
- [ ] Parameterized SQL only

## Design Notes

- **Savepoints:** use `with self.db.begin_nested():` around handler + flush. On an exception, restore a stats snapshot (the handlers increment counters before flushing), clear the caches, and bump `failed`.
- **Session factory for concurrent tasks:** a `session_factory` argument (default `get_session_factory()`), so tests can inject one.
- **People orchestrator:** `collect_batch` uses `copy.copy(self)` with `_provided_session=None`. Each company then opens and closes its own session through the existing `_get_session` / `finally` logic.
- **LP runner:** each LP uses a shallow copy with `db = factory()`. The LP is re-fetched in that session. Job progress updates stay on `self.db`, in synchronous blocks with no `await` in between.
- **Migration id:** `0001_pe_firm_people_role_type`. It uses `ADD COLUMN IF NOT EXISTS`, so it can run on DBs that already got the column from the runtime ALTER.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| alembic/versions/0001_pe_firm_people_role_type.py | Create | D1 migration |
| app/core/database.py | Modify | D1 runtime ALTER |
| app/sources/pe_collection/persister.py | Modify | D2 savepoint per item |
| app/sources/people_collection/orchestrator.py | Modify | D3, D4 rollback, D5 collect_batch |
| app/services/dedup_service.py | Modify | D4 reports_to re-point + savepoint |
| app/sources/lp_collection/runner.py | Modify | D5 session per LP |
| app/services/bio_parser_service.py | Modify | D5 session per person |
| app/core/api_registry.py, app/core/rate_limiter.py | Modify | D22 |
| tests/test_pe_persister.py | Modify | Make `test_item_failure_isolation` actually raise |

## Feedback History

_No corrections yet._
