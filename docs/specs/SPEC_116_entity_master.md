# SPEC 116 — Entity master and CIK↔CRD bridge

**Status:** Draft
**Task type:** service
**Date:** 2026-09-18
**Test file:** tests/test_spec_116_entity_master.py
**Plan:** docs/plans/PLAN_083_entity_master_pe_marts.md

## Goal

Give the loaded SEC data a join layer. Today nothing links a Form ADV adviser (CRD) to its EDGAR filings (CIK), so PE firms can't be assembled from SEC sources. Port the workbench's identifier-only resolver into nexdata, feed it the PE-relevant records, and publish an entity master plus a CIK↔CRD bridge.

Identifier-only by design: EIN, CIK, CRD, LEI, UEI. No fuzzy name matching (the review's "identifiers before names"; name matching is what produced "Investor Relations" as a person linked to 4 firms).

## Acceptance Criteria

- [ ] `app/entities/norm.py` — ported verbatim from wildcard-workbench (pure stdlib): `norm`, `core`, `clean_ein`, `state2`, `zip5`, `phone10`, `domain`, `NAME_NORM_VERSION`. Its self-test passes unchanged.
- [ ] `app/entities/resolve.py` — ported pure functions (`_UF`, `_extract_keys`, `plan`, `canonical_row`, `assign_ids`, `parse_veto_target`) plus the workbench's self-test, with I/O rewritten for SQLAlchemy and the `core` schema:
  - strong keys canonicalized (CRD/CIK digits-only, leading zeros stripped; degenerate repeated-digit values rejected)
  - components of ≥2 records become entities; singletons are counted, not written
  - `assign_ids` reuse / merge / split rules kept, including entity revival through `last_members`
  - `canonical_row` agreement-only promotion, conflicts recorded in `field_conflicts`
  - fan-out caps (`KEY_FANOUT_CAP`, `COMPONENT_RECORD_CAP`) with refusals counted in the run ledger
  - writes guarded by `IS DISTINCT FROM`, so an unchanged re-run reports 0 changes
- [ ] `app/entities/cik_crd_bridge.py` — tier 1 `cover_page_crd` and tier 2 `other_manager_crd` from `sec_13f_filings`, tier 3 `name_state` only for names unique in `sec_filers`. Ambiguity inside or across tiers is refused and recorded. `crd_cik_count` kept; the relation stays many-to-many.
- [ ] `app/entities/feeds.py` — projects records into `core.source_record`:
  - `adv:<crd>` from the latest ADV roster snapshot per CRD
  - `iapd:<crd>` from the IAPD feed
  - `f13:<cik>` from `sec_13f_filings` carrying **both** cik and crd (the identifier-grade bridge evidence)
  - `formd:<cik>` from `form_d_issuers`
  - `edgar:<cik>` from `sec_filers`, restricted to CIKs referenced by the other feeds, `sec_8k_index` or `sec_insider_owners`
  - CRDs unpadded, CIKs 10-digit padded, all-zero placeholders dropped, names case-folded
- [ ] Alembic `0008_entity_master`: schema `core` with `source_record`, `entity`, `identifier`, `alias`, `key_veto`, `entity_merge`, `resolve_run`, `cik_crd_bridge`, `cik_crd_bridge_refused`.
- [ ] Worker job type `entity_resolve` + executor running feeds → bridge → resolve in a thread, reporting counts.
- [ ] API: `POST /api/v1/entities/master/resolve`, `GET /api/v1/entities/master/stats`, `GET /api/v1/entities/master/by-id/{id_type}/{value}`.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_norm_selftest_passes | The ported norm self-test runs clean |
| T2 | test_resolve_selftest_passes | The ported resolver self-test (plan/assign_ids rules) runs clean |
| T3 | test_strong_key_canonicalization | Zero-padded CRD == unpadded CRD; all-zero and repeated-digit values rejected |
| T4 | test_plan_links_records_sharing_identifier | Two records sharing a CRD form one component; unrelated records stay singletons |
| T5 | test_plan_respects_veto | A vetoed key stops the merge |
| T6 | test_assign_ids_reuse_merge_split | Entity id reused, merged (oldest survives), split (largest fragment keeps id) |
| T7 | test_canonical_row_conflicts | Disagreeing values → NULL + field_conflicts |
| T8 | test_feeds_normalize_identifiers_pg | (PG) CRD unpadded, CIK padded, placeholders dropped, latest roster snapshot only |
| T9 | test_resolve_end_to_end_pg | (PG) adviser + 13F filer + Form D issuer resolve into one entity with both cik and crd |
| T10 | test_resolve_is_idempotent_pg | (PG) second run writes nothing (0 entities/memberships changed) |
| T11 | test_bridge_tiers_and_refusals_pg | (PG) cover-page tier wins; ambiguous CRDs refused with a reason; name tier only for unique names |
| T12 | test_executor_and_endpoints | Job runs feeds→bridge→resolve in a thread; stats and by-id endpoints return the expected shape |
| T13 | test_migration_0008_chain | 0008 revises 0007 and creates the core schema |

## Rubric Checklist

_No service rubric in memory/rubrics; generic:_
- [ ] Identifier-only matching; no silent name merges
- [ ] Idempotent re-runs proven by test
- [ ] Refusals and caps counted, never silently dropped
- [ ] Parameterized SQL; identifiers quoted via `safe_sql.qi`

## Design Notes

- The port keeps the workbench's separation: pure planning functions (unit-testable, no DB) vs a thin I/O layer. The self-tests come across unchanged, which is the cheapest possible proof the port didn't change behavior.
- The `f13` feed is what makes the bridge mostly redundant: a 13F cover page carries CIK and CRD on the same record, so the union-find merges adviser and filer directly. The bridge stays because it also covers other-manager CRDs and gives an auditable tier/evidence trail.
- Scope is the PE-relevant subset (~200k orgs), not all 985k filers.

## Files to Create/Modify

| File | Action |
|------|--------|
| app/entities/{__init__,norm,resolve,cik_crd_bridge,feeds}.py | Create |
| alembic/versions/0008_entity_master.py | Create |
| app/worker/executors/entity_resolve.py | Create |
| app/api/v1/entity_master.py | Create |
| app/core/models_queue.py, app/worker/main.py, app/main.py | Modify (job type, executor, router) |
| tests/test_spec_116_entity_master.py | Create |

## Feedback History

_No corrections yet._
