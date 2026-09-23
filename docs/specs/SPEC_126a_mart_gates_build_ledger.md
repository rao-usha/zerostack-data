# SPEC 126a — Mart input assertions, build ledger, ship gates as code

**Status:** Implemented (branch spec-126a; live verification pending)
**Task type:** service
**Date:** 2026-09-23
**Plan:** PLAN_087 wave 1; PLAN_085 §D5; review `docs/reviews/2026-09-23_daas_platform_review.md` rows 17, 18
**Test file:** tests/test_spec_126a_mart_gates_build_ledger.py

## Goal

A mart build must never publish on inputs that failed to load, must never
report success when its own output fails the gates that were run by hand for
SPEC_117-119, and every build must leave a ledger row saying what it consumed
and what happened. A bulk run where some releases failed must not read as a
clean success.

Today:

1. `pe_mart_build` and `entity_resolve` read their inputs with no check of
   `raw.source_release` (0 references in `app/marts`, `app/entities`,
   executors). If the 13F load fails on the 9th, the 10th builds on last
   quarter and reports success (PLAN_085 §D5).
2. The ship gates (SPEC_118: ≥95% of existing links reproduced; the platform
   adviser share; SPEC_119: tier shape, no silent merge, volumes) were run
   once, by hand, from a scratchpad script (log 2026-09-20). Nothing runs them
   on the monthly schedule.
3. Stages commit separately: a failure in `people` leaves `firms` and `funds`
   rebuilt; a failure in `resolve` leaves the feeds and bridge rewritten.
   `entity_resolve dry_run` still writes feeds and the bridge (only `resolve`
   honours it) — the same leak SPEC_129 fixed in `pe_marts`.
4. `bulk_ingest` fails the job only when *every* release fails; one failed
   release out of four reports plain success (`bulk_ingest.py:116-120`).

## Acceptance Criteria

- [x] Alembic `0013_mart_build` (down_revision `0012_access_lockdown`) creates
      `core.mart_build`: id, mart, status (`running|success|failed|refused`),
      dry_run, started_at, finished_at, code_version, inputs, stage_counts,
      gate_results, overrides (jsonb), refusal_reason, error,
      ingestion_job_id, job_queue_id.
- [x] Before building, every input bulk source of the stages that will run is
      asserted: the latest discovery batch of `raw.source_release` rows must
      all be `loaded`, and the newest `loaded_at` must be within the input's
      max age. A failed/fetched/discovered latest release, a source with no
      loaded release, or a stale one => the build is **refused**: ledger row
      `refused` with the reason, job fails with that reason, no stage runs,
      previous mart untouched.
- [x] Admin overrides via payload: `input_override` (true or list of
      sources), `input_max_age_days` ({source: days}), `gate_override` (true
      or list of gate names). Every override used is recorded in the ledger.
- [x] Ship gates as code in `app/marts/gates.py`, evaluated after the build
      and before commit, including tolerance against the previous successful
      (non-dry-run) build: a stage candidate count or published row count that
      drops more than 20% (env `MART_GATE_MAX_DROP`) fails.
- [x] Build-then-verify in ONE transaction: all stages and all gates run on
      one connection; commit only when every gate passes (or is overridden).
      A gate failure rolls everything back, marks the ledger row `failed` with
      the gate results, and fails the job. `dry_run` always rolls back (fixes
      the `entity_resolve` dry-run leak) but is still gated and ledgered.
- [x] Every guarded `pe_mart_build` and `entity_resolve` run writes one ledger
      row (running → success/failed/refused), with the consumed release keys,
      stage counts, published row counts, code version, and job ids.
- [x] Bulk tri-state: loaded>0 and failed>0 => `partial`. The job_queue row
      stays `success` (status enum unchanged), with `error_message`
      `PARTIAL: ...` and progress "Completed with partial failures"; the
      IngestionJob is `success` with `error_message` `PARTIAL: ...`. All
      failed => failed (unchanged).
- [x] Watchdog: a new `mart_build` rule alerts (critical) while a mart's
      latest non-dry-run build is failed or refused. Partial bulk runs alert
      through the existing `failed_release` rule (the failed release rows).
- [x] `GET /api/v1/pe/marts/builds` (user-level: any signed-in user reads)
      lists recent ledger rows, filterable by mart/status.

## Design

### Why `success` + `PARTIAL:` and not a new status

- `job_queue.status` is a non-native enum column (`models_queue.py`); a new
  value breaks every reader that compares statuses, including SPEC_121's
  sweep (`TERMINAL_QUEUE_STATUSES`) and the claim query.
- `IngestionJob` FAILED for a partial run would stop the schedule watermark
  (SPEC_121 advances `last_run_at` only on success) and, after 1.5x cadence,
  raise a *stalled* alert although data did land — and the next run retries
  the failed releases anyway (`run_source` skips only `loaded`).
- So: status success (rows landed, watermark advances), `error_message`
  starting `PARTIAL:` on both rows (`ingestion_job_sync.PARTIAL_PREFIX`,
  `is_partial()`), `summary["status"] = "partial"`. The SPEC_121 write-back
  is a no-op on the already-terminal IngestionJob. The watchdog's
  `failed_release` rule already alerts on the failed release rows of that run.

### Inputs (`app/marts/inputs.py`)

| Mart stage | Inputs (bulk sources) |
|---|---|
| pe_marts: firms | sec_adv_roster |
| pe_marts: adv_private_funds | sec_adv_schedule_d |
| pe_marts: funds | sec_form_d, sec_adv_roster |
| pe_marts: people | sec_form_d, sec_adv_roster |
| entity_resolve: feeds | sec_adv_roster, sec_iapd_feed, sec_13f, sec_form_d, sec_edgar_submissions, sec_insider |
| entity_resolve: bridge | sec_13f, sec_adv_roster |

Max age of the newest `loaded_at`: daily sources 7 days, monthly 75, quarterly
150 (a quarter's zip lands weeks after quarter end and the loaders skip loaded
releases, so the newest load of a quarterly source is legitimately ~4 months
old right before the next quarter lands).

"Latest release" = the latest *discovery batch*: rows discovered within 6 h of
the newest `discovered_at`. `discovered_at` is written once
(`_upsert_discovered` only bumps `updated_at`), so it orders releases by when
the publisher produced them. A batch catches e.g. `ria:2026-06` loaded with
`era:2026-06` failed. Older unloaded releases are recorded
(`older_unloaded`) but do not refuse.

### Ledger (`app/marts/build_ledger.py`)

`run_guarded(engine, mart, stage_inputs, build, gates, dry_run, ...)`:

1. insert `running` row (own transaction). Older `running` rows of the same
   mart (> 6 h) are marked `failed` "abandoned" (a worker died mid-build).
2. assert inputs; refused => finish `refused`, raise `MartInputRefused`.
3. `engine.connect()` + `conn.begin()`; `build(conn)`; count published rows;
   evaluate gates against the previous successful build; commit / rollback.
4. finish `success` or `failed` (+ gate results, stage counts, error).
   Failure raises `MartGateFailed` / re-raises the build error.

The ledger writes use their own transactions so a rolled-back build still
leaves its row. `code_version`: env `NEXDATA_GIT_SHA` / `GIT_SHA`, else the
repo's `.git` HEAD, else `app-0.1.0`.

`run_pe_marts(..., guard=False)` and `run_entity_master(..., guard=False)`
keep their direct-call behaviour for tests and scripts (still one
transaction, still no gates); the worker executors pass `guard=True`.

### Gates (`app/marts/gates.py`)

Each gate returns `{passed, value, threshold, detail, skipped}`. Skipped when
the stage did not run or there is no baseline.

pe_marts (from SPEC_117/118/119's hand-run gates, turned into ratios so they
hold as the data grows):

| Gate | Rule | Origin |
|---|---|---|
| firms_nonempty | firms.candidates > 0 | SPEC_117 |
| adv_undated_share | adv skipped_undated ≤ 1% of candidates | SPEC_118 undated bug (149,507) |
| funds_reproduce_links | agreed / (agreed + disagreed) ≥ 0.95 | SPEC_118 gate a |
| funds_link_rate | linked / candidates ≥ 0.40 | measured 52% (20,490 / 39,148) |
| platform_link_share | adv_platform / linked ≤ 0.40 | measured 32%; AngelList took 31% pre-fix |
| platform_adviser_count | platform advisers ≤ 10 | measured 4, the 21→64 cliff |
| people_signer_share | form_d_signer / pairs in [0.90, 0.99] | SPEC_119 gate 3 (94–98%) |
| people_fund_admin_share | fund_admin / pairs ≤ 0.05 | SPEC_119 gate 3 (≤400 of ~10k) |
| people_platform_fund_share | platform_fund / pairs ≤ 0.02 | SPEC_119 gate 3 (≤100) |
| people_no_silent_merge | collides_same_firm == 0 | SPEC_119 gate 6 |
| people_links_resolved | link_person_missing == 0 | SPEC_119 two-phase merge |
| drop:&lt;stage&gt;.&lt;count&gt; | ≤ 20% drop vs previous success | review row 17 |
| rows:&lt;table&gt; | published rows ≤ 20% drop vs previous success | review row 17 |

entity_resolve: `feeds_nonempty`, drop gates on feeds total, bridge accepted,
`rows:source_record`, `rows:entities_live`, `rows:bridge`, and
`entity_mass_merge` (merges ≤ max(500, 2% of the previous live entity count)).

Not ported: SPEC_119's absolute volume bands (9,700–10,200 pairs) — they were
a one-time sanity range and would fail as the data grows; the drop gates
replace them. The admin-threshold cliff (≤150 filings in the 0.25–0.90 band)
needs a stat `pe_people_sec` does not emit; refusal closure is already
asserted inside `pe_people_sec.build`.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | `test_migration_0013_revision` | revision chain, table + status check |
| T2 | `test_gates_*` (unit) | thresholds, tolerance vs baseline, skip without baseline/stage |
| T3 | `test_code_version_env_then_git` | env wins, git fallback |
| T4 | `test_partial_prefix` | helper |
| T5 | `test_builds_route_is_user_level` | policy of the new route |
| T6 | (pg) `test_refused_on_failed_latest_release` | refusal, ledger row, no stage run |
| T7 | (pg) `test_refused_on_stale_input` | age check |
| T8 | (pg) `test_refused_on_missing_input` | never loaded |
| T9 | (pg) `test_input_override_proceeds_and_is_recorded` | override |
| T10 | (pg) `test_gate_failure_rolls_back` | drop > tolerance → rollback, failed row |
| T11 | (pg) `test_within_tolerance_commits` | success row, release keys, counts |
| T12 | (pg) `test_gate_override_commits` | recorded override |
| T13 | (pg) `test_dry_run_is_gated_and_rolled_back` | dry run ledger row, nothing kept |
| T14 | (pg) `test_build_error_marks_failed` | exception → failed row, rollback |
| T15 | (pg) `test_entity_resolve_guarded` | ledger + refusal for entity_resolve, dry run keeps nothing |
| T16 | (pg) `test_executor_*` | executor passes guard + ids, job fails on refusal |
| T17 | (pg) `test_bulk_partial_*` | partial/all-failed/all-loaded outcomes, worker progress |
| T18 | (pg) `test_watchdog_mart_build_rule` | alerts on latest failed/refused, clears on success |
| T19 | (pg) `test_builds_endpoint_lists_rows` | GET returns ledger rows |
| T20 | (pg) `test_real_pe_stages_guarded` | real firms/ADV/funds stages inside the guarded tx; counts + tolerance on a rerun |
| T21 | (pg) `test_refused_on_partial_discovery_batch`, `test_older_failure_does_not_refuse`, `test_abandoned_running_rows_are_closed`, `test_unguarded_run_keeps_direct_behaviour`, `test_entity_dry_run_keeps_nothing` | batch rule, old failures, dead-worker rows, direct calls, entity dry-run leak |

## Files to Create/Modify

| File | Action |
|------|--------|
| alembic/versions/0013_mart_build.py | Create |
| app/marts/build_ledger.py | Create (ledger, run_guarded, code_version) |
| app/marts/inputs.py | Create (input specs + assertions) |
| app/marts/gates.py | Create |
| app/worker/executors/pe_marts.py | Modify (one tx, guard) |
| app/worker/executors/entity_resolve.py | Modify (one tx, guard) |
| app/worker/executors/bulk_ingest.py | Modify (partial) |
| app/core/ingestion_job_sync.py | Modify (PARTIAL_PREFIX, is_partial) |
| app/worker/main.py | Modify (success message keeps partial) |
| app/services/data_watchdog.py | Modify (mart_build rule) |
| app/api/v1/mart_builds.py | Create (GET /pe/marts/builds) |
| app/api/v1/pe_marts.py | Modify (POST /build takes `input_override`, `gate_override`) |
| app/main.py | Modify (register router, `_auth`) |
| tests/test_spec_126a_mart_gates_build_ledger.py | Create |

## Out of Scope

- `mart_build_id` on mart rows, change tables, `is_current` from last_seen
  (review rows 16, 20; SPEC_132).
- Chaining marts on loader completion; cadence profiles (SPEC_126b).
- Alerting delivery changes (SPEC_128 owns delivery).

## Feedback History

_No corrections yet._
