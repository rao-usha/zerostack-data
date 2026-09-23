# SPEC 126a — Mart input assertions, build ledger, ship gates as code

**Status:** Implemented + review fixes (branches spec-126a, spec-126a-fix; live verification pending)
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
      asserted: the latest release — by the *period its key names*, per
      series — must be `loaded`, and the newest period must have been first
      seen within the input's max age (see Inputs). Upstream marts too: the
      pe_marts firms stage needs the latest real `entity_resolve` build to be
      a recent success. A failed/fetched/discovered latest release, a source with no
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
      latest non-dry-run build is failed or refused (not `busy:` refusals),
      closes `running` rows older than 6 h whose build lock nobody holds
      (dead worker), and warns on a live build running > 6 h. A new
      `partial_job` rule (warning) alerts on `PARTIAL:` queue jobs of the last
      24 h; the `failed_release` rule still alerts on the failed rows.
- [x] One build per mart at a time (advisory lock); a second is refused
      `busy:`. Cancel / timeout of the job rolls the build back and finishes
      the ledger row `failed` "cancelled".
- [x] `GET /api/v1/pe/marts/builds` (user-level: any signed-in user reads)
      lists recent ledger rows, filterable by mart/status.

## Design

### Why `success` + `PARTIAL:` and not a new status

- `job_queue.status` is a non-native enum column (`models_queue.py`); a new
  value breaks every reader that compares statuses, including SPEC_121's
  sweep (`TERMINAL_QUEUE_STATUSES`) and the claim query.
- `IngestionJob` FAILED for a partial run would stop the schedule watermark
  (`last_run_at` advances only on success) and, after 1.5x cadence,
  raise a *stalled* alert although data did land — and the next run retries
  the failed releases anyway (`run_source` skips only `loaded`).
- So: status success (rows landed, watermark advances), `error_message`
  starting `PARTIAL:` on both rows (`ingestion_job_sync.PARTIAL_PREFIX`,
  `is_partial()`), `summary["status"] = "partial"`. The SPEC_121 write-back
  is a no-op on the already-terminal IngestionJob — which is also why it
  never advanced the watermark for bulk runs (pre-existing). The fix round
  makes `bulk_ingest._finish_ingestion_job` advance `last_run_at` itself on
  success (clean or partial), like `mirror_queue_outcome`. Alerts: the new
  `partial_job` watchdog rule reads `PARTIAL:`; `failed_release` still fires
  on the failed release rows.

### Inputs (`app/marts/inputs.py`)

| Mart stage | Inputs (bulk sources) |
|---|---|
| pe_marts: firms | sec_adv_roster |
| pe_marts: adv_private_funds | sec_adv_schedule_d |
| pe_marts: funds | sec_form_d, sec_adv_roster |
| pe_marts: people | sec_form_d, sec_adv_roster |
| entity_resolve: feeds | sec_adv_roster, sec_iapd_feed, sec_13f, sec_form_d, sec_edgar_submissions, sec_insider |
| entity_resolve: bridge | sec_13f, sec_adv_roster |

| pe_marts: firms (upstream mart) | entity_resolve (latest real build must be `success`, ≤ 45 days) |

**Latest release = newest period, not newest discovery.** Every release key
names a period: `2026q3` (quarter end), `01jun2026-31aug2026_form13f` (range
end), `ria:2026-06-01` / `edition:…` / `snapshot:…` (date),
`adv1:2026-06:<upload>` (month). `inputs.release_period` returns (period,
series) where series is the key text before the period (`ria:` / `era:` /
`adv1:` / ``). For the newest period, each series' most recently discovered
release must be `loaded`. Keys with no period fall back to the newest
discovery batch (6 h window). Ordering by `discovered_at` (the first version)
was wrong for backfills: `?since=2019-01-01` discovers old quarters *now*, so
they hid a failed newest quarter (and `MAX(loaded_at)` made it look fresh),
and one bad 2019 zip refused the mart until a new release appeared.

**Staleness = age of the newest period's first sighting** (`discovered_at`,
written once), not `MAX(loaded_at)` over all rows: retrying an old failed
release or reloading any release no longer resets the clock. Max age: daily
sources 7 days, monthly 75, quarterly 150 (a quarter's zip lands weeks after
quarter end, so right before the next one lands the newest was first seen
~4 months ago). A newest period that was discovered long ago and loaded only
recently is (correctly) stale: the publisher has had nothing newer.

The input record keeps `release_keys` (the latest release), `latest_by`,
`first_seen`, `loaded_at`, `older_unloaded`, and every loaded release the mart
reads (`loaded_release_count`, `loaded_release_keys`, newest period first,
capped at 100).

Upstream marts (`check_upstream_mart`): no ledger history → ok with a note
(the tables predate the ledger); latest finished real build not `success`, or
older than the max age → refused. Overridable with
`input_override=["entity_resolve"]`.

### Ledger (`app/marts/build_ledger.py`)

`run_guarded(engine, mart, sources, build, dry_run, ..., upstream, cancel_event)`:

1. open the build connection + transaction, `pg_try_advisory_xact_lock(1260,
   hashtext('mart_build:'||mart))`. Not acquired → row `refused`
   "busy: another <mart> build is running" (`MartBuildBusy`); the watchdog
   ignores busy refusals. Only the lock holder closes older `running` rows
   as abandoned, so a live long build is never marked dead.
2. insert `running` row (own transaction).
3. assert inputs + upstream marts; refused => finish `refused`, raise
   `MartInputRefused`.
4. read the published counts (`tables_before`), `build(conn, checkpoint)`,
   count again, probe published data, evaluate gates against the previous
   successful build — or, before the first one, against `tables_before` —
   then commit / rollback.
5. finish `success` or `failed` (+ gate results, stage counts, error).
   Failure raises `MartGateFailed` / re-raises the build error.

Cancellation: the worker cancels the executor coroutine on cancel/timeout but
cannot stop the build thread. The executor sets a `threading.Event`;
`checkpoint()` (called before every stage and right before commit) also
re-reads the `job_queue` row. Either one → rollback, row `failed`
"cancelled: …", `MartBuildCancelled`.

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
| people_signer_share | form_d_signer / pairs in [0.94, 0.98] | SPEC_119 gate 3 (94–98%, measured 96.3%) |
| people_fund_admin_share | fund_admin / pairs ≤ 0.04 | SPEC_119 gate 3 (≤400 of ~10k) |
| people_platform_fund_share | platform_fund / pairs ≤ 0.01 | SPEC_119 gate 3 (≤100 of ~10k) |
| people_admin_cliff | filings by names with admin share in [0.25, 0.90] ≤ max(150, 0.2% of kept) | SPEC_119 gate 4 (new stat `admin_band_filings`) |
| people_title_shape | published SEC Form D links: 0 empty titles, ≤ 7 values, all unions of Director / Executive Officer / Promoter | SPEC_119 gate 5 (probe on the open tx) |
| people_no_silent_merge | collides_same_firm == 0 | SPEC_119 gate 6 |
| people_links_resolved | link_person_missing == 0 | SPEC_119 two-phase merge |
| drop:&lt;stage&gt;.&lt;count&gt; | ≤ 20% drop vs previous success | review row 17 |
| rows:&lt;table&gt; | published rows ≤ 20% drop vs previous success (first build: vs the rows published before it) | review row 17 |
| floor:&lt;stage&gt;.&lt;count&gt; | first ledgered build only: candidates ≥ 80% of the rows the mart already published | SPEC_119 gate 2 volume, data-relative |

entity_resolve: `feeds_nonempty`, drop gates on feeds total, bridge accepted,
`rows:source_record`, `rows:entities_live`, `rows:bridge`, and
`entity_mass_merge` (merges ≤ max(500, 2% of the previous live entity count)).

Not ported, on purpose:
- SPEC_119's absolute volume bands (9,700–10,200 pairs; firms ≥ 3,100): a
  one-time sanity range that fails as the data grows. The `drop:*` gates
  replace them from the second ledgered build, and the `floor:*` gates plus
  pre-build `rows:*` baseline cover the first one.
- Idempotency (second run inserts/updates 0) and the quarantine walk: code
  properties, not data properties — a second full build inside the build
  transaction would double the lock window. Covered by the SPEC_119 tests on
  every change; the ledger records `code_version`.
- `people_no_silent_merge` stays although `collides_same_firm` is structurally
  0 (the key carries firm_id): it guards a future key change.
- Refusal closure is asserted inside `pe_people_sec.build` (raises).

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
| T22 | `TestReleasePeriod`, (pg) `test_backfill_after_failed_newest_release_still_refuses`, `test_backfill_with_one_bad_old_quarter_does_not_refuse`, `test_old_retry_or_reload_does_not_make_stale_input_fresh`, `test_schedule_d_reupload_of_newest_period` | review R1/R4: period order, first-seen age, loaded keys |
| T23 | (pg) `test_pe_marts_refused_after_refused_entity_resolve`, `test_pe_marts_refused_on_stale_entity_master` | R3 upstream mart |
| T24 | `test_entity_resolve_endpoint_takes_admin_overrides` | R2 |
| T25 | (pg) `test_second_concurrent_build_is_refused_busy` | R5 lock, busy refusal, no false abandon |
| T26 | (pg) `test_cancel_event_rolls_back_instead_of_committing`, `test_queue_row_no_longer_running_aborts_commit`, `test_executor_sets_cancel_event_when_cancelled` | R6 |
| T27 | (pg) `test_watchdog_closes_dead_running_build`, `test_watchdog_partial_job_rule` | R7, partial alert |
| T28 | `TestRestoredGates`, (pg) `test_bulk_partial_advances_schedule_watermark` | SPEC_119 gates, watermark |
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
| app/api/v1/pe_marts.py | Modify (POST /build takes `input_override`, `input_max_age_days`, `gate_override`, `dry_run`) |
| app/api/v1/entity_master.py | Modify (POST /resolve, admin on write, takes the same overrides) |
| app/marts/pe_people_sec.py | Modify (`admin_band_filings` stat) |
| app/main.py | Modify (register router, `_auth`) |
| tests/test_spec_126a_mart_gates_build_ledger.py | Create |

## Out of Scope

- `mart_build_id` on mart rows, change tables, `is_current` from last_seen
  (review rows 16, 20; SPEC_132).
- Chaining marts on loader completion; cadence profiles (SPEC_126b).
- Alerting delivery changes (SPEC_128 owns delivery).

## Feedback History

- 2026-09-23 review (fix round, branch spec-126a-fix): latest release by
  period not discovery; staleness from first sighting; entity_resolve admin
  overrides; upstream entity-master assertion; per-mart advisory lock;
  cancel/timeout rollback; watchdog closes dead `running` rows; SPEC_119
  bands restored and admin-cliff / title gates ported; first-build floors;
  partial alert rule; bulk watermark.

## Post-merge fix (2026-09-23): snapshot sources × SPEC_122

SPEC_122 mints no release when a snapshot is unchanged upstream and bumps
`loaded_at` on the previous release instead. `check_source` aged inputs from
the newest period's `discovered_at`, so seven quiet days would have refused the
mart on current data. For `BulkSource.snapshot` sources only, age is now
measured from `max(first_seen, loaded_at)`; archive sources still age from first
sighting (a reload must not make an old period look fresh). Tests:
`test_snapshot_source_freshness_follows_unchanged_recheck`,
`test_non_snapshot_source_still_ages_from_first_sighting`.
