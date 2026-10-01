# SPEC_146 — SEC fair-access gate (process-wide + cross-worker)

**Status:** Implemented
**Task type:** bug_fix
**Date:** 2026-09-30
**Plan:** `docs/plans/PLAN_090_sec_fair_access_gate.md`
**Test file:** `tests/test_spec_146_sec_rate_gate.py`

## Goal

Stop NexData tripping SEC's rate limit (measured 2026-09-30: the 18:00 UTC people 8-K batch kept this IP 429'd for hours).
Every request to a `*.sec.gov` host from any NexData process goes through one gate: at most 5 req/s total, Retry-After honoured,
exponential backoff with jitter, a circuit breaker, and one User-Agent setting.

## Acceptance Criteria

- [ ] N concurrent callers (threads and asyncio tasks) never exceed rate + burst in any 1 s window.
- [ ] Two gates sharing one backend (= two processes sharing the DB) together stay under the rate.
- [ ] A 429 with Retry-After: R blocks every caller for at least R seconds; without it, backoff grows exponentially with jitter.
- [ ] A wait longer than max_wait raises `SecRateLimited` without sleeping (no hammering).
- [ ] 3 consecutive 429/403 (each after the previous cooldown ended; 429s from requests already in flight count once) trip the breaker: all calls paused breaker_seconds, logged exactly once; a later success resets it (logged once).
- [ ] Every SEC request carries `SEC_USER_AGENT`; non-SEC hosts pass through untouched.
- [ ] The httpx hook gates every httpx client; SecHttp is gated once (no double counting).
- [ ] The people aiohttp collector uses the gate and no longer sleeps a fixed 60 s on 429.
- [ ] People SEC phase concurrency is capped (`SEC_PHASE_CONCURRENCY`).
- [ ] No personal e-mail in code; blank `SEC_USER_AGENT` falls back to the default.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_concurrent_threads_never_exceed_rate | sliding-window count at most rate+burst, real clock |
| T2 | test_concurrent_async_tasks_never_exceed_rate | same for asyncio |
| T3 | test_two_gates_share_backend | cross-process sharing via one backend |
| T4 | test_retry_after_blocks_all_callers | Retry-After honoured globally |
| T5 | test_backoff_exponential_with_jitter | no fixed interval |
| T6 | test_long_wait_raises_without_sleeping | SecRateLimited fast-fail |
| T7 | test_breaker_trips_once_and_resets | breaker trip / pause / single log / reset |
| T8 | test_transport_hook_gates_and_sets_ua | httpx hook: UA + gate + pass-through |
| T9 | test_sec_http_gated_once | SecHttp counted once |
| T10 | test_people_collector_uses_gate_no_fixed_sleep | aiohttp path |
| T11 | test_phase_slot_caps_concurrency | phase cap |
| T12 | test_user_agent_setting | default, blank falls back, no personal e-mail in app/ |
| T13 | test_postgres_backend_* (TEST_PG_URL) | SQL backend semantics on a disposable DB |
| T14 | test_inflight_success_does_not_close_open_breaker | a success landing during a pause is stale: no reset, no "closed" log; half-open re-trip |
| T15 | test_strikes_during_cooldown_do_not_escalate | in-flight 429s during a cooldown are one event (Retry-After still extends) |
| T16 | test_strike_count_cannot_overflow | backoff exponent capped (no OverflowError -> fallback) |
| T17 | test_lock_contention_is_a_wait_not_a_fallback | lock_timeout/statement_timeout on the gate rows = busy, not backend down |
| T17b | test_postgres_row_lock_held_elsewhere_is_busy_not_down (TEST_PG_URL) | same, real Postgres + psycopg2 LockNotAvailable |
| T18 | test_build_gate_bounds_db_waits | gate engine sets connect_timeout, lock_timeout, statement_timeout, pool_timeout |
| T19 | test_failed_take_writes_nothing_and_grant_writes_bucket_only | fewest round trips under the row lock |
| T20 | test_sync_lock_wait_counts_against_budget | queue time counts toward max_wait (2 s in-loop cap holds in total) |

## Design Notes

See PLAN_090. The state machine is pure (`take_token`, `strike`, `success` on `GateState`); backends only load/store it under a lock
(`threading.Lock` locally, `SELECT ... FOR UPDATE` on two `rate_limit_bucket` rows in Postgres, DB clock).

## Files

| File | Action |
|------|--------|
| app/core/sec_gate.py | Create |
| app/core/config.py | Modify (SEC settings, UA default) |
| app/core/sec_http.py | Modify (use gate) |
| app/core/rate_limiter.py | Modify (seed sec.gov 5/2) |
| app/sources/people_collection/base_collector.py | Modify |
| app/sources/people_collection/sec_agent.py | Modify (phase slot) |
| app/main.py, app/worker/main.py | Modify (install hook) |
| app/ingest/bulk/base.py | Modify (drop session_factory) |
| docker-compose.yml | Modify (SEC_USER_AGENT passthrough) |
| tests/conftest.py | Modify (fast fake gate for every test) |
| tests/test_spec_107_bulk_source_framework.py | Modify (new UA / Retry-After contract) |

## Feedback History

- 2026-09-30 adversarial review: (1) a stale in-flight success during an open breaker reset strikes and logged "closed" while still paused (lost half-open); (2) one burst of in-flight 429s could trip the 10-min breaker at once; (3) 2**strikes overflowed after ~1000 strikes, pushing the process into its own bucket past the shared breaker; (4) SELECT ... FOR UPDATE had no lock/statement/connect timeout, so a stuck transaction could block every SEC caller (and the api event loop) indefinitely, and any lock wait was treated as "backend down" (per-process fallback ignores the shared rate); (5) every take wrote both rows under the lock (4 RTTs at ~40 ms through the Cloud SQL proxy); (6) queue time behind another thread was not counted toward max_wait. All fixed; T14-T20 + T17b.
