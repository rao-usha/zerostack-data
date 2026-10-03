# SPEC 160 — Runtime split: one scheduler, by flag and by leader lock

**Status:** Implemented (branch `spec-160`; takes effect on the next `docker-compose restart api worker`)
**Task type:** service
**Date:** 2026-10-03
**Plan:** `docs/plans/PLAN_089_hosted_runtime.md` Phase 1 (first half; the image split and `docker-compose.gcp.yml` are the second half)
**Test file:** tests/test_spec_160_scheduler_leader_runtime_split.py
**Builds on:** SPEC_121 (orphan sweep, misfire grace), SPEC_128 (watchdog, `/health`)

## Goal

Before a cloud stack and the laptop stack (or two Cloud Run API instances) can
share Cloud SQL, exactly one process may run APScheduler. Today every API
process starts the scheduler and re-registers all 35 jobs into the shared
`apscheduler_jobs` store, so a second API process fires every schedule twice,
and every API start fails any job that has been RUNNING for more than 2 hours.

## Measured (2026-10-03, read-only on live)

| Fact | Evidence |
|---|---|
| `apscheduler_jobs` holds 36 jobs, all registered by the API lifespan or by `install_default_bulk_schedules` / `load_all_schedules` | `SELECT id FROM apscheduler_jobs` |
| 5 lifespan jobs (PE portfolio check, PE weekly digest, PE market scan, PE deal sourcing, Les Schwab report) have never been stored: their callables are closures inside `lifespan`, which APScheduler cannot serialize ("This Job cannot be serialized ...") | api log 2026-10-03 01:40:04 |
| The startup stale resolver last fired 2026-03-12. All 53 rows it ever resolved (03-05 → 03-12) had a linked `job_queue` row that was already `failed`, which SPEC_121's orphan sweep now settles every 30 min | `ingestion_jobs` `error_message='Stale job auto-resolved on startup'` joined to `job_queue` |
| 48 connections, 2 "idle in transaction"; one held for > 54 s with last query `SELECT job_queue.id AS job_queue_id, ...` (an ORM refresh of a `JobQueue` row) | `pg_stat_activity`, sampled 6× over 24 s |
| `application_name` is empty on every connection, so sessions cannot be attributed to api/worker | `pg_stat_activity` |

### Cause of the "idle in transaction" sessions

`worker/main.py:execute_job` commits the RUNNING status (which expires the
`JobQueue` object), then reads `job.payload`. That attribute access re-SELECTs
the row and opens a transaction, and the worker then awaits the rate limiter
(up to 60 s) and the executor, which runs for minutes to hours on the same
session. Until the executor's first commit the connection sits "idle in
transaction" holding a snapshot (blocks vacuum, counts against
`max_connections`). Fix: load the row's attributes, then end that read
transaction without expiring the object, before any await.

## Acceptance Criteria

- [x] `RUN_SCHEDULER` (default true) gates every APScheduler registration at
      startup. With it off the process registers nothing, never asks for the
      leader lock, never runs the stale resolver, and `/health` reports
      `scheduler_leader: null`.
- [x] With it on, a process becomes scheduler leader only while it holds
      `pg_try_advisory_lock(804261160)` on a dedicated long-lived AUTOCOMMIT
      connection (`application_name=nexdata-scheduler-leader`). Two processes →
      exactly one leader; the other logs who holds the lock and retries every
      `SCHEDULER_LEADER_RETRY_SECONDS` (default 120).
- [x] The leader re-checks the lock every `SCHEDULER_LEADER_CHECK_SECONDS`
      (default 30). If the connection drops (the server then releases the
      lock) it pauses its scheduler, reports `scheduler_leader: false`, and goes
      back to retrying. Shutdown stops the scheduler first, then releases the
      lock.
- [x] Every process starts APScheduler **paused** so schedule edits made
      through the API (`/schedules`, people/PE/agentic endpoints) still reach
      the shared job store; only the leader resumes it. `POST
      /schedules/start` on a non-leader keeps it paused.
- [x] The startup stale-running resolver runs only when a process is elected
      leader, and only fails RUNNING rows older than 2 h that have **no live
      linked queue row** (a worker still running a 4 h CMS or mart job is left
      alone).
- [x] `DB_POOL_SIZE` (5), `DB_MAX_OVERFLOW` (10), `DB_POOL_RECYCLE` (-1 = off)
      and `DB_APPLICATION_NAME` (`nexdata-api`; worker `nexdata-worker`)
      configure the shared engine.
- [x] The worker no longer holds a transaction open across the rate-limit
      wait and the executor run.
- [x] `/health` and `GET /schedules/status` expose `scheduler_leader`
      (true / false / null).

## Design

### `app/core/scheduler_leader.py` (new)

- `LeaderLock(url, key)` — sync; `try_acquire()`, `is_held()`, `release()`,
  `holder()` (best-effort: pid / application_name / client_addr of the
  holder, for the standby log line). The lock connection is NullPool,
  AUTOCOMMIT, with client TCP keepalives and per-session
  `tcp_keepalives_*` (so the **server** notices a dead leader host in about
  a minute instead of the OS default of 2 h) and `idle_session_timeout = 0`
  (a server-wide idle timeout must not kill the lock session).
  `is_held()` looks for our own granted advisory lock in `pg_locks`; any
  error means the connection is gone, and it is closed.
- `SchedulerLeadership(lock, on_elected, on_demoted, retry_seconds,
  check_seconds)` — the async loop. `start()` makes the first attempt inline
  (so a laptop's single API is leader before it serves requests, as today)
  and then runs the loop as a task. DB calls go through `asyncio.to_thread`.
- `start_scheduler_runtime(register_jobs, ...)` / `stop_scheduler_runtime()` —
  what `main.py` calls. `on_elected` = stale resolver → `register_jobs()` →
  resume the scheduler. `on_demoted` = pause.
- `leader_status()` → True / False / None for `/health`;
  `may_run_jobs()` for `scheduler_service.start_scheduler()`.

The retry interval (120 s) is longer than the check interval (30 s), so a
leader whose connection dropped normally pauses before a standby can take
over. This is a lease-free lock, not consensus: a leader that is alive but
partitioned from Postgres can keep firing in-memory jobs for up to one check
interval. Every job it fires needs the database anyway.

### `app/main.py`

All scheduler registration (bulk/mart schedules, `load_all_schedules`, the
system jobs, watchdog, people/PE/site-intel schedules, queue recovery jobs,
batch, the five PE/report closures, eval runs) moves unchanged into
`_register_scheduled_jobs()`. The lifespan calls `start_scheduler_runtime`;
the stale resolver moves out of the batch-metadata migration block into
`scheduler_leader.resolve_stale_running_jobs`.

### Which in-process loops are leader-only

| Loop | Where | Decision |
|---|---|---|
| All APScheduler jobs: schedules, stuck-job cleanup + SPEC_121 orphan sweep, retry processor, freshness/quality/validation, watchdog + dead-man ping, `reset_stale_queue_jobs`, `cancel_stale_pending_jobs`, batch, eval, people/PE/site-intel | APScheduler | **Leader only.** They write shared state; two copies double-fire. The watchdog ping going quiet when no leader exists is the right alarm. |
| Startup stale-running resolver | lifespan | **Leader only**, on each election, narrowed (above). Kept, not removed: it is the only fast path for in-process (`WORKER_MODE=0`) runs lost with the API that hosted them; `cleanup_stuck_jobs` waits the per-source timeout. |
| PG LISTEN → EventBus bridge | lifespan | **Per process.** Each API process feeds its own SSE clients from its own in-memory EventBus. |
| Rate-limit bucket seeding, dataset_registry mirror, batch-metadata DDL/backfill, migrations | lifespan | **Per process.** Idempotent; migrations already take their own advisory lock. |
| Worker poll loop, heartbeats, liveness | worker | **Per process** (queue claims use `SKIP LOCKED`). |

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `RUN_SCHEDULER` | `true` | `false`: never run APScheduler jobs in this process |
| `SCHEDULER_LEADER_RETRY_SECONDS` | `120` | standby retry interval |
| `SCHEDULER_LEADER_CHECK_SECONDS` | `30` | leader lock re-check interval |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` / `DB_POOL_RECYCLE` | `5` / `10` / `-1` | shared engine pool |
| `DB_APPLICATION_NAME` | `nexdata-api` (`nexdata-worker` in the worker) | `pg_stat_activity.application_name` |

Laptop: nothing to set. GCP compose (PLAN_089 Phase 1b): set `RUN_SCHEDULER`
explicitly on the api; with 3 workers × 6, `DB_POOL_SIZE=3 DB_MAX_OVERFLOW=4`
on workers keeps the worst case under 100 connections.

## Tests

`tests/test_spec_160_scheduler_leader_runtime_split.py`:
flag off → nothing registered / no lock / null status; `start_scheduler`
stays paused off-leader; two lock connections → one leader; release hands
over; terminated lock backend → leader demoted, standby takes over; two
async leaderships → one elected; stale resolver runs only on election and
skips rows with a live queue row; `/health` and `/schedules/status` expose the
field; pool settings reach the engine; `execute_job` holds no transaction
while the executor runs; every `add_job`/`start_scheduler` in `main.py` is
inside `_register_scheduled_jobs`.
