# SPEC 160 — Runtime split: one scheduler, by flag and by leader lock

**Status:** Implemented (branch `spec-160`, review fixes on `spec-160-fix`; takes effect on the next `docker-compose restart api worker`)
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
  (the lock is taken before the API serves requests) and then runs the loop
  as a task; the election itself follows after the fence (below), so a
  laptop's single API starts firing jobs about 40 s after boot (SPEC_121
  misfire grace covers anything due in that window). DB calls go through
  `asyncio.to_thread`, each bounded by a timeout.
- `start_scheduler_runtime(register_jobs, ...)` / `stop_scheduler_runtime()` —
  what `main.py` calls. `on_elected` = stale resolver → `register_jobs()` →
  resume the scheduler. `on_demoted` = pause.
- `leader_status()` → True / False / None for `/health`;
  `may_run_jobs()` for `scheduler_service.start_scheduler()`.

**Overlap bound (review fix).** Every lock check is bounded by a timeout
(`min(10 s, check)`); a check that hangs (dead socket after a laptop sleep or
a proxy death) counts as a lost lock: the leader pauses first, then abandons
the connection to a background close. The lock connection also sets
`tcp_user_timeout=30 s` and `statement_timeout=10 s`. A process that takes
the lock **fences**: it waits `check + check timeout` (40 s by default)
before it resolves, registers and resumes, so a previous leader has paused
by then (assuming every host uses the same check settings).
`leader_status()` is `false` while fencing. Residual case, documented not
solved: a process frozen whole (laptop sleep) may fire due jobs on wake
before its next check runs; the schedule active-job guard (SPEC_121) limits
the damage for ingestion schedules.

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
| Startup stale-running resolver | lifespan | **Leader only, first election of a process only**, narrowed (above and below). Kept, not removed: it is the only fast path for in-process runs lost with the API that hosted them; `cleanup_stuck_jobs` waits the per-source timeout. |
| PG LISTEN → EventBus bridge | lifespan | **Per process.** Each API process feeds its own SSE clients from its own in-memory EventBus. |
| Rate-limit bucket seeding, dataset_registry mirror, batch-metadata DDL/backfill, migrations | lifespan | **Per process.** Idempotent; migrations already take their own advisory lock. |
| Worker poll loop, heartbeats, liveness | worker | **Per process** (queue claims use `SKIP LOCKED`). |

### In-process runs and the resolver (review fix)

Ordinary schedules (a plain source, not `bulk:` / `job:`) run **inside the
leader API process** even with `WORKER_MODE=1`:
`run_scheduled_job -> _execute_ingestion_job -> run_ingestion_job`, with no
`job_queue` row; so does the freshness auto-refresh. Which host leads
therefore decides where those ingestions run, and a failover strands the
ones in flight on the old leader (they keep running there if it is alive;
`cleanup_stuck_jobs` settles them if it died). BackgroundTasks runs
(`WORKER_MODE=0`, and the API routes that always use them) are the same:
RUNNING rows with no queue row.

SQL cannot tell such a row of a dead process from one of a live process, so
the resolver now fails a RUNNING row (> 2 h, no live queue row) only if:

- this is the process's **first** election (a re-election after a brief lock
  loss would otherwise fail its own live runs, unblocking the schedule's
  active-job guard and double-running it);
- the row **started before this process booted** (`PROCESS_STARTED_AT`), so
  it cannot be this process's own run;
- **no other process of the same role is connected**: engine connections are
  now named `<role>@<host>:<pid>` (`nexdata-api@…`, `nexdata-worker@…`); if
  any other `nexdata-api@…` session exists, the resolver does nothing and
  leaves real orphans to `cleanup_stuck_jobs` (per-source timeout). A process
  restarted in the same container keeps its `host:pid` name, so a dead
  predecessor's lingering sessions do not block it.

### `idle in transaction`: API in-process paths (review fix)

`run_scheduled_job` and the auto-refresh committed the job (expiring it) and
then read `job.id` inside `_execute_ingestion_job`, which re-SELECTed the row
and held the scheduler's session "idle in transaction" for the whole
in-process run. `_execute_ingestion_job` now reads id/source/config and ends
that read (`database.end_read_transaction`, shared with the worker) before it
awaits.

### `POST /schedules/stop` on the leader

It shuts APScheduler down but **keeps the leader lock**, so no standby takes
over: scheduling stops everywhere until `POST /schedules/start` on that
process (or its restart). That is the intended meaning of "stop"; the
response now says so. To move scheduling to another host, restart (or set
`RUN_SCHEDULER=0` on) the leader instead.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `RUN_SCHEDULER` | `true` | `false`: never run APScheduler jobs in this process |
| `SCHEDULER_LEADER_RETRY_SECONDS` | `120` | standby retry interval |
| `SCHEDULER_LEADER_CHECK_SECONDS` | `30` | leader lock re-check interval; a new leader fences for check + `min(10, check)` s |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` / `DB_POOL_RECYCLE` | `5` / `10` / `-1` | shared engine pool |
| `DB_APPLICATION_NAME` | `nexdata-api` (`nexdata-worker` in the worker) | role part of `pg_stat_activity.application_name` (`<role>@<host>:<pid>`) |

Laptop: nothing to set (docker-compose passes the leader intervals through
with their defaults). GCP compose (PLAN_089 Phase 1b): set `RUN_SCHEDULER`
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

Review fixes (`spec-160-fix`): R1 resolver only on the first election; R2 a
hung check demotes within check + timeout; R3 the fence delays the first
election; R4 no overlap when a fast standby takes over (fake server); R5 lock
connection timeouts; R6 `/schedules/stop` response; R7 (PG) re-election
leaves a live in-process run alone; R8 (PG) resolver skips while another
same-role process is connected; R9 (PG) `run_scheduled_job` holds no
transaction during the run; R10 the auto-refresh helper too; R11 (PG)
statement timeout + abandon frees the lock; R12 (PG) no overlap after the
leader's backend is terminated.

## Not verified / open

- Through the Cloud SQL Auth Proxy the server's TCP peer is the proxy, so
  the per-session `tcp_keepalives_*` may not detect a dead leader host and
  the lock could outlive it (no leader until the proxy session drops). Not
  tested against Cloud SQL; check before cutover (PLAN_089).
- Connection budget (Cloud SQL `max_connections=100`) is guidance only; the
  GCP compose (Phase 1b) must set the pools.
