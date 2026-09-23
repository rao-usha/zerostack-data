# SPEC 128 — Data watchdog and honest health

**Status:** Draft
**Task type:** service
**Date:** 2026-09-23
**Plan:** PLAN_086 ("Now: stop the bleeding")
**Test file:** tests/test_spec_128_data_watchdog_and_health.py

## Goal

The API ingestor fleet was dead for about 159 days and nothing told anyone.
Every signal we had was pull-only, and the ones that existed lied:

- `/health` closes its connection and then reuses it (`main.py:1821-1841`),
  swallows the error, and so reports `worker="unknown"` and `status="healthy"`
  forever. Its queries use `'RUNNING'`/`'SUCCESS'` but `job_queue.status` stores
  lowercase (`models_queue.py:29-34`). It leaks `str(e)` from the driver.
- `JobMonitor.check_alerts` measures staleness as `MAX(created_at)` over all
  statuses (`monitoring.py:268-289`), so a source failing every hour looks fresh.
- `/datasets/freshness` reports any source without an SLA as `"fresh"`
  (`freshness.py:97-102`) and leaves out sources that never succeeded.
- `check_freshness_violations` (`freshness.py:217`) has no caller.

This spec adds a **push** watchdog that runs on a timer, notifies a Slack
webhook from env, pings an external dead-man's switch, and makes the pull
surfaces honest.

## Acceptance Criteria

- [ ] `GET /livez` returns 200 whenever the process is up (no DB call).
- [ ] `GET /readyz` returns 200 when `SELECT 1` works, 503 otherwise.
- [ ] `GET /health` keeps its keys (`status`, `service`, `database`, `worker`),
      reads worker liveness from `worker_heartbeats`, reports queue depth, and
      returns 503 with a generic message when the DB is unreachable. No driver
      error text in any response.
- [ ] A watchdog runs every 15 minutes from APScheduler and evaluates:
  - **worker**: jobs pending in `job_queue` for > N minutes and no
    `worker_heartbeats` row seen within N minutes (default 10).
  - **stalled schedule**: each active schedule whose last *successful* run is
    older than 1.5x its cadence (cadence from cron or frequency). Success is
    read from `ingestion_jobs`, `job_queue` and `raw.source_release`
    (`loaded`) — never from failed rows. A schedule that never succeeded is
    alerted once it is older than the threshold.
  - **failed releases**: `raw.source_release` rows with status `failed`
    updated in the last 24h, one alert per source.
  - **failure spike**: >= N failed `job_queue` rows in the last hour
    (default 5).
  - **SLA**: every `source_freshness_sla` row with `alert_on_violation` whose
    last success is older than `max_age_hours`, or that never succeeded.
    This replaces `check_freshness_violations`, which is deleted.
- [ ] Each alert has a stable key. It notifies once when it opens, re-notifies
      at most every 24h while open (or on escalation to critical), and sends a
      `resolved` notice when the condition clears. State lives in a new table
      `watchdog_alerts`.
- [ ] Delivery: `ALERT_WEBHOOK_URL` gets one Slack-compatible `{"text": ...}`
      POST per run with every change (httpx, 10s timeout). Every alert is also
      logged at ERROR (critical) or WARNING (warning). If the POST fails the
      alert stays un-notified and is retried on the next run.
- [ ] `HEARTBEAT_PING_URL` (healthchecks.io style) is pinged after every
      completed watchdog run, so an external service notices if the whole
      stack dies. Both env vars are optional; unset means a clean no-op.
- [ ] `GET /api/v1/watchdog/status` lists open alerts and the last run;
      `POST /api/v1/watchdog/run` triggers a run.
- [ ] `JobMonitor.check_alerts` staleness uses successful jobs only; never
      succeeded => `critical`; older than 1.5x scheduled cadence => `critical`.
- [ ] `/datasets/freshness` labels no-SLA/no-schedule sources `unknown` and
      includes scheduled/SLA'd/attempted sources that never succeeded.

## Design

### Health (`app/api/health.py`)

The three probes move out of `main.py` into a small router with no prefix, so
`main.py` only swaps the old function for one `include_router`. `/health`
opens one connection and does all its queries inside the `with` block.
Worker state:

| live heartbeats (5 min) | claimed/running jobs | `worker` | `status` |
|---|---|---|---|
| > 0 | > 0 | `active` | healthy |
| > 0 | 0 | `idle` | healthy |
| 0 | — | `unavailable` | degraded if `WORKER_MODE`, else healthy |

A DB failure returns 503, `status="unhealthy"`, `database="unreachable"`.
Worker is deliberately not a readiness condition: the API can serve reads
without a worker.

### Watchdog (`app/services/data_watchdog.py`)

```
run_watchdog(db, now=None, transport=None)
  pg_try_advisory_xact_lock  -> skip if another run holds it
  findings = rules(db, now)            # each rule isolated; a crashing rule
                                       # becomes a `watchdog:rule_error:<rule>` finding
  transitions = reconcile(db, findings, now)   # open / remind / escalate / resolve
  deliver(transitions)  -> log always; POST ALERT_WEBHOOK_URL if set
  mark notified only if delivery succeeded (or no webhook configured)
  commit
  ping HEARTBEAT_PING_URL if set
```

Cadence: `CUSTOM` + cron uses the largest gap between the next four fire
times of the same `CronTrigger` APScheduler uses (UTC); frequencies map to
1h / 24h / 7d / 31d / 92d. Unknown => no stall check.

Alert keys: `worker:none_alive`, `schedule:stalled:<id>`,
`release:failed:<source>`, `queue:failure_spike`, `sla:<source>`,
`watchdog:rule_error:<rule>`.

`watchdog_alerts` columns: `key` (PK), `rule`, `severity`, `status`
(`open`/`resolved`), `message`, `details` JSON, `first_seen_at`,
`last_seen_at`, `last_notified_at`, `resolved_at`, `notify_count`.
New table, created by `create_all` (no Alembic migration, per PLAN_086 rules).

Newly opened `schedule:stalled:*` and `sla:*` alerts are also forwarded to
registered webhook subscribers of `ALERT_DATA_STALENESS`, which is what
`check_freshness_violations` did, but deduplicated.

Registration: `register_watchdog_job()` lives in the watchdog module (keeps
`scheduler_service.py` untouched) and is called from the lifespan next to the
other maintenance jobs; `max_instances=1`, `coalesce=True`.

Env: `ALERT_WEBHOOK_URL`, `HEARTBEAT_PING_URL`,
`WATCHDOG_WORKER_DEAD_MINUTES` (10), `WATCHDOG_FAILURE_SPIKE` (5),
`WATCHDOG_INTERVAL_MINUTES` (15).

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | `test_livez_is_200_without_db` | liveness never touches the DB |
| T2 | `test_readyz_and_health_503_when_db_down` | 503, generic text, no leak |
| T3 | `test_health_reads_worker_heartbeats` (PG) | idle/active/unavailable, queue depth |
| T4 | `test_cadence_from_cron_and_frequency` | cron gaps, frequency map |
| T5 | `test_stalled_schedule_uses_success_only` (PG) | failed runs don't count; never-succeeded alerts |
| T6 | `test_dead_worker_with_pending_jobs` (PG) | alerts only when work is waiting |
| T7 | `test_failed_release_alert` (PG) | one alert per source, last 24h |
| T8 | `test_failure_spike` (PG) | threshold on failed queue rows |
| T9 | `test_sla_violation_and_never_succeeded` (PG) | replaces check_freshness_violations |
| T10 | `test_dedupe_remind_and_resolve` (PG) | fire once, 24h reminder, resolved notice |
| T11 | `test_failed_delivery_is_retried` (PG) | webhook down => re-sent next run |
| T12 | `test_webhook_payload_is_slack_text` | `{"text": ...}` via httpx mock |
| T13 | `test_heartbeat_ping_and_noop_when_unset` | dead-man's ping |
| T14 | `test_monitoring_staleness_success_only` (PG) | never => critical, failed != activity |
| T15 | `test_freshness_unknown_and_never_run` | no SLA => unknown; never-run listed |

## Out of scope

- A consumer-facing `meta.as_of` and the dataset status portal (PLAN_085).
- Consolidating the five overlapping status endpoints.
- Alert routing by severity, or paging integrations beyond one webhook.
- Auth on the new router (SPEC_127 owns router access control).

## Files to Create/Modify

| File | Action |
|------|--------|
| app/api/health.py | Create (`/livez`, `/readyz`, `/health`) |
| app/services/data_watchdog.py | Create |
| app/core/models_watchdog.py | Create (`watchdog_alerts`) |
| app/api/v1/watchdog.py | Create (`/watchdog/status`, `/watchdog/run`) |
| app/main.py | Modify (swap `/health`, register router + tag + job) |
| app/core/database.py | Modify (register model import) |
| app/core/monitoring.py | Modify (success-only staleness) |
| app/api/v1/freshness.py | Modify (`unknown`, never-run; delete dead checker) |
| tests/test_freshness_sla.py | Modify (checker tests move to SPEC_128) |
| tests/test_spec_128_data_watchdog_and_health.py | Create |

## Feedback History

_No corrections yet._
