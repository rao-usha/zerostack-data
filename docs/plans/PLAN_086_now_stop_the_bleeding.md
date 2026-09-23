# PLAN 086 — "Now": stop silent loss and exposure

**Status:** Approved by user 2026-09-23 ("do those things in the now camp")
**Source:** `docs/reviews/2026-09-23_daas_platform_review.md` §5–6, PLAN_085 rev_01
**Hard deadline:** 2026-10-04 (first unattended bulk cycle; marts 10-10)

## Specs (execution order ≠ number order; SPEC_122–126 keep PLAN_085's meaning)

| Spec | Scope |
|---|---|
| **SPEC_121** Schedule & job reliability | Generic `IngestionJob` write-back in the worker (success + failure, keyed on `job_table_id`); orphan sweep for PENDING/RUNNING `IngestionJob` whose queue row is terminal (repairs row 3891 automatically); `_finish_ingestion_job` in `finally` in `bulk_ingest` (discover() raising); scheduler `job_defaults` (misfire_grace_time, coalesce, max_instances=1); `restart: unless-stopped` for api/worker; `depends_on: cloudsqlproxy`; pin proxy image |
| **SPEC_127** Access lockdown | `REQUIRE_AUTH` default true; user `role` (admin/user) with bootstrap path; admin-only on mutating/ops routers; close open register (admin-created or `ALLOW_SIGNUP`); playground tokens cannot pass protected routers; export/preview allowlist + hard deny (users, tokens, api_keys, leads); hash password-reset tokens; API-key scope enforced; auth on GraphQL + job_stream (short-lived token for EventSource); quota on Atlas LLM/Places routes; frontend attaches JWT + remove `authDevBypass`; ports bound to 127.0.0.1; replace MIT `license_info` |
| **SPEC_128** Watchdog & health | Scheduled data watchdog (success-only freshness per scheduled source, worker heartbeats, stalled schedules, failed releases) → Slack/webhook from env + log; external dead-man's ping URL; fix `/health` (closed connection reuse, status casing, `worker_heartbeats`), `/livez` `/readyz` (503); staleness from successful jobs only; "no SLA" → `unknown` not `fresh` |
| **SPEC_129** CI & publish guards | CI: py3.11, `TEST_PG_URL` to service postgres, `alembic upgrade head`, secret scan; fixes: `pe_mart_build` dry_run leak, 13F/Form D missing-member/header guard + zero-row/large-drop publish guard, `pe_firms` strategy total, `float(x) if x` zero coercion, pe_marts summary counting all tiers |
| Ops | Untrack `.kaggle/`, scrub `LAUNCH_READINESS.md`, rotate Cloud SQL password, commit PLAN_085/086 + review, push `main`. Kaggle key rotation: user (web UI only). |

## Method
Four implementers in isolated worktrees (spec-first, tests first, per-spec
throwaway DB `qtest_<n>`), an adversarial reviewer + fixer per spec, then
sequential merge to `main`, full test run, container restart, live verification.
