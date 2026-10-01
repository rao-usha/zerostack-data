# PLAN 090 — One SEC fair-access gate for every NexData process

**Status:** Approved by task (workbench orchestrator, 2026-09-30) · **Spec:** `docs/specs/SPEC_146_sec_rate_gate.md`

## Why (measured 2026-09-30 by the workbench)

The weekday 18:00 UTC `people_daily_sec_check` batch (30 companies, `max_concurrent_companies=5`) in
`nexdata-api-1` hit www.sec.gov with UA `Nexdata Research contact@nexdata.com` and kept the host IP 429'd for hours. Causes found in code:

| Cause | Where |
|---|---|
| Each `BaseCollector` instance has its OWN per-domain limiter at 10 req/s (`RATE_LIMITS["sec_edgar"]`), so 5 concurrent companies = up to 50 req/s from one process | `app/sources/people_collection/base_collector.py`, `config.py` |
| On 429 it sleeps `Retry-After or 60` then retries (tenacity, 3 attempts) per request, per company: a fixed 60 s hammer while blocked | `base_collector.py::_fetch_with_retry` |
| No limiter is shared across the api process and the 6 workers except `SecHttp` (bulk) and `BaseAPIClient` in WORKER_MODE; ~40 other modules call *.sec.gov through bare `httpx` clients | grep `sec.gov` in `app/` (43 files) |
| User-Agent is hard-coded per module (12+ different strings) | same |

## Design

1. `app/core/sec_gate.py` — ONE gate:
   * token bucket **5 req/s, burst 2** (any 1 s window ≤ 7, under SEC's 10) shared by **all** processes;
   * on 429/403 from a SEC host: a global cooldown = max(Retry-After (capped 1 h), exponential backoff 5 s·2^(k-1) with jitter, cap 10 min);
   * circuit breaker: 3 consecutive strikes (any process) → every NexData SEC call paused 10 min; logged once per trip (the process that makes the transition logs it); reset on the next success, logged once;
   * a waiter whose wait would exceed `SEC_GATE_MAX_WAIT` (120 s) gets `SecRateLimited` immediately (no sleeping through a 10-minute breaker, no hammering);
   * `sec_phase_slot()` caps concurrent SEC phases of people jobs per process (`SEC_PHASE_CONCURRENCY`, 2).
2. **Cross-process state lives in Postgres**, in the existing `rate_limit_bucket` table: row `sec.gov` (bucket) and row `sec.gov#breaker` (tokens = strikes, max_tokens = trips, last_refill_at = blocked-until). Justification: every NexData process (api + 6 workers) already shares this DB (Cloud SQL via proxy), the table is the documented cross-worker limiter (CLAUDE.md, SPEC_107), there is no Redis, and a lock file on a Docker-Desktop bind mount is not a reliable cross-container lock. No DDL: rows are inserted on first use (`ON CONFLICT DO NOTHING`). Time comes from the DB clock (`clock_timestamp()`), so containers cannot skew it. Dedicated engine, pool 1+1 (Cloud SQL max_connections=100 is tight, PLAN_089). If the DB is unreachable the gate fails safe to an in-process bucket at 1 req/s (7 processes × 1 < 10).
3. **Coverage:** `install()` patches `httpx.HTTPTransport.handle_request` / `httpx.AsyncHTTPTransport.handle_async_request` once per process (called from `app/main.py` lifespan and `app/worker/main.py`), so every httpx client — all ~40 SEC call sites — is gated without editing each. The patch is a pass-through for non-SEC hosts. `SecHttp` wraps its transport in the gate explicitly (a context variable prevents double counting). The aiohttp people `BaseCollector` calls the gate explicitly. The User-Agent of every SEC request is forced to `SEC_USER_AGENT`.
4. `SEC_USER_AGENT` is one setting; default `NexdataResearch/1.0 (research@nexdata.com; respectful research bot)` (CLAUDE.md). No personal e-mail in code (the old default in `config.py`/`sec_http.py` is removed). Passed through docker-compose.

## Steps
- [x] Spec + failing tests (`tests/test_spec_146_sec_rate_gate.py`)
- [x] `sec_gate.py` (pure state machine, Local + Postgres backends, gate, transport hook, phase slot)
- [x] Wire: config settings, SecHttp, people BaseCollector, SECAgent slot, main.py + worker install, compose env, seed defaults
- [x] Update SPEC_107 T2/T3 to the new contract (UA from setting; Retry-After 3600 is honoured, not cut to 60 s)
- [x] ruff, unit tests, session log
- [x] Adversarial review fixes (stale in-flight strikes/successes, overflow, bounded DB waits + contention != down, minimal writes under the row lock, queue time in budget); T14-T20, T17b

## Deploy
Code is bind-mounted: `docker-compose restart api worker` picks up the code. Setting `SEC_USER_AGENT` in `.env` needs a recreate: `docker-compose up -d --no-deps api worker` (compose v2 honours `deploy.replicas: 6`).
