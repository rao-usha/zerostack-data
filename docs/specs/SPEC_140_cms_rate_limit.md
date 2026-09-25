# SPEC_140 — CMS client rate limit, split states, streaming, idempotent reruns

**Type:** bug_fix
**Date:** 2026-09-25
**Trigger:** the nightly batch's `cms` splits hung Docker Desktop three nights
running (2026-09-23..25). cms was pulled from the batch (9d7a562); this spec makes
it safe to put back.

## What was wrong (verified in code)

1. **No rate limiting on paginated calls.** `CMSClient._fetch_with_retry` is not
   behind any limiter; only `fetch_bulk_file` uses the semaphore. A
   `data.cms.gov` distributed bucket exists (`rate_limiter.py`, 1 req/s) but
   `CMSClient` does not inherit `BaseAPIClient`, so it is never consulted.
2. **A new TLS connection per page.** `_fetch_with_retry` opens a fresh
   `httpx.AsyncClient` for every request: ~75 KB pages back to back, each on a new
   connection. Docker Desktop's userspace forwarder logged ~50 in-flight
   connections to the Akamai edge before the engine became unreachable.
3. **Splits ignore their states.** `job_splitter` gives each split
   `config={"states": [...]}`, but `SOURCE_DISPATCH["cms"]` only forwards
   `year/state/limit`, so every split downloaded the whole national dataset
   (~10M rows) in parallel.
4. **Everything held in memory.** `fetch_dkan_data` accumulates all pages before
   the first insert.
5. **Reruns duplicate.** Plain `INSERT` with only a surrogate `id` key.
6. No `User-Agent`, contrary to the project scraping rules.

## Design

- `CMSClient` keeps one pooled `httpx.AsyncClient` (connection limits =
  `max_concurrency`, keep-alive) with the project User-Agent; `close()` closes it.
- Every HTTP request (pages and bulk) goes through `_throttle()`: the semaphore,
  a local minimum interval (`CMS_REQUESTS_PER_SECOND`, default 1.0), and — when
  `WORKER_MODE=1` — the shared `data.cms.gov` token bucket so the limit holds
  across all workers. Limiter failures never block a request (logged).
- `iter_dkan_pages()` async generator yields one lower-cased page at a time;
  `fetch_dkan_data()` is kept for callers and built on it.
- `ingest_medicare_utilization(states=[...])`: iterate states sequentially
  (no `state`/`states` = all 50 states + DC + territories), filter DKAN by
  `Rndrng_Prvdr_State_Abrvtn`, and per state: `DELETE` that state's rows, insert
  page by page, commit once at the end of the state. A failed state rolls back to
  its previous rows. Reruns replace, never duplicate.
- `SOURCE_DISPATCH["cms"]` forwards `states`.
- `limit` keeps its meaning (total records cap, for tests).

## Tests (`tests/test_spec_140_cms_rate_limit.py`)

- T1 one AsyncClient reused across all pages; closed by `close()`.
- T2 every page request passes through the throttle (semaphore + interval).
- T3 local pacing: N requests take ≥ (N-1)/rps (fake clock).
- T4 distributed bucket consulted per request when WORKER_MODE=1 (injected acquirer).
- T5 User-Agent sent.
- T6 429 honours Retry-After.
- T7 `SOURCE_DISPATCH["cms"]` forwards `states`.
- T8 (PG) ingest per state with filter; run twice → same row count (no duplicates);
  a state failing mid-way keeps its previous rows.
- T9 streaming: rows are inserted before the last page is fetched.

## Out of scope

Re-adding cms to the nightly batch (separate decision after a live run);
`cms:hospital_cost_reports` / `cms:drug_pricing` ingest logic beyond the shared
client changes.
