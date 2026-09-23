# SPEC 125 — Dataset status page

**Status:** Implemented (branch spec-125, review fixes on spec-125-fix; jsdom-checked, real-browser check pending)
**Task type:** report (frontend page)
**Date:** 2026-09-23
**Plan:** PLAN_087 wave 2; PLAN_085 §6 (the portal page) + rev_01 (v1 read-only); review `docs/reviews/2026-09-23_daas_platform_review.md` §4 / §6 ("read-only public view plus operator view; delete the dead panel")
**Builds on:** SPEC_124 (`GET /datasets/status`), SPEC_123 (`GET /catalog` rights), SPEC_126a (`GET /pe/marts/builds`), SPEC_122 (`superseded` releases), SPEC_127 (`frontend/js/auth.js`, stream tokens, admin-only routers)
**Test file:** tests/test_spec_125_dataset_status_page.py

## Goal

One page that answers "is each dataset current, and if not, why?" from the
SPEC_124 API, with the finding up front: how many datasets nothing is
collecting (dormant + never_run + stalled). Read-only: no run, pause or
schedule controls (PLAN_085 rev_01 decision 4/5).

## Page — `frontend/status.html`

Standalone static HTML, no build step, dark palette and classes copied from
`index.html` (`.card`, `.stat-card`, `.job-status-badge`, `.ds-table`,
`.health-bar-*`). The console is dark-only, so is this page. Loads
`/js/auth.js` first: every `/api` fetch carries the JWT and a 401 sends the
browser to the console login and back.

### Data

| Call | Access | Used for |
|---|---|---|
| `GET /api/v1/datasets/status` | user | everything on the page (one call renders it) |
| `GET /api/v1/catalog` | user | rights badge (`rights.effective_redistribution`), SLO, rerun, description |
| `GET /api/v1/pe/marts/builds?mart=` | user (error text: admin) | row expansion for `job:pe_mart_build*` / `job:entity_resolve` |
| `GET /api/v1/bulk/releases?source=` | admin | row expansion for `bulk:*` |
| `GET /api/v1/jobs?producer=` | admin | row expansion: recent runs (`bulk:`/`job:`/`dispatch:` producers) |
| `GET /api/v1/job-queue/stream` (SSE, `NexdataAuth.streamUrl`) | admin | live refresh |

Admin-only calls are skipped when the token's `role` is not `admin`, and a
403 renders "admin only" in place — never an error page. No token at all
(REQUIRE_AUTH=false local dev) is treated as the synthetic local admin.

### Layout

1. **Banner** — headline `dormant + never_run + stalled` of `total`; one
   stat card per status (clickable filter, zero counts dimmed); worker line
   (`worker_mode`, live workers); `degraded` stores warning.
2. **Lag strip** — one bar per dataset with an expectation: coverage lag in
   units of its cadence (capped at 3 cadences), coloured by lag (health-bar
   green / yellow / orange / red at 0, <1, <2, >=2 cadences); datasets
   with no expectation are counted, not drawn.
3. **Table** grouped by kind: dataset · kind · status · coverage through ·
   lag · last run · next run (with `next_run_source`) · rows (`~` when
   estimated) · rights badge. Collapses columns at 1150, 900 and 640 px; the
   dataset cell wraps — no horizontal scroll.
4. **Row expansion** (lazy, cached per dataset): status reason, blockers,
   the `can_run` verdict as information ("runs are not started from this
   page"), clocks, tables with counts, cadence + schedule, releases, mart
   builds with gate results and refusal reasons, recent runs.
5. **Filters** — status chips, kind, text search, and a view toggle:
   operator (all datasets) / public (`status_public` ga or beta).

### Live updates

`EventSource(await NexdataAuth.streamUrl('/api/v1/job-queue/stream'))`, a
fresh stream token per (re)connect with exponential backoff. `job_started`,
`job_completed`, `job_failed` (not `job_progress`) mark the producer prefix
of the job type (`bulk_ingest` → `bulk:`, `ingestion` → `dispatch:`,
`site_intel` → `collector:`, other → `job:<type>`) and schedule one debounced
`GET /datasets/status`; only rows whose payload changed are re-rendered
(plus banner and strip), and their cached expansion is dropped. When SSE is
unavailable (non-admin, or three failed connects without an open) the page
falls back to a 90 s reload (`FALLBACK_POLL_MS >= 60000`) and retries SSE
every 5 minutes.

Review fixes (spec-125-fix):
- **WORKER_MODE=0.** `worker.worker_mode === false` in the status body means
  jobs run in-process and emit no queue events: the page does not open SSE
  and polls every 90 s instead (no SSE retry). Re-planned on every refresh.
- **Slow tick (`TICK_MS` = 60 s, always on).** Re-renders every relative
  time (`[data-rel]` spans: "5m ago", "in 2h", "3h overdue"), flags the
  "as of" stamp as stale past `STALE_AFTER_MS` (10 min), retries a failed
  load, and re-fetches when nothing has for `SAFETY_REFRESH_MS` (5 min) —
  also while SSE is live, because time alone moves statuses (stalled, an
  overdue next run), api/APScheduler work emits no queue events, and the
  coverage cache revalidates in the background.
- **Cold coverage cache.** When datasets come back with
  `coverage_error` `deadline`/`timeout`, one re-fetch after 20 s (at most 3)
  picks up the values the server computed in the background.

### Recent runs (review fix)

`GET /jobs?producer=<producer>` (new, admin, SPEC_125) resolves each job's
producer at read time with the same `ProducerMap.producer_for_job` the
status API uses: source candidates are narrowed in SQL (`source IN (...)` or
`<source>:split_%`), then matched in Python, scanning newest first in
batches of 500 up to 5,000 rows. So `dispatch:treasury` no longer lists
`config.dataset='auctions'` jobs (those are `dispatch:treasury:auctions`),
split jobs are included, and history written before `dataset_key` existed
is covered. When the cap is reached first the response carries
`X-Producer-Scan-Truncated: true` and the page says "none among the most
recent jobs" rather than "no jobs recorded".

### Mart build error text (review fix)

`GET /pe/marts/builds` is readable by any signed-in user, so it now treats
free text like SPEC_124 does: `error` is returned (redacted) to admins only
(non-admins get `error: null`, `error_hidden: true`), and `refusal_reason`
and `inputs[].problem` are passed through `redact`. The page renders build
error text only for admins in the operator view.

### Safety

Every string from the API is DB text (reasons, blocker messages, schedule
names, errors): all of it goes through `esc()` before `innerHTML`.

## `index.html` — dead panel and nav

- The Sources dashboard read `avg_coverage_score`, `coverage_depth`,
  `total_records` and `categories` from `/datasets/freshness`, which returns
  none of them (PLAN_085 §2.6), so the "Avg Coverage" card never rendered and
  the coverage-by-category branch was dead. Removed, with the
  `/datasets/freshness` fetch that fed only them. The fourth hero card now
  shows `dormant + never_run + stalled` of `total` from `/datasets/status`
  (fetched after the first render, so it cannot slow the dashboard) and links
  to the status page; until it loads, the old "Categories" count shows. The
  coverage-by-category card keeps its table-count branch (real numbers).
- Nav: a `Status` link appended after the last tab (tab highlighting is
  positional — appending keeps indices 0..8 unchanged).

## Test Plan

| ID | Test | Kind |
|---|---|---|
| T1 | status.html loads `/js/auth.js` before its own script | unit |
| T2 | read-only: no button labelled Run/Pause/Resume, no non-GET `method:`, no `/run` path | unit |
| T3 | every `/api/v1/...` path in status.html matches a FastAPI route (GET) | unit |
| T4 | SSE through `NexdataAuth.streamUrl('/api/v1/job-queue/stream')`; fallback poll >= 60 s; no plain EventSource URL | unit |
| T5 | narrow-width media queries hide columns; no fixed min-width on the table | unit |
| T6 | index.html: no `avg_coverage_score` / `coverage_depth` / `total_records` / `freshnessData`; links `/status.html`; nav link after the last tab button | unit |
| T7 | index.html reads `/datasets/status` for the hero card | unit |
| T8 | node: `StatusCore` helpers — esc, headline count, lag units, job type → producer prefix, grouping order, changed-row diff | unit (skipped without node) |
| T9 | `GET /jobs?producer=`: source candidates; bare dispatch key excludes qualified siblings and includes split jobs; qualified key finds only its jobs; scan cap sets the truncation header; page uses it | unit + pg |
| T10 | node: `relText`, `isStale`, `livePlan` (no SSE when WORKER_MODE=0), `pendingCoverage`; page has the tick / safety refresh | unit |
| T11 | `/pe/marts/builds`: error hidden from non-admins, redacted for admins, refusal/input text redacted; page gates build errors on admin + operator view | unit + pg |
| T12 | jsdom DOM smoke (`tests/js/status_page_smoke.js`): admin (producer queries, truncation note, quiet-SSE tick re-renders times and refreshes, stale flag, debounced event refresh), in-process (no SSE, poll), viewer (no build error, no /jobs), cold coverage catch-up | unit (skipped unless node + jsdom resolve, e.g. NODE_PATH) |

Real-browser tests (Playwright) are not run: Playwright is not installed
here. Column widths at 1150/900/640 px are checked only statically (T5).

## Files

| File | Action |
|---|---|
| frontend/status.html | Create |
| frontend/index.html | Modify (dead panel, hero card, nav link) |
| tests/test_spec_125_dataset_status_page.py | Create |
| tests/js/status_page_smoke.js | Create (review fix) |
| app/api/v1/jobs.py | Modify (review fix: `producer=` filter) |
| app/api/v1/mart_builds.py | Modify (review fix: admin-only, redacted error text) |

## Out of scope

Run / pause buttons (admin, later spec), a light theme (the console has
none), collector / api run history (site_intel queue rows carry no
per-dataset key the page can query), `also_produced_by` producers in recent
runs (the primary producer only), per-dataset event targeting (queue events
carry only job id / type, so an event re-fetches the whole status body and
rows are diffed).
