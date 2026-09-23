# SPEC 125 — Dataset status page

**Status:** Implemented (branch spec-125; browser check pending)
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
| `GET /api/v1/pe/marts/builds?mart=` | user | row expansion for `job:pe_mart_build*` / `job:entity_resolve` |
| `GET /api/v1/bulk/releases?source=` | admin | row expansion for `bulk:*` |
| `GET /api/v1/jobs?source=` | admin | row expansion: recent runs (`bulk:`/`job:`/`dispatch:` producers) |
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
every 5 minutes. No other timers.

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

Browser tests (Playwright) are not run: Playwright is not installed here.

## Files

| File | Action |
|---|---|
| frontend/status.html | Create |
| frontend/index.html | Modify (dead panel, hero card, nav link) |
| tests/test_spec_125_dataset_status_page.py | Create |

## Out of scope

Run / pause buttons (admin, later spec), a light theme (the console has
none), per-dataset `dataset_key` filter on `/jobs` (recent runs filter
`config.dataset` client-side), collector run history (site_intel queue rows
carry no per-dataset key the page can query).
