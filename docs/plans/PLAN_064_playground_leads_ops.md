# PLAN 064 — Playground Leads-Ops Fast-Follow

**Status:** ⛔ PAUSED — SUPERSEDED BY PLAN_065 (2026-05-16)
**Date:** 2026-05-15
**Builds on:** PLAN_063 (thin MVP shipped, commits 0679341 + fb4e082)
**Approx. effort:** 1 working session (3 specs)

> **Why paused:** This plan optimized a vague top-of-funnel (Slack alerts + digest +
> admin UI for the synthetic-playground free-tier leads) before establishing who
> the paid buyer actually is. The Synthetic Data Playground is being repositioned
> as a demo artifact only — the first paid wedge is a service-led diligence-memo
> product targeting LMM PE / independent sponsors / lenders / industrial operators.
> See `PLAN_065_industrial_diligence_pack.md` for the revenue-first replacement.
> If lead-ops becomes valuable AFTER the paid wedge proves out, the work scoped
> here is still good — just deferred until there's a real funnel worth ops'ing.

## Goal

Close the deferred fast-follow items from PLAN_063 so sales can operate the
playground lead pipeline without reading a CSV by hand. Three deliverables,
each its own spec:

| # | Spec      | Deliverable                                         | Why now                                                  |
|---|-----------|-----------------------------------------------------|----------------------------------------------------------|
| 1 | SPEC_060  | Slack alert on first hot-lead crossing              | Real-time signal — sales reacts within minutes, not days |
| 2 | SPEC_061  | Daily leads-digest email (cron-scheduled)           | Catches everything the Slack hook didn't fire on         |
| 3 | SPEC_062  | `frontend/leads.html` JWT-gated admin UI            | Replaces "download CSV in Excel" MVP workflow            |

## Connected sub-plan flow

The three specs should ship as one connected leads-ops loop, not as isolated
features:

1. **Lead event source:** `LeadService.recompute_intent()` remains the single
   source of truth for intent-score changes and first-hot detection. SPEC_060
   hooks only into the existing `routed_to_sales_at IS NULL -> hot` transition.
2. **Immediate action:** SPEC_060 sends one best-effort Slack alert for that
   first-hot transition. It must not change scoring, block DB commits, or
   re-fire for already-routed leads.
3. **Daily catch-up:** SPEC_061 reads the same `leads` + `playground_runs`
   tables over a trailing 24-hour window. It should summarize all activity,
   including hot leads that did not Slack-alert because Slack was disabled or
   temporarily failed.
4. **Human operating surface:** SPEC_062 displays the same API-backed lead data
   sales sees in the digest, with drill-in history from `playground_runs` and
   links back to public reports.
5. **Access control:** SPEC_062 must also tighten the admin API access path.
   The existing router is JWT-gated but any playground signup can get a JWT, so
   `playground-admin` needs an explicit admin allow-list or admin/tier check
   before exposing lead emails and run history.

Implementation dependency order:

`SPEC_060 first-hot event` -> `SPEC_061 digest queries over the same state` ->
`SPEC_062 admin UI + API guard over the same state`.

## Open architectural decision

A generic `app/core/webhook_service.py` already exists with `format_slack_payload`
+ DB-backed registry. Two choices for the Slack hook:

| Option                          | Pros                                                 | Cons                                                |
|---------------------------------|------------------------------------------------------|-----------------------------------------------------|
| **A. Direct httpx + config URL** | Simplest path. One env var. No DB table.             | Single channel only; harder to add per-event routing later |
| **B. Webhook-service integration** | Multi-channel; HMAC; per-event-type config in DB.    | Heavier — new `WebhookEventType.LEAD_HOT_CROSSED`, DB row to insert, more moving parts |

**Default recommendation:** A. It matches "fast-follow" scope and is trivially
upgradeable later (the call site is one notifier function). If you'd rather wire
B for forward-compatibility, flag it before approving and I'll adjust SPEC_060.

## Plan

### Step 1 — SPEC_060: Slack hot-lead notifier  *(small)*

**Files:**
- `app/core/config.py` — add `slack_webhook_url: Optional[str]` (one field)
- `app/services/leads/slack_notifier.py` — new module with one best-effort
  function callable from the synchronous `LeadService` path:
  ```python
  def notify_hot_lead(lead: dict) -> None:
      """Best-effort POST to Slack incoming-webhook. No-op if URL unset.
      Never raises — caller (LeadService) must not block on notification."""
  ```
- `app/services/leads/lead_service.py` — fire the notifier from
  `recompute_intent` at the exact `stamp_routing=True` branch (post-commit).
  Best-effort try/except with logger.warning on failure — matches the existing
  `_notify_lead_event` precedent in `auth.py`.
- `tests/test_spec_060_slack_notifier.py` — 4 tests:
  T1: URL unset → no HTTP call.
  T2: URL set → POSTs JSON with `text` + `blocks` Slack format, returns None.
  T3: Slack returns 500 → notifier swallows, no exception.
  T4: `recompute_intent` end-to-end (Postgres-backed) → first hot crossing
       calls notifier exactly once; second call (already-hot) doesn't re-fire.

**Acceptance:**
- A lead transitioning into hot for the first time triggers exactly one Slack
  POST when `slack_webhook_url` is set.
- A lead's *subsequent* hot recomputes never re-fire.
- Notifier failure (network error, 4xx, 5xx) never prevents
  `recompute_intent` from succeeding.

### Step 2 — SPEC_061: Daily leads-digest job  *(medium)*

**Files:**
- `app/core/config.py` — three new fields:
  - `digest_enabled: bool` (default `True`)
  - `digest_recipients: Optional[str]` (comma-separated, e.g. `"alex@nexdata.com,sales@nexdata.com"`)
  - `digest_hour_utc: int` (default `13` = 8 AM ET)
- `app/services/leads/digest.py` — new module:
  - `compute_digest_window(db, hours=24) -> dict` (pure query — runs over the
    last `hours` worth of `leads` + `playground_runs`; returns
    `{new_leads, new_hot_leads, total_runs, top_generators, hot_lead_summaries}`)
  - `render_digest_html(window: dict) -> str` (inline-HTML, self-contained,
    same design language as the synthetic_playground template's KPI strip)
  - `async send_daily_digest(db) -> None` — gathers, renders, fans out to
    each recipient via `get_email_service()`. No-op if disabled or no recipients.
- `app/core/scheduler_service.py` — new `register_leads_digest_job(scheduler)`
  using `CronTrigger(hour=settings.digest_hour_utc, minute=0)`. Follows the
  existing `register_daily_quality_snapshots` shape exactly.
- `app/main.py` — register call inside the lifespan, gated on `digest_enabled`.
- `tests/test_spec_061_leads_digest.py` — 6 tests:
  T1: `compute_digest_window` correctly tallies new leads / new hot leads /
      runs / top generators from seeded Postgres rows.
  T2: `compute_digest_window` returns zeros when no rows in the window.
  T3: `render_digest_html` renders the four KPIs + top-N table + valid HTML doc.
  T4: `send_daily_digest` no-ops cleanly when `digest_enabled=False`.
  T5: `send_daily_digest` no-ops cleanly when `digest_recipients` is unset.
  T6: `send_daily_digest` with mocked email provider records one `.sent`
      per recipient and the rendered HTML contains the expected KPIs.

**Acceptance:**
- Scheduler registers exactly one job (id=`leads-daily-digest`) at the
  configured hour when `digest_enabled`.
- Digest covers the trailing 24 h.
- Disable via config → zero scheduled jobs, zero emails, zero queries.

### Step 3 — SPEC_062: `frontend/leads.html` admin page  *(medium)*

**Files:**
- `frontend/leads.html` — single self-contained file matching `playground.html`'s
  visual language (same CSS variables, dark theme, indigo+cyan accents).
  Sections:
  1. **Header strip**: KPI cards — total leads, hot, warm, new last 24h.
  2. **Filter bar**: tier dropdown (all/hot/warm/cold), generator multi-select,
     corporate-only checkbox, date range. Calls `/api/v1/playground-admin/leads`
     with the assembled query.
  3. **Lead table**: id, email (with `mailto:`), domain, tier (color-coded),
     intent_score, total_runs, top generator, first_seen, routed_to_sales_at.
     Sortable by score / first_seen. Click row → drill-in panel.
  4. **Drill-in side panel**: full lead detail from `/leads/{id}` + the
     lead's `playground_runs` history. "Open last report" button (links to
     `/p/<short_code>`).
  5. **Footer**: link to `/api/v1/playground-admin/leads/export.csv` (existing).
  - Auth: reads `nexdata_token` from localStorage (same key as `playground.html`).
    No token → redirect to `/playground.html#login`. 401 from API → clear
    token + redirect.
- `app/api/v1/playground_admin.py` — add an explicit admin gate before returning
  leads data. Use the smallest viable rule for v1: either a
  `playground_admin_emails` config allow-list or an existing admin/tier field if
  the user model already exposes one cleanly.
- `app/core/config.py` — add the admin allow-list setting if no existing role
  mechanism is available.
- `frontend/nginx.conf` — already serves `*.html` from the static root,
  so no nginx change needed.

**Tests:** One small backend test file for the admin gate, plus static HTML
manual verification (same convention as SPEC_058 for frontend-only behavior):
1. Non-admin JWT receives 403 from all `playground-admin` lead endpoints.
2. Admin allow-listed JWT can list leads, view detail, and export CSV.

End-to-end manual verification:
1. Authed admin loads `/leads.html`, sees the leads dashboard populated.
2. A non-admin playground user receives 403 from the admin API and cannot load
   lead data.
3. Filter changes update the table without a page reload.
4. Row click opens the drill-in panel with the run history.
5. Export-CSV link still works.

**Acceptance:**
- All four UI sections render and function on a real auth'd browser session.
- No console errors. No 4xx/5xx from any panel for an authorized admin.
- File self-contained: no external scripts beyond the Chart.js CDN already
  used in `playground.html` (drop Chart.js if not actually needed — the
  initial cut likely doesn't).

### Step 4 — End-to-end verification

- Restart API + frontend.
- Force a lead into hot tier (insert a synthetic run set) → verify Slack
  fires (use a request-bin URL for the smoke test).
- Manually trigger the digest job (`scheduler.run_job(...)` or a one-off
  CLI) → verify email lands in the console-provider log.
- Click through `/leads.html` end-to-end.
- Run full PLAN_063 + PLAN_064 test suite — target: 0 regressions, ≥10 new
  tests passing.

## Out of scope (explicit deferrals)

- Slack webhook signing / IP allowlisting — incoming-webhook URLs are the
  shared secret. Adequate for MVP.
- Multi-channel Slack routing (e.g. enterprise leads vs SMB) — single channel
  for now, can lift to webhook-service later.
- Digest unsubscribe link — internal-only recipients for v1.
- Email-provider HTML rendering polish — relies on the same design system
  blocks the report template uses.
- Lead notes / tagging / pipeline-stage UI — read-only admin for now.

## Risks

- **Slack rate limits** — incoming webhooks are 1 req/sec sustained. A burst
  of 10 hot leads in one minute is fine; tested ceiling, not anticipated.
- **Digest job overlap** — APScheduler with the SQLAlchemy store dedupes by
  `id`, so a restart in the cron window doesn't double-send. Confirmed
  pattern at `scheduler_service.py:575-589`.
- **Admin data exposure** — `leads.html` re-uses `playground.html`'s token, so
  a plain JWT is not enough. SPEC_062 must add a backend admin gate before this
  ships; hiding the page in frontend JavaScript is not sufficient.

## Commits expected

One commit per spec (3 total). Pattern matches the SPEC_059 fence — keep
diffs focused and reviewable.
