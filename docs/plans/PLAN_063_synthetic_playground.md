# PLAN_063 — Synthetic Data Playground (free self-serve top-of-funnel)

**Status:** Approved 2026-05-14 — implementing thin MVP (steps 1–8)
**Spec-first:** each component gets a `/spec` + skeleton tests before code.

## Context

Nexdata's GTM is four enterprise verticals (Underwrite / DCII / IRADev / LitInt) at $200K–$400K ACV with 9–12 month sales cycles (`docs/strategy/PLATFORM_POSITIONING_2026.md`). There is **no top-of-funnel** — no way for a prospect to self-serve, no lead capture, no viral surface. This plan adds a **free, self-serve "Synthetic Data Playground"**: anyone can run Nexdata's synthetic data generators, sign up with email-only passwordless auth, and get results as a shareable self-contained HTML report carrying a "Made with Nexdata" CTA. Every shared report is a growth loop; every signup is a captured, intent-scored lead routed to enterprise sales.

**Why the synthetic generators specifically:** they are the *only* capability that is (a) fully built end-to-end, (b) impressive, and (c) independent of empty data tables. PE Intelligence and Site Intelligence infra exists but their warehouse tables are empty — a "lite vertical" tool would show hollow scores. The synthetic generators (`app/services/synthetic/`) work standalone today and double as a credibility demo for the platform's modeling depth.

**Intended outcome:** a public `/playground` product live behind the existing nginx frontend, a `leads` pipeline visible to sales, and a measurable viral loop (forwarded report → new signup).

## What already exists (reuse — do not rebuild)

- **JWT auth**: `app/users/auth.py` + `app/api/v1/auth.py` — token issuance, refresh, on-demand table creation pattern. Passwordless extends this.
- **Synthetic generators**: `app/api/v1/synthetic.py` + `app/api/v1/synthetic_crowd.py`, services in `app/services/synthetic/`. The playground router *calls* these, never reimplements.
- **Report system**: `app/reports/builder.py` (template registry + `_ensure_table()` with `CREATE TABLE IF NOT EXISTS`), `app/api/v1/reports.py` (`FileResponse` streaming pattern). Produces self-contained HTML. Design helpers in `app/reports/design_system.py`.
- **Public-router precedent**: `app/api/v1/public.py` — a non-auth-gated router that self-gates. Quota dependency mirrors its `APIKeyAuth` pattern.
- **Rate-limit pattern**: `app/auth/api_keys.py` `RateLimiter._increment_bucket` — `ON CONFLICT DO UPDATE` counter pattern (copy the pattern, not the table).
- **Email-domain parsing**: `app/sources/people_collection/email_inferrer.py` `EmailInferrer` — reuse for company-domain derivation.
- **Frontend**: static files in `frontend/` served by `nginx:alpine` on `:3001`, no build tooling. Dark-mode/indigo design language; `pe-demo.html` / `synthetic-validation.html` are the self-contained-page precedent. `localStorage` keys `nexdata_token` / `nexdata_user`.

## Resolved cross-cutting decisions

- **No Alembic.** CLAUDE.md is authoritative ("no migration tool"); the dormant `alembic/` dir is not used. All schema work uses `CREATE TABLE IF NOT EXISTS` / `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` inside service `_ensure_table()` methods or the `main.py` lifespan hook (matches the recent `ingestion_jobs.synthetic_model` precedent).
- **Email provider: Resend.** Pure HTTP API — build on `app/core/http_client.BaseAPIClient` for retry/backoff. Behind an `EmailProvider` ABC so it is swappable; `ConsoleProvider` is the default (local/CI never sends real mail); `MockProvider` for tests.
- **Passwordless = code AND link.** The email contains both a 6-digit code (cross-device) and a one-click magic link (`/playground.html?verify=<token>`). One `login_codes` table backs both.
- **Anonymous-first.** 1 free run per IP, no email. The result — including the shareable URL — is fully shown anonymously (the artifact must spread even from anon runs). The 2nd run triggers the email gate.
- **Report built inline** in the `/run` endpoint (free-tier `n` caps bound latency; existing `/synthetic/*` already run synchronously).
- **Ship at `/playground`**, do not repoint nginx `/` (keeps `index.html` as the authed app).

## Architecture

```
Public:   frontend/playground.html ──> POST /api/v1/playground/run ──┐
                                       (no auth gate; PlaygroundQuota dep)
                                                                      │
          email gate ──> /playground/auth/request + /verify ──> AuthService (passwordless)
                                                                      │
   run ──> generator service ──> ReportBuilder("synthetic_playground") ──> reports table (+short_code)
                              ──> LeadService.record_run ──> leads + playground_runs (+intent score)
                                                                      │
Shared:   GET /p/<short_code> (nginx ──> /api/v1/playground/report/{code}) ──> streams self-contained HTML
Sales:    GET /api/v1/playground-admin/leads + /leads/export.csv (JWT-gated)
```

## Components

### C1 — Passwordless email auth
**Modify** `app/users/auth.py`:
- New table in `AuthService._ensure_tables`: `login_codes(id, email, code_hash SHA-256, token_hash, purpose, expires_at, consumed_at, attempts, request_ip, created_at)`; index `(email, created_at DESC)`.
- `users` idempotent alters: `password_hash` → nullable; `ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'free'`, `signup_source VARCHAR(100)`.
- `request_login_code(email, ip)` — rate-limit (≥3 unconsumed/15min or ≥1/60s → reject); generate 6-digit code + long link token; auto-create `users` row if new; call `LeadService.upsert_from_signup`; call `EmailService.send_login_code`.
- `verify_login_code(email, code)` / `verify_login_token(token)` — newest unconsumed/unexpired row; `attempts` lockout > 5; constant-time compare; on success set `consumed_at` + `is_verified`, issue tokens via **existing** `_create_access_token`/`_create_refresh_token`; call `LeadService.mark_verified`.

**Modify** `app/api/v1/auth.py` — add `async` endpoints `POST /auth/request-code`, `POST /auth/verify-code`, `POST /auth/verify-token`. Always return a neutral message on request (no email enumeration). Returned tokens are JWT-compatible with `verify_token` so playground users are real accounts usable in `index.html`.

**New** `app/services/email/__init__.py` — `EmailProvider` ABC + `ResendProvider` (on `BaseAPIClient`) + `ConsoleProvider` (default) + `MockProvider`; `get_email_service()` factory; `send_login_code(email, code, link)`.

**Modify** `app/core/config.py` — `email_provider="console"`, `resend_api_key`, `email_from`, `playground_base_url`.

### C2 — Lead capture + intent scoring
**New** `app/services/leads/lead_service.py`:
- `leads(id, email UNIQUE, user_id FK, company_domain, company_name, is_corporate_email, signup_source, first_seen_at, last_active_at, total_runs, verified, intent_score, intent_tier, generators_used JSONB, max_n_requested, routed_to_sales_at)`.
- `playground_runs(id, lead_id FK nullable, user_id nullable, generator, n_requested, anon_ip, report_id, created_at)` — append-only event log; indexes `(lead_id, created_at)`, `(anon_ip, created_at)`.
- `derive_company_domain(email)` — split on `@`, normalize via `EmailInferrer`; free-mail blocklist sets `is_corporate_email`.
- `upsert_from_signup`, `record_run`, `mark_verified`, `recompute_intent`, `list_hot_leads`.
- **Intent score** (additive 0–100, pure arithmetic): corporate email +30; verified +15; `+5 × min(total_runs,6)`; any high-value generator (`lp-gp-universe`/`private-financials`/`consumer-crowd`) +15; `max_n_requested ≥ 200` +10. Tiers: ≥70 hot, ≥40 warm, else cold. First crossing into hot sets `routed_to_sales_at`. Runs synchronously inside `record_run`.

### C3 — Free-tier quota / metering
**New** `app/services/quota/quota_service.py`:
- `playground_quota_buckets(subject_type, subject_key, day, run_count, PK(subject_type,subject_key,day))` — `ON CONFLICT DO UPDATE` increment.
- `check_and_consume(...)`, `peek(...)`.

**Modify** `app/core/config.py` — `playground_free_runs_per_day=10`, `playground_anon_runs_per_ip=1`, `playground_pro_runs_per_day=200`.

**New** `PlaygroundQuota` dependency class in `app/api/v1/playground.py` (modeled on `public.py` `APIKeyAuth`): optional Bearer → resolve user+tier (`subject_type='user'`) else `subject_type='ip'` by `request.client.host`; on deny → `429` + `X-RateLimit-*`/`Retry-After` headers + body `{action: "signup_required" | "upgrade"}`; on allow → stash `{subject_*, user, remaining}` on `request.state`.

### C4 — Public playground API router
**New** `app/api/v1/playground.py` — `APIRouter(prefix="/playground", tags=["Synthetic Data Playground"])`:
- `POST /playground/run` (dispatch on `generator`) and/or per-generator routes — each `Depends(PlaygroundQuota())`. Reuse request models imported from `app/api/v1/synthetic.py`; optional lower free-tier `n` caps. Flow: quota consumed → call generator service → `LeadService.record_run` → `ReportBuilder.generate("synthetic_playground","html",...)` → write `report_id` + `short_code` → return `{result, report:{short_code, url_path}, preview_data}` + rate-limit headers.
- `GET /playground/generators` — generator metadata + param schemas (drives the UI).
- `GET /playground/quota` — remaining runs (`QuotaService.peek`).
- `GET /playground/report/{short_code}` — **un-gated**, streams stored self-contained HTML inline (mirror `reports.py` `FileResponse`, `media_type="text/html"`).
- `POST /playground/auth/request` + `/verify` — thin wrappers over C1.

**New** `app/api/v1/playground_admin.py` — JWT-gated: `GET /playground-admin/leads`, `/leads/{id}`, `/leads/export.csv`.

**Modify** `app/main.py` — import both routers; register `playground` **non-auth-gated** (alongside `auth`/`public`); register `playground_admin` with `dependencies=_auth`; add OpenAPI tags.

### C5 — Synthetic-playground report template
**New** `app/reports/templates/synthetic_playground.py` — `SyntheticPlaygroundTemplate` (inherit `ICReportBase`, mirror `macro_sector_brief.py`). `gather_data(db, params)` reshapes `{generator, generator_output, request_params}` into KPIs + chart configs + a ~20-row sample table + a CTA placeholder. `render_html` uses `design_system.py` helpers; includes a stable `<!-- CTA_BLOCK_PLACEHOLDER -->` / `<div id="nexdata-cta">` anchor.
**Modify** `app/reports/builder.py` — import + register `"synthetic_playground"` in the `self.templates` dict; **append** `ALTER TABLE reports ADD COLUMN IF NOT EXISTS short_code VARCHAR(16) UNIQUE` (+ optional `is_public`, `view_count`) to `_ensure_table()`.

### C6 — Frontend: playground + email gate + viewer
**New** `frontend/playground.html` — single self-contained page (matches demo-page convention), JS-toggled views: `landing` (hero + 3 generator cards + how-it-works + vertical footer links) → `generator` (picker + dynamic param form mapping 1:1 to backend request models) → `running` (spinner) → `gate` (email input → "check inbox" with code field; magic link handled via `?verify=` on load, stores `nexdata_token`/`nexdata_user`, auto-resumes pending run) → `result` (summary stats + Chart.js preview + "Open full report" → `/p/<code>` + "Share" copy-link + enterprise CTA). Anonymous `session_id` via `crypto.randomUUID()` in `localStorage`.
**Modify** `frontend/nginx.conf` — add `location ~ ^/p/([A-Za-z0-9]+)$ { proxy_pass http://api:8000/api/v1/playground/report/$1; }` + a `limit_req` zone (~30/min/IP) on it; optional `location = /playground`.

### C7 — Viral CTA (in MVP)
- **CTA block** (content lives in the C5 template, finalized during C5/C6): footer band, headline "Made with Nexdata — this is a synthetic sample. The full platform runs on your real data."; primary "See the platform", secondary "Run your own" → `/playground`; generator-aware vertical deep-link + `?ref=<short_code>` attribution param. Until canonical marketing URLs exist, point at `/playground.html` anchors + contact mailto. This is the core growth mechanism — it ships in the MVP, not deferred.

## Scope — Thin MVP first (steps 1–8)

This plan's first build pass is the **thin MVP**: the complete viral loop end-to-end. The `playground_admin.py` router stays in scope — its `GET /playground-admin/leads/export.csv` endpoint is how sales reads leads until the loop is validated. **Deferred to a separate fast-follow plan:** the `frontend/leads.html` admin page, the daily hot-leads digest job, and the Slack webhook. The CTA block (C7) is *not* deferred — it is the growth mechanism and is folded into C5/C6.

### Build sequence (MVP)

| # | Component | Spec type | Depends on |
|---|-----------|-----------|------------|
| 1 | C1 EmailService (`app/services/email/`) | service | — |
| 2 | C1 Passwordless auth (`auth.py`) | api_endpoint | 1 |
| 3 | C2 LeadService | service | 2 (signup hook) |
| 4 | C3 QuotaService | service | — (parallel) |
| 5 | C5 Report template + CTA block content | report | — (parallel) |
| 6 | C4 Playground router + `playground_admin.py` (CSV export) | api_endpoint | 3, 4, 5 |
| 7 | `main.py` wiring | — | 6 |
| 8 | C6 `playground.html` + nginx (+ CTA wired into result view) | service | 6, 7 |

### Deferred fast-follow (separate plan, after loop is validated)

- `frontend/leads.html` — JWT-gated admin hot-leads table (`GET /api/v1/playground-admin/leads`), sortable, score badges, per-row report links; admin nav link in `frontend/index.html`.
- Daily hot-leads digest job (existing scheduler).
- Real-time Slack webhook on high-threshold leads (existing `app/api/v1/webhooks.py`).

## Verification (end-to-end)

1. `docker-compose up --build -d`; confirm `/api/v1/playground/generators` returns metadata; confirm `login_codes`, `leads`, `playground_runs`, `playground_quota_buckets` tables + `reports.short_code` column exist.
2. **Anon path**: open `http://localhost:3001/playground`, run a generator → result view + shareable `/p/<code>` URL; open `/p/<code>` in an incognito window → self-contained report renders with CTA. 2nd anon run → email gate appears.
3. **Passwordless**: `EMAIL_PROVIDER=console` — request a code, read it from `docker-compose logs api`, verify via code AND via the magic link; confirm `nexdata_token` set and a real `users` row created; pending run auto-resumes.
4. **Quota**: authed free user — 11th run in a day → `429 {action:"upgrade"}`.
5. **Leads/intent**: sign up with a corporate-domain email, run a high-value generator twice → `leads.intent_tier='hot'`, `routed_to_sales_at` set; `GET /playground-admin/leads?tier=hot` returns the row; `GET /playground-admin/leads/export.csv` downloads (this is the sales surface for the MVP).
6. `pytest tests/ -v --ignore=tests/integration/` — all new unit tests green (`MockProvider` injected; generator services monkeypatched in router tests).

## Open decisions / notes

1. **Resend** as the email provider (vs SES/Postmark/SMTP) — swap is a one-class change behind the ABC.
2. **Scope: confirmed thin MVP (steps 1–8).** Full viral loop ships; `leads.html` admin page + digest + Slack are a deferred fast-follow plan.
3. **Canonical marketing/vertical URLs** for the CTA — none exist yet; plan falls back to in-app anchors + mailto.
4. Pre-existing `AuthService.refresh_token()` bug (global newest-token lookup) — passwordless users inherit it; flagged, recommend a separate fix, out of scope here.
5. `pro`/`enterprise` tiers are schema-only placeholders — no billing flow in this plan.
