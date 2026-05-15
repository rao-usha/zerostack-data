# SPEC 058 — Playground Frontend (playground.html + nginx)

**Status:** Draft
**Task type:** service
**Date:** 2026-05-14
**Test file:** _none — static HTML + nginx; verification is the end-to-end browser check in PLAN_063 Step 9._
**Plan:** [PLAN_063](../plans/PLAN_063_synthetic_playground.md) — Component C6 / Step 8

## Goal

The user-facing surface of the Synthetic Data Playground: a single self-contained `frontend/playground.html` (no build tooling — vanilla HTML/CSS/JS, matching the existing dark-mode demo pages) plus one `nginx.conf` location so shared reports resolve at `/p/<short_code>`. This closes the viral loop end-to-end.

## Acceptance Criteria

- [ ] New `frontend/playground.html` — one self-contained file, dark-mode/indigo design language matching `synthetic-validation.html` (CSS vars `--bg:#0f172a --bg-card:#1e293b --primary:#6366f1 --accent:#06b6d4`). Chart.js via the existing CDN.
- [ ] JS-toggled views via a `showView(name)` function: `landing` → `generator` → `running` → `gate` → `result`.
  - **landing**: hero, 3 generator cards (private-financials, macro-scenarios, consumer-crowd), a 3-step "how it works" strip.
  - **generator**: generator picker + a param form whose fields come from `GET /api/v1/playground/generators`.
  - **running**: spinner.
  - **gate**: email input → "check your inbox" → 6-digit code input. Shown when `/run` returns `429 action=signup_required`.
  - **result**: summary KPIs, "Open full report" → `/p/<short_code>`, a "Share" copy-link button, and the enterprise CTA.
- [ ] Anonymous session: on load, generate `crypto.randomUUID()` into `localStorage.nexdata_pg_session` (reserved for future per-session metering; the backend currently meters anon by IP).
- [ ] `POST /api/v1/playground/run` is called with the chosen generator + params; if `localStorage.nexdata_token` exists it is sent as `Authorization: Bearer`.
- [ ] On `429`: `action=signup_required` → show the email gate; `action=upgrade` → show an "upgrade" message.
- [ ] Email gate calls `POST /api/v1/auth/request-code` then `POST /api/v1/auth/verify-code`; on success stores `nexdata_token` + `nexdata_user` in `localStorage` (same keys as `index.html`) and auto-resumes the pending run.
- [ ] Magic-link: on load, if `?verify=<token>` is present, call `POST /api/v1/auth/verify-token`, store the token bundle, strip the param via `history.replaceState`, and resume.
- [ ] `frontend/nginx.conf` gains a `location ~ ^/p/([A-Za-z0-9]+)$` block that proxies to `http://api:8000/api/v1/playground/report/$1` — shared reports open at `/p/<code>` without auth. `/playground.html` is already served by the existing `try_files` rule.

## Verification

End-to-end browser check (PLAN_063 Step 9): open `http://localhost:3001/playground.html`, run a generator anonymously → result view + a working `/p/<code>` link; second run → email gate; with `EMAIL_PROVIDER=console`, read the code from `docker-compose logs api`, verify, confirm the pending run resumes.

## Rubric Checklist

_Generic (frontend / no rubric file):_

- [ ] No build tooling — single static file, CDN libs only (matches existing demo pages)
- [ ] Matches the established dark-mode/indigo design tokens
- [ ] Reuses `localStorage` keys `nexdata_token` / `nexdata_user` so the session carries to `index.html`
- [ ] Graceful error states (run failure, quota messages, email gate errors)
- [ ] No secrets or codes ever rendered client-side beyond what the API returns
- [ ] nginx change is minimal and additive (existing `/` and `/api/` rules untouched)

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `frontend/playground.html` | Create | the entire playground product (landing → generator → gate → result) |
| `frontend/nginx.conf` | Modify | add the `/p/<short_code>` proxy location |

## Feedback History

_No corrections yet._
