# SPEC 057 — Playground API Router + Admin Router

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-05-14
**Test file:** tests/test_spec_057_playground_router.py
**Plan:** [PLAN_063](../plans/PLAN_063_synthetic_playground.md) — Component C4 / Step 6

## Goal

Wire the playground together: a public, quota-metered router that runs the synthetic generators, builds a shareable report, captures the lead, and serves the public report URL — plus a JWT-gated admin router that exposes the leads pipeline (the MVP sales surface). This is the keystone that makes Steps 1–5 reachable from the outside.

## Acceptance Criteria

### `app/api/v1/playground.py` — public, non-auth-gated, self-metered

- [ ] `PlaygroundQuota` dependency class (modeled on `public.py` `APIKeyAuth`): reads an optional `Authorization: Bearer` header. If a valid token → resolve the user + tier (`subject_type="user"`, `subject_key=user_id`, limit by tier: free/pro/enterprise from settings, enterprise effectively unlimited). If absent/invalid → `subject_type="ip"`, `subject_key=request.client.host`, limit = `playground_anon_runs_per_ip`. Calls `QuotaService.check_and_consume`. On deny → `429` with `X-RateLimit-*` + `Retry-After` headers and body `{detail, action}` where `action` is `"signup_required"` (anon) or `"upgrade"` (authed). On allow → stashes `{subject_type, subject_key, user_id, email, tier, remaining, reset_ts}` on `request.state.playground_quota` and returns it.
- [ ] `GET /playground/generators` — static metadata for the supported generators (`name`, `label`, `description`, `param_schema`, `high_value`). No quota. Drives the UI.
- [ ] `GET /playground/quota` — `QuotaService.peek` for the caller's subject; never consumes.
- [ ] `POST /playground/run` — body `{generator, params}`, `Depends(PlaygroundQuota())`. Flow: dispatch on `generator` → call the existing generator service (reuse `app/services/synthetic/*` + `synthetic_crowd` model; never reimplement) → normalize the output to the report payload (`{generator, generator_label, request_params, summary, rows}`) → `ReportBuilder.generate("synthetic_playground", "html", payload)` → generate a 10-char alphanumeric `short_code`, set it + `is_public=TRUE` on the report row → `LeadService.record_run(email, user_id, generator, n_requested, anon_ip, report_id)` → return `{result: summary, report: {short_code, url_path: "/p/<code>"}, quota: {remaining, reset_ts}}`. Unknown `generator` → `422`. Generator failure → `502` with a clean message (the quota token is already spent — acceptable for the MVP).
- [ ] `GET /playground/report/{short_code}` — **un-gated**; looks up the report by `short_code` (must be `is_public` + `status='complete'`), increments `view_count`, streams the stored self-contained HTML inline (`FileResponse`, `media_type="text/html"`, no `filename` so it renders in-browser). `404` if missing/not public.
- [ ] Supported generators: `private-financials`, `macro-scenarios`, `consumer-crowd`. The normalizer is defensive — extracts a `summary` from top-level scalar fields and `rows` from the first list-of-dicts in the result; tolerates shape variation without raising.

### `app/api/v1/playground_admin.py` — JWT-gated

- [ ] `GET /playground-admin/leads` — query params `tier`, `min_score`, `since`, `limit`; returns leads via `LeadService.list_hot_leads` (or a broader list when no `min_score`).
- [ ] `GET /playground-admin/leads/{lead_id}` — a single lead + its recent `playground_runs`.
- [ ] `GET /playground-admin/leads/export.csv` — all leads as CSV via `StreamingResponse` (the MVP sales surface).

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_generators_endpoint | `GET /playground/generators` lists the 3 generators with param schemas |
| T2 | test_run_anonymous_first_then_gated | 1st anon run → 200 + report short_code; 2nd → 429 `action=signup_required` |
| T3 | test_run_authed_free_limit | an authed free user gets `playground_free_runs_per_day` runs then `429 action=upgrade` |
| T4 | test_run_unknown_generator | unknown `generator` → 422 |
| T5 | test_run_records_lead_and_report | a run writes a `playground_runs` row and a `reports` row with a `short_code` + `is_public` |
| T6 | test_report_by_short_code_served | `GET /playground/report/<code>` streams the HTML and bumps `view_count`; bad code → 404 |
| T7 | test_quota_endpoint_peek_only | `GET /playground/quota` reports remaining without consuming |
| T8 | test_admin_leads_list_and_csv | `GET /playground-admin/leads` returns rows; `/export.csv` returns CSV content |

## Rubric Checklist

_No `api_endpoint` rubric file exists in `memory/rubrics/` — generic checklist:_

- [ ] Public router self-gates (quota dependency) — registered with **no** `dependencies=_auth`
- [ ] Admin router registered **with** `dependencies=_auth`
- [ ] Reuses generator services, `ReportBuilder`, `LeadService`, `QuotaService` — no reimplementation
- [ ] `short_code` is unguessable (10 random alphanumerics) — integer report ids never exposed publicly
- [ ] Generator failures return a clean `502`, not a stack trace
- [ ] Parameterized SQL only
- [ ] FastAPI `TestClient` integration tests cover the anon→gated boundary, the authed limit, lead+report side effects, public serving

## Design Notes

```python
# app/api/v1/playground.py
class PlaygroundQuota:
    async def __call__(self, request, authorization: Optional[str] = Header(None),
                       db: Session = Depends(get_db)) -> dict: ...

GENERATORS = {  # static metadata; also the dispatch allow-list
    "private-financials": {...}, "macro-scenarios": {...}, "consumer-crowd": {...},
}

def _run_generator(db, generator, params) -> dict          # dispatch -> raw result
def _normalize(generator, raw, params) -> dict             # -> report payload
def _short_code() -> str                                   # 10 alphanumerics
```

Tests use FastAPI `TestClient` against an app that mounts both routers + the
`auth` router (for issuing a real token in T3). They run in-container against
real Postgres; skip without `DATABASE_URL`. Test rows use `spec057-` markers /
a dedicated test IP and are cleaned up.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/api/v1/playground.py` | Create | `PlaygroundQuota`, `/generators`, `/quota`, `/run`, `/report/{short_code}` |
| `app/api/v1/playground_admin.py` | Create | `/leads`, `/leads/{id}`, `/leads/export.csv` |
| `tests/test_spec_057_playground_router.py` | Create | T1–T8 |

`app/main.py` wiring is **Step 7 / a separate task** — not in this spec.

## Feedback History

_No corrections yet._
