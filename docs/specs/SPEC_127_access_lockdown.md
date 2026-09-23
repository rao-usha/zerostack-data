# SPEC 127 — Access lockdown

**Status:** Draft
**Task type:** service
**Date:** 2026-09-23
**Plan:** PLAN_086 ("Now: stop silent loss and exposure")
**Source:** `docs/reviews/2026-09-23_daas_platform_review.md` §5 findings 2, 4, 12, 22, 23, 26
**Test file:** tests/test_spec_127_access_lockdown.py

## Goal

Every router is anonymous today. `REQUIRE_AUTH` defaults to `false`
(`main.py:1519`), there is no role model, open `/auth/register` and the
playground's passwordless sign-in both mint a JWT that passes every router, and
`/export/tables/{t}/preview` will return `users.password_hash` and the
**plaintext** `password_reset_tokens.token` to anyone who asks. The job of this
spec is to make "who may do what" explicit and closed by default, without
breaking the playground, Atlas or the operator console.

## Acceptance criteria

- [ ] `REQUIRE_AUTH` defaults to **true**. `REQUIRE_AUTH=false` still works for
      local dev and logs a loud `WARNING` at import/startup.
- [ ] `users.role` exists (`'admin' | 'user'`, default `'user'`), via Alembic
      `0012_access_lockdown`. The JWT carries `role`; authorization re-reads the
      role from the DB on every request so a demotion is immediate.
- [ ] Bootstrap: `ADMIN_EMAILS` (comma list) promotes a *verified* matching
      user to admin at password login and at creation; `scripts/create_user.py
      EMAIL [--password P] [--admin]` creates or updates a verified password
      user so an admin can exist while signup is closed.
- [ ] `require_admin` dependency; mutating/ops routers are admin-only (table
      below). Read-only data `GET`s stay user-level.
- [ ] `/auth/register` returns 403 unless `ALLOW_SIGNUP=true`.
- [ ] Playground (passwordless) tokens carry `aud=nexdata-playground`;
      protected routers only accept `aud=nexdata-app`. The playground keeps
      working (its quota dependency accepts both).
- [ ] `/auth/refresh` looks the refresh token up by its SHA-256 (it previously
      ignored the token and refreshed the most recent session in the table —
      anyone could mint an access token for the last user who logged in).
- [ ] Export/preview/columns/create/list: hard deny list of auth, key, lead
      and quota tables, plus any table with a credential-looking column
      (`password`, `secret`, `token`, `api_key`, `*_hash` of a credential,
      `credential`, `private_key`). Denied tables are invisible (404) and
      filtered from `/export/tables`. The whole export router is admin-only.
- [ ] Password-reset tokens are stored as SHA-256 (`token_hash`) and looked up
      by hash; the migration deletes all existing (plaintext) rows. Passwordless
      accounts cannot use password reset to acquire a password.
- [ ] API keys: scope is enforced (`read < write < admin`; `/public` needs
      `read`, unknown scopes fail closed). Keys are accepted only via the
      `X-API-Key` header; `?api_key=` now returns 401 with a message telling
      the caller to move the key into the header (**breaking** for any client
      using the query string — none are known).
- [ ] GraphQL requires a user-level app token.
- [ ] `/job-queue/*` (SSE + JSON) is admin-only. `EventSource` cannot send
      headers, so `POST /auth/stream-token` (authenticated) issues a 5-minute
      `aud=nexdata-stream` JWT that the job-queue routes accept as
      `?stream_token=`. The token is checked once at connect time.
- [ ] Atlas routes that call an LLM or Google Places (`/plan`, `/explain`,
      `/pilot`, `/pilot/stream`, `/competition`) are metered by the existing
      `QuotaService` under their own subject namespace (`atlas_ip`,
      `atlas_usr`); an admin app token is unmetered.
- [ ] Frontend: shared `frontend/js/auth.js` wraps `fetch` for `/api/` and
      `/graphql` URLs (same origin or `localhost:8001`), attaches the
      `nexdata_token` bearer, and on 401 calls the page's handler or redirects
      to the console login. `authDevBypass` and its "Skip login" link are gone.
- [ ] `docker-compose.yml` host ports bind to `127.0.0.1` (api 8001, postgres
      5434, proxy 5435, frontend 3001) and pass `REQUIRE_AUTH`,
      `ALLOW_SIGNUP`, `ADMIN_EMAILS` through to the api container.
- [ ] OpenAPI `license_info` says "Proprietary — all rights reserved", not MIT.

## Design

### Principals and token audiences

| Token | `aud` | Minted by | Accepted by |
|---|---|---|---|
| App access token | `nexdata-app` | `/auth/login`, `/auth/register` (if enabled), `/auth/refresh` of an app session | every protected router, `get_current_user` |
| Playground token | `nexdata-playground` | `/auth/verify-code`, `/auth/verify-token` | playground quota + `/playground/quota` only |
| Stream token | `nexdata-stream` | `POST /auth/stream-token` (app token required), TTL 5 min | `/job-queue/*` via `?stream_token=` |

Tokens minted before this spec carry no `aud` and are rejected — everyone
signs in once more. Refresh tokens remember their audience
(`refresh_tokens.audience`), so a playground session can never be refreshed
into an app session.

`app/core/authz.py` holds the policy:

- `current_principal` — `REQUIRE_AUTH=false` → a synthetic local-dev admin;
  otherwise a valid `nexdata-app` bearer or 401.
- `require_user` — any authenticated principal, all methods.
- `require_admin` — role `admin` or 403.
- `require_admin_for_writes` — `GET/HEAD/OPTIONS` need a user;
  `POST/PUT/PATCH/DELETE` need admin, except the endpoints named in
  `USER_WRITE_ENDPOINTS` (pure computations exposed as `POST`).
- `require_admin_or_stream_token` — admin via bearer or `?stream_token=`.

The auth setting is read **per request** (`get_settings().require_auth`), not
at import, so every route always carries its dependency and the route-table
test can assert the policy regardless of the environment.

`AuthService._ensure_tables` used to run ~12 DDL statements on every
construction; now that every request authenticates it runs once per process
per database URL.

### Router policy (every `include_router` in `app/main.py`)

| Level | Dependency | Routers |
|---|---|---|
| Public | none | `auth` (login, refresh, request-code/verify, reset; register gated by `ALLOW_SIGNUP`), `playground` (self-gated by `PlaygroundQuota`), `diligence_pack` (intake form), `/`, `/health` |
| Public, metered | `AtlasQuota` on LLM/Places routes | `atlas` |
| API key | `APIKeyAuth(required_scope="read")`, header only | `public` |
| User, all methods | `require_user` | `graphql`, `workspaces` (endpoints also enforce membership) |
| Admin, all methods | `require_admin` | `jobs`, `jobs_monitor`, `schedules`, `webhooks`, `chains`, `rate_limits`, `templates`, `export`, `bulk`, `pe_marts`, `api_keys`, `settings`, `source_configs`, `audit`, `llm_costs`, `playground_admin`, `collection_jobs`, `people_jobs`, `pe_collection`, `evals`, `dq_review`, `import_portfolio`, `pe_import` |
| Admin, stream token allowed | `require_admin_or_stream_token` | `job_stream` (`/job-queue/*`) |
| Reads user / writes admin | `require_admin_for_writes` | every other router (≈150: all data-source routers such as `census_*`, `fred`, `eia`, `sec`, `bls`, …; PE, people, site-intel, deal radar, entity master, data quality, lineage, freshness, source health, search, agents, agentic research, lp/fo collection, …). Their `POST` ingest/collect/compute/build/resolve, all `PUT`/`PATCH`, and all 42+ `DELETE` handlers therefore need admin. |

`USER_WRITE_ENDPOINTS` (POST, no persistence or LLM at the router layer):
`compare.compare_portfolios`, `app_rankings.compare_apps`,
`site_intel_sites.{search_sites, compare_sites, score_site, unified_score,
unified_compare}`, `lineage.compute_impact_analysis`,
`deal_models.run_sensitivity`, `pe_macro.score_lbo_entry`.

A route-table test enumerates `app.routes` and fails if any `/api/v1` or
`/graphql` route outside the public list lacks one of these dependencies, or
if a router in the admin list is not admin-gated. Adding a router without a
policy is therefore a test failure, not a silent hole.

Known limitations (logged, not fixed here): `watchlists`/`people_watchlists`
take the owner from the request body, so their writes stay admin; the
`/jobs/monitor` HTML page cannot be opened by browser navigation under auth
(it is an admin JSON-authenticated route) — use the console's Jobs tab;
`site_intel_sites` SSE streams are user-level GETs with a header, and no
frontend page uses them.

### Export deny list

`app/core/export_policy.py`:

- `DENIED_TABLES` — `users`, `password_reset_tokens`, `refresh_tokens`,
  `login_codes`, `api_keys`, `api_usage`, `api_key_usage`,
  `rate_limit_buckets`, `source_api_keys`, `leads`, `playground_leads`,
  `playground_quota_buckets`, `playground_runs`, `workspace_invitations`,
  `webhooks`, `webhook_deliveries`, `sessions`, `alembic_version`.
- `DENIED_TABLE_PATTERNS` — names containing `password`, `secret`, `token`,
  `credential`, `session`, `lead`, `api_key`.
- `SENSITIVE_COLUMN_PATTERN` — any column named like `password*`, `*secret*`,
  `*token*` (whole word), `api_key`, `key_hash`, `code_hash`,
  `credential*`, `private_key`.

`is_exportable(table, columns)` is checked in `list_tables`,
`get_table_columns`, `preview_table`, `create_export_job` and again in
`execute_export` (defense in depth for jobs queued before the upgrade).

### Migration `0012_access_lockdown`

All statements are guarded by `to_regclass(...)` because these tables are
created lazily by `AuthService`, not by `create_all`, so a fresh database may
not have them yet (the lazy `CREATE TABLE` now includes the new columns).

- `users.role VARCHAR(20) NOT NULL DEFAULT 'user'` + check constraint.
- `password_reset_tokens`: delete all rows, drop `token`, add
  `token_hash VARCHAR(64)` unique.
- `refresh_tokens.audience VARCHAR(20) NOT NULL DEFAULT 'nexdata-app'`; revoke
  all existing rows (their `token_hash` was random and never matched anything).

## Test plan (`tests/test_spec_127_access_lockdown.py`)

Unit (no DB):
- settings default `require_auth=True`, `allow_signup=False`.
- route table: every non-public route carries an auth dependency; admin
  routers carry `require_admin`; `job_stream` carries the stream dependency;
  graphql carries `require_user`; each `USER_WRITE_ENDPOINTS` entry resolves
  to a real endpoint.
- export policy: deny list, pattern and column detection; a normal data table
  passes.
- API-key scope ordering; unknown scope fails closed.
- license_info is not MIT.
- docker-compose host ports all bind to 127.0.0.1.
- `frontend/index.html` has no `authDevBypass`; pages calling protected
  routes include `js/auth.js`.

PG-backed (`TEST_PG_URL`):
- migration `0012` applies on a DB with old-shape tables (plaintext reset
  row is deleted, `role` added) and on a DB without them.
- role enforcement through a mounted app: anonymous 401; user GET 200 / POST
  403 on a method-based router; user 403 / admin 200 on an admin router;
  allowlisted POST passes for a user.
- `ADMIN_EMAILS` promotion at login (verified users only).
- `/auth/register` 403 by default, 200 with `ALLOW_SIGNUP=true`.
- playground token rejected by protected routes, accepted by playground
  quota; refresh keeps the audience; refresh with a bogus token fails.
- reset tokens stored hashed; reset works with the raw token; the raw token
  is not in the table; passwordless users get no reset token.
- stream token accepted by `/job-queue` only for admins; plain token in
  query rejected.
- `APIKeyAuth`: query-string key rejected, header key accepted, insufficient
  scope 403.
- Atlas quota: anonymous caller gets 429 after the limit; admin unmetered.
- export preview of `users` 404 even for admin.

## Out of scope

- Tenancy / entitlements / per-customer data scopes (SPEC_130+).
- Actor column on `collection_audit_log`.
- Rate-limiter atomicity and DDL-per-request in `api_keys.py` (finding 23
  beyond header-only keys).
- GraphQL depth/limit caps (retire-or-rebuild decision is PLAN_085 Q7).
- Scrubbing `config`/`error_message` from the SSE payload (admin-only now).
- Email delivery for password reset (still logged server-side only).
