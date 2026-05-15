# SPEC 053 — Passwordless Email Auth

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-05-14
**Test file:** tests/test_spec_053_passwordless_auth.py
**Plan:** [PLAN_063](../plans/PLAN_063_synthetic_playground.md) — Component C1 / Step 2

## Goal

Add email-only passwordless sign-in to Nexdata, extending the existing JWT `AuthService` (`app/users/auth.py`). A visitor enters their email, receives a 6-digit code **and** a magic link (both backed by one `login_codes` table), and verifying either one issues the same JWT bundle as password login. This makes playground users real accounts usable everywhere, with minimal signup friction.

## Acceptance Criteria

- [ ] New `login_codes` table created in `AuthService._ensure_tables` (idempotent): `id, email, code_hash (SHA-256), token_hash (SHA-256 of magic-link token), purpose, expires_at, consumed_at, attempts, request_ip, created_at` + index on `(email, created_at DESC)`.
- [ ] `users` table idempotent alters in `_ensure_tables`: `password_hash` dropped to nullable; `ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'free'`; `ADD COLUMN IF NOT EXISTS signup_source VARCHAR(100)`. All wrapped so a re-run or a non-Postgres backend can't crash startup.
- [ ] `_generate_login_code()` returns a zero-padded 6-digit string.
- [ ] `_hash_login_code(code)` / `_hash_login_token(token)` return SHA-256 hex digests; codes/tokens are never stored in plaintext.
- [ ] `request_login_code(email, request_ip)`: rate-limits (reject if ≥3 unconsumed codes for the email in the last 15 min, or ≥1 in the last 60 s); generates a 6-digit code + a URL-safe link token; stores hashes with a 10-minute expiry; auto-creates a `users` row (`is_verified=FALSE`, `password_hash=NULL`, `tier='free'`, `signup_source`) if the email is new; dispatches via `app.services.email.send_login_code`; best-effort lead-capture hook (never raises). Returns the (code, link) for the caller/logs — the endpoint never returns them to the client.
- [ ] `verify_login_code(email, code)`: looks up the newest unconsumed/unexpired row for the email; increments `attempts`; locks the row (`consumed_at`) once `attempts > 5`; constant-time compares the code hash; on success marks `consumed_at`, sets `users.is_verified=TRUE` + `last_login_at`, issues access+refresh tokens via the **existing** `_create_access_token`/`_create_refresh_token`; best-effort `mark_verified` lead hook. Raises `ValueError` on bad/expired/locked.
- [ ] `verify_login_token(token)`: same as above but matches `token_hash` (magic-link path), email not required.
- [ ] New endpoints in `app/api/v1/auth.py` (all `async`): `POST /auth/request-code` (body `{email}`, reads client IP, always returns a neutral message — no email enumeration), `POST /auth/verify-code` (body `{email, code}`), `POST /auth/verify-token` (body `{token}`). Verify endpoints return the same shape as `/auth/login`.
- [ ] Tokens issued by the passwordless flow validate through the existing `verify_token` (real accounts).
- [ ] Lead-capture integration is best-effort: a missing/failing `LeadService` (Step 3 not yet built) must never block sign-in.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_generate_code_is_six_digits | `_generate_login_code()` is a 6-char numeric string, zero-padded |
| T2 | test_hash_helpers_are_sha256 | `_hash_login_code` / `_hash_login_token` return 64-char hex, deterministic, differ for different inputs |
| T3 | test_request_login_code_creates_row_and_user | request creates a `login_codes` row + auto-creates the `users` row when email is new |
| T4 | test_request_login_code_rate_limited | 4th request inside 15 min (or 2nd inside 60 s) raises ValueError |
| T5 | test_verify_code_happy_path | correct code → consumed, user verified, returns access+refresh tokens that pass `verify_token` |
| T6 | test_verify_code_wrong_code_increments_attempts | wrong code raises, `attempts` incremented, row not consumed |
| T7 | test_verify_code_lockout_after_5_attempts | 6th wrong attempt locks the row (`consumed_at` set); further attempts rejected |
| T8 | test_verify_code_expired_rejected | a code past `expires_at` raises ValueError |
| T9 | test_verify_login_token_happy_path | the magic-link token verifies and issues tokens |
| T10 | test_lead_hook_failure_does_not_block | request/verify still succeed when the lead hook raises |

## Rubric Checklist

_No `api_endpoint` rubric file exists in `memory/rubrics/` — generic checklist:_

- [ ] Endpoints follow the existing `app/api/v1/auth.py` pattern (`db = next(get_db())`, `try/finally db.close()`)
- [ ] No email enumeration — `/auth/request-code` always returns the same neutral message
- [ ] Secrets (codes, tokens) hashed before storage; constant-time comparison on verify
- [ ] Rate limiting on code requests (abuse control)
- [ ] Reuses existing token issuance — no parallel JWT logic
- [ ] Schema changes are idempotent (`IF NOT EXISTS` / safe `ALTER`)
- [ ] Best-effort side effects (lead hook, email send) never break the critical path
- [ ] Tests cover happy path + expiry + lockout + rate-limit

## Design Notes

```python
# app/users/auth.py — added to AuthService
LOGIN_CODE_EXPIRE_MINUTES = 10
LOGIN_CODE_MAX_ATTEMPTS = 5

def _generate_login_code(self) -> str:        # f"{secrets.randbelow(1_000_000):06d}"
def _hash_login_code(self, code: str) -> str: # hashlib.sha256(code.encode()).hexdigest()
def _hash_login_token(self, token: str) -> str
def request_login_code(self, email, request_ip=None, signup_source="playground") -> dict
def verify_login_code(self, email, code) -> dict   # same bundle shape as login()
def verify_login_token(self, token) -> dict
def _notify_lead_event(self, event, **kw) -> None  # best-effort; guarded import of LeadService
```

`request_login_code` builds the magic link as `{settings.playground_base_url or ''}/playground.html?verify=<token>` and passes it to `send_login_code`. Lead hook (`_notify_lead_event`) does a guarded `from app.services.leads.lead_service import LeadService` — until Step 3 ships, the `ImportError` is swallowed.

**Testing approach:** `AuthService` is Postgres-specific raw SQL (`SERIAL`, etc.) — it has never had SQLite unit tests. SPEC_053 tests use a `pg_session` fixture local to the test file that connects to the real Postgres via `DATABASE_URL` and rolls back each test in a transaction; tests `pytest.skip` if `DATABASE_URL` is unset/unreachable (so they run in-container, skip on a bare host). Pure helpers (T1, T2) need no DB.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/users/auth.py` | Modify | `login_codes` table + `users` alters in `_ensure_tables`; code/hash helpers; `request_login_code`, `verify_login_code`, `verify_login_token`, `_notify_lead_event` |
| `app/api/v1/auth.py` | Modify | `RequestCodeRequest`/`VerifyCodeRequest`/`VerifyTokenRequest` models; async `/auth/request-code`, `/auth/verify-code`, `/auth/verify-token` endpoints |
| `tests/test_spec_053_passwordless_auth.py` | Create | T1–T10 |

## Feedback History

_No corrections yet._
