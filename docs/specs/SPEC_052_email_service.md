# SPEC 052 — Email Service (transactional email, swappable provider)

**Status:** Draft
**Task type:** service
**Date:** 2026-05-14
**Test file:** tests/test_spec_052_email_service.py
**Plan:** [PLAN_063](../plans/PLAN_063_synthetic_playground.md) — Component C1 / Step 1

## Goal

Add a transactional email-sending capability to Nexdata. None exists today (only email *inference* for people-collection). This is the hard dependency for passwordless auth (SPEC_053). The service is provider-swappable behind an ABC: `ConsoleProvider` (default — logs, never sends), `ResendProvider` (HTTP API, production), `MockProvider` (records sends for tests).

## Acceptance Criteria

- [ ] `EmailProvider` ABC defines `async def send(to, subject, html, text=None) -> dict`.
- [ ] `ConsoleProvider` logs the full message (incl. recipient + body) to the logger and returns a success dict; never makes a network call. It is the **default**.
- [ ] `ResendProvider` POSTs to `https://api.resend.com/emails` with a Bearer key, built on `BaseAPIClient` (inherits retry/backoff/timeout). Raises a clear error if no API key is configured.
- [ ] `MockProvider` records every message in an in-memory list (`.sent`) and returns success — for test assertions.
- [ ] `get_email_service()` factory reads `settings.email_provider` and returns the matching provider; unknown value falls back to `ConsoleProvider` with a warning.
- [ ] `send_login_code(email, code, link)` helper renders a minimal inline-HTML email (subject "Your Nexdata sign-in code", body contains both the 6-digit code and the magic link, states a 10-minute expiry) and dispatches via the configured provider.
- [ ] New config fields on `app/core/config.py` `Settings`: `email_provider` (default `"console"`), `resend_api_key` (Optional), `email_from` (default `"noreply@nexdata.com"`), `playground_base_url` (Optional).
- [ ] No real network call happens in the default config or under pytest.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_console_provider_logs_no_network | ConsoleProvider.send returns success and makes no HTTP call |
| T2 | test_mock_provider_records_sends | MockProvider.send appends to `.sent` with to/subject/html |
| T3 | test_factory_default_is_console | get_email_service() with default settings returns ConsoleProvider |
| T4 | test_factory_unknown_falls_back | unknown email_provider value → ConsoleProvider + warning, no raise |
| T5 | test_resend_provider_requires_key | ResendProvider with no resend_api_key raises a clear error on send |
| T6 | test_resend_provider_posts_correct_payload | ResendProvider.send issues a POST to the Resend endpoint with from/to/subject/html and Bearer auth (HTTP layer mocked) |
| T7 | test_send_login_code_contains_code_and_link | send_login_code dispatches a message whose html contains both the code and the link; subject is the sign-in subject |
| T8 | test_send_login_code_uses_configured_provider | send_login_code routes through whatever get_email_service() returns (assert via MockProvider) |

## Rubric Checklist

_No `service` rubric file exists in `memory/rubrics/` — using a generic checklist:_

- [ ] Service is importable without side effects (no network/db at import time)
- [ ] External calls go through `BaseAPIClient` (retry/backoff/timeout inherited)
- [ ] Secrets (`resend_api_key`) read from `Settings`, never hardcoded
- [ ] Safe-by-default: default config sends nothing
- [ ] Errors are explicit (missing key → clear exception, not silent failure)
- [ ] Unit tests are offline (`@pytest.mark.unit`), no real network
- [ ] Logging at appropriate levels (info on send, warning on fallback)

## Design Notes

```python
# app/services/email/__init__.py
class EmailProvider(ABC):
    @abstractmethod
    async def send(self, to: str, subject: str, html: str, text: str | None = None) -> dict: ...

class ConsoleProvider(EmailProvider):      # default; logs only
class ResendProvider(EmailProvider, BaseAPIClient):
    SOURCE_NAME = "resend"; BASE_URL = "https://api.resend.com"
    # _build_headers() adds Authorization: Bearer <resend_api_key>
    # send() -> self._request("POST", "/emails", json_body={from,to,subject,html})
class MockProvider(EmailProvider):         # .sent: list[dict]

def get_email_service() -> EmailProvider: ...          # reads settings.email_provider
async def send_login_code(email, code, link) -> dict:  # renders + dispatches
```

`send_login_code` builds the magic link as `{settings.playground_base_url or ''}/playground.html?verify=<link_token>` — the caller (SPEC_053) passes the already-built `link`.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/services/email/__init__.py` | Create | EmailProvider ABC + 3 providers + factory + send_login_code |
| `app/core/config.py` | Modify | Add email_provider, resend_api_key, email_from, playground_base_url to Settings |
| `tests/test_spec_052_email_service.py` | Create | T1–T8 |

## Feedback History

_No corrections yet._
