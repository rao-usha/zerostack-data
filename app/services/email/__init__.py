"""
Transactional email service — PLAN_063 / SPEC_052.

Provider-swappable behind an ABC:
  - ConsoleProvider  — default; logs the message, never sends. Safe for local/CI.
  - ResendProvider   — production; POSTs to the Resend HTTP API via BaseAPIClient.
  - MockProvider     — records sends in-memory for test assertions.

`get_email_service()` picks the provider from settings.email_provider.
`send_login_code()` is the one helper the passwordless-auth flow (SPEC_053) calls.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from app.core.config import get_settings
from app.core.http_client import BaseAPIClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provider ABC
# ---------------------------------------------------------------------------

class EmailProvider(ABC):
    """Transactional email provider interface."""

    @abstractmethod
    async def send(
        self, to: str, subject: str, html: str, text: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send one email. Returns a provider-specific result dict on success."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Console provider (default — never sends)
# ---------------------------------------------------------------------------

class ConsoleProvider(EmailProvider):
    """Logs the message instead of sending. The default — local dev / CI never
    touch the network. The login code/link is visible in `docker-compose logs api`."""

    async def send(
        self, to: str, subject: str, html: str, text: Optional[str] = None
    ) -> Dict[str, Any]:
        logger.info(
            "[ConsoleProvider] email NOT sent (console mode)\n"
            "  to: %s\n  subject: %s\n  html:\n%s",
            to, subject, html,
        )
        return {"provider": "console", "delivered": False, "to": to}


# ---------------------------------------------------------------------------
# Resend provider (production — HTTP API on BaseAPIClient)
# ---------------------------------------------------------------------------

class ResendProvider(EmailProvider, BaseAPIClient):
    """Sends via the Resend HTTP API (https://resend.com/docs).

    Built on BaseAPIClient so it inherits retry/backoff/timeout/pooling.
    """

    SOURCE_NAME = "resend"
    BASE_URL = "https://api.resend.com"

    def __init__(self, api_key: Optional[str] = None, email_from: Optional[str] = None):
        settings = get_settings()
        resolved_key = api_key if api_key is not None else settings.resend_api_key
        BaseAPIClient.__init__(self, api_key=resolved_key)
        self.email_from = email_from or settings.email_from

    def _build_headers(self) -> Dict[str, str]:
        headers = super()._build_headers()
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def send(
        self, to: str, subject: str, html: str, text: Optional[str] = None
    ) -> Dict[str, Any]:
        if not self.api_key:
            raise RuntimeError(
                "ResendProvider requires RESEND_API_KEY. Set it in the environment "
                "or switch EMAIL_PROVIDER to 'console'. Get a key at https://resend.com."
            )
        body: Dict[str, Any] = {
            "from": self.email_from,
            "to": [to],
            "subject": subject,
            "html": html,
        }
        if text:
            body["text"] = text
        result = await self._request("POST", "/emails", json_body=body, resource_id=to)
        logger.info("[ResendProvider] sent email to %s (subject: %s)", to, subject)
        return {"provider": "resend", "delivered": True, "to": to, "response": result}


# ---------------------------------------------------------------------------
# Mock provider (tests)
# ---------------------------------------------------------------------------

class MockProvider(EmailProvider):
    """Records every send in-memory. For test assertions only."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    async def send(
        self, to: str, subject: str, html: str, text: Optional[str] = None
    ) -> Dict[str, Any]:
        record = {"to": to, "subject": subject, "html": html, "text": text}
        self.sent.append(record)
        return {"provider": "mock", "delivered": True, "to": to}


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_email_service() -> EmailProvider:
    """Return the configured email provider. Unknown values fall back to
    ConsoleProvider with a warning (never raises — email config should not
    take down the app)."""
    provider = (get_settings().email_provider or "console").strip().lower()
    if provider == "resend":
        return ResendProvider()
    if provider == "mock":
        return MockProvider()
    if provider != "console":
        logger.warning(
            "Unknown email_provider %r — falling back to ConsoleProvider", provider
        )
    return ConsoleProvider()


# ---------------------------------------------------------------------------
# Login-code email
# ---------------------------------------------------------------------------

_LOGIN_CODE_SUBJECT = "Your Nexdata sign-in code"


def _render_login_code_html(code: str, link: str) -> str:
    """Minimal inline-HTML email body with both the code and the magic link."""
    return f"""\
<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:480px;margin:0 auto;color:#0f172a">
  <h2 style="margin:0 0 16px">Sign in to Nexdata</h2>
  <p style="margin:0 0 20px;color:#475569">Enter this code in the playground, or click the button below.</p>
  <div style="font-size:32px;font-weight:700;letter-spacing:6px;background:#f1f5f9;padding:16px;text-align:center;border-radius:8px">{code}</div>
  <p style="margin:24px 0">
    <a href="{link}" style="background:#6366f1;color:#fff;text-decoration:none;padding:12px 24px;border-radius:8px;display:inline-block">Sign in to Nexdata</a>
  </p>
  <p style="margin:0;color:#94a3b8;font-size:13px">This code and link expire in 10 minutes. If you didn't request this, you can ignore this email.</p>
</div>"""


async def send_login_code(email: str, code: str, link: str) -> Dict[str, Any]:
    """Render and dispatch a passwordless sign-in email via the configured provider."""
    html = _render_login_code_html(code, link)
    text = (
        f"Your Nexdata sign-in code is {code}.\n"
        f"Or sign in directly: {link}\n"
        f"This expires in 10 minutes."
    )
    service = get_email_service()
    return await service.send(email, _LOGIN_CODE_SUBJECT, html, text)
