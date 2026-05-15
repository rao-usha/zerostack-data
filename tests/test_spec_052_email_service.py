"""
Tests for SPEC 052 — Email Service (transactional email, swappable provider)
PLAN_063 C1 / Step 1.
"""
import asyncio

import pytest

from app.core.config import reset_settings
from app.services.email import (
    ConsoleProvider,
    EmailProvider,
    MockProvider,
    ResendProvider,
    get_email_service,
    send_login_code,
)


@pytest.mark.unit
class TestSpec052EmailService:
    """Tests for the transactional email service (PLAN_063 C1 / Step 1)."""

    def test_console_provider_logs_no_network(self):
        """T1: ConsoleProvider.send returns success and makes no HTTP call."""
        provider = ConsoleProvider()
        result = asyncio.run(
            provider.send("user@example.com", "Subject", "<p>Body</p>")
        )
        assert result["provider"] == "console"
        assert result["delivered"] is False
        assert result["to"] == "user@example.com"
        # ConsoleProvider is not a BaseAPIClient — it has no HTTP client at all.
        assert not hasattr(provider, "_client")

    def test_mock_provider_records_sends(self):
        """T2: MockProvider.send appends to .sent with to/subject/html."""
        provider = MockProvider()
        assert provider.sent == []
        asyncio.run(provider.send("a@b.com", "Hi", "<p>hello</p>", text="hello"))
        assert len(provider.sent) == 1
        rec = provider.sent[0]
        assert rec["to"] == "a@b.com"
        assert rec["subject"] == "Hi"
        assert rec["html"] == "<p>hello</p>"
        assert rec["text"] == "hello"

    def test_factory_default_is_console(self, monkeypatch):
        """T3: get_email_service() with default settings returns ConsoleProvider."""
        monkeypatch.delenv("EMAIL_PROVIDER", raising=False)
        reset_settings()
        try:
            service = get_email_service()
            assert isinstance(service, ConsoleProvider)
            assert isinstance(service, EmailProvider)
        finally:
            reset_settings()

    def test_factory_unknown_falls_back(self, monkeypatch):
        """T4: unknown email_provider value -> ConsoleProvider + warning, no raise."""
        monkeypatch.setenv("EMAIL_PROVIDER", "bogus-provider")
        reset_settings()
        try:
            service = get_email_service()
            assert isinstance(service, ConsoleProvider)
        finally:
            reset_settings()

    def test_resend_provider_requires_key(self, monkeypatch):
        """T5: ResendProvider with no resend_api_key raises a clear error on send."""
        monkeypatch.delenv("RESEND_API_KEY", raising=False)
        reset_settings()
        try:
            provider = ResendProvider(api_key=None)
            with pytest.raises(RuntimeError, match="RESEND_API_KEY"):
                asyncio.run(provider.send("a@b.com", "S", "<p>h</p>"))
        finally:
            reset_settings()

    def test_resend_provider_posts_correct_payload(self, monkeypatch):
        """T6: ResendProvider.send POSTs to the Resend endpoint with correct payload + Bearer auth."""
        provider = ResendProvider(api_key="test_key_123", email_from="from@nexdata.com")

        # Bearer auth is wired into the headers
        headers = provider._build_headers()
        assert headers["Authorization"] == "Bearer test_key_123"

        captured = {}

        async def fake_request(method, url, json_body=None, resource_id="unknown", **kw):
            captured["method"] = method
            captured["url"] = url
            captured["json_body"] = json_body
            return {"id": "resend-msg-1"}

        monkeypatch.setattr(provider, "_request", fake_request)
        result = asyncio.run(
            provider.send("dest@example.com", "Hello", "<p>hi</p>")
        )

        assert captured["method"] == "POST"
        assert captured["url"] == "/emails"
        assert captured["json_body"]["from"] == "from@nexdata.com"
        assert captured["json_body"]["to"] == ["dest@example.com"]
        assert captured["json_body"]["subject"] == "Hello"
        assert captured["json_body"]["html"] == "<p>hi</p>"
        assert result["provider"] == "resend"
        assert result["delivered"] is True

    def test_send_login_code_contains_code_and_link(self, monkeypatch):
        """T7: send_login_code dispatches a message whose html contains both the code and the link."""
        mock = MockProvider()
        monkeypatch.setattr(
            "app.services.email.get_email_service", lambda: mock
        )
        link = "https://app.example.com/playground.html?verify=tok_abc"
        asyncio.run(send_login_code("lead@corp.com", "482913", link))

        assert len(mock.sent) == 1
        rec = mock.sent[0]
        assert rec["to"] == "lead@corp.com"
        assert "Nexdata sign-in code" in rec["subject"]
        assert "482913" in rec["html"]
        assert link in rec["html"]
        # text fallback also carries both
        assert "482913" in rec["text"]
        assert link in rec["text"]

    def test_send_login_code_uses_configured_provider(self, monkeypatch):
        """T8: send_login_code routes through whatever get_email_service() returns."""
        mock = MockProvider()
        monkeypatch.setattr(
            "app.services.email.get_email_service", lambda: mock
        )
        result = asyncio.run(
            send_login_code("x@y.com", "000111", "https://x/verify?t=1")
        )
        assert result["provider"] == "mock"
        assert len(mock.sent) == 1
