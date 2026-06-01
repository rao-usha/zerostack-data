"""
Tests for SPEC 098 — Visible reasoning surface (PLAN_078 Layer 4).
"""
import re
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec098CSS:

    def test_thinking_chip_class_present(self, html):
        """T1: CSS .thinking-chip declared with the pulse animation."""
        assert ".msg.assistant .thinking-chip" in html
        assert "thinking-pulse" in html

    def test_cause_receipt_class_present(self, html):
        """T2: CSS .cause-receipt declared (incl. error variant)."""
        assert ".msg.assistant .cause-receipt" in html
        assert ".cause-receipt.is-error" in html


class TestSpec098Handler:

    def test_handler_creates_thinking_chip_on_started(self, html):
        """T3: handler creates a thinking chip on tool_call_started
        and stores it on c.thinkingEl."""
        # The tool_call_started branch creates a div.thinking-chip
        m = re.search(r"ev\.event === 'tool_call_started'.*?\n    \}",
                      html, re.DOTALL)
        assert m
        body = m.group(0)
        assert "thinking-chip" in body
        assert "c.thinkingEl = chip" in body \
            or "c.thinkingEl = " in body

    def test_handler_replaces_chip_with_receipt(self, html):
        """T4: tool_call_completed drops the chip and appends a
        .cause-receipt line to c.receiptsEl."""
        m = re.search(r"ev\.event === 'tool_call_completed'.*?\n    \}",
                      html, re.DOTALL)
        assert m
        body = m.group(0)
        assert "thinkingEl" in body
        assert "cause-receipt" in body
        assert "c.receiptsEl.appendChild" in body

    def test_handler_clears_chip_on_done(self, html):
        """T5: done branch in handlePilotEvent defensively removes any
        lingering chip. We grab the handlePilotEvent body and check it
        contains the cleanup pattern (avoids matching streamCoT's own
        `done` branch)."""
        m = re.search(r"function handlePilotEvent\(ev, c\).*?\n  \}",
                      html, re.DOTALL)
        assert m
        body = m.group(0)
        # Inside handlePilotEvent: defensive thinkingEl removal
        assert "SPEC_098 — defensively clear" in body
        assert "thinkingEl.parentNode.removeChild" in body

    def test_appendPilotTurn_includes_receipts(self, html):
        """appendPilotTurn returns the receipts container ref."""
        m = re.search(r"function appendPilotTurn\(question\).*?\n  \}",
                      html, re.DOTALL)
        assert m
        body = m.group(0)
        assert 'class="receipts"' in body
        assert "receiptsEl" in body

    def test_collected_has_visible_reasoning_refs(self, html):
        """askPilot's collected dict gets receiptsEl + asstEl + thinkingEl."""
        m = re.search(r"const collected = \{(.*?)\};", html, re.DOTALL)
        assert m
        body = m.group(0)
        for k in ("receiptsEl", "asstEl", "thinkingEl"):
            assert k in body
