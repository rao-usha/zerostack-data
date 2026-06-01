"""
Tests for SPEC 096 — Action history ring buffer (PLAN_078 Layer 2).
"""
import re
import time
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec096Backend:

    def test_format_session_block_recent_actions(self):
        """T1: block contains 'Recent actions:' with relative ts."""
        from app.services.atlas.pilot import _format_session_state_block
        now_ms = int(time.time() * 1000)
        block = _format_session_state_block({"recent_actions": [
            {"ts": now_ms - 15_000, "actor": "user",
             "kind": "chip_added", "detail": "HHI ≥ $75K"},
            {"ts": now_ms - 180_000, "actor": "planner",
             "kind": "trade_area_entered", "detail": "Loudoun · 50 mi"},
        ]})
        assert "Recent actions (last 10):" in block
        assert "T-15s" in block
        assert "T-3 min" in block
        assert "chip_added" in block
        assert "Loudoun" in block

    def test_format_session_block_action_detail_truncated(self):
        """T2: overlong detail truncated to 120 chars."""
        from app.services.atlas.pilot import _format_session_state_block
        block = _format_session_state_block({"recent_actions": [
            {"ts": int(time.time() * 1000), "actor": "user",
             "kind": "asked",
             "detail": "x" * 500},
        ]})
        # The detail line should be cut at 120 chars
        m = re.search(r"asked\s+(x+)", block)
        assert m and len(m.group(1)) <= 120

    def test_format_session_block_no_actions_no_section(self):
        """T3: empty recent_actions → section omitted."""
        from app.services.atlas.pilot import _format_session_state_block
        assert _format_session_state_block({"recent_actions": []}) == ""
        assert "Recent actions" not in _format_session_state_block({})

    def test_relative_ts_under_minute(self):
        """T4: ts ≈ now-30s → 'T-30s'"""
        from app.services.atlas.pilot import _format_session_state_block
        now_ms = int(time.time() * 1000)
        block = _format_session_state_block({"recent_actions": [
            {"ts": now_ms - 30_000, "actor": "u",
             "kind": "x", "detail": "z"},
        ]})
        assert "T-30s" in block

    def test_relative_ts_minutes(self):
        """T5: ts ≈ now-180s → 'T-3 min'"""
        from app.services.atlas.pilot import _format_session_state_block
        now_ms = int(time.time() * 1000)
        block = _format_session_state_block({"recent_actions": [
            {"ts": now_ms - 180_000, "actor": "u",
             "kind": "x", "detail": "z"},
        ]})
        assert "T-3 min" in block

    def test_malformed_action_skipped(self):
        """Non-dict entries are dropped silently."""
        from app.services.atlas.pilot import _format_session_state_block
        block = _format_session_state_block({"recent_actions": [
            "bad", None, {"ts": int(time.time()*1000),
                           "actor": "u", "kind": "ok", "detail": "ok"},
        ]})
        # Still renders the good one
        assert "ok" in block


class TestSpec096Frontend:

    def test_log_action_present(self, html):
        """T6: logAction + SESSION_ACTIONS declared."""
        assert "let SESSION_ACTIONS" in html
        assert "function logAction" in html
        assert "SESSION_ACTIONS_MAX" in html

    def test_action_log_persists(self, html):
        """T7: localStorage key referenced."""
        assert "atlas_session_actions_v1" in html

    def test_actions_wired_into_chips(self, html):
        """T8: addChip + removeChip both call logAction."""
        for fn in ("function addChip", "function removeChip"):
            m = re.search(re.escape(fn) + r"\(.*?\n  \}", html, re.DOTALL)
            assert m, f"{fn} not found"
            assert "logAction(" in m.group(0), \
                f"{fn} body does not call logAction"

    def test_actions_wired_into_trade_area(self, html):
        for fn in ("async function enterTradeArea", "function exitTradeArea"):
            m = re.search(re.escape(fn) + r"\(.*?\n  \}", html, re.DOTALL)
            assert m, f"{fn} not found"
            assert "logAction(" in m.group(0)

    def test_actions_wired_into_planner_runner(self, html):
        m = re.search(r"async function runStoryFromPlan\(plan\).*?\n  \}",
                      html, re.DOTALL)
        assert m
        body = m.group(0)
        assert "logAction('planner', 'plan_started'" in body
        assert "__PLANNER_RUNNING" in body

    def test_gather_session_state_includes_recent_actions(self, html):
        m = re.search(r"function gatherSessionState\(\).*?\n  \}",
                      html, re.DOTALL)
        assert m
        assert "recent_actions" in m.group(0)
