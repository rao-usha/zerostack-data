"""
Tests for SPEC 099 — Pilot-driven walkthrough (unify ⚡ Demo with chat).
"""
import re
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec099Frontend:

    def test_run_pilot_walkthrough_declared(self, html):
        """T1: runPilotWalkthrough exists as an async function."""
        assert "async function runPilotWalkthrough" in html

    def test_demo_button_routes_to_walkthrough(self, html):
        """T2: ⚡ Demo onClick now invokes runPilotWalkthrough."""
        m = re.search(
            r"getElementById\('thesis-demo'\)\.onclick\s*=.{0,400}",
            html, re.DOTALL,
        )
        assert m, "thesis-demo wiring not found"
        body = m.group(0)
        assert "runPilotWalkthrough" in body, body[:200]
        # And NOT the old planner path
        assert "fetchAndRunPlan" not in body

    def test_walkthrough_persists_thesis(self, html):
        """T3: runPilotWalkthrough writes the thesis to localStorage
        so the next session_state snapshot reflects it."""
        m = re.search(r"async function runPilotWalkthrough\(.*?\)\s*\{.*?\n  \}",
                      html, re.DOTALL)
        assert m, "runPilotWalkthrough body not found"
        body = m.group(0)
        assert "localStorage.setItem(THESIS_LS_KEY" in body

    def test_walkthrough_calls_askpilot(self, html):
        """T4: runPilotWalkthrough body ends with an askPilot(...) call."""
        m = re.search(r"async function runPilotWalkthrough\(.*?\)\s*\{.*?\n  \}",
                      html, re.DOTALL)
        assert m
        body = m.group(0)
        assert "askPilot(" in body

    def test_walkthrough_prompt_present(self, html):
        """T5: WALKTHROUGH_PROMPT constant names the key tools."""
        assert "WALKTHROUGH_PROMPT" in html
        # The prompt mentions the spine tools
        m = re.search(r"WALKTHROUGH_PROMPT\s*=\s*\(.*?\);", html, re.DOTALL)
        assert m
        prompt = m.group(0)
        for tool in ("add_constraint", "recommend_candidates",
                     "enter_trade_area", "find_competition",
                     "present_options"):
            assert tool in prompt, f"prompt should mention {tool}"

    def test_planner_path_kept_as_fallback(self, html):
        """T7: runStoryFromPlan + fetchAndRunPlan still exist in code
        as a fallback (resilience). Not deleted, just no longer the
        default ⚡ Demo wiring."""
        assert "async function runStoryFromPlan" in html
        assert "async function fetchAndRunPlan" in html


class TestSpec099Prompt:

    def test_system_prompt_walkthrough_rule(self):
        """T6: SYSTEM_PROMPT has a WALKTHROUGH MODE rule (9f)."""
        from app.services.atlas.pilot import SYSTEM_PROMPT
        assert "WALKTHROUGH MODE" in SYSTEM_PROMPT
        # Key components of the rule
        assert "ONE tool call at a time" in SYSTEM_PROMPT
        assert "BEFORE each tool" in SYSTEM_PROMPT
        # And the canonical spine is named
        for tool in ("add_constraint", "recommend_candidates",
                     "enter_trade_area", "present_options"):
            assert tool in SYSTEM_PROMPT
