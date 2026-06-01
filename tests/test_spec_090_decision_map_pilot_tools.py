"""
Tests for SPEC 090 — Decision Map Pilot tools (Phase E of PLAN_075).

Backend: tool registration, dispatch shape, validation. Frontend:
parse atlas.html for the new UI-action handlers in applyOnePilotAction.
"""
import re
from pathlib import Path
from unittest.mock import patch

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec090ToolRegistration:

    def test_tools_registered(self):
        """T1: TOOLS includes all 5 by name."""
        from app.services.atlas.pilot_tools import TOOL_CALLABLES, TOOL_KINDS
        for name in ("recommend_candidates", "add_constraint",
                      "remove_constraint", "enter_trade_area",
                      "exit_trade_area"):
            assert name in TOOL_CALLABLES, f"{name} not registered"
        # Kinds: recommend = read, others = ui
        assert TOOL_KINDS["recommend_candidates"] == "read"
        for ui in ("add_constraint", "remove_constraint",
                   "enter_trade_area", "exit_trade_area"):
            assert TOOL_KINDS[ui] == "ui"


class TestSpec090Recommend:

    def test_recommend_candidates_returns_list(self):
        """T2: with mocked compute_fit_score, dispatch returns candidates."""
        from app.services.atlas import pilot_tools
        fake_score = {
            "scores": {"06037": 80, "51107": 90},
            "weights": [{"layer_id": "x", "label": "Income", "weight": 1.0}],
            "top_n": [{"geo_id": "51107", "score": 90},
                       {"geo_id": "06037", "score": 80}],
            "recipe": "retail", "total_candidates": 2,
            "filtered_candidates": 2,
        }
        with patch("app.services.atlas.fit_score.compute_fit_score",
                   return_value=fake_score):
            r = pilot_tools.dispatch(None, "recommend_candidates",
                                      {"top_n": 2,
                                       "thesis_context": {"industry_label": "Furniture stores"}})
        assert "candidates" in r and len(r["candidates"]) == 2
        assert r["candidates"][0]["geo_id"] == "51107"
        assert r["recipe"] == "retail"
        assert r["total_candidates"] == 2


class TestSpec090ConstraintTools:

    def test_add_constraint_returns_ui_action(self):
        """T3: add_constraint returns action descriptor."""
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "add_constraint",
                                  {"dimension": "hhi_min", "value": 80000})
        assert r["ok"] is True
        assert r["action"]["name"] == "add_constraint"
        assert r["action"]["args"]["dimension"] == "hhi_min"
        assert r["action"]["args"]["value"] == 80000.0

    def test_remove_constraint_returns_ui_action(self):
        """T4."""
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "remove_constraint",
                                  {"dimension": "exclude_nri"})
        assert r["ok"] is True
        assert r["action"]["name"] == "remove_constraint"
        assert r["action"]["args"]["dimension"] == "exclude_nri"

    def test_add_constraint_validates_dimension(self):
        """T7: unknown dimension → error result."""
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "add_constraint",
                                  {"dimension": "made_up", "value": 1})
        assert "error" in r
        assert "valid" in r["error"].lower()

    def test_add_constraint_validates_numeric(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "add_constraint",
                                  {"dimension": "hhi_min", "value": "nope"})
        assert "error" in r


class TestSpec090TradeAreaTools:

    def test_enter_trade_area_returns_ui_action(self):
        """T5."""
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "enter_trade_area",
                                  {"geo_id": "51107", "radius_mi": 30})
        assert r["ok"] is True
        assert r["action"]["name"] == "enter_trade_area"
        assert r["action"]["args"]["geo_id"] == "51107"
        assert r["action"]["args"]["radius_mi"] == 30.0

    def test_enter_trade_area_radius_clamps(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "enter_trade_area",
                                  {"geo_id": "11001", "radius_mi": 9999})
        assert r["action"]["args"]["radius_mi"] == 250.0

    def test_exit_trade_area_returns_ui_action(self):
        """T6."""
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "exit_trade_area", {})
        assert r["ok"] is True
        assert r["action"]["name"] == "exit_trade_area"


class TestSpec090Frontend:

    def test_frontend_applies_new_ui_actions(self, html):
        """T8: applyOnePilotAction handles all 4 new action names."""
        m = re.search(r"async function applyOnePilotAction\(a\).*?\n  \}",
                      html, re.DOTALL)
        assert m, "applyOnePilotAction not found"
        body = m.group(0)
        for name in ("add_constraint", "remove_constraint",
                      "enter_trade_area", "exit_trade_area"):
            assert f"a.name === '{name}'" in body, f"missing dispatch for {name}"

    def test_pilot_prompt_describes_decision_map_tools(self):
        """The system prompt's 9c block mentions the 5 new tools."""
        from app.services.atlas.pilot import SYSTEM_PROMPT
        assert "recommend_candidates" in SYSTEM_PROMPT
        assert "add_constraint" in SYSTEM_PROMPT
        assert "enter_trade_area" in SYSTEM_PROMPT
