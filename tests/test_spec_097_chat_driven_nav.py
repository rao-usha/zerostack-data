"""
Tests for SPEC 097 — Chat-driven nav tools (PLAN_078 Layer 3).
"""
import re
from pathlib import Path
from unittest.mock import patch

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec097Tools:

    def test_all_tools_registered(self):
        from app.services.atlas.pilot_tools import TOOL_CALLABLES, TOOL_KINDS
        for name in ("select_pin", "set_fit_weights", "describe_session",
                      "reset_thesis", "set_thesis_field"):
            assert name in TOOL_CALLABLES
        assert TOOL_KINDS["select_pin"]        == "ui"
        assert TOOL_KINDS["set_fit_weights"]   == "ui"
        assert TOOL_KINDS["describe_session"]  == "read"
        assert TOOL_KINDS["reset_thesis"]      == "ui"
        assert TOOL_KINDS["set_thesis_field"]  == "ui"

    def test_select_pin_rank(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "select_pin", {"rank": 3})
        assert r["ok"] is True
        assert r["action"]["args"]["rank"] == 3

    def test_select_pin_geo_id(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "select_pin", {"geo_id": "51107"})
        assert r["action"]["args"]["geo_id"] == "51107"

    def test_select_pin_rejects_invalid_rank(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "select_pin", {"rank": 99})
        assert "error" in r

    def test_select_pin_requires_one(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "select_pin", {})
        assert "error" in r

    def test_set_fit_weights_ok(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "set_fit_weights",
                                  {"weights": {"income": 0.6, "broadband": 0.4}})
        assert r["ok"] is True
        assert r["action"]["args"]["weights"] == {"income": 0.6, "broadband": 0.4}

    def test_set_fit_weights_filters_invalid_keys(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "set_fit_weights",
                                  {"weights": {"income": 0.5, "bogus": 0.5}})
        assert r["action"]["args"]["weights"] == {"income": 0.5}

    def test_set_fit_weights_rejects_empty(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "set_fit_weights",
                                  {"weights": {"bogus": 1}})
        assert "error" in r

    def test_describe_session_read_only(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "describe_session", {})
        assert r["ok"] is True
        assert "session_state" in r["note"].lower() \
            or "session_state" in r["note"]

    def test_reset_thesis_returns_action(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "reset_thesis", {})
        assert r["ok"] is True
        assert r["action"]["name"] == "reset_thesis"

    def test_set_thesis_field_ok(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "set_thesis_field",
                                  {"key": "industry_label",
                                   "value": "Coffee shops"})
        assert r["ok"] is True
        assert r["action"]["args"] == {
            "key": "industry_label", "value": "Coffee shops"}

    def test_set_thesis_field_rejects_unknown_key(self):
        from app.services.atlas import pilot_tools
        r = pilot_tools.dispatch(None, "set_thesis_field",
                                  {"key": "bogus", "value": 1})
        assert "error" in r


class TestSpec097WeightsOverride:

    def _stub_layer(self, _db, _layer_id):
        # build_layer is called positionally as (db, layer_id) so the
        # side_effect needs both — earlier signature dropped layer_id.
        class R:
            pass
        r = R()
        r.values = {"A": 10, "B": 20, "C": 30, "D": 40}
        return r

    def test_weights_override_changes_recipe(self):
        from app.services.atlas import fit_score as fs
        # Run twice — once with defaults, once with overrides — and
        # verify the returned weights field reflects the override.
        with patch.object(fs, "build_layer", side_effect=self._stub_layer):
            r_default = fs.compute_fit_score(
                None, thesis={"industry_label": "Furniture stores"}, top_n=5)
            r_override = fs.compute_fit_score(
                None, thesis={"industry_label": "Furniture stores"}, top_n=5,
                weights_override={"income": 0.10, "broadband": 0.80})
        # Default retail recipe puts income at 55%; override should drop it.
        income_default = next(w for w in r_default["weights"]
                               if "income" in w["label"].lower())["weight"]
        income_over = next(w for w in r_override["weights"]
                            if "income" in w["label"].lower())["weight"]
        assert income_over < income_default
        # Broadband should rise.
        bb_default = next(w for w in r_default["weights"]
                           if "broadband" in w["label"].lower())["weight"]
        bb_over = next(w for w in r_override["weights"]
                        if "broadband" in w["label"].lower())["weight"]
        assert bb_over > bb_default


class TestSpec097Frontend:

    def test_apply_one_pilot_action_handles_new_actions(self, html):
        m = re.search(r"async function applyOnePilotAction\(a\).*?\n  \}",
                      html, re.DOTALL)
        assert m
        body = m.group(0)
        for name in ("select_pin", "set_fit_weights", "reset_thesis",
                      "set_thesis_field"):
            assert f"a.name === '{name}'" in body, name

    def test_custom_weights_forwarded(self, html):
        assert "let CUSTOM_WEIGHTS" in html
        assert "weights_override: CUSTOM_WEIGHTS" in html
