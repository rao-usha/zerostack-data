"""
Tests for SPEC 095 — Pilot session-state injection (PLAN_078 Layer 1).

Backend: PilotBody schema gains session_state; _format_session_state_block
renders the snapshot; system prompt has the 9d rule. Frontend: parse
atlas.html for gatherSessionState + pilot body field.
"""
import re
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


def _snapshot():
    """A realistic aggressive snapshot covering every section."""
    return {
        "thesis": {
            "industry_label": "Furniture stores",
            "industry_naics": "442110",
            "target_hhi_min": 75000,
            "target_age_band": "25-44",
            "region": "Austin, TX metro",
            "notes": "Mid-to-high-end home furnishings.",
        },
        "chips": [
            {"dimension": "hhi_min", "value": 75000, "label": "HHI ≥ $75K"},
            {"dimension": "exclude_nri", "value": 50,
             "label": "Exclude NRI > 50"},
        ],
        "fit_result": {
            "recipe": "retail",
            "total_candidates": 3248,
            "filtered_candidates": 716,
            "weights": [
                {"layer_id": "demo_acs_median_income",
                 "label": "Median income", "weight": 0.55},
                {"layer_id": "econ_cbp_establishments_county",
                 "label": "Commercial activity", "weight": 0.30},
                {"layer_id": "infra_broadband_subscription",
                 "label": "Broadband", "weight": 0.15},
            ],
        },
        "top_pins": [
            {"rank": 1, "geo_id": "51107", "name": "Loudoun", "score": 70},
            {"rank": 2, "geo_id": "06037", "name": "Los Angeles", "score": 67},
            {"rank": 3, "geo_id": "06085", "name": "Santa Clara", "score": 67},
        ],
        "trade_area": {
            "radius_mi": 50,
            "focal": {
                "geo_id": "51107", "name": "Loudoun",
                "income": 178707, "establishments": 7500,
                "broadband": 92, "nri": 25,
            },
            "summary": {
                "n_neighbors": 25, "avg_income": 107586,
                "avg_broadband": 91.5, "max_nri": 97.6,
            },
            "neighbors": [
                {"name": "Jefferson", "geo_id": "54037",
                 "distance_mi": 12.9, "income": 95523},
                {"name": "Frederick", "geo_id": "24021",
                 "distance_mi": 18.5, "income": 120458},
            ],
        },
        "map_view": {"zoom": 8, "lat": 39.09, "lon": -77.64},
    }


class TestSpec095Body:

    def test_pilot_body_session_state_optional(self):
        from app.api.v1.atlas import PilotBody
        b = PilotBody(question="hi")
        assert b.session_state is None

    def test_pilot_body_session_state_present(self):
        from app.api.v1.atlas import PilotBody
        b = PilotBody(question="hi", session_state=_snapshot())
        assert b.session_state["thesis"]["industry_label"] == "Furniture stores"


class TestSpec095Block:

    def test_format_session_state_block_empty(self):
        from app.services.atlas.pilot import _format_session_state_block
        assert _format_session_state_block(None) == ""
        assert _format_session_state_block({}) == ""
        assert _format_session_state_block("nope") == ""

    def test_format_session_state_block_thesis(self):
        from app.services.atlas.pilot import _format_session_state_block
        block = _format_session_state_block({"thesis": {
            "industry_label": "Furniture stores",
            "industry_naics": "442110",
            "target_hhi_min": 75000,
        }})
        assert "<session_state>" in block
        assert "Furniture stores" in block
        assert "NAICS 442110" in block
        assert "$75,000+" in block

    def test_format_session_state_block_chips(self):
        from app.services.atlas.pilot import _format_session_state_block
        block = _format_session_state_block({
            "chips": [
                {"dimension": "hhi_min", "value": 75000, "label": "HHI ≥ $75K"},
                {"dimension": "exclude_nri", "value": 50,
                 "label": "Exclude NRI > 50"},
            ],
        })
        assert "HHI ≥ $75K" in block
        assert "Exclude NRI > 50" in block

    def test_format_session_state_block_counter_and_recipe(self):
        from app.services.atlas.pilot import _format_session_state_block
        block = _format_session_state_block({"fit_result": {
            "recipe": "retail",
            "total_candidates": 3248, "filtered_candidates": 716,
            "weights": [{"layer_id": "x", "label": "Median income",
                          "weight": 0.55}],
        }})
        assert "716 of 3,248 candidates" in block
        assert "retail" in block
        assert "55% Median income" in block

    def test_format_session_state_block_top_pins(self):
        from app.services.atlas.pilot import _format_session_state_block
        block = _format_session_state_block({
            "top_pins": [
                {"rank": 1, "geo_id": "51107",
                 "name": "Loudoun", "score": 70},
                {"rank": 2, "geo_id": "06037",
                 "name": "Los Angeles", "score": 67},
            ],
        })
        assert "Top candidates on map:" in block
        assert "1. Loudoun" in block
        assert "fit 70" in block

    def test_format_session_state_block_trade_area(self):
        from app.services.atlas.pilot import _format_session_state_block
        block = _format_session_state_block({"trade_area": {
            "radius_mi": 50,
            "focal": {"geo_id": "51107", "name": "Loudoun",
                       "income": 178707, "nri": 25},
            "summary": {"n_neighbors": 25, "avg_income": 107586,
                         "max_nri": 97.6, "avg_broadband": 91.5},
            "neighbors": [
                {"name": "Jefferson", "distance_mi": 12.9, "income": 95523},
            ],
        }})
        assert "Current trade area" in block
        assert "Loudoun · 50 mi radius" in block
        assert "25 counties within 50 mi" in block
        assert "Jefferson (12.9 mi)" in block

    def test_format_session_state_block_map_view(self):
        from app.services.atlas.pilot import _format_session_state_block
        block = _format_session_state_block({
            "map_view": {"zoom": 8, "lat": 39.09, "lon": -77.64},
        })
        assert "zoom 8" in block
        assert "39.09" in block

    def test_format_session_state_block_caps_strings(self):
        from app.services.atlas.pilot import _format_session_state_block
        long = "x" * 5000
        block = _format_session_state_block({"thesis": {
            "industry_label": long, "notes": long,
        }})
        # Industry label cap is 80, notes cap is 320 — both must fit.
        assert "x" * 81 not in block.split("Industry: ")[1].split("\n")[0]


class TestSpec095SystemPrompt:

    def test_system_prompt_has_session_state_rule(self):
        from app.services.atlas.pilot import SYSTEM_PROMPT
        assert "<session_state>" in SYSTEM_PROMPT
        assert "what just happened" in SYSTEM_PROMPT
        # The rule explicitly forbids get_recent_events for recap intent
        assert "get_recent_events" in SYSTEM_PROMPT


class TestSpec095Frontend:

    def test_gather_session_state_present(self, html):
        assert "function gatherSessionState" in html
        # The new helper that resolves a name from BOUNDARY_LAYER is wired too
        assert "function nameForGeo" in html

    def test_pilot_fetch_sends_session_state(self, html):
        # Look for the body line that includes session_state next to thesis_context
        m = re.search(r"session_state:\s*\(typeof gatherSessionState",
                      html, re.DOTALL)
        assert m, "session_state body field not found in pilot fetch"

    def test_current_trade_area_data_cached(self, html):
        assert "CURRENT_TRADE_AREA_DATA" in html
        # Cached on enterTradeArea
        assert "CURRENT_TRADE_AREA_DATA = r;" in html
        # Cleared on exit
        assert "CURRENT_TRADE_AREA_DATA = null;" in html
