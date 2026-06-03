"""
Tests for SPEC 094 — Planner → Executor (generalised walkthrough).

Backend: Pydantic validation of Plan/Beat, per-tool arg checks,
fallback path when the LLM is unavailable, cache hit/miss, body
schema. Frontend: parse atlas.html for the executor + dispatcher +
demo wiring.
"""
import json
import re
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


def _good_plan() -> dict:
    return {
        "title": "Furniture walkthrough",
        "summary": "Filter income + hazard, score, drill the top trade area.",
        "beats": [
            {"title": "Load thesis", "rationale": "Loading the saved thesis.",
             "tool": "setup_thesis"},
            {"title": "Filter HHI", "rationale": "Furniture buyers are affluent.",
             "tool": "add_constraint",
             "args": {"dimension": "hhi_min", "value": 75000}},
            {"title": "Skip risk", "rationale": "High-NRI areas dampen retail.",
             "tool": "add_constraint",
             "args": {"dimension": "exclude_nri", "value": 50}},
            {"title": "Score", "rationale": "Paint the 0-100 fit choropleth.",
             "tool": "recommend_candidates", "args": {"top_n": 10}},
            {"title": "Drill top", "rationale": "Open the 50-mi trade area.",
             "tool": "enter_trade_area",
             "args": {"top_pick": True, "radius_mi": 50}},
        ],
    }


class TestSpec094Validator:

    def test_validate_plan_happy_path(self):
        from app.services.atlas.planner import validate_plan
        plan, err = validate_plan(_good_plan())
        assert err is None
        assert plan is not None
        assert plan["beats"][0]["tool"] == "setup_thesis"
        assert len(plan["beats"]) == 5

    def test_validate_plan_unknown_tool(self):
        from app.services.atlas.planner import validate_plan
        raw = _good_plan()
        raw["beats"][1]["tool"] = "summon_dragon"
        plan, err = validate_plan(raw)
        assert plan is None
        assert err and "schema invalid" in err

    def test_validate_plan_bad_dimension(self):
        from app.services.atlas.planner import validate_plan
        raw = _good_plan()
        raw["beats"][1]["args"]["dimension"] = "bogus_dim"
        plan, err = validate_plan(raw)
        assert plan is None
        assert err and "dimension" in err

    def test_validate_plan_too_many_beats(self):
        from app.services.atlas.planner import validate_plan
        raw = _good_plan()
        # Pad to 13 beats — over the 12 cap
        raw["beats"] = raw["beats"] + [
            {"title": "extra", "rationale": "x", "tool": "no_op"}
        ] * 8
        plan, err = validate_plan(raw)
        assert plan is None

    def test_validate_plan_too_few_beats(self):
        from app.services.atlas.planner import validate_plan
        raw = _good_plan()
        raw["beats"] = raw["beats"][:2]
        plan, err = validate_plan(raw)
        assert plan is None

    def test_validate_plan_truncates_overlong_strings(self):
        """Over-length strings are truncated, not rejected — keeps the
        planner lenient when the LLM is verbose (a 240-char cap was
        causing real plans to fall back into the hardcoded default)."""
        from app.services.atlas.planner import validate_plan, _CAPS
        raw = _good_plan()
        raw["beats"][0]["title"] = "x" * 500
        raw["summary"] = "y" * 800
        plan, err = validate_plan(raw)
        assert err is None
        assert plan is not None
        assert len(plan["beats"][0]["title"]) <= _CAPS["beat_title"]
        assert len(plan["summary"]) <= _CAPS["summary"]

    def test_validate_plan_inserts_setup_thesis_if_missing(self):
        from app.services.atlas.planner import validate_plan
        raw = _good_plan()
        raw["beats"] = raw["beats"][1:]  # drop the setup_thesis first beat
        plan, err = validate_plan(raw)
        assert err is None
        assert plan["beats"][0]["tool"] == "setup_thesis"

    def test_validate_enter_trade_area_requires_geo_or_top_pick(self):
        from app.services.atlas.planner import validate_plan
        raw = _good_plan()
        raw["beats"][-1]["args"] = {"radius_mi": 50}
        plan, err = validate_plan(raw)
        assert plan is None
        assert "geo_id or top_pick" in (err or "")


class TestSpec094GeneratePlan:

    def test_generate_plan_uses_fallback_when_llm_fails(self, monkeypatch):
        from app.services.atlas import planner
        planner.clear_cache()
        monkeypatch.setenv("OPENAI_API_KEY", "test-stub")

        def _raise(**kwargs):
            raise RuntimeError("simulated LLM outage")

        fake = MagicMock()
        fake.chat.completions.create = _raise
        with patch("openai.OpenAI", return_value=fake):
            plan, err, cache_hit = planner.generate_plan(
                None, thesis_context={"industry_label": "Furniture"})
        assert plan == planner._FALLBACK_PLAN
        assert err and "llm error" in err.lower()
        assert cache_hit is False

    def test_generate_plan_uses_fallback_when_no_key(self, monkeypatch):
        from app.services.atlas import planner
        planner.clear_cache()
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        plan, err, cache_hit = planner.generate_plan(
            None, thesis_context={"industry_label": "Coffee"})
        assert plan == planner._FALLBACK_PLAN
        assert "OPENAI_API_KEY" in (err or "")

    def test_generate_plan_cache_hit(self, monkeypatch):
        from app.services.atlas import planner
        planner.clear_cache()
        monkeypatch.setenv("OPENAI_API_KEY", "test-stub")

        good_json = json.dumps(_good_plan())
        fake_resp = MagicMock()
        fake_resp.choices = [MagicMock(message=MagicMock(content=good_json))]
        fake = MagicMock()
        fake.chat.completions.create = MagicMock(return_value=fake_resp)
        with patch("openai.OpenAI", return_value=fake):
            plan1, _, hit1 = planner.generate_plan(
                None, thesis_context={"industry_label": "Furniture"})
            # Second call must NOT hit the LLM again
            plan2, _, hit2 = planner.generate_plan(
                None, thesis_context={"industry_label": "Furniture"})
        assert hit1 is False
        assert hit2 is True
        assert plan1 == plan2
        # Only one LLM call
        assert fake.chat.completions.create.call_count == 1

    def test_generate_plan_different_thesis_misses_cache(self, monkeypatch):
        from app.services.atlas import planner
        planner.clear_cache()
        monkeypatch.setenv("OPENAI_API_KEY", "test-stub")

        good = _good_plan()
        fake_resp = MagicMock()
        fake_resp.choices = [MagicMock(
            message=MagicMock(content=json.dumps(good)))]
        fake = MagicMock()
        fake.chat.completions.create = MagicMock(return_value=fake_resp)
        with patch("openai.OpenAI", return_value=fake):
            planner.generate_plan(None,
                                   thesis_context={"industry_label": "Furniture"})
            planner.generate_plan(None,
                                   thesis_context={"industry_label": "Coffee"})
        # Two distinct cache keys → two LLM calls
        assert fake.chat.completions.create.call_count == 2


class TestSpec094Body:

    def test_plan_body_schema(self):
        from app.api.v1.atlas import PlanBody
        b = PlanBody()
        assert b.thesis_context is None
        assert b.prompt is None
        b2 = PlanBody(thesis_context={"industry_label": "Furniture"},
                       prompt="walk me through this")
        assert b2.thesis_context["industry_label"] == "Furniture"
        assert b2.prompt == "walk me through this"


class TestSpec094Frontend:

    def test_fetch_and_run_plan_present(self, html):
        assert "async function fetchAndRunPlan" in html
        assert "async function runStoryFromPlan" in html
        assert "async function applyPlanBeat" in html
        assert "function appendStaticCoT" in html
        # Hits the new endpoint
        assert "/plan" in html

    def test_demo_wired_to_planner(self, html):
        """⚡ Demo originally routed through fetchAndRunPlan (SPEC_094).
        SPEC_099 re-routed it through the Pilot itself (runPilotWalkthrough),
        but fetchAndRunPlan + runStoryFromPlan stay in the codebase as a
        fallback for non-chat surfaces. So the planner module must still
        exist; the ⚡ Demo wiring may point to either."""
        # Grab a generous window after the onclick assignment.
        m = re.search(
            r"getElementById\('thesis-demo'\)\.onclick\s*=.{0,400}",
            html, re.DOTALL,
        )
        assert m, "thesis-demo wiring not found"
        wiring = m.group(0)
        assert ("fetchAndRunPlan" in wiring
                or "runPilotWalkthrough" in wiring), wiring[:200]
        # Planner module remains present in the codebase
        assert "async function fetchAndRunPlan" in html
        assert "async function runStoryFromPlan" in html

    def test_apply_plan_beat_dispatches_every_tool(self, html):
        """applyPlanBeat must handle every whitelisted tool name."""
        m = re.search(r"async function applyPlanBeat\(beat\).*?\n  \}",
                      html, re.DOTALL)
        assert m, "applyPlanBeat body not found"
        body = m.group(0)
        for tool in ("setup_thesis", "add_constraint", "remove_constraint",
                      "recommend_candidates", "enter_trade_area",
                      "exit_trade_area", "find_competition", "no_op"):
            assert tool in body, f"applyPlanBeat missing branch for {tool}"

    def test_run_story_from_plan_walks_beats(self, html):
        m = re.search(r"async function runStoryFromPlan\(plan\).*?\n  \}",
                      html, re.DOTALL)
        assert m, "runStoryFromPlan body not found"
        body = m.group(0)
        assert "applyPlanBeat(beat)" in body
        assert "appendStaticCoT" in body
        # Honours the cancellation token used by skipStory
        assert "tok.cancelled" in body
