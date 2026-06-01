"""
Tests for SPEC 087 — Atlas Decision Map: blank landing + fit-score +
top-N pins. Backend: classify_industry + compute_fit_score + the POST
/atlas/fit-score endpoint contract. Frontend: parse atlas.html for the
new structures (empty-card, paintFitScore, renderTopNPins, etc.).
"""
import re
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec087Backend:

    def test_classify_industry_retail(self):
        """T1: 'Furniture stores' routes to retail recipe."""
        from app.services.atlas.fit_score import classify_industry
        assert classify_industry({"industry_label": "Furniture stores"}) == "retail"
        assert classify_industry({"industry_label": "Coffee shop chain"}) == "retail"

    def test_classify_industry_housing(self):
        """T2: housing keywords route to housing recipe."""
        from app.services.atlas.fit_score import classify_industry
        assert classify_industry({"industry_label": "Apartment buildings"}) == "housing"
        assert classify_industry({"industry_label": "Multifamily REIT"}) == "housing"

    def test_classify_industry_industrial(self):
        """T3: industrial keywords route to industrial recipe."""
        from app.services.atlas.fit_score import classify_industry
        assert classify_industry({"industry_label": "Distribution warehouse"}) == "industrial"
        assert classify_industry({"industry_label": "3PL operator"}) == "industrial"

    def test_classify_industry_default(self):
        """T4: empty / unknown industry → default recipe."""
        from app.services.atlas.fit_score import classify_industry
        assert classify_industry(None) == "default"
        assert classify_industry({}) == "default"
        assert classify_industry({"industry_label": ""}) == "default"
        assert classify_industry({"industry_label": "something niche"}) == "default"

    def test_normalize_min_max(self):
        from app.services.atlas.fit_score import _normalize
        out = _normalize({"a": 0, "b": 50, "c": 100})
        assert out == {"a": 0.0, "b": 0.5, "c": 1.0}

    def test_normalize_handles_constant(self):
        """When max == min, every value gets 0.5 (avoids div-by-zero)."""
        from app.services.atlas.fit_score import _normalize
        out = _normalize({"a": 7, "b": 7, "c": 7})
        assert out == {"a": 0.5, "b": 0.5, "c": 0.5}

    def test_normalize_skips_nones_and_non_numeric(self):
        from app.services.atlas.fit_score import _normalize
        out = _normalize({"a": 1, "b": None, "c": "oops", "d": 10})
        assert set(out.keys()) == {"a", "d"}

    def test_pilot_body_fit_score_optional(self):
        """T6: POST body with no thesis is valid (empty thesis path)."""
        from app.api.v1.atlas import FitScoreBody
        b = FitScoreBody()
        assert b.thesis is None
        assert b.top_n == 10

    def test_pilot_body_fit_score_present(self):
        """T7: POST body with populated thesis is accepted."""
        from app.api.v1.atlas import FitScoreBody
        b = FitScoreBody(thesis={"industry_label": "Furniture stores"}, top_n=5)
        assert b.thesis["industry_label"] == "Furniture stores"
        assert b.top_n == 5


class TestSpec087Frontend:

    def test_empty_landing_init_guard(self, html):
        """T9: init() picks an entryMode and only fetches a specific layer
        in url-layer mode. (SPEC_092 removed the #map-empty centered modal
        — blank mode is now a clean basemap with the header onboarding
        nudge as the only landing cue.)"""
        assert "entryMode = 'blank'" in html
        assert "entryMode = 'fit-score'" in html
        assert "entryMode = 'url-layer'" in html
        # And the deprecated modal is truly gone
        assert 'id="map-empty"' not in html

    def test_paint_fit_score_helper(self, html):
        """T10: paintFitScore / renderTopNPins / renderFitLegend exist
        and the fit-score path is wired into thesis save + demo."""
        assert "function fetchAndPaintFitScore" in html
        assert "function paintFitScore" in html
        assert "function renderTopNPins" in html
        assert "function renderFitLegend" in html
        # POST /fit-score (vs GET /layer/...)
        assert "/fit-score" in html
        # saveThesis + loadFurnitureDemo trigger a re-fit
        assert html.count("fetchAndPaintFitScore(") >= 3

    def test_old_default_layer_path_removed_from_thesis_save(self, html):
        """saveThesis no longer falls back to pickDefaultLayer +
        activateLayer (now goes through the fit-score)."""
        m = re.search(r"function saveThesis\(\).*?\n  \}", html, re.DOTALL)
        assert m, "saveThesis not found"
        body = m.group(0)
        assert "activateLayer(next)" not in body
        assert "fetchAndPaintFitScore" in body
