"""
Tests for SPEC 091 — Decision Map competition lookup.

History:
  SPEC_091 — Yelp Fusion (deprecated 2026-06-03, trial expired)
  SPEC_100 — Census CBP backfill (current backend)

These tests pin the *current* CBP-backed shape. The original Yelp-shape
assertions (rating, review_count, .competition-pin CSS, Yelp `businesses`
list) have been rewritten to the CBP shape. The radius/meters helpers
are gone (no more Yelp 25-mi cap). Frontend assertions check the new
per-county breakdown markup instead of the orange Yelp pin layer.
"""
import re
from pathlib import Path
from unittest.mock import patch

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec091Backend:
    """Backend assertions kept from SPEC_091 but mapped onto SPEC_100."""

    def test_response_shape(self):
        """T3: returned dict has count, per_county, naics_used,
        radius_mi (the CBP shape, not the Yelp shape)."""
        from app.services.atlas import competition as comp
        # SPEC_100 — the lookup is now SQL-only. Build a minimal fake
        # DB session so the test stays a unit test.
        class _Row:
            def __init__(self, geo_id, n):
                self.geo_id = geo_id; self.establishments = n
        class _DB:
            def execute(self, *a, **kw):
                class _R:
                    def all(self_): return [_Row("51107", 183)]
                    def scalar(self_): return None
                return _R()
            def close(self): pass
        fake_centroids = {"51107": (39.09, -77.64, "Loudoun")}
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=fake_centroids):
            r = comp.find_competition(
                lat=39.09, lon=-77.64, radius_mi=10,
                term="furniture", db=_DB())
        for k in ("count", "per_county", "naics_used", "radius_mi",
                   "year"):
            assert k in r, f"missing key {k}"
        # CBP shape — count is establishments, per_county is a list
        assert r["count"] == 183
        assert isinstance(r["per_county"], list)
        # NO Yelp-specific fields leak through
        assert "businesses" not in r
        assert "rating" not in str(r)

    def test_returns_empty_when_db_empty_soft_fails(self):
        """T4 (was: YELP_API_KEY missing; now: empty CBP rowset).
        Soft-fail: count=0, no exception, no error string."""
        from app.services.atlas import competition as comp
        class _DB:
            def execute(self, *a, **kw):
                class _R:
                    def all(self_): return []
                    def scalar(self_): return None
                return _R()
            def close(self): pass
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value={"51107": (39.0, -77.6, "Loudoun")}):
            r = comp.find_competition(
                lat=39.0, lon=-77.6, radius_mi=10,
                term="furniture", db=_DB())
        assert r["count"] == 0
        assert r["per_county"] == [{
            "geo_id": "51107", "name": "Loudoun",
            "establishments": 0, "distance_mi": 0.0, "is_focal": True,
        }] or r["per_county"] == []  # either shape acceptable for empty rowset

    def test_competition_body_schema(self):
        """T6 — body shape: SPEC_100 supports both legacy (lat/lon)
        and new (focal_geo_id) — both optional, but the endpoint
        complains if neither is supplied."""
        from app.api.v1.atlas import CompetitionBody
        # Either lat/lon OR focal_geo_id is accepted at construction;
        # the endpoint enforces the requirement.
        b = CompetitionBody(focal_geo_id="51107")
        assert b.focal_geo_id == "51107"
        assert b.radius_mi == 5.0
        b2 = CompetitionBody(lat=39.0, lon=-77.6)
        assert b2.lat == 39.0 and b2.lon == -77.6


class TestSpec091Frontend:
    """Frontend assertions — the orange Yelp pins layer is gone;
    fetchCompetition + updateCompetitionCardRow stay."""

    def test_competition_helpers_still_present(self, html):
        """T7: the trade-area card still calls a competition fetcher
        and renders the result into the .ta-comp row."""
        assert "function fetchCompetition" in html
        assert "function updateCompetitionCardRow" in html

    def test_enter_trade_area_calls_competition(self, html):
        """T8: enterTradeArea still triggers a competition fetch when
        a thesis industry_label is set."""
        m = re.search(r"async function enterTradeArea.*?\n  \}",
                      html, re.DOTALL)
        assert m, "enterTradeArea not found"
        body = m.group(0)
        assert "fetchCompetition(" in body
        assert "industry_label" in body

    def test_competition_card_row_markup(self, html):
        """The .ta-comp row markup + the helper that writes into it
        still exist."""
        assert 'class="ta-comp"' in html
        assert "function updateCompetitionCardRow" in html
        assert "#trade-area-card .ta-comp" in html


class TestSpec091PilotTool:
    """Pilot tool registration + dispatch — same surface, new shape."""

    def test_tool_find_competition_registered(self):
        """T9: pilot_tools TOOL_CALLABLES still contains find_competition."""
        from app.services.atlas.pilot_tools import TOOL_CALLABLES, TOOL_KINDS
        assert "find_competition" in TOOL_CALLABLES
        assert TOOL_KINDS["find_competition"] == "read"

    def test_tool_find_competition_returns_count(self):
        """T10: dispatch returns count + name + top (per_county)."""
        from app.services.atlas import pilot_tools
        fake_centroids = {"51107": (39.09, -77.64, "Loudoun")}
        fake_cbp = {
            "count": 183, "focal_count": 183, "neighbours_count": 0,
            "per_county": [
                {"geo_id": "51107", "name": "Loudoun",
                 "establishments": 183, "distance_mi": 0.0,
                 "is_focal": True},
            ],
            "naics_used": "442", "naics_label": "Retail trade",
            "year": 2022, "error": None,
        }
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=fake_centroids), \
             patch("app.services.atlas.competition.find_competition_cbp",
                    return_value=fake_cbp), \
             patch("app.services.atlas.competition._neighbor_geo_ids_for",
                    return_value=[]):
            r = pilot_tools.dispatch(
                None, "find_competition",
                {"geo_id": "51107", "radius_mi": 10, "term": "furniture"})
        assert r["count"] == 183
        assert r["name"] == "Loudoun"
        assert r["term_used"] == "furniture"
        # `top` is per_county now (max 5)
        assert len(r["top"]) <= 5
        assert r["top"][0]["geo_id"] == "51107"

    def test_tool_find_competition_unknown_geo(self):
        from app.services.atlas import pilot_tools
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value={}):
            r = pilot_tools.dispatch(None, "find_competition",
                                      {"geo_id": "99999", "term": "x"})
        assert "error" in r
