"""
Tests for SPEC 091 — Decision Map real competition data (Yelp Fusion).

Backend: radius clamp, response shape, soft-fail when YELP_API_KEY is
missing, body schema. Frontend: parse atlas.html for the competition
helpers + CSS. Pilot tool: registration + dispatch.
"""
import os
import re
from pathlib import Path
from unittest.mock import patch, AsyncMock

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


# A minimal fake Yelp search response covering the fields we care about.
def _fake_yelp_response():
    return {
        "total": 3,
        "businesses": [
            {"id": "a", "name": "Asher Furniture",
             "coordinates": {"latitude": 39.10, "longitude": -77.55},
             "rating": 4.5, "review_count": 80,
             "url": "https://yelp.com/biz/a",
             "location": {"display_address": ["1 Main St", "Leesburg, VA"]}},
            {"id": "b", "name": "Bardstown Modern",
             "coordinates": {"latitude": 39.20, "longitude": -77.40},
             "rating": 4.0, "review_count": 120,
             "url": "https://yelp.com/biz/b",
             "location": {"display_address": ["2 Elm Ave", "Hamilton, VA"]}},
            # One bad row: missing coordinates → must be dropped, not crash
            {"id": "c", "name": "No Location",
             "coordinates": {}, "rating": 4.7, "review_count": 5,
             "location": {"display_address": []}},
        ],
    }


class _FakeYelpClient:
    """Async-context manager fake for app.sources.yelp.client.YelpClient."""
    def __init__(self, *a, **kw): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return None
    search_businesses = AsyncMock(return_value=_fake_yelp_response())


class TestSpec091Backend:

    def setup_method(self):
        # Tests assume YELP_API_KEY is set so the network path executes;
        # the test for missing-key flips it off explicitly.
        os.environ.setdefault("YELP_API_KEY", "test-stub")

    def test_radius_capped_at_25mi(self):
        """T1: radius_mi > 25 caps to 25."""
        from app.services.atlas.competition import _cap_radius
        assert _cap_radius(50) == 25.0
        assert _cap_radius(1000) == 25.0
        assert _cap_radius(0.1) == 0.5
        assert _cap_radius(12) == 12.0
        assert _cap_radius(None) == 5.0

    def test_meters_conversion(self):
        """T2: miles → integer meters."""
        from app.services.atlas.competition import _miles_to_meters
        assert _miles_to_meters(1) == 1609
        assert _miles_to_meters(5) == 8047
        assert _miles_to_meters(25) == 40234

    def test_response_shape(self):
        """T3: returned dict has count, businesses, term_used, radius_mi."""
        from app.services.atlas import competition as comp
        with patch.object(comp, "YelpClient", _FakeYelpClient, create=True):
            # The lazy import inside _do_search reads `from app.sources.yelp.client import YelpClient`,
            # so we patch THAT module's symbol instead:
            pass
        with patch("app.sources.yelp.client.YelpClient", _FakeYelpClient):
            r = comp.find_competition(
                lat=39.0, lon=-77.6, radius_mi=10, term="furniture", limit=5)
        for k in ("count", "businesses", "term_used", "radius_mi"):
            assert k in r, f"missing key {k}"
        # Bad rows (no coords) dropped
        assert r["count"] == 2
        assert all("distance_mi" in b for b in r["businesses"])

    def test_business_distance_field_computed(self):
        """T5: each business gets a distance_mi field (haversine)."""
        from app.services.atlas import competition as comp
        with patch("app.sources.yelp.client.YelpClient", _FakeYelpClient):
            r = comp.find_competition(
                lat=39.0, lon=-77.6, radius_mi=10, term="furniture")
        for b in r["businesses"]:
            assert isinstance(b["distance_mi"], (int, float))
            assert b["distance_mi"] >= 0
        # And the list is sorted by distance ascending
        ds = [b["distance_mi"] for b in r["businesses"]]
        assert ds == sorted(ds)

    def test_returns_empty_when_yelp_key_missing(self, monkeypatch):
        """T4: YELP_API_KEY absent → soft fail, no exception."""
        from app.services.atlas.competition import find_competition
        monkeypatch.delenv("YELP_API_KEY", raising=False)
        r = find_competition(lat=39.0, lon=-77.6,
                              radius_mi=10, term="furniture")
        assert r["count"] == 0
        assert r["businesses"] == []
        assert "YELP_API_KEY" in (r.get("error") or "")

    def test_competition_body_schema(self):
        """T6: CompetitionBody requires lat/lon; defaults radius_mi=5."""
        from app.api.v1.atlas import CompetitionBody
        b = CompetitionBody(lat=39.0, lon=-77.6)
        assert b.radius_mi == 5.0
        assert b.limit == 20
        with pytest.raises(Exception):
            CompetitionBody(lat=39.0)  # missing lon


class TestSpec091Frontend:

    def test_competition_pin_class(self, html):
        """T7: .competition-pin CSS + render helpers present."""
        assert ".competition-pin" in html
        assert "function renderCompetitionPins" in html
        assert "function clearCompetitionPins" in html
        assert "function fetchCompetition" in html

    def test_enter_trade_area_calls_competition(self, html):
        """T8: enterTradeArea calls fetchCompetition when industry_label set."""
        m = re.search(r"async function enterTradeArea.*?\n  \}",
                      html, re.DOTALL)
        assert m, "enterTradeArea not found"
        body = m.group(0)
        assert "fetchCompetition(" in body
        assert "industry_label" in body

    def test_competition_card_row_markup(self, html):
        """Trade Area card has a .ta-comp row inserted (the regex for the
        nested card block is fragile; we just check the row exists and
        the helper that writes into it is defined)."""
        assert 'class="ta-comp"' in html
        assert "function updateCompetitionCardRow" in html
        # Style for the row exists
        assert "#trade-area-card .ta-comp" in html


class TestSpec091PilotTool:

    def test_tool_find_competition_registered(self):
        """T9: pilot_tools TOOL_CALLABLES contains find_competition (read)."""
        from app.services.atlas.pilot_tools import TOOL_CALLABLES, TOOL_KINDS
        assert "find_competition" in TOOL_CALLABLES
        assert TOOL_KINDS["find_competition"] == "read"

    def test_tool_find_competition_returns_count(self):
        """T10: dispatch with mocked centroids + competition → count + top."""
        from app.services.atlas import pilot_tools
        fake_centroids = {"51107": (39.09, -77.64, "Loudoun")}
        fake_comp = {
            "count": 7, "total": 7, "businesses": [
                {"name": f"Shop {i}", "rating": 4.0,
                 "distance_mi": i, "lat": 39, "lon": -77}
                for i in range(10)
            ],
            "term_used": "furniture", "radius_mi": 10.0,
        }
        with patch("app.services.atlas.trade_area.county_centroids",
                   return_value=fake_centroids), \
             patch("app.services.atlas.competition.find_competition",
                   return_value=fake_comp):
            r = pilot_tools.dispatch(
                None, "find_competition",
                {"geo_id": "51107", "radius_mi": 10, "term": "furniture"})
        assert r["count"] == 7
        assert r["name"] == "Loudoun"
        assert len(r["top"]) == 5
        assert r["term_used"] == "furniture"

    def test_tool_find_competition_unknown_geo(self):
        from app.services.atlas import pilot_tools
        with patch("app.services.atlas.trade_area.county_centroids",
                   return_value={}):
            r = pilot_tools.dispatch(None, "find_competition",
                                      {"geo_id": "99999", "term": "x"})
        assert "error" in r
