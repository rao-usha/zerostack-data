"""
Tests for SPEC 089 — Decision Map trade-area mode.

Backend: haversine, compute_trade_area with mocked centroids+layers,
TradeAreaBody schema. Frontend: parse atlas.html for the card + helpers.
"""
import re
from pathlib import Path
from unittest.mock import patch

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


# Mock centroid set (lat, lon, name) — three counties in DC/NoVA area
# and one far away (LA County) to exercise the radius filter.
_FAKE_CENTROIDS = {
    "11001": (38.91, -77.02, "District of Columbia"),
    "51107": (39.09, -77.64, "Loudoun County, VA"),
    "51059": (38.83, -77.28, "Fairfax County, VA"),
    "06037": (34.05, -118.24, "Los Angeles County, CA"),
}


class _FakeLayerResult:
    def __init__(self, values):
        self.values = values


def _fake_build_layer(_db, layer_id):
    if layer_id == "demo_acs_median_income":
        return _FakeLayerResult({
            "11001": 92000, "51107": 175000, "51059": 130000, "06037": 76000,
        })
    if layer_id == "econ_cbp_establishments_county":
        return _FakeLayerResult({
            "11001": 20000, "51107": 7500, "51059": 28000, "06037": 240000,
        })
    if layer_id == "infra_broadband_subscription":
        return _FakeLayerResult({
            "11001": 85, "51107": 92, "51059": 90, "06037": 80,
        })
    if layer_id == "disaster_nri":
        return _FakeLayerResult({
            "11001": 40, "51107": 25, "51059": 30, "06037": 92,
        })
    return _FakeLayerResult({})


class TestSpec089Backend:

    def test_haversine_basic(self):
        """T1: distance from DC to NYC is ~204 mi great-circle."""
        from app.services.atlas.trade_area import haversine_miles
        d = haversine_miles(38.91, -77.02, 40.71, -74.01)
        assert 195 < d < 215, d

    def test_haversine_identity(self):
        from app.services.atlas.trade_area import haversine_miles
        assert haversine_miles(38.91, -77.02, 38.91, -77.02) == 0.0

    def test_neighbors_within_radius_excludes_focal(self):
        """T2: focal not in neighbours list."""
        from app.services.atlas import trade_area as ta
        with patch.object(ta, "county_centroids", return_value=_FAKE_CENTROIDS), \
             patch.object(ta, "build_layer", side_effect=_fake_build_layer):
            r = ta.compute_trade_area(None, geo_id="11001", radius_mi=60)
        gids = {n["geo_id"] for n in r["neighbors"]}
        assert "11001" not in gids
        # DC↔Loudoun ≈ 34mi, DC↔Fairfax ≈ 14mi, LA out of range
        assert {"51107", "51059"} <= gids
        assert "06037" not in gids

    def test_radius_clamps(self):
        """T3: radius_mi clamped to [1, 250]."""
        from app.services.atlas import trade_area as ta
        with patch.object(ta, "county_centroids", return_value=_FAKE_CENTROIDS), \
             patch.object(ta, "build_layer", side_effect=_fake_build_layer):
            r_lo = ta.compute_trade_area(None, geo_id="11001", radius_mi=0.1)
            r_hi = ta.compute_trade_area(None, geo_id="11001", radius_mi=99999)
        assert r_lo["radius_mi"] == 1.0
        assert r_hi["radius_mi"] == 250.0

    def test_trade_area_returns_focal_and_summary(self):
        """T4: shape — focal has name + stats, summary has 5 keys."""
        from app.services.atlas import trade_area as ta
        with patch.object(ta, "county_centroids", return_value=_FAKE_CENTROIDS), \
             patch.object(ta, "build_layer", side_effect=_fake_build_layer):
            r = ta.compute_trade_area(None, geo_id="11001", radius_mi=60)
        assert r["focal"]["name"] == "District of Columbia"
        assert r["focal"]["income"] == 92000
        for k in ("n_neighbors", "avg_income", "avg_establishments",
                  "avg_broadband", "max_nri"):
            assert k in r["summary"]
        assert r["summary"]["n_neighbors"] >= 2

    def test_trade_area_unknown_geo(self):
        """T5: unknown geo_id returns an error payload, not a crash."""
        from app.services.atlas import trade_area as ta
        with patch.object(ta, "county_centroids", return_value=_FAKE_CENTROIDS), \
             patch.object(ta, "build_layer", side_effect=_fake_build_layer):
            r = ta.compute_trade_area(None, geo_id="00000", radius_mi=50)
        assert "error" in r
        assert r["focal"] is None

    def test_body_schema_trade_area(self):
        """T6: TradeAreaBody required geo_id; radius_mi default 50."""
        from app.api.v1.atlas import TradeAreaBody
        b = TradeAreaBody(geo_id="51107")
        assert b.geo_id == "51107"
        assert b.radius_mi == 50.0
        with pytest.raises(Exception):
            TradeAreaBody()  # geo_id required


class TestSpec089Frontend:

    def test_frontend_pin_click_enters_trade_area(self, html):
        """T7: the pin click handler routes through enterTradeArea."""
        assert "function enterTradeArea" in html
        # The pin's click handler must call enterTradeArea(cand.geo_id, ...)
        assert "enterTradeArea(cand.geo_id" in html

    def test_frontend_trade_area_card_markup(self, html):
        """T8: card + exit markup present."""
        assert 'id="trade-area-card"' in html
        assert 'id="trade-area-exit"' in html
        assert ".ta-focal-stats" in html
        assert ".ta-neighbors-list" in html
        # Exit button is wired
        assert "exitTradeArea" in html

    def test_frontend_trade_area_radius_default(self, html):
        """T9: pin handler defaults to 50-mile radius."""
        # The default arg appears in the pin click + the function signature
        m = re.search(r"enterTradeArea\(cand\.geo_id,\s*(\d+)", html)
        assert m and int(m.group(1)) == 50

    def test_frontend_circle_uses_meters(self, html):
        """L.circle takes radius in meters — should multiply by 1609.34."""
        assert "1609.34" in html
