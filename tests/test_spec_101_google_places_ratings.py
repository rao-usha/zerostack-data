"""Skeleton tests for SPEC_101 — Google Places ratings for competition."""
import pytest


class TestSpec101GoogleClient:
    """T1 — client construction + auth."""

    def test_google_client_uses_env_key(self, monkeypatch):
        """T1: client reads GOOGLE_PLACES_API_KEY from env at construction."""
        from app.sources.google_places import GooglePlacesClient
        monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "test-key-abc")
        c = GooglePlacesClient()
        assert c.api_key == "test-key-abc"
        assert c.has_key is True
        # Override wins
        c2 = GooglePlacesClient(api_key="other-key")
        assert c2.api_key == "other-key"
        # Missing key → has_key=False, search returns []
        monkeypatch.delenv("GOOGLE_PLACES_API_KEY")
        c3 = GooglePlacesClient()
        assert c3.has_key is False
        # And searching without a key returns [] (soft-fail)
        assert c3.search_nearby(39.0, -77.6, 5000, ["furniture_store"]) == []


class TestSpec101CategoryMap:
    """T2-T3 — NAICS → Google Place Type."""

    def test_naics_to_category_known_industries(self):
        """T2: exact-match NAICS resolves to the right Place Type."""
        from app.services.atlas.google_place_categories import (
            naics_to_google_category,
        )
        assert naics_to_google_category("442") == "furniture_store"
        assert naics_to_google_category("442110") == "furniture_store"
        assert naics_to_google_category("722515") == "cafe"
        assert naics_to_google_category("722511") == "restaurant"
        assert naics_to_google_category("722513") == "fast_food_restaurant"
        assert naics_to_google_category("445110") == "supermarket"
        assert naics_to_google_category("447110") == "gas_station"
        # Walk-up: a deeper code that isn't in the map resolves
        # via the closest ancestor.
        assert naics_to_google_category("44211") == "furniture_store"  # via "4421"->"442"

    def test_naics_to_category_unknown_falls_back(self):
        """T3: unknown NAICS → None (caller skips the Google call)."""
        from app.services.atlas.google_place_categories import (
            naics_to_google_category,
        )
        assert naics_to_google_category(None) is None
        assert naics_to_google_category("") is None
        assert naics_to_google_category("99") is None
        assert naics_to_google_category("923456") is None


class TestSpec101FetchRatings:
    """T4-T7 — fetch_ratings happy path + cache + soft-fail."""

    def test_fetch_ratings_happy_path(self, monkeypatch):
        """T4: mocked Google response → RatingSummary populated."""
        from unittest.mock import patch
        monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "k")
        from app.services.atlas import ratings as rmod
        # Mock the cache table I/O so we don't need a real DB.
        with patch.object(rmod, "_ensure_table"), \
             patch.object(rmod, "_read_cache", return_value=None), \
             patch.object(rmod, "_write_cache"), \
             patch("app.services.atlas.trade_area.county_centroids",
                    return_value={"51107": (39.09, -77.64, "Loudoun")}), \
             patch("app.sources.google_places.GooglePlacesClient") as MockC:
            MockC.return_value.has_key = True
            MockC.return_value.search_nearby.return_value = [
                {"id": "p1", "rating": 4.5, "userRatingCount": 80},
                {"id": "p2", "rating": 4.0, "userRatingCount": 120},
                {"id": "p3", "rating": 3.0, "userRatingCount": 20},
                {"id": "p4", "rating": None},   # ignored (no rating)
            ]
            summary = rmod.fetch_ratings(db=object(), geo_id="51107", naics="442")
        assert summary is not None
        assert summary.count == 3                # one dropped (rating=None)
        assert abs(summary.mean_rating - (4.5 + 4.0 + 3.0)/3) < 1e-6
        assert summary.median_rating == 4.0
        # Distribution buckets (rounded ratings) — Python's banker's
        # rounding sends 4.5 → 4 (round-half-to-even).
        assert summary.rating_distribution[3] == 1   # 3.0
        assert summary.rating_distribution[4] == 2   # 4.0 + 4.5
        assert summary.rating_distribution[5] == 0
        assert summary.total_user_ratings == 220
        # API response shape includes attribution + no place_id leak
        resp = summary.to_response()
        assert resp["attribution"] == "Powered by Google"
        assert "id" not in resp
        assert "p1" not in str(resp)

    def test_fetch_ratings_caches_30d(self, monkeypatch):
        """T5: cache hit short-circuits the Google call entirely."""
        from unittest.mock import patch
        from app.services.atlas import ratings as rmod
        monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "k")
        cached = rmod.RatingSummary(count=5, mean_rating=4.2,
                                     median_rating=4.0,
                                     rating_distribution={4: 3, 5: 2},
                                     total_user_ratings=100)
        with patch.object(rmod, "_read_cache", return_value=cached), \
             patch("app.sources.google_places.GooglePlacesClient") as MockC:
            r = rmod.fetch_ratings(db=object(), geo_id="51107", naics="442")
        assert r is cached
        MockC.return_value.search_nearby.assert_not_called()

    def test_fetch_ratings_soft_fails_missing_key(self, monkeypatch):
        """T6: no GOOGLE_PLACES_API_KEY → None, no exception."""
        from unittest.mock import patch
        monkeypatch.delenv("GOOGLE_PLACES_API_KEY", raising=False)
        from app.services.atlas import ratings as rmod
        with patch.object(rmod, "_read_cache", return_value=None):
            r = rmod.fetch_ratings(db=object(), geo_id="51107", naics="442")
        assert r is None

    def test_fetch_ratings_soft_fails_on_quota(self, monkeypatch):
        """T7: Google returns empty places (quota / 429 / etc) → None."""
        from unittest.mock import patch
        monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "k")
        from app.services.atlas import ratings as rmod
        with patch.object(rmod, "_read_cache", return_value=None), \
             patch("app.services.atlas.trade_area.county_centroids",
                    return_value={"51107": (39.09, -77.64, "Loudoun")}), \
             patch("app.sources.google_places.GooglePlacesClient") as MockC:
            MockC.return_value.has_key = True
            MockC.return_value.search_nearby.return_value = []  # quota → []
            r = rmod.fetch_ratings(db=object(), geo_id="51107", naics="442")
        assert r is None

    def test_fetch_ratings_unknown_naics_skips_call(self, monkeypatch):
        """Bonus: unknown NAICS → no Google call at all (saves quota)."""
        from unittest.mock import patch
        monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "k")
        from app.services.atlas import ratings as rmod
        with patch("app.sources.google_places.GooglePlacesClient") as MockC:
            r = rmod.fetch_ratings(db=object(), geo_id="51107",
                                    naics="999999")  # not in map
        assert r is None
        MockC.assert_not_called()


class TestSpec101Endpoint:
    """T8-T9, T12 — /competition endpoint shape + ToS guarantees."""

    def test_competition_endpoint_includes_ratings_when_key_set(self, monkeypatch):
        """T8: when GOOGLE_PLACES_API_KEY is set + a NAICS resolves to a
        category + fetch_ratings returns a summary → result.ratings is
        the to_response() dict."""
        from unittest.mock import patch
        from app.api.v1.atlas import CompetitionBody, atlas_competition
        from app.services.atlas.ratings import RatingSummary
        monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "k")
        body = CompetitionBody(focal_geo_id="51107", term="Furniture stores")

        fake_summary = RatingSummary(count=12, mean_rating=4.3,
                                      median_rating=4.0,
                                      rating_distribution={3: 2, 4: 6, 5: 4},
                                      total_user_ratings=487)

        with patch("app.services.atlas.competition.find_competition_cbp",
                    return_value={"count": 70, "focal_count": 70,
                                  "neighbours_count": 0, "per_county": [],
                                  "naics_used": "442", "naics_label": "x",
                                  "year": 2022, "live_fetched": 0,
                                  "error": None}), \
             patch("app.services.atlas.competition._neighbor_geo_ids_for",
                    return_value=[]), \
             patch("app.services.atlas.ratings.fetch_ratings",
                    return_value=fake_summary):
            r = atlas_competition(body, db=None)
        assert r["ratings"] is not None
        assert r["ratings"]["mean_rating"] == 4.3
        assert r["ratings"]["attribution"] == "Powered by Google"

    def test_competition_endpoint_omits_ratings_without_key(self, monkeypatch):
        """T9: no GOOGLE_PLACES_API_KEY → result.ratings is None."""
        from unittest.mock import patch
        from app.api.v1.atlas import CompetitionBody, atlas_competition
        monkeypatch.delenv("GOOGLE_PLACES_API_KEY", raising=False)
        body = CompetitionBody(focal_geo_id="51107", term="Furniture stores")
        with patch("app.services.atlas.competition.find_competition_cbp",
                    return_value={"count": 70, "focal_count": 70,
                                  "neighbours_count": 0, "per_county": [],
                                  "naics_used": "442", "naics_label": "x",
                                  "year": 2022, "live_fetched": 0,
                                  "error": None}), \
             patch("app.services.atlas.competition._neighbor_geo_ids_for",
                    return_value=[]):
            r = atlas_competition(body, db=None)
        assert "ratings" in r
        assert r["ratings"] is None

    def test_no_place_id_leakage(self, monkeypatch):
        """T12 — ToS regression. No `id`/`place_id` field, no Google
        place ID strings, no review text in the /competition response.

        Google's place_id format is a long opaque string starting with
        'ChIJ' or 'GhIJ'. We assert (a) the to_response() dict has no
        'id' or 'place_id' keys and (b) no ChIJ/GhIJ prefixes leak
        into the JSON dump."""
        import json
        from unittest.mock import patch
        from app.api.v1.atlas import CompetitionBody, atlas_competition
        from app.services.atlas.ratings import RatingSummary
        monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "k")
        body = CompetitionBody(focal_geo_id="51107", term="Furniture stores")
        # Build a summary as if Google had returned real place_ids
        summary = RatingSummary(count=2, mean_rating=4.0, median_rating=4.0,
                                 rating_distribution={4: 2}, total_user_ratings=20)
        with patch("app.services.atlas.competition.find_competition_cbp",
                    return_value={"count": 70, "focal_count": 70,
                                  "neighbours_count": 0, "per_county": [],
                                  "naics_used": "442", "naics_label": "x",
                                  "year": 2022, "live_fetched": 0,
                                  "error": None}), \
             patch("app.services.atlas.competition._neighbor_geo_ids_for",
                    return_value=[]), \
             patch("app.services.atlas.ratings.fetch_ratings",
                    return_value=summary):
            r = atlas_competition(body, db=None)
        s = json.dumps(r)
        # ToS — no Google place_id field name or prefix
        assert "place_id" not in s.lower()
        assert "ChIJ" not in s
        assert "GhIJ" not in s
        # Ratings block exists but only aggregates
        assert "id" not in r["ratings"]
        assert "places" not in r["ratings"]


class TestSpec101Frontend:
    """T10 — Trade Area card ratings strip + attribution."""

    def test_trade_area_card_renders_ratings_strip(self):
        """T10: updateCompetitionCardRow reads r.ratings and renders
        the strip with "Powered by Google" attribution. CSS classes
        for the strip and attribution exist."""
        from pathlib import Path
        html = (Path(__file__).parent.parent / "frontend" /
                "atlas.html").read_text(encoding="utf-8")
        # JS — reads the new shape
        assert "r.ratings" in html
        assert "mean_rating" in html
        assert "median_rating" in html
        assert "total_user_ratings" in html
        # Hardcoded ToS attribution string
        assert "Powered by Google" in html
        # CSS classes for the strip
        assert ".ta-comp-ratings" in html
        assert ".ta-comp-attribution" in html
        # The strip is only shown when r.ratings is truthy — verify the
        # gating condition exists in the JS.
        assert "if (r && r.ratings)" in html


class TestSpec101Cache:
    """T11 — cache table schema."""

    def test_cache_table_schema(self):
        """T11: _create_cache_table_sql lists every required column +
        the PK + the asked_at index."""
        from app.services.atlas.ratings import _create_cache_table_sql
        sql = _create_cache_table_sql()
        for needle in ("google_places_cache",
                        "geo_id", "naics_code", "asked_at",
                        "summary_json", "JSONB",
                        "PRIMARY KEY (geo_id, naics_code)",
                        "idx_gpc_asked_at"):
            assert needle in sql, f"cache table SQL missing {needle!r}"

    def test_cache_table_write_roundtrip(self, monkeypatch):
        """T11b: _write_cache + _read_cache roundtrip on a real DB."""
        from app.core.database import get_session_factory
        from app.services.atlas.ratings import (
            _write_cache, _read_cache, _ensure_table, RatingSummary,
        )
        Session = get_session_factory()
        db = Session()
        try:
            _ensure_table(db)
            # Clear any leftover from prior runs
            from sqlalchemy import text as _t
            db.execute(_t("DELETE FROM google_places_cache WHERE geo_id='99999'"))
            db.commit()
            s = RatingSummary(count=3, mean_rating=4.0, median_rating=4.0,
                              rating_distribution={4: 2, 5: 1},
                              total_user_ratings=87)
            _write_cache(db, "99999", "999", s)
            r = _read_cache(db, "99999", "999")
            assert r is not None
            assert r.count == 3
            assert r.mean_rating == 4.0
            assert r.total_user_ratings == 87
            # Cleanup
            db.execute(_t("DELETE FROM google_places_cache WHERE geo_id='99999'"))
            db.commit()
        finally:
            db.close()
