"""Tests for SPEC_102 — live Census CBP fallback for missing cells."""
from unittest.mock import patch, MagicMock

import pytest


# ────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────
def _mock_census_200(estab: int = 47, emp: int = 300, pay: int = 12000):
    """Build a fake Census 200 response with one row at NAICS=442."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = [
        ["NAME", "ESTAB", "EMP", "PAYANN", "NAICS2017", "state", "county"],
        ["Loudoun County, Virginia", str(estab), str(emp), str(pay),
         "442", "51", "107"],
    ]
    resp.raise_for_status = MagicMock()
    return resp


def _mock_census_204():
    resp = MagicMock(); resp.status_code = 204
    resp.raise_for_status = MagicMock()
    return resp


class _CapturingDB:
    """Tiny stand-in for the real Session — records UPSERT params."""
    def __init__(self):
        self.upserts = []
    def execute(self, sql, params=None):
        if params is not None:
            self.upserts.append(params)
        class _R:
            def scalar(self_): return None
            def all(self_): return []
        return _R()
    def commit(self): pass
    def rollback(self): pass


# ────────────────────────────────────────────────────────────────────
# T1-T4 — fetch_cbp_cell unit tests
# ────────────────────────────────────────────────────────────────────
class TestSpec102FetchCell:

    def setup_method(self):
        from app.services.atlas.cbp_live import clear_cache
        clear_cache()

    def test_fetch_cbp_cell_happy_path(self, monkeypatch):
        """T1: mocked 200 → returns establishment int."""
        monkeypatch.setenv("CENSUS_SURVEY_API_KEY", "test-stub")
        from app.services.atlas import cbp_live
        db = _CapturingDB()
        with patch.object(cbp_live.httpx, "Client") as MockC:
            cli = MagicMock()
            cli.get.return_value = _mock_census_200(estab=47)
            MockC.return_value.__enter__.return_value = cli
            n = cbp_live.fetch_cbp_cell(db, "51107", "442", year=2022)
        assert n == 47

    def test_fetch_cbp_cell_returns_none_on_204(self, monkeypatch):
        """T2: Census 204 = truly missing → returns None."""
        monkeypatch.setenv("CENSUS_SURVEY_API_KEY", "test-stub")
        from app.services.atlas import cbp_live
        db = _CapturingDB()
        with patch.object(cbp_live.httpx, "Client") as MockC:
            cli = MagicMock()
            cli.get.return_value = _mock_census_204()
            MockC.return_value.__enter__.return_value = cli
            n = cbp_live.fetch_cbp_cell(db, "51107", "999999", year=2022)
        assert n is None
        # No UPSERT for a None result
        assert db.upserts == []

    def test_fetch_cbp_cell_caches(self, monkeypatch):
        """T3: same key inside the 60s window = no second API call."""
        monkeypatch.setenv("CENSUS_SURVEY_API_KEY", "test-stub")
        from app.services.atlas import cbp_live
        db = _CapturingDB()
        with patch.object(cbp_live.httpx, "Client") as MockC:
            cli = MagicMock()
            cli.get.return_value = _mock_census_200(estab=47)
            MockC.return_value.__enter__.return_value = cli
            a = cbp_live.fetch_cbp_cell(db, "51107", "442", year=2022)
            b = cbp_live.fetch_cbp_cell(db, "51107", "442", year=2022)
            c = cbp_live.fetch_cbp_cell(db, "51107", "442", year=2022)
        assert a == b == c == 47
        assert cli.get.call_count == 1     # not 3

    def test_fetch_cbp_cell_upserts_to_table(self, monkeypatch):
        """T4: successful fetch UPSERTs (year, geo_id, naics, estab)."""
        monkeypatch.setenv("CENSUS_SURVEY_API_KEY", "test-stub")
        from app.services.atlas import cbp_live
        db = _CapturingDB()
        with patch.object(cbp_live.httpx, "Client") as MockC:
            cli = MagicMock()
            cli.get.return_value = _mock_census_200(estab=47, emp=300, pay=12000)
            MockC.return_value.__enter__.return_value = cli
            cbp_live.fetch_cbp_cell(db, "51107", "442", year=2022)
        assert len(db.upserts) == 1
        u = db.upserts[0]
        assert u == {"y": 2022, "g": "51107", "n": "442",
                     "e": 47, "emp": 300, "pay": 12000}


# ────────────────────────────────────────────────────────────────────
# T5-T7, T9 — find_competition_cbp(live_fallback=True) wiring
# ────────────────────────────────────────────────────────────────────
class _Row:
    def __init__(self, geo_id, n): self.geo_id = geo_id; self.establishments = n


class _FakeCompDB:
    def __init__(self, rows): self._rows = rows
    def execute(self, sql, params=None):
        rows = self._rows
        if params and "geo_ids" in params:
            geo_ids = params["geo_ids"]; naics = params.get("naics")
            rows = [r for r in rows
                    if r.geo_id in geo_ids
                    and getattr(r, "_naics", naics) == naics]
        class _R:
            def __init__(self_, rs): self_._r = rs
            def all(self_): return self_._r
            def scalar(self_): return None
        return _R(rows)
    def commit(self): pass


_CENTROIDS = {
    "51107": (39.09, -77.64, "Loudoun"),
    "51059": (38.83, -77.28, "Fairfax"),
    "51610": (38.88, -77.17, "Falls Church"),
}


class TestSpec102FindCompetitionFallback:

    def setup_method(self):
        from app.services.atlas.cbp_live import clear_cache
        clear_cache()

    def test_find_competition_cbp_falls_back(self):
        """T5: focal county missing; live_fallback fetches & merges."""
        from app.services.atlas.competition import find_competition_cbp
        # Only Fairfax has a row; Loudoun is missing → live fallback.
        r1 = _Row("51059", 186); r1._naics = "442"
        db = _FakeCompDB([r1])
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS), \
             patch("app.services.atlas.cbp_live.fetch_cbp_cell",
                    return_value=70):
            r = find_competition_cbp(
                db, focal_geo_id="51107",
                neighbor_geo_ids=["51059"],
                naics="442", live_fallback=True)
        assert r["focal_count"] == 70
        assert r["neighbours_count"] == 186
        assert r["count"] == 256
        assert r["live_fetched"] == 1

    def test_find_competition_cbp_respects_per_request_cap(self):
        """T6: more misses than the per-request cap → first N fetched,
        rest stay 0. Default cap is 30; we stub the cap to 1 to keep
        the test small."""
        from app.services.atlas.competition import find_competition_cbp
        db = _FakeCompDB([])  # nothing in DB → every cell is a miss
        calls = []
        def _fake_fetch(db_, geo_id, naics, year=2022):
            calls.append(geo_id); return 5
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS), \
             patch("app.services.atlas.cbp_live.fetch_cbp_cell",
                    side_effect=_fake_fetch):
            r = find_competition_cbp(
                db, focal_geo_id="51107",
                neighbor_geo_ids=["51059", "51610"],
                naics="442", live_fallback=True,
                live_fallback_cap=1)
        # Only the first miss was fetched
        assert len(calls) == 1
        assert r["live_fetched"] == 1
        # Total count = 5 (just the focal that got fetched)
        assert r["focal_count"] == 5
        assert r["count"] == 5

    def test_find_competition_cbp_live_fallback_false(self):
        """T7: live_fallback=False behaves like SPEC_100 — no fetches,
        no live_fetched counter."""
        from app.services.atlas.competition import find_competition_cbp
        db = _FakeCompDB([])
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS), \
             patch("app.services.atlas.cbp_live.fetch_cbp_cell") as mock_fetch:
            r = find_competition_cbp(
                db, focal_geo_id="51107",
                neighbor_geo_ids=["51059"],
                naics="442", live_fallback=False)
        mock_fetch.assert_not_called()
        assert r["count"] == 0
        assert r.get("live_fetched", 0) == 0

    def test_live_fetched_counter_in_response(self):
        """T9: response always includes live_fetched: int (≥0)."""
        from app.services.atlas.competition import find_competition_cbp
        r1 = _Row("51107", 47); r1._naics = "442"
        db = _FakeCompDB([r1])
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS):
            r = find_competition_cbp(
                db, focal_geo_id="51107",
                neighbor_geo_ids=[], naics="442",
                live_fallback=True)
        assert "live_fetched" in r
        assert r["live_fetched"] == 0  # focal was already in DB


# ────────────────────────────────────────────────────────────────────
# T8 — soft-fail
# ────────────────────────────────────────────────────────────────────
class TestSpec102SoftFail:

    def setup_method(self):
        from app.services.atlas.cbp_live import clear_cache
        clear_cache()

    def test_fetch_cbp_cell_soft_fails_on_timeout(self, monkeypatch):
        """T8: Census timeout → None, no exception escapes."""
        monkeypatch.setenv("CENSUS_SURVEY_API_KEY", "test-stub")
        from app.services.atlas import cbp_live
        db = _CapturingDB()
        with patch.object(cbp_live.httpx, "Client") as MockC:
            cli = MagicMock()
            cli.get.side_effect = TimeoutError("simulated timeout")
            MockC.return_value.__enter__.return_value = cli
            n = cbp_live.fetch_cbp_cell(db, "51107", "442", year=2022)
        assert n is None
        # And the result is cached as None (so we don't retry every request)
        assert ("51107", "442", 2022) in cbp_live._CACHE

    def test_fetch_cbp_cell_no_key_returns_none(self, monkeypatch):
        """T8b: missing key → None + log WARN."""
        monkeypatch.delenv("CENSUS_SURVEY_API_KEY", raising=False)
        monkeypatch.delenv("CENSUS_API_KEY", raising=False)
        from app.services.atlas import cbp_live
        db = _CapturingDB()
        n = cbp_live.fetch_cbp_cell(db, "51107", "442", year=2022)
        assert n is None
