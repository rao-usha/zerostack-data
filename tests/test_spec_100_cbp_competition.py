"""
Skeleton tests for SPEC 100 — CBP-backed competition lookup.
Fill in implementations before writing source code.
"""
import re
import pytest


class TestSpec100IndustryNAICS:
    """T1-T3 — industry_to_naics resolver."""

    def test_industry_to_naics_known_keywords(self):
        """T1: common decision-map industry labels → NAICS prefix."""
        from app.services.atlas.industry_naics import industry_to_naics
        # Retail family
        assert industry_to_naics("Furniture stores") == "442"
        assert industry_to_naics("home furnishings store") == "442"
        # Food service — multi-word keys beat single-word
        assert industry_to_naics("coffee shop") == "722515"
        assert industry_to_naics("Bar / drinking place") == "7224"
        # Industrial
        assert industry_to_naics("warehouse") == "493"
        assert industry_to_naics("Logistics & distribution") == "493"
        # Multi-word > single-word disambiguation
        assert industry_to_naics("auto repair") == "8111"
        # Word-boundary: "bar" must not be matched inside "barber"
        assert industry_to_naics("barber shop") == "812111"
        # Accent fold
        assert industry_to_naics("Café latte") == "722513"

    def test_industry_to_naics_falls_back_to_total(self):
        """T2: unknown label → None (caller uses NAICS='00')."""
        from app.services.atlas.industry_naics import industry_to_naics
        assert industry_to_naics("zorblax research collective") is None
        assert industry_to_naics("") is None
        assert industry_to_naics(None) is None
        # Whitespace-only stays None
        assert industry_to_naics("   ") is None

    def test_industry_to_naics_uses_explicit_naics_field(self):
        """T3: naics_hint short-circuits the label lookup."""
        from app.services.atlas.industry_naics import industry_to_naics
        # A clean 6-digit hint wins regardless of label.
        assert industry_to_naics("coffee shop", naics_hint="332710") == "332710"
        # A 2-digit hint is also valid.
        assert industry_to_naics("anything", naics_hint="44") == "44"
        # "31-33" range collapses to "31" for LIKE prefix matching.
        assert industry_to_naics("manufacturing", naics_hint="31-33") == "31"
        # A bogus hint is ignored — falls through to label match.
        assert industry_to_naics("coffee shop", naics_hint="not-a-naics") == "722515"
        # A bogus hint + an unknown label → None.
        assert industry_to_naics("zorblax", naics_hint="!!!") is None


class _FakeRow:
    """A row that mimics the named-attribute access SQLAlchemy returns."""
    def __init__(self, geo_id, establishments):
        self.geo_id = geo_id
        self.establishments = establishments


class _FakeDB:
    """Tiny mock for the db.execute(text(sql), params).all() pattern."""
    def __init__(self, rows):
        self._rows = rows
        self.last_params = None
    def execute(self, sql, params=None):
        self.last_params = params or {}
        class _R:
            def __init__(self_, rows): self_._rows = rows
            def all(self_):
                # Filter rows by params so the test exercises the SQL semantics
                geo_ids = self.last_params.get("geo_ids", [])
                naics = self.last_params.get("naics")
                out = [r for r in self._rows
                       if r.geo_id in geo_ids
                       and getattr(r, "_naics", naics) == naics]
                return out
            def scalar(self_): return None
        return _R(self._rows)
    def close(self): pass


# Mock centroids: (lat, lon, name)
_CENTROIDS = {
    "51107": (39.09, -77.64, "Loudoun"),
    "51059": (38.83, -77.28, "Fairfax"),
    "51610": (38.88, -77.17, "Falls Church"),
    "11001": (38.90, -77.02, "District of Columbia"),
}


class TestSpec100CBPLookup:
    """T4-T7 — find_competition_cbp SQL helper."""

    def test_find_competition_cbp_focal_only(self):
        """T4: one county, one NAICS — count + per_county[0] match."""
        from unittest.mock import patch
        from app.services.atlas.competition import find_competition_cbp
        # Tag rows with their NAICS so the fake-db filter can match
        rows = []
        r1 = _FakeRow("51107", 183); r1._naics = "442"; rows.append(r1)
        db = _FakeDB(rows)
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS):
            r = find_competition_cbp(
                db, focal_geo_id="51107",
                neighbor_geo_ids=[], naics="442")
        assert r["count"] == 183
        assert r["focal_count"] == 183
        assert r["neighbours_count"] == 0
        assert len(r["per_county"]) == 1
        assert r["per_county"][0]["geo_id"] == "51107"
        assert r["per_county"][0]["is_focal"] is True
        assert r["per_county"][0]["distance_mi"] == 0.0
        assert r["naics_used"] == "442"
        assert r["error"] is None

    def test_find_competition_cbp_focal_plus_neighbours(self):
        """T5: focal + 2 neighbours; per_county sorted by establishments
        descending; aggregate counts add up."""
        from unittest.mock import patch
        from app.services.atlas.competition import find_competition_cbp
        rows = []
        for gid, n in [("51107", 183), ("51059", 412), ("51610", 14)]:
            r = _FakeRow(gid, n); r._naics = "442"; rows.append(r)
        db = _FakeDB(rows)
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS):
            r = find_competition_cbp(
                db, focal_geo_id="51107",
                neighbor_geo_ids=["51059", "51610"],
                naics="442")
        assert r["count"] == 183 + 412 + 14
        assert r["focal_count"] == 183
        assert r["neighbours_count"] == 412 + 14
        # Per-county is sorted by establishments desc, with focal pinned
        # at index 0 (focal-first tie-break). Loudoun is focal, so 183
        # comes first even though Fairfax (412) is higher.
        assert r["per_county"][0]["geo_id"] == "51107"
        assert r["per_county"][0]["is_focal"] is True
        assert r["per_county"][1]["geo_id"] == "51059"  # next highest
        assert r["per_county"][2]["geo_id"] == "51610"
        # distance_mi for neighbours is > 0
        assert r["per_county"][1]["distance_mi"] > 0

    def test_find_competition_cbp_naics_prefix_match(self):
        """T6: querying a 4-digit NAICS picks up only the exact level.
        The backfill stores levels separately; aggregating across levels
        would double-count. So query for "4421" should NOT roll up
        underlying 442110 detail."""
        from unittest.mock import patch
        from app.services.atlas.competition import find_competition_cbp
        rows = []
        # Loudoun has rows at multiple NAICS levels — but the query is
        # exact-match, so only the 4421 row should be counted.
        for naics, n in [("442", 183), ("4421", 47), ("442110", 23)]:
            r = _FakeRow("51107", n); r._naics = naics; rows.append(r)
        db = _FakeDB(rows)
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS):
            r = find_competition_cbp(
                db, focal_geo_id="51107",
                neighbor_geo_ids=[], naics="4421")
        # Exact-match query → only the 4421 row picked up, NOT 442 or 442110
        assert r["count"] == 47
        assert r["naics_used"] == "4421"

    def test_find_competition_cbp_no_rows_soft_fails(self):
        """T7: empty CBP rowset → count=0, no exception, no error."""
        from unittest.mock import patch
        from app.services.atlas.competition import find_competition_cbp
        db = _FakeDB([])  # empty rowset
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS):
            r = find_competition_cbp(
                db, focal_geo_id="51107",
                neighbor_geo_ids=["51059"], naics="442")
        assert r["count"] == 0
        assert r["focal_count"] == 0
        assert r["neighbours_count"] == 0
        # per_county still has the requested geos with establishments=0
        assert len(r["per_county"]) == 2
        assert all(c["establishments"] == 0 for c in r["per_county"])
        assert r["error"] is None  # soft-success on empty


class TestSpec100Endpoint:
    """T8b — /competition endpoint resolves term → NAICS in new-shape body.

    Regression for a bug shipped in the first SPEC_100 commit: when the
    caller hit /competition with {focal_geo_id, term} but no `naics`,
    the new-shape branch dropped `term` on the floor and silently used
    NAICS='00' (all-establishments total), making the Trade Area card
    show the focal county's TOTAL instead of the term-specific count.
    """

    def test_new_shape_endpoint_resolves_term_to_naics(self):
        from unittest.mock import patch
        from app.api.v1.atlas import CompetitionBody, atlas_competition
        body = CompetitionBody(
            focal_geo_id="51107", radius_mi=50,
            term="Furniture stores",   # NO naics — endpoint must resolve
        )
        captured = {}
        def fake_cbp(db, focal_geo_id, neighbor_geo_ids, naics, year, **kw):
            # **kw absorbs the SPEC_102 `live_fallback` kwarg.
            captured["naics"] = naics
            return {"count": 70, "focal_count": 70, "neighbours_count": 0,
                    "per_county": [], "naics_used": naics or "00",
                    "naics_label": "x", "year": year,
                    "live_fetched": 0, "error": None}
        with patch("app.services.atlas.competition.find_competition_cbp",
                    side_effect=fake_cbp), \
             patch("app.services.atlas.competition._neighbor_geo_ids_for",
                    return_value=[]):
            atlas_competition(body, db=None)
        # "Furniture stores" → industry_to_naics → "442"
        assert captured["naics"] == "442", (
            f"endpoint should resolve term → NAICS, got {captured['naics']!r}")


class TestSpec100LegacyAdaptor:
    """T8 — old find_competition(lat,lon,term) still works."""

    def test_find_competition_legacy_adaptor(self):
        """T8: legacy (lat, lon, term) signature resolves to the new
        CBP path via nearest-centroid + industry_to_naics."""
        from unittest.mock import patch
        from app.services.atlas.competition import find_competition
        # Loudoun approx
        rows = []
        r1 = _FakeRow("51107", 183); r1._naics = "442"; rows.append(r1)
        db = _FakeDB(rows)
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS):
            r = find_competition(
                lat=39.09, lon=-77.64,
                radius_mi=5, term="Furniture stores",
                db=db)
        # term=Furniture stores → industry_to_naics → "442"
        assert r["naics_used"] == "442"
        assert r["count"] == 183
        # term echoed back
        assert r["term_used"] == "Furniture stores"
        # radius preserved
        assert r["radius_mi"] == 5.0


class TestSpec100PilotTool:
    """T9 — Pilot find_competition tool returns new shape."""

    def test_pilot_tool_find_competition_new_shape(self):
        """T9: dispatch with mocked centroids + CBP rows; verify the
        tool returns per_county (top) — NOT Yelp `businesses`."""
        from unittest.mock import patch
        from app.services.atlas import pilot_tools
        fake_cbp = {
            "count": 612, "focal_count": 183, "neighbours_count": 429,
            "per_county": [
                {"geo_id": "51107", "name": "Loudoun",
                 "establishments": 183, "distance_mi": 0.0,
                 "is_focal": True},
                {"geo_id": "51059", "name": "Fairfax",
                 "establishments": 429, "distance_mi": 22.3,
                 "is_focal": False},
            ],
            "naics_used": "442",
            "naics_label": "Retail trade",
            "year": 2022,
            "error": None,
        }
        with patch("app.services.atlas.trade_area.county_centroids",
                    return_value=_CENTROIDS), \
             patch("app.services.atlas.competition.find_competition_cbp",
                    return_value=fake_cbp), \
             patch("app.services.atlas.competition._neighbor_geo_ids_for",
                    return_value=["51059"]):
            r = pilot_tools.dispatch(
                None, "find_competition",
                {"geo_id": "51107", "radius_mi": 25, "term": "Furniture"})
        assert r["count"] == 612
        assert r["focal_count"] == 183
        assert r["neighbours_count"] == 429
        assert r["name"] == "Loudoun"
        assert r["naics_used"] == "442"
        # `top` is now the per_county breakdown, capped at 5
        assert len(r["top"]) == 2
        assert r["top"][0]["geo_id"] == "51107"
        # No Yelp leakage
        assert "businesses" not in r
        assert "rating" not in str(r["top"])


class TestSpec100Frontend:
    """T10 — Trade Area card reads CBP shape."""

    def test_trade_area_card_shows_cbp_count(self):
        """T10: updateCompetitionCardRow reads the SPEC_100 CBP shape:
        per_county / focal_count / neighbours_count / naics_used /
        naics_label / year. The old Yelp keys (businesses, rating,
        review_count, "open it on Yelp") are gone."""
        from pathlib import Path
        html = (Path(__file__).parent.parent / "frontend" /
                "atlas.html").read_text(encoding="utf-8")

        # New CBP shape keys are referenced in the helper
        assert "function updateCompetitionCardRow" in html
        assert "focal_count" in html
        assert "neighbours_count" in html
        assert "per_county" in html
        assert "naics_label" in html
        assert "naics_used" in html

        # Old Yelp shape gone from the live JS (comments referencing
        # SPEC_091 lineage are tolerated). We check that the strings
        # which would only appear in active code paths are absent.
        assert 'r.businesses' not in html
        assert 'Click an orange pin to open it on Yelp' not in html
        # renderCompetitionPins is still declared but is a no-op
        assert "function renderCompetitionPins" in html
        m = re.search(
            r"function renderCompetitionPins\([^)]*\)\s*\{(.+?)\}",
            html, re.DOTALL)
        assert m, "renderCompetitionPins not found"
        body = m.group(1)
        # No L.divIcon, L.marker, .addLayer etc — it's now a stub
        assert "L.divIcon" not in body
        assert "L.marker" not in body
        assert "addLayer" not in body

    def test_fetch_competition_supports_focal_geo_id(self):
        """T10b: fetchCompetition prefers focal_geo_id over lat/lon
        when caller supplies it — the new-shape endpoint short-circuits
        the nearest-centroid resolution server-side."""
        import re as _re
        from pathlib import Path
        html = (Path(__file__).parent.parent / "frontend" /
                "atlas.html").read_text(encoding="utf-8")
        m = _re.search(
            r"async function fetchCompetition\([^)]*\)\s*\{(.+?)\}\s*\n\s*function",
            html, _re.DOTALL)
        assert m, "fetchCompetition body not found"
        body = m.group(1)
        # New signature takes focalGeoId
        assert "focalGeoId" in body
        # New body shape uses focal_geo_id (new-shape) or lat/lon (legacy)
        assert "focal_geo_id" in body


class TestSpec100Ingest:
    """T11 — county_cbp ingest stores all NAICS levels."""

    def test_ingest_county_cbp_year_all_naics(self, monkeypatch):
        """T11: mock Census response with mixed NAICS levels (2/4/6
        digit) for one county; verify each row is upserted with the
        ACTUAL NAICS from the response (not the request param)."""
        from unittest.mock import patch, MagicMock
        from app.sources.census import county_cbp as mod

        # Census-shaped 2D response: header row + data rows
        fake_response = [
            ["NAME", "ESTAB", "EMP", "PAYANN", "NAICS2017", "state", "county"],
            ["Loudoun County, Virginia", "183", "1200", "55000", "442",   "51", "107"],
            ["Loudoun County, Virginia",  "47", "300",  "12000", "4421",  "51", "107"],
            ["Loudoun County, Virginia",  "23", "150",   "6000", "442110","51", "107"],
        ]
        mock_resp = MagicMock(); mock_resp.json.return_value = fake_response
        mock_resp.raise_for_status = MagicMock()
        mock_cli = MagicMock()
        mock_cli.get.return_value = mock_resp

        # Capture every batch payload the ingest tries to upsert
        captured: list = []
        class _DB:
            def execute(self, sql, params=None):
                if params is not None and not isinstance(params, dict):
                    # batch upsert — list of dicts
                    captured.extend(params)
                class _R:
                    def scalar(self_): return len(captured)
                return _R()
            def commit(self): pass
            def rollback(self): pass

        monkeypatch.setenv("CENSUS_API_KEY", "test-stub")
        with patch("app.sources.census.county_cbp.httpx.Client") as MockC:
            MockC.return_value.__enter__.return_value = mock_cli
            r = mod.ingest_county_cbp_year(_DB(), year=2022, naics_code="44")

        # Each NAICS level made it in with its ACTUAL code from the
        # response, not "44" from the request param.
        stored_naics = sorted({c["naics"] for c in captured})
        assert "442" in stored_naics
        assert "4421" in stored_naics
        assert "442110" in stored_naics
        # Geo + counts parsed
        assert captured[0]["geo_id"] == "51107"
        assert captured[0]["est"] == 183
        # Summary keys present
        assert r["year"] == 2022 and r["naics_code"] == "44"
        assert r["rows_inserted"] == 3

    def test_naics_2_sectors_list_complete(self):
        """T11b: NAICS_2_SECTORS holds the 20 semantic CBP sector codes
        (with the three combined codes 31-33 / 44-45 / 48-49 written
        out). NAICS_2_BACKFILL_PREFIXES holds the 24 single-code
        prefixes the backfill loop actually requests with `*` suffix.

        Why split: Census's `NAICS2017={prefix}*` wildcard returns
        deep detail only for single-code prefixes — `44-45*` returns
        the 2-digit aggregate alone. So the operational loop splits
        each combined sector. Verified 2026-06-03 live against the
        2022 CBP endpoint.
        """
        from app.sources.census.county_cbp import (
            NAICS_2_SECTORS, NAICS_2_BACKFILL_PREFIXES,
        )
        # Semantic list — 20 distinct codes
        assert len(set(NAICS_2_SECTORS)) == 20
        for required in ("11", "31-33", "44-45", "48-49", "62", "72"):
            assert required in NAICS_2_SECTORS
        # Operational list — 24 distinct prefixes, no hyphens
        assert len(set(NAICS_2_BACKFILL_PREFIXES)) == 24
        assert all("-" not in p for p in NAICS_2_BACKFILL_PREFIXES)
        # Each combined sector exploded out
        for half in ("31", "32", "33", "44", "45", "48", "49"):
            assert half in NAICS_2_BACKFILL_PREFIXES
