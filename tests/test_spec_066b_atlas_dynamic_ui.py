"""
Tests for SPEC 066b — Atlas Dynamic UI

The bulk of SPEC_066b is frontend (D3 + Observable Plot in atlas.html, no
Python) so the test surface here is narrow — only the small server-side
additions: place-level series and migration flows.

What we explicitly defend:
  - SERIES_CAPABLE keys MUST match real layer registry IDs. We had a bug
    where the keys were stale (`disaster_fema_declarations_per_county`
    instead of `disaster_fema_declarations`) and every sparkline silently
    failed. This test would have caught it.
  - The series endpoint returns the documented shape even for non-series
    layers (empty `points: []` rather than 404 — the UI gracefully hides).
  - The migration endpoint filters out IRS aggregation rows (state codes
    96/97/98) and returns only real US county FIPS pairs.
"""
import os
import pytest

from app.services.atlas import layers as layers_mod
from app.services.atlas.series import SERIES_CAPABLE


# ─── unit-style (no DB) ──────────────────────────────────────────────────

class TestSeriesCapableKeys:
    """T1 — every SERIES_CAPABLE key must be a real registered layer id."""

    def test_keys_match_layer_registry(self):
        registry_ids = set(layers_mod.LAYERS.keys())
        missing = [k for k in SERIES_CAPABLE if k not in registry_ids]
        assert not missing, (
            f"SERIES_CAPABLE references unknown layer ids: {missing}. "
            f"Sparklines would silently fail. Fix the keys to match "
            f"app/services/atlas/layers.py LAYERS."
        )


# ─── DB-backed tests — skipped without cloud ────────────────────────────

@pytest.fixture(scope="module")
def db():
    """Yield a real DB session, skipping if the geometry table is absent
    (i.e. local-only test environment)."""
    from app.core.database import get_db
    from sqlalchemy import text
    session = next(get_db())
    try:
        session.execute(text("SELECT 1 FROM irs_soi_migration LIMIT 1"))
    except Exception:
        try: session.rollback()
        except Exception: pass
        pytest.skip("irs_soi_migration absent — series tests need the cloud DB")
    yield session
    session.close()


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from app.main import app
    return TestClient(app)


class TestPlaceSeriesEndpoint:

    def test_unsupported_layer_returns_empty_points(self, client, db):
        """T2 — a non-series-capable layer returns 200 with `points: []`
        (UI gracefully hides the sparkline rather than failing)."""
        resp = client.get("/api/v1/atlas/place/48201/series",
                          params={"layer": "disaster_nri"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["layer_id"] == "disaster_nri"
        assert body["points"] == []

    def test_fema_per_year_returns_points(self, client, db):
        """T3 — FEMA-per-year for Harris Co returns ≥5 years of data."""
        resp = client.get("/api/v1/atlas/place/48201/series",
                          params={"layer": "disaster_fema_declarations"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["kind"] == "fema_per_year"
        assert len(body["points"]) >= 5
        for p in body["points"]:
            assert isinstance(p["x"], int)
            assert isinstance(p["y"], int)

    def test_fdic_quarterly_returns_points(self, client, db):
        """T4 — FDIC quarterly for Harris Co returns ≥100 quarters."""
        resp = client.get("/api/v1/atlas/place/48201/series",
                          params={"layer": "finance_fdic_county_deposits"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["kind"] == "fdic_quarterly"
        assert len(body["points"]) >= 100

    def test_invalid_geo_id_400(self, client, db):
        """T5 — non-FIPS geo_id returns 400."""
        resp = client.get("/api/v1/atlas/place/abc/series",
                          params={"layer": "disaster_fema_declarations"})
        assert resp.status_code == 400


class TestMigrationEndpoint:

    def test_top_flows_default(self, client, db):
        """T6 — /migration?top_n=10 returns 10 real county-pair flows."""
        resp = client.get("/api/v1/atlas/migration", params={"top_n": 10})
        assert resp.status_code == 200
        body = resp.json()
        assert body["n"] == 10
        assert body["tax_year"] >= 2018
        for f in body["flows"]:
            # both FIPS must be real US states (01-56) — no IRS 96/97/98 aggregations
            assert f["orig"][:2] in {f"{i:02d}" for i in range(1, 57)}
            assert f["dest"][:2] in {f"{i:02d}" for i in range(1, 57)}
            assert f["orig"] != f["dest"]
            assert f["agi"] > 0

    def test_top_n_bounds(self, client, db):
        """T7 — top_n outside [1, 1000] returns 400."""
        resp = client.get("/api/v1/atlas/migration", params={"top_n": 0})
        assert resp.status_code == 400
        resp = client.get("/api/v1/atlas/migration", params={"top_n": 9999})
        assert resp.status_code == 400
