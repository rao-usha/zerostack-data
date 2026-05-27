"""
Tests for SPEC 077 — Phase A foundation.

Covers A.2 (sub-county boundary ingest) primarily. A.1 (basemap) is
frontend-only and verified by smoke harness. A.3 (tract ACS) +
A.4 (zoom defaults) add tests as they ship.
"""
import re
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text


@pytest.fixture(scope="module")
def db():
    from app.core.database import get_db
    session = next(get_db())
    try:
        # Phase A.2 must have run; tract rows must exist
        n = session.execute(text(
            "SELECT COUNT(*) FROM geojson_boundaries WHERE geo_level = 'tract'"
        )).scalar()
        if not n or n < 1000:
            pytest.skip(
                f"tract boundaries absent or sparse ({n} rows) — "
                f"run scripts/ingest_tiger_boundaries.py first"
            )
    except Exception:
        try: session.rollback()
        except Exception: pass
        pytest.skip("geojson_boundaries absent — needs cloud DB")
    yield session
    session.close()


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


class TestTractIngest:

    def test_tract_count_at_least_70k(self, db):
        """T1: ≥70,000 tract rows post-ingest."""
        n = db.execute(text(
            "SELECT COUNT(*) FROM geojson_boundaries WHERE geo_level = 'tract'"
        )).scalar()
        assert n >= 70000, f"expected ≥70,000 tract rows, got {n}"

    def test_tract_geo_ids_are_11_digit(self, db):
        """T2: every tract geo_id is 11-digit (state+county+tract)."""
        rows = db.execute(text(
            "SELECT DISTINCT geo_id FROM geojson_boundaries "
            "WHERE geo_level = 'tract' LIMIT 200"
        )).all()
        pat = re.compile(r"^\d{11}$")
        bad = [r[0] for r in rows if not pat.match(r[0] or "")][:5]
        assert not bad, f"non-11-digit tract geo_ids: {bad}"


class TestZctaIngest:

    def test_zcta_count_at_least_30k(self, db):
        """T3: ≥30,000 ZCTA rows post-ingest."""
        n = db.execute(text(
            "SELECT COUNT(*) FROM geojson_boundaries WHERE geo_level = 'zcta'"
        )).scalar()
        if not n or n < 1000:
            pytest.skip("ZCTA ingest hasn't run yet")
        assert n >= 30000, f"expected ≥30,000 ZCTA rows, got {n}"

    def test_zcta_geo_ids_are_5_digit(self, db):
        """T4: every ZCTA geo_id is 5-digit numeric."""
        rows = db.execute(text(
            "SELECT DISTINCT geo_id FROM geojson_boundaries "
            "WHERE geo_level = 'zcta' LIMIT 200"
        )).all()
        if not rows:
            pytest.skip("ZCTA ingest hasn't run yet")
        pat = re.compile(r"^\d{5}$")
        bad = [r[0] for r in rows if not pat.match(r[0] or "")][:5]
        assert not bad, f"non-5-digit ZCTA geo_ids: {bad}"


class TestBoundaryEndpoint:

    def test_tract_endpoint_returns_feature_collection(self, client, db):
        """T5: /atlas/boundaries?geo_level=tract returns a real FeatureCollection."""
        resp = client.get("/api/v1/atlas/boundaries",
                          params={"geo_level": "tract"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["type"] == "FeatureCollection"
        assert len(body["features"]) >= 70000

    def test_zcta_endpoint_returns_feature_collection(self, client, db):
        """T6: /atlas/boundaries?geo_level=zcta returns a real FeatureCollection."""
        n_z = db.execute(text(
            "SELECT COUNT(*) FROM geojson_boundaries WHERE geo_level = 'zcta'"
        )).scalar()
        if not n_z or n_z < 1000:
            pytest.skip("ZCTA ingest hasn't run yet")
        resp = client.get("/api/v1/atlas/boundaries",
                          params={"geo_level": "zcta"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["type"] == "FeatureCollection"
        assert len(body["features"]) >= 30000

    def test_unknown_geo_level_400(self, client):
        """T7: unsupported geo_level returns 400."""
        resp = client.get("/api/v1/atlas/boundaries",
                          params={"geo_level": "imaginary"})
        assert resp.status_code == 400
