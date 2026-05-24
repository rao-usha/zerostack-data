"""
Tests for SPEC 070 — ACS county-grain median household income.

T1-T3 are DB-backed (skip if cloud unreachable). T4-T6 verify the layer
registry wire-up — these run anywhere.
"""
import re
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text


# ─── fixtures ────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def db():
    from app.core.database import get_db
    session = next(get_db())
    try:
        session.execute(text("SELECT 1 FROM acs5_county_2023_b19013 LIMIT 1"))
    except Exception:
        try: session.rollback()
        except Exception: pass
        pytest.skip("acs5_county_2023_b19013 absent — run "
                    "scripts/ingest_acs_county_b19013.py first")
    yield session
    session.close()


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


# ─── DB-backed ──────────────────────────────────────────────────────────

class TestIngestedData:

    def test_ingest_produces_county_rows(self, db):
        """T1: ≥3,000 rows post-ingest (3,143 total US counties)."""
        n = db.execute(text("SELECT COUNT(*) FROM acs5_county_2023_b19013")).scalar()
        assert n >= 3000, f"expected ≥3,000 county rows, got {n}"

    def test_all_geo_ids_are_5_digit_county_fips(self, db):
        """T2: every geo_id is a 5-digit county FIPS — no ZCTAs, no
        2-digit state codes."""
        rows = db.execute(text("SELECT geo_id FROM acs5_county_2023_b19013")).all()
        pattern = re.compile(r"^\d{5}$")
        bad = [r[0] for r in rows if not pattern.match(r[0] or "")][:5]
        assert not bad, f"non-5-digit geo_ids found: {bad}"
        # Real US state prefixes are 01-72 (territories ≤ 72); reject
        # IRS-style aggregations (96/97/98) which would indicate the
        # ingest leaked migration codes.
        bad_state = [r[0] for r in rows if r[0] and r[0][:2] in {"96", "97", "98"}][:5]
        assert not bad_state, f"non-real state prefixes: {bad_state}"

    def test_no_collision_with_zcta_table(self, db):
        """T3: the legacy ZCTA table is undisturbed."""
        # The ZCTA table existed pre-ingest with 33,772 rows; if anyone
        # accidentally writes county data there it would change.
        n_zcta = db.execute(text("SELECT COUNT(*) FROM acs5_2023_b19013")).scalar()
        assert n_zcta == 33772, \
            f"ZCTA table row count changed unexpectedly: {n_zcta}"

    def test_value_format_is_realistic(self, db):
        """T7: median income values fall in [10000, 250000]."""
        row = db.execute(text("""
            SELECT MIN(b19013_001e), MAX(b19013_001e), AVG(b19013_001e)
            FROM acs5_county_2023_b19013
            WHERE b19013_001e IS NOT NULL AND b19013_001e > 0
        """)).first()
        mn, mx, avg = row
        assert 10000 <= mn <= 50000, f"unrealistic min income: {mn}"
        assert 100000 <= mx <= 300000, f"unrealistic max income: {mx}"
        assert 40000 <= avg <= 100000, f"unrealistic avg income: {avg}"


# ─── Registry / API ─────────────────────────────────────────────────────

class TestRegistryAndAPI:

    def test_layer_appears_in_registry(self, client):
        """T4: the new layer is exposed via /atlas/layers."""
        resp = client.get("/api/v1/atlas/layers")
        assert resp.status_code == 200
        registry = resp.json()["layers_by_domain"]
        all_ids = {layer["id"] for layers in registry.values() for layer in layers}
        assert "demo_acs_median_income" in all_ids

    def test_layer_endpoint_returns_county_values(self, client, db):
        """T5: the layer endpoint returns ≥3,000 values."""
        resp = client.get("/api/v1/atlas/layer/demo_acs_median_income")
        assert resp.status_code == 200
        body = resp.json()
        assert body["grain"] == "county"
        assert len(body.get("values", {})) >= 3000

    def test_excluded_by_design_entry_removed(self):
        """T6: the deferred-ACS-county-wealth entry in EXCLUDED_BY_DESIGN
        is gone now that the layer is real."""
        from app.services.atlas.layers import EXCLUDED_BY_DESIGN
        for k in EXCLUDED_BY_DESIGN:
            assert "acs5_2023_b19013_as_county" not in k, \
                f"EXCLUDED_BY_DESIGN still contains the now-implemented ACS county layer: {k}"
