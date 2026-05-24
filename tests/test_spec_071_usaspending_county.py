"""
Tests for SPEC 071 — USAspending county-aggregate federal-dollars layer.

DB-backed tests skip when the new table is absent. Registry tests run
anywhere. T7 defends the legacy `usaspending_awards` row count — SPEC_071
must NOT touch the existing 10k-row stub.
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
        session.execute(text("SELECT 1 FROM usaspending_county_fy_totals LIMIT 1"))
    except Exception:
        try: session.rollback()
        except Exception: pass
        pytest.skip("usaspending_county_fy_totals absent — run "
                    "scripts/ingest_usaspending_county.py first")
    yield session
    session.close()


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


# ─── DB-backed ──────────────────────────────────────────────────────────

class TestIngestedData:

    def test_table_populated_for_fy2024(self, db):
        """T1: ≥2,500 contract rows for FY2024."""
        n = db.execute(text("""
            SELECT COUNT(*) FROM usaspending_county_fy_totals
            WHERE fiscal_year = 2024 AND award_type_group = 'contracts'
        """)).scalar()
        assert n >= 2500, f"expected ≥2,500 FY2024 contract rows, got {n}"

    def test_all_geo_ids_are_5_digit_county_fips(self, db):
        """T2: every geo_id is a 5-digit numeric county FIPS, no nulls."""
        rows = db.execute(text("SELECT geo_id FROM usaspending_county_fy_totals")).all()
        pattern = re.compile(r"^\d{5}$")
        bad = [r[0] for r in rows if not r[0] or not pattern.match(r[0])][:5]
        assert not bad, f"non-5-digit geo_ids found: {bad}"

    def test_total_obligation_realistic(self, db):
        """T3: aggregate sum across all counties is in [$100B, $1T] for
        FY2024 contracts — sanity check vs published USAspending totals."""
        total = db.execute(text("""
            SELECT SUM(total_obligation) FROM usaspending_county_fy_totals
            WHERE fiscal_year = 2024 AND award_type_group = 'contracts'
        """)).scalar() or 0
        assert 100_000_000_000 <= total <= 1_000_000_000_000, \
            f"FY2024 contract total ${total/1e9:.1f}B outside expected range"

    def test_no_zero_dollar_rows(self, db):
        """T8: all rows have total_obligation > 0 — USAspending
        spending_by_geography only returns counties with spending."""
        n_zero = db.execute(text("""
            SELECT COUNT(*) FROM usaspending_county_fy_totals
            WHERE total_obligation <= 0
        """)).scalar()
        assert n_zero == 0, f"{n_zero} rows have zero/negative obligation"

    def test_legacy_table_untouched(self, db):
        """T7: legacy usaspending_awards table is NOT modified."""
        n = db.execute(text("SELECT COUNT(*) FROM usaspending_awards")).scalar()
        # Was 10,190 at the time of SPEC_071 pre-flight; row count must
        # remain stable.
        assert n == 10190, \
            f"usaspending_awards row count changed unexpectedly: {n} (was 10,190)"


# ─── Registry / API ─────────────────────────────────────────────────────

class TestRegistryAndAPI:

    def test_layer_appears_in_registry(self, client):
        """T4: the new layer is exposed via /atlas/layers."""
        resp = client.get("/api/v1/atlas/layers")
        assert resp.status_code == 200
        registry = resp.json()["layers_by_domain"]
        all_ids = {l["id"] for layers in registry.values() for l in layers}
        assert "econ_federal_dollars" in all_ids

    def test_layer_endpoint_returns_county_values(self, client, db):
        """T5: the layer endpoint returns ≥2,500 values."""
        resp = client.get("/api/v1/atlas/layer/econ_federal_dollars")
        assert resp.status_code == 200
        body = resp.json()
        assert body["grain"] == "county"
        assert len(body.get("values", {})) >= 2500

    def test_excluded_by_design_removed_usaspending(self):
        """T6: usaspending_awards entry is gone from EXCLUDED_BY_DESIGN
        now that SPEC_071 backfills it."""
        from app.services.atlas.layers import EXCLUDED_BY_DESIGN
        assert "usaspending_awards" not in EXCLUDED_BY_DESIGN, \
            "SPEC_071 should remove usaspending_awards from EXCLUDED_BY_DESIGN"
