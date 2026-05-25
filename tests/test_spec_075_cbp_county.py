"""
Tests for SPEC 075 — County CBP multi-year.
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
        session.execute(text("SELECT 1 FROM census_cbp_county_yearly LIMIT 1"))
    except Exception:
        try: session.rollback()
        except Exception: pass
        pytest.skip("census_cbp_county_yearly absent — run "
                    "scripts/ingest_cbp_county.py first")
    yield session
    session.close()


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


class TestIngestedData:

    def test_at_least_15k_rows(self, db):
        """T1: ≥15,000 rows post-ingest (5 years × ~3,000 counties)."""
        n = db.execute(text("SELECT COUNT(*) FROM census_cbp_county_yearly")).scalar()
        assert n >= 15000, f"expected ≥15k rows, got {n}"

    def test_geo_ids_are_5_digit_fips(self, db):
        """T2: every geo_id is 5-digit county FIPS."""
        rows = db.execute(text("SELECT DISTINCT geo_id FROM census_cbp_county_yearly")).all()
        pat = re.compile(r"^\d{5}$")
        bad = [r[0] for r in rows if not pat.match(r[0] or "")][:5]
        assert not bad, f"non-5-digit geo_ids: {bad}"

    def test_each_year_has_county_rows(self, db):
        """T3: each year 2018-2022 has ≥2,500 county rows."""
        for yr in (2018, 2019, 2020, 2021, 2022):
            n = db.execute(text(
                "SELECT COUNT(*) FROM census_cbp_county_yearly WHERE year = :y"
            ), {"y": yr}).scalar()
            assert n >= 2500, f"year {yr} has only {n} rows (expected ≥2,500)"

    def test_establishment_counts_realistic(self, db):
        """T6: sum of establishments across all counties is in [5M, 20M]
        per year (US has ~8M business establishments)."""
        for yr in (2020, 2021, 2022):
            total = db.execute(text("""
                SELECT SUM(establishments) FROM census_cbp_county_yearly
                WHERE year = :y AND naics_code = '00'
            """), {"y": yr}).scalar() or 0
            assert 5_000_000 <= total <= 20_000_000, \
                f"year {yr} total establishments {total:,} outside [5M, 20M]"


class TestRegistryAndAPI:

    def test_layer_in_registry(self, client, db):
        """T4a: /atlas/layers includes econ_cbp_establishments_county."""
        body = client.get("/api/v1/atlas/layers").json()
        all_ids = {l["id"] for layers in body["layers_by_domain"].values()
                            for l in layers}
        assert "econ_cbp_establishments_county" in all_ids

    def test_layer_returns_county_values(self, client, db):
        """T4b: /atlas/layer returns ≥3,000 county values."""
        body = client.get("/api/v1/atlas/layer/econ_cbp_establishments_county").json()
        assert body["grain"] == "county"
        assert len(body.get("values", {})) >= 3000

    def test_cascade_endpoint(self, client, db):
        """T5: cascade returns multi-year per-county counts."""
        body = client.get(
            "/api/v1/atlas/layer/econ_cbp_establishments_county/cascade"
        ).json()
        assert "years" in body and "values_by_year" in body
        years = body["years"]
        assert len(years) >= 5
        # Each year has a non-trivial value dict
        sample_year = str(max(years))
        year_values = body["values_by_year"][sample_year]
        assert len(year_values) >= 2500
        pat = re.compile(r"^\d{5}$")
        for k in list(year_values)[:30]:
            assert pat.match(k), f"non-FIPS geo_id in cascade: {k!r}"
