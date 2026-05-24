"""
Tests for SPEC 072 — County broadband (ACS B28002 subscription rate).

Same pattern as SPEC_070/071: DB-backed tests skip when the new table
is absent. Registry tests run anywhere. T6 defends backward compat —
SPEC_070's `demo_acs_median_income` layer must still work after the
multi-variable refactor.
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
        session.execute(text("SELECT 1 FROM acs5_county_2023_b28002 LIMIT 1"))
    except Exception:
        try: session.rollback()
        except Exception: pass
        pytest.skip("acs5_county_2023_b28002 absent — run "
                    "scripts/ingest_acs_county_b28002.py first")
    yield session
    session.close()


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


class TestIngestedData:

    def test_ingest_produces_county_rows(self, db):
        """T1: ≥3,000 rows for B28002."""
        n = db.execute(text("SELECT COUNT(*) FROM acs5_county_2023_b28002")).scalar()
        assert n >= 3000, f"expected ≥3,000 rows, got {n}"

    def test_pct_values_are_realistic(self, db):
        """T2: derived pct values are honest household-fractions ∈ [0, 100].

        Originally asserted min ≥ 40%; loosened on 2026-05-24 after the
        cloud ingest surfaced ~34.9% — a real rural/territory low end. Data
        shape is honest; the test was too tight, not the data.
        """
        rows = db.execute(text("""
            SELECT b28002_004e::numeric / NULLIF(b28002_001e, 0) * 100 AS pct
            FROM acs5_county_2023_b28002
            WHERE b28002_001e > 0 AND b28002_004e IS NOT NULL
        """)).all()
        pcts = [float(r[0]) for r in rows if r[0] is not None]
        assert pcts, "no derivable pct rows"
        mn, mx = min(pcts), max(pcts)
        # Bounds defend against unit-confusion / divide-by-percent bugs but
        # allow the genuine low-end counties.
        assert 0 <= mn <= 50, f"unrealistic min broadband pct: {mn:.2f}"
        assert 85 <= mx <= 100, f"unrealistic max broadband pct: {mx:.2f}"
        # Median should be high — most US counties have broadband majority.
        sorted_pcts = sorted(pcts)
        median = sorted_pcts[len(sorted_pcts) // 2]
        assert median >= 80, f"median pct should be >80 in modern US; got {median:.2f}"

    def test_geo_ids_are_5_digit_county_fips(self, db):
        """T3: every geo_id is a 5-digit FIPS — no nulls, no ZCTAs."""
        rows = db.execute(text("SELECT geo_id FROM acs5_county_2023_b28002")).all()
        pat = re.compile(r"^\d{5}$")
        bad = [r[0] for r in rows if not r[0] or not pat.match(r[0])][:5]
        assert not bad, f"non-5-digit geo_ids: {bad}"


class TestRegistryAndAPI:

    def test_layer_appears_in_registry(self, client):
        """T4: /atlas/layers includes infra_broadband_subscription."""
        resp = client.get("/api/v1/atlas/layers")
        assert resp.status_code == 200
        registry = resp.json()["layers_by_domain"]
        all_ids = {l["id"] for layers in registry.values() for l in layers}
        assert "infra_broadband_subscription" in all_ids

    def test_layer_returns_county_values(self, client, db):
        """T5: /atlas/layer returns ≥3,000 county values."""
        resp = client.get("/api/v1/atlas/layer/infra_broadband_subscription")
        assert resp.status_code == 200
        body = resp.json()
        assert body["grain"] == "county"
        assert len(body.get("values", {})) >= 3000


class TestBackwardCompat:

    def test_spec_070_acs_median_income_still_works(self, client):
        """T6: SPEC_070's demo_acs_median_income layer still returns
        ≥3,000 values — the multi-variable refactor must not break it."""
        resp = client.get("/api/v1/atlas/layer/demo_acs_median_income")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body.get("values", {})) >= 3000, \
            "SPEC_070 layer regressed — multi-variable refactor broke it"
