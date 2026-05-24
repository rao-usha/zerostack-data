"""
SPEC 065 — Atlas Layer-Data + Boundaries API.

Tests split:
  * Pure-Python (registry shape, exclusions, ordering): no DB.
  * Postgres-backed (against cloud — that's where the layer source data
    lives): use `pg_session` against `DATABASE_URL` (the api container is
    on cloud). Skips if DATABASE_URL points only at local docker postgres
    and `geojson_boundaries` is absent.
"""
import os
import pytest
from unittest.mock import MagicMock


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://nexdata:nexdata_dev_password@postgres:5432/nexdata",
)


# ─────────────────────────────────────────────────────────────────────────────
# Pure-Python tests (no DB)
# ─────────────────────────────────────────────────────────────────────────────

class TestRegistryShape:

    def test_registry_has_layers_across_domains(self):
        """T1: ≥12 layers across ≥6 domains; required fields present."""
        from app.services.atlas.layers import LAYERS, list_layers_by_domain
        assert len(LAYERS) >= 12, f"only {len(LAYERS)} layers registered"
        grouped = list_layers_by_domain()
        assert len(grouped) >= 6, f"only {len(grouped)} domains"
        # Every layer carries the required fields
        for layer_id, spec in LAYERS.items():
            for f in ("id", "label", "domain", "grain", "default_on",
                      "vintage", "coverage_note", "unit", "description", "builder"):
                assert hasattr(spec, f), f"layer {layer_id} missing {f}"

    def test_registry_honest_cuts_state(self):
        """T2: the honest-cut layers PLAN_066 §4 originally deferred are
        now backfilled — defend that they're genuinely present.

        History:
        - SPEC_065: registry deferred ACS-county-wealth + usaspending_awards
          via EXCLUDED_BY_DESIGN.
        - SPEC_070 (2026-05-23): implemented ACS-county-wealth as
          `demo_acs_median_income` (table `acs5_county_2023_b19013`);
          removed from EXCLUDED_BY_DESIGN.
        - SPEC_071 (2026-05-23): implemented federal-dollars as
          `econ_federal_dollars` (table `usaspending_county_fy_totals`);
          removed `usaspending_awards` from EXCLUDED_BY_DESIGN.

        The test now defends presence of the backfilled layers and the
        absence of the legacy stub-table names in the registry.
        """
        from app.services.atlas.layers import LAYERS, EXCLUDED_BY_DESIGN
        ids = set(LAYERS)
        # Legacy stub tables must not appear as layer ids
        assert not any("usaspending_awards" == lid for lid in ids), \
            "the usable layer is econ_federal_dollars, not the legacy stub"
        # SPEC_070 — ACS county wealth backfilled
        assert "demo_acs_median_income" in ids, \
            "SPEC_070 should have added demo_acs_median_income"
        # SPEC_071 — federal dollars backfilled
        assert "econ_federal_dollars" in ids, \
            "SPEC_071 should have added econ_federal_dollars"
        # EXCLUDED_BY_DESIGN itself remains a real mechanism even if currently empty
        assert isinstance(EXCLUDED_BY_DESIGN, dict)

    def test_registry_groups_by_domain(self):
        """T3: list_layers_by_domain returns dict keyed by domain, sorted."""
        from app.services.atlas.layers import list_layers_by_domain
        grouped = list_layers_by_domain()
        assert isinstance(grouped, dict)
        domains = list(grouped.keys())
        assert domains == sorted(domains), "domains must be sorted deterministically"
        for dom, layers in grouped.items():
            assert isinstance(layers, list) and len(layers) >= 1
            ids = [l["id"] for l in layers]
            assert ids == sorted(ids), f"layers within {dom} must be sorted"

    def test_layer_declares_honest_grain(self):
        """T12: each layer's declared grain matches data reality (FCC=state, CBP=state)."""
        from app.services.atlas.layers import LAYERS
        # FCC layer must be state grain (no county data in the table — PLAN_066 §4)
        assert LAYERS["infra_fcc_providers_state"].grain == "state"
        assert "state" in LAYERS["infra_fcc_providers_state"].coverage_note.lower()
        # CBP layer must be state grain (county partial — PLAN_067 SPEC_073)
        assert LAYERS["econ_cbp_establishments_state"].grain == "state"
        # IRS county must be county grain (county_code IS 5-digit FIPS — corrected)
        assert LAYERS["demo_irs_county_agi_per_return"].grain == "county"
        # FDIC must be county grain via the cert→stcnty join
        assert LAYERS["finance_fdic_county_deposits"].grain == "county"

    def test_safe_query_rolls_back_on_failure(self):
        """T11: _safe_query swallows + rolls back (the SPEC_061 lesson)."""
        from app.services.atlas.layers import _safe_query
        db = MagicMock()
        db.execute.side_effect = RuntimeError("simulated query failure")
        result = _safe_query(db, "SELECT 1")
        assert result == []
        db.rollback.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# Postgres-backed tests — cloud DB is where the data lives
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def pg_session():
    if not DATABASE_URL:
        pytest.skip("DATABASE_URL not set")
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(DATABASE_URL, future=True)
    Session = sessionmaker(bind=engine, future=True)
    session = Session()
    # Skip the live-data tests if we're on a DB without the geometry table
    # (the local docker postgres). A cheap probe.
    try:
        session.execute(__import__('sqlalchemy').text(
            "SELECT 1 FROM geojson_boundaries LIMIT 1"))
        session.rollback()
    except Exception:
        session.rollback()
        session.close()
        pytest.skip("geojson_boundaries absent — tests need the cloud DB")
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def client(pg_session):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.api.v1 import atlas
    from app.core.database import get_db

    app = FastAPI()
    app.include_router(atlas.router, prefix="/api/v1")

    def _override():
        try:
            yield pg_session
        finally:
            pass

    app.dependency_overrides[get_db] = _override
    return TestClient(app)


class TestLayerBuilders:

    def test_choropleth_layer_returns_fips_value_map(self, pg_session):
        """T4: a registered choropleth layer returns {geo_id: number}."""
        from app.services.atlas.layers import build_layer
        result = build_layer(pg_session, "disaster_nri")
        assert result.grain == "county"
        assert result.values is not None and len(result.values) >= 3000
        # Sample shape: all values numeric; majority of keys are 5-digit FIPS
        # (NRI source data has some non-padded / territory entries — that's
        # data reality, not a layer-builder bug).
        sample = list(result.values.items())[:20]
        for fips, val in sample:
            assert isinstance(fips, str) and fips
            assert isinstance(val, (int, float))
        five_digit_pct = sum(1 for k in result.values if len(k) == 5) / len(result.values)
        assert five_digit_pct > 0.9, (
            f"only {five_digit_pct:.0%} of keys are 5-digit FIPS — investigate")

    def test_point_layer_returns_geojson_featurecollection(self, pg_session):
        """T5: a registered point layer returns valid GeoJSON features."""
        from app.services.atlas.layers import build_layer
        result = build_layer(pg_session, "energy_power_plants")
        assert result.grain == "point"
        assert result.features is not None and len(result.features) > 100
        f0 = result.features[0]
        assert f0["type"] == "Feature"
        assert f0["geometry"]["type"] == "Point"
        coords = f0["geometry"]["coordinates"]
        assert len(coords) == 2  # [lon, lat]

    def test_fdic_layer_county_join(self, pg_session):
        """T7: FDIC layer values keyed by 5-digit stcnty via institutions join."""
        from app.services.atlas.layers import build_layer
        result = build_layer(pg_session, "finance_fdic_county_deposits")
        assert result.grain == "county"
        assert result.values is not None and len(result.values) > 100
        # All keys are 5-digit county FIPS strings (the join produced stcnty)
        for geo_id in list(result.values)[:10]:
            assert isinstance(geo_id, str) and len(geo_id) == 5

    def test_irs_county_uses_county_code_directly(self, pg_session):
        """T8: IRS county layer keys on county_code (5-digit FIPS)."""
        from app.services.atlas.layers import build_layer
        result = build_layer(pg_session, "demo_irs_county_agi_per_return")
        assert result.grain == "county"
        assert result.values is not None and len(result.values) >= 2000
        # The corrected derivation: county_code IS the 5-digit FIPS, not a concat
        for geo_id in list(result.values)[:10]:
            assert isinstance(geo_id, str) and len(geo_id) == 5 and geo_id.isdigit()


class TestEndpoints:

    def test_unknown_layer_id_returns_404(self, client):
        """T6: GET /layer/does-not-exist → 404."""
        resp = client.get("/api/v1/atlas/layer/does-not-exist")
        assert resp.status_code == 404

    def test_boundaries_endpoint_returns_county_geojson(self, client):
        """T9: GET /boundaries?geo_level=county → FeatureCollection with ≥3,000 features.

        Hardened 2026-05-23: also asserts each feature has a Leaflet-renderable
        geometry — `type` ∈ {Polygon, MultiPolygon} and non-empty coordinates.
        The original assertion missed a bug where the `geojson_boundaries`
        column stored a wrapped Feature and `_simplify_python` returned it
        verbatim, producing `geometry.type == "Feature"` with no coordinates
        — Leaflet refused to render the result.
        """
        resp = client.get("/api/v1/atlas/boundaries", params={"geo_level": "county"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["type"] == "FeatureCollection"
        assert len(body["features"]) >= 3000
        # Each feature carries geo_id
        assert "geo_id" in body["features"][0]["properties"]
        # Each feature has a Leaflet-renderable geometry — sample the first 50
        for f in body["features"][:50]:
            g = f.get("geometry") or {}
            assert g.get("type") in ("Polygon", "MultiPolygon"), \
                f"feature {f['properties']['geo_id']} has wrong geometry type: {g.get('type')!r}"
            coords = g.get("coordinates")
            assert coords, \
                f"feature {f['properties']['geo_id']} has empty coordinates"

    def test_place_endpoint_returns_multilayer_aggregate(self, client):
        """T10: GET /place/48201 (Harris County) returns ≥4 layer values."""
        resp = client.get("/api/v1/atlas/place/48201")
        assert resp.status_code == 200
        body = resp.json()
        assert body["geo_id"] == "48201"
        assert body["grain"] == "county"
        assert len(body["layers"]) >= 4, \
            f"expected ≥4 layers for Harris County, got {len(body['layers'])}"
        # Each layer entry has the expected shape
        for layer_id, info in body["layers"].items():
            for field in ("label", "domain", "grain", "value", "unit"):
                assert field in info, f"{layer_id} missing {field}"
