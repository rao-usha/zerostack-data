"""
SPEC 062 — Diligence Orders Model + Intake API.

Two test surfaces:
  * Postgres-backed: T1, T2, T11, T12 use a `pg_session` fixture that connects
    to the local docker postgres and cleans test rows with the 'spec062-'
    prefix on setup/teardown. Matches the SPEC_053/054/055 convention.
  * FastAPI TestClient: T3-T10 mount the diligence-pack router on an isolated
    app and assert via HTTP semantics.
"""
import os
import pytest
from unittest.mock import MagicMock, patch

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://nexdata:nexdata_dev_password@postgres:5432/nexdata",
)


# ─────────────────────────────────────────────────────────────────────────────
# Postgres fixture
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def pg_session():
    """Real postgres session for service-layer tests; cleans test rows."""
    if not DATABASE_URL:
        pytest.skip("DATABASE_URL not set")
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(DATABASE_URL, future=True)
    Session = sessionmaker(bind=engine, future=True)
    session = Session()
    # Instantiate once so _ensure_tables() runs before cleanup.
    from app.services.diligence.orders import DiligenceOrderService
    DiligenceOrderService(session)
    session.execute(text(
        "DELETE FROM diligence_orders WHERE contact_email LIKE 'spec062-%'"
    ))
    session.commit()
    try:
        yield session
    finally:
        session.execute(text(
            "DELETE FROM diligence_orders WHERE contact_email LIKE 'spec062-%'"
        ))
        session.commit()
        session.close()


# ─────────────────────────────────────────────────────────────────────────────
# TestClient fixture — mounts only the diligence-pack router so we don't
# accidentally collide with the legacy /diligence router.
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def client(pg_session):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.api.v1 import diligence_pack
    from app.core.database import get_db

    app = FastAPI()
    app.include_router(diligence_pack.router, prefix="/api/v1")

    def _override_get_db():
        try:
            yield pg_session
        finally:
            pass

    app.dependency_overrides[get_db] = _override_get_db
    return TestClient(app)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec062OrdersIntake:

    # ── T1 ────────────────────────────────────────────────────────────────
    def test_request_happy_path_creates_order_row(self, client, pg_session):
        from sqlalchemy import text
        resp = client.post("/api/v1/diligence-pack/request", json={
            "contact_name": "Alex T",
            "contact_email": "spec062-happy@example.com",
            "naics_code": "2382",
            "sku": "single_map",
            "geography_mode": "msa",
            "msa_code": "26420",
            "contact_org": "Acme Search Fund",
            "client_note": "Houston HVAC roll-up thesis.",
        })
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "requested"
        assert body["sku"] == "single_map"
        assert body["order_id"] > 0
        # Row exists with the right denormalized fields
        row = pg_session.execute(
            text("SELECT contact_email, naics_code, naics_label, msa_code, msa_title, sku, status "
                 "FROM diligence_orders WHERE id = :id"),
            {"id": body["order_id"]},
        ).mappings().first()
        assert row["contact_email"] == "spec062-happy@example.com"
        assert row["naics_code"] == "2382"
        assert row["naics_label"]  # denormalized
        assert row["msa_code"] == "26420"
        assert "Houston" in (row["msa_title"] or "")
        assert row["sku"] == "single_map"
        assert row["status"] == "requested"

    # ── T2 ────────────────────────────────────────────────────────────────
    def test_request_returns_correct_stripe_link_per_sku(self, client, monkeypatch):
        # Override settings with fake Stripe URLs
        from app.core.config import get_settings
        settings = get_settings()
        monkeypatch.setattr(settings, "stripe_payment_link_url_2500",
                            "https://buy.stripe.com/test_2500", raising=False)
        monkeypatch.setattr(settings, "stripe_payment_link_url_7500",
                            "https://buy.stripe.com/test_7500", raising=False)

        for sku, expected_url in [
            ("single_map",    "https://buy.stripe.com/test_2500"),
            ("pilot_3_maps",  "https://buy.stripe.com/test_7500"),
        ]:
            resp = client.post("/api/v1/diligence-pack/request", json={
                "contact_name": "T2",
                "contact_email": f"spec062-t2-{sku}@example.com",
                "naics_code": "2382",
                "sku": sku,
                "geography_mode": "msa",
                "msa_code": "26420",
            })
            assert resp.status_code == 200, resp.text
            assert resp.json()["payment_url"] == expected_url

        # Retainer should return None payment_url + sales-led next step.
        resp = client.post("/api/v1/diligence-pack/request", json={
            "contact_name": "T2",
            "contact_email": "spec062-t2-retainer@example.com",
            "naics_code": "2382",
            "sku": "retainer",
            "geography_mode": "msa",
            "msa_code": "26420",
        })
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["payment_url"] is None
        assert "retainer" in body["next_step"].lower()

    # ── T3 ────────────────────────────────────────────────────────────────
    def test_request_unknown_naics_returns_422(self, client):
        resp = client.post("/api/v1/diligence-pack/request", json={
            "contact_name": "T3",
            "contact_email": "spec062-t3@example.com",
            "naics_code": "99999",
            "sku": "single_map",
            "geography_mode": "msa",
            "msa_code": "26420",
        })
        assert resp.status_code == 422
        assert "NAICS" in resp.text or "naics" in resp.text

    # ── T4 ────────────────────────────────────────────────────────────────
    def test_request_non_4digit_naics_returns_422(self, client):
        for bad in ["33", "332", "332323"]:
            resp = client.post("/api/v1/diligence-pack/request", json={
                "contact_name": "T4",
                "contact_email": "spec062-t4@example.com",
                "naics_code": bad,
                "sku": "single_map",
                "geography_mode": "msa",
                "msa_code": "26420",
            })
            assert resp.status_code == 422, f"NAICS {bad}: got {resp.status_code}: {resp.text}"

    # ── T5 ────────────────────────────────────────────────────────────────
    def test_request_unknown_msa_returns_422(self, client):
        resp = client.post("/api/v1/diligence-pack/request", json={
            "contact_name": "T5",
            "contact_email": "spec062-t5@example.com",
            "naics_code": "2382",
            "sku": "single_map",
            "geography_mode": "msa",
            "msa_code": "00000",
        })
        assert resp.status_code == 422
        assert "MSA" in resp.text or "msa" in resp.text

    # ── T6 ────────────────────────────────────────────────────────────────
    def test_request_missing_required_fields_returns_422(self, client):
        # missing contact_email
        resp = client.post("/api/v1/diligence-pack/request", json={
            "contact_name": "T6",
            "naics_code": "2382",
            "sku": "single_map",
            "geography_mode": "msa",
            "msa_code": "26420",
        })
        assert resp.status_code == 422
        # missing naics_code
        resp = client.post("/api/v1/diligence-pack/request", json={
            "contact_name": "T6",
            "contact_email": "spec062-t6@example.com",
            "sku": "single_map",
            "geography_mode": "msa",
            "msa_code": "26420",
        })
        assert resp.status_code == 422

    # ── T7 ────────────────────────────────────────────────────────────────
    def test_request_invalid_sku_returns_422(self, client):
        resp = client.post("/api/v1/diligence-pack/request", json={
            "contact_name": "T7",
            "contact_email": "spec062-t7@example.com",
            "naics_code": "2382",
            "sku": "gold_plan",
            "geography_mode": "msa",
            "msa_code": "26420",
        })
        assert resp.status_code == 422

    # ── T8 ────────────────────────────────────────────────────────────────
    def test_request_notify_email_failure_does_not_block(self, client, monkeypatch):
        from app.core.config import get_settings
        settings = get_settings()
        # Force notify-email path to fire
        monkeypatch.setattr(settings, "diligence_notify_email",
                            "ops@example.com", raising=False)
        # Make the email service raise on import — exact error pattern from a
        # broken downstream isn't important; the helper must swallow ANY exception.
        with patch("app.services.email.get_email_service",
                   side_effect=RuntimeError("simulated email outage")):
            resp = client.post("/api/v1/diligence-pack/request", json={
                "contact_name": "T8",
                "contact_email": "spec062-t8@example.com",
                "naics_code": "2382",
                "sku": "single_map",
                "geography_mode": "msa",
                "msa_code": "26420",
            })
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "requested"

    # ── T9 ────────────────────────────────────────────────────────────────
    def test_skus_endpoint_returns_pricing(self, client):
        resp = client.get("/api/v1/diligence-pack/skus")
        assert resp.status_code == 200
        skus = resp.json()["skus"]
        codes = {s["sku"] for s in skus}
        assert codes == {"single_map", "pilot_3_maps", "retainer"}
        for s in skus:
            assert "price_display" in s and s["price_display"]
            assert "copy" in s and s["copy"]
            assert isinstance(s["has_payment_url"], bool)

    # ── T10 ───────────────────────────────────────────────────────────────
    def test_taxonomies_endpoint_returns_naics_and_msa(self, client):
        resp = client.get("/api/v1/diligence-pack/taxonomies")
        assert resp.status_code == 200
        body = resp.json()
        # NAICS is a list of {sector_code, sector_label, industries:[{code,label}]}
        assert isinstance(body["naics"], list) and body["naics"]
        first_sector = body["naics"][0]
        assert {"sector_code", "sector_label", "industries"} <= set(first_sector.keys())
        # MSA is a flat list of {cbsa_code, title, state_abbrs, county_count}
        assert isinstance(body["msa"], list) and len(body["msa"]) >= 380
        first_msa = body["msa"][0]
        assert {"cbsa_code", "title", "state_abbrs", "county_count"} <= set(first_msa.keys())

    # ── T11 ───────────────────────────────────────────────────────────────
    def test_table_migration_idempotent(self, pg_session):
        from app.services.diligence.orders import DiligenceOrderService
        # First call already ran via the fixture; this is the second.
        DiligenceOrderService(pg_session)  # should not raise

    # ── T12 ───────────────────────────────────────────────────────────────
    def test_state_mode_intake_works(self, client, pg_session):
        from sqlalchemy import text
        resp = client.post("/api/v1/diligence-pack/request", json={
            "contact_name": "T12",
            "contact_email": "spec062-t12@example.com",
            "naics_code": "2382",
            "sku": "single_map",
            "geography_mode": "state",
            "state_fips": "48",
        })
        assert resp.status_code == 200, resp.text
        row = pg_session.execute(text(
            "SELECT geography_mode, state_fips, msa_code "
            "FROM diligence_orders WHERE id = :id"),
            {"id": resp.json()["order_id"]},
        ).mappings().first()
        assert row["geography_mode"] == "state"
        assert row["state_fips"] == "48"
        assert row["msa_code"] is None
