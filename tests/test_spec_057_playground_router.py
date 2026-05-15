"""
Tests for SPEC 057 — Playground API Router + Admin Router (PLAN_063 C4 / Step 6).

Integration tests via FastAPI TestClient against an app that mounts the
playground, playground-admin, and auth routers. They run in-container against
real Postgres and skip without DATABASE_URL. Function-scoped fixture cleans
quota buckets + `spec057-` rows + test reports each test for isolation.
"""
import os
import uuid
from datetime import datetime

import pytest

# Small free-tier limit so the "authed limit" test stays fast.
os.environ.setdefault("PLAYGROUND_FREE_RUNS_PER_DAY", "2")
os.environ.setdefault("PLAYGROUND_ANON_RUNS_PER_IP", "1")

from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402


def _db_url():
    return os.getenv("DATABASE_URL")


@pytest.fixture
def pg_app():
    """A TestClient app mounting playground + playground-admin + auth routers,
    backed by real Postgres. Skips without DATABASE_URL. Function-scoped:
    cleans quota buckets, spec057- rows, and test reports each invocation."""
    db_url = _db_url()
    if not db_url:
        pytest.skip("DATABASE_URL not set — playground router tests run in-container only")
    try:
        engine = create_engine(db_url)
        engine.connect().close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Postgres unreachable: {exc}")

    os.environ.setdefault("JWT_SECRET_KEY", "spec057-test-secret-key-not-for-prod")
    from app.core.config import reset_settings
    reset_settings()

    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.api.v1 import auth as auth_router
    from app.api.v1 import playground as playground_router
    from app.api.v1 import playground_admin as playground_admin_router

    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v1")
    app.include_router(playground_router.router, prefix="/api/v1")
    app.include_router(playground_admin_router.router, prefix="/api/v1")

    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    test_start = datetime.utcnow()

    def _cleanup():
        db.execute(text("DELETE FROM playground_quota_buckets "
                        "WHERE subject_key = 'testclient' OR subject_key LIKE 'spec057%'"))
        db.execute(text("DELETE FROM playground_runs WHERE lead_id IN "
                        "(SELECT id FROM leads WHERE email LIKE 'spec057-%') "
                        "OR anon_ip = 'testclient'"))
        db.execute(text("DELETE FROM leads WHERE email LIKE 'spec057-%'"))
        db.execute(text("DELETE FROM login_codes WHERE email LIKE 'spec057-%'"))
        db.execute(text("DELETE FROM refresh_tokens WHERE user_id IN "
                        "(SELECT id FROM users WHERE email LIKE 'spec057-%')"))
        db.execute(text("DELETE FROM users WHERE email LIKE 'spec057-%'"))
        db.execute(text("DELETE FROM reports WHERE template = 'synthetic_playground' "
                        "AND created_at >= :ts"), {"ts": test_start})
        db.commit()

    # ensure tables exist (services create them on first construction)
    from app.services.quota.quota_service import QuotaService
    from app.services.leads.lead_service import LeadService
    from app.users.auth import AuthService
    QuotaService(db)
    LeadService(db)
    AuthService(db)
    _cleanup()

    client = TestClient(app)
    try:
        yield client, db
    finally:
        _cleanup()
        db.close()
        engine.dispose()
        reset_settings()


def _issue_token(db, email: str) -> str:
    """Create a passwordless user and return a valid access token."""
    from app.users.auth import AuthService
    svc = AuthService(db)
    issued = svc.request_login_code(email)
    bundle = svc.verify_login_code(email, issued["code"])
    return bundle["access_token"]


_RUN_BODY = {"generator": "macro-scenarios", "params": {"n_scenarios": 3, "horizon_months": 6, "seed": 42}}


@pytest.mark.unit
class TestSpec057PlaygroundRouter:

    def test_generators_endpoint(self, pg_app):
        """T1: GET /playground/generators lists the 3 generators with param schemas."""
        client, _ = pg_app
        resp = client.get("/api/v1/playground/generators")
        assert resp.status_code == 200
        gens = {g["name"] for g in resp.json()["generators"]}
        assert gens == {"private-financials", "macro-scenarios", "consumer-crowd"}
        for g in resp.json()["generators"]:
            assert "param_schema" in g and "label" in g

    def test_run_anonymous_first_then_gated(self, pg_app):
        """T2: 1st anon run -> 200 + report short_code; 2nd -> 429 action=signup_required."""
        client, _ = pg_app
        r1 = client.post("/api/v1/playground/run", json=_RUN_BODY)
        assert r1.status_code == 200, r1.text
        assert r1.json()["report"]["short_code"]
        r2 = client.post("/api/v1/playground/run", json=_RUN_BODY)
        assert r2.status_code == 429
        assert r2.json()["detail"]["action"] == "signup_required"

    def test_run_authed_free_limit(self, pg_app):
        """T3: an authed free user gets PLAYGROUND_FREE_RUNS_PER_DAY (=2) runs then 429 upgrade."""
        client, db = pg_app
        token = _issue_token(db, f"spec057-{uuid.uuid4().hex[:8]}@acmecorp.com")
        headers = {"Authorization": f"Bearer {token}"}
        assert client.post("/api/v1/playground/run", json=_RUN_BODY, headers=headers).status_code == 200
        assert client.post("/api/v1/playground/run", json=_RUN_BODY, headers=headers).status_code == 200
        r3 = client.post("/api/v1/playground/run", json=_RUN_BODY, headers=headers)
        assert r3.status_code == 429
        assert r3.json()["detail"]["action"] == "upgrade"

    def test_run_unknown_generator(self, pg_app):
        """T4: unknown generator -> 422."""
        client, _ = pg_app
        resp = client.post(
            "/api/v1/playground/run",
            json={"generator": "does-not-exist", "params": {}},
        )
        assert resp.status_code == 422

    def test_run_records_lead_and_report(self, pg_app):
        """T5: a run writes a playground_runs row and a reports row with short_code + is_public."""
        client, db = pg_app
        token = _issue_token(db, f"spec057-{uuid.uuid4().hex[:8]}@acmecorp.com")
        resp = client.post(
            "/api/v1/playground/run", json=_RUN_BODY,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200, resp.text
        short_code = resp.json()["report"]["short_code"]

        report = db.execute(
            text("SELECT short_code, is_public, status, template FROM reports "
                 "WHERE short_code = :sc"),
            {"sc": short_code},
        ).fetchone()
        assert report is not None
        assert report[1] is True  # is_public
        assert report[2] == "complete"
        assert report[3] == "synthetic_playground"

        runs = db.execute(
            text("SELECT generator FROM playground_runs WHERE report_id = "
                 "(SELECT id FROM reports WHERE short_code = :sc)"),
            {"sc": short_code},
        ).fetchall()
        assert len(runs) == 1 and runs[0][0] == "macro-scenarios"

    def test_report_by_short_code_served(self, pg_app):
        """T6: GET /playground/report/<code> streams HTML + bumps view_count; bad code -> 404."""
        client, db = pg_app
        resp = client.post("/api/v1/playground/run", json=_RUN_BODY)
        assert resp.status_code == 200, resp.text
        short_code = resp.json()["report"]["short_code"]

        served = client.get(f"/api/v1/playground/report/{short_code}")
        assert served.status_code == 200
        assert served.headers["content-type"].startswith("text/html")
        assert "<!DOCTYPE html>" in served.text
        assert 'id="nexdata-cta"' in served.text  # the CTA shipped in the artifact

        vc = db.execute(
            text("SELECT view_count FROM reports WHERE short_code = :sc"),
            {"sc": short_code},
        ).scalar()
        assert vc >= 1

        assert client.get("/api/v1/playground/report/nonexistentcode").status_code == 404

    def test_quota_endpoint_peek_only(self, pg_app):
        """T7: GET /playground/quota reports remaining without consuming."""
        client, _ = pg_app
        q1 = client.get("/api/v1/playground/quota").json()
        q2 = client.get("/api/v1/playground/quota").json()
        assert q1["used"] == 0 and q2["used"] == 0  # peek never consumes
        assert q1["remaining"] == q1["limit"]
        assert q1["authenticated"] is False

    def test_admin_leads_list_and_csv(self, pg_app):
        """T8: GET /playground-admin/leads returns rows; /export.csv returns CSV."""
        client, db = pg_app
        # seed a lead via an authed run
        token = _issue_token(db, f"spec057-{uuid.uuid4().hex[:8]}@acmecorp.com")
        client.post(
            "/api/v1/playground/run", json=_RUN_BODY,
            headers={"Authorization": f"Bearer {token}"},
        )
        listing = client.get("/api/v1/playground-admin/leads")
        assert listing.status_code == 200
        assert listing.json()["total"] >= 1

        csv_resp = client.get("/api/v1/playground-admin/leads/export.csv")
        assert csv_resp.status_code == 200
        assert csv_resp.headers["content-type"].startswith("text/csv")
        assert "email" in csv_resp.text  # header row present
