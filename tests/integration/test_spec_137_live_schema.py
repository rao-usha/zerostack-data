"""
SPEC_137 against the live database (read-only): /schema latency, restricted-example gate,
personal-dataset masking.

    RUN_INTEGRATION_TESTS=true pytest tests/integration/test_spec_137_live_schema.py -v

Runs inside the API container or anywhere DATABASE_URL reaches the live database. Every
connection is ``default_transaction_read_only``; nothing is written.
"""
import os
import statistics
import time

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("RUN_INTEGRATION_TESTS", "").lower() not in ("1", "true", "yes")
                       or not os.environ.get("DATABASE_URL"),
                       reason="needs RUN_INTEGRATION_TESTS=true and DATABASE_URL (live database)"),
]

SCHEMA_BUDGET_MS = 300  # PLAN_088 §3 SPEC_137


@pytest.fixture(scope="module")
def live():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from app.api.v1 import catalog_schema
    from app.core.authz import current_principal
    from app.core.database import get_db

    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True,
                           connect_args={"options": "-c default_transaction_read_only=on"})
    factory = sessionmaker(bind=engine)

    def _db():
        db = factory()
        try:
            yield db
        finally:
            db.close()

    role = {"role": "user"}
    app = FastAPI()
    app.include_router(catalog_schema.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[current_principal] = lambda: dict(role)
    yield TestClient(app), role
    engine.dispose()


def test_schema_sec_13f_under_budget(live):
    client, _ = live
    assert client.get("/api/v1/catalog/sec_13f/schema").status_code == 200  # warm pool and caches
    times = []
    for _ in range(5):
        t0 = time.monotonic()
        r = client.get("/api/v1/catalog/sec_13f/schema")
        times.append((time.monotonic() - t0) * 1000)
        assert r.status_code == 200
    assert statistics.median(times) < SCHEMA_BUDGET_MS, times
    assert {t["table"] for t in r.json()["tables"]} >= {"sec_13f_filings", "sec_13f_holdings"}


def test_restricted_examples_hidden_from_non_admins(live):
    client, role = live
    role["role"] = "user"
    body = client.get("/api/v1/catalog/yelp_businesses/schema").json()
    assert body["flags"]["redistribution"] == "restricted"
    assert not [c["name"] for t in body["tables"] for c in t["columns"] if c["example"]]
    assert client.get("/api/v1/catalog/yelp_businesses/sample").status_code == 403


def test_people_sample_masks_person_columns(live):
    client, role = live
    role["role"] = "user"
    r = client.get("/api/v1/catalog/people_org_charts/sample", params={"table": "people", "limit": 20})
    assert r.status_code == 200
    meta = {c["name"]: c for c in r.json()["columns"]}
    for col in ("full_name", "linkedin_url", "photo_url", "twitter_url"):
        if col in meta:
            assert meta[col]["masked"], col
    assert r.headers["X-Dataset-Origin"]
