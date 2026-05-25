"""
Tests for SPEC 067 — Atlas Recent Activity feed (FEMA-only MVP).

Frontend (floating panel + item-click → openPlace) is verified by the
SPEC_073 smoke harness; this file covers the new `/atlas/recent`
endpoint.
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
        session.execute(text("SELECT 1 FROM fema_disaster_declarations LIMIT 1"))
    except Exception:
        try: session.rollback()
        except Exception: pass
        pytest.skip("fema_disaster_declarations absent — needs cloud DB")
    yield session
    session.close()


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


class TestRecentEndpoint:

    def test_recent_fema_returns_items(self, client, db):
        """T1: ?sources=fema returns ≥10 items."""
        resp = client.get("/api/v1/atlas/recent", params={"sources": "fema"})
        assert resp.status_code == 200
        body = resp.json()
        assert "items" in body
        assert len(body["items"]) >= 10, \
            f"expected ≥10 items, got {len(body['items'])}"

    def test_items_have_required_fields(self, client, db):
        """T2: each item has source, date, type, title, place_id (5-digit FIPS), place_name, event_id."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "fema", "limit": 20}).json()
        pat = re.compile(r"^\d{5}$")
        for item in body["items"][:10]:
            for f in ("source", "date", "type", "title", "place_id",
                      "place_name", "event_id"):
                assert f in item, f"item missing field {f}: {item}"
            assert pat.match(item["place_id"]), \
                f"place_id not 5-digit FIPS: {item['place_id']!r}"
            assert re.match(r"^\d{4}-\d{2}-\d{2}$", item["date"]), \
                f"bad date format: {item['date']!r}"
            assert item["source"] == "fema"

    def test_items_sorted_desc_by_date(self, client, db):
        """T3: dates non-increasing."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "fema", "limit": 50}).json()
        dates = [i["date"] for i in body["items"]]
        assert dates == sorted(dates, reverse=True), \
            "items not sorted desc by date"

    def test_unknown_source_400(self, client, db):
        """T4: ?sources=imaginary returns 400."""
        resp = client.get("/api/v1/atlas/recent",
                          params={"sources": "imaginary"})
        assert resp.status_code == 400
        assert "unknown source" in resp.text.lower() \
               or "unknown sources" in resp.text.lower()

    def test_default_limit_50(self, client, db):
        """T5a: default limit is 50."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "fema"}).json()
        assert len(body["items"]) <= 50

    def test_explicit_limit_honored(self, client, db):
        """T5b: explicit limit honored."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "fema", "limit": 5}).json()
        assert len(body["items"]) <= 5

    def test_limit_capped_at_200(self, client, db):
        """T5c: limit > 200 capped to 200."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "fema", "limit": 1000}).json()
        assert len(body["items"]) <= 200
