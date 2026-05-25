"""
Tests for SPEC 066c — Atlas dynamic UI Wave 2 server additions.

Frontend features (calendar heatmap, heat toggle, time scrubber) are
verified by the SPEC_073 smoke harness; this file covers only the two
new server endpoints.
"""
import json
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


class TestPlaceEventsEndpoint:

    def test_harris_fema_events(self, client, db):
        """T1: /atlas/place/48201/events returns Harris Co events with dates."""
        resp = client.get("/api/v1/atlas/place/48201/events",
                          params={"source": "fema"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["geo_id"] == "48201"
        assert body["source"] == "fema"
        events = body["events"]
        assert len(events) >= 20, f"expected ≥20 Harris Co events, got {len(events)}"
        e0 = events[0]
        for field in ("date", "type", "title", "disaster_number"):
            assert field in e0, f"event missing field {field}"
        # Dates sorted desc
        dates = [e["date"] for e in events if e["date"]]
        assert dates == sorted(dates, reverse=True)
        # All dates are valid YYYY-MM-DD
        for d in dates[:5]:
            assert re.match(r"^\d{4}-\d{2}-\d{2}$", d), f"bad date format: {d}"

    def test_invalid_geo_id_400(self, client, db):
        """T1b: non-FIPS returns 400."""
        resp = client.get("/api/v1/atlas/place/abc/events",
                          params={"source": "fema"})
        assert resp.status_code == 400

    def test_unknown_source_400(self, client, db):
        """T1c: unknown source returns 400."""
        resp = client.get("/api/v1/atlas/place/48201/events",
                          params={"source": "imaginary"})
        assert resp.status_code == 400


class TestFemaCascadeEndpoint:

    def test_cascade_shape(self, client, db):
        """T2: cascade endpoint returns years + values_by_year."""
        resp = client.get("/api/v1/atlas/layer/disaster_fema_declarations/cascade")
        assert resp.status_code == 200
        body = resp.json()
        assert "years" in body
        assert "values_by_year" in body
        assert len(body["years"]) >= 25, \
            f"expected ≥25 years, got {len(body['years'])}"
        # Years sorted, all 4-digit ints
        years = body["years"]
        assert years == sorted(years)
        for y in years:
            assert isinstance(y, int) and 1990 <= y <= 2030

    def test_cascade_geo_ids_are_county_fips(self, client, db):
        """T3: every value_by_year key is a 5-digit county FIPS."""
        resp = client.get("/api/v1/atlas/layer/disaster_fema_declarations/cascade")
        body = resp.json()
        sample_year = max(body["years"])
        year_values = body["values_by_year"][str(sample_year)]
        pat = re.compile(r"^\d{5}$")
        bad = [k for k in year_values if not pat.match(k)][:5]
        assert not bad, f"non-5-digit geo_ids in cascade year {sample_year}: {bad}"
        # All counts are positive ints
        for k, v in list(year_values.items())[:30]:
            assert isinstance(v, int) and v > 0

    def test_cascade_payload_size_reasonable(self, client, db):
        """T2b: cascade payload < 2 MB (32k cells fits well under)."""
        resp = client.get("/api/v1/atlas/layer/disaster_fema_declarations/cascade")
        size = len(resp.content)
        assert size < 2 * 1024 * 1024, \
            f"cascade payload {size/1024:.0f}KB exceeds 2MB budget"
