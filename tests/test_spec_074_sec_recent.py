"""
Tests for SPEC 074 — SEC Recent Activity lane + state-grain filers layer.

DB-backed tests skip when cloud SEC tables absent. The frontend item-
click `url` branch + source chip are verified by smoke harness.
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
        session.execute(text("SELECT 1 FROM sec_10k LIMIT 1"))
        session.execute(text("SELECT 1 FROM sec_10q LIMIT 1"))
        session.execute(text("SELECT 1 FROM sec_company_metadata LIMIT 1"))
    except Exception:
        try: session.rollback()
        except Exception: pass
        pytest.skip("SEC tables absent — needs cloud DB")
    yield session
    session.close()


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


# ─── state_fips helper (unit) ────────────────────────────────────────────

class TestStateFips:
    def test_known_states(self):
        from app.services.atlas.state_fips import state_fips
        assert state_fips("IL") == "17"
        assert state_fips("CA") == "06"
        assert state_fips("NY") == "36"
        assert state_fips("DC") == "11"
        assert state_fips("PR") == "72"
    def test_unknown_returns_none(self):
        from app.services.atlas.state_fips import state_fips
        assert state_fips("ZZ") is None
        assert state_fips("") is None
        assert state_fips(None) is None
    def test_case_insensitive(self):
        from app.services.atlas.state_fips import state_fips
        assert state_fips("il") == "17"
        assert state_fips("  ny  ") == "36"


# ─── SEC lane via /atlas/recent ─────────────────────────────────────────

class TestRecentSecLane:

    def test_sec_only_returns_items(self, client, db):
        """T1: ?sources=sec returns ≥10 items."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "sec", "limit": 20}).json()
        assert len(body["items"]) >= 10, \
            f"expected ≥10 SEC items, got {len(body['items'])}"

    def test_items_have_required_fields(self, client, db):
        """T1b: each SEC item has source, date, type, title, event_id, url."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "sec", "limit": 10}).json()
        for item in body["items"]:
            assert item["source"] == "sec"
            for f in ("date", "type", "title", "event_id", "url"):
                assert f in item, f"missing field {f}: {item}"
            assert re.match(r"^\d{4}-\d{2}-\d{2}$", item["date"])

    def test_place_id_is_state_fips_or_null(self, client, db):
        """T2: place_id is 2-digit state FIPS or null (companies w/o known state)."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "sec", "limit": 30}).json()
        pat = re.compile(r"^\d{2}$")
        for item in body["items"]:
            pid = item.get("place_id")
            assert pid is None or pat.match(pid), \
                f"place_id not 2-digit FIPS or null: {pid!r}"

    def test_urls_well_formed(self, client, db):
        """T3: each SEC item url is a sec.gov / sec EDGAR URL."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "sec", "limit": 10}).json()
        for item in body["items"]:
            url = item["url"]
            assert url and url.startswith("http"), f"bad url: {url!r}"
            assert "sec.gov" in url.lower(), f"not a sec.gov URL: {url!r}"

    def test_merged_fema_sec_sorted_desc(self, client, db):
        """T4: ?sources=fema,sec returns merged stream sorted by date desc."""
        body = client.get("/api/v1/atlas/recent",
                          params={"sources": "fema,sec", "limit": 50}).json()
        dates = [i["date"] for i in body["items"]]
        assert dates == sorted(dates, reverse=True), \
            "merged items not sorted desc by date"
        sources = {i["source"] for i in body["items"]}
        # At minimum at least one source present; ideally both
        assert sources.issubset({"fema", "sec"})


# ─── SEC active filers layer ────────────────────────────────────────────

class TestSecFilersLayer:

    def test_layer_appears_in_registry(self, client, db):
        """T6: econ_sec_active_filers in /atlas/layers economy domain."""
        body = client.get("/api/v1/atlas/layers").json()
        all_ids = {l["id"] for layers in body["layers_by_domain"].values()
                            for l in layers}
        assert "econ_sec_active_filers" in all_ids

    def test_layer_returns_state_values(self, client, db):
        """T5: /atlas/layer/econ_sec_active_filers returns ≥40 states."""
        body = client.get("/api/v1/atlas/layer/econ_sec_active_filers").json()
        assert body["grain"] == "state"
        assert len(body.get("values", {})) >= 40
        # geo_id keys should all be 2-digit FIPS
        pat = re.compile(r"^\d{2}$")
        bad = [k for k in body["values"] if not pat.match(k)][:5]
        assert not bad, f"non-2-digit state geo_ids: {bad}"
