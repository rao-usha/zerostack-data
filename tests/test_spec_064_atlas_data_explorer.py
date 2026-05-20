"""
SPEC 064 — Nexdata Atlas Data Explorer.

Test surfaces:
  * Pure-Python: resolver (T1, T2, T5), card schema (T3), connections (T6).
  * Postgres-backed: explore() integration + telemetry (T4, T7, T8, T10) use a
    `pg_session` fixture against local docker postgres. Against local the
    cloud-only CBP/ACS/etc. tables are absent, so cards skip-on-empty and the
    exploration is coverage-notes-only — which is exactly what T4 verifies.
    Live cards-with-real-data is exercised by the manual cloud smoke test.
  * Taxonomy endpoint (T9) via TestClient — loads JSON, no DB needed.
"""
import os
import pytest
from unittest.mock import MagicMock

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://nexdata:nexdata_dev_password@postgres:5432/nexdata",
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def pg_session():
    if not DATABASE_URL:
        pytest.skip("DATABASE_URL not set")
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from app.services.atlas.telemetry import AtlasTelemetry

    engine = create_engine(DATABASE_URL, future=True)
    Session = sessionmaker(bind=engine, future=True)
    session = Session()
    AtlasTelemetry(session)  # ensure atlas_* tables exist
    # clean test rows (slug prefix 'zz-spec064')
    for tbl in ("atlas_events", "atlas_card_feedback", "atlas_queries",
                "atlas_shared_links"):
        session.execute(text(
            f"DELETE FROM {tbl} WHERE exploration_id IN "
            f"(SELECT id FROM atlas_explorations WHERE slug LIKE 'zz-spec064%')"
        ))
    session.execute(text("DELETE FROM atlas_explorations WHERE slug LIKE 'zz-spec064%'"))
    session.commit()
    try:
        yield session
    finally:
        for tbl in ("atlas_events", "atlas_card_feedback", "atlas_queries",
                    "atlas_shared_links"):
            session.execute(text(
                f"DELETE FROM {tbl} WHERE exploration_id IN "
                f"(SELECT id FROM atlas_explorations WHERE slug LIKE 'zz-spec064%')"
            ))
        session.execute(text("DELETE FROM atlas_explorations WHERE slug LIKE 'zz-spec064%'"))
        session.commit()
        session.close()


@pytest.fixture()
def client(pg_session):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.api.v1 import atlas
    from app.core.database import get_db

    app = FastAPI()
    app.include_router(atlas.router, prefix="/api/v1")

    def _override_get_db():
        try:
            yield pg_session
        finally:
            pass

    app.dependency_overrides[get_db] = _override_get_db
    return TestClient(app)


def _synthetic_gather():
    """Minimal gather_data-shaped dict with one populated section."""
    return {
        "input": {"naics_code": "2382", "msa_code": "26420"},
        "structural_density": {
            "establishments": 412, "employees": 5230,
            "annual_payroll_thousands": 387_000, "hhi_avg": 0.0824,
            "source": "census_cbp",
        },
        "demand_context": {}, "migration_economy": {},
        "operating_environment": {}, "risk_profile": {},
        "infra_proximity": {}, "trade_exposure": {}, "federal_spending": {},
        "public_cos": {}, "named_privates": {},
        "_fallback_used": False,
        "_msa_county_coverage_pct": 100.0,
        "_provenance": [
            {"section": "structural_density", "table": "census_cbp", "rows": 9},
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# T1, T2, T5 — resolver (pure)
# ─────────────────────────────────────────────────────────────────────────────

class TestResolver:

    def test_resolver_direct_msa_naics(self):
        """T1: direct msa=26420 + naics=2382 resolves expected labels + counties."""
        from app.services.atlas.resolver import resolve
        entities, slug = resolve("anything", msa="26420", naics="2382")
        assert entities.msa["code"] == "26420"
        assert "Houston" in entities.msa["title"]
        assert entities.naics["code"] == "2382"
        assert "uilding" in entities.naics["label"]  # "Building Equipment Contractors"
        assert len(entities.geographies) >= 1  # constituent counties
        assert all(len(c) == 5 for c in entities.geographies)

    def test_resolver_query_text_houston_2382(self):
        """T2: free-text 'Houston building equipment contractors' infers both."""
        from app.services.atlas.resolver import resolve
        entities, slug = resolve("Houston building equipment contractors")
        assert entities.msa is not None and "Houston" in entities.msa["title"]
        assert entities.naics is not None and entities.naics["code"] == "2382"

    def test_slug_stable(self):
        """T5: same query/entities produce the same slug."""
        from app.services.atlas.resolver import resolve
        _, slug_a = resolve("Houston building equipment contractors")
        _, slug_b = resolve("Houston building equipment contractors")
        assert slug_a == slug_b
        assert slug_a  # non-empty
        # direct-param path produces the same slug as the text path
        _, slug_c = resolve("xyz", msa="26420", naics="2382")
        assert slug_c == slug_a


# ─────────────────────────────────────────────────────────────────────────────
# T3 — card schema
# ─────────────────────────────────────────────────────────────────────────────

class TestCardSchema:

    def test_explore_returns_cards_with_provenance(self):
        """T3: a built card carries every required Atlas field."""
        from app.services.atlas.cards import _card_industry_footprint
        card = _card_industry_footprint(_synthetic_gather())
        assert card is not None
        d = card.to_dict()
        for key in ("id", "title", "summary", "metrics", "why_it_matters",
                    "datasets_used", "confidence", "coverage", "provenance", "links"):
            assert key in d, f"card missing required field: {key}"
        assert d["datasets_used"], "datasets_used must be non-empty"
        assert d["provenance"], "provenance must be non-empty"
        assert d["provenance"][0]["dataset"] == "census_cbp"
        assert d["confidence"] in ("high", "medium", "low")


# ─────────────────────────────────────────────────────────────────────────────
# T4 — coverage notes, no fake cards
# ─────────────────────────────────────────────────────────────────────────────

class TestCoverage:

    def test_missing_data_creates_coverage_note(self):
        """T4: a section with no data → coverage note, never a fabricated card."""
        from app.services.atlas.cards import build_cards
        from app.services.atlas.types import ResolvedEntities
        # No naics → build_cards can't run the gather layer → all cards skip.
        resolved = ResolvedEntities(
            msa={"code": "26420", "title": "Houston-Pasadena-The Woodlands, TX"},
            naics=None, geographies=["48201"], datasets=[],
        )
        db = MagicMock()
        cards, coverage_notes, datasets = build_cards(db, resolved)
        # No cards fabricated when there's no data
        assert all(c.metrics for c in cards), "a card was emitted with no metrics"
        # Coverage notes explain the gaps
        assert len(coverage_notes) >= 1
        assert any("industry" in n.lower() or "no usable data" in n.lower()
                   for n in coverage_notes)


# ─────────────────────────────────────────────────────────────────────────────
# T6 — connections reference existing cards
# ─────────────────────────────────────────────────────────────────────────────

class TestConnections:

    def test_connections_reference_existing_cards(self):
        """T6: every connection's source/target is a card that exists."""
        from app.services.atlas.graph import build_connections
        from app.services.atlas.types import Card
        cards = [
            Card(id="industry_footprint", title="A", summary=""),
            Card(id="named_operators", title="B", summary=""),
            Card(id="macro_rates", title="C", summary=""),
        ]
        conns = build_connections(cards)
        present = {c.id for c in cards}
        assert conns, "expected at least one connection among these 3 cards"
        for conn in conns:
            assert conn.source_card in present
            assert conn.target_card in present

        # And with a single card, zero connections (nothing to connect).
        assert build_connections([Card(id="industry_footprint", title="A", summary="")]) == []


# ─────────────────────────────────────────────────────────────────────────────
# T7, T8, T10 — endpoints (Postgres-backed)
# ─────────────────────────────────────────────────────────────────────────────

class TestEndpoints:

    def test_router_smoke_explore(self, client, pg_session):
        """T10: POST /atlas/explore returns a valid exploration object."""
        from sqlalchemy import text
        resp = client.post("/api/v1/atlas/explore", json={
            "query": "Houston building equipment contractors",
        })
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["slug"]
        assert body["resolved_entities"]["msa"]["code"] == "26420"
        assert body["resolved_entities"]["naics"]["code"] == "2382"
        assert "cards" in body and isinstance(body["cards"], list)
        assert "connections" in body
        assert "summary" in body
        # rename the persisted row so the fixture's cleanup catches it
        pg_session.execute(text(
            "UPDATE atlas_explorations SET slug = 'zz-spec064-' || slug "
            "WHERE slug = :s AND slug NOT LIKE 'zz-spec064%'"), {"s": body["slug"]})
        pg_session.commit()

    def test_event_endpoint_records_event(self, client, pg_session):
        """T7: POST /atlas/events persists a card_expanded row in atlas_events."""
        from sqlalchemy import text
        resp = client.post("/api/v1/atlas/events", json={
            "event_type": "card_expanded",
            "card_id": "industry_footprint",
            "session_id": "zz-spec064-sess",
            "payload": {"test": True},
        })
        assert resp.status_code == 200, resp.text
        assert resp.json()["recorded"] is True
        row = pg_session.execute(text(
            "SELECT event_type, card_id FROM atlas_events WHERE id = :id"),
            {"id": resp.json()["event_id"]},
        ).mappings().first()
        assert row["event_type"] == "card_expanded"
        assert row["card_id"] == "industry_footprint"
        # cleanup this stray event (no exploration_id → fixture won't catch it)
        pg_session.execute(text("DELETE FROM atlas_events WHERE id = :id"),
                           {"id": resp.json()["event_id"]})
        pg_session.commit()

    def test_feedback_endpoint_records_feedback(self, client, pg_session):
        """T8: POST /atlas/feedback persists a thumbs_up in atlas_card_feedback."""
        from sqlalchemy import text
        resp = client.post("/api/v1/atlas/feedback", json={
            "card_id": "risk_context",
            "feedback": "thumbs_up",
            "session_id": "zz-spec064-sess",
            "comment": "useful",
        })
        assert resp.status_code == 200, resp.text
        assert resp.json()["recorded"] is True
        row = pg_session.execute(text(
            "SELECT card_id, feedback FROM atlas_card_feedback WHERE id = :id"),
            {"id": resp.json()["feedback_id"]},
        ).mappings().first()
        assert row["card_id"] == "risk_context"
        assert row["feedback"] == "thumbs_up"
        pg_session.execute(text("DELETE FROM atlas_card_feedback WHERE id = :id"),
                           {"id": resp.json()["feedback_id"]})
        pg_session.commit()


# ─────────────────────────────────────────────────────────────────────────────
# T9 — taxonomies endpoint
# ─────────────────────────────────────────────────────────────────────────────

class TestTaxonomies:

    def test_taxonomies_filters_to_available_options(self, client):
        """T9: GET /atlas/taxonomies returns NAICS tree + MSA list, expected shape."""
        resp = client.get("/api/v1/atlas/taxonomies")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body["naics"], list) and body["naics"]
        sec = body["naics"][0]
        assert {"sector_code", "sector_label", "industries"} <= set(sec.keys())
        assert isinstance(body["msa"], list) and len(body["msa"]) >= 380
        assert {"cbsa_code", "title", "state_abbrs"} <= set(body["msa"][0].keys())
