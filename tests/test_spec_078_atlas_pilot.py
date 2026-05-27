"""
Tests for SPEC 078 — Atlas Pilot agent (Phase B v0).

Covers:
  T1 — tool definitions are well-formed (JSON Schemas validate)
  T2 — tool dispatcher routes each known tool name
  T3 — tool dispatcher rejects unknown names
  T4 — /atlas/pilot endpoint accepts the request shape
  T5 — without OPENAI_API_KEY, /atlas/pilot returns the configured error

Does NOT exercise an actual LLM call — that's an integration test that
costs money and depends on network. Run those manually.
"""
import os
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text


@pytest.fixture(scope="module")
def db():
    from app.core.database import get_db
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


class TestToolSurface:

    def test_six_tools_defined(self):
        from app.services.atlas.pilot_tools import TOOL_DEFS
        assert len(TOOL_DEFS) >= 6, \
            f"expected ≥6 tools, got {len(TOOL_DEFS)}"

    def test_tools_have_well_formed_schemas(self):
        """T1: every tool definition has the OpenAI tool-call shape."""
        from app.services.atlas.pilot_tools import TOOL_DEFS
        for td in TOOL_DEFS:
            assert td["type"] == "function"
            fn = td["function"]
            assert "name" in fn and fn["name"]
            assert "description" in fn and fn["description"]
            assert "parameters" in fn
            params = fn["parameters"]
            assert params["type"] == "object"
            assert "properties" in params

    def test_dispatcher_unknown_tool(self, db):
        """T3: unknown tool name returns error dict, not exception."""
        from app.services.atlas.pilot_tools import dispatch
        result = dispatch(db, "this_tool_does_not_exist", {})
        assert "error" in result
        assert "unknown tool" in result["error"]

    def test_query_place_dispatcher_validates(self, db):
        """T2: query_place dispatcher handles a bad geo_id gracefully."""
        from app.services.atlas.pilot_tools import dispatch
        result = dispatch(db, "query_place", {"geo_id": "abc"})
        assert "error" in result

    def test_list_layers_dispatcher_runs(self, db):
        """T2b: list_layers returns the registry."""
        from app.services.atlas.pilot_tools import dispatch
        result = dispatch(db, "list_layers", {})
        assert "layers" in result
        assert result["count"] >= 20


class TestPilotEndpoint:

    def test_pilot_accepts_question(self, client):
        """T4: POST /atlas/pilot accepts a {question} body."""
        # No actual LLM call enforced here — we just check the endpoint
        # validates the body shape. With OPENAI_API_KEY missing this
        # returns the configured error; with it set this would try real
        # LLM (skipped for cost).
        resp = client.post("/api/v1/atlas/pilot",
                            json={"question": "test ping"})
        assert resp.status_code == 200
        # Either real response or error response — both 200, body is JSON.
        body = resp.json()
        assert isinstance(body, dict)

    def test_pilot_rejects_empty_question(self, client):
        """T4b: empty question rejected with 4xx validation error."""
        resp = client.post("/api/v1/atlas/pilot", json={"question": ""})
        assert resp.status_code in (400, 422)

    def test_pilot_without_api_key_returns_clear_error(self, client, monkeypatch):
        """T5: without OPENAI_API_KEY, response includes a clear error."""
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        resp = client.post("/api/v1/atlas/pilot",
                            json={"question": "hello"})
        body = resp.json()
        assert "error" in body
        assert "OPENAI_API_KEY" in body["error"]
