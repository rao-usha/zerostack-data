"""
Tests for SPEC 127 — access lockdown.

Every router was anonymous by default, self-registration and the playground's
passwordless sign-in minted tokens that passed every router, and the export
preview would hand anyone `users.password_hash` and plaintext reset tokens.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database: they drop
and recreate the auth tables (users, password_reset_tokens, refresh_tokens,
login_codes, api_keys, ...).
"""
import importlib.util
import os
import re
from pathlib import Path

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]
JWT_SECRET = "spec127-test-secret-" + "x" * 48
_DUMMY_DB = "postgresql://nobody:nobody@localhost:1/nothing"


@pytest.fixture
def env(monkeypatch):
    """Settings are a process singleton; point them at this test's env."""
    from app.core.config import reset_settings

    monkeypatch.setenv("DATABASE_URL", PG_URL or _DUMMY_DB)
    monkeypatch.setenv("JWT_SECRET_KEY", JWT_SECRET)
    for var in ("REQUIRE_AUTH", "ALLOW_SIGNUP", "ADMIN_EMAILS"):
        monkeypatch.delenv(var, raising=False)
    reset_settings()
    yield monkeypatch
    reset_settings()


@pytest.fixture
def main_app(env):
    from app.main import app

    return app


# =============================================================================
# Unit — no database
# =============================================================================


@pytest.mark.unit
class TestDefaults:
    def test_auth_on_and_signup_closed_by_default(self, env):
        from app.core.config import get_settings

        s = get_settings()
        assert s.require_auth is True
        assert s.allow_signup is False
        assert s.admin_email_set() == set()

    def test_require_auth_false_is_still_possible(self, env):
        from app.core.authz import auth_required
        from app.core.config import reset_settings

        env.setenv("REQUIRE_AUTH", "false")
        reset_settings()
        assert auth_required() is False

    def test_admin_emails_parse(self, env):
        from app.core.config import get_settings, reset_settings

        env.setenv("ADMIN_EMAILS", " Boss@Example.com, ops@example.com ,,")
        reset_settings()
        assert get_settings().admin_email_set() == {"boss@example.com", "ops@example.com"}

    def test_license_is_not_mit(self, main_app):
        info = main_app.openapi().get("info", {}).get("license") or {}
        assert "MIT" not in str(info)


def _route_deps(route):
    """Every callable in a route's dependency tree (router-level included)."""
    seen = []

    def walk(dependant):
        for d in dependant.dependencies:
            seen.append(d.call)
            walk(d)

    walk(route.dependant)
    return seen


def _module_of(route):
    return getattr(route.endpoint, "__module__", "")


PUBLIC_MODULES = {
    "app.api.v1.auth",
    "app.api.v1.public",
    "app.api.v1.playground",
    "app.api.v1.diligence_pack",
    "app.api.v1.atlas",
}

ADMIN_MODULES = {
    "jobs", "jobs_monitor", "schedules", "webhooks", "chains", "rate_limits",
    "templates", "export", "bulk", "pe_marts", "api_keys", "settings",
    "source_configs", "audit", "llm_costs", "playground_admin",
    "collection_jobs", "people_jobs", "pe_collection", "evals", "dq_review",
    "import_portfolio", "pe_import",
}


@pytest.mark.unit
class TestRouteTable:
    """Adding a router without an access policy must fail here, not in prod."""

    def _api_routes(self, app):
        from fastapi.routing import APIRoute

        return [r for r in app.routes if isinstance(r, APIRoute)]

    def test_every_non_public_route_has_a_policy(self, main_app):
        from app.core import authz

        policies = {
            authz.require_user,
            authz.require_admin,
            authz.require_admin_for_writes,
            authz.require_admin_or_stream_token,
        }
        missing = []
        for r in self._api_routes(main_app):
            if not (r.path.startswith("/api/v1") or r.path.startswith("/graphql")):
                continue
            if _module_of(r) in PUBLIC_MODULES:
                continue
            if not policies & set(_route_deps(r)):
                missing.append(f"{sorted(r.methods)} {r.path}")
        assert not missing, "routes without an access policy:\n" + "\n".join(missing)

    def test_admin_routers_are_admin_for_every_method(self, main_app):
        from app.core import authz

        wrong = []
        found = set()
        for r in self._api_routes(main_app):
            mod = _module_of(r).rsplit(".", 1)[-1]
            if _module_of(r).startswith("app.api.v1.") and mod in ADMIN_MODULES:
                found.add(mod)
                if authz.require_admin not in _route_deps(r):
                    wrong.append(f"{mod}: {sorted(r.methods)} {r.path}")
        assert not wrong, "\n".join(wrong)
        assert found == ADMIN_MODULES, ADMIN_MODULES - found

    def test_job_stream_accepts_stream_token_and_graphql_needs_user(self, main_app):
        from app.core import authz

        stream = [r for r in self._api_routes(main_app) if r.path.startswith("/api/v1/job-queue")]
        assert stream
        for r in stream:
            assert authz.require_admin_or_stream_token in _route_deps(r), r.path

        gql = [r for r in self._api_routes(main_app) if r.path.startswith("/graphql")]
        assert gql
        for r in gql:
            assert authz.require_user in _route_deps(r), r.path

    def test_user_write_allowlist_names_real_post_endpoints(self, main_app):
        from app.core.authz import USER_WRITE_ENDPOINTS, endpoint_key

        posts = {
            endpoint_key(r.endpoint)
            for r in self._api_routes(main_app)
            if "POST" in r.methods
        }
        assert USER_WRITE_ENDPOINTS <= posts, USER_WRITE_ENDPOINTS - posts

    def test_atlas_paid_routes_are_metered(self, main_app):
        from app.api.v1.atlas import atlas_quota

        paid = {"/api/v1/atlas/plan", "/api/v1/atlas/explain", "/api/v1/atlas/pilot",
                "/api/v1/atlas/pilot/stream", "/api/v1/atlas/competition"}
        seen = set()
        for r in self._api_routes(main_app):
            if r.path in paid:
                seen.add(r.path)
                assert atlas_quota in _route_deps(r), r.path
        assert seen == paid


@pytest.mark.unit
class TestExportPolicy:
    @pytest.mark.parametrize(
        "table",
        ["users", "password_reset_tokens", "refresh_tokens", "login_codes",
         "api_keys", "api_usage", "source_api_keys", "leads",
         "playground_quota_buckets", "workspace_invitations", "webhooks",
         "diligence_orders", "alembic_version", "user_sessions",
         "oauth_tokens", "vendor_secrets", "crm_leads"],
    )
    def test_denied_tables(self, table):
        from app.core.export_policy import is_exportable

        assert not is_exportable(table, ["id", "name"])

    @pytest.mark.parametrize(
        "column",
        ["password_hash", "password", "token", "access_token", "token_hash",
         "api_key", "key_hash", "code_hash", "client_secret", "credentials",
         "private_key"],
    )
    def test_sensitive_columns_deny_the_table(self, column):
        from app.core.export_policy import is_exportable

        assert not is_exportable("some_vendor_table", ["id", column])

    @pytest.mark.parametrize(
        "table,columns",
        [
            ("pe_firms", ["id", "name", "cik", "content_hash"]),
            ("leadership_changes", ["id", "person_name", "change_type"]),
            ("fred_series", ["series_id", "date", "value"]),
            ("sec_13f_holdings", ["cik", "cusip", "value_usd", "row_hash"]),
        ],
    )
    def test_product_tables_pass(self, table, columns):
        from app.core.export_policy import is_exportable

        assert is_exportable(table, columns)


@pytest.mark.unit
class TestApiKeyScope:
    def test_scope_order(self):
        from app.auth.api_keys import scope_allows

        assert scope_allows("read", "read")
        assert scope_allows("write", "read")
        assert scope_allows("admin", "write")
        assert not scope_allows("read", "write")
        assert not scope_allows("write", "admin")

    def test_unknown_scope_fails_closed(self):
        from app.auth.api_keys import scope_allows

        assert not scope_allows("superuser", "read")
        assert not scope_allows(None, "read")
        assert not scope_allows("read", "bogus")


@pytest.mark.unit
class TestDeployAndFrontend:
    def test_compose_host_ports_are_loopback(self):
        text_ = (REPO / "docker-compose.yml").read_text(encoding="utf-8")
        ports = re.findall(r'^\s*-\s*"([^"]+:\d+)"', text_, flags=re.M)
        published = [p for p in ports if re.match(r"^[\d.]*:?\d+:\d+$", p)]
        assert published, "no published ports found"
        for p in published:
            assert p.startswith("127.0.0.1:"), p
        for host_port in ("8001", "5434", "5435", "3001"):
            assert f'"127.0.0.1:{host_port}:' in text_, host_port

    def test_compose_passes_auth_env(self):
        text_ = (REPO / "docker-compose.yml").read_text(encoding="utf-8")
        assert "REQUIRE_AUTH: ${REQUIRE_AUTH:-true}" in text_
        assert "ALLOW_SIGNUP: ${ALLOW_SIGNUP:-false}" in text_
        assert "ADMIN_EMAILS: ${ADMIN_EMAILS:-}" in text_

    def test_no_dev_bypass(self):
        html = (REPO / "frontend" / "index.html").read_text(encoding="utf-8")
        assert "authDevBypass" not in html
        assert "dev-token" not in html
        assert "Skip login" not in html

    def test_pages_calling_protected_routes_load_auth_js(self):
        assert (REPO / "frontend" / "js" / "auth.js").exists()
        public_pages = {"atlas.html", "playground.html", "diligence.html",
                        "diligence-sample.html", "resources.html"}
        missing = []
        for page in (REPO / "frontend").glob("*.html"):
            if page.name in public_pages:
                continue
            html = page.read_text(encoding="utf-8")
            if "fetch(" in html and 'src="/js/auth.js"' not in html:
                missing.append(page.name)
        assert not missing, missing

    def test_job_stream_uses_stream_token(self):
        html = (REPO / "frontend" / "index.html").read_text(encoding="utf-8")
        assert "new EventSource('/api/v1/job-queue/stream')" not in html
        assert "NexdataAuth.streamUrl" in html


# =============================================================================
# PG-backed
# =============================================================================

AUTH_TABLES = [
    "password_reset_tokens", "refresh_tokens", "login_codes",
    "workspace_members", "workspace_invitations", "workspaces",
    "api_usage", "rate_limit_buckets", "api_keys", "users",
    "playground_quota_buckets",
]


def _load_migration():
    path = REPO / "alembic" / "versions" / "0012_access_lockdown.py"
    spec = importlib.util.spec_from_file_location("mig0012", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def pg_engine(env):
    """Fresh auth tables in the disposable DB; the app's engine points at it."""
    from sqlalchemy import create_engine, text

    import app.core.database as database
    from app.users import auth as users_auth

    engine = create_engine(PG_URL)
    with engine.begin() as c:
        for t in AUTH_TABLES:
            c.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))
    users_auth._ENSURED_URLS.clear()

    old_engine, old_factory = database._engine, database._SessionLocal
    database._engine, database._SessionLocal = engine, None
    yield engine
    database._engine, database._SessionLocal = old_engine, old_factory
    users_auth._ENSURED_URLS.clear()
    engine.dispose()


def _session(engine):
    from sqlalchemy.orm import sessionmaker

    return sessionmaker(bind=engine)()


def _make_user(engine, email, password="correct-horse-1", admin=False):
    from app.users.auth import AuthService

    db = _session(engine)
    try:
        return AuthService(db).create_user(email, password, admin=admin)
    finally:
        db.close()


def _login(client, email, password="correct-horse-1"):
    r = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return r.json()


@pytest.fixture
def client(main_app, pg_engine):
    from fastapi.testclient import TestClient

    # handlers may hit tables this disposable DB lacks; we assert on auth codes
    return TestClient(main_app, raise_server_exceptions=False)


@pg
class TestMigration:
    def test_upgrade_old_shape(self, pg_engine):
        from sqlalchemy import text

        with pg_engine.begin() as c:
            c.execute(text("""CREATE TABLE users (id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL, password_hash VARCHAR(255))"""))
            c.execute(text("""CREATE TABLE password_reset_tokens (id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                token VARCHAR(64) NOT NULL UNIQUE, expires_at TIMESTAMP NOT NULL,
                used_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""))
            c.execute(text("""CREATE TABLE refresh_tokens (id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                token_hash VARCHAR(64) NOT NULL UNIQUE, expires_at TIMESTAMP NOT NULL,
                revoked_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""))
            c.execute(text("INSERT INTO users (email) VALUES ('a@x.com')"))
            c.execute(text("""INSERT INTO password_reset_tokens (user_id, token, expires_at)
                VALUES (1, 'plaintext-secret', now() + interval '1 day')"""))
            c.execute(text("""INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
                VALUES (1, 'abc', now() + interval '1 day')"""))

        mig = _load_migration()
        with pg_engine.begin() as c:
            for stmt in mig.UPGRADE_SQL:
                c.execute(text(stmt))
            # idempotent
            for stmt in mig.UPGRADE_SQL:
                c.execute(text(stmt))

        with pg_engine.connect() as c:
            assert c.execute(text("SELECT role FROM users")).scalar() == "user"
            assert c.execute(text("SELECT count(*) FROM password_reset_tokens")).scalar() == 0
            cols = {r[0] for r in c.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'password_reset_tokens'"))}
            assert "token" not in cols and "token_hash" in cols
            assert c.execute(text(
                "SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL")).scalar() == 0
            assert c.execute(text("SELECT audience FROM refresh_tokens")).scalar() == "nexdata-app"

    def test_upgrade_on_db_without_auth_tables(self, pg_engine):
        from sqlalchemy import text

        mig = _load_migration()
        with pg_engine.begin() as c:
            for stmt in mig.UPGRADE_SQL:
                c.execute(text(stmt))
        with pg_engine.connect() as c:
            assert c.execute(text("SELECT to_regclass('users')")).scalar() is None


@pg
class TestRoleEnforcement:
    def test_anonymous_is_401(self, client):
        assert client.get("/api/v1/export/formats").status_code == 401
        assert client.get("/api/v1/sources").status_code == 401
        assert client.post("/graphql", json={"query": "{__typename}"}).status_code == 401

    def test_user_reads_but_cannot_write(self, client, pg_engine):
        _make_user(pg_engine, "reader@x.com")
        h = {"Authorization": "Bearer " + _login(client, "reader@x.com")["access_token"]}

        # method-based router: GET passes auth, POST ingest does not
        assert client.get("/api/v1/sources", headers=h).status_code not in (401, 403)
        assert client.post("/api/v1/fred/ingest", json={}, headers=h).status_code == 403
        assert client.delete("/api/v1/pe/firms/1", headers=h).status_code == 403
        # admin router: even GET is refused
        assert client.get("/api/v1/export/formats", headers=h).status_code == 403
        assert client.get("/api/v1/settings/api-keys", headers=h).status_code == 403
        # allowlisted pure-computation POST passes auth
        r = client.post("/api/v1/compare/portfolios", json={}, headers=h)
        assert r.status_code not in (401, 403)
        # graphql is user-level
        r = client.post("/graphql", json={"query": "{__typename}"}, headers=h)
        assert r.status_code == 200

    def test_admin_passes_admin_routes(self, client, pg_engine):
        _make_user(pg_engine, "boss@x.com", admin=True)
        body = _login(client, "boss@x.com")
        assert body["user"]["role"] == "admin"
        h = {"Authorization": "Bearer " + body["access_token"]}
        assert client.get("/api/v1/export/formats", headers=h).status_code == 200
        assert client.post("/api/v1/fred/ingest", json={}, headers=h).status_code not in (401, 403)

    def test_demotion_is_immediate(self, client, pg_engine):
        from sqlalchemy import text

        _make_user(pg_engine, "fired@x.com", admin=True)
        h = {"Authorization": "Bearer " + _login(client, "fired@x.com")["access_token"]}
        assert client.get("/api/v1/export/formats", headers=h).status_code == 200
        with pg_engine.begin() as c:
            c.execute(text("UPDATE users SET role = 'user' WHERE email = 'fired@x.com'"))
        assert client.get("/api/v1/export/formats", headers=h).status_code == 403

    def test_require_auth_false_is_open(self, client, env):
        from app.core.config import reset_settings

        env.setenv("REQUIRE_AUTH", "false")
        reset_settings()
        assert client.get("/api/v1/export/formats").status_code == 200

    def test_admin_emails_promote_verified_users_at_login(self, client, pg_engine, env):
        from sqlalchemy import text
        from app.core.config import reset_settings

        _make_user(pg_engine, "owner@x.com")
        with pg_engine.begin() as c:
            c.execute(text("INSERT INTO users (email, password_hash, is_verified) "
                           "VALUES ('unverified@x.com', 'x', FALSE)"))
        env.setenv("ADMIN_EMAILS", "OWNER@x.com,unverified@x.com")
        reset_settings()

        assert _login(client, "owner@x.com")["user"]["role"] == "admin"
        with pg_engine.connect() as c:
            roles = dict(c.execute(text("SELECT email, role FROM users")).fetchall())
        assert roles["owner@x.com"] == "admin"
        assert roles["unverified@x.com"] == "user"


@pg
class TestCreateUserScript:
    def _run(self, argv, env):
        path = REPO / "scripts" / "create_user.py"
        spec = importlib.util.spec_from_file_location("create_user_script", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.main(argv)

    def test_bootstraps_an_admin_and_upgrades_passwordless(self, client, pg_engine, env, capsys):
        from app.users.auth import AuthService

        # a playground account already exists for this email
        db = _session(pg_engine)
        try:
            AuthService(db).request_login_code("founder@x.com")
        finally:
            db.close()

        env.setenv("NEXDATA_NEW_PASSWORD", "founder-pass-1")
        assert self._run(["founder@x.com", "--admin"], env) == 0
        assert "role=admin" in capsys.readouterr().out
        body = _login(client, "founder@x.com", "founder-pass-1")
        assert body["user"]["role"] == "admin"

        # re-running without --admin never demotes
        assert self._run(["founder@x.com"], env) == 0
        assert _login(client, "founder@x.com", "founder-pass-1")["user"]["role"] == "admin"

        env.setenv("NEXDATA_NEW_PASSWORD", "short")
        assert self._run(["other@x.com"], env) == 2


@pg
class TestSignupAndPlayground:
    def test_register_closed_by_default(self, client):
        r = client.post("/api/v1/auth/register",
                        json={"email": "new@x.com", "password": "longenough1"})
        assert r.status_code == 403

    def test_register_open_with_flag_gives_user_role(self, client, env):
        from app.core.config import reset_settings

        env.setenv("ALLOW_SIGNUP", "true")
        env.setenv("ADMIN_EMAILS", "new@x.com")  # unverified: must NOT promote
        reset_settings()
        r = client.post("/api/v1/auth/register",
                        json={"email": "new@x.com", "password": "longenough1"})
        assert r.status_code == 200, r.text
        assert r.json()["user"]["role"] == "user"
        h = {"Authorization": "Bearer " + r.json()["access_token"]}
        assert client.get("/api/v1/export/formats", headers=h).status_code == 403

    def _playground_bundle(self, pg_engine):
        from app.users.auth import AuthService

        db = _session(pg_engine)
        try:
            svc = AuthService(db)
            code = svc.request_login_code("visitor@x.com")["code"]
            return svc.verify_login_code("visitor@x.com", code)
        finally:
            db.close()

    def test_playground_token_cannot_pass_protected_routes(self, client, pg_engine):
        bundle = self._playground_bundle(pg_engine)
        h = {"Authorization": "Bearer " + bundle["access_token"]}
        assert client.get("/api/v1/sources", headers=h).status_code == 401
        assert client.get("/api/v1/auth/me", headers=h).status_code == 401
        assert client.post("/graphql", json={"query": "{__typename}"}, headers=h).status_code == 401
        # the playground still recognises it
        r = client.get("/api/v1/playground/quota", headers=h)
        assert r.status_code == 200 and r.json()["authenticated"] is True

    def test_playground_refresh_stays_playground(self, client, pg_engine):
        bundle = self._playground_bundle(pg_engine)
        r = client.post("/api/v1/auth/refresh", json={"refresh_token": bundle["refresh_token"]})
        assert r.status_code == 200, r.text
        h = {"Authorization": "Bearer " + r.json()["access_token"]}
        assert client.get("/api/v1/sources", headers=h).status_code == 401

    def test_refresh_requires_the_real_token(self, client, pg_engine):
        _make_user(pg_engine, "someone@x.com")
        body = _login(client, "someone@x.com")
        bad = client.post("/api/v1/auth/refresh", json={"refresh_token": "made-up"})
        assert bad.status_code == 401
        good = client.post("/api/v1/auth/refresh", json={"refresh_token": body["refresh_token"]})
        assert good.status_code == 200
        h = {"Authorization": "Bearer " + good.json()["access_token"]}
        assert client.get("/api/v1/sources", headers=h).status_code not in (401, 403)

    def test_passwordless_user_cannot_password_login(self, client, pg_engine):
        self._playground_bundle(pg_engine)
        r = client.post("/api/v1/auth/login",
                        json={"email": "visitor@x.com", "password": "anything12"})
        assert r.status_code == 401


@pg
class TestPasswordReset:
    def test_tokens_stored_hashed(self, pg_engine):
        import hashlib
        from sqlalchemy import text
        from app.users.auth import AuthService

        _make_user(pg_engine, "forgetful@x.com")
        db = _session(pg_engine)
        try:
            svc = AuthService(db)
            raw = svc.request_password_reset("forgetful@x.com")
            assert raw
            stored = db.execute(text("SELECT token_hash FROM password_reset_tokens")).scalar()
            assert stored == hashlib.sha256(raw.encode()).hexdigest()
            assert raw not in stored

            with pytest.raises(ValueError):
                svc.reset_password(stored, "brand-new-pass")  # the hash is not the token
            assert svc.reset_password(raw, "brand-new-pass") is True
            with pytest.raises(ValueError):
                svc.reset_password(raw, "another-pass-1")  # single use
            assert svc.login("forgetful@x.com", "brand-new-pass")["access_token"]
        finally:
            db.close()

    def test_passwordless_account_gets_no_reset_token(self, pg_engine):
        from app.users.auth import AuthService

        db = _session(pg_engine)
        try:
            svc = AuthService(db)
            svc.request_login_code("pgonly@x.com")
            assert svc.request_password_reset("pgonly@x.com") is None
        finally:
            db.close()


@pg
class TestStreamToken:
    def test_stream_token_for_admin(self, client, pg_engine):
        _make_user(pg_engine, "ops@x.com", admin=True)
        h = {"Authorization": "Bearer " + _login(client, "ops@x.com")["access_token"]}
        tok = client.post("/api/v1/auth/stream-token", headers=h)
        assert tok.status_code == 200
        body = tok.json()
        assert body["expires_in"] <= 300
        r = client.get("/api/v1/job-queue/summary", params={"stream_token": body["stream_token"]})
        assert r.status_code not in (401, 403)
        # bearer header still works for the JSON endpoints
        assert client.get("/api/v1/job-queue/summary", headers=h).status_code not in (401, 403)

    def test_stream_token_rules(self, client, pg_engine):
        _make_user(pg_engine, "viewer@x.com")
        login = _login(client, "viewer@x.com")
        h = {"Authorization": "Bearer " + login["access_token"]}
        tok = client.post("/api/v1/auth/stream-token", headers=h).json()["stream_token"]
        # non-admin stream token
        assert client.get("/api/v1/job-queue/summary",
                          params={"stream_token": tok}).status_code == 403
        # an access token is not a stream token
        assert client.get("/api/v1/job-queue/summary",
                          params={"stream_token": login["access_token"]}).status_code == 401
        # and a stream token is not an access token
        assert client.get("/api/v1/sources",
                          headers={"Authorization": "Bearer " + tok}).status_code == 401
        assert client.get("/api/v1/job-queue/summary").status_code == 401


@pg
class TestApiKeyAuth:
    def _key(self, pg_engine, scope):
        from app.auth.api_keys import APIKeyCreate, APIKeyService

        db = _session(pg_engine)
        try:
            return APIKeyService(db).create_key(
                APIKeyCreate(name="t", owner_email="k@x.com", scope=scope)).key
        finally:
            db.close()

    def _mini_app(self, scope_needed):
        from fastapi import Depends, FastAPI
        from app.api.v1.public import APIKeyAuth

        app = FastAPI()

        @app.get("/probe")
        def probe(info=Depends(APIKeyAuth(required_scope=scope_needed))):
            return {"scope": info["scope"]}

        return app

    def test_header_only_and_scope(self, pg_engine):
        from fastapi.testclient import TestClient

        read_key = self._key(pg_engine, "read")
        c = TestClient(self._mini_app("read"))
        assert c.get("/probe", headers={"X-API-Key": read_key}).status_code == 200
        r = c.get("/probe", params={"api_key": read_key})
        assert r.status_code == 401 and "X-API-Key" in r.json()["detail"]

        w = TestClient(self._mini_app("write"))
        assert w.get("/probe", headers={"X-API-Key": read_key}).status_code == 403
        write_key = self._key(pg_engine, "write")
        assert w.get("/probe", headers={"X-API-Key": write_key}).status_code == 200


@pg
class TestAtlasQuota:
    def test_anonymous_metered_admin_not(self, client, pg_engine, env):
        from app.core.config import reset_settings

        env.setenv("ATLAS_LLM_RUNS_PER_IP", "2")
        reset_settings()
        calls = [client.post("/api/v1/atlas/competition", json={}) for _ in range(3)]
        assert [c.status_code for c in calls[:2]] == [200, 200]
        assert calls[2].status_code == 429

        _make_user(pg_engine, "atlasadmin@x.com", admin=True)
        h = {"Authorization": "Bearer " + _login(client, "atlasadmin@x.com")["access_token"]}
        for _ in range(3):
            assert client.post("/api/v1/atlas/competition", json={}, headers=h).status_code == 200


@pg
class TestExportEndpoints:
    def test_sensitive_tables_invisible_even_to_admin(self, client, pg_engine):
        _make_user(pg_engine, "exporter@x.com", admin=True)
        h = {"Authorization": "Bearer " + _login(client, "exporter@x.com")["access_token"]}
        for t in ("users", "password_reset_tokens", "refresh_tokens"):
            assert client.get(f"/api/v1/export/tables/{t}/preview", headers=h).status_code == 404
            assert client.get(f"/api/v1/export/tables/{t}/columns", headers=h).status_code == 404
            r = client.post("/api/v1/export/jobs", headers=h,
                            json={"table_name": t, "format": "csv"})
            assert r.status_code == 400
        listed = client.get("/api/v1/export/tables?refresh=true", headers=h)
        assert listed.status_code == 200
        names = {t["table_name"] for t in listed.json()}
        assert not names & {"users", "password_reset_tokens", "refresh_tokens", "login_codes"}
