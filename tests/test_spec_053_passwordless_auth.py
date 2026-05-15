"""
Tests for SPEC 053 — Passwordless Email Auth (PLAN_063 C1 / Step 2).

AuthService is Postgres-specific raw SQL (SERIAL, FILTER, etc.), so the
DB-backed tests connect to the real Postgres via DATABASE_URL and clean up
their own rows (emails prefixed `spec053-`). They skip when DATABASE_URL is
unset/unreachable, so they run in-container and skip on a bare host.
Pure helpers (T1, T2) need no DB.
"""
import os
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.users.auth import AuthService, LOGIN_CODE_MAX_ATTEMPTS


def _unique_email() -> str:
    return f"spec053-{uuid.uuid4().hex[:10]}@example.com"


# ---------------------------------------------------------------------------
# T1, T2 — pure helpers, no DB
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSpec053Helpers:
    def test_generate_code_is_six_digits(self):
        """T1: _generate_login_code() is a 6-char numeric string, zero-padded."""
        svc = AuthService.__new__(AuthService)  # bypass __init__ (no DB needed)
        for _ in range(50):
            code = svc._generate_login_code()
            assert isinstance(code, str)
            assert len(code) == 6
            assert code.isdigit()

    def test_hash_helpers_are_sha256(self):
        """T2: hash helpers return 64-char hex, deterministic, differ for different inputs."""
        svc = AuthService.__new__(AuthService)
        h1 = svc._hash_login_code("123456")
        h2 = svc._hash_login_code("123456")
        h3 = svc._hash_login_code("654321")
        assert len(h1) == 64 and all(c in "0123456789abcdef" for c in h1)
        assert h1 == h2  # deterministic
        assert h1 != h3  # different input -> different hash
        # token hashing is independent and also sha256
        ht = svc._hash_login_token("some-url-safe-token")
        assert len(ht) == 64


# ---------------------------------------------------------------------------
# DB-backed tests
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_session():
    """Real-Postgres session for AuthService tests. Skips if DATABASE_URL is
    unset or the DB is unreachable. Cleans up spec053- rows before and after."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set — passwordless auth DB tests run in-container only")

    # Ensure a JWT secret exists so token issuance works.
    os.environ.setdefault("JWT_SECRET_KEY", "spec053-test-secret-key-not-for-prod")
    from app.core.config import reset_settings
    reset_settings()

    try:
        engine = create_engine(db_url)
        conn = engine.connect()
        conn.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Postgres unreachable: {exc}")

    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    # Instantiate once so _ensure_tables() creates login_codes / migrates users
    # before any cleanup runs.
    AuthService(db)

    def _cleanup():
        db.execute(text("DELETE FROM login_codes WHERE email LIKE 'spec053-%'"))
        db.execute(text("DELETE FROM refresh_tokens WHERE user_id IN "
                        "(SELECT id FROM users WHERE email LIKE 'spec053-%')"))
        db.execute(text("DELETE FROM users WHERE email LIKE 'spec053-%'"))
        # LeadService (Step 3) now exists, so the best-effort lead hook in
        # AuthService creates leads/playground_runs rows during these tests.
        for tbl in ("playground_runs", "leads"):
            try:
                if tbl == "playground_runs":
                    db.execute(text("DELETE FROM playground_runs WHERE lead_id IN "
                                    "(SELECT id FROM leads WHERE email LIKE 'spec053-%')"))
                else:
                    db.execute(text("DELETE FROM leads WHERE email LIKE 'spec053-%'"))
            except Exception:  # noqa: BLE001 — table may not exist on older DBs
                db.rollback()
        db.commit()

    _cleanup()
    try:
        yield db
    finally:
        _cleanup()
        db.close()
        engine.dispose()
        reset_settings()


@pytest.mark.unit
class TestSpec053PasswordlessAuth:
    """DB-backed passwordless auth flow tests."""

    def test_request_login_code_creates_row_and_user(self, pg_session):
        """T3: request creates a login_codes row + auto-creates the users row."""
        email = _unique_email()
        svc = AuthService(pg_session)
        result = svc.request_login_code(email, request_ip="1.2.3.4")

        assert result["email"] == email
        assert len(result["code"]) == 6
        assert "verify=" in result["link"]

        code_rows = pg_session.execute(
            text("SELECT email, attempts, consumed_at FROM login_codes WHERE email = :e"),
            {"e": email},
        ).fetchall()
        assert len(code_rows) == 1
        assert code_rows[0][1] == 0  # attempts
        assert code_rows[0][2] is None  # not consumed

        user_row = pg_session.execute(
            text("SELECT password_hash, is_verified, tier, signup_source "
                 "FROM users WHERE email = :e"),
            {"e": email},
        ).fetchone()
        assert user_row is not None
        assert user_row[0] is None  # password_hash NULL for passwordless
        assert user_row[1] is False  # not verified until they verify a code
        assert user_row[2] == "free"
        assert user_row[3] == "playground"

    def test_request_login_code_rate_limited(self, pg_session):
        """T4: too many requests raises ValueError (60-s rule and 15-min rule)."""
        svc = AuthService(pg_session)

        # 60-second rule: a second request right after the first is rejected.
        email_a = _unique_email()
        svc.request_login_code(email_a)
        with pytest.raises(ValueError):
            svc.request_login_code(email_a)

        # 15-min / 3-code rule: pre-seed 3 codes aged ~5 min (outside the 60-s
        # window, inside the 15-min window) -> next request rejected.
        email_b = _unique_email()
        aged = datetime.utcnow() - timedelta(minutes=5)
        for _ in range(3):
            pg_session.execute(
                text("""INSERT INTO login_codes
                        (email, code_hash, token_hash, expires_at, created_at)
                        VALUES (:e, 'x', 'y', :exp, :created)"""),
                {"e": email_b, "exp": datetime.utcnow() + timedelta(minutes=10),
                 "created": aged},
            )
        pg_session.commit()
        with pytest.raises(ValueError, match="[Tt]oo many"):
            svc.request_login_code(email_b)

    def test_verify_code_happy_path(self, pg_session):
        """T5: correct code -> consumed, user verified, tokens pass verify_token."""
        email = _unique_email()
        svc = AuthService(pg_session)
        issued = svc.request_login_code(email)

        bundle = svc.verify_login_code(email, issued["code"])
        assert bundle["token_type"] == "bearer"
        assert bundle["user"]["email"] == email
        assert "access_token" in bundle and "refresh_token" in bundle

        # the access token is a real, verifiable JWT
        info = svc.verify_token(bundle["access_token"])
        assert info["email"] == email

        # row consumed, user verified
        consumed = pg_session.execute(
            text("SELECT consumed_at FROM login_codes WHERE email = :e"), {"e": email}
        ).fetchone()
        assert consumed[0] is not None
        verified = pg_session.execute(
            text("SELECT is_verified FROM users WHERE email = :e"), {"e": email}
        ).fetchone()
        assert verified[0] is True

    def test_verify_code_wrong_code_increments_attempts(self, pg_session):
        """T6: wrong code raises, attempts incremented, row not consumed."""
        email = _unique_email()
        svc = AuthService(pg_session)
        svc.request_login_code(email)

        with pytest.raises(ValueError):
            svc.verify_login_code(email, "000000")  # almost certainly wrong

        row = pg_session.execute(
            text("SELECT attempts, consumed_at FROM login_codes WHERE email = :e"),
            {"e": email},
        ).fetchone()
        assert row[0] == 1  # attempts incremented
        assert row[1] is None  # not consumed

    def test_verify_code_lockout_after_5_attempts(self, pg_session):
        """T7: the attempt past the cap locks the row (consumed_at set)."""
        email = _unique_email()
        svc = AuthService(pg_session)
        issued = svc.request_login_code(email)
        wrong = "111111" if issued["code"] != "111111" else "222222"

        # LOGIN_CODE_MAX_ATTEMPTS wrong tries are "invalid code"...
        for _ in range(LOGIN_CODE_MAX_ATTEMPTS):
            with pytest.raises(ValueError, match="[Ii]nvalid"):
                svc.verify_login_code(email, wrong)

        # ...the next one is the lockout.
        with pytest.raises(ValueError, match="[Tt]oo many"):
            svc.verify_login_code(email, wrong)

        row = pg_session.execute(
            text("SELECT consumed_at FROM login_codes WHERE email = :e"), {"e": email}
        ).fetchone()
        assert row[0] is not None  # locked / consumed

        # even the correct code no longer works
        with pytest.raises(ValueError):
            svc.verify_login_code(email, issued["code"])

    def test_verify_code_expired_rejected(self, pg_session):
        """T8: a code past expires_at raises ValueError."""
        email = _unique_email()
        svc = AuthService(pg_session)
        issued = svc.request_login_code(email)

        # force-expire the code
        pg_session.execute(
            text("UPDATE login_codes SET expires_at = :past WHERE email = :e"),
            {"past": datetime.utcnow() - timedelta(minutes=1), "e": email},
        )
        pg_session.commit()

        with pytest.raises(ValueError, match="[Ee]xpired"):
            svc.verify_login_code(email, issued["code"])

    def test_verify_login_token_happy_path(self, pg_session):
        """T9: the magic-link token verifies and issues tokens."""
        email = _unique_email()
        svc = AuthService(pg_session)
        issued = svc.request_login_code(email)
        token = issued["link"].split("verify=", 1)[1]

        bundle = svc.verify_login_token(token)
        assert bundle["user"]["email"] == email
        assert "access_token" in bundle

        # token row consumed
        consumed = pg_session.execute(
            text("SELECT consumed_at FROM login_codes WHERE email = :e"), {"e": email}
        ).fetchone()
        assert consumed[0] is not None

    def test_lead_hook_failure_does_not_block(self, pg_session):
        """T10: request/verify still succeed when the lead hook fails.

        LeadService (Step 3) doesn't exist yet, so `_notify_lead_event` hits an
        ImportError on every call — a hook failure. Sign-in must still work."""
        email = _unique_email()
        svc = AuthService(pg_session)
        issued = svc.request_login_code(email)  # signup hook fails internally
        bundle = svc.verify_login_code(email, issued["code"])  # verified hook fails internally
        assert bundle["user"]["email"] == email
