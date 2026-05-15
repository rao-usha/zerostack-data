"""
Tests for SPEC 055 — Free-Tier Quota / Metering Service (PLAN_063 C3 / Step 4).

Pure date helpers (T1) need no DB. DB-backed tests use the same real-Postgres
`pg_session` pattern as SPEC_053/054 — they clean up `spec055-` subject_keys
and skip when DATABASE_URL is unset/unreachable.
"""
import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.services.quota.quota_service import QuotaResult, QuotaService


def _key() -> str:
    return f"spec055-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# T1 — pure date helpers, no DB
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSpec055Helpers:
    def test_day_key_and_reset_ts(self):
        """T1: _day_key is YYYY-MM-DD UTC; _reset_ts is the next UTC midnight, after now."""
        fixed = datetime(2026, 5, 14, 18, 30, 0, tzinfo=timezone.utc)
        assert QuotaService._day_key(fixed) == "2026-05-14"
        reset = QuotaService._reset_ts(fixed)
        assert reset.startswith("2026-05-15T00:00:00")
        # reset is strictly after the reference instant
        assert datetime.fromisoformat(reset) > fixed
        # no-arg form returns today's UTC date
        assert QuotaService._day_key() == datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# DB-backed tests
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_session():
    """Real-Postgres session for QuotaService tests. Skips without DATABASE_URL.
    Cleans up spec055- subject_keys before and after."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set — quota service DB tests run in-container only")
    try:
        engine = create_engine(db_url)
        engine.connect().close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Postgres unreachable: {exc}")

    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    QuotaService(db)  # ensure table exists before cleanup

    def _cleanup():
        db.execute(text("DELETE FROM playground_quota_buckets "
                        "WHERE subject_key LIKE 'spec055-%'"))
        db.commit()

    _cleanup()
    try:
        yield db
    finally:
        _cleanup()
        db.close()
        engine.dispose()


@pytest.mark.unit
class TestSpec055QuotaServiceDB:
    def test_check_and_consume_under_limit(self, pg_session):
        """T2: calls under the limit return allowed=True and increment run_count."""
        svc = QuotaService(pg_session)
        key = _key()
        r1 = svc.check_and_consume("ip", key, limit=3)
        assert isinstance(r1, QuotaResult)
        assert r1.allowed is True and r1.used == 1 and r1.remaining == 2
        r2 = svc.check_and_consume("ip", key, limit=3)
        assert r2.allowed is True and r2.used == 2 and r2.remaining == 1

    def test_check_and_consume_at_limit_denies(self, pg_session):
        """T3: the call at the limit returns allowed=False and does not increment."""
        svc = QuotaService(pg_session)
        key = _key()
        svc.check_and_consume("user", key, limit=2)
        svc.check_and_consume("user", key, limit=2)
        denied = svc.check_and_consume("user", key, limit=2)
        assert denied.allowed is False
        assert denied.used == 2  # not incremented past the limit
        assert denied.remaining == 0
        # confirm the bucket really wasn't bumped
        count = pg_session.execute(
            text("SELECT run_count FROM playground_quota_buckets "
                 "WHERE subject_type='user' AND subject_key=:k AND day=:d"),
            {"k": key, "d": QuotaService._day_key()},
        ).scalar()
        assert count == 2

    def test_zero_limit_always_denies(self, pg_session):
        """T4: limit=0 (and negative) denies without incrementing."""
        svc = QuotaService(pg_session)
        key = _key()
        assert svc.check_and_consume("ip", key, limit=0).allowed is False
        assert svc.check_and_consume("ip", key, limit=-5).allowed is False
        count = pg_session.execute(
            text("SELECT COUNT(*) FROM playground_quota_buckets WHERE subject_key=:k"),
            {"k": key},
        ).scalar()
        assert count == 0  # nothing was written

    def test_peek_does_not_mutate(self, pg_session):
        """T5: peek returns the right remaining/allowed and never changes run_count."""
        svc = QuotaService(pg_session)
        key = _key()
        svc.check_and_consume("ip", key, limit=5)  # used=1
        p1 = svc.peek("ip", key, limit=5)
        assert p1.allowed is True and p1.used == 1 and p1.remaining == 4
        p2 = svc.peek("ip", key, limit=5)
        assert p2.used == 1  # peek didn't change anything
        # peeking an untouched subject
        fresh = svc.peek("ip", _key(), limit=5)
        assert fresh.used == 0 and fresh.remaining == 5 and fresh.allowed is True

    def test_separate_subjects_independent(self, pg_session):
        """T6: (user,A), (ip,A), (user,B) are independent buckets."""
        svc = QuotaService(pg_session)
        key_a, key_b = _key(), _key()
        svc.check_and_consume("user", key_a, limit=10)
        svc.check_and_consume("user", key_a, limit=10)
        assert svc.peek("user", key_a, 10).used == 2
        assert svc.peek("ip", key_a, 10).used == 0      # different subject_type
        assert svc.peek("user", key_b, 10).used == 0    # different subject_key

    def test_new_day_resets(self, pg_session):
        """T7: a different day key starts fresh at 0."""
        svc = QuotaService(pg_session)
        key = _key()
        # seed yesterday's bucket directly
        pg_session.execute(
            text("INSERT INTO playground_quota_buckets "
                 "(subject_type, subject_key, day, run_count) "
                 "VALUES ('ip', :k, '2000-01-01', 99)"),
            {"k": key},
        )
        pg_session.commit()
        # today's bucket is untouched by yesterday's count
        today = svc.peek("ip", key, limit=5)
        assert today.used == 0 and today.remaining == 5

    def test_remaining_never_negative(self, pg_session):
        """T8: remaining is clamped at 0 even when denied."""
        svc = QuotaService(pg_session)
        key = _key()
        svc.check_and_consume("ip", key, limit=1)
        denied = svc.check_and_consume("ip", key, limit=1)
        assert denied.allowed is False
        assert denied.remaining == 0
        # even a peek with a now-lower limit clamps, never negative
        assert svc.peek("ip", key, limit=0).remaining == 0
