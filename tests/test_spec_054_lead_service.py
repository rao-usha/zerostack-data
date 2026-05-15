"""
Tests for SPEC 054 — Lead Capture + Intent Scoring Service (PLAN_063 C2 / Step 3).

Pure helpers (T1-T4) need no DB. DB-backed tests (T5-T10) use the same
real-Postgres `pg_session` pattern as SPEC_053 — they clean up `spec054-` rows
and skip when DATABASE_URL is unset/unreachable.
"""
import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.services.leads.lead_service import (
    LeadService,
    derive_company_domain,
    is_corporate_email,
)


def _unique_email(corp: bool = True) -> str:
    suffix = uuid.uuid4().hex[:10]
    return f"spec054-{suffix}@{'acmecorp' if corp else 'gmail'}.com" if corp \
        else f"spec054.{suffix}@gmail.com"


# ---------------------------------------------------------------------------
# T1-T4 — pure helpers / pure function, no DB
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSpec054Helpers:
    def test_derive_company_domain(self):
        """T1: extracts lowercased domain; returns None for malformed input."""
        assert derive_company_domain("Jane.Doe@ACME.com") == "acme.com"
        assert derive_company_domain("a@sub.example.co.uk") == "sub.example.co.uk"
        assert derive_company_domain("not-an-email") is None
        assert derive_company_domain("") is None
        assert derive_company_domain("missing@domain") is None  # no dot
        assert derive_company_domain("two@@ats.com") == "ats.com"

    def test_corporate_email_classification(self):
        """T2: free-mail domains -> not corporate; company domains -> corporate."""
        assert is_corporate_email("ceo@stonepeak.com") is True
        assert is_corporate_email("analyst@kkr.com") is True
        assert is_corporate_email("someone@gmail.com") is False
        assert is_corporate_email("someone@outlook.com") is False
        assert is_corporate_email("someone@proton.me") is False
        assert is_corporate_email("garbage") is False

    def test_compute_intent_score_terms(self):
        """T3: each scoring term adds the right amount; pure function, no DB."""
        f = LeadService._compute_intent_score
        # baseline: nothing
        assert f(False, False, 0, {}, 0) == (0, "cold")
        # corporate email +30
        assert f(True, False, 0, {}, 0)[0] == 30
        # verified +15
        assert f(False, True, 0, {}, 0)[0] == 15
        # runs: +5 * min(runs, 6)
        assert f(False, False, 3, {}, 0)[0] == 15
        assert f(False, False, 10, {}, 0)[0] == 30  # capped at 6 runs
        # high-value generator +15
        assert f(False, False, 0, {"private-financials": 1}, 0)[0] == 15
        assert f(False, False, 0, {"job-postings": 1}, 0)[0] == 0  # not high-value
        # large N +10
        assert f(False, False, 0, {}, 250)[0] == 10
        assert f(False, False, 0, {}, 199)[0] == 0

    def test_compute_intent_tiers(self):
        """T4: score -> tier thresholds; score clamped <= 100."""
        f = LeadService._compute_intent_score
        # corporate(30) + verified(15) + 6 runs(30) + hi-val(15) + bigN(10) = 100
        score, tier = f(True, True, 6, {"consumer-crowd": 2}, 500)
        assert score == 100 and tier == "hot"
        # corporate(30) + verified(15) = 45 -> warm
        assert f(True, True, 0, {}, 0) == (45, "warm")
        # corporate(30) only -> cold
        assert f(True, False, 0, {}, 0) == (30, "cold")
        # exactly 70 -> hot ; corporate(30)+verified(15)+5runs(25) = 70
        assert f(True, True, 5, {}, 0) == (70, "hot")


# ---------------------------------------------------------------------------
# DB-backed tests
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_session():
    """Real-Postgres session for LeadService tests. Skips without DATABASE_URL.
    Cleans up spec054- rows before and after."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set — lead service DB tests run in-container only")
    try:
        engine = create_engine(db_url)
        engine.connect().close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Postgres unreachable: {exc}")

    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    LeadService(db)  # ensure tables exist before cleanup

    def _cleanup():
        db.execute(text("DELETE FROM playground_runs WHERE lead_id IN "
                        "(SELECT id FROM leads WHERE email LIKE 'spec054-%' "
                        "OR email LIKE 'spec054.%')"))
        db.execute(text("DELETE FROM leads WHERE email LIKE 'spec054-%' "
                        "OR email LIKE 'spec054.%'"))
        db.commit()

    _cleanup()
    try:
        yield db
    finally:
        _cleanup()
        db.close()
        engine.dispose()


@pytest.mark.unit
class TestSpec054LeadServiceDB:
    def test_upsert_from_signup_creates_and_updates(self, pg_session):
        """T5: first call creates the lead; second updates, no duplicate, no raise."""
        svc = LeadService(pg_session)
        email = _unique_email(corp=True)

        lead = svc.upsert_from_signup(email, "playground", request_ip="1.2.3.4")
        assert lead["email"] == email
        assert lead["is_corporate_email"] is True
        assert lead["company_domain"] == email.split("@", 1)[1]

        # second call must not duplicate or raise
        svc.upsert_from_signup(email, "playground")
        count = pg_session.execute(
            text("SELECT COUNT(*) FROM leads WHERE email = :e"), {"e": email}
        ).scalar()
        assert count == 1

    def test_record_run_logs_and_updates_lead(self, pg_session):
        """T6: inserts a playground_runs row; bumps total_runs + generators_used."""
        svc = LeadService(pg_session)
        email = _unique_email(corp=True)
        svc.upsert_from_signup(email, "playground")

        svc.record_run(email, None, "macro-scenarios", n_requested=50)
        svc.record_run(email, None, "macro-scenarios", n_requested=300)
        svc.record_run(email, None, "private-financials", n_requested=20)

        lead = svc.get_lead(email)
        assert lead["total_runs"] == 3
        assert lead["generators_used"] == {"macro-scenarios": 2, "private-financials": 1}
        assert lead["max_n_requested"] == 300

        runs = pg_session.execute(
            text("SELECT COUNT(*) FROM playground_runs WHERE lead_id = :id"),
            {"id": lead["id"]},
        ).scalar()
        assert runs == 3

    def test_record_run_anonymous(self, pg_session):
        """T7: a run with no email still logs a playground_runs row (lead_id NULL)."""
        svc = LeadService(pg_session)
        before = pg_session.execute(
            text("SELECT COUNT(*) FROM playground_runs WHERE lead_id IS NULL "
                 "AND anon_ip = :ip"),
            {"ip": "203.0.113.99"},
        ).scalar()
        svc.record_run(None, None, "macro-scenarios", n_requested=10,
                       anon_ip="203.0.113.99")
        after = pg_session.execute(
            text("SELECT COUNT(*) FROM playground_runs WHERE lead_id IS NULL "
                 "AND anon_ip = :ip"),
            {"ip": "203.0.113.99"},
        ).scalar()
        assert after == before + 1
        # cleanup this stray anon row (not caught by email-prefix cleanup)
        pg_session.execute(
            text("DELETE FROM playground_runs WHERE anon_ip = '203.0.113.99'")
        )
        pg_session.commit()

    def test_mark_verified_recomputes(self, pg_session):
        """T8: mark_verified sets verified + raises the score."""
        svc = LeadService(pg_session)
        email = _unique_email(corp=True)
        svc.upsert_from_signup(email, "playground")
        before = svc.get_lead(email)["intent_score"]  # corporate only -> 30

        svc.mark_verified(email)
        lead = svc.get_lead(email)
        assert lead["verified"] is True
        assert lead["intent_score"] == before + 15  # +15 for verified

    def test_hot_lead_routed_to_sales_stamped_once(self, pg_session):
        """T9: crossing into hot stamps routed_to_sales_at once; not overwritten."""
        svc = LeadService(pg_session)
        email = _unique_email(corp=True)  # +30 corporate
        svc.upsert_from_signup(email, "playground")
        svc.mark_verified(email)  # +15 -> 45 (warm), not routed yet
        assert svc.get_lead(email)["routed_to_sales_at"] is None

        # push into hot: 5 runs (+25) + a high-value generator (+15) -> 100
        for _ in range(5):
            svc.record_run(email, None, "private-financials", n_requested=10)
        lead = svc.get_lead(email)
        assert lead["intent_tier"] == "hot"
        stamped = lead["routed_to_sales_at"]
        assert stamped is not None

        # further activity must not overwrite the routing stamp
        svc.record_run(email, None, "private-financials", n_requested=10)
        assert svc.get_lead(email)["routed_to_sales_at"] == stamped

    def test_list_hot_leads(self, pg_session):
        """T10: returns only leads >= threshold."""
        svc = LeadService(pg_session)
        hot_email = _unique_email(corp=True)
        cold_email = _unique_email(corp=False)  # gmail -> not corporate

        # hot: corporate + verified + 5 runs + hi-val generator
        svc.upsert_from_signup(hot_email, "playground")
        svc.mark_verified(hot_email)
        for _ in range(5):
            svc.record_run(hot_email, None, "consumer-crowd", n_requested=10)
        # cold: free-mail signup only
        svc.upsert_from_signup(cold_email, "playground")

        hot = svc.list_hot_leads(min_score=70)
        emails = {r["email"] for r in hot}
        assert hot_email in emails
        assert cold_email not in emails
