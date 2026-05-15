"""
Lead capture + intent scoring — PLAN_063 / SPEC_054.

Every playground signup and generator run becomes a `lead` with an intent
score so enterprise sales can see who is worth a conversation. Net-new — there
was no CRM before this.

`AuthService._notify_lead_event` (SPEC_053) calls `upsert_from_signup` /
`mark_verified` behind a guarded import. The playground router (SPEC_056)
calls `record_run` after each generation.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# Generators whose use signals a serious evaluator.
HIGH_VALUE_GENERATORS = {"lp-gp-universe", "private-financials", "consumer-crowd"}

# Free / consumer mailbox providers — an address here is not a corporate lead.
FREE_EMAIL_DOMAINS = {
    "gmail.com", "outlook.com", "yahoo.com", "proton.me", "protonmail.com",
    "icloud.com", "aol.com", "hotmail.com", "live.com", "msn.com",
    "gmx.com", "mail.com", "yandex.com", "zoho.com", "fastmail.com",
}

# Intent tier thresholds
INTENT_HOT = 70
INTENT_WARM = 40


# ---------------------------------------------------------------------------
# Module-level helpers (pure, DB-free)
# ---------------------------------------------------------------------------

def derive_company_domain(email: str) -> Optional[str]:
    """Return the lowercased domain part of an email, or None if malformed.

    (Website-domain normalization lives in
    `app.sources.people_collection.email_inferrer.EmailInferrer`; an email
    domain is just the part after '@', so no import is needed here.)
    """
    if not email or "@" not in email:
        return None
    domain = email.rsplit("@", 1)[1].strip().lower()
    if not domain or "." not in domain or " " in domain:
        return None
    return domain


def is_corporate_email(email: str) -> bool:
    """True only for valid, non-free-mail domains."""
    domain = derive_company_domain(email)
    return bool(domain) and domain not in FREE_EMAIL_DOMAINS


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class LeadService:
    """Lead capture + intent scoring."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                user_id INTEGER,
                company_domain VARCHAR(255),
                company_name VARCHAR(255),
                is_corporate_email BOOLEAN DEFAULT FALSE,
                signup_source VARCHAR(100),
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_runs INTEGER DEFAULT 0,
                verified BOOLEAN DEFAULT FALSE,
                intent_score INTEGER DEFAULT 0,
                intent_tier VARCHAR(10) DEFAULT 'cold',
                generators_used JSONB DEFAULT '{}',
                max_n_requested INTEGER DEFAULT 0,
                routed_to_sales_at TIMESTAMP
            )
        """)
        )
        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS playground_runs (
                id SERIAL PRIMARY KEY,
                lead_id INTEGER,
                user_id INTEGER,
                generator VARCHAR(50) NOT NULL,
                n_requested INTEGER,
                anon_ip VARCHAR(45),
                report_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )
        self.db.execute(
            text("CREATE INDEX IF NOT EXISTS idx_playground_runs_lead "
                 "ON playground_runs(lead_id, created_at DESC)")
        )
        self.db.execute(
            text("CREATE INDEX IF NOT EXISTS idx_playground_runs_ip "
                 "ON playground_runs(anon_ip, created_at DESC)")
        )
        self.db.execute(
            text("CREATE INDEX IF NOT EXISTS idx_leads_intent "
                 "ON leads(intent_score DESC, last_active_at DESC)")
        )
        self.db.commit()

    # ------------------------------------------------------------------
    # Intent scoring — pure function
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_intent_score(
        is_corporate: bool,
        verified: bool,
        total_runs: int,
        generators_used: Dict[str, int],
        max_n_requested: int,
    ) -> tuple[int, str]:
        """Additive 0-100 intent score + tier. Pure — unit-testable without a DB."""
        score = 0
        if is_corporate:
            score += 30
        if verified:
            score += 15
        score += 5 * min(int(total_runs or 0), 6)
        if generators_used and HIGH_VALUE_GENERATORS.intersection(generators_used):
            score += 15
        if (max_n_requested or 0) >= 200:
            score += 10

        score = max(0, min(100, score))
        if score >= INTENT_HOT:
            tier = "hot"
        elif score >= INTENT_WARM:
            tier = "warm"
        else:
            tier = "cold"
        return score, tier

    # ------------------------------------------------------------------
    # Lead lifecycle
    # ------------------------------------------------------------------

    def upsert_from_signup(
        self, email: str, signup_source: str, request_ip: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create or refresh a lead at signup time. Safe to call repeatedly."""
        email = email.lower().strip()
        domain = derive_company_domain(email)
        corporate = is_corporate_email(email)
        company_name = domain.rsplit(".", 1)[0].replace("-", " ").title() if domain else None

        self.db.execute(
            text("""
            INSERT INTO leads (email, company_domain, company_name,
                               is_corporate_email, signup_source)
            VALUES (:email, :domain, :company_name, :corporate, :signup_source)
            ON CONFLICT (email) DO UPDATE SET
                last_active_at = CURRENT_TIMESTAMP,
                company_domain = COALESCE(leads.company_domain, EXCLUDED.company_domain),
                company_name = COALESCE(leads.company_name, EXCLUDED.company_name),
                is_corporate_email = EXCLUDED.is_corporate_email,
                signup_source = COALESCE(leads.signup_source, EXCLUDED.signup_source)
        """),
            {
                "email": email,
                "domain": domain,
                "company_name": company_name,
                "corporate": corporate,
                "signup_source": signup_source,
            },
        )
        self.db.commit()

        lead_id = self._lead_id_for_email(email)
        if lead_id is not None:
            self.recompute_intent(lead_id)
        return self.get_lead(email) or {}

    def record_run(
        self,
        email: Optional[str],
        user_id: Optional[int],
        generator: str,
        n_requested: Optional[int],
        anon_ip: Optional[str] = None,
        report_id: Optional[int] = None,
    ) -> None:
        """Log a playground run. Always inserts a `playground_runs` row; when a
        lead is known (by email), also updates its rollup counters + intent."""
        email = email.lower().strip() if email else None
        lead_id = self._lead_id_for_email(email) if email else None

        self.db.execute(
            text("""
            INSERT INTO playground_runs
                (lead_id, user_id, generator, n_requested, anon_ip, report_id)
            VALUES (:lead_id, :user_id, :generator, :n_requested, :anon_ip, :report_id)
        """),
            {
                "lead_id": lead_id,
                "user_id": user_id,
                "generator": generator,
                "n_requested": n_requested,
                "anon_ip": anon_ip,
                "report_id": report_id,
            },
        )
        self.db.commit()

        if lead_id is None:
            return  # anonymous run — logged, but no lead rollup to update

        row = self.db.execute(
            text("SELECT generators_used, max_n_requested FROM leads WHERE id = :id"),
            {"id": lead_id},
        ).fetchone()
        generators_used = self._as_dict(row[0]) if row else {}
        generators_used[generator] = generators_used.get(generator, 0) + 1
        new_max = max(int(row[1] or 0) if row else 0, int(n_requested or 0))

        self.db.execute(
            text("""
            UPDATE leads SET
                total_runs = total_runs + 1,
                generators_used = CAST(:generators_used AS JSONB),
                max_n_requested = :max_n,
                last_active_at = CURRENT_TIMESTAMP
            WHERE id = :id
        """),
            {
                "generators_used": json.dumps(generators_used),
                "max_n": new_max,
                "id": lead_id,
            },
        )
        self.db.commit()
        self.recompute_intent(lead_id)

    def mark_verified(self, email: str) -> None:
        """Mark a lead's email verified (completed the passwordless flow)."""
        email = email.lower().strip()
        self.db.execute(
            text("UPDATE leads SET verified = TRUE, last_active_at = CURRENT_TIMESTAMP "
                 "WHERE email = :email"),
            {"email": email},
        )
        self.db.commit()
        lead_id = self._lead_id_for_email(email)
        if lead_id is not None:
            self.recompute_intent(lead_id)

    def recompute_intent(self, lead_id: int) -> None:
        """Recompute and persist intent_score/intent_tier. The first transition
        into `hot` stamps `routed_to_sales_at` (idempotent — never overwritten)."""
        row = self.db.execute(
            text("""
            SELECT is_corporate_email, verified, total_runs,
                   generators_used, max_n_requested, routed_to_sales_at
            FROM leads WHERE id = :id
        """),
            {"id": lead_id},
        ).fetchone()
        if not row:
            return

        score, tier = self._compute_intent_score(
            is_corporate=bool(row[0]),
            verified=bool(row[1]),
            total_runs=int(row[2] or 0),
            generators_used=self._as_dict(row[3]),
            max_n_requested=int(row[4] or 0),
        )
        already_routed = row[5] is not None
        stamp_routing = (tier == "hot") and not already_routed

        if stamp_routing:
            self.db.execute(
                text("""
                UPDATE leads SET intent_score = :score, intent_tier = :tier,
                                 routed_to_sales_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """),
                {"score": score, "tier": tier, "id": lead_id},
            )
        else:
            self.db.execute(
                text("UPDATE leads SET intent_score = :score, intent_tier = :tier "
                     "WHERE id = :id"),
                {"score": score, "tier": tier, "id": lead_id},
            )
        self.db.commit()

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def list_hot_leads(
        self,
        min_score: int = INTENT_HOT,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Leads at/above `min_score`, most recently active first."""
        clauses = ["intent_score >= :min_score"]
        params: Dict[str, Any] = {"min_score": min_score, "limit": limit}
        if since is not None:
            clauses.append("last_active_at >= :since")
            params["since"] = since
        where = " AND ".join(clauses)
        rows = self.db.execute(
            text(f"""
            SELECT id, email, company_domain, company_name, is_corporate_email,
                   signup_source, total_runs, verified, intent_score, intent_tier,
                   generators_used, max_n_requested, first_seen_at, last_active_at,
                   routed_to_sales_at
            FROM leads
            WHERE {where}
            ORDER BY last_active_at DESC
            LIMIT :limit
        """),
            params,
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_lead(self, email: str) -> Optional[Dict[str, Any]]:
        row = self.db.execute(
            text("""
            SELECT id, email, company_domain, company_name, is_corporate_email,
                   signup_source, total_runs, verified, intent_score, intent_tier,
                   generators_used, max_n_requested, first_seen_at, last_active_at,
                   routed_to_sales_at
            FROM leads WHERE email = :email
        """),
            {"email": email.lower().strip()},
        ).fetchone()
        return self._row_to_dict(row) if row else None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _lead_id_for_email(self, email: Optional[str]) -> Optional[int]:
        if not email:
            return None
        row = self.db.execute(
            text("SELECT id FROM leads WHERE email = :email"),
            {"email": email.lower().strip()},
        ).fetchone()
        return row[0] if row else None

    @staticmethod
    def _as_dict(value: Any) -> Dict[str, int]:
        """JSONB columns come back as dict on Postgres but may be a str elsewhere."""
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return {}

    @classmethod
    def _row_to_dict(cls, row) -> Dict[str, Any]:
        return {
            "id": row[0],
            "email": row[1],
            "company_domain": row[2],
            "company_name": row[3],
            "is_corporate_email": row[4],
            "signup_source": row[5],
            "total_runs": row[6],
            "verified": row[7],
            "intent_score": row[8],
            "intent_tier": row[9],
            "generators_used": cls._as_dict(row[10]),
            "max_n_requested": row[11],
            "first_seen_at": row[12].isoformat() if row[12] else None,
            "last_active_at": row[13].isoformat() if row[13] else None,
            "routed_to_sales_at": row[14].isoformat() if row[14] else None,
        }
