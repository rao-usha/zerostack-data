"""
Executive Signal Scorer — Chain 4 of PLAN_052.

Detects leadership transition signals that indicate deal sourcing opportunities:
companies in flux (hiring C-suite), thin leadership, or founder dependency.

3 signals:
  Management Buildup — active C-suite/VP hiring = company in transition
  Founder Risk — old company with thin senior leadership = succession risk
  Leadership Depth — ratio of senior execs to total headcount signal
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass
class ExecSignal:
    signal: str
    score: int           # 0-100 (higher = stronger transition signal)
    reading: str
    flag: Optional[str]  # "succession_in_progress", "founder_transition", "management_buildup", None


@dataclass
class CompanyExecProfile:
    company_id: int
    company_name: str
    industry: str
    transition_score: int    # 0-100 composite (higher = more transition activity)
    signals: List[ExecSignal] = field(default_factory=list)
    flags: List[str] = field(default_factory=list)
    details: Dict = field(default_factory=dict)


def _safe_query(db: Session, sql: str, params: dict):
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.debug("Exec signal query failed: %s", exc)
        return []


class ExecSignalScorer:

    def __init__(self, db: Session):
        self.db = db

    def scan_companies(self, limit: int = 50) -> List[CompanyExecProfile]:
        """Scan companies with executive hiring activity, ranked by transition score."""
        # Find companies with open C-suite/VP/director postings
        rows = _safe_query(self.db, """
            SELECT jp.company_id, ic.name,
                   SUM(CASE WHEN jp.seniority_level = 'c_suite' THEN 1 ELSE 0 END) as csuite,
                   SUM(CASE WHEN jp.seniority_level = 'vp' THEN 1 ELSE 0 END) as vp,
                   SUM(CASE WHEN jp.seniority_level = 'director' THEN 1 ELSE 0 END) as director,
                   COUNT(*) as total_open
            FROM job_postings jp
            JOIN industrial_companies ic ON ic.id = jp.company_id
            WHERE jp.status = 'open'
              AND jp.seniority_level IN ('c_suite', 'vp', 'director')
            GROUP BY jp.company_id, ic.name
            ORDER BY csuite DESC, vp DESC, director DESC
            LIMIT :limit
        """, {"limit": limit})

        results = []
        for r in rows:
            profile = self._score_company(
                company_id=r[0], company_name=r[1],
                csuite_open=int(r[2]), vp_open=int(r[3]),
                director_open=int(r[4]), total_open=int(r[5]),
            )
            results.append(profile)

        results.sort(key=lambda p: p.transition_score, reverse=True)
        return results

    def score_company(self, company_id: int) -> CompanyExecProfile:
        """Score a single company for executive transition signals."""
        # Get company info
        co_rows = _safe_query(self.db, """
            SELECT id, name FROM industrial_companies WHERE id = :cid
        """, {"cid": company_id})
        if not co_rows:
            # Try pe_portfolio_companies
            co_rows = _safe_query(self.db, """
                SELECT id, name FROM pe_portfolio_companies WHERE id = :cid
            """, {"cid": company_id})
        name = co_rows[0][1] if co_rows else f"Company #{company_id}"

        # Get open senior postings
        post_rows = _safe_query(self.db, """
            SELECT seniority_level, COUNT(*) FROM job_postings
            WHERE company_id = :cid AND status = 'open'
              AND seniority_level IN ('c_suite', 'vp', 'director')
            GROUP BY seniority_level
        """, {"cid": company_id})

        counts = dict(post_rows) if post_rows else {}
        csuite = int(counts.get("c_suite", 0))
        vp = int(counts.get("vp", 0))
        director = int(counts.get("director", 0))

        total_rows = _safe_query(self.db, """
            SELECT COUNT(*) FROM job_postings
            WHERE company_id = :cid AND status = 'open'
        """, {"cid": company_id})
        total_open = int(total_rows[0][0]) if total_rows else 0

        return self._score_company(company_id, name, csuite, vp, director, total_open)

    def _score_company(
        self, company_id: int, company_name: str,
        csuite_open: int, vp_open: int, director_open: int, total_open: int,
    ) -> CompanyExecProfile:
        signals = []
        flags = []

        # --- Signal 1: Management Buildup ---
        senior_total = csuite_open + vp_open + director_open
        if csuite_open >= 2:
            score = 100
            reading = f"{csuite_open} C-suite + {vp_open} VP + {director_open} director roles open — major leadership overhaul"
            flag = "succession_in_progress"
        elif csuite_open >= 1:
            score = 80
            reading = f"{csuite_open} C-suite + {vp_open} VP roles open — active executive search"
            flag = "succession_in_progress"
        elif vp_open >= 3:
            score = 70
            reading = f"{vp_open} VP + {director_open} director roles open — management team buildout"
            flag = "management_buildup"
        elif vp_open >= 1 or director_open >= 3:
            score = 50
            reading = f"{vp_open} VP + {director_open} director roles — moderate senior hiring"
            flag = "management_buildup"
        elif director_open >= 1:
            score = 30
            reading = f"{director_open} director-level roles open — routine hiring"
            flag = None
        else:
            score = 0
            reading = "No senior executive hiring activity"
            flag = None

        signals.append(ExecSignal("Management buildup", score, reading, flag))
        if flag:
            flags.append(flag)

        # --- Signal 2: Senior Hiring Intensity ---
        # Ratio of senior roles to total open positions
        if total_open > 0:
            senior_pct = (senior_total / total_open) * 100
            if senior_pct > 20:
                score2 = 90
                reading2 = f"{senior_pct:.0f}% of {total_open} open roles are senior — leadership-heavy hiring"
            elif senior_pct > 10:
                score2 = 60
                reading2 = f"{senior_pct:.0f}% of {total_open} open roles are senior — above normal"
            elif senior_pct > 5:
                score2 = 30
                reading2 = f"{senior_pct:.0f}% of {total_open} open roles are senior — normal ratio"
            else:
                score2 = 10
                reading2 = f"{senior_pct:.0f}% of {total_open} open roles are senior — growth hiring, not transition"
        else:
            score2 = 0
            senior_pct = 0
            reading2 = "No open positions tracked"

        signals.append(ExecSignal("Senior hiring intensity", score2, reading2, None))

        # --- Signal 3: Hiring Velocity ---
        # Total open positions as growth/transition proxy
        if total_open > 500:
            score3 = 90
            reading3 = f"{total_open} open positions — aggressive expansion or transformation"
        elif total_open > 100:
            score3 = 70
            reading3 = f"{total_open} open positions — significant hiring wave"
        elif total_open > 30:
            score3 = 50
            reading3 = f"{total_open} open positions — active hiring"
        elif total_open > 0:
            score3 = 25
            reading3 = f"{total_open} open positions — modest activity"
        else:
            score3 = 0
            reading3 = "No hiring activity tracked"

        signals.append(ExecSignal("Hiring velocity", score3, reading3, None))

        # --- Composite ---
        composite = int(signals[0].score * 0.50 + signals[1].score * 0.30 + signals[2].score * 0.20)
        composite = max(0, min(100, composite))

        return CompanyExecProfile(
            company_id=company_id, company_name=company_name,
            industry="", transition_score=composite,
            signals=signals, flags=list(set(flags)),
            details={
                "csuite_open": csuite_open,
                "vp_open": vp_open,
                "director_open": director_open,
                "total_open": total_open,
                "senior_pct": round(senior_pct, 1) if total_open > 0 else 0,
            },
        )
