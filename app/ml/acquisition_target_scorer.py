"""
Acquisition Target Score — core scoring engine.

Identifies PE portfolio companies that are attractive acquisition targets
by combining growth signals, market attractiveness, management gaps,
deal activity signals, and sector momentum into a 0-100 composite score.
"""

import json
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.ml.acquisition_target_metadata import (
    generate_create_acquisition_target_scores_sql,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
MODEL_VERSION = "v1.0"

WEIGHTS = {
    "growth_signal": 0.30,
    "market_attractiveness": 0.20,
    "management_gap": 0.20,
    "deal_activity": 0.15,
    "sector_momentum": 0.15,
}

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]

NEUTRAL_SCORE = 50.0

# Job title keywords signaling deal activity
DEAL_ACTIVITY_TITLES = [
    "corporate development",
    "corp dev",
    "vp business development",
    "m&a",
    "mergers and acquisitions",
    "business development director",
    "strategic partnerships",
]

DISTRESS_TITLES = [
    "restructuring",
    "turnaround",
    "chief restructuring",
    "interim ceo",
    "interim cfo",
]


class AcquisitionTargetScorer:
    """Compute acquisition target scores for PE portfolio companies."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        from app.core.database import get_engine
        try:
            engine = get_engine()
            raw_conn = engine.raw_connection()
            try:
                cursor = raw_conn.cursor()
                cursor.execute(generate_create_acquisition_target_scores_sql())
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"Acquisition target table creation warning: {e}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_grade(score: float) -> str:
        for threshold, grade in GRADE_THRESHOLDS:
            if score >= threshold:
                return grade
        return "F"

    # ------------------------------------------------------------------
    # Data retrieval
    # ------------------------------------------------------------------

    def _get_company_info(self, company_id: int) -> Optional[Dict]:
        query = text("""
            SELECT id, name, industry, sector, current_pe_owner,
                   founded_year, employee_count
            FROM pe_portfolio_companies WHERE id = :id
        """)
        try:
            row = self.db.execute(query, {"id": company_id}).fetchone()
            if row:
                return {
                    "id": row[0], "name": row[1], "industry": row[2],
                    "sector": row[3], "pe_owner": row[4],
                    "founded_year": row[5], "employee_count": row[6],
                }
            return None
        except Exception:
            self.db.rollback()
            return None

    def _get_financials(self, company_id: int) -> List:
        query = text("""
            SELECT fiscal_year, revenue_usd, revenue_growth_pct,
                   ebitda_usd, ebitda_margin_pct, net_income_usd,
                   free_cash_flow_usd, debt_to_ebitda
            FROM pe_company_financials
            WHERE company_id = :id AND fiscal_period = 'FY'
            ORDER BY fiscal_year DESC LIMIT 5
        """)
        try:
            return self.db.execute(query, {"id": company_id}).fetchall()
        except Exception:
            self.db.rollback()
            return []

    def _get_leadership(self, company_id: int) -> List:
        query = text("""
            SELECT title, role_category, is_ceo, is_cfo, is_board_member,
                   start_date, is_current, appointed_by_pe
            FROM pe_company_leadership
            WHERE company_id = :id AND is_current = true
        """)
        try:
            return self.db.execute(query, {"id": company_id}).fetchall()
        except Exception:
            self.db.rollback()
            return []

    def _get_hiring_velocity_via_match(self, company_id: int) -> Optional[float]:
        """Get hiring velocity by name-matching PE company to industrial_companies."""
        query = text("""
            SELECT name FROM pe_portfolio_companies WHERE id = :id
        """)
        try:
            row = self.db.execute(query, {"id": company_id}).fetchone()
            if not row:
                return None
            pe_name = row[0]
        except Exception:
            self.db.rollback()
            return None

        match_query = text("""
            SELECT ic.id FROM industrial_companies ic
            WHERE LOWER(TRIM(ic.name)) = LOWER(TRIM(:name))
            LIMIT 1
        """)
        try:
            match_row = self.db.execute(match_query, {"name": pe_name}).fetchone()
            if not match_row:
                return None
        except Exception:
            self.db.rollback()
            return None

        vel_query = text("""
            SELECT overall_score FROM hiring_velocity_scores
            WHERE company_id = :cid
            ORDER BY score_date DESC LIMIT 1
        """)
        try:
            vel_row = self.db.execute(vel_query, {"cid": match_row[0]}).fetchone()
            return float(vel_row[0]) if vel_row else None
        except Exception:
            self.db.rollback()
            return None

    def _get_sector_deal_count(self, industry: Optional[str]) -> int:
        """Count PE deals in same industry in last 3 years."""
        if not industry:
            return 0
        query = text("""
            SELECT COUNT(DISTINCT fi.company_id)
            FROM pe_fund_investments fi
            JOIN pe_portfolio_companies pc ON fi.company_id = pc.id
            WHERE pc.industry = :industry
              AND fi.investment_date >= CURRENT_DATE - 1095
        """)
        try:
            row = self.db.execute(query, {"industry": industry}).fetchone()
            return int(row[0]) if row and row[0] else 0
        except Exception:
            self.db.rollback()
            return 0

    def _get_sector_fund_count(self, sector: Optional[str]) -> int:
        """Count PE firms with matching sector_focus."""
        if not sector:
            return 0
        query = text("""
            SELECT COUNT(DISTINCT id) FROM pe_firms
            WHERE sector_focus ILIKE :pattern
        """)
        try:
            row = self.db.execute(
                query, {"pattern": f"%{sector}%"}
            ).fetchone()
            return int(row[0]) if row and row[0] else 0
        except Exception:
            self.db.rollback()
            return 0

    def _get_deal_activity_titles(self, company_id: int) -> Dict[str, Any]:
        """Scan job postings for deal activity and distress keywords."""
        query = text("""
            SELECT name FROM pe_portfolio_companies WHERE id = :id
        """)
        try:
            row = self.db.execute(query, {"id": company_id}).fetchone()
            if not row:
                return {"deal_titles": [], "distress_titles": []}
            pe_name = row[0]
        except Exception:
            self.db.rollback()
            return {"deal_titles": [], "distress_titles": []}

        search_query = text("""
            SELECT DISTINCT LOWER(title) as title_lower
            FROM job_postings
            WHERE LOWER(company_name) LIKE :pattern
              AND posted_date >= CURRENT_DATE - 180
        """)
        deal_found = []
        distress_found = []
        try:
            pattern = f"%{pe_name.lower().split()[0]}%"
            jp_rows = self.db.execute(search_query, {"pattern": pattern}).fetchall()
            for jp_row in jp_rows:
                title = jp_row[0] if jp_row[0] else ""
                for kw in DEAL_ACTIVITY_TITLES:
                    if kw in title:
                        deal_found.append(title)
                        break
                for kw in DISTRESS_TITLES:
                    if kw in title:
                        distress_found.append(title)
                        break
        except Exception:
            self.db.rollback()

        return {
            "deal_titles": deal_found[:10],
            "distress_titles": distress_found[:10],
            "deal_count": len(deal_found),
            "distress_count": len(distress_found),
        }

    # ------------------------------------------------------------------
    # Sub-score calculations
    # ------------------------------------------------------------------

    def _calc_growth_signal(
        self, fin_rows: List, velocity: Optional[float]
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        """Revenue growth from financials + hiring velocity via name match."""
        strengths, risks = [], []
        score = NEUTRAL_SCORE

        rev_growth = None
        if fin_rows:
            latest = fin_rows[0]
            rev_growth = float(latest[2]) if latest[2] else None

            if rev_growth is not None:
                if rev_growth > 20:
                    score = 85
                    strengths.append(f"High revenue growth ({rev_growth:.1f}%)")
                elif rev_growth > 10:
                    score = 70
                    strengths.append(f"Solid revenue growth ({rev_growth:.1f}%)")
                elif rev_growth > 0:
                    score = 55
                elif rev_growth < -5:
                    score = 25
                    risks.append(f"Revenue declining ({rev_growth:.1f}%)")
                else:
                    score = 40

        # Blend with hiring velocity if available
        if velocity is not None:
            # Weight: 60% financials, 40% hiring
            fin_component = score * 0.6
            hire_component = velocity * 0.4
            score = fin_component + hire_component
            if velocity > 70:
                strengths.append("Strong hiring momentum signals continued growth")

        details = {
            "revenue_growth_pct": rev_growth,
            "hiring_velocity": velocity,
            "blended": velocity is not None,
        }
        return max(0, min(100, round(score, 1))), details, strengths, risks

    def _calc_market_attractiveness(
        self, industry: Optional[str], sector_deal_count: int
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        """Count PE deals in same industry (last 3yr) — hot sector = high score."""
        strengths, risks = [], []

        if not industry:
            return NEUTRAL_SCORE, {"message": "No industry data"}, [], []

        if sector_deal_count >= 20:
            score = 90
            strengths.append(
                f"Hot sector: {sector_deal_count} PE deals in {industry} (3yr)"
            )
        elif sector_deal_count >= 10:
            score = 75
            strengths.append(
                f"Active sector: {sector_deal_count} PE deals in {industry} (3yr)"
            )
        elif sector_deal_count >= 5:
            score = 60
        elif sector_deal_count >= 1:
            score = 45
        else:
            score = 30
            risks.append(f"Limited PE deal activity in {industry}")

        details = {
            "industry": industry,
            "sector_pe_deal_count": sector_deal_count,
        }
        return score, details, strengths, risks

    def _calc_management_gap(
        self, lead_rows: List, employee_count: Optional[int]
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        """
        Thin leadership = PE value-add opportunity = high acquisition score.

        INVERTED from exit readiness: fewer leaders = MORE attractive target
        because PE can install professional management and create value.
        """
        strengths, risks = [], []

        if not lead_rows:
            # No leadership data = maximum management gap = high target score
            return 80.0, {
                "message": "No leadership data - significant management gap opportunity",
                "management_gap": "high",
            }, ["Significant management gap - PE value-add opportunity"], []

        has_ceo = any(r[2] for r in lead_rows)
        has_cfo = any(r[3] for r in lead_rows)
        c_suite_count = sum(1 for r in lead_rows if r[1] == "C-Suite")
        pe_appointed = sum(1 for r in lead_rows if r[7])

        # Start high (thin org = opportunity), subtract for completeness
        score = 80

        if has_ceo:
            score -= 10
        else:
            strengths.append("No CEO - PE can install operator")
        if has_cfo:
            score -= 15
        else:
            strengths.append("No CFO - PE can professionalize finance")
        if c_suite_count >= 4:
            score -= 20  # Already well-managed, less PE value-add
            risks.append("Well-staffed C-suite reduces PE value-add opportunity")
        elif c_suite_count >= 2:
            score -= 10
        if pe_appointed >= 2:
            score -= 15  # Already PE-managed
            risks.append("Already has PE-appointed management")

        # Small company with thin team = bigger opportunity
        if employee_count and employee_count > 500 and c_suite_count < 3:
            score += 10
            strengths.append(
                f"{employee_count} employees with only {c_suite_count} C-suite - "
                "management buildout opportunity"
            )

        details = {
            "has_ceo": has_ceo,
            "has_cfo": has_cfo,
            "c_suite_count": c_suite_count,
            "pe_appointed_count": pe_appointed,
            "total_leaders": len(lead_rows),
            "employee_count": employee_count,
            "management_gap": (
                "high" if score >= 65 else "medium" if score >= 45 else "low"
            ),
        }
        return max(0, min(100, score)), details, strengths, risks

    def _calc_deal_activity(
        self, title_data: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        """Job titles signaling deal exploration or distress."""
        strengths, risks = [], []
        score = NEUTRAL_SCORE

        deal_count = title_data.get("deal_count", 0)
        distress_count = title_data.get("distress_count", 0)

        if deal_count >= 3:
            score = 80
            strengths.append(
                f"{deal_count} deal-related job postings (Corp Dev, M&A, Biz Dev)"
            )
        elif deal_count >= 1:
            score = 65
            strengths.append("Deal-related hiring activity detected")

        if distress_count >= 2:
            score = min(100, score + 20)
            strengths.append(
                f"{distress_count} distress-signal postings (restructuring, turnaround)"
            )
        elif distress_count == 1:
            score = min(100, score + 10)

        details = {
            "deal_titles": title_data.get("deal_titles", []),
            "distress_titles": title_data.get("distress_titles", []),
            "deal_count": deal_count,
            "distress_count": distress_count,
        }
        return max(0, min(100, score)), details, strengths, risks

    def _calc_sector_momentum(
        self, sector: Optional[str], fund_count: int
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        """PE firms with matching sector_focus — more funds = market consensus."""
        strengths, risks = [], []

        if not sector:
            return NEUTRAL_SCORE, {"message": "No sector data"}, [], []

        if fund_count >= 15:
            score = 90
            strengths.append(
                f"{fund_count} PE firms focused on {sector} - strong consensus"
            )
        elif fund_count >= 8:
            score = 75
            strengths.append(
                f"{fund_count} PE firms in {sector} sector"
            )
        elif fund_count >= 3:
            score = 60
        elif fund_count >= 1:
            score = 45
        else:
            score = 30
            risks.append(f"Few PE firms targeting {sector} sector")

        details = {
            "sector": sector,
            "pe_firms_with_focus": fund_count,
        }
        return score, details, strengths, risks

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------

    def _calculate_confidence(
        self,
        fin_rows: List,
        lead_rows: List,
        velocity: Optional[float],
        sector_deal_count: int,
        title_data: Dict,
        fund_count: int,
    ) -> float:
        confidence = 0.0
        if fin_rows:
            confidence += 0.30
        if lead_rows:
            confidence += 0.15
        if velocity is not None:
            confidence += 0.15
        if sector_deal_count > 0:
            confidence += 0.15
        if title_data.get("deal_count", 0) > 0 or title_data.get("distress_count", 0) > 0:
            confidence += 0.10
        if fund_count > 0:
            confidence += 0.10
        return min(confidence, 1.0)

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def _get_cached_score(
        self, company_id: int, score_date: date
    ) -> Optional[Dict[str, Any]]:
        query = text("""
            SELECT * FROM acquisition_target_scores
            WHERE company_id = :cid AND score_date = :sd
              AND model_version = :ver
            LIMIT 1
        """)
        try:
            row = self.db.execute(
                query,
                {"cid": company_id, "sd": score_date, "ver": MODEL_VERSION},
            ).mappings().fetchone()
            if row:
                result = dict(row)
                result["cached"] = True
                return result
        except Exception:
            self.db.rollback()
        return None

    def _save_score(self, result: Dict[str, Any]) -> None:
        query = text("""
            INSERT INTO acquisition_target_scores (
                company_id, score_date, overall_score, grade, confidence,
                growth_signal_score, market_attractiveness_score,
                management_gap_score, deal_activity_score,
                sector_momentum_score,
                revenue_growth_pct, employee_count,
                leadership_count, sector_pe_deal_count,
                strengths, risks, metadata, model_version
            ) VALUES (
                :company_id, :score_date, :overall_score, :grade, :confidence,
                :growth_signal_score, :market_attractiveness_score,
                :management_gap_score, :deal_activity_score,
                :sector_momentum_score,
                :revenue_growth_pct, :employee_count,
                :leadership_count, :sector_pe_deal_count,
                CAST(:strengths AS jsonb), CAST(:risks AS jsonb),
                CAST(:metadata AS jsonb), :version
            )
            ON CONFLICT (company_id, score_date) DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                grade = EXCLUDED.grade,
                confidence = EXCLUDED.confidence,
                growth_signal_score = EXCLUDED.growth_signal_score,
                market_attractiveness_score = EXCLUDED.market_attractiveness_score,
                management_gap_score = EXCLUDED.management_gap_score,
                deal_activity_score = EXCLUDED.deal_activity_score,
                sector_momentum_score = EXCLUDED.sector_momentum_score,
                revenue_growth_pct = EXCLUDED.revenue_growth_pct,
                employee_count = EXCLUDED.employee_count,
                leadership_count = EXCLUDED.leadership_count,
                sector_pe_deal_count = EXCLUDED.sector_pe_deal_count,
                strengths = EXCLUDED.strengths,
                risks = EXCLUDED.risks,
                metadata = EXCLUDED.metadata,
                model_version = EXCLUDED.model_version
        """)
        try:
            sub = result.get("sub_scores", {})
            meta = result.get("metadata", {})
            growth_details = meta.get("growth_signal", {})
            market_details = meta.get("market_attractiveness", {})
            mgmt_details = meta.get("management_gap", {})

            self.db.execute(query, {
                "company_id": result["company_id"],
                "score_date": result["score_date"],
                "overall_score": result["overall_score"],
                "grade": result["grade"],
                "confidence": result["confidence"],
                "growth_signal_score": sub.get("growth_signal"),
                "market_attractiveness_score": sub.get("market_attractiveness"),
                "management_gap_score": sub.get("management_gap"),
                "deal_activity_score": sub.get("deal_activity"),
                "sector_momentum_score": sub.get("sector_momentum"),
                "revenue_growth_pct": growth_details.get("revenue_growth_pct"),
                "employee_count": mgmt_details.get("employee_count"),
                "leadership_count": mgmt_details.get("total_leaders"),
                "sector_pe_deal_count": market_details.get("sector_pe_deal_count"),
                "strengths": json.dumps(result.get("strengths", [])),
                "risks": json.dumps(result.get("risks", [])),
                "metadata": json.dumps(meta),
                "version": MODEL_VERSION,
            })
            self.db.commit()
        except Exception as e:
            logger.warning(f"Error saving acquisition target score: {e}")
            self.db.rollback()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score_company(
        self,
        company_id: int,
        score_date: Optional[date] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Compute acquisition target score for a single PE portfolio company."""
        score_date = score_date or date.today()

        if not force:
            cached = self._get_cached_score(company_id, score_date)
            if cached:
                return cached

        company = self._get_company_info(company_id)
        if not company:
            return {"error": f"Company {company_id} not found", "company_id": company_id}

        # Gather signals
        fin_rows = self._get_financials(company_id)
        lead_rows = self._get_leadership(company_id)
        velocity = self._get_hiring_velocity_via_match(company_id)
        sector_deal_count = self._get_sector_deal_count(company.get("industry"))
        title_data = self._get_deal_activity_titles(company_id)
        fund_count = self._get_sector_fund_count(company.get("sector"))

        # Compute sub-scores
        gs_score, gs_meta, gs_str, gs_risk = self._calc_growth_signal(
            fin_rows, velocity
        )
        ma_score, ma_meta, ma_str, ma_risk = self._calc_market_attractiveness(
            company.get("industry"), sector_deal_count
        )
        mg_score, mg_meta, mg_str, mg_risk = self._calc_management_gap(
            lead_rows, company.get("employee_count")
        )
        da_score, da_meta, da_str, da_risk = self._calc_deal_activity(title_data)
        sm_score, sm_meta, sm_str, sm_risk = self._calc_sector_momentum(
            company.get("sector"), fund_count
        )

        # Weighted composite
        overall = (
            gs_score * WEIGHTS["growth_signal"]
            + ma_score * WEIGHTS["market_attractiveness"]
            + mg_score * WEIGHTS["management_gap"]
            + da_score * WEIGHTS["deal_activity"]
            + sm_score * WEIGHTS["sector_momentum"]
        )
        overall = max(0.0, min(100.0, overall))

        confidence = self._calculate_confidence(
            fin_rows, lead_rows, velocity, sector_deal_count, title_data, fund_count
        )

        all_strengths = gs_str + ma_str + mg_str + da_str + sm_str
        all_risks = gs_risk + ma_risk + mg_risk + da_risk + sm_risk

        grade = self._get_grade(overall)

        result = {
            "company_id": company_id,
            "company_name": company["name"],
            "industry": company.get("industry"),
            "sector": company.get("sector"),
            "score_date": score_date,
            "overall_score": round(overall, 2),
            "grade": grade,
            "confidence": round(confidence, 3),
            "sub_scores": {
                "growth_signal": round(gs_score, 2),
                "market_attractiveness": round(ma_score, 2),
                "management_gap": round(mg_score, 2),
                "deal_activity": round(da_score, 2),
                "sector_momentum": round(sm_score, 2),
            },
            "weights": WEIGHTS,
            "metadata": {
                "growth_signal": gs_meta,
                "market_attractiveness": ma_meta,
                "management_gap": mg_meta,
                "deal_activity": da_meta,
                "sector_momentum": sm_meta,
            },
            "strengths": all_strengths[:8],
            "risks": all_risks[:8],
            "model_version": MODEL_VERSION,
        }

        self._save_score(result)
        return result

    def score_all_companies(self, force: bool = False) -> Dict[str, Any]:
        """Batch-score all PE portfolio companies."""
        query = text("""
            SELECT id FROM pe_portfolio_companies ORDER BY id
        """)
        try:
            rows = self.db.execute(query).fetchall()
        except Exception as e:
            logger.error(f"Error fetching companies for acquisition scoring: {e}")
            return {"error": str(e)}

        scored = 0
        errors = 0
        for row in rows:
            cid = row[0]
            try:
                self.score_company(cid, force=force)
                scored += 1
            except Exception as e:
                logger.warning(f"Acquisition target error for company {cid}: {e}")
                errors += 1

        return {
            "total_companies": len(rows),
            "scored": scored,
            "errors": errors,
        }

    @staticmethod
    def get_methodology() -> Dict[str, Any]:
        """Return scoring methodology documentation."""
        return {
            "model_version": MODEL_VERSION,
            "description": (
                "Acquisition Target Score identifies PE portfolio companies "
                "that are attractive acquisition targets. Combines growth "
                "potential, market dynamics, management buildout opportunity, "
                "deal signals, and sector momentum into a 0-100 score. "
                "Higher scores = more attractive target."
            ),
            "key_insight": (
                "Management Gap scoring is INVERTED from exit readiness: "
                "thin leadership = high acquisition attractiveness because "
                "PE can add value through management buildout."
            ),
            "sub_scores": [
                {
                    "name": "growth_signal",
                    "weight": WEIGHTS["growth_signal"],
                    "description": (
                        "Revenue growth from financials blended with hiring "
                        "velocity (60/40 split when both available)"
                    ),
                    "source": "pe_company_financials + hiring_velocity_scores",
                },
                {
                    "name": "market_attractiveness",
                    "weight": WEIGHTS["market_attractiveness"],
                    "description": (
                        "PE deal count in same industry over last 3 years — "
                        "hot sector = high score"
                    ),
                    "source": "pe_fund_investments + pe_portfolio_companies",
                },
                {
                    "name": "management_gap",
                    "weight": WEIGHTS["management_gap"],
                    "description": (
                        "Thin leadership = PE value-add opportunity. "
                        "Missing CEO/CFO, few C-suite = high score (inverted)"
                    ),
                    "source": "pe_company_leadership",
                },
                {
                    "name": "deal_activity",
                    "weight": WEIGHTS["deal_activity"],
                    "description": (
                        "Job postings signaling deal exploration (Corp Dev, "
                        "M&A) or distress (restructuring, turnaround)"
                    ),
                    "source": "job_postings (via name match)",
                },
                {
                    "name": "sector_momentum",
                    "weight": WEIGHTS["sector_momentum"],
                    "description": (
                        "Count of PE firms with matching sector_focus — "
                        "more funds targeting sector = market consensus"
                    ),
                    "source": "pe_firms",
                },
            ],
            "grade_thresholds": {
                "A": ">=80 — Highly attractive target",
                "B": ">=65 — Strong target",
                "C": ">=50 — Moderate attractiveness",
                "D": ">=35 — Limited attractiveness",
                "F": "<35 — Not an attractive target",
            },
        }
