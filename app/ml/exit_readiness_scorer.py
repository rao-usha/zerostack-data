"""
Exit Readiness Score — core scoring engine.

Combines financial health, financial trajectory, leadership stability,
valuation momentum, market position, hold period, and hiring signals
into a 0-100 exit readiness score for PE portfolio companies.

Extracted from app/api/v1/pe_companies.py:990-1399 with DB persistence
and a new hiring signal based on job postings.
"""

import json
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.ml.exit_readiness_metadata import (
    generate_create_exit_readiness_scores_sql,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
MODEL_VERSION = "v1.0"

WEIGHTS = {
    "financial_health": 0.22,
    "financial_trajectory": 0.18,
    "leadership_stability": 0.15,
    "valuation_momentum": 0.13,
    "market_position": 0.12,
    "hold_period": 0.10,
    "hiring_signal": 0.10,
}

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]

NEUTRAL_SCORE = 50.0

# Keywords in job titles that signal exit preparation
EXIT_INDICATOR_TITLES = [
    "investor relations",
    "corporate development",
    "corp dev",
    "m&a",
    "mergers and acquisitions",
    "ipo",
    "sec reporting",
    "sox compliance",
    "capital markets",
]


class ExitReadinessScorer:
    """Compute exit readiness scores for PE portfolio companies."""

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
                cursor.execute(generate_create_exit_readiness_scores_sql())
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"Exit readiness table creation warning: {e}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_grade(score: float) -> str:
        for threshold, grade in GRADE_THRESHOLDS:
            if score >= threshold:
                return grade
        return "F"

    @staticmethod
    def _get_timing(score: float, grade: str) -> str:
        if grade == "A":
            return "Strong exit candidate - consider initiating process within 6-12 months"
        elif grade == "B":
            return "Near exit-ready - address remaining gaps, target 12-18 month exit"
        elif grade == "C":
            return "Moderate readiness - 18-24 months of value creation work recommended"
        elif grade == "D":
            return "Early stage - significant operational improvements needed before exit"
        return "Not exit-ready - fundamental issues must be resolved"

    # ------------------------------------------------------------------
    # Data retrieval
    # ------------------------------------------------------------------

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

    def _get_valuations(self, company_id: int) -> List:
        query = text("""
            SELECT valuation_date, enterprise_value_usd,
                   ev_revenue_multiple, ev_ebitda_multiple, event_type
            FROM pe_company_valuations
            WHERE company_id = :id
            ORDER BY valuation_date DESC LIMIT 5
        """)
        try:
            return self.db.execute(query, {"id": company_id}).fetchall()
        except Exception:
            self.db.rollback()
            return []

    def _get_competitors(self, company_id: int) -> List:
        query = text("""
            SELECT competitor_type, relative_size, market_position
            FROM pe_competitor_mappings WHERE company_id = :id
        """)
        try:
            return self.db.execute(query, {"id": company_id}).fetchall()
        except Exception:
            self.db.rollback()
            return []

    def _get_investment_info(self, company_id: int) -> Optional[tuple]:
        query = text("""
            SELECT MIN(fi.investment_date), pf.name as firm_name
            FROM pe_fund_investments fi
            JOIN pe_funds f ON fi.fund_id = f.id
            JOIN pe_firms pf ON f.firm_id = pf.id
            WHERE fi.company_id = :id AND fi.status = 'Active'
            GROUP BY pf.name
            ORDER BY MIN(fi.investment_date) ASC LIMIT 1
        """)
        try:
            return self.db.execute(query, {"id": company_id}).fetchone()
        except Exception:
            self.db.rollback()
            return None

    def _get_hiring_signal(self, company_id: int) -> Dict[str, Any]:
        """
        Get hiring signal via name-match to industrial_companies + job_postings.

        1. Match pe_portfolio_companies.name -> industrial_companies.name
        2. Get hiring_velocity_scores.overall_score if available
        3. Scan job_postings.title for exit-indicator keywords
        """
        # Get company name from PE universe
        name_query = text("""
            SELECT name FROM pe_portfolio_companies WHERE id = :id
        """)
        try:
            row = self.db.execute(name_query, {"id": company_id}).fetchone()
            if not row:
                return {"available": False, "note": "company not found"}
            pe_name = row[0]
        except Exception:
            self.db.rollback()
            return {"available": False, "note": "query error"}

        # Match to industrial_companies
        match_query = text("""
            SELECT id FROM industrial_companies
            WHERE LOWER(TRIM(name)) = LOWER(TRIM(:name))
            LIMIT 1
        """)
        ic_id = None
        try:
            match_row = self.db.execute(match_query, {"name": pe_name}).fetchone()
            if match_row:
                ic_id = match_row[0]
        except Exception:
            self.db.rollback()

        # Get hiring velocity if matched
        velocity_score = None
        if ic_id:
            vel_query = text("""
                SELECT overall_score FROM hiring_velocity_scores
                WHERE company_id = :cid
                ORDER BY score_date DESC LIMIT 1
            """)
            try:
                vel_row = self.db.execute(vel_query, {"cid": ic_id}).fetchone()
                if vel_row:
                    velocity_score = float(vel_row[0])
            except Exception:
                self.db.rollback()

        # Scan job postings for exit-indicator titles
        exit_titles_found = []
        search_query = text("""
            SELECT DISTINCT LOWER(title) as title_lower
            FROM job_postings
            WHERE LOWER(company_name) LIKE :pattern
              AND posted_date >= CURRENT_DATE - 180
        """)
        try:
            pattern = f"%{pe_name.lower().split()[0]}%"
            jp_rows = self.db.execute(search_query, {"pattern": pattern}).fetchall()
            for jp_row in jp_rows:
                title = jp_row[0] if jp_row[0] else ""
                for kw in EXIT_INDICATOR_TITLES:
                    if kw in title:
                        exit_titles_found.append(title)
                        break
        except Exception:
            self.db.rollback()

        return {
            "available": velocity_score is not None or len(exit_titles_found) > 0,
            "ic_id": ic_id,
            "velocity_score": velocity_score,
            "exit_indicator_titles": exit_titles_found[:10],
            "exit_title_count": len(exit_titles_found),
        }

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

    # ------------------------------------------------------------------
    # Sub-score calculations
    # ------------------------------------------------------------------

    def _calc_financial_health(
        self, fin_rows: List
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        strengths, risks = [], []

        if not fin_rows:
            return 30.0, {"message": "No financial data available"}, [], \
                ["No financial data available - limits buyer confidence"]

        latest = fin_rows[0]
        revenue = float(latest[1]) if latest[1] else 0
        growth = float(latest[2]) if latest[2] else 0
        ebitda_margin = float(latest[4]) if latest[4] else 0
        fcf = float(latest[6]) if latest[6] else 0
        dte = float(latest[7]) if latest[7] else None

        # Revenue scale score (0-25)
        scale_score = min(25, revenue / 40_000_000)
        # Growth score (0-25)
        growth_score = min(25, max(0, growth * 1.0))
        # Profitability score (0-25)
        profit_score = min(25, max(0, ebitda_margin * 0.8))
        # Cash flow score (0-25)
        fcf_score = 20 if fcf > 0 else 5

        score = min(100, scale_score + growth_score + profit_score + fcf_score)

        details = {
            "latest_revenue_usd": revenue,
            "revenue_growth_pct": growth,
            "ebitda_margin_pct": ebitda_margin,
            "free_cash_flow_positive": fcf > 0,
            "debt_to_ebitda": dte,
        }

        if growth > 15:
            strengths.append(f"Strong revenue growth ({growth:.1f}%)")
        elif growth < 5:
            risks.append(f"Slowing growth ({growth:.1f}%) may reduce buyer interest")
        if ebitda_margin > 20:
            strengths.append(f"Attractive EBITDA margins ({ebitda_margin:.1f}%)")
        elif ebitda_margin < 10:
            risks.append(f"Below-market margins ({ebitda_margin:.1f}%)")
        if fcf > 0:
            strengths.append("Positive free cash flow")
        else:
            risks.append("Negative free cash flow")
        if dte is not None and dte > 5:
            risks.append(f"High leverage ({dte:.1f}x debt/EBITDA)")

        return round(score, 1), details, strengths, risks

    def _calc_financial_trajectory(
        self, fin_rows: List
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        strengths, risks = [], []

        if len(fin_rows) < 3:
            return 40.0, {"message": "Insufficient historical data"}, [], []

        margins = [float(r[4]) for r in fin_rows[:3] if r[4] is not None]
        growths = [float(r[2]) for r in fin_rows[:3] if r[2] is not None]

        trajectory = 50
        if len(margins) >= 2:
            margin_trend = margins[0] - margins[-1]
            if margin_trend > 5:
                trajectory += 25
                strengths.append(
                    f"Margin expansion of {margin_trend:.1f}pp over {len(margins)} years"
                )
            elif margin_trend > 0:
                trajectory += 10
            elif margin_trend < -5:
                trajectory -= 20
                risks.append("Margin compression trend")

        if len(growths) >= 2:
            avg_growth = sum(growths) / len(growths)
            if avg_growth > 15 and all(g > 5 for g in growths):
                trajectory += 25
                strengths.append("Consistent high growth trajectory")
            elif avg_growth > 10:
                trajectory += 15
            elif avg_growth < 0:
                trajectory -= 15
                risks.append("Revenue declining")

        score = min(100, max(0, trajectory))
        details = {
            "margin_trend": margins,
            "growth_trend": growths,
            "years_of_data": len(fin_rows),
        }
        return score, details, strengths, risks

    def _calc_leadership_stability(
        self, lead_rows: List
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        strengths, risks = [], []

        if not lead_rows:
            return 25.0, {"message": "No leadership data available"}, [], \
                ["No leadership data - limits buyer visibility"]

        has_ceo = any(r[2] for r in lead_rows)
        has_cfo = any(r[3] for r in lead_rows)
        c_suite_count = sum(1 for r in lead_rows if r[1] == "C-Suite")
        board_count = sum(1 for r in lead_rows if r[4])
        pe_appointed = sum(1 for r in lead_rows if r[7])

        score = 30
        if has_ceo:
            score += 20
        else:
            risks.append("No CEO identified - leadership gap")
        if has_cfo:
            score += 15
            strengths.append("CFO in place (critical for exit process)")
        else:
            risks.append("No CFO identified - may delay exit process")
        if c_suite_count >= 3:
            score += 15
        if board_count >= 2:
            score += 10
        if pe_appointed >= 2:
            score += 10
            strengths.append(
                f"{pe_appointed} PE-appointed leaders (professionalized management)"
            )

        details = {
            "has_ceo": has_ceo,
            "has_cfo": has_cfo,
            "c_suite_count": c_suite_count,
            "board_members": board_count,
            "pe_appointed_count": pe_appointed,
            "total_current_leaders": len(lead_rows),
        }
        return min(100, score), details, strengths, risks

    def _calc_valuation_momentum(
        self, val_rows: List
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        strengths, risks = [], []

        if not val_rows:
            return 35.0, {"message": "No valuation data available"}, [], []

        if len(val_rows) == 1:
            return 50.0, {
                "latest_ev_usd": float(val_rows[0][1]) if val_rows[0][1] else None,
                "message": "Only one valuation point - trend unknown",
            }, [], []

        latest_ev = float(val_rows[0][1]) if val_rows[0][1] else 0
        earliest_ev = float(val_rows[-1][1]) if val_rows[-1][1] else 0

        val_score = 50
        ev_growth = None
        if earliest_ev > 0 and latest_ev > 0:
            ev_growth = (latest_ev / earliest_ev - 1) * 100
            if ev_growth > 50:
                val_score = 90
                strengths.append(f"Enterprise value up {ev_growth:.0f}% since entry")
            elif ev_growth > 20:
                val_score = 70
                strengths.append(f"Enterprise value up {ev_growth:.0f}%")
            elif ev_growth > 0:
                val_score = 55
            elif ev_growth < -10:
                val_score = 20
                risks.append(f"Enterprise value declined {abs(ev_growth):.0f}%")

        # Multiple expansion bonus
        latest_mult = float(val_rows[0][2]) if val_rows[0][2] else None
        earliest_mult = float(val_rows[-1][2]) if val_rows[-1][2] else None
        if latest_mult and earliest_mult and earliest_mult > 0:
            mult_change = (latest_mult / earliest_mult - 1) * 100
            if mult_change > 0:
                val_score = min(100, val_score + 10)

        details = {
            "latest_ev_usd": latest_ev,
            "entry_ev_usd": earliest_ev,
            "ev_growth_pct": round(ev_growth, 1) if ev_growth is not None else None,
            "latest_ev_revenue": float(val_rows[0][2]) if val_rows[0][2] else None,
            "valuations_count": len(val_rows),
        }
        return min(100, max(0, val_score)), details, strengths, risks

    def _calc_market_position(
        self, comp_rows: List
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        strengths, risks = [], []

        if not comp_rows:
            return 40.0, {"message": "No competitor data available"}, [], []

        market_score = 50
        positions = [r[2] for r in comp_rows if r[2]]
        sizes = [r[1] for r in comp_rows if r[1]]

        leader_count = sum(1 for p in positions if p == "Leader")
        challenger_count = sum(1 for p in positions if p == "Challenger")
        if leader_count > 0:
            market_score += 20

        smaller_count = sum(1 for s in sizes if s == "Smaller")
        if smaller_count > len(sizes) / 2:
            market_score += 20
            strengths.append("Market leader position vs. majority of competitors")
        elif smaller_count > 0:
            market_score += 10

        direct_count = sum(1 for r in comp_rows if r[0] == "Direct")
        if direct_count >= 3:
            market_score += 10

        details = {
            "total_competitors": len(comp_rows),
            "direct_competitors": direct_count,
            "position_distribution": {
                "leader": leader_count,
                "challenger": challenger_count,
                "niche": sum(1 for p in positions if p == "Niche"),
            },
            "relative_size_distribution": {
                "larger": sum(1 for s in sizes if s == "Larger"),
                "similar": sum(1 for s in sizes if s == "Similar"),
                "smaller": smaller_count,
            },
        }
        return min(100, max(0, market_score)), details, strengths, risks

    def _calc_hold_period(
        self, inv_row: Optional[tuple]
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        strengths, risks = [], []

        if not inv_row or not inv_row[0]:
            return 50.0, {"message": "No investment date found"}, [], []

        from datetime import date as date_type
        entry_date = inv_row[0]
        if isinstance(entry_date, str):
            entry_date = date_type.fromisoformat(entry_date)
        hold_years = (date_type.today() - entry_date).days / 365.25

        if 3 <= hold_years <= 6:
            hold_score = 85
            strengths.append(
                f"Hold period ({hold_years:.1f}y) in optimal exit window"
            )
        elif hold_years < 3:
            hold_score = 40
            risks.append(
                f"Short hold period ({hold_years:.1f}y) - may be early for exit"
            )
        elif hold_years <= 8:
            hold_score = 65
        else:
            hold_score = 45
            risks.append(
                f"Extended hold period ({hold_years:.1f}y) - fund may be aging"
            )

        details = {
            "entry_date": entry_date.isoformat(),
            "hold_years": round(hold_years, 1),
            "sponsor": inv_row[1],
        }
        return hold_score, details, strengths, risks

    def _calc_hiring_signal(
        self, hiring_data: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any], List[str], List[str]]:
        strengths, risks = [], []

        if not hiring_data.get("available"):
            return NEUTRAL_SCORE, {
                "note": "No hiring signal data",
                "available": False,
            }, [], []

        score = NEUTRAL_SCORE
        velocity = hiring_data.get("velocity_score")
        exit_count = hiring_data.get("exit_title_count", 0)

        # Hiring velocity component (0-70 range)
        if velocity is not None:
            score = velocity * 0.7  # Scale to 70% of contribution

        # Exit-indicator title bonus (up to +30)
        if exit_count >= 3:
            score = min(100, score + 30)
            strengths.append(
                f"{exit_count} exit-related job postings (IR, Corp Dev, M&A)"
            )
        elif exit_count >= 1:
            score = min(100, score + 15)
            strengths.append("Exit-related hiring activity detected")

        details = {
            "velocity_score": velocity,
            "exit_indicator_titles": hiring_data.get("exit_indicator_titles", []),
            "exit_title_count": exit_count,
            "ic_id": hiring_data.get("ic_id"),
            "available": True,
        }
        return max(0, min(100, score)), details, strengths, risks

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------

    def _calculate_confidence(
        self,
        fin_rows: List,
        lead_rows: List,
        val_rows: List,
        comp_rows: List,
        inv_row: Optional[tuple],
        hiring_data: Dict,
    ) -> float:
        confidence = 0.0
        if fin_rows:
            confidence += 0.25
            if len(fin_rows) >= 3:
                confidence += 0.10  # trajectory bonus
        if lead_rows:
            confidence += 0.15
        if len(val_rows) >= 2:
            confidence += 0.15
        if comp_rows:
            confidence += 0.12
        if inv_row and inv_row[0]:
            confidence += 0.10
        if hiring_data.get("available"):
            confidence += 0.10
        return min(confidence, 1.0)

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def _get_cached_score(
        self, company_id: int, score_date: date
    ) -> Optional[Dict[str, Any]]:
        query = text("""
            SELECT * FROM exit_readiness_scores
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
            INSERT INTO exit_readiness_scores (
                company_id, score_date, overall_score, grade, confidence,
                financial_health_score, financial_trajectory_score,
                leadership_stability_score, valuation_momentum_score,
                market_position_score, hold_period_score, hiring_signal_score,
                latest_revenue_usd, ebitda_margin_pct, hold_years,
                hiring_velocity_raw,
                strengths, risks, metadata, model_version
            ) VALUES (
                :company_id, :score_date, :overall_score, :grade, :confidence,
                :financial_health_score, :financial_trajectory_score,
                :leadership_stability_score, :valuation_momentum_score,
                :market_position_score, :hold_period_score, :hiring_signal_score,
                :latest_revenue_usd, :ebitda_margin_pct, :hold_years,
                :hiring_velocity_raw,
                CAST(:strengths AS jsonb), CAST(:risks AS jsonb),
                CAST(:metadata AS jsonb), :version
            )
            ON CONFLICT (company_id, score_date) DO UPDATE SET
                overall_score = EXCLUDED.overall_score,
                grade = EXCLUDED.grade,
                confidence = EXCLUDED.confidence,
                financial_health_score = EXCLUDED.financial_health_score,
                financial_trajectory_score = EXCLUDED.financial_trajectory_score,
                leadership_stability_score = EXCLUDED.leadership_stability_score,
                valuation_momentum_score = EXCLUDED.valuation_momentum_score,
                market_position_score = EXCLUDED.market_position_score,
                hold_period_score = EXCLUDED.hold_period_score,
                hiring_signal_score = EXCLUDED.hiring_signal_score,
                latest_revenue_usd = EXCLUDED.latest_revenue_usd,
                ebitda_margin_pct = EXCLUDED.ebitda_margin_pct,
                hold_years = EXCLUDED.hold_years,
                hiring_velocity_raw = EXCLUDED.hiring_velocity_raw,
                strengths = EXCLUDED.strengths,
                risks = EXCLUDED.risks,
                metadata = EXCLUDED.metadata,
                model_version = EXCLUDED.model_version
        """)
        try:
            sub = result.get("sub_scores", {})
            meta = result.get("metadata", {})
            fin_details = meta.get("financial_health", {})
            hold_details = meta.get("hold_period", {})
            hiring_details = meta.get("hiring_signal", {})

            self.db.execute(query, {
                "company_id": result["company_id"],
                "score_date": result["score_date"],
                "overall_score": result["overall_score"],
                "grade": result["grade"],
                "confidence": result["confidence"],
                "financial_health_score": sub.get("financial_health"),
                "financial_trajectory_score": sub.get("financial_trajectory"),
                "leadership_stability_score": sub.get("leadership_stability"),
                "valuation_momentum_score": sub.get("valuation_momentum"),
                "market_position_score": sub.get("market_position"),
                "hold_period_score": sub.get("hold_period"),
                "hiring_signal_score": sub.get("hiring_signal"),
                "latest_revenue_usd": fin_details.get("latest_revenue_usd"),
                "ebitda_margin_pct": fin_details.get("ebitda_margin_pct"),
                "hold_years": hold_details.get("hold_years"),
                "hiring_velocity_raw": hiring_details.get("velocity_score"),
                "strengths": json.dumps(result.get("strengths", [])),
                "risks": json.dumps(result.get("risks", [])),
                "metadata": json.dumps(meta),
                "version": MODEL_VERSION,
            })
            self.db.commit()
        except Exception as e:
            logger.warning(f"Error saving exit readiness score: {e}")
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
        """Compute exit readiness score for a single PE portfolio company."""
        score_date = score_date or date.today()

        if not force:
            cached = self._get_cached_score(company_id, score_date)
            if cached:
                return cached

        # Get company info
        company = self._get_company_info(company_id)
        if not company:
            return {"error": f"Company {company_id} not found", "company_id": company_id}

        # Gather signals
        fin_rows = self._get_financials(company_id)
        lead_rows = self._get_leadership(company_id)
        val_rows = self._get_valuations(company_id)
        comp_rows = self._get_competitors(company_id)
        inv_row = self._get_investment_info(company_id)
        hiring_data = self._get_hiring_signal(company_id)

        # Compute sub-scores
        fh_score, fh_meta, fh_str, fh_risk = self._calc_financial_health(fin_rows)
        ft_score, ft_meta, ft_str, ft_risk = self._calc_financial_trajectory(fin_rows)
        ls_score, ls_meta, ls_str, ls_risk = self._calc_leadership_stability(lead_rows)
        vm_score, vm_meta, vm_str, vm_risk = self._calc_valuation_momentum(val_rows)
        mp_score, mp_meta, mp_str, mp_risk = self._calc_market_position(comp_rows)
        hp_score, hp_meta, hp_str, hp_risk = self._calc_hold_period(inv_row)
        hs_score, hs_meta, hs_str, hs_risk = self._calc_hiring_signal(hiring_data)

        # Weighted composite
        overall = (
            fh_score * WEIGHTS["financial_health"]
            + ft_score * WEIGHTS["financial_trajectory"]
            + ls_score * WEIGHTS["leadership_stability"]
            + vm_score * WEIGHTS["valuation_momentum"]
            + mp_score * WEIGHTS["market_position"]
            + hp_score * WEIGHTS["hold_period"]
            + hs_score * WEIGHTS["hiring_signal"]
        )
        overall = max(0.0, min(100.0, overall))

        confidence = self._calculate_confidence(
            fin_rows, lead_rows, val_rows, comp_rows, inv_row, hiring_data
        )

        # Aggregate strengths/risks
        all_strengths = fh_str + ft_str + ls_str + vm_str + mp_str + hp_str + hs_str
        all_risks = fh_risk + ft_risk + ls_risk + vm_risk + mp_risk + hp_risk + hs_risk

        grade = self._get_grade(overall)

        result = {
            "company_id": company_id,
            "company_name": company["name"],
            "pe_owner": company.get("pe_owner"),
            "score_date": score_date,
            "overall_score": round(overall, 2),
            "grade": grade,
            "confidence": round(confidence, 3),
            "timing_recommendation": self._get_timing(overall, grade),
            "sub_scores": {
                "financial_health": round(fh_score, 2),
                "financial_trajectory": round(ft_score, 2),
                "leadership_stability": round(ls_score, 2),
                "valuation_momentum": round(vm_score, 2),
                "market_position": round(mp_score, 2),
                "hold_period": round(hp_score, 2),
                "hiring_signal": round(hs_score, 2),
            },
            "weights": WEIGHTS,
            "metadata": {
                "financial_health": fh_meta,
                "financial_trajectory": ft_meta,
                "leadership_stability": ls_meta,
                "valuation_momentum": vm_meta,
                "market_position": mp_meta,
                "hold_period": hp_meta,
                "hiring_signal": hs_meta,
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
            logger.error(f"Error fetching companies for exit readiness: {e}")
            return {"error": str(e)}

        scored = 0
        errors = 0
        for row in rows:
            cid = row[0]
            try:
                self.score_company(cid, force=force)
                scored += 1
            except Exception as e:
                logger.warning(f"Exit readiness error for company {cid}: {e}")
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
                "Exit Readiness Score evaluates PE portfolio companies across "
                "7 signals to determine optimal exit timing. Each signal is "
                "scored 0-100 and weighted to produce a composite score. "
                "Higher scores indicate greater readiness for exit."
            ),
            "sub_scores": [
                {
                    "name": "financial_health",
                    "weight": WEIGHTS["financial_health"],
                    "description": (
                        "Revenue scale, growth rate, EBITDA margin, "
                        "and free cash flow position"
                    ),
                    "source": "pe_company_financials",
                },
                {
                    "name": "financial_trajectory",
                    "weight": WEIGHTS["financial_trajectory"],
                    "description": (
                        "Margin expansion/compression trend and growth "
                        "consistency over 3+ fiscal years"
                    ),
                    "source": "pe_company_financials (multi-year)",
                },
                {
                    "name": "leadership_stability",
                    "weight": WEIGHTS["leadership_stability"],
                    "description": (
                        "CEO/CFO presence, C-suite depth, board composition, "
                        "and PE-appointed leadership"
                    ),
                    "source": "pe_company_leadership",
                },
                {
                    "name": "valuation_momentum",
                    "weight": WEIGHTS["valuation_momentum"],
                    "description": (
                        "Enterprise value growth and revenue multiple "
                        "expansion since entry"
                    ),
                    "source": "pe_company_valuations",
                },
                {
                    "name": "market_position",
                    "weight": WEIGHTS["market_position"],
                    "description": (
                        "Competitive dynamics: leader/challenger status, "
                        "relative size vs. peers"
                    ),
                    "source": "pe_competitor_mappings",
                },
                {
                    "name": "hold_period",
                    "weight": WEIGHTS["hold_period"],
                    "description": (
                        "Time since PE entry vs. optimal 3-6 year exit window"
                    ),
                    "source": "pe_fund_investments",
                },
                {
                    "name": "hiring_signal",
                    "weight": WEIGHTS["hiring_signal"],
                    "description": (
                        "Hiring velocity + exit-indicator job titles "
                        "(IR, Corp Dev, M&A, IPO, SEC reporting)"
                    ),
                    "source": "job_postings + hiring_velocity_scores (via name match)",
                },
            ],
            "grade_thresholds": {
                "A": ">=80 — Strong exit candidate (6-12 months)",
                "B": ">=65 — Near exit-ready (12-18 months)",
                "C": ">=50 — Moderate readiness (18-24 months)",
                "D": ">=35 — Early stage (significant work needed)",
                "F": "<35 — Not exit-ready",
            },
            "missing_signals": (
                "Missing signals receive default scores (25-50 depending on "
                "signal importance) and reduce confidence proportionally."
            ),
        }
