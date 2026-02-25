"""
Med-Spa Market Analysis Report Template.

Generates an investor-ready aesthetics roll-up thesis report synthesizing
medspa_prospects (Yelp-sourced acquisition targets), zip_medspa_scores
(ZIP affluence model), pe_portfolio_companies (PE platform comps), and
Census ACS income data into a comprehensive market analysis.

Sections:
  1. Executive Summary
  2. Market Map (Chart.js visualizations)
  3. Top Acquisition Targets
  4. Market Concentration Analysis
  5. ZIP Affluence Profile
  6. Competitive Landscape
  7. Data Sources & Methodology
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from io import BytesIO

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.reports.design_system import (
    html_document, page_header, kpi_strip, kpi_card,
    toc, section_start, section_end,
    data_table, pill_badge, callout,
    chart_container, chart_init_js, page_footer,
    build_doughnut_config, build_horizontal_bar_config,
    build_bar_fallback, build_chart_legend, CHART_COLORS,
    BLUE, BLUE_LIGHT, BLUE_DARK, ORANGE, GREEN, RED, GRAY,
    PURPLE, TEAL, PINK,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Grade color mapping for badges
# ---------------------------------------------------------------------------

GRADE_BADGE_COLORS = {
    "A": "public",    # blue
    "B": "private",   # green
    "C": "pe",        # amber
    "D": "sub",       # gray
    "F": "default",   # gray
}

# Prospect scoring weights (mirrored from metadata for methodology section)
PROSPECT_WEIGHTS = {
    "zip_affluence": 0.30,
    "yelp_rating": 0.25,
    "review_volume": 0.20,
    "low_competition": 0.15,
    "price_tier": 0.10,
}

ZIP_SCORE_WEIGHTS = {
    "affluence_density": 0.30,
    "discretionary_wealth": 0.25,
    "market_size": 0.20,
    "professional_density": 0.15,
    "wealth_concentration": 0.10,
}

# Extra CSS for med-spa report specifics
MEDSPA_EXTRA_CSS = """
/* Med-Spa Report Extras */
.thesis-box {
    background: linear-gradient(135deg, #ebf8ff 0%, #f0fff4 100%);
    border: 1px solid #bee3f8;
    border-radius: 10px;
    padding: 24px;
    margin: 16px 0;
}
[data-theme="dark"] .thesis-box {
    background: linear-gradient(135deg, #1a365d 0%, #22543d 100%);
    border-color: #2a4365;
}
.thesis-box h3 {
    font-size: 15px;
    font-weight: 700;
    color: var(--primary);
    margin-bottom: 10px;
}
.thesis-box ul {
    padding-left: 20px;
    margin: 0;
}
.thesis-box li {
    font-size: 13px;
    color: var(--gray-700);
    margin-bottom: 6px;
    line-height: 1.5;
}
.thesis-box li strong { color: var(--gray-900); }

.score-bar {
    display: inline-block;
    height: 8px;
    border-radius: 4px;
    background: var(--primary-light);
    vertical-align: middle;
    margin-left: 6px;
}

.metric-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
    gap: 16px;
    margin: 16px 0;
}
.metric-card {
    background: var(--white);
    border-radius: 10px;
    padding: 20px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}
.metric-card .metric-label {
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--gray-500);
    font-weight: 600;
}
.metric-card .metric-value {
    font-size: 24px;
    font-weight: 700;
    color: var(--primary);
    margin: 4px 0;
}
.metric-card .metric-detail {
    font-size: 12px;
    color: var(--gray-500);
}

.weight-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 0;
    border-bottom: 1px solid var(--gray-100);
    font-size: 13px;
}
.weight-row:last-child { border-bottom: none; }
.weight-label { flex: 1; color: var(--gray-700); font-weight: 500; }
.weight-bar-track {
    flex: 2;
    height: 8px;
    background: var(--gray-100);
    border-radius: 4px;
    overflow: hidden;
}
.weight-bar-fill {
    height: 100%;
    border-radius: 4px;
    background: var(--primary-light);
}
.weight-pct {
    min-width: 40px;
    text-align: right;
    font-weight: 600;
    color: var(--gray-900);
}
"""


def _fmt(n, decimals=0) -> str:
    """Format a number with commas."""
    if n is None:
        return "-"
    if decimals == 0:
        return f"{int(n):,}"
    return f"{n:,.{decimals}f}"


def _fmt_currency(n) -> str:
    """Format as dollar amount."""
    if n is None:
        return "-"
    if n >= 1_000_000:
        return f"${n / 1_000_000:,.1f}M"
    if n >= 1_000:
        return f"${n / 1_000:,.0f}K"
    return f"${n:,.0f}"


def _grade_badge(grade: str) -> str:
    """Return a pill badge colored by grade."""
    variant = GRADE_BADGE_COLORS.get(grade, "default")
    return pill_badge(f"Grade {grade}", variant)


class MedSpaMarketTemplate:
    """Med-spa market analysis report template."""

    name = "medspa_market"
    description = "Investor-ready med-spa market analysis with acquisition targets and roll-up thesis"

    # ------------------------------------------------------------------
    # Data Gathering
    # ------------------------------------------------------------------

    def gather_data(self, db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
        """Gather all data needed for the report."""
        # Optional filters
        state_filter = params.get("state")  # e.g. "TX" to scope to one state
        min_grade = params.get("min_grade", "F")  # minimum grade to include
        top_n = params.get("top_n", 25)  # number of top targets to display

        data = {
            "generated_at": datetime.utcnow().isoformat(),
            "params": params,
            "summary": self._get_summary_stats(db, state_filter),
            "prospects_by_state": self._get_prospects_by_state(db, state_filter),
            "grade_distribution": self._get_grade_distribution(db, state_filter),
            "score_histogram": self._get_score_histogram(db, state_filter),
            "top_targets": self._get_top_targets(db, top_n, state_filter),
            "zip_concentration": self._get_zip_concentration(db, state_filter),
            "state_avg_scores": self._get_state_avg_scores(db, state_filter),
            "a_grade_by_state": self._get_a_grade_by_state(db, state_filter),
            "zip_affluence_by_state": self._get_zip_affluence_by_state(db, state_filter),
            "census_income": self._get_census_income_correlation(db, state_filter),
            "high_income_zip_penetration": self._get_high_income_zip_penetration(db, state_filter),
            "pe_comps": self._get_pe_aesthetics_comps(db),
            "recent_deals": self._get_recent_deals(db),
            "data_freshness": self._get_data_freshness(db),
        }

        return data

    def _get_summary_stats(self, db: Session, state: Optional[str]) -> Dict:
        """Get total addressable market overview stats."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        result = db.execute(
            text(f"""
                SELECT
                    COUNT(*) as total_prospects,
                    COUNT(*) FILTER (WHERE acquisition_grade = 'A') as a_grade,
                    COUNT(*) FILTER (WHERE acquisition_grade = 'B') as b_grade,
                    COUNT(*) FILTER (WHERE acquisition_grade IN ('A', 'B')) as ab_grade,
                    COUNT(DISTINCT state) as states_covered,
                    COUNT(DISTINCT zip_code) as zips_covered,
                    ROUND(AVG(acquisition_score), 1) as avg_score,
                    ROUND(MAX(acquisition_score), 1) as max_score,
                    ROUND(AVG(rating), 2) as avg_rating,
                    ROUND(AVG(review_count), 0) as avg_reviews,
                    ROUND(AVG(zip_overall_score), 1) as avg_zip_score,
                    SUM(review_count) as total_reviews
                FROM medspa_prospects
                WHERE 1=1 {state_clause}
            """),
            params,
        )
        row = result.fetchone()
        if not row or row[0] == 0:
            return {
                "total_prospects": 0, "a_grade": 0, "b_grade": 0,
                "ab_grade": 0, "states_covered": 0, "zips_covered": 0,
                "avg_score": 0, "max_score": 0, "avg_rating": 0,
                "avg_reviews": 0, "avg_zip_score": 0, "total_reviews": 0,
            }

        return {
            "total_prospects": row[0],
            "a_grade": row[1],
            "b_grade": row[2],
            "ab_grade": row[3],
            "states_covered": row[4],
            "zips_covered": row[5],
            "avg_score": float(row[6]) if row[6] else 0,
            "max_score": float(row[7]) if row[7] else 0,
            "avg_rating": float(row[8]) if row[8] else 0,
            "avg_reviews": int(row[9]) if row[9] else 0,
            "avg_zip_score": float(row[10]) if row[10] else 0,
            "total_reviews": int(row[11]) if row[11] else 0,
        }

    def _get_prospects_by_state(self, db: Session, state: Optional[str]) -> List[Dict]:
        """Get prospect count by state (top 20)."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        result = db.execute(
            text(f"""
                SELECT state, COUNT(*) as cnt
                FROM medspa_prospects
                WHERE state IS NOT NULL {state_clause}
                GROUP BY state
                ORDER BY cnt DESC
                LIMIT 20
            """),
            params,
        )
        return [{"state": row[0], "count": row[1]} for row in result.fetchall()]

    def _get_grade_distribution(self, db: Session, state: Optional[str]) -> List[Dict]:
        """Get grade distribution."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        result = db.execute(
            text(f"""
                SELECT acquisition_grade, COUNT(*) as cnt
                FROM medspa_prospects
                WHERE 1=1 {state_clause}
                GROUP BY acquisition_grade
                ORDER BY
                    CASE acquisition_grade
                        WHEN 'A' THEN 1
                        WHEN 'B' THEN 2
                        WHEN 'C' THEN 3
                        WHEN 'D' THEN 4
                        WHEN 'F' THEN 5
                    END
            """),
            params,
        )
        rows = result.fetchall()
        total = sum(r[1] for r in rows)
        return [
            {
                "grade": row[0],
                "count": row[1],
                "pct": round(row[1] / total * 100, 1) if total > 0 else 0,
            }
            for row in rows
        ]

    def _get_score_histogram(self, db: Session, state: Optional[str]) -> List[Dict]:
        """Get score distribution in 10-point buckets."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        result = db.execute(
            text(f"""
                SELECT
                    FLOOR(acquisition_score / 10) * 10 as bucket,
                    COUNT(*) as cnt
                FROM medspa_prospects
                WHERE 1=1 {state_clause}
                GROUP BY bucket
                ORDER BY bucket
            """),
            params,
        )
        return [
            {"bucket": f"{int(row[0])}-{int(row[0]) + 9}", "count": row[1]}
            for row in result.fetchall()
        ]

    def _get_top_targets(
        self, db: Session, top_n: int, state: Optional[str]
    ) -> List[Dict]:
        """Get top acquisition targets."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {"limit": top_n}
        if state:
            params["state"] = state.upper()

        result = db.execute(
            text(f"""
                SELECT
                    name, city, state, zip_code,
                    acquisition_score, acquisition_grade,
                    rating, review_count, price,
                    zip_overall_score, zip_grade,
                    zip_affluence_sub, yelp_rating_sub,
                    review_volume_sub, low_competition_sub,
                    price_tier_sub, competitor_count_in_zip,
                    phone, url
                FROM medspa_prospects
                WHERE acquisition_grade IN ('A', 'B') {state_clause}
                ORDER BY acquisition_score DESC
                LIMIT :limit
            """),
            params,
        )
        return [
            {
                "name": row[0],
                "city": row[1],
                "state": row[2],
                "zip_code": row[3],
                "score": float(row[4]) if row[4] else 0,
                "grade": row[5],
                "rating": float(row[6]) if row[6] else 0,
                "reviews": int(row[7]) if row[7] else 0,
                "price": row[8],
                "zip_score": float(row[9]) if row[9] else 0,
                "zip_grade": row[10],
                "zip_affluence_sub": float(row[11]) if row[11] else 0,
                "yelp_rating_sub": float(row[12]) if row[12] else 0,
                "review_volume_sub": float(row[13]) if row[13] else 0,
                "low_competition_sub": float(row[14]) if row[14] else 0,
                "price_tier_sub": float(row[15]) if row[15] else 0,
                "competitors": int(row[16]) if row[16] else 0,
                "phone": row[17],
                "url": row[18],
            }
            for row in result.fetchall()
        ]

    def _get_zip_concentration(self, db: Session, state: Optional[str]) -> List[Dict]:
        """Get top ZIPs by prospect count (competition density)."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        result = db.execute(
            text(f"""
                SELECT
                    zip_code, state, city,
                    COUNT(*) as prospect_count,
                    ROUND(AVG(acquisition_score), 1) as avg_score,
                    ROUND(AVG(zip_overall_score), 1) as avg_zip_score
                FROM medspa_prospects
                WHERE zip_code IS NOT NULL {state_clause}
                GROUP BY zip_code, state, city
                ORDER BY prospect_count DESC
                LIMIT 20
            """),
            params,
        )
        return [
            {
                "zip_code": row[0],
                "state": row[1],
                "city": row[2],
                "prospect_count": row[3],
                "avg_score": float(row[4]) if row[4] else 0,
                "avg_zip_score": float(row[5]) if row[5] else 0,
            }
            for row in result.fetchall()
        ]

    def _get_state_avg_scores(self, db: Session, state: Optional[str]) -> List[Dict]:
        """Get states ranked by avg acquisition score."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        result = db.execute(
            text(f"""
                SELECT
                    state,
                    COUNT(*) as prospect_count,
                    ROUND(AVG(acquisition_score), 1) as avg_score,
                    ROUND(MAX(acquisition_score), 1) as max_score,
                    COUNT(*) FILTER (WHERE acquisition_grade = 'A') as a_count
                FROM medspa_prospects
                WHERE state IS NOT NULL {state_clause}
                GROUP BY state
                ORDER BY avg_score DESC
                LIMIT 15
            """),
            params,
        )
        return [
            {
                "state": row[0],
                "prospect_count": row[1],
                "avg_score": float(row[2]) if row[2] else 0,
                "max_score": float(row[3]) if row[3] else 0,
                "a_count": row[4],
            }
            for row in result.fetchall()
        ]

    def _get_a_grade_by_state(self, db: Session, state: Optional[str]) -> List[Dict]:
        """Get A-grade concentration by state."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        result = db.execute(
            text(f"""
                SELECT
                    state,
                    COUNT(*) FILTER (WHERE acquisition_grade = 'A') as a_count,
                    COUNT(*) as total,
                    ROUND(
                        COUNT(*) FILTER (WHERE acquisition_grade = 'A')::numeric
                        / NULLIF(COUNT(*), 0) * 100, 1
                    ) as a_pct
                FROM medspa_prospects
                WHERE state IS NOT NULL {state_clause}
                GROUP BY state
                HAVING COUNT(*) FILTER (WHERE acquisition_grade = 'A') > 0
                ORDER BY a_count DESC
                LIMIT 15
            """),
            params,
        )
        return [
            {
                "state": row[0],
                "a_count": row[1],
                "total": row[2],
                "a_pct": float(row[3]) if row[3] else 0,
            }
            for row in result.fetchall()
        ]

    def _get_zip_affluence_by_state(self, db: Session, state: Optional[str]) -> List[Dict]:
        """Get average ZIP affluence score by state from zip_medspa_scores."""
        state_clause = "AND state_abbr = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        try:
            result = db.execute(
                text(f"""
                    SELECT
                        state_abbr,
                        COUNT(*) as zip_count,
                        ROUND(AVG(overall_score), 1) as avg_score,
                        ROUND(AVG(affluence_density_score), 1) as avg_affluence,
                        ROUND(AVG(avg_agi), 0) as avg_agi,
                        COUNT(*) FILTER (WHERE grade = 'A') as a_zips
                    FROM zip_medspa_scores
                    WHERE state_abbr IS NOT NULL {state_clause}
                    GROUP BY state_abbr
                    ORDER BY avg_score DESC
                    LIMIT 15
                """),
                params,
            )
            return [
                {
                    "state": row[0],
                    "zip_count": row[1],
                    "avg_score": float(row[2]) if row[2] else 0,
                    "avg_affluence": float(row[3]) if row[3] else 0,
                    "avg_agi": float(row[4]) if row[4] else 0,
                    "a_zips": row[5],
                }
                for row in result.fetchall()
            ]
        except Exception as e:
            logger.warning(f"Could not fetch ZIP affluence data: {e}")
            db.rollback()
            return []

    def _get_census_income_correlation(self, db: Session, state: Optional[str]) -> List[Dict]:
        """Get median household income from Census ACS by state, correlated with ZIP scores."""
        try:
            # Try to pull ACS median income data
            result = db.execute(
                text("""
                    SELECT
                        z.state_abbr,
                        ROUND(AVG(z.avg_agi), 0) as avg_zip_agi,
                        ROUND(AVG(z.overall_score), 1) as avg_zip_score,
                        COUNT(DISTINCT z.zip_code) as zip_count
                    FROM zip_medspa_scores z
                    WHERE z.state_abbr IS NOT NULL
                    GROUP BY z.state_abbr
                    HAVING COUNT(*) >= 10
                    ORDER BY avg_zip_score DESC
                    LIMIT 15
                """),
            )
            return [
                {
                    "state": row[0],
                    "avg_agi": float(row[1]) if row[1] else 0,
                    "avg_zip_score": float(row[2]) if row[2] else 0,
                    "zip_count": row[3],
                }
                for row in result.fetchall()
            ]
        except Exception as e:
            logger.warning(f"Could not fetch census income data: {e}")
            db.rollback()
            return []

    def _get_high_income_zip_penetration(self, db: Session, state: Optional[str]) -> Dict:
        """Get penetration of prospects into high-income ZIPs."""
        try:
            result = db.execute(
                text("""
                    SELECT
                        COUNT(DISTINCT z.zip_code) as total_a_zips,
                        COUNT(DISTINCT mp.zip_code) as zips_with_prospects,
                        ROUND(
                            COUNT(DISTINCT mp.zip_code)::numeric
                            / NULLIF(COUNT(DISTINCT z.zip_code), 0) * 100, 1
                        ) as penetration_pct
                    FROM zip_medspa_scores z
                    LEFT JOIN medspa_prospects mp ON z.zip_code = mp.zip_code
                    WHERE z.grade = 'A'
                """),
            )
            row = result.fetchone()
            if row:
                return {
                    "total_a_zips": int(row[0]) if row[0] else 0,
                    "zips_with_prospects": int(row[1]) if row[1] else 0,
                    "penetration_pct": float(row[2]) if row[2] else 0,
                }
        except Exception as e:
            logger.warning(f"Could not fetch high-income penetration: {e}")
            db.rollback()
        return {"total_a_zips": 0, "zips_with_prospects": 0, "penetration_pct": 0}

    def _get_pe_aesthetics_comps(self, db: Session) -> List[Dict]:
        """Get PE-backed aesthetics/med-spa companies from pe_portfolio_companies."""
        try:
            result = db.execute(
                text("""
                    SELECT
                        pc.name, pc.industry, pc.sub_industry,
                        pc.headquarters_city, pc.headquarters_state,
                        pc.current_pe_owner, pc.ownership_status,
                        pc.employee_count, pc.founded_year,
                        pc.website, pc.status
                    FROM pe_portfolio_companies pc
                    WHERE (
                        LOWER(pc.industry) LIKE '%aesthet%'
                        OR LOWER(pc.industry) LIKE '%med%spa%'
                        OR LOWER(pc.industry) LIKE '%dermatol%'
                        OR LOWER(pc.industry) LIKE '%cosmetic%'
                        OR LOWER(pc.sub_industry) LIKE '%aesthet%'
                        OR LOWER(pc.sub_industry) LIKE '%med%spa%'
                        OR LOWER(pc.name) LIKE '%medspa%'
                        OR LOWER(pc.name) LIKE '%med spa%'
                        OR LOWER(pc.name) LIKE '%aesthet%'
                        OR LOWER(pc.sector) LIKE '%aesthet%'
                        OR LOWER(pc.sector) LIKE '%healthcare%services%'
                    )
                    ORDER BY pc.name
                """),
            )
            return [
                {
                    "name": row[0],
                    "industry": row[1],
                    "sub_industry": row[2],
                    "city": row[3],
                    "state": row[4],
                    "pe_owner": row[5],
                    "ownership_status": row[6],
                    "employees": row[7],
                    "founded": row[8],
                    "website": row[9],
                    "status": row[10],
                }
                for row in result.fetchall()
            ]
        except Exception as e:
            logger.warning(f"Could not fetch PE aesthetics comps: {e}")
            db.rollback()
            return []

    def _get_recent_deals(self, db: Session) -> List[Dict]:
        """Get recent aesthetics/med-spa deals."""
        try:
            result = db.execute(
                text("""
                    SELECT
                        d.deal_name, d.deal_type, d.deal_sub_type,
                        d.announced_date, d.closed_date,
                        d.enterprise_value_usd, d.ev_ebitda_multiple,
                        d.ev_revenue_multiple,
                        pc.name as company_name, pc.industry
                    FROM pe_deals d
                    JOIN pe_portfolio_companies pc ON d.company_id = pc.id
                    WHERE (
                        LOWER(pc.industry) LIKE '%aesthet%'
                        OR LOWER(pc.industry) LIKE '%med%spa%'
                        OR LOWER(pc.industry) LIKE '%dermatol%'
                        OR LOWER(pc.industry) LIKE '%cosmetic%'
                        OR LOWER(pc.sub_industry) LIKE '%aesthet%'
                        OR LOWER(pc.name) LIKE '%medspa%'
                        OR LOWER(pc.name) LIKE '%med spa%'
                    )
                    ORDER BY d.announced_date DESC NULLS LAST
                    LIMIT 10
                """),
            )
            return [
                {
                    "deal_name": row[0],
                    "deal_type": row[1],
                    "deal_sub_type": row[2],
                    "announced_date": row[3].isoformat() if row[3] else None,
                    "closed_date": row[4].isoformat() if row[4] else None,
                    "ev": float(row[5]) if row[5] else None,
                    "ev_ebitda": float(row[6]) if row[6] else None,
                    "ev_revenue": float(row[7]) if row[7] else None,
                    "company": row[8],
                    "industry": row[9],
                }
                for row in result.fetchall()
            ]
        except Exception as e:
            logger.warning(f"Could not fetch recent deals: {e}")
            db.rollback()
            return []

    def _get_data_freshness(self, db: Session) -> Dict:
        """Get data freshness timestamps."""
        freshness = {}

        # Prospects freshness
        try:
            result = db.execute(
                text("""
                    SELECT
                        MIN(discovered_at) as earliest,
                        MAX(discovered_at) as latest,
                        COUNT(*) as total
                    FROM medspa_prospects
                """),
            )
            row = result.fetchone()
            if row and row[2]:
                freshness["prospects"] = {
                    "earliest": row[0].isoformat() if row[0] else None,
                    "latest": row[1].isoformat() if row[1] else None,
                    "total": row[2],
                }
        except Exception:
            db.rollback()

        # ZIP scores freshness
        try:
            result = db.execute(
                text("""
                    SELECT
                        MIN(score_date) as earliest,
                        MAX(score_date) as latest,
                        COUNT(*) as total
                    FROM zip_medspa_scores
                """),
            )
            row = result.fetchone()
            if row and row[2]:
                freshness["zip_scores"] = {
                    "earliest": row[0].isoformat() if row[0] else None,
                    "latest": row[1].isoformat() if row[1] else None,
                    "total": row[2],
                }
        except Exception:
            db.rollback()

        return freshness

    # ------------------------------------------------------------------
    # HTML Rendering
    # ------------------------------------------------------------------

    def render_html(self, data: Dict[str, Any]) -> str:
        """Render report as HTML using the shared design system."""
        summary = data.get("summary", {})
        prospects_by_state = data.get("prospects_by_state", [])
        grade_dist = data.get("grade_distribution", [])
        score_hist = data.get("score_histogram", [])
        top_targets = data.get("top_targets", [])
        zip_conc = data.get("zip_concentration", [])
        state_avgs = data.get("state_avg_scores", [])
        a_grade_state = data.get("a_grade_by_state", [])
        zip_affluence = data.get("zip_affluence_by_state", [])
        census_income = data.get("census_income", [])
        hi_zip_pen = data.get("high_income_zip_penetration", {})
        pe_comps = data.get("pe_comps", [])
        recent_deals = data.get("recent_deals", [])
        data_fresh = data.get("data_freshness", {})
        params = data.get("params", {})

        charts_js = ""
        body = ""

        # ---- Determine scope for header ----
        state_filter = params.get("state")
        scope_label = f" ({state_filter})" if state_filter else " (National)"

        # ---- Page Header ----
        body += page_header(
            title=f"Aesthetics Roll-Up Thesis{scope_label}",
            subtitle="Med-Spa Market Analysis \u00b7 Acquisition Target Intelligence",
            badge=f"{_fmt(summary.get('total_prospects'))} Prospects Discovered",
        )

        # ---- KPI Strip ----
        cards = ""
        cards += kpi_card("Total Prospects", _fmt(summary.get("total_prospects")))
        cards += kpi_card("A-Grade Targets", _fmt(summary.get("a_grade")))
        cards += kpi_card("States Covered", _fmt(summary.get("states_covered")))
        cards += kpi_card("Avg Acq. Score", str(summary.get("avg_score", 0)))
        cards += kpi_card("Avg Yelp Rating", f"{summary.get('avg_rating', 0):.1f}")

        body += '\n<div class="container">'
        body += "\n" + kpi_strip(cards)

        # ---- Table of Contents ----
        toc_items = [
            {"number": 1, "id": "exec-summary", "title": "Executive Summary"},
            {"number": 2, "id": "market-map", "title": "Market Map"},
            {"number": 3, "id": "top-targets", "title": "Top Acquisition Targets"},
            {"number": 4, "id": "concentration", "title": "Market Concentration Analysis"},
            {"number": 5, "id": "zip-affluence", "title": "ZIP Affluence Profile"},
            {"number": 6, "id": "competitive", "title": "Competitive Landscape"},
            {"number": 7, "id": "methodology", "title": "Data Sources & Methodology"},
        ]
        body += "\n" + toc(toc_items)

        # ==================================================================
        # Section 1: Executive Summary
        # ==================================================================
        body += "\n" + section_start(1, "Executive Summary", "exec-summary")

        total_p = summary.get("total_prospects", 0)
        a_count = summary.get("a_grade", 0)
        ab_count = summary.get("ab_grade", 0)
        states_n = summary.get("states_covered", 0)

        body += f"""<p>Nexdata has identified <strong>{_fmt(total_p)}</strong> med-spa acquisition prospects
across <strong>{states_n}</strong> states, scored using a 5-factor weighted composite model combining
ZIP-level affluence data (IRS SOI), Yelp consumer signals, and competitive density analysis.</p>"""

        # Investment thesis box
        body += '<div class="thesis-box">'
        body += "<h3>Investment Thesis: Aesthetics Roll-Up</h3>"
        body += "<ul>"
        body += f"<li><strong>{_fmt(a_count)} A-grade</strong> and <strong>{_fmt(ab_count)} A/B-grade</strong> prospects identified as high-conviction acquisition targets</li>"

        # Top 5 states
        top5_states = prospects_by_state[:5]
        if top5_states:
            states_str = ", ".join(
                f"{s['state']} ({s['count']})" for s in top5_states
            )
            body += f"<li><strong>Top target markets:</strong> {states_str}</li>"

        body += f"<li><strong>Average acquisition score:</strong> {summary.get('avg_score', 0)}/100 across all prospects</li>"
        body += f"<li><strong>Average Yelp rating:</strong> {summary.get('avg_rating', 0):.1f} stars with {_fmt(summary.get('avg_reviews'))} avg reviews per location</li>"

        if hi_zip_pen.get("penetration_pct"):
            body += f"<li><strong>Market coverage:</strong> {hi_zip_pen['penetration_pct']:.0f}% of A-grade affluent ZIPs already have discoverable prospects</li>"

        body += "</ul></div>"

        # Grade distribution summary as metric cards
        body += '<div class="metric-grid">'
        for gd in grade_dist:
            body += f"""<div class="metric-card">
    <div class="metric-label">Grade {gd['grade']} Prospects</div>
    <div class="metric-value">{_fmt(gd['count'])}</div>
    <div class="metric-detail">{gd['pct']}% of total pipeline</div>
</div>"""
        body += "</div>"

        body += "\n" + section_end()

        # ==================================================================
        # Section 2: Market Map (Charts)
        # ==================================================================
        body += "\n" + section_start(2, "Market Map", "market-map")
        body += '<p>Visual distribution of prospects by geography, grade, and score.</p>'

        # Chart 2a: Prospects by state (horizontal bar)
        if prospects_by_state:
            state_labels = [s["state"] for s in prospects_by_state[:15]]
            state_values = [float(s["count"]) for s in prospects_by_state[:15]]
            state_bar_config = build_horizontal_bar_config(
                state_labels, state_values, dataset_label="Prospects"
            )
            state_bar_json = json.dumps(state_bar_config)
            bar_height = f"{max(len(state_labels) * 48 + 40, 200)}px"

            body += chart_container(
                "stateBarChart", state_bar_json,
                build_bar_fallback(state_labels, state_values),
                title="Prospects by State (Top 15)",
                height=bar_height,
            )
            charts_js += chart_init_js("stateBarChart", state_bar_json)

        # Chart row: Grade donut + Score histogram
        body += '<div class="chart-row">'

        # Chart 2b: Grade distribution (donut)
        if grade_dist:
            grade_labels = [f"Grade {g['grade']}" for g in grade_dist]
            grade_values = [float(g["count"]) for g in grade_dist]
            grade_colors_map = {
                "A": BLUE, "B": GREEN, "C": ORANGE, "D": GRAY, "F": RED,
            }
            grade_colors = [
                grade_colors_map.get(g["grade"], GRAY) for g in grade_dist
            ]
            donut_config = build_doughnut_config(grade_labels, grade_values, grade_colors)
            donut_json = json.dumps(donut_config)

            body += "<div>"
            body += chart_container(
                "gradeDonut", donut_json,
                build_bar_fallback(grade_labels, grade_values),
                size="medium",
                title="Grade Distribution",
            )
            charts_js += chart_init_js("gradeDonut", donut_json)
            body += build_chart_legend(
                grade_labels, grade_values, grade_colors, show_pct=True
            )
            body += "</div>"

        # Chart 2c: Score histogram (vertical bar)
        if score_hist:
            hist_labels = [h["bucket"] for h in score_hist]
            hist_values = [float(h["count"]) for h in score_hist]
            hist_config = {
                "type": "bar",
                "data": {
                    "labels": hist_labels,
                    "datasets": [{
                        "label": "Prospects",
                        "data": hist_values,
                        "backgroundColor": BLUE_LIGHT,
                        "borderWidth": 0,
                        "borderRadius": 4,
                    }],
                },
                "options": {
                    "responsive": True,
                    "maintainAspectRatio": False,
                    "plugins": {"legend": {"display": False}},
                    "scales": {
                        "x": {
                            "grid": {"display": False},
                            "ticks": {"color": "#4a5568"},
                        },
                        "y": {
                            "grid": {"color": "#edf2f7"},
                            "ticks": {"color": "#4a5568"},
                            "beginAtZero": True,
                        },
                    },
                },
            }
            hist_json = json.dumps(hist_config)

            body += "<div>"
            body += chart_container(
                "scoreHistogram", hist_json,
                build_bar_fallback(hist_labels, hist_values),
                size="medium",
                title="Score Distribution (10-pt Buckets)",
            )
            charts_js += chart_init_js("scoreHistogram", hist_json)
            body += "</div>"

        body += "</div>"  # close chart-row
        body += "\n" + section_end()

        # ==================================================================
        # Section 3: Top Acquisition Targets
        # ==================================================================
        body += "\n" + section_start(3, "Top Acquisition Targets", "top-targets")
        body += f'<p><strong>{len(top_targets)}</strong> highest-scoring A/B-grade prospects ranked by composite acquisition score.</p>'

        if top_targets:
            table_rows = []
            for i, t in enumerate(top_targets, 1):
                grade_html = _grade_badge(t["grade"])
                zip_grade_html = _grade_badge(t["zip_grade"]) if t.get("zip_grade") else "-"
                score_bar_width = min(t["score"], 100)
                score_html = (
                    f'{t["score"]:.0f}'
                    f'<span class="score-bar" style="width:{score_bar_width}px"></span>'
                )
                price_display = t.get("price") or "-"

                table_rows.append([
                    str(i),
                    f'<span class="company-name">{t["name"]}</span>',
                    t.get("city") or "-",
                    t.get("state") or "-",
                    score_html,
                    grade_html,
                    f'{t["rating"]:.1f}',
                    _fmt(t["reviews"]),
                    zip_grade_html,
                    price_display,
                ])

            body += data_table(
                headers=[
                    "#", "Name", "City", "State", "Score",
                    "Grade", "Rating", "Reviews", "ZIP Grade", "Price",
                ],
                rows=table_rows,
                numeric_columns={0, 4, 6, 7},
            )

            # Insight callout
            if top_targets:
                top_state_counts: Dict[str, int] = {}
                for t in top_targets:
                    st = t.get("state", "??")
                    top_state_counts[st] = top_state_counts.get(st, 0) + 1
                top_state = max(top_state_counts, key=top_state_counts.get)
                body += callout(
                    f"<strong>Insight:</strong> {top_state_counts[top_state]} of the top "
                    f"{len(top_targets)} targets are in <strong>{top_state}</strong>. "
                    f"Average score among top targets: <strong>"
                    f"{sum(t['score'] for t in top_targets) / len(top_targets):.1f}</strong>.",
                )
        else:
            body += callout(
                "<strong>No A/B-grade targets found.</strong> Run the med-spa discovery "
                "pipeline to populate prospects.",
                variant="warn",
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 4: Market Concentration Analysis
        # ==================================================================
        body += "\n" + section_start(4, "Market Concentration Analysis", "concentration")
        body += '<p>Understanding competition density and geographic distribution of targets.</p>'

        # 4a: Top ZIPs by prospect count
        if zip_conc:
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Top ZIPs by Prospect Density</h3>'

            zip_rows = []
            for z in zip_conc[:15]:
                zip_rows.append([
                    z["zip_code"],
                    z.get("city") or "-",
                    z.get("state") or "-",
                    str(z["prospect_count"]),
                    str(z["avg_score"]),
                    str(z["avg_zip_score"]),
                ])
            body += data_table(
                headers=["ZIP", "City", "State", "Prospects", "Avg Score", "ZIP Score"],
                rows=zip_rows,
                numeric_columns={3, 4, 5},
            )

        # 4b: States ranked by avg score
        if state_avgs:
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">States Ranked by Average Acquisition Score</h3>'

            state_avg_labels = [s["state"] for s in state_avgs]
            state_avg_values = [s["avg_score"] for s in state_avgs]
            state_avg_config = build_horizontal_bar_config(
                state_avg_labels, state_avg_values, dataset_label="Avg Score"
            )
            state_avg_json = json.dumps(state_avg_config)
            sa_height = f"{max(len(state_avg_labels) * 48 + 40, 200)}px"

            body += chart_container(
                "stateAvgChart", state_avg_json,
                build_bar_fallback(state_avg_labels, state_avg_values),
                title="Average Acquisition Score by State",
                height=sa_height,
            )
            charts_js += chart_init_js("stateAvgChart", state_avg_json)

        # 4c: A-grade concentration
        if a_grade_state:
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">A-Grade Concentration by State</h3>'

            ag_rows = []
            for a in a_grade_state:
                ag_rows.append([
                    a["state"],
                    str(a["a_count"]),
                    str(a["total"]),
                    f'{a["a_pct"]}%',
                ])
            body += data_table(
                headers=["State", "A-Grade", "Total", "A-Grade %"],
                rows=ag_rows,
                numeric_columns={1, 2, 3},
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 5: ZIP Affluence Profile
        # ==================================================================
        body += "\n" + section_start(5, "ZIP Affluence Profile", "zip-affluence")
        body += '<p>ZIP-level affluence analysis from IRS SOI income data, correlated with med-spa market potential.</p>'

        # 5a: ZIP affluence by state
        if zip_affluence:
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Average ZIP Affluence Score by State (Top 15)</h3>'

            za_rows = []
            for z in zip_affluence:
                za_rows.append([
                    z["state"],
                    str(z["zip_count"]),
                    str(z["avg_score"]),
                    str(z["avg_affluence"]),
                    _fmt_currency(z["avg_agi"]),
                    str(z["a_zips"]),
                ])
            body += data_table(
                headers=["State", "ZIPs Scored", "Avg Score", "Affluence Score", "Avg AGI", "A-Grade ZIPs"],
                rows=za_rows,
                numeric_columns={1, 2, 3, 4, 5},
            )

        # 5b: Income correlation
        if census_income:
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Income vs. Med-Spa Score Correlation</h3>'

            inc_labels = [c["state"] for c in census_income]
            inc_values = [c["avg_zip_score"] for c in census_income]
            inc_config = build_horizontal_bar_config(
                inc_labels, inc_values, dataset_label="Avg ZIP Score"
            )
            inc_json = json.dumps(inc_config)
            inc_height = f"{max(len(inc_labels) * 48 + 40, 200)}px"

            body += chart_container(
                "incomeChart", inc_json,
                build_bar_fallback(inc_labels, inc_values),
                title="Average ZIP Med-Spa Score by State",
                height=inc_height,
            )
            charts_js += chart_init_js("incomeChart", inc_json)

        # 5c: High-income ZIP penetration
        if hi_zip_pen.get("total_a_zips"):
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">High-Income ZIP Penetration</h3>'
            body += '<div class="metric-grid">'
            body += f"""<div class="metric-card">
    <div class="metric-label">A-Grade Affluent ZIPs</div>
    <div class="metric-value">{_fmt(hi_zip_pen['total_a_zips'])}</div>
    <div class="metric-detail">ZIPs scoring 80+ on affluence model</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">ZIPs with Prospects</div>
    <div class="metric-value">{_fmt(hi_zip_pen['zips_with_prospects'])}</div>
    <div class="metric-detail">A-grade ZIPs where we found med-spas</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">Market Penetration</div>
    <div class="metric-value">{hi_zip_pen['penetration_pct']:.0f}%</div>
    <div class="metric-detail">Coverage of top-affluence ZIP codes</div>
</div>"""
            body += "</div>"

            if hi_zip_pen["penetration_pct"] < 50:
                body += callout(
                    f"<strong>Opportunity:</strong> Only {hi_zip_pen['penetration_pct']:.0f}% of "
                    f"A-grade affluent ZIPs have discoverable med-spa businesses. "
                    f"This represents <strong>{hi_zip_pen['total_a_zips'] - hi_zip_pen['zips_with_prospects']}</strong> "
                    f"underserved high-income markets.",
                    variant="good",
                )
            else:
                body += callout(
                    f"<strong>Market Maturity:</strong> {hi_zip_pen['penetration_pct']:.0f}% of "
                    f"A-grade affluent ZIPs already have med-spa businesses. "
                    f"Focus on acquisition quality over greenfield expansion.",
                )

        body += "\n" + section_end()

        # ==================================================================
        # Section 6: Competitive Landscape
        # ==================================================================
        body += "\n" + section_start(6, "Competitive Landscape", "competitive")
        body += '<p>PE-backed aesthetics platforms and recent deal activity in the sector.</p>'

        # 6a: PE platform comps
        if pe_comps:
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">PE-Backed Aesthetics Platforms</h3>'

            comp_rows = []
            for c in pe_comps:
                owner = c.get("pe_owner") or "-"
                location = ", ".join(
                    p for p in [c.get("city"), c.get("state")] if p
                ) or "-"
                emp = _fmt(c.get("employees")) if c.get("employees") else "-"
                status_badge = pill_badge(
                    c.get("status") or "Active",
                    "public" if c.get("status") == "Active" else "default",
                )
                comp_rows.append([
                    f'<span class="company-name">{c["name"]}</span>',
                    c.get("industry") or "-",
                    location,
                    owner,
                    emp,
                    status_badge,
                ])

            body += data_table(
                headers=["Company", "Industry", "Location", "PE Owner", "Employees", "Status"],
                rows=comp_rows,
            )

            body += callout(
                f"<strong>Competitive context:</strong> {len(pe_comps)} PE-backed platforms "
                f"identified in aesthetics/med-spa sector. Roll-up activity signals "
                f"strong institutional interest in the space.",
            )
        else:
            body += callout(
                "<strong>No PE-backed aesthetics companies found in database.</strong> "
                "Run PE portfolio collection with aesthetics sector filter to populate comps.",
                variant="warn",
            )

        # 6b: Recent deals
        if recent_deals:
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Recent Deal Activity</h3>'

            deal_rows = []
            for d in recent_deals:
                ev_display = _fmt_currency(d.get("ev")) if d.get("ev") else "-"
                mult_display = f'{d["ev_ebitda"]:.1f}x' if d.get("ev_ebitda") else "-"
                date_display = d.get("announced_date") or d.get("closed_date") or "-"
                deal_rows.append([
                    f'<span class="company-name">{d.get("company") or d.get("deal_name") or "N/A"}</span>',
                    d.get("deal_type") or "-",
                    date_display,
                    ev_display,
                    mult_display,
                ])

            body += data_table(
                headers=["Company", "Deal Type", "Date", "EV", "EV/EBITDA"],
                rows=deal_rows,
                numeric_columns={3, 4},
            )

        # Market consolidation trends narrative
        body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Market Consolidation Trends</h3>'
        body += """<div class="thesis-box">
<h3>Why Aesthetics is Attractive for PE Roll-Up</h3>
<ul>
    <li><strong>Fragmented market:</strong> The US aesthetics market is estimated at $20B+ with thousands of independent operators, creating ample buy-and-build opportunity</li>
    <li><strong>Recurring revenue:</strong> Med-spas benefit from membership models, repeat visit patterns, and consumable product revenue (Botox, fillers, skincare)</li>
    <li><strong>Demographic tailwinds:</strong> Growing demand across age cohorts, male demographics, and geographic markets driven by social media normalization</li>
    <li><strong>Operating leverage:</strong> Multi-unit platforms achieve procurement savings (15-25%), shared marketing, and provider network effects</li>
    <li><strong>Multiple arbitrage:</strong> Single-unit med-spas trade at 3-5x EBITDA; scaled platforms command 8-12x, creating immediate value on consolidation</li>
</ul>
</div>"""

        body += "\n" + section_end()

        # ==================================================================
        # Section 7: Data Sources & Methodology
        # ==================================================================
        body += "\n" + section_start(7, "Data Sources & Methodology", "methodology")

        # 7a: Scoring model explanation
        body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Acquisition Prospect Scoring Model</h3>'
        body += '<p>Each med-spa prospect receives a composite score (0-100) based on five weighted factors:</p>'

        # Weight visualization
        for factor, weight in PROSPECT_WEIGHTS.items():
            label = factor.replace("_", " ").title()
            pct = int(weight * 100)
            body += f"""<div class="weight-row">
    <span class="weight-label">{label}</span>
    <div class="weight-bar-track"><div class="weight-bar-fill" style="width:{pct * 3}px"></div></div>
    <span class="weight-pct">{pct}%</span>
</div>"""

        body += '<p style="margin-top:16px">Scores are converted to letter grades using standard thresholds:</p>'

        grade_table_rows = [
            ["A", "80-100", "Top-tier acquisition target"],
            ["B", "65-79", "Strong prospect"],
            ["C", "50-64", "Moderate potential"],
            ["D", "35-49", "Below-average prospect"],
            ["F", "0-34", "Not recommended"],
        ]
        body += data_table(
            headers=["Grade", "Score Range", "Interpretation"],
            rows=grade_table_rows,
        )

        # ZIP affluence model
        body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">ZIP Revenue Potential Score</h3>'
        body += '<p>Each US ZIP code is scored (0-100) for med-spa revenue potential using five IRS SOI-derived signals:</p>'

        for factor, weight in ZIP_SCORE_WEIGHTS.items():
            label = factor.replace("_", " ").title()
            pct = int(weight * 100)
            body += f"""<div class="weight-row">
    <span class="weight-label">{label}</span>
    <div class="weight-bar-track"><div class="weight-bar-fill" style="width:{pct * 3}px"></div></div>
    <span class="weight-pct">{pct}%</span>
</div>"""

        # 7b: Data freshness
        body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Data Freshness</h3>'

        freshness_rows = []
        if data_fresh.get("prospects"):
            p = data_fresh["prospects"]
            freshness_rows.append([
                "Med-Spa Prospects",
                "Yelp Business Search API",
                _fmt(p.get("total")),
                p.get("latest", "-"),
            ])
        if data_fresh.get("zip_scores"):
            z = data_fresh["zip_scores"]
            freshness_rows.append([
                "ZIP Affluence Scores",
                "IRS SOI ZIP Income Data (2021 tax year)",
                _fmt(z.get("total")),
                z.get("latest", "-"),
            ])

        # Always show these static rows
        freshness_rows.append([
            "PE Portfolio Companies",
            "SEC EDGAR, Company Websites",
            "-",
            "-",
        ])
        freshness_rows.append([
            "Census ACS Income",
            "US Census Bureau ACS 5-Year",
            "-",
            "-",
        ])

        body += data_table(
            headers=["Dataset", "Source", "Records", "Last Updated"],
            rows=freshness_rows,
        )

        # 7c: Coverage statistics
        body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Coverage Statistics</h3>'
        body += '<div class="metric-grid">'
        body += f"""<div class="metric-card">
    <div class="metric-label">Prospects Discovered</div>
    <div class="metric-value">{_fmt(summary.get('total_prospects'))}</div>
    <div class="metric-detail">Unique med-spa locations from Yelp</div>
</div>"""
        body += f"""<div class="metric-card">
    <div class="metric-label">ZIPs Covered</div>
    <div class="metric-value">{_fmt(summary.get('zips_covered'))}</div>
    <div class="metric-detail">Unique ZIP codes with prospects</div>
</div>"""
        body += f"""<div class="metric-card">
    <div class="metric-label">Total Consumer Reviews</div>
    <div class="metric-value">{_fmt(summary.get('total_reviews'))}</div>
    <div class="metric-detail">Aggregate Yelp reviews analyzed</div>
</div>"""
        body += "</div>"

        body += "\n" + section_end()

        # ---- Close container ----
        body += "\n</div>"

        # ---- Footer ----
        notes = [
            "Prospect data sourced from Yelp Business Search API; scores reflect publicly available ratings and review counts.",
            "ZIP affluence model uses IRS Statistics of Income (SOI) ZIP-level data, tax year 2021.",
            "PE competitive landscape data from SEC EDGAR filings and company websites.",
            "Acquisition scores are model-generated estimates and should be validated with on-the-ground diligence.",
            "This report does not constitute investment advice. All data is from public sources.",
        ]
        body += "\n" + page_footer(
            notes=notes,
            generated_line=f"Report generated {data.get('generated_at', 'N/A')} | Nexdata Investment Intelligence",
        )

        return html_document(
            title=f"Med-Spa Market Analysis{scope_label}",
            body_content=body,
            charts_js=charts_js,
            extra_css=MEDSPA_EXTRA_CSS,
        )

    # ------------------------------------------------------------------
    # Excel Rendering
    # ------------------------------------------------------------------

    def render_excel(self, data: Dict[str, Any]) -> bytes:
        """Render report as Excel workbook."""
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment

        wb = Workbook()

        header_fill = PatternFill(
            start_color="1A365D", end_color="1A365D", fill_type="solid"
        )
        header_font = Font(bold=True, color="FFFFFF", size=11)
        title_font = Font(bold=True, size=16)
        section_font = Font(bold=True, size=13)

        summary = data.get("summary", {})

        # ---- Sheet 1: Executive Summary ----
        ws = wb.active
        ws.title = "Executive Summary"

        ws["A1"] = "Med-Spa Market Analysis"
        ws["A1"].font = title_font
        ws.merge_cells("A1:D1")

        ws["A3"] = "Total Prospects"
        ws["B3"] = summary.get("total_prospects", 0)
        ws["A4"] = "A-Grade Targets"
        ws["B4"] = summary.get("a_grade", 0)
        ws["A5"] = "A/B-Grade Targets"
        ws["B5"] = summary.get("ab_grade", 0)
        ws["A6"] = "States Covered"
        ws["B6"] = summary.get("states_covered", 0)
        ws["A7"] = "ZIPs Covered"
        ws["B7"] = summary.get("zips_covered", 0)
        ws["A8"] = "Avg Acquisition Score"
        ws["B8"] = summary.get("avg_score", 0)
        ws["A9"] = "Avg Yelp Rating"
        ws["B9"] = summary.get("avg_rating", 0)
        ws["A10"] = "Avg Reviews"
        ws["B10"] = summary.get("avg_reviews", 0)

        for row in range(3, 11):
            ws[f"A{row}"].font = Font(bold=True)

        ws["A12"] = "Grade Distribution"
        ws["A12"].font = section_font
        row_num = 13
        for gd in data.get("grade_distribution", []):
            ws[f"A{row_num}"] = f"Grade {gd['grade']}"
            ws[f"B{row_num}"] = gd["count"]
            ws[f"C{row_num}"] = f"{gd['pct']}%"
            row_num += 1

        ws.column_dimensions["A"].width = 25
        ws.column_dimensions["B"].width = 15
        ws.column_dimensions["C"].width = 10

        # ---- Sheet 2: Top Targets ----
        ws_targets = wb.create_sheet("Top Targets")
        top_targets = data.get("top_targets", [])

        headers = [
            "Rank", "Name", "City", "State", "ZIP", "Score", "Grade",
            "Rating", "Reviews", "Price", "ZIP Grade", "ZIP Score",
            "Competitors in ZIP", "Phone",
        ]
        for col, header in enumerate(headers, 1):
            cell = ws_targets.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, t in enumerate(top_targets, 1):
            row_num = i + 1
            ws_targets.cell(row=row_num, column=1, value=i)
            ws_targets.cell(row=row_num, column=2, value=t.get("name"))
            ws_targets.cell(row=row_num, column=3, value=t.get("city"))
            ws_targets.cell(row=row_num, column=4, value=t.get("state"))
            ws_targets.cell(row=row_num, column=5, value=t.get("zip_code"))
            ws_targets.cell(row=row_num, column=6, value=t.get("score"))
            ws_targets.cell(row=row_num, column=7, value=t.get("grade"))
            ws_targets.cell(row=row_num, column=8, value=t.get("rating"))
            ws_targets.cell(row=row_num, column=9, value=t.get("reviews"))
            ws_targets.cell(row=row_num, column=10, value=t.get("price"))
            ws_targets.cell(row=row_num, column=11, value=t.get("zip_grade"))
            ws_targets.cell(row=row_num, column=12, value=t.get("zip_score"))
            ws_targets.cell(row=row_num, column=13, value=t.get("competitors"))
            ws_targets.cell(row=row_num, column=14, value=t.get("phone"))

        col_widths = {
            "A": 6, "B": 35, "C": 18, "D": 8, "E": 10, "F": 8,
            "G": 8, "H": 8, "I": 10, "J": 8, "K": 10, "L": 10,
            "M": 18, "N": 15,
        }
        for col_letter, width in col_widths.items():
            ws_targets.column_dimensions[col_letter].width = width

        # ---- Sheet 3: By State ----
        ws_state = wb.create_sheet("By State")
        state_avgs = data.get("state_avg_scores", [])

        headers = ["State", "Prospects", "Avg Score", "Max Score", "A-Grade Count"]
        for col, header in enumerate(headers, 1):
            cell = ws_state.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, s in enumerate(state_avgs, 2):
            ws_state.cell(row=i, column=1, value=s.get("state"))
            ws_state.cell(row=i, column=2, value=s.get("prospect_count"))
            ws_state.cell(row=i, column=3, value=s.get("avg_score"))
            ws_state.cell(row=i, column=4, value=s.get("max_score"))
            ws_state.cell(row=i, column=5, value=s.get("a_count"))

        ws_state.column_dimensions["A"].width = 8
        ws_state.column_dimensions["B"].width = 12
        ws_state.column_dimensions["C"].width = 12
        ws_state.column_dimensions["D"].width = 12
        ws_state.column_dimensions["E"].width = 15

        # ---- Sheet 4: ZIP Concentration ----
        ws_zip = wb.create_sheet("ZIP Concentration")
        zip_conc = data.get("zip_concentration", [])

        headers = ["ZIP", "City", "State", "Prospects", "Avg Score", "ZIP Score"]
        for col, header in enumerate(headers, 1):
            cell = ws_zip.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, z in enumerate(zip_conc, 2):
            ws_zip.cell(row=i, column=1, value=z.get("zip_code"))
            ws_zip.cell(row=i, column=2, value=z.get("city"))
            ws_zip.cell(row=i, column=3, value=z.get("state"))
            ws_zip.cell(row=i, column=4, value=z.get("prospect_count"))
            ws_zip.cell(row=i, column=5, value=z.get("avg_score"))
            ws_zip.cell(row=i, column=6, value=z.get("avg_zip_score"))

        for col_letter in ["A", "B", "C", "D", "E", "F"]:
            ws_zip.column_dimensions[col_letter].width = 14

        # ---- Sheet 5: ZIP Affluence ----
        ws_aff = wb.create_sheet("ZIP Affluence")
        zip_affluence = data.get("zip_affluence_by_state", [])

        headers = ["State", "ZIPs Scored", "Avg Score", "Affluence Score", "Avg AGI", "A-Grade ZIPs"]
        for col, header in enumerate(headers, 1):
            cell = ws_aff.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, z in enumerate(zip_affluence, 2):
            ws_aff.cell(row=i, column=1, value=z.get("state"))
            ws_aff.cell(row=i, column=2, value=z.get("zip_count"))
            ws_aff.cell(row=i, column=3, value=z.get("avg_score"))
            ws_aff.cell(row=i, column=4, value=z.get("avg_affluence"))
            ws_aff.cell(row=i, column=5, value=z.get("avg_agi"))
            ws_aff.cell(row=i, column=6, value=z.get("a_zips"))

        for col_letter in ["A", "B", "C", "D", "E", "F"]:
            ws_aff.column_dimensions[col_letter].width = 16

        # ---- Sheet 6: PE Comps ----
        ws_comps = wb.create_sheet("PE Comps")
        pe_comps = data.get("pe_comps", [])

        headers = ["Company", "Industry", "Sub-Industry", "Location", "PE Owner", "Employees", "Status"]
        for col, header in enumerate(headers, 1):
            cell = ws_comps.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, c in enumerate(pe_comps, 2):
            location = ", ".join(p for p in [c.get("city"), c.get("state")] if p) or "-"
            ws_comps.cell(row=i, column=1, value=c.get("name"))
            ws_comps.cell(row=i, column=2, value=c.get("industry"))
            ws_comps.cell(row=i, column=3, value=c.get("sub_industry"))
            ws_comps.cell(row=i, column=4, value=location)
            ws_comps.cell(row=i, column=5, value=c.get("pe_owner"))
            ws_comps.cell(row=i, column=6, value=c.get("employees"))
            ws_comps.cell(row=i, column=7, value=c.get("status"))

        col_widths = {"A": 30, "B": 20, "C": 20, "D": 20, "E": 25, "F": 12, "G": 10}
        for col_letter, width in col_widths.items():
            ws_comps.column_dimensions[col_letter].width = width

        # Save to bytes
        output = BytesIO()
        wb.save(output)
        return output.getvalue()
