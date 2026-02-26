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
  8. Whitespace Analysis
  9. Workforce Economics
  10. Service Category Breakdown
  11. PE Platform Benchmarking
  12. Review Velocity & Growth Signals
  13. Deal Model — Unit Economics
  14. Deal Model — Capital Requirements
  15. Deal Model — Returns Analysis
  16. Stealth Wealth Signal (IRS SOI non-wage income analysis)
  17. Migration Alpha (IRS county-to-county wealth flow leading indicator)
  18. Medical Provider Density Signal (CMS Medicare provider-to-medspa imbalance)
  19. Real Estate Appreciation Alpha (Redfin/FHFA home price timing signal)
  20. Deposit Wealth Concentration (FDIC branch deposits per capita)
  21. Business Formation Velocity (IRS SOI business income density)
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

# ---------------------------------------------------------------------------
# Deal Model Benchmarks (Industry averages by Yelp price tier)
# Sources: AmSpa State of the Industry, IBISWorld, PE deal comps
# ---------------------------------------------------------------------------

MEDSPA_BENCHMARKS = {
    "$":    {"revenue": 400_000, "ebitda_margin": 0.12, "entry_multiple": 3.0},
    "$$":   {"revenue": 700_000, "ebitda_margin": 0.18, "entry_multiple": 3.5},
    "$$$":  {"revenue": 1_200_000, "ebitda_margin": 0.22, "entry_multiple": 4.0},
    "$$$$": {"revenue": 2_000_000, "ebitda_margin": 0.25, "entry_multiple": 4.5},
    None:   {"revenue": 600_000, "ebitda_margin": 0.15, "entry_multiple": 3.5},
}

DEAL_ASSUMPTIONS = {
    "debt_pct": 0.60,
    "equity_pct": 0.40,
    "transaction_cost_pct": 0.05,
    "working_capital_months": 3,
    "sga_pct": 0.32,
    "cogs_pct": 0.40,
    "scenarios": {
        "conservative": {"entry_multiple": 3.0, "exit_multiple": 7, "margin_improvement": 0.03, "hold_years": 5},
        "base":         {"entry_multiple": 4.0, "exit_multiple": 10, "margin_improvement": 0.05, "hold_years": 5},
        "aggressive":   {"entry_multiple": 3.5, "exit_multiple": 12, "margin_improvement": 0.07, "hold_years": 4},
    },
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

/* Deal Model Sections */
.deal-scenario-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 16px;
    margin: 16px 0;
}
.scenario-card {
    background: var(--white);
    border-radius: 10px;
    padding: 20px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    border-top: 4px solid var(--primary);
}
.scenario-card.conservative { border-top-color: #48bb78; }
.scenario-card.base { border-top-color: #4299e1; }
.scenario-card.aggressive { border-top-color: #ed8936; }
.scenario-card .scenario-label {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    font-weight: 700;
    margin-bottom: 12px;
}
.scenario-card.conservative .scenario-label { color: #48bb78; }
.scenario-card.base .scenario-label { color: #4299e1; }
.scenario-card.aggressive .scenario-label { color: #ed8936; }
.scenario-card .scenario-metric {
    display: flex;
    justify-content: space-between;
    padding: 6px 0;
    border-bottom: 1px solid var(--gray-100);
    font-size: 13px;
}
.scenario-card .scenario-metric:last-child { border-bottom: none; }
.scenario-card .scenario-metric .label { color: var(--gray-500); }
.scenario-card .scenario-metric .value { font-weight: 600; color: var(--gray-900); }

.capital-stack {
    display: flex;
    height: 40px;
    border-radius: 8px;
    overflow: hidden;
    margin: 16px 0;
}
.capital-stack .stack-segment {
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 11px;
    font-weight: 600;
    color: #fff;
    transition: width 0.3s ease;
}

.pnl-waterfall {
    display: flex;
    align-items: flex-end;
    gap: 8px;
    height: 180px;
    padding: 16px 0;
}
.pnl-waterfall .waterfall-bar {
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: flex-end;
}
.pnl-waterfall .waterfall-bar .bar {
    width: 100%;
    max-width: 80px;
    border-radius: 4px 4px 0 0;
    transition: height 0.3s ease;
}
.pnl-waterfall .waterfall-bar .bar-label {
    font-size: 11px;
    color: var(--gray-500);
    margin-top: 6px;
    text-align: center;
}
.pnl-waterfall .waterfall-bar .bar-value {
    font-size: 12px;
    font-weight: 600;
    color: var(--gray-900);
    margin-bottom: 4px;
}

/* Stealth Wealth & Migration Alpha Sections */
.wealth-composition-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    margin: 16px 0;
    align-items: start;
}
@media (max-width: 768px) {
    .wealth-composition-grid { grid-template-columns: 1fr; }
}
.highlight-table tr.emerging-highlight {
    background: rgba(72, 187, 120, 0.08);
}
.highlight-table tr.emerging-highlight td:first-child {
    border-left: 3px solid #48bb78;
}
[data-theme="dark"] .highlight-table tr.emerging-highlight {
    background: rgba(72, 187, 120, 0.12);
}
.migration-bar-pos { background: #48bb78; }
.migration-bar-neg { background: #fc8181; }

/* Sections 18-21: Provider / RE / Deposit / Business Formation */
.provider-split-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    margin: 16px 0;
    align-items: start;
}
@media (max-width: 768px) {
    .provider-split-grid { grid-template-columns: 1fr; }
}
.highlight-table tr.opportunity-highlight {
    background: rgba(66, 153, 225, 0.08);
}
.highlight-table tr.opportunity-highlight td:first-child {
    border-left: 3px solid #4299e1;
}
[data-theme="dark"] .highlight-table tr.opportunity-highlight {
    background: rgba(66, 153, 225, 0.12);
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
            # Section 8: Whitespace Analysis
            **self._get_whitespace_data(db),
            # Section 9: Workforce Economics
            **self._get_bls_wage_data(db),
            # Section 10: Service Categories
            "category_breakdown": self._get_category_breakdown(db, state_filter),
            # Section 11: PE Benchmarking
            **self._get_pe_financial_benchmarks(db),
            # Section 12: Growth Signals
            **self._get_growth_signals(db, state_filter),
            # Sections 13-15: Deal Model
            "deal_model": self._get_deal_model_data(db, state_filter),
            # Section 16: Stealth Wealth Signal
            **self._get_stealth_wealth_data(db, state_filter),
            # Section 17: Migration Alpha
            **self._get_migration_alpha_data(db, state_filter),
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

        # IRS SOI ZIP Income freshness
        try:
            result = db.execute(
                text("""
                    SELECT
                        MIN(tax_year) as earliest_year,
                        MAX(tax_year) as latest_year,
                        COUNT(*) as total
                    FROM irs_soi_zip_income
                """),
            )
            row = result.fetchone()
            if row and row[2]:
                freshness["irs_soi_zip_income"] = {
                    "earliest": str(row[0]) if row[0] else None,
                    "latest": str(row[1]) if row[1] else None,
                    "total": row[2],
                }
        except Exception:
            db.rollback()

        # IRS SOI Migration freshness
        try:
            result = db.execute(
                text("""
                    SELECT
                        MIN(tax_year) as earliest_year,
                        MAX(tax_year) as latest_year,
                        COUNT(*) as total
                    FROM irs_soi_migration
                """),
            )
            row = result.fetchone()
            if row and row[2]:
                freshness["irs_soi_migration"] = {
                    "earliest": str(row[0]) if row[0] else None,
                    "latest": str(row[1]) if row[1] else None,
                    "total": row[2],
                }
        except Exception:
            db.rollback()

        return freshness

    # ------------------------------------------------------------------
    # Section 8: Whitespace Analysis
    # ------------------------------------------------------------------

    def _get_whitespace_data(self, db: Session) -> Dict:
        """Get A-grade ZIPs with no discovered prospects (greenfield opportunities)."""
        try:
            # Whitespace ZIPs: A-grade with no prospects
            result = db.execute(
                text("""
                    SELECT z.zip_code, z.state_abbr, z.overall_score, z.avg_agi,
                           z.total_returns, z.affluence_density_score
                    FROM zip_medspa_scores z
                    WHERE z.grade = 'A'
                      AND z.zip_code NOT IN (
                          SELECT DISTINCT zip_code FROM medspa_prospects
                          WHERE zip_code IS NOT NULL
                      )
                    ORDER BY z.overall_score DESC
                """),
            )
            whitespace_zips = [
                {
                    "zip_code": row[0],
                    "state": row[1],
                    "score": float(row[2]) if row[2] else 0,
                    "avg_agi": float(row[3]) if row[3] else 0,
                    "total_returns": int(row[4]) if row[4] else 0,
                    "affluence_score": float(row[5]) if row[5] else 0,
                }
                for row in result.fetchall()
            ]

            # By-state rollup
            state_counts: Dict[str, int] = {}
            for z in whitespace_zips:
                st = z["state"] or "??"
                state_counts[st] = state_counts.get(st, 0) + 1
            whitespace_by_state = sorted(
                [{"state": k, "count": v} for k, v in state_counts.items()],
                key=lambda x: x["count"],
                reverse=True,
            )

            # Summary stats
            total_a_result = db.execute(
                text("SELECT COUNT(*) FROM zip_medspa_scores WHERE grade = 'A'"),
            )
            total_a = total_a_result.scalar() or 0

            return {
                "whitespace_zips": whitespace_zips,
                "whitespace_by_state": whitespace_by_state,
                "whitespace_summary": {
                    "total_a_zips": total_a,
                    "a_with_prospects": total_a - len(whitespace_zips),
                    "whitespace_count": len(whitespace_zips),
                },
            }
        except Exception as e:
            logger.warning(f"Could not fetch whitespace data: {e}")
            db.rollback()
            return {
                "whitespace_zips": [],
                "whitespace_by_state": [],
                "whitespace_summary": {"total_a_zips": 0, "a_with_prospects": 0, "whitespace_count": 0},
            }

    # ------------------------------------------------------------------
    # Section 9: Workforce Economics (BLS OES)
    # ------------------------------------------------------------------

    def _get_bls_wage_data(self, db: Session) -> Dict:
        """Get BLS OES wage trends for aesthetics-adjacent occupations."""
        # Target series IDs: annual mean wages (data type 04) for key occupations
        WAGE_SERIES = {
            "OEUM000000000000029122904": "Dermatologists",
            "OEUM000000000000029114104": "Registered Nurses",
            "OEUM000000000000029117104": "Nurse Practitioners",
            "OEUM000000000000031901104": "Massage Therapists",
            "OEUM000000000000039501204": "Cosmetologists",
            "OEUM000000000000031909904": "Healthcare Support",
            "OEUM000000000000029107104": "Physician Assistants",
            "OEUM000000000000029209904": "Health Technicians",
        }
        EMPLOYMENT_SERIES = {
            "OEUM000000000000029122901": "Dermatologists",
            "OEUM000000000000029114101": "Registered Nurses",
            "OEUM000000000000029117101": "Nurse Practitioners",
            "OEUM000000000000031901101": "Massage Therapists",
            "OEUM000000000000039501201": "Cosmetologists",
            "OEUM000000000000031909901": "Healthcare Support",
            "OEUM000000000000029107101": "Physician Assistants",
            "OEUM000000000000029209901": "Health Technicians",
        }
        try:
            # Wages
            wage_ids = list(WAGE_SERIES.keys())
            result = db.execute(
                text("""
                    SELECT series_id, year, value
                    FROM bls_oes
                    WHERE series_id = ANY(:ids)
                      AND period = 'M13'
                    ORDER BY year
                """),
                {"ids": wage_ids},
            )
            bls_wages: Dict[str, Dict[int, float]] = {}
            for row in result.fetchall():
                occ = WAGE_SERIES.get(row[0], row[0])
                yr = int(row[1])
                val = float(row[2]) if row[2] else 0
                bls_wages.setdefault(occ, {})[yr] = val

            # Employment counts
            emp_ids = list(EMPLOYMENT_SERIES.keys())
            result = db.execute(
                text("""
                    SELECT series_id, year, value
                    FROM bls_oes
                    WHERE series_id = ANY(:ids)
                      AND period = 'M13'
                    ORDER BY year
                """),
                {"ids": emp_ids},
            )
            bls_employment: Dict[str, Dict[int, float]] = {}
            for row in result.fetchall():
                occ = EMPLOYMENT_SERIES.get(row[0], row[0])
                yr = int(row[1])
                val = float(row[2]) if row[2] else 0
                bls_employment.setdefault(occ, {})[yr] = val

            return {
                "bls_wages": bls_wages,
                "bls_employment": bls_employment,
            }
        except Exception as e:
            logger.warning(f"Could not fetch BLS wage data: {e}")
            db.rollback()
            return {"bls_wages": {}, "bls_employment": {}}

    # ------------------------------------------------------------------
    # Section 10: Service Category Breakdown
    # ------------------------------------------------------------------

    def _get_category_breakdown(self, db: Session, state: Optional[str]) -> List[Dict]:
        """Get prospect counts by Yelp category."""
        state_clause = "WHERE state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        try:
            result = db.execute(
                text(f"""
                    SELECT unnest(categories) AS category, COUNT(*) AS cnt,
                           ROUND(AVG(acquisition_score), 1) AS avg_score,
                           ROUND(AVG(rating), 2) AS avg_rating,
                           ROUND(AVG(review_count), 0) AS avg_reviews
                    FROM medspa_prospects
                    {state_clause}
                    GROUP BY category
                    ORDER BY cnt DESC
                """),
                params,
            )
            return [
                {
                    "category": row[0],
                    "count": row[1],
                    "avg_score": float(row[2]) if row[2] else 0,
                    "avg_rating": float(row[3]) if row[3] else 0,
                    "avg_reviews": int(row[4]) if row[4] else 0,
                }
                for row in result.fetchall()
            ]
        except Exception as e:
            logger.warning(f"Could not fetch category breakdown: {e}")
            db.rollback()
            return []

    # ------------------------------------------------------------------
    # Section 11: PE Platform Benchmarking
    # ------------------------------------------------------------------

    def _get_pe_financial_benchmarks(self, db: Session) -> Dict:
        """Get financial benchmarks for PE-backed aesthetics platforms."""
        try:
            result = db.execute(
                text("""
                    SELECT c.name, c.headquarters_state, c.employee_count,
                           c.current_pe_owner,
                           f.revenue_usd, f.revenue_growth_pct, f.ebitda_margin_pct,
                           f.gross_margin_pct, f.debt_to_ebitda, f.is_estimated,
                           f.confidence
                    FROM pe_portfolio_companies c
                    JOIN pe_company_financials f ON f.company_id = c.id
                    WHERE (c.industry ILIKE '%aesthetics%'
                           OR c.sub_industry ILIKE '%med%spa%'
                           OR c.sub_industry ILIKE '%dermatolog%')
                      AND f.fiscal_year = (
                          SELECT MAX(fiscal_year) FROM pe_company_financials
                          WHERE company_id = c.id
                      )
                    ORDER BY f.revenue_usd DESC NULLS LAST
                """),
            )
            pe_financials = [
                {
                    "name": row[0],
                    "state": row[1],
                    "employees": int(row[2]) if row[2] else None,
                    "pe_owner": row[3],
                    "revenue": float(row[4]) if row[4] else None,
                    "rev_growth": float(row[5]) if row[5] else None,
                    "ebitda_margin": float(row[6]) if row[6] else None,
                    "gross_margin": float(row[7]) if row[7] else None,
                    "debt_ebitda": float(row[8]) if row[8] else None,
                    "is_estimated": bool(row[9]) if row[9] is not None else True,
                    "confidence": row[10] or "low",
                }
                for row in result.fetchall()
            ]

            # Compute summary stats
            ebitda_margins = [f["ebitda_margin"] for f in pe_financials if f["ebitda_margin"] is not None]
            rev_growths = [f["rev_growth"] for f in pe_financials if f["rev_growth"] is not None]
            debt_ratios = [f["debt_ebitda"] for f in pe_financials if f["debt_ebitda"] is not None]

            pe_summary = {
                "avg_ebitda_margin": round(sum(ebitda_margins) / len(ebitda_margins), 1) if ebitda_margins else None,
                "avg_rev_growth": round(sum(rev_growths) / len(rev_growths), 1) if rev_growths else None,
                "median_debt_ebitda": round(
                    sorted(debt_ratios)[len(debt_ratios) // 2], 1
                ) if debt_ratios else None,
            }

            return {
                "pe_financials": pe_financials,
                "pe_financial_summary": pe_summary,
            }
        except Exception as e:
            logger.warning(f"Could not fetch PE financial benchmarks: {e}")
            db.rollback()
            return {
                "pe_financials": [],
                "pe_financial_summary": {"avg_ebitda_margin": None, "avg_rev_growth": None, "median_debt_ebitda": None},
            }

    # ------------------------------------------------------------------
    # Section 12: Review Velocity & Growth Signals
    # ------------------------------------------------------------------

    def _get_growth_signals(self, db: Session, state: Optional[str]) -> Dict:
        """Get review velocity and low-competition opportunities."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        try:
            # Top by review count
            result = db.execute(
                text(f"""
                    SELECT name, city, state, review_count, rating,
                           acquisition_score, acquisition_grade,
                           competitor_count_in_zip, zip_overall_score
                    FROM medspa_prospects
                    WHERE acquisition_grade IN ('A', 'B') {state_clause}
                    ORDER BY review_count DESC
                    LIMIT 25
                """),
                params,
            )
            top_by_reviews = [
                {
                    "name": row[0], "city": row[1], "state": row[2],
                    "reviews": int(row[3]) if row[3] else 0,
                    "rating": float(row[4]) if row[4] else 0,
                    "score": float(row[5]) if row[5] else 0,
                    "grade": row[6],
                    "competitors": int(row[7]) if row[7] else 0,
                    "zip_score": float(row[8]) if row[8] else 0,
                }
                for row in result.fetchall()
            ]

            # Review volume distribution
            result = db.execute(
                text(f"""
                    SELECT
                        CASE
                            WHEN review_count >= 500 THEN '500+'
                            WHEN review_count >= 200 THEN '200-499'
                            WHEN review_count >= 100 THEN '100-199'
                            WHEN review_count >= 50  THEN '50-99'
                            ELSE '<50'
                        END AS bucket,
                        COUNT(*) AS cnt,
                        ROUND(AVG(acquisition_score), 1) AS avg_score,
                        ROUND(AVG(rating), 2) AS avg_rating
                    FROM medspa_prospects
                    WHERE 1=1 {state_clause}
                    GROUP BY bucket
                    ORDER BY MIN(review_count) DESC
                """),
                params,
            )
            review_buckets = [
                {
                    "bucket": row[0], "count": row[1],
                    "avg_score": float(row[2]) if row[2] else 0,
                    "avg_rating": float(row[3]) if row[3] else 0,
                }
                for row in result.fetchall()
            ]

            # Low competition gems
            result = db.execute(
                text(f"""
                    SELECT name, city, state, acquisition_score, acquisition_grade,
                           rating, review_count, competitor_count_in_zip, zip_overall_score
                    FROM medspa_prospects
                    WHERE acquisition_grade IN ('A', 'B')
                      AND competitor_count_in_zip <= 3
                      {state_clause}
                    ORDER BY acquisition_score DESC
                    LIMIT 15
                """),
                params,
            )
            low_competition_gems = [
                {
                    "name": row[0], "city": row[1], "state": row[2],
                    "score": float(row[3]) if row[3] else 0,
                    "grade": row[4],
                    "rating": float(row[5]) if row[5] else 0,
                    "reviews": int(row[6]) if row[6] else 0,
                    "competitors": int(row[7]) if row[7] else 0,
                    "zip_score": float(row[8]) if row[8] else 0,
                }
                for row in result.fetchall()
            ]

            return {
                "top_by_reviews": top_by_reviews,
                "review_buckets": review_buckets,
                "low_competition_gems": low_competition_gems,
            }
        except Exception as e:
            logger.warning(f"Could not fetch growth signals: {e}")
            db.rollback()
            return {
                "top_by_reviews": [],
                "review_buckets": [],
                "low_competition_gems": [],
            }

    # ------------------------------------------------------------------
    # Sections 13-15: Deal Model
    # ------------------------------------------------------------------

    def _get_deal_model_data(self, db: Session, state: Optional[str]) -> Dict:
        """Build full PE roll-up deal model from A-grade prospects and industry benchmarks."""
        state_clause = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        try:
            # Count A-grade prospects by price tier
            result = db.execute(
                text(f"""
                    SELECT price, COUNT(*) as cnt
                    FROM medspa_prospects
                    WHERE acquisition_grade = 'A' {state_clause}
                    GROUP BY price
                    ORDER BY cnt DESC
                """),
                params,
            )
            tier_counts: Dict[Optional[str], int] = {}
            for row in result.fetchall():
                tier_counts[row[0]] = row[1]

            # A-grade by state for geographic breakdown
            result = db.execute(
                text(f"""
                    SELECT state, COUNT(*) as cnt
                    FROM medspa_prospects
                    WHERE acquisition_grade = 'A' AND state IS NOT NULL {state_clause}
                    GROUP BY state
                    ORDER BY cnt DESC
                """),
                params,
            )
            a_grade_states = [{"state": row[0], "count": row[1]} for row in result.fetchall()]

            # Build per-tier economics
            tier_economics = []
            total_locations = 0
            total_revenue = 0
            total_ebitda = 0
            total_acquisition_cost = 0

            for tier, count in tier_counts.items():
                benchmarks = MEDSPA_BENCHMARKS.get(tier, MEDSPA_BENCHMARKS[None])
                rev = benchmarks["revenue"]
                margin = benchmarks["ebitda_margin"]
                ebitda = rev * margin
                multiple = benchmarks["entry_multiple"]
                acq_cost = ebitda * multiple

                tier_economics.append({
                    "tier": tier or "Unknown",
                    "count": count,
                    "avg_revenue": rev,
                    "ebitda_margin": margin,
                    "avg_ebitda": ebitda,
                    "entry_multiple": multiple,
                    "total_revenue": rev * count,
                    "total_ebitda": ebitda * count,
                    "total_acq_cost": acq_cost * count,
                })
                total_locations += count
                total_revenue += rev * count
                total_ebitda += ebitda * count
                total_acquisition_cost += acq_cost * count

            # Weighted average margin
            weighted_margin = total_ebitda / total_revenue if total_revenue > 0 else 0

            # Capital stack
            da = DEAL_ASSUMPTIONS
            debt = total_acquisition_cost * da["debt_pct"]
            equity = total_acquisition_cost * da["equity_pct"]
            transaction_costs = total_acquisition_cost * da["transaction_cost_pct"]
            monthly_sga = total_revenue * da["sga_pct"] / 12
            working_capital = monthly_sga * da["working_capital_months"]
            total_capital_required = equity + transaction_costs + working_capital

            # Leverage check
            leverage_ratio = debt / total_ebitda if total_ebitda > 0 else 0

            # P&L waterfall (per average location)
            avg_revenue = total_revenue / total_locations if total_locations > 0 else 0
            avg_cogs = avg_revenue * da["cogs_pct"]
            avg_gross_profit = avg_revenue - avg_cogs
            avg_sga = avg_revenue * da["sga_pct"]
            avg_ebitda = avg_gross_profit - avg_sga

            # Scenario returns
            scenarios = {}
            for name, s in da["scenarios"].items():
                improved_ebitda = total_ebitda * (1 + s["margin_improvement"]) ** s["hold_years"]
                exit_ev = improved_ebitda * s["exit_multiple"]
                entry_ev = total_acquisition_cost
                gross_moic = exit_ev / entry_ev if entry_ev > 0 else 0
                # Net IRR approximation: (MOIC)^(1/years) - 1
                net_irr = (gross_moic ** (1.0 / s["hold_years"]) - 1) if gross_moic > 0 and s["hold_years"] > 0 else 0
                scenarios[name] = {
                    "entry_ev": entry_ev,
                    "exit_ev": exit_ev,
                    "exit_multiple": s["exit_multiple"],
                    "margin_improvement": s["margin_improvement"],
                    "hold_years": s["hold_years"],
                    "improved_ebitda": improved_ebitda,
                    "gross_moic": gross_moic,
                    "net_irr": net_irr,
                }

            return {
                "tier_economics": tier_economics,
                "total_locations": total_locations,
                "total_revenue": total_revenue,
                "total_ebitda": total_ebitda,
                "weighted_margin": weighted_margin,
                "total_acquisition_cost": total_acquisition_cost,
                "capital_stack": {
                    "debt": debt,
                    "equity": equity,
                    "transaction_costs": transaction_costs,
                    "working_capital": working_capital,
                    "total_capital_required": total_capital_required,
                },
                "leverage_ratio": leverage_ratio,
                "pnl_waterfall": {
                    "revenue": avg_revenue,
                    "cogs": avg_cogs,
                    "gross_profit": avg_gross_profit,
                    "sga": avg_sga,
                    "ebitda": avg_ebitda,
                },
                "scenarios": scenarios,
                "a_grade_states": a_grade_states,
            }
        except Exception as e:
            logger.warning(f"Could not compute deal model data: {e}")
            db.rollback()
            return {
                "tier_economics": [],
                "total_locations": 0,
                "total_revenue": 0,
                "total_ebitda": 0,
                "weighted_margin": 0,
                "total_acquisition_cost": 0,
                "capital_stack": {
                    "debt": 0, "equity": 0, "transaction_costs": 0,
                    "working_capital": 0, "total_capital_required": 0,
                },
                "leverage_ratio": 0,
                "pnl_waterfall": {
                    "revenue": 0, "cogs": 0, "gross_profit": 0,
                    "sga": 0, "ebitda": 0,
                },
                "scenarios": {},
                "a_grade_states": [],
            }

    # ------------------------------------------------------------------
    # Section 16: Stealth Wealth Signal
    # ------------------------------------------------------------------

    def _get_stealth_wealth_data(self, db: Session, state: Optional[str]) -> Dict:
        """Cross-reference IRS SOI non-wage income with medspa scores to find hidden demand."""
        try:
            # Check if table exists
            check = db.execute(text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_name = 'irs_soi_zip_income'"
            ))
            if not check.fetchone():
                return {"stealth_wealth": {}}

            # Get latest tax year
            yr_row = db.execute(text(
                "SELECT MAX(tax_year) FROM irs_soi_zip_income"
            )).fetchone()
            if not yr_row or not yr_row[0]:
                return {"stealth_wealth": {}}
            latest_year = yr_row[0]

            state_clause = "AND zi.state_abbr = :state" if state else ""
            params: Dict[str, Any] = {"year": latest_year}
            if state:
                params["state"] = state.upper()

            # --- Wealth composition summary (national/state) ---
            comp_row = db.execute(text(f"""
                SELECT
                    SUM(total_wages) AS wages,
                    SUM(total_capital_gains) AS cap_gains,
                    SUM(total_dividends) AS dividends,
                    SUM(total_business_income) AS biz_income,
                    SUM(total_agi) AS total_agi,
                    SUM(num_returns) AS total_returns
                FROM irs_soi_zip_income zi
                WHERE tax_year = :year AND agi_class = '0'
                    {state_clause}
            """), params).fetchone()

            wealth_composition = {}
            if comp_row and comp_row[4] and float(comp_row[4]) > 0:
                total_agi = float(comp_row[4])
                wages = float(comp_row[0] or 0)
                cap_gains = float(comp_row[1] or 0)
                dividends = float(comp_row[2] or 0)
                biz_income = float(comp_row[3] or 0)
                other = total_agi - wages - cap_gains - dividends - biz_income
                wealth_composition = {
                    "wages_pct": round(wages / total_agi * 100, 1),
                    "cap_gains_pct": round(cap_gains / total_agi * 100, 1),
                    "dividends_pct": round(dividends / total_agi * 100, 1),
                    "biz_income_pct": round(biz_income / total_agi * 100, 1),
                    "other_pct": round(max(other, 0) / total_agi * 100, 1),
                    "total_returns": int(comp_row[5] or 0),
                    "total_agi_billions": round(total_agi * 1000 / 1e9, 1),
                    "tax_year": latest_year,
                }

            # --- Stealth ZIPs: high non-wage income but low medspa score ---
            stealth_rows = db.execute(text(f"""
                WITH zip_wealth AS (
                    SELECT
                        zi.zip_code,
                        zi.state_abbr,
                        zi.num_returns,
                        zi.total_agi,
                        zi.total_wages,
                        (COALESCE(zi.total_capital_gains, 0)
                         + COALESCE(zi.total_dividends, 0)
                         + COALESCE(zi.total_business_income, 0)) AS non_wage_total,
                        CASE WHEN zi.num_returns > 0
                            THEN (COALESCE(zi.total_capital_gains, 0)
                                  + COALESCE(zi.total_dividends, 0)
                                  + COALESCE(zi.total_business_income, 0))
                                 * 1000.0 / zi.num_returns
                            ELSE 0 END AS non_wage_per_return,
                        CASE WHEN zi.total_agi > 0
                            THEN (COALESCE(zi.total_capital_gains, 0)
                                  + COALESCE(zi.total_dividends, 0)
                                  + COALESCE(zi.total_business_income, 0))
                                 * 100.0 / zi.total_agi
                            ELSE 0 END AS non_wage_pct
                    FROM irs_soi_zip_income zi
                    WHERE zi.tax_year = :year
                        AND zi.agi_class = '0'
                        AND zi.num_returns > 100
                        {state_clause}
                )
                SELECT
                    zw.zip_code,
                    zw.state_abbr,
                    ROUND(zw.non_wage_per_return::numeric, 0) AS non_wage_per_return,
                    ROUND(zw.non_wage_pct::numeric, 1) AS non_wage_pct,
                    zw.num_returns,
                    ROUND(zw.total_agi * 1000.0 / NULLIF(zw.num_returns, 0), 0) AS avg_agi,
                    COALESCE(zms.overall_score, 0) AS medspa_score,
                    COALESCE(zms.grade, '-') AS medspa_grade,
                    COALESCE(mp_cnt.a_count, 0) AS a_grade_medspas
                FROM zip_wealth zw
                LEFT JOIN zip_medspa_scores zms ON zms.zip_code = zw.zip_code
                LEFT JOIN (
                    SELECT zip_code, COUNT(*) AS a_count
                    FROM medspa_prospects
                    WHERE acquisition_grade = 'A'
                    GROUP BY zip_code
                ) mp_cnt ON mp_cnt.zip_code = zw.zip_code
                WHERE zw.non_wage_pct > 25
                    AND COALESCE(zms.overall_score, 0) < 70
                ORDER BY zw.non_wage_per_return DESC
                LIMIT 50
            """), params).fetchall()

            stealth_zips = [
                {
                    "zip_code": r[0],
                    "state": r[1],
                    "non_wage_per_return": float(r[2] or 0),
                    "non_wage_pct": float(r[3] or 0),
                    "num_returns": int(r[4] or 0),
                    "avg_agi": float(r[5] or 0),
                    "medspa_score": float(r[6] or 0),
                    "medspa_grade": r[7] or "-",
                    "a_grade_medspas": int(r[8] or 0),
                }
                for r in stealth_rows
            ]

            # Count validated ZIPs (high non-wage AND high medspa score)
            validated_row = db.execute(text(f"""
                SELECT COUNT(*)
                FROM irs_soi_zip_income zi
                JOIN zip_medspa_scores zms ON zms.zip_code = zi.zip_code
                WHERE zi.tax_year = :year AND zi.agi_class = '0'
                    AND zi.num_returns > 100
                    AND zms.overall_score >= 70
                    AND (COALESCE(zi.total_capital_gains, 0)
                         + COALESCE(zi.total_dividends, 0)
                         + COALESCE(zi.total_business_income, 0))
                         * 100.0 / NULLIF(zi.total_agi, 0) > 25
                    {state_clause}
            """), params).fetchone()
            validated_count = int(validated_row[0]) if validated_row else 0

            # Top states by stealth ZIP count
            top_states_rows = db.execute(text(f"""
                WITH zip_wealth AS (
                    SELECT zi.zip_code, zi.state_abbr,
                        CASE WHEN zi.total_agi > 0
                            THEN (COALESCE(zi.total_capital_gains, 0)
                                  + COALESCE(zi.total_dividends, 0)
                                  + COALESCE(zi.total_business_income, 0))
                                 * 100.0 / zi.total_agi
                            ELSE 0 END AS non_wage_pct
                    FROM irs_soi_zip_income zi
                    WHERE zi.tax_year = :year AND zi.agi_class = '0'
                        AND zi.num_returns > 100
                        {state_clause}
                )
                SELECT zw.state_abbr, COUNT(*) AS stealth_count
                FROM zip_wealth zw
                LEFT JOIN zip_medspa_scores zms ON zms.zip_code = zw.zip_code
                WHERE zw.non_wage_pct > 25
                    AND COALESCE(zms.overall_score, 0) < 70
                GROUP BY zw.state_abbr
                ORDER BY stealth_count DESC
                LIMIT 15
            """), params).fetchall()

            top_states = [
                {"state": r[0], "count": int(r[1])}
                for r in top_states_rows
            ]

            avg_non_wage = (
                round(sum(z["non_wage_per_return"] for z in stealth_zips) / len(stealth_zips), 0)
                if stealth_zips else 0
            )

            return {
                "stealth_wealth": {
                    "stealth_zips": stealth_zips,
                    "validated_count": validated_count,
                    "wealth_composition": wealth_composition,
                    "top_states": top_states,
                    "summary": {
                        "total_stealth": len(stealth_zips),
                        "validated": validated_count,
                        "avg_non_wage_income": avg_non_wage,
                        "tax_year": latest_year,
                    },
                }
            }
        except Exception as e:
            logger.warning(f"Could not compute stealth wealth data: {e}")
            db.rollback()
            return {"stealth_wealth": {}}

    # ------------------------------------------------------------------
    # Section 17: Migration Alpha
    # ------------------------------------------------------------------

    def _get_migration_alpha_data(self, db: Session, state: Optional[str]) -> Dict:
        """Use IRS county-to-county migration to identify wealth inflow vs medspa density."""
        try:
            # Check if table exists
            check = db.execute(text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_name = 'irs_soi_migration'"
            ))
            if not check.fetchone():
                return {"migration_alpha": {}}

            # Get latest tax year
            yr_row = db.execute(text(
                "SELECT MAX(tax_year) FROM irs_soi_migration"
            )).fetchone()
            if not yr_row or not yr_row[0]:
                return {"migration_alpha": {}}
            latest_year = yr_row[0]

            state_clause_dest = "AND m.dest_state_abbr = :state" if state else ""
            params: Dict[str, Any] = {"year": latest_year}
            if state:
                params["state"] = state.upper()

            # --- State-level net AGI flows ---
            state_flows_rows = db.execute(text(f"""
                WITH inflows AS (
                    SELECT dest_state_abbr AS state_abbr,
                           SUM(total_agi) AS inflow_agi,
                           SUM(num_returns) AS inflow_returns
                    FROM irs_soi_migration m
                    WHERE tax_year = :year AND flow_type = 'inflow'
                        {state_clause_dest}
                    GROUP BY dest_state_abbr
                ),
                outflows AS (
                    SELECT dest_state_abbr AS state_abbr,
                           SUM(total_agi) AS outflow_agi,
                           SUM(num_returns) AS outflow_returns
                    FROM irs_soi_migration m
                    WHERE tax_year = :year AND flow_type = 'outflow'
                        {state_clause_dest}
                    GROUP BY dest_state_abbr
                ),
                medspa_density AS (
                    SELECT state, COUNT(*) AS total_medspas,
                           COUNT(*) FILTER (WHERE acquisition_grade = 'A') AS a_grade_count
                    FROM medspa_prospects
                    GROUP BY state
                )
                SELECT
                    COALESCE(i.state_abbr, o.state_abbr) AS state_abbr,
                    COALESCE(i.inflow_agi, 0) AS inflow_agi,
                    COALESCE(o.outflow_agi, 0) AS outflow_agi,
                    (COALESCE(i.inflow_agi, 0) - COALESCE(o.outflow_agi, 0)) AS net_agi,
                    COALESCE(i.inflow_returns, 0) AS inflow_returns,
                    COALESCE(o.outflow_returns, 0) AS outflow_returns,
                    COALESCE(md.total_medspas, 0) AS total_medspas,
                    COALESCE(md.a_grade_count, 0) AS a_grade_count,
                    CASE WHEN COALESCE(md.a_grade_count, 0) > 0
                        THEN (COALESCE(i.inflow_agi, 0) - COALESCE(o.outflow_agi, 0))
                             * 1.0 / md.a_grade_count
                        ELSE (COALESCE(i.inflow_agi, 0) - COALESCE(o.outflow_agi, 0)) * 1.0
                    END AS migration_alpha
                FROM inflows i
                FULL OUTER JOIN outflows o ON i.state_abbr = o.state_abbr
                LEFT JOIN medspa_density md
                    ON md.state = COALESCE(i.state_abbr, o.state_abbr)
                WHERE COALESCE(i.state_abbr, o.state_abbr) IS NOT NULL
                ORDER BY net_agi DESC
            """), params).fetchall()

            state_flows = [
                {
                    "state": r[0],
                    "inflow_agi_m": round(float(r[1] or 0) * 1000 / 1e6, 1),
                    "outflow_agi_m": round(float(r[2] or 0) * 1000 / 1e6, 1),
                    "net_agi_m": round(float(r[3] or 0) * 1000 / 1e6, 1),
                    "inflow_returns": int(r[4] or 0),
                    "outflow_returns": int(r[5] or 0),
                    "total_medspas": int(r[6] or 0),
                    "a_grade_count": int(r[7] or 0),
                    "migration_alpha": round(float(r[8] or 0), 1),
                }
                for r in state_flows_rows
            ]

            # --- Top county-level inflows ---
            county_rows = db.execute(text(f"""
                SELECT
                    m.dest_state_abbr,
                    m.dest_county_name,
                    m.orig_state_abbr,
                    SUM(m.total_agi) AS inflow_agi,
                    SUM(m.num_returns) AS inflow_returns
                FROM irs_soi_migration m
                WHERE m.tax_year = :year AND m.flow_type = 'inflow'
                    {state_clause_dest}
                GROUP BY m.dest_state_abbr, m.dest_county_name, m.orig_state_abbr
                ORDER BY inflow_agi DESC
                LIMIT 20
            """), params).fetchall()

            top_county_inflows = [
                {
                    "dest_state": r[0],
                    "county": r[1],
                    "orig_state": r[2],
                    "agi_m": round(float(r[3] or 0) * 1000 / 1e6, 1),
                    "returns": int(r[4] or 0),
                }
                for r in county_rows
            ]

            # --- Classify emerging markets ---
            emerging = [
                s for s in state_flows
                if s["net_agi_m"] > 0 and s["a_grade_count"] < 20
            ]
            emerging.sort(key=lambda x: x["migration_alpha"], reverse=True)

            # --- Summary stats ---
            total_net = sum(s["net_agi_m"] for s in state_flows)
            top_gainer = state_flows[0]["state"] if state_flows and state_flows[0]["net_agi_m"] > 0 else "-"
            top_loser = state_flows[-1]["state"] if state_flows and state_flows[-1]["net_agi_m"] < 0 else "-"

            # Wealth exodus: states losing the most
            wealth_exodus = [s for s in state_flows if s["net_agi_m"] < 0]
            wealth_exodus.sort(key=lambda x: x["net_agi_m"])

            return {
                "migration_alpha": {
                    "state_flows": state_flows,
                    "top_county_inflows": top_county_inflows,
                    "wealth_exodus": wealth_exodus[:10],
                    "emerging_markets": emerging[:15],
                    "summary": {
                        "total_net_agi_m": round(total_net, 1),
                        "top_gainer": top_gainer,
                        "top_loser": top_loser,
                        "emerging_count": len(emerging),
                        "tax_year": latest_year,
                    },
                }
            }
        except Exception as e:
            logger.warning(f"Could not compute migration alpha data: {e}")
            db.rollback()
            return {"migration_alpha": {}}

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

        # New sections (8-12) data
        whitespace_zips = data.get("whitespace_zips", [])
        whitespace_by_state = data.get("whitespace_by_state", [])
        whitespace_summary = data.get("whitespace_summary", {})
        bls_wages = data.get("bls_wages", {})
        bls_employment = data.get("bls_employment", {})
        category_breakdown = data.get("category_breakdown", [])
        pe_financials = data.get("pe_financials", [])
        pe_fin_summary = data.get("pe_financial_summary", {})
        top_by_reviews = data.get("top_by_reviews", [])
        review_buckets = data.get("review_buckets", [])
        low_competition_gems = data.get("low_competition_gems", [])

        # Deal model data (sections 13-15)
        deal_model = data.get("deal_model", {})

        # Sections 16-17 data
        stealth_wealth = data.get("stealth_wealth", {})
        migration_alpha = data.get("migration_alpha", {})

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
            {"number": 8, "id": "whitespace", "title": "Whitespace Analysis"},
            {"number": 9, "id": "workforce", "title": "Workforce Economics"},
            {"number": 10, "id": "categories", "title": "Service Category Breakdown"},
            {"number": 11, "id": "pe-benchmarks", "title": "PE Platform Benchmarking"},
            {"number": 12, "id": "growth-signals", "title": "Review Velocity & Growth Signals"},
            {"number": 13, "id": "deal-unit-econ", "title": "Deal Model \u2014 Unit Economics"},
            {"number": 14, "id": "deal-capital", "title": "Deal Model \u2014 Capital Requirements"},
            {"number": 15, "id": "deal-returns", "title": "Deal Model \u2014 Returns Analysis"},
            {"number": 16, "id": "stealth-wealth", "title": "Stealth Wealth Signal"},
            {"number": 17, "id": "migration-alpha", "title": "Migration Alpha"},
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
        if data_fresh.get("irs_soi_zip_income"):
            iz = data_fresh["irs_soi_zip_income"]
            freshness_rows.append([
                "IRS SOI ZIP Income (Stealth Wealth)",
                f"IRS SOI, tax years {iz.get('earliest', '?')}-{iz.get('latest', '?')}",
                _fmt(iz.get("total")),
                iz.get("latest", "-"),
            ])
        if data_fresh.get("irs_soi_migration"):
            im = data_fresh["irs_soi_migration"]
            freshness_rows.append([
                "IRS SOI Migration (Migration Alpha)",
                f"IRS SOI, tax years {im.get('earliest', '?')}-{im.get('latest', '?')}",
                _fmt(im.get("total")),
                im.get("latest", "-"),
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

        # ==================================================================
        # Section 8: Whitespace Analysis
        # ==================================================================
        body += "\n" + section_start(8, "Whitespace Analysis", "whitespace")
        body += '<p>Grade-A affluent ZIPs with zero discovered med-spa prospects — greenfield acquisition opportunities.</p>'

        ws_total = whitespace_summary.get("total_a_zips", 0)
        ws_with = whitespace_summary.get("a_with_prospects", 0)
        ws_count = whitespace_summary.get("whitespace_count", 0)

        if ws_total > 0:
            # KPI cards
            body += '<div class="metric-grid">'
            body += f"""<div class="metric-card">
    <div class="metric-label">Total A-Grade ZIPs</div>
    <div class="metric-value">{_fmt(ws_total)}</div>
    <div class="metric-detail">ZIPs scoring 80+ on affluence model</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">A-Grade w/ Prospects</div>
    <div class="metric-value">{_fmt(ws_with)}</div>
    <div class="metric-detail">Already have discoverable med-spas</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">Whitespace ZIPs</div>
    <div class="metric-value" style="color:{GREEN}">{_fmt(ws_count)}</div>
    <div class="metric-detail">Greenfield opportunity zones</div>
</div>"""
            body += "</div>"

            # Data table: top 25 whitespace ZIPs
            if whitespace_zips:
                ws_rows = []
                for z in whitespace_zips[:25]:
                    ws_rows.append([
                        z["zip_code"],
                        z.get("state") or "-",
                        f'{z["score"]:.1f}',
                        _fmt_currency(z["avg_agi"]),
                        _fmt(z["total_returns"]),
                        f'{z["affluence_score"]:.1f}',
                    ])
                body += data_table(
                    headers=["ZIP", "State", "Score", "Avg AGI", "Tax Returns", "Affluence Score"],
                    rows=ws_rows,
                    numeric_columns={2, 3, 4, 5},
                )

            # Horizontal bar: whitespace by state (top 15)
            if whitespace_by_state:
                ws_state_labels = [s["state"] for s in whitespace_by_state[:15]]
                ws_state_values = [float(s["count"]) for s in whitespace_by_state[:15]]
                ws_bar_config = build_horizontal_bar_config(
                    ws_state_labels, ws_state_values, dataset_label="Whitespace ZIPs"
                )
                ws_bar_json = json.dumps(ws_bar_config)
                ws_bar_height = f"{max(len(ws_state_labels) * 48 + 40, 200)}px"

                body += chart_container(
                    "whitespaceStateChart", ws_bar_json,
                    build_bar_fallback(ws_state_labels, ws_state_values),
                    title="Whitespace ZIPs by State (Top 15)",
                    height=ws_bar_height,
                )
                charts_js += chart_init_js("whitespaceStateChart", ws_bar_json)

            body += callout(
                f"<strong>Opportunity:</strong> {_fmt(ws_count)} high-affluence A-grade ZIPs have "
                f"no discoverable med-spa businesses — these represent greenfield targets for "
                f"de novo clinic launches or franchise expansion.",
                variant="good",
            )
        else:
            body += callout(
                "<strong>No whitespace data available.</strong> Run ZIP affluence scoring "
                "and med-spa discovery to populate this analysis.",
                variant="warn",
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 9: Workforce Economics
        # ==================================================================
        body += "\n" + section_start(9, "Workforce Economics", "workforce")
        body += '<p>BLS Occupational Employment & Wage Statistics for aesthetics-adjacent roles — labor cost trends that impact unit economics.</p>'

        if bls_wages:
            # Collect all years across all occupations
            all_years = sorted(set(yr for occ_data in bls_wages.values() for yr in occ_data.keys()))

            # Wage table: occupation × year matrix
            wage_rows = []
            for occ in sorted(bls_wages.keys()):
                yr_data = bls_wages[occ]
                row_vals = [occ]
                for yr in all_years:
                    val = yr_data.get(yr)
                    row_vals.append(_fmt_currency(val) if val else "-")
                # Growth column: first year → last year
                first_val = float(yr_data.get(all_years[0], 0) or 0) if all_years else 0
                last_val = float(yr_data.get(all_years[-1], 0) or 0) if all_years else 0
                if first_val > 0 and last_val > 0:
                    growth = (last_val - first_val) / first_val * 100
                    row_vals.append(f"{growth:+.1f}%")
                else:
                    row_vals.append("-")
                wage_rows.append(row_vals)

            wage_headers = ["Occupation"] + [str(y) for y in all_years] + ["Growth"]
            body += data_table(
                headers=wage_headers,
                rows=wage_rows,
                numeric_columns=set(range(1, len(wage_headers))),
            )

            # Horizontal bar: latest year wages per occupation
            if all_years:
                latest_yr = all_years[-1]
                bar_occs = []
                bar_wages = []
                for occ in sorted(bls_wages.keys()):
                    val = bls_wages[occ].get(latest_yr)
                    if val:
                        bar_occs.append(occ)
                        bar_wages.append(val)
                if bar_occs:
                    wage_bar_config = build_horizontal_bar_config(
                        bar_occs, bar_wages, dataset_label=f"Annual Mean Wage ({latest_yr})"
                    )
                    wage_bar_json = json.dumps(wage_bar_config)
                    wage_bar_height = f"{max(len(bar_occs) * 48 + 40, 200)}px"

                    body += chart_container(
                        "wageBarChart", wage_bar_json,
                        build_bar_fallback(bar_occs, [float(v) for v in bar_wages]),
                        title=f"Annual Mean Wage by Occupation ({latest_yr})",
                        height=wage_bar_height,
                    )
                    charts_js += chart_init_js("wageBarChart", wage_bar_json)

            # Compute insight for NPs and PAs
            np_growth = None
            pa_growth = None
            for occ in bls_wages:
                yr_data = bls_wages[occ]
                fv = float(yr_data.get(all_years[0], 0) or 0) if all_years else 0
                lv = float(yr_data.get(all_years[-1], 0) or 0) if all_years else 0
                if fv > 0 and lv > 0:
                    g = (lv - fv) / fv * 100
                    if "Nurse Practitioner" in occ:
                        np_growth = g
                    elif "Physician Assistant" in occ:
                        pa_growth = g

            insight_parts = []
            if np_growth is not None:
                insight_parts.append(f"Nurse Practitioners ({np_growth:+.1f}%)")
            if pa_growth is not None:
                insight_parts.append(f"Physician Assistants ({pa_growth:+.1f}%)")
            if insight_parts:
                body += callout(
                    f"<strong>Labor Cost Signal:</strong> Key mid-level providers — "
                    f"{' and '.join(insight_parts)} — show significant wage growth, "
                    f"signaling rising talent competition that impacts med-spa unit economics.",
                )
            else:
                body += callout(
                    "<strong>Labor Cost Signal:</strong> BLS wage data shows multi-year "
                    "trends in aesthetics-adjacent occupations. Monitor provider wages "
                    "as a key input to unit economics modeling.",
                )
        else:
            body += callout(
                "<strong>No BLS wage data available.</strong> Ingest BLS OES data to "
                "populate workforce economics analysis.",
                variant="warn",
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 10: Service Category Breakdown
        # ==================================================================
        body += "\n" + section_start(10, "Service Category Breakdown", "categories")
        body += '<p>Disaggregation of discovered prospects by Yelp business category — reveals service mix and niche positioning.</p>'

        if category_breakdown:
            total_cat_count = sum(c["count"] for c in category_breakdown)

            # Doughnut chart: top 8 categories
            top_cats = category_breakdown[:8]
            other_count = sum(c["count"] for c in category_breakdown[8:])
            cat_labels = [c["category"] for c in top_cats]
            cat_values = [float(c["count"]) for c in top_cats]
            if other_count > 0:
                cat_labels.append("Other")
                cat_values.append(float(other_count))

            cat_colors = list(CHART_COLORS[:len(cat_labels)])
            donut_config = build_doughnut_config(cat_labels, cat_values, cat_colors)
            donut_json = json.dumps(donut_config)

            body += '<div class="chart-row">'
            body += "<div>"
            body += chart_container(
                "categoryDonut", donut_json,
                build_bar_fallback(cat_labels, cat_values),
                size="medium",
                title="Category Distribution",
            )
            charts_js += chart_init_js("categoryDonut", donut_json)
            body += build_chart_legend(cat_labels, cat_values, cat_colors, show_pct=True)
            body += "</div>"
            body += "</div>"

            # Data table
            cat_rows = []
            for c in category_breakdown[:20]:
                pct = round(c["count"] / total_cat_count * 100, 1) if total_cat_count > 0 else 0
                cat_rows.append([
                    c["category"],
                    _fmt(c["count"]),
                    f"{pct}%",
                    f'{c["avg_score"]:.1f}',
                    f'{c["avg_rating"]:.1f}',
                    _fmt(c["avg_reviews"]),
                ])
            body += data_table(
                headers=["Category", "Count", "% Share", "Avg Score", "Avg Rating", "Avg Reviews"],
                rows=cat_rows,
                numeric_columns={1, 2, 3, 4, 5},
            )

            # Insight
            top_cat = category_breakdown[0] if category_breakdown else None
            if top_cat:
                body += callout(
                    f"<strong>Insight:</strong> <strong>{top_cat['category']}</strong> is the dominant "
                    f"category with {_fmt(top_cat['count'])} prospects "
                    f"({round(top_cat['count'] / total_cat_count * 100, 1)}% share). "
                    f"Niche categories may represent differentiated positioning or "
                    f"underserved sub-segments worth investigating.",
                )
        else:
            body += callout(
                "<strong>No category data available.</strong> Prospect categories are populated "
                "during med-spa discovery.",
                variant="warn",
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 11: PE Platform Benchmarking
        # ==================================================================
        body += "\n" + section_start(11, "PE Platform Benchmarking", "pe-benchmarks")
        body += '<p>Financial benchmarks for PE-backed aesthetics platforms — revenue, margins, and leverage metrics.</p>'

        if pe_financials:
            # Metric cards
            body += '<div class="metric-grid">'
            avg_em = pe_fin_summary.get("avg_ebitda_margin")
            avg_rg = pe_fin_summary.get("avg_rev_growth")
            med_de = pe_fin_summary.get("median_debt_ebitda")
            body += f"""<div class="metric-card">
    <div class="metric-label">Avg EBITDA Margin</div>
    <div class="metric-value">{f'{avg_em:.1f}%' if avg_em is not None else '-'}</div>
    <div class="metric-detail">Across PE-backed platforms</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">Avg Revenue Growth</div>
    <div class="metric-value">{f'{avg_rg:+.1f}%' if avg_rg is not None else '-'}</div>
    <div class="metric-detail">Year-over-year</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">Median Debt/EBITDA</div>
    <div class="metric-value">{f'{med_de:.1f}x' if med_de is not None else '-'}</div>
    <div class="metric-detail">Leverage benchmark</div>
</div>"""
            body += "</div>"

            # Data table
            pef_rows = []
            for f in pe_financials:
                rev_display = _fmt_currency(f["revenue"]) if f["revenue"] else "-"
                rg_display = f'{f["rev_growth"]:+.1f}%' if f["rev_growth"] is not None else "-"
                em_display = f'{f["ebitda_margin"]:.1f}%' if f["ebitda_margin"] is not None else "-"
                emp_display = _fmt(f["employees"]) if f["employees"] else "-"
                de_display = f'{f["debt_ebitda"]:.1f}x' if f["debt_ebitda"] is not None else "-"
                conf_badge = pill_badge(f["confidence"], "public" if f["confidence"] == "high" else "default")
                pef_rows.append([
                    f'<span class="company-name">{f["name"]}</span>',
                    f.get("pe_owner") or "-",
                    rev_display,
                    rg_display,
                    em_display,
                    emp_display,
                    de_display,
                    conf_badge,
                ])
            body += data_table(
                headers=["Company", "PE Owner", "Revenue", "Rev Growth", "EBITDA Margin",
                         "Employees", "Debt/EBITDA", "Confidence"],
                rows=pef_rows,
                numeric_columns={2, 3, 4, 5, 6},
            )

            # Revenue bar chart (top 8)
            rev_companies = [f for f in pe_financials if f["revenue"]][:8]
            if rev_companies:
                rev_labels = [f["name"] for f in rev_companies]
                rev_values = [float(f["revenue"]) for f in rev_companies]
                rev_bar_config = build_horizontal_bar_config(
                    rev_labels, rev_values, dataset_label="Revenue (USD)"
                )
                rev_bar_json = json.dumps(rev_bar_config)
                rev_bar_height = f"{max(len(rev_labels) * 48 + 40, 200)}px"

                body += chart_container(
                    "peRevenueChart", rev_bar_json,
                    build_bar_fallback(rev_labels, rev_values),
                    title="Revenue by Platform (Top 8)",
                    height=rev_bar_height,
                )
                charts_js += chart_init_js("peRevenueChart", rev_bar_json)

            body += callout(
                "<strong>Data Confidence Note:</strong> Financial data for private PE-backed "
                "platforms is estimated based on industry analysis, employee counts, and "
                "comparable transactions. Treat as directional benchmarks, not audited figures.",
                variant="warn",
            )
        else:
            body += callout(
                "<strong>No PE financial data available.</strong> Seed PE company financials "
                "to populate benchmarking analysis.",
                variant="warn",
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 12: Review Velocity & Growth Signals
        # ==================================================================
        body += "\n" + section_start(12, "Review Velocity & Growth Signals", "growth-signals")
        body += '<p>Demand momentum signals via review volume, competitive dynamics, and market positioning.</p>'

        if top_by_reviews:
            # Data table: top 25 by review volume
            rev_rows = []
            for t in top_by_reviews:
                grade_html = _grade_badge(t["grade"])
                rev_rows.append([
                    f'<span class="company-name">{t["name"]}</span>',
                    t.get("city") or "-",
                    t.get("state") or "-",
                    _fmt(t["reviews"]),
                    f'{t["rating"]:.1f}',
                    f'{t["score"]:.0f}',
                    grade_html,
                    str(t["competitors"]),
                ])
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Top 25 by Review Volume (A/B Grade)</h3>'
            body += data_table(
                headers=["Name", "City", "State", "Reviews", "Rating", "Score", "Grade", "Competitors"],
                rows=rev_rows,
                numeric_columns={3, 4, 5, 7},
            )

        # Review distribution chart
        if review_buckets:
            bucket_labels = [b["bucket"] for b in review_buckets]
            bucket_values = [float(b["count"]) for b in review_buckets]
            bucket_config = {
                "type": "bar",
                "data": {
                    "labels": bucket_labels,
                    "datasets": [{
                        "label": "Prospects",
                        "data": bucket_values,
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
                        "x": {"grid": {"display": False}, "ticks": {"color": "#4a5568"}},
                        "y": {"grid": {"color": "#edf2f7"}, "ticks": {"color": "#4a5568"}, "beginAtZero": True},
                    },
                },
            }
            bucket_json = json.dumps(bucket_config)

            body += chart_container(
                "reviewBucketChart", bucket_json,
                build_bar_fallback(bucket_labels, bucket_values),
                title="Review Volume Distribution",
            )
            charts_js += chart_init_js("reviewBucketChart", bucket_json)

        # Low competition gems
        if low_competition_gems:
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Low Competition Gems (A/B Grade, \u22643 Competitors)</h3>'
            gem_rows = []
            for g in low_competition_gems:
                grade_html = _grade_badge(g["grade"])
                gem_rows.append([
                    f'<span class="company-name">{g["name"]}</span>',
                    g.get("city") or "-",
                    g.get("state") or "-",
                    f'{g["score"]:.0f}',
                    grade_html,
                    f'{g["rating"]:.1f}',
                    _fmt(g["reviews"]),
                    str(g["competitors"]),
                ])
            body += data_table(
                headers=["Name", "City", "State", "Score", "Grade", "Rating", "Reviews", "Competitors"],
                rows=gem_rows,
                numeric_columns={3, 5, 6, 7},
            )

        # Insight callout
        high_review_count = sum(1 for t in top_by_reviews if t["reviews"] >= 200)
        gem_count = len(low_competition_gems)
        if top_by_reviews or low_competition_gems:
            body += callout(
                f"<strong>Demand Signals:</strong> {high_review_count} A/B-grade prospects have 200+ "
                f"reviews, indicating established consumer demand and brand recognition. "
                f"{gem_count} prospects are A/B-grade with 3 or fewer competitors in their ZIP — "
                f"prime bolt-on acquisition targets with limited competitive pressure.",
                variant="good",
            )
        else:
            body += callout(
                "<strong>No growth signal data available.</strong> Run med-spa discovery "
                "to populate review and competition analysis.",
                variant="warn",
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 13: Deal Model — Unit Economics
        # ==================================================================
        body += "\n" + section_start(13, "Deal Model \u2014 Unit Economics", "deal-unit-econ")
        body += '<p>Estimated portfolio economics for all A-grade acquisition targets using industry benchmark revenue and margins by Yelp price tier.</p>'

        dm_tiers = deal_model.get("tier_economics", [])
        dm_total_loc = deal_model.get("total_locations", 0)
        dm_total_rev = deal_model.get("total_revenue", 0)
        dm_total_ebitda = deal_model.get("total_ebitda", 0)
        dm_w_margin = deal_model.get("weighted_margin", 0)
        dm_pnl = deal_model.get("pnl_waterfall", {})

        if dm_tiers:
            # KPI cards
            body += '<div class="metric-grid">'
            body += f"""<div class="metric-card">
    <div class="metric-label">Total A-Grade Locations</div>
    <div class="metric-value">{_fmt(dm_total_loc)}</div>
    <div class="metric-detail">Across all price tiers</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">Est. Portfolio Revenue</div>
    <div class="metric-value">{_fmt_currency(dm_total_rev)}</div>
    <div class="metric-detail">Based on industry benchmarks</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">Est. Portfolio EBITDA</div>
    <div class="metric-value">{_fmt_currency(dm_total_ebitda)}</div>
    <div class="metric-detail">Weighted avg margin: {dm_w_margin:.1%}</div>
</div>"""
            body += "</div>"

            # Price tier distribution table
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Revenue Estimates by Price Tier</h3>'

            tier_rows = []
            for t in dm_tiers:
                tier_rows.append([
                    t["tier"],
                    _fmt(t["count"]),
                    _fmt_currency(t["avg_revenue"]),
                    f'{t["ebitda_margin"]:.0%}',
                    _fmt_currency(t["avg_ebitda"]),
                    f'{t["entry_multiple"]:.1f}x',
                    _fmt_currency(t["total_revenue"]),
                    _fmt_currency(t["total_ebitda"]),
                ])
            # Totals row
            tier_rows.append([
                "<strong>Total</strong>",
                f'<strong>{_fmt(dm_total_loc)}</strong>',
                "-",
                f'<strong>{dm_w_margin:.0%}</strong>',
                "-",
                "-",
                f'<strong>{_fmt_currency(dm_total_rev)}</strong>',
                f'<strong>{_fmt_currency(dm_total_ebitda)}</strong>',
            ])
            body += data_table(
                headers=["Price Tier", "Locations", "Avg Revenue", "EBITDA Margin",
                         "Avg EBITDA", "Entry Multiple", "Total Revenue", "Total EBITDA"],
                rows=tier_rows,
                numeric_columns={1, 2, 3, 4, 5, 6, 7},
            )

            # Price tier doughnut chart
            if len(dm_tiers) > 1:
                tier_labels = [t["tier"] for t in dm_tiers]
                tier_values = [float(t["count"]) for t in dm_tiers]
                tier_colors = [BLUE, GREEN, ORANGE, PURPLE, GRAY][:len(tier_labels)]
                tier_donut_config = build_doughnut_config(tier_labels, tier_values, tier_colors)
                tier_donut_json = json.dumps(tier_donut_config)

                body += '<div class="chart-row">'
                body += "<div>"
                body += chart_container(
                    "tierDonut", tier_donut_json,
                    build_bar_fallback(tier_labels, tier_values),
                    size="medium",
                    title="A-Grade Locations by Price Tier",
                )
                charts_js += chart_init_js("tierDonut", tier_donut_json)
                body += build_chart_legend(tier_labels, tier_values, tier_colors, show_pct=True)
                body += "</div>"
                body += "</div>"

            # P&L waterfall (per average location)
            if dm_pnl.get("revenue", 0) > 0:
                body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Per-Location P&L Waterfall (Portfolio Average)</h3>'

                pnl_rev = dm_pnl["revenue"]
                pnl_items = [
                    ("Revenue", pnl_rev, BLUE),
                    ("COGS (40%)", dm_pnl["cogs"], RED),
                    ("Gross Profit", dm_pnl["gross_profit"], GREEN),
                    ("SG&A (32%)", dm_pnl["sga"], ORANGE),
                    ("EBITDA", dm_pnl["ebitda"], TEAL),
                ]
                max_val = max(v for _, v, _ in pnl_items) if pnl_items else 1

                body += '<div class="pnl-waterfall">'
                for label, value, color in pnl_items:
                    bar_h = max(int(value / max_val * 150), 8) if max_val > 0 else 8
                    body += f"""<div class="waterfall-bar">
    <div class="bar-value">{_fmt_currency(value)}</div>
    <div class="bar" style="height:{bar_h}px;background:{color}"></div>
    <div class="bar-label">{label}</div>
</div>"""
                body += "</div>"

            body += callout(
                "<strong>Methodology:</strong> Revenue and margin estimates are based on AmSpa "
                "State of the Industry Report, IBISWorld Medspa Industry Analysis, and PE deal "
                "comps benchmarks. Actual financials will vary by location — these are planning estimates.",
                variant="warn",
            )
        else:
            body += callout(
                "<strong>No A-grade targets found.</strong> Run med-spa discovery and scoring "
                "to populate the deal model.",
                variant="warn",
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 14: Deal Model — Capital Requirements
        # ==================================================================
        body += "\n" + section_start(14, "Deal Model \u2014 Capital Requirements", "deal-capital")
        body += '<p>Total capital needed to acquire all A-grade targets, including financing structure, transaction costs, and working capital reserves.</p>'

        dm_acq_cost = deal_model.get("total_acquisition_cost", 0)
        dm_cap = deal_model.get("capital_stack", {})
        dm_leverage = deal_model.get("leverage_ratio", 0)

        if dm_acq_cost > 0:
            # Acquisition cost by tier
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Acquisition Cost by Price Tier</h3>'

            acq_rows = []
            for t in dm_tiers:
                acq_rows.append([
                    t["tier"],
                    _fmt(t["count"]),
                    _fmt_currency(t["avg_ebitda"]),
                    f'{t["entry_multiple"]:.1f}x',
                    _fmt_currency(t["total_acq_cost"]),
                ])
            acq_rows.append([
                "<strong>Total</strong>",
                f'<strong>{_fmt(dm_total_loc)}</strong>',
                "-",
                "-",
                f'<strong>{_fmt_currency(dm_acq_cost)}</strong>',
            ])
            body += data_table(
                headers=["Price Tier", "Locations", "Avg EBITDA", "Entry Multiple", "Total Acquisition Cost"],
                rows=acq_rows,
                numeric_columns={1, 2, 3, 4},
            )

            # Capital stack KPIs
            equity = dm_cap.get("equity", 0)
            debt = dm_cap.get("debt", 0)
            txn_costs = dm_cap.get("transaction_costs", 0)
            wc = dm_cap.get("working_capital", 0)
            total_cap = dm_cap.get("total_capital_required", 0)

            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Capital Stack</h3>'
            body += '<div class="metric-grid">'
            body += f"""<div class="metric-card">
    <div class="metric-label">Total Acquisition Cost</div>
    <div class="metric-value">{_fmt_currency(dm_acq_cost)}</div>
    <div class="metric-detail">{_fmt(dm_total_loc)} locations × avg EBITDA × entry multiple</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">Equity Check (40%)</div>
    <div class="metric-value">{_fmt_currency(equity)}</div>
    <div class="metric-detail">Sponsor equity contribution</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">Senior Debt (60%)</div>
    <div class="metric-value">{_fmt_currency(debt)}</div>
    <div class="metric-detail">Leverage: {dm_leverage:.1f}x Debt/EBITDA</div>
</div>"""
            body += f"""<div class="metric-card">
    <div class="metric-label">Total Capital Required</div>
    <div class="metric-value" style="color:{GREEN}">{_fmt_currency(total_cap)}</div>
    <div class="metric-detail">Equity + transaction costs + working capital</div>
</div>"""
            body += "</div>"

            # Detailed capital table
            cap_rows = [
                ["Equity Contribution (40%)", _fmt_currency(equity)],
                ["Senior Debt (60%)", _fmt_currency(debt)],
                ["Transaction Costs (5%)", _fmt_currency(txn_costs)],
                [f"Working Capital ({DEAL_ASSUMPTIONS['working_capital_months']}mo SG&A)", _fmt_currency(wc)],
                ["<strong>Total Capital Required</strong>", f'<strong>{_fmt_currency(total_cap)}</strong>'],
            ]
            body += data_table(
                headers=["Component", "Amount"],
                rows=cap_rows,
                numeric_columns={1},
            )

            # Capital stack visualization (horizontal bar)
            cap_items = [
                ("Equity", equity, BLUE),
                ("Senior Debt", debt, TEAL),
                ("Transaction Costs", txn_costs, ORANGE),
                ("Working Capital", wc, GRAY),
            ]
            cap_total = sum(v for _, v, _ in cap_items)

            if cap_total > 0:
                body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Capital Stack Breakdown</h3>'
                body += '<div class="capital-stack">'
                for label, value, color in cap_items:
                    pct = value / cap_total * 100
                    if pct >= 3:  # Only show label if segment is wide enough
                        body += f'<div class="stack-segment" style="width:{pct:.1f}%;background:{color}" title="{label}: {_fmt_currency(value)}">{label} {_fmt_currency(value)}</div>'
                    elif pct > 0:
                        body += f'<div class="stack-segment" style="width:{pct:.1f}%;background:{color}" title="{label}: {_fmt_currency(value)}"></div>'
                body += '</div>'

                # Chart.js horizontal bar version
                cap_labels = [c[0] for c in cap_items]
                cap_values = [c[1] for c in cap_items]
                cap_bar_config = build_horizontal_bar_config(
                    cap_labels, cap_values, dataset_label="Capital ($)"
                )
                cap_bar_json = json.dumps(cap_bar_config)

                body += chart_container(
                    "capitalStackChart", cap_bar_json,
                    build_bar_fallback(cap_labels, cap_values),
                    title="Capital Stack Components",
                    height="280px",
                )
                charts_js += chart_init_js("capitalStackChart", cap_bar_json)

            # Leverage check callout
            if dm_leverage > 4:
                body += callout(
                    f"<strong>Leverage Warning:</strong> At {dm_leverage:.1f}x Debt/EBITDA, the portfolio "
                    f"exceeds the typical 4.0x leverage threshold. Consider reducing the debt percentage "
                    f"or phasing acquisitions to maintain healthy coverage ratios.",
                    variant="warn",
                )
            else:
                body += callout(
                    f"<strong>Leverage Check:</strong> At {dm_leverage:.1f}x Debt/EBITDA, the portfolio "
                    f"is within the typical 4.0x leverage comfort zone for PE-backed healthcare roll-ups.",
                    variant="good",
                )
        else:
            body += callout(
                "<strong>No acquisition cost data available.</strong> "
                "Deal model requires A-grade targets with price tier data.",
                variant="warn",
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 15: Deal Model — Returns Analysis
        # ==================================================================
        body += "\n" + section_start(15, "Deal Model \u2014 Returns Analysis", "deal-returns")
        body += '<p>Projected returns across three scenarios with varying exit multiples, margin improvement, and hold periods.</p>'

        dm_scenarios = deal_model.get("scenarios", {})
        dm_a_states = deal_model.get("a_grade_states", [])

        if dm_scenarios:
            # Scenario cards
            body += '<div class="deal-scenario-grid">'
            for scenario_name in ["conservative", "base", "aggressive"]:
                s = dm_scenarios.get(scenario_name, {})
                if not s:
                    continue
                label_map = {"conservative": "Conservative", "base": "Base Case", "aggressive": "Aggressive"}
                body += f'<div class="scenario-card {scenario_name}">'
                body += f'<div class="scenario-label">{label_map.get(scenario_name, scenario_name)}</div>'
                metrics = [
                    ("Entry EV", _fmt_currency(s.get("entry_ev", 0))),
                    ("Exit Multiple", f'{s.get("exit_multiple", 0)}x'),
                    ("EBITDA Improvement", f'{s.get("margin_improvement", 0):.0%}/yr'),
                    ("Hold Period", f'{s.get("hold_years", 0)} years'),
                    ("Exit EV", _fmt_currency(s.get("exit_ev", 0))),
                    ("Gross MOIC", f'{s.get("gross_moic", 0):.1f}x'),
                    ("Net IRR", f'{s.get("net_irr", 0):.1%}'),
                ]
                for label, value in metrics:
                    body += f"""<div class="scenario-metric">
    <span class="label">{label}</span>
    <span class="value">{value}</span>
</div>"""
                body += "</div>"
            body += "</div>"

            # Scenario comparison table
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Scenario Comparison</h3>'

            scenario_rows = []
            for scenario_name in ["conservative", "base", "aggressive"]:
                s = dm_scenarios.get(scenario_name, {})
                if not s:
                    continue
                label_map = {"conservative": "Conservative", "base": "Base Case", "aggressive": "Aggressive"}
                scenario_rows.append([
                    f'<strong>{label_map.get(scenario_name)}</strong>',
                    _fmt_currency(s.get("entry_ev", 0)),
                    f'{s.get("exit_multiple", 0)}x',
                    f'{s.get("margin_improvement", 0):.0%}/yr',
                    f'{s.get("hold_years", 0)} yrs',
                    _fmt_currency(s.get("exit_ev", 0)),
                    f'{s.get("gross_moic", 0):.1f}x',
                    f'{s.get("net_irr", 0):.1%}',
                ])

            body += data_table(
                headers=["Scenario", "Entry EV", "Exit Multiple", "EBITDA Improvement",
                         "Hold Period", "Exit EV", "Gross MOIC", "Net IRR"],
                rows=scenario_rows,
                numeric_columns={1, 2, 3, 4, 5, 6, 7},
            )

            # Synergy callout
            body += callout(
                "<strong>Synergy Upside (Not Modeled):</strong> Procurement savings (15-25% on "
                "injectables/consumables), shared marketing infrastructure, centralized scheduling "
                "& billing technology platform, and provider network effects could add 300-500bps "
                "of additional margin improvement beyond baseline projections.",
                variant="good",
            )

            # Phased rollout strategy
            body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:16px 0 8px">Phased Rollout Strategy</h3>'
            body += """<div class="thesis-box">
<h3>Recommended Acquisition Cadence</h3>
<ul>
    <li><strong>Year 1 — Platform Build (Top 100):</strong> Acquire highest-scoring A-grade targets in top 5 states. Establish shared services infrastructure, negotiate vendor contracts, deploy tech platform.</li>
    <li><strong>Year 2 — Scale (+200):</strong> Expand to 300 total locations. Leverage platform economics for procurement savings. Begin cross-selling across locations.</li>
    <li><strong>Years 3-5 — Full Portfolio:</strong> Complete remaining ~{0} acquisitions. Drive margin improvement through operational integration. Prepare for exit.</li>
</ul>
</div>""".format(max(dm_total_loc - 300, 0))

            # Exit value by state doughnut
            if dm_a_states and dm_scenarios.get("base"):
                base_exit_ev = dm_scenarios["base"].get("exit_ev", 0)
                total_a = sum(s["count"] for s in dm_a_states)

                if total_a > 0 and base_exit_ev > 0:
                    top_exit_states = dm_a_states[:10]
                    other_count = sum(s["count"] for s in dm_a_states[10:])

                    exit_labels = [s["state"] for s in top_exit_states]
                    exit_values = [round(s["count"] / total_a * base_exit_ev / 1_000_000, 1) for s in top_exit_states]
                    if other_count > 0:
                        exit_labels.append("Other")
                        exit_values.append(round(other_count / total_a * base_exit_ev / 1_000_000, 1))

                    exit_colors = list(CHART_COLORS[:len(exit_labels)])
                    exit_donut_config = build_doughnut_config(exit_labels, exit_values, exit_colors)
                    exit_donut_json = json.dumps(exit_donut_config)

                    body += '<div class="chart-row">'
                    body += "<div>"
                    body += chart_container(
                        "exitValueDonut", exit_donut_json,
                        build_bar_fallback(exit_labels, exit_values),
                        size="medium",
                        title="Base Case Exit Value by State ($M)",
                    )
                    charts_js += chart_init_js("exitValueDonut", exit_donut_json)
                    body += build_chart_legend(exit_labels, exit_values, exit_colors, show_pct=True)
                    body += "</div>"
                    body += "</div>"
        else:
            body += callout(
                "<strong>No scenario data available.</strong> Deal model requires A-grade "
                "targets to generate returns analysis.",
                variant="warn",
            )

        body += "\n" + section_end()

        # ==================================================================
        # Section 16: Stealth Wealth Signal
        # ==================================================================
        body += "\n" + section_start(16, "Stealth Wealth Signal", "stealth-wealth")

        sw_summary = stealth_wealth.get("summary", {})
        sw_zips = stealth_wealth.get("stealth_zips", [])
        sw_comp = stealth_wealth.get("wealth_composition", {})
        sw_states = stealth_wealth.get("top_states", [])

        if not stealth_wealth:
            body += callout(
                "<strong>IRS SOI ZIP income data not yet ingested.</strong> "
                "Run <code>POST /api/v1/irs-soi/zip-income/ingest</code> to enable "
                "stealth wealth analysis. This cross-references non-wage income "
                "(capital gains, dividends, partnership income) with medspa scores "
                "to reveal hidden demand in affluent ZIPs.",
                variant="info",
            )
        else:
            # KPI cards
            sw_cards = ""
            sw_cards += kpi_card("ZIPs Analyzed", _fmt(sw_comp.get("total_returns")))
            sw_cards += kpi_card(
                "Stealth ZIPs Found",
                _fmt(sw_summary.get("total_stealth")),
            )
            sw_cards += kpi_card(
                "Validated (High Score)",
                _fmt(sw_summary.get("validated")),
            )
            sw_cards += kpi_card(
                "Avg Non-Wage Income",
                _fmt_currency(sw_summary.get("avg_non_wage_income", 0)),
            )
            body += kpi_strip(sw_cards)

            body += f"""<p style="margin:12px 0;font-size:14px;color:var(--gray-600)">
Stealth wealth ZIPs have <strong>&gt;25% non-wage income</strong> (capital gains + dividends +
partnership income) but a medspa score below 70 — indicating affluent populations
underserved by current med-spa supply. Tax year: <strong>{sw_summary.get('tax_year', '?')}</strong>.</p>"""

            # Wealth composition doughnut + explanation
            if sw_comp:
                body += '<div class="wealth-composition-grid">'

                # Doughnut chart
                comp_labels = ["W-2 Wages", "Capital Gains", "Dividends", "Business/Partnership", "Other"]
                comp_values = [
                    sw_comp.get("wages_pct", 0),
                    sw_comp.get("cap_gains_pct", 0),
                    sw_comp.get("dividends_pct", 0),
                    sw_comp.get("biz_income_pct", 0),
                    sw_comp.get("other_pct", 0),
                ]
                comp_colors = [BLUE, GREEN, ORANGE, PURPLE, GRAY]
                comp_config = build_doughnut_config(comp_labels, comp_values, comp_colors)
                comp_json = json.dumps(comp_config)

                body += "<div>"
                body += chart_container(
                    "wealthCompDonut", comp_json,
                    build_bar_fallback(comp_labels, comp_values),
                    size="medium",
                    title="Income Composition (% of AGI)",
                )
                charts_js += chart_init_js("wealthCompDonut", comp_json)
                body += build_chart_legend(comp_labels, comp_values, comp_colors, show_pct=True)
                body += "</div>"

                # Explanation card
                body += """<div>
<div class="thesis-box">
    <h3>Why Non-Wage Income Matters</h3>
    <ul>
        <li><strong>Capital gains &amp; dividends</strong> indicate investable wealth — these households have assets producing returns, not just paychecks.</li>
        <li><strong>Partnership/business income</strong> signals entrepreneurs and professionals with high discretionary spend.</li>
        <li>Traditional models use W-2 wages or median household income, <strong>missing 25-40% of actual purchasing power</strong> in wealthy ZIPs.</li>
        <li>Stealth wealth ZIPs are undervalued by competitors using standard data — a <strong>first-mover advantage</strong> for informed acquirers.</li>
    </ul>
</div>
</div>"""
                body += "</div>"  # close wealth-composition-grid

            # Stealth ZIPs data table
            if sw_zips:
                body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:20px 0 8px">Top Stealth Wealth ZIPs</h3>'
                sw_rows = []
                for z in sw_zips[:25]:
                    sw_rows.append([
                        z["zip_code"],
                        z["state"],
                        _fmt_currency(z["non_wage_per_return"]),
                        f"{z['non_wage_pct']:.1f}%",
                        _fmt(z["num_returns"]),
                        _fmt_currency(z["avg_agi"]),
                        str(z["medspa_score"]),
                        z["medspa_grade"],
                        str(z["a_grade_medspas"]),
                    ])
                body += data_table(
                    headers=[
                        "ZIP", "State", "Non-Wage/Return", "Non-Wage %",
                        "Returns", "Avg AGI", "Medspa Score", "Grade", "A-Grade Medspas",
                    ],
                    rows=sw_rows,
                )

            # States bar chart
            if sw_states:
                st_labels = [s["state"] for s in sw_states[:12]]
                st_values = [s["count"] for s in sw_states[:12]]
                st_colors = [BLUE] * len(st_labels)
                st_config = build_horizontal_bar_config(
                    st_labels, st_values, st_colors,
                    dataset_label="Stealth ZIPs",
                )
                st_json = json.dumps(st_config)

                body += chart_container(
                    "stealthByState", st_json,
                    build_bar_fallback(st_labels, st_values),
                    size="large",
                    title="Stealth Wealth ZIPs by State",
                )
                charts_js += chart_init_js("stealthByState", st_json)

            # Methodology callout
            body += callout(
                "<strong>Methodology:</strong> ZIPs are flagged as \"stealth\" when &gt;25% of total AGI "
                "comes from non-wage sources (capital gains + dividends + business income) AND the "
                "current medspa acquisition score is below 70. This identifies affluent areas where "
                "traditional income-based models underestimate demand. Minimum 100 tax returns per ZIP "
                "to ensure statistical significance.",
                variant="info",
            )

            # Opportunity callout
            if sw_zips:
                body += callout(
                    f"<strong>Opportunity:</strong> {sw_summary.get('total_stealth', 0)} stealth wealth ZIPs "
                    f"identified with an average non-wage income of "
                    f"{_fmt_currency(sw_summary.get('avg_non_wage_income', 0))} per return. "
                    f"These ZIPs represent a first-mover acquisition opportunity invisible to "
                    f"competitors using standard income data.",
                    variant="tip",
                )

        body += "\n" + section_end()

        # ==================================================================
        # Section 17: Migration Alpha
        # ==================================================================
        body += "\n" + section_start(17, "Migration Alpha", "migration-alpha")

        ma_summary = migration_alpha.get("summary", {})
        ma_flows = migration_alpha.get("state_flows", [])
        ma_emerging = migration_alpha.get("emerging_markets", [])
        ma_counties = migration_alpha.get("top_county_inflows", [])
        ma_exodus = migration_alpha.get("wealth_exodus", [])

        if not migration_alpha:
            body += callout(
                "<strong>IRS SOI migration data not yet ingested.</strong> "
                "Run <code>POST /api/v1/irs-soi/migration/ingest</code> to enable "
                "migration alpha analysis. This uses county-to-county wealth flows "
                "as a 2-3 year leading indicator of medspa demand.",
                variant="info",
            )
        else:
            # KPI cards
            ma_cards = ""
            ma_cards += kpi_card(
                "Net Wealth Movement",
                f"${abs(ma_summary.get('total_net_agi_m', 0)):,.0f}M",
            )
            ma_cards += kpi_card("Top Gainer", ma_summary.get("top_gainer", "-"))
            ma_cards += kpi_card("Top Loser", ma_summary.get("top_loser", "-"))
            ma_cards += kpi_card(
                "Emerging Markets",
                _fmt(ma_summary.get("emerging_count")),
            )
            body += kpi_strip(ma_cards)

            body += f"""<p style="margin:12px 0;font-size:14px;color:var(--gray-600)">
Migration Alpha measures <strong>net wealth inflows</strong> relative to existing medspa density.
States receiving large AGI inflows but with few A-grade medspas represent a
<strong>2-3 year leading indicator</strong> of unmet demand. Tax year: <strong>{ma_summary.get('tax_year', '?')}</strong>.</p>"""

            # Dual-color horizontal bar: top 15 net flows
            if ma_flows:
                top_flow_states = ma_flows[:10]
                bottom_flow_states = sorted(ma_flows, key=lambda x: x["net_agi_m"])[:5]
                bar_states = top_flow_states + [s for s in bottom_flow_states if s not in top_flow_states]
                bar_states.sort(key=lambda x: x["net_agi_m"], reverse=True)

                bar_labels = [s["state"] for s in bar_states]
                bar_values = [s["net_agi_m"] for s in bar_states]
                bar_colors = ["#48bb78" if v >= 0 else "#fc8181" for v in bar_values]
                bar_config = build_horizontal_bar_config(
                    bar_labels, bar_values, bar_colors,
                    dataset_label="Net AGI ($M)",
                )
                bar_json = json.dumps(bar_config)

                body += chart_container(
                    "migrationNetFlow", bar_json,
                    build_bar_fallback(bar_labels, bar_values),
                    size="large",
                    title="Net Wealth Migration by State ($M AGI)",
                )
                charts_js += chart_init_js("migrationNetFlow", bar_json)

            # State flows table
            if ma_flows:
                body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:20px 0 8px">State Wealth Flows</h3>'
                flow_rows = []
                for s in ma_flows[:20]:
                    net_color = "color:#48bb78" if s["net_agi_m"] >= 0 else "color:#fc8181"
                    flow_rows.append([
                        s["state"],
                        f"${s['inflow_agi_m']:,.1f}M",
                        f"${s['outflow_agi_m']:,.1f}M",
                        f'<span style="{net_color};font-weight:600">${s["net_agi_m"]:+,.1f}M</span>',
                        _fmt(s["inflow_returns"]),
                        _fmt(s["total_medspas"]),
                        _fmt(s["a_grade_count"]),
                        f"{s['migration_alpha']:,.0f}",
                    ])
                body += data_table(
                    headers=[
                        "State", "Inflow AGI", "Outflow AGI", "Net AGI",
                        "Inflow Returns", "Medspas", "A-Grade", "Migration Alpha",
                    ],
                    rows=flow_rows,
                )

            # Emerging markets highlight table
            if ma_emerging:
                body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:20px 0 8px">Emerging Markets (Positive Inflow, &lt;20 A-Grade Medspas)</h3>'
                em_rows = []
                for s in ma_emerging[:10]:
                    em_rows.append([
                        f'<strong>{s["state"]}</strong>',
                        f"${s['net_agi_m']:+,.1f}M",
                        _fmt(s["inflow_returns"]),
                        _fmt(s["total_medspas"]),
                        _fmt(s["a_grade_count"]),
                        f"{s['migration_alpha']:,.0f}",
                    ])
                body += '<div class="highlight-table">'
                body += data_table(
                    headers=[
                        "State", "Net AGI", "Inflow Returns",
                        "Total Medspas", "A-Grade", "Migration Alpha",
                    ],
                    rows=em_rows,
                )
                body += "</div>"

                body += callout(
                    f"<strong>{len(ma_emerging)} emerging markets identified.</strong> "
                    "These states are receiving net wealth inflows but have fewer than 20 "
                    "A-grade medspa targets — signaling greenfield opportunity before "
                    "competitors recognize the demand shift.",
                    variant="tip",
                )

            # County drill-down
            if ma_counties:
                body += '<h3 style="font-size:15px;font-weight:600;color:var(--primary);margin:20px 0 8px">Top County-Level Inflows</h3>'
                county_rows = []
                for c in ma_counties[:15]:
                    county_rows.append([
                        c["dest_state"],
                        c["county"],
                        c["orig_state"],
                        f"${c['agi_m']:,.1f}M",
                        _fmt(c["returns"]),
                    ])
                body += data_table(
                    headers=["Dest State", "County", "Origin State", "AGI Inflow", "Returns"],
                    rows=county_rows,
                )

            # Leading indicator callout
            body += callout(
                "<strong>Leading Indicator:</strong> Wealth migration is a 2-3 year "
                "leading indicator of local service demand. High-AGI movers "
                "typically establish primary residence before seeking premium "
                "services like med-spas. States with high migration alpha today "
                "will see increased medspa demand in 2-3 years.",
                variant="info",
            )

        body += "\n" + section_end()

        # ---- Close container ----
        body += "\n</div>"

        # ---- Footer ----
        notes = [
            "Prospect data sourced from Yelp Business Search API; scores reflect publicly available ratings and review counts.",
            "ZIP affluence model uses IRS Statistics of Income (SOI) ZIP-level data, tax year 2021.",
            "PE competitive landscape data from SEC EDGAR filings and company websites.",
            "Acquisition scores are model-generated estimates and should be validated with on-the-ground diligence.",
            "Deal model uses industry benchmark economics (AmSpa, IBISWorld) — actual financials require location-level diligence.",
            "Stealth Wealth Signal uses IRS SOI ZIP-level income composition; dollar amounts reported in thousands.",
            "Migration Alpha uses IRS SOI county-to-county migration flows as a 2-3 year leading indicator.",
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

        # ---- Sheet 7: Whitespace ZIPs ----
        ws_ws = wb.create_sheet("Whitespace ZIPs")
        whitespace_zips = data.get("whitespace_zips", [])

        headers = ["ZIP", "State", "Score", "Avg AGI", "Tax Returns", "Affluence Score"]
        for col, header in enumerate(headers, 1):
            cell = ws_ws.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, z in enumerate(whitespace_zips, 2):
            ws_ws.cell(row=i, column=1, value=z.get("zip_code"))
            ws_ws.cell(row=i, column=2, value=z.get("state"))
            ws_ws.cell(row=i, column=3, value=z.get("score"))
            ws_ws.cell(row=i, column=4, value=z.get("avg_agi"))
            ws_ws.cell(row=i, column=5, value=z.get("total_returns"))
            ws_ws.cell(row=i, column=6, value=z.get("affluence_score"))

        for col_letter in ["A", "B", "C", "D", "E", "F"]:
            ws_ws.column_dimensions[col_letter].width = 16

        # ---- Sheet 8: Workforce Wages ----
        ws_wage = wb.create_sheet("Workforce Wages")
        bls_wages = data.get("bls_wages", {})

        if bls_wages:
            all_years = sorted(set(yr for occ_data in bls_wages.values() for yr in occ_data.keys()))
            wage_headers = ["Occupation"] + [str(y) for y in all_years]
            for col, header in enumerate(wage_headers, 1):
                cell = ws_wage.cell(row=1, column=col, value=header)
                cell.font = header_font
                cell.fill = header_fill

            for i, occ in enumerate(sorted(bls_wages.keys()), 2):
                ws_wage.cell(row=i, column=1, value=occ)
                for j, yr in enumerate(all_years, 2):
                    val = bls_wages[occ].get(yr)
                    ws_wage.cell(row=i, column=j, value=val)

            ws_wage.column_dimensions["A"].width = 25
            for j in range(2, len(all_years) + 2):
                from openpyxl.utils import get_column_letter
                ws_wage.column_dimensions[get_column_letter(j)].width = 14

        # ---- Sheet 9: Growth Signals ----
        ws_gs = wb.create_sheet("Growth Signals")
        top_by_reviews = data.get("top_by_reviews", [])
        low_competition_gems = data.get("low_competition_gems", [])

        # Top by reviews
        ws_gs["A1"] = "Top Prospects by Review Volume"
        ws_gs["A1"].font = Font(bold=True, size=13)
        ws_gs.merge_cells("A1:H1")

        headers = ["Name", "City", "State", "Reviews", "Rating", "Score", "Grade", "Competitors"]
        for col, header in enumerate(headers, 1):
            cell = ws_gs.cell(row=2, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, t in enumerate(top_by_reviews, 3):
            ws_gs.cell(row=i, column=1, value=t.get("name"))
            ws_gs.cell(row=i, column=2, value=t.get("city"))
            ws_gs.cell(row=i, column=3, value=t.get("state"))
            ws_gs.cell(row=i, column=4, value=t.get("reviews"))
            ws_gs.cell(row=i, column=5, value=t.get("rating"))
            ws_gs.cell(row=i, column=6, value=t.get("score"))
            ws_gs.cell(row=i, column=7, value=t.get("grade"))
            ws_gs.cell(row=i, column=8, value=t.get("competitors"))

        # Low competition gems section
        gem_start = len(top_by_reviews) + 5
        ws_gs.cell(row=gem_start, column=1, value="Low Competition Gems (≤3 Competitors)")
        ws_gs.cell(row=gem_start, column=1).font = Font(bold=True, size=13)
        ws_gs.merge_cells(f"A{gem_start}:H{gem_start}")

        headers = ["Name", "City", "State", "Score", "Grade", "Rating", "Reviews", "Competitors"]
        for col, header in enumerate(headers, 1):
            cell = ws_gs.cell(row=gem_start + 1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, g in enumerate(low_competition_gems, gem_start + 2):
            ws_gs.cell(row=i, column=1, value=g.get("name"))
            ws_gs.cell(row=i, column=2, value=g.get("city"))
            ws_gs.cell(row=i, column=3, value=g.get("state"))
            ws_gs.cell(row=i, column=4, value=g.get("score"))
            ws_gs.cell(row=i, column=5, value=g.get("grade"))
            ws_gs.cell(row=i, column=6, value=g.get("rating"))
            ws_gs.cell(row=i, column=7, value=g.get("reviews"))
            ws_gs.cell(row=i, column=8, value=g.get("competitors"))

        col_widths = {"A": 35, "B": 18, "C": 8, "D": 10, "E": 8, "F": 8, "G": 10, "H": 14}
        for col_letter, width in col_widths.items():
            ws_gs.column_dimensions[col_letter].width = width

        # ---- Sheet 10: Deal Model ----
        ws_deal = wb.create_sheet("Deal Model")
        deal_model = data.get("deal_model", {})
        dm_tiers = deal_model.get("tier_economics", [])
        dm_scenarios = deal_model.get("scenarios", {})
        dm_cap = deal_model.get("capital_stack", {})

        ws_deal["A1"] = "Deal Model — Unit Economics"
        ws_deal["A1"].font = Font(bold=True, size=13)
        ws_deal.merge_cells("A1:H1")

        # Tier economics table
        deal_headers = [
            "Price Tier", "Locations", "Avg Revenue", "EBITDA Margin",
            "Avg EBITDA", "Entry Multiple", "Total Revenue", "Total EBITDA", "Total Acq Cost",
        ]
        for col, header in enumerate(deal_headers, 1):
            cell = ws_deal.cell(row=2, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, t in enumerate(dm_tiers, 3):
            ws_deal.cell(row=i, column=1, value=t.get("tier", "Unknown"))
            ws_deal.cell(row=i, column=2, value=t.get("count", 0))
            ws_deal.cell(row=i, column=3, value=t.get("avg_revenue", 0))
            ws_deal.cell(row=i, column=4, value=t.get("ebitda_margin", 0))
            ws_deal.cell(row=i, column=5, value=t.get("avg_ebitda", 0))
            ws_deal.cell(row=i, column=6, value=t.get("entry_multiple", 0))
            ws_deal.cell(row=i, column=7, value=t.get("total_revenue", 0))
            ws_deal.cell(row=i, column=8, value=t.get("total_ebitda", 0))
            ws_deal.cell(row=i, column=9, value=t.get("total_acq_cost", 0))

        # Totals row
        total_row = len(dm_tiers) + 3
        ws_deal.cell(row=total_row, column=1, value="TOTAL").font = Font(bold=True)
        ws_deal.cell(row=total_row, column=2, value=deal_model.get("total_locations", 0)).font = Font(bold=True)
        ws_deal.cell(row=total_row, column=7, value=deal_model.get("total_revenue", 0)).font = Font(bold=True)
        ws_deal.cell(row=total_row, column=8, value=deal_model.get("total_ebitda", 0)).font = Font(bold=True)
        ws_deal.cell(row=total_row, column=9, value=deal_model.get("total_acquisition_cost", 0)).font = Font(bold=True)

        # Capital stack section
        cap_start = total_row + 2
        ws_deal.cell(row=cap_start, column=1, value="Capital Requirements").font = Font(bold=True, size=13)
        ws_deal.merge_cells(f"A{cap_start}:D{cap_start}")

        cap_items = [
            ("Equity Contribution (40%)", dm_cap.get("equity", 0)),
            ("Senior Debt (60%)", dm_cap.get("debt", 0)),
            ("Transaction Costs (5%)", dm_cap.get("transaction_costs", 0)),
            ("Working Capital Reserve", dm_cap.get("working_capital", 0)),
            ("Total Capital Required", dm_cap.get("total_capital_required", 0)),
        ]
        for j, (label, value) in enumerate(cap_items, cap_start + 1):
            ws_deal.cell(row=j, column=1, value=label)
            ws_deal.cell(row=j, column=2, value=value)
            if label.startswith("Total"):
                ws_deal.cell(row=j, column=1).font = Font(bold=True)
                ws_deal.cell(row=j, column=2).font = Font(bold=True)

        ws_deal.cell(row=cap_start + len(cap_items) + 1, column=1, value="Leverage (Debt/EBITDA)")
        ws_deal.cell(row=cap_start + len(cap_items) + 1, column=2, value=deal_model.get("leverage_ratio", 0))

        # Scenarios section
        sc_start = cap_start + len(cap_items) + 3
        ws_deal.cell(row=sc_start, column=1, value="Returns Analysis").font = Font(bold=True, size=13)
        ws_deal.merge_cells(f"A{sc_start}:H{sc_start}")

        sc_headers = ["Scenario", "Entry EV", "Exit Multiple", "Margin Improvement",
                      "Hold Years", "Exit EV", "Gross MOIC", "Net IRR"]
        for col, header in enumerate(sc_headers, 1):
            cell = ws_deal.cell(row=sc_start + 1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        sc_row = sc_start + 2
        for sc_name in ["conservative", "base", "aggressive"]:
            s = dm_scenarios.get(sc_name, {})
            if not s:
                continue
            ws_deal.cell(row=sc_row, column=1, value=sc_name.title())
            ws_deal.cell(row=sc_row, column=2, value=s.get("entry_ev", 0))
            ws_deal.cell(row=sc_row, column=3, value=s.get("exit_multiple", 0))
            ws_deal.cell(row=sc_row, column=4, value=s.get("margin_improvement", 0))
            ws_deal.cell(row=sc_row, column=5, value=s.get("hold_years", 0))
            ws_deal.cell(row=sc_row, column=6, value=s.get("exit_ev", 0))
            ws_deal.cell(row=sc_row, column=7, value=s.get("gross_moic", 0))
            ws_deal.cell(row=sc_row, column=8, value=s.get("net_irr", 0))
            sc_row += 1

        deal_col_widths = {"A": 28, "B": 16, "C": 16, "D": 16, "E": 16, "F": 16, "G": 16, "H": 14, "I": 16}
        for col_letter, width in deal_col_widths.items():
            ws_deal.column_dimensions[col_letter].width = width

        # ---- Sheet 11: Stealth Wealth ----
        ws_sw = wb.create_sheet("Stealth Wealth")
        stealth_wealth = data.get("stealth_wealth", {})
        sw_comp = stealth_wealth.get("wealth_composition", {})
        sw_zips = stealth_wealth.get("stealth_zips", [])
        sw_summary = stealth_wealth.get("summary", {})

        ws_sw["A1"] = "Stealth Wealth Signal"
        ws_sw["A1"].font = Font(bold=True, size=13)
        ws_sw.merge_cells("A1:E1")

        # Summary stats
        ws_sw["A3"] = "Tax Year"
        ws_sw["B3"] = sw_summary.get("tax_year", "-")
        ws_sw["A4"] = "Stealth ZIPs Found"
        ws_sw["B4"] = sw_summary.get("total_stealth", 0)
        ws_sw["A5"] = "Validated (Score ≥70)"
        ws_sw["B5"] = sw_summary.get("validated", 0)
        ws_sw["A6"] = "Avg Non-Wage Income"
        ws_sw["B6"] = sw_summary.get("avg_non_wage_income", 0)
        for r in range(3, 7):
            ws_sw[f"A{r}"].font = Font(bold=True)

        # Wealth composition
        if sw_comp:
            ws_sw["A8"] = "Income Composition"
            ws_sw["A8"].font = Font(bold=True, size=11)
            ws_sw["A9"] = "W-2 Wages %"
            ws_sw["B9"] = sw_comp.get("wages_pct", 0)
            ws_sw["A10"] = "Capital Gains %"
            ws_sw["B10"] = sw_comp.get("cap_gains_pct", 0)
            ws_sw["A11"] = "Dividends %"
            ws_sw["B11"] = sw_comp.get("dividends_pct", 0)
            ws_sw["A12"] = "Business/Partnership %"
            ws_sw["B12"] = sw_comp.get("biz_income_pct", 0)
            ws_sw["A13"] = "Other %"
            ws_sw["B13"] = sw_comp.get("other_pct", 0)

        # Stealth ZIPs table
        sw_start = 15
        ws_sw.cell(row=sw_start, column=1, value="Top Stealth Wealth ZIPs").font = Font(bold=True, size=11)
        sw_headers = [
            "ZIP", "State", "Non-Wage/Return", "Non-Wage %",
            "Returns", "Avg AGI", "Medspa Score", "Grade", "A-Grade Medspas",
        ]
        for col, header in enumerate(sw_headers, 1):
            cell = ws_sw.cell(row=sw_start + 1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, z in enumerate(sw_zips, sw_start + 2):
            ws_sw.cell(row=i, column=1, value=z.get("zip_code"))
            ws_sw.cell(row=i, column=2, value=z.get("state"))
            ws_sw.cell(row=i, column=3, value=z.get("non_wage_per_return", 0))
            ws_sw.cell(row=i, column=4, value=z.get("non_wage_pct", 0))
            ws_sw.cell(row=i, column=5, value=z.get("num_returns", 0))
            ws_sw.cell(row=i, column=6, value=z.get("avg_agi", 0))
            ws_sw.cell(row=i, column=7, value=z.get("medspa_score", 0))
            ws_sw.cell(row=i, column=8, value=z.get("medspa_grade", "-"))
            ws_sw.cell(row=i, column=9, value=z.get("a_grade_medspas", 0))

        sw_col_widths = {"A": 12, "B": 8, "C": 16, "D": 12, "E": 10, "F": 12, "G": 14, "H": 8, "I": 16}
        for col_letter, width in sw_col_widths.items():
            ws_sw.column_dimensions[col_letter].width = width

        # ---- Sheet 12: Migration Alpha ----
        ws_ma = wb.create_sheet("Migration Alpha")
        migration_alpha = data.get("migration_alpha", {})
        ma_flows = migration_alpha.get("state_flows", [])
        ma_emerging = migration_alpha.get("emerging_markets", [])
        ma_summary = migration_alpha.get("summary", {})

        ws_ma["A1"] = "Migration Alpha"
        ws_ma["A1"].font = Font(bold=True, size=13)
        ws_ma.merge_cells("A1:E1")

        ws_ma["A3"] = "Tax Year"
        ws_ma["B3"] = ma_summary.get("tax_year", "-")
        ws_ma["A4"] = "Top Gainer"
        ws_ma["B4"] = ma_summary.get("top_gainer", "-")
        ws_ma["A5"] = "Top Loser"
        ws_ma["B5"] = ma_summary.get("top_loser", "-")
        ws_ma["A6"] = "Emerging Markets"
        ws_ma["B6"] = ma_summary.get("emerging_count", 0)
        for r in range(3, 7):
            ws_ma[f"A{r}"].font = Font(bold=True)

        # State flows table
        ma_start = 8
        ws_ma.cell(row=ma_start, column=1, value="State Wealth Flows").font = Font(bold=True, size=11)
        ma_headers = [
            "State", "Inflow AGI ($M)", "Outflow AGI ($M)", "Net AGI ($M)",
            "Inflow Returns", "Total Medspas", "A-Grade", "Migration Alpha",
        ]
        for col, header in enumerate(ma_headers, 1):
            cell = ws_ma.cell(row=ma_start + 1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for i, s in enumerate(ma_flows, ma_start + 2):
            ws_ma.cell(row=i, column=1, value=s.get("state"))
            ws_ma.cell(row=i, column=2, value=s.get("inflow_agi_m", 0))
            ws_ma.cell(row=i, column=3, value=s.get("outflow_agi_m", 0))
            ws_ma.cell(row=i, column=4, value=s.get("net_agi_m", 0))
            ws_ma.cell(row=i, column=5, value=s.get("inflow_returns", 0))
            ws_ma.cell(row=i, column=6, value=s.get("total_medspas", 0))
            ws_ma.cell(row=i, column=7, value=s.get("a_grade_count", 0))
            ws_ma.cell(row=i, column=8, value=s.get("migration_alpha", 0))

        # Emerging markets highlight
        em_start = ma_start + len(ma_flows) + 4
        ws_ma.cell(row=em_start, column=1, value="Emerging Markets (<20 A-Grade, Positive Inflow)").font = Font(bold=True, size=11)
        ws_ma.merge_cells(f"A{em_start}:F{em_start}")

        em_headers = ["State", "Net AGI ($M)", "Inflow Returns", "Total Medspas", "A-Grade", "Migration Alpha"]
        for col, header in enumerate(em_headers, 1):
            cell = ws_ma.cell(row=em_start + 1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        green_fill = PatternFill(start_color="E6F4EA", end_color="E6F4EA", fill_type="solid")
        for i, s in enumerate(ma_emerging, em_start + 2):
            ws_ma.cell(row=i, column=1, value=s.get("state"))
            ws_ma.cell(row=i, column=2, value=s.get("net_agi_m", 0))
            ws_ma.cell(row=i, column=3, value=s.get("inflow_returns", 0))
            ws_ma.cell(row=i, column=4, value=s.get("total_medspas", 0))
            ws_ma.cell(row=i, column=5, value=s.get("a_grade_count", 0))
            ws_ma.cell(row=i, column=6, value=s.get("migration_alpha", 0))
            for c in range(1, 7):
                ws_ma.cell(row=i, column=c).fill = green_fill

        ma_col_widths = {"A": 12, "B": 16, "C": 16, "D": 14, "E": 14, "F": 14, "G": 10, "H": 16}
        for col_letter, width in ma_col_widths.items():
            ws_ma.column_dimensions[col_letter].width = width

        # Save to bytes
        output = BytesIO()
        wb.save(output)
        return output.getvalue()
