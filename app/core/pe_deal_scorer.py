"""
PE Deal Scorer.

Scores acquisition targets on 5 weighted dimensions to produce a composite
0-100 deal attractiveness score. Pure computation functions are static and
testable without DB access.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PECompanyFinancials,
    PECompanyLeadership,
    PECompetitorMapping,
    PEPortfolioCompany,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DIMENSION_WEIGHTS: Dict[str, float] = {
    "financial_quality": 0.35,
    "market_position": 0.20,
    "management": 0.15,
    "growth_trajectory": 0.20,
    "deal_attractiveness": 0.10,
}


# ---------------------------------------------------------------------------
# Grade helper
# ---------------------------------------------------------------------------

def _letter_grade(score: float) -> str:
    """Convert numeric score (0-100) to letter grade."""
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    if score >= 35:
        return "D"
    return "F"


def _clamp(val: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, val))


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DimensionScore:
    """One dimension of the deal score."""
    dimension: str
    label: str
    weight: float
    raw_score: float  # 0-100
    weighted_score: float
    grade: str
    explanation: str
    data_gaps: List[str] = field(default_factory=list)


@dataclass
class DealScoreResult:
    """Complete deal scoring result."""
    company_id: int
    company_name: str
    composite_score: float
    grade: str
    dimensions: List[DimensionScore] = field(default_factory=list)
    strengths: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    data_gaps: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Dimension scorers (pure functions — no DB)
# ---------------------------------------------------------------------------

def _score_financial_quality(financials: List[Dict[str, Any]]) -> DimensionScore:
    """Score financial quality from company financials dicts.

    Evaluates: EBITDA margin, gross margin, revenue growth, revenue per employee.
    """
    weight = DIMENSION_WEIGHTS["financial_quality"]

    if not financials:
        return DimensionScore(
            dimension="financial_quality", label="Financial Quality",
            weight=weight, raw_score=0, weighted_score=0,
            grade="F", explanation="No financial data available.",
            data_gaps=["financials"],
        )

    # Use most recent year
    latest = sorted(financials, key=lambda f: f.get("fiscal_year", 0), reverse=True)[0]

    points = 0.0
    components = 0
    explanations = []

    # EBITDA margin (0-30 points)
    ebitda_margin = latest.get("ebitda_margin_pct")
    if ebitda_margin is not None:
        components += 1
        if ebitda_margin >= 25:
            pts = 30
            explanations.append(f"Strong EBITDA margin ({ebitda_margin:.1f}%)")
        elif ebitda_margin >= 15:
            pts = 20
            explanations.append(f"Healthy EBITDA margin ({ebitda_margin:.1f}%)")
        elif ebitda_margin >= 8:
            pts = 10
            explanations.append(f"Moderate EBITDA margin ({ebitda_margin:.1f}%)")
        else:
            pts = 3
            explanations.append(f"Low EBITDA margin ({ebitda_margin:.1f}%)")
        points += pts

    # Gross margin (0-25 points)
    gross_margin = latest.get("gross_margin_pct")
    if gross_margin is not None:
        components += 1
        if gross_margin >= 50:
            pts = 25
        elif gross_margin >= 35:
            pts = 18
        elif gross_margin >= 20:
            pts = 10
        else:
            pts = 3
        points += pts
        explanations.append(f"Gross margin {gross_margin:.1f}%")

    # Revenue growth (0-25 points)
    rev_growth = latest.get("revenue_growth_pct")
    if rev_growth is not None:
        components += 1
        if rev_growth >= 15:
            pts = 25
            explanations.append(f"Strong revenue growth ({rev_growth:.1f}%)")
        elif rev_growth >= 8:
            pts = 18
        elif rev_growth >= 0:
            pts = 10
        else:
            pts = 2
            explanations.append(f"Revenue declining ({rev_growth:.1f}%)")
        points += pts

    # Revenue per employee (0-20 points)
    revenue = latest.get("revenue_usd")
    employees = latest.get("employees")
    if revenue and employees and employees > 0:
        components += 1
        rev_per_emp = revenue / employees
        if rev_per_emp >= 300_000:
            pts = 20
        elif rev_per_emp >= 200_000:
            pts = 15
        elif rev_per_emp >= 100_000:
            pts = 10
        else:
            pts = 5
        points += pts
        explanations.append(f"Rev/employee ${rev_per_emp:,.0f}")

    if components == 0:
        raw = 0
        explanation = "Insufficient financial metrics."
        gaps = ["detailed_financials"]
    else:
        raw = _clamp(points)
        explanation = "; ".join(explanations)
        gaps = []

    return DimensionScore(
        dimension="financial_quality", label="Financial Quality",
        weight=weight, raw_score=round(raw, 1),
        weighted_score=round(raw * weight, 1),
        grade=_letter_grade(raw), explanation=explanation,
        data_gaps=gaps,
    )


def _score_market_position(competitors: List[Dict[str, Any]]) -> DimensionScore:
    """Score market position from competitor mapping dicts."""
    weight = DIMENSION_WEIGHTS["market_position"]

    if not competitors:
        return DimensionScore(
            dimension="market_position", label="Market Position",
            weight=weight, raw_score=30, weighted_score=round(30 * weight, 1),
            grade="D", explanation="No competitor data — assuming average position.",
            data_gaps=["competitors"],
        )

    points = 0.0
    explanations = []

    # Number of mapped competitors (more data = better understanding)
    n = len(competitors)
    points += min(n * 5, 20)  # up to 20 points for 4+ competitors

    # How many competitors are smaller (target is larger = stronger position)
    smaller = sum(1 for c in competitors if c.get("relative_size") == "Smaller")
    larger = sum(1 for c in competitors if c.get("relative_size") == "Larger")

    if smaller > larger:
        points += 30
        explanations.append(f"Larger than {smaller} of {n} competitors")
    elif smaller == larger:
        points += 20
        explanations.append("Mid-market position among peers")
    else:
        points += 10
        explanations.append(f"Smaller than {larger} of {n} competitors")

    # Market position labels
    leaders = sum(1 for c in competitors if c.get("market_position") == "Leader")
    if leaders == 0:
        points += 25
        explanations.append("No dominant market leader — fragmented opportunity")
    elif leaders <= 2:
        points += 15
        explanations.append(f"{leaders} market leader(s) present")
    else:
        points += 5

    # PE-backed competitors (more PE activity = validated thesis)
    pe_backed = sum(1 for c in competitors if c.get("is_pe_backed"))
    if pe_backed > 0:
        points += 15
        explanations.append(f"{pe_backed} PE-backed competitors (thesis validation)")
    else:
        points += 10

    raw = _clamp(points)
    return DimensionScore(
        dimension="market_position", label="Market Position",
        weight=weight, raw_score=round(raw, 1),
        weighted_score=round(raw * weight, 1),
        grade=_letter_grade(raw), explanation="; ".join(explanations),
    )


def _score_management(leaders: List[Dict[str, Any]]) -> DimensionScore:
    """Score management quality from leadership dicts."""
    weight = DIMENSION_WEIGHTS["management"]

    if not leaders:
        return DimensionScore(
            dimension="management", label="Management Quality",
            weight=weight, raw_score=20, weighted_score=round(20 * weight, 1),
            grade="F", explanation="No leadership data available.",
            data_gaps=["leadership"],
        )

    points = 0.0
    explanations = []

    # C-suite completeness (0-35 points)
    has_ceo = any(l.get("is_ceo") for l in leaders)
    has_cfo = any(l.get("is_cfo") for l in leaders)
    c_suite = sum(1 for l in leaders if l.get("role_category") == "C-Suite")

    if has_ceo and has_cfo:
        points += 35
        explanations.append("Full CEO + CFO in place")
    elif has_ceo:
        points += 25
        explanations.append("CEO in place, CFO missing")
    else:
        points += 10
        explanations.append("CEO position unclear")

    # Team depth (0-25 points)
    team_size = len(leaders)
    if team_size >= 5:
        points += 25
        explanations.append(f"Deep leadership team ({team_size} leaders)")
    elif team_size >= 3:
        points += 15
    else:
        points += 5
        explanations.append(f"Thin leadership ({team_size} leaders)")

    # PE-appointed leaders (0-20 points — indicates professionalization)
    pe_appointed = sum(1 for l in leaders if l.get("appointed_by_pe"))
    if pe_appointed >= 2:
        points += 20
        explanations.append(f"{pe_appointed} PE-appointed leaders")
    elif pe_appointed == 1:
        points += 10

    # Average tenure (0-20 points)
    tenures = [l["tenure_years"] for l in leaders if l.get("tenure_years")]
    if tenures:
        avg_tenure = sum(tenures) / len(tenures)
        if 2 <= avg_tenure <= 7:
            points += 20  # sweet spot
            explanations.append(f"Avg tenure {avg_tenure:.1f}y (stable)")
        elif avg_tenure > 7:
            points += 12
            explanations.append(f"Avg tenure {avg_tenure:.1f}y (long, potential succession risk)")
        else:
            points += 8
            explanations.append(f"Avg tenure {avg_tenure:.1f}y (recent changes)")

    raw = _clamp(points)
    return DimensionScore(
        dimension="management", label="Management Quality",
        weight=weight, raw_score=round(raw, 1),
        weighted_score=round(raw * weight, 1),
        grade=_letter_grade(raw), explanation="; ".join(explanations),
    )


def _score_growth_trajectory(financials: List[Dict[str, Any]]) -> DimensionScore:
    """Score growth trajectory from multi-year financials."""
    weight = DIMENSION_WEIGHTS["growth_trajectory"]

    if len(financials) < 2:
        return DimensionScore(
            dimension="growth_trajectory", label="Growth Trajectory",
            weight=weight, raw_score=0, weighted_score=0,
            grade="F", explanation="Need 2+ years of data for growth analysis.",
            data_gaps=["multi_year_financials"],
        )

    sorted_fin = sorted(financials, key=lambda f: f.get("fiscal_year", 0))
    oldest = sorted_fin[0]
    newest = sorted_fin[-1]
    years = newest.get("fiscal_year", 0) - oldest.get("fiscal_year", 0)

    if years <= 0:
        return DimensionScore(
            dimension="growth_trajectory", label="Growth Trajectory",
            weight=weight, raw_score=0, weighted_score=0,
            grade="F", explanation="Cannot compute growth — same fiscal year.",
            data_gaps=["multi_year_financials"],
        )

    points = 0.0
    explanations = []

    # Revenue CAGR (0-40 points)
    rev_old = oldest.get("revenue_usd")
    rev_new = newest.get("revenue_usd")
    if rev_old and rev_new and rev_old > 0:
        cagr = ((rev_new / rev_old) ** (1 / years) - 1) * 100
        if cagr >= 20:
            points += 40
            explanations.append(f"Revenue CAGR {cagr:.1f}% (exceptional)")
        elif cagr >= 10:
            points += 30
            explanations.append(f"Revenue CAGR {cagr:.1f}% (strong)")
        elif cagr >= 5:
            points += 20
            explanations.append(f"Revenue CAGR {cagr:.1f}% (moderate)")
        elif cagr >= 0:
            points += 10
            explanations.append(f"Revenue CAGR {cagr:.1f}% (slow)")
        else:
            points += 0
            explanations.append(f"Revenue declining (CAGR {cagr:.1f}%)")

    # Employee growth (0-25 points)
    emp_old = oldest.get("employees")
    emp_new = newest.get("employees")
    if emp_old and emp_new and emp_old > 0:
        emp_growth = ((emp_new / emp_old) ** (1 / years) - 1) * 100
        if emp_growth >= 10:
            points += 25
        elif emp_growth >= 5:
            points += 18
        elif emp_growth >= 0:
            points += 10
        else:
            points += 3
        explanations.append(f"Employee CAGR {emp_growth:.1f}%")

    # Margin expansion (0-35 points)
    margin_old = oldest.get("ebitda_margin_pct")
    margin_new = newest.get("ebitda_margin_pct")
    if margin_old is not None and margin_new is not None:
        margin_delta = margin_new - margin_old
        if margin_delta >= 5:
            points += 35
            explanations.append(f"Margin expansion +{margin_delta:.1f}pp")
        elif margin_delta >= 2:
            points += 25
            explanations.append(f"Margin expansion +{margin_delta:.1f}pp")
        elif margin_delta >= 0:
            points += 15
            explanations.append(f"Stable margins ({margin_delta:+.1f}pp)")
        else:
            points += 5
            explanations.append(f"Margin compression {margin_delta:.1f}pp")

    raw = _clamp(points)
    return DimensionScore(
        dimension="growth_trajectory", label="Growth Trajectory",
        weight=weight, raw_score=round(raw, 1),
        weighted_score=round(raw * weight, 1),
        grade=_letter_grade(raw), explanation="; ".join(explanations),
    )


def _score_deal_attractiveness(
    financials: List[Dict[str, Any]],
    competitors: List[Dict[str, Any]],
) -> DimensionScore:
    """Score deal attractiveness based on size, fragmentation signals, and scalability."""
    weight = DIMENSION_WEIGHTS["deal_attractiveness"]

    points = 0.0
    explanations = []
    gaps = []

    if not financials:
        gaps.append("financials")
    else:
        latest = sorted(financials, key=lambda f: f.get("fiscal_year", 0), reverse=True)[0]
        revenue = latest.get("revenue_usd")

        # Revenue size — PE sweet spot is $20M-$200M
        if revenue:
            rev_m = revenue / 1_000_000
            if 20 <= rev_m <= 200:
                points += 40
                explanations.append(f"Revenue ${rev_m:.0f}M (PE sweet spot)")
            elif 10 <= rev_m < 20 or 200 < rev_m <= 500:
                points += 25
                explanations.append(f"Revenue ${rev_m:.0f}M (viable deal size)")
            elif rev_m > 500:
                points += 15
                explanations.append(f"Revenue ${rev_m:.0f}M (large platform)")
            else:
                points += 10
                explanations.append(f"Revenue ${rev_m:.0f}M (small, may need platform)")

        # EBITDA positive = cash flowing
        ebitda = latest.get("ebitda_usd")
        if ebitda and ebitda > 0:
            points += 20
            explanations.append("EBITDA positive")
        elif ebitda is not None:
            explanations.append("EBITDA negative")

    # Fragmentation signal from competitor count
    if competitors:
        n = len(competitors)
        pe_backed_count = sum(1 for c in competitors if c.get("is_pe_backed"))
        if n >= 4 and pe_backed_count <= 1:
            points += 25
            explanations.append(f"Fragmented market ({n} competitors, {pe_backed_count} PE-backed)")
        elif n >= 2:
            points += 15
            explanations.append(f"{n} mapped competitors")
    else:
        gaps.append("competitors")
        points += 10  # assume moderate

    # Cap at 100
    raw = _clamp(points)
    return DimensionScore(
        dimension="deal_attractiveness", label="Deal Attractiveness",
        weight=weight, raw_score=round(raw, 1),
        weighted_score=round(raw * weight, 1),
        grade=_letter_grade(raw), explanation="; ".join(explanations) if explanations else "Insufficient data.",
        data_gaps=gaps,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def score_deal(db: Session, company_id: int) -> Optional[DealScoreResult]:
    """Score an acquisition target across 5 dimensions.

    Returns DealScoreResult with composite score, dimension breakdowns,
    strengths, risks, and data gaps.
    """
    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()
    if not company:
        return None

    # Fetch data
    financials_raw = db.execute(
        select(PECompanyFinancials)
        .where(PECompanyFinancials.company_id == company_id)
        .order_by(PECompanyFinancials.fiscal_year.desc())
        .limit(5)
    ).scalars().all()

    financials = [
        {
            "fiscal_year": f.fiscal_year,
            "revenue_usd": float(f.revenue_usd) if f.revenue_usd else None,
            "ebitda_usd": float(f.ebitda_usd) if f.ebitda_usd else None,
            "ebitda_margin_pct": float(f.ebitda_margin_pct) if f.ebitda_margin_pct else None,
            "gross_margin_pct": float(f.gross_margin_pct) if f.gross_margin_pct else None,
            "revenue_growth_pct": float(f.revenue_growth_pct) if f.revenue_growth_pct else None,
            "employees": f.employee_count if hasattr(f, "employee_count") else None,
        }
        for f in financials_raw
    ]

    competitors_raw = db.execute(
        select(PECompetitorMapping).where(PECompetitorMapping.company_id == company_id)
    ).scalars().all()

    competitors = [
        {
            "relative_size": c.relative_size,
            "market_position": c.market_position,
            "is_pe_backed": c.is_pe_backed or False,
        }
        for c in competitors_raw
    ]

    leaders_raw = db.execute(
        select(PECompanyLeadership)
        .where(PECompanyLeadership.company_id == company_id, PECompanyLeadership.is_current == True)
    ).scalars().all()

    from datetime import date
    leaders = [
        {
            "is_ceo": l.is_ceo or False,
            "is_cfo": l.is_cfo or False,
            "is_board_member": l.is_board_member or False,
            "appointed_by_pe": l.appointed_by_pe,
            "tenure_years": round((date.today() - l.start_date).days / 365.25, 1) if l.start_date else None,
            "role_category": l.role_category,
        }
        for l in leaders_raw
    ]

    # Score each dimension
    dim_scores = [
        _score_financial_quality(financials),
        _score_market_position(competitors),
        _score_management(leaders),
        _score_growth_trajectory(financials),
        _score_deal_attractiveness(financials, competitors),
    ]

    # Composite
    composite = sum(d.weighted_score for d in dim_scores)
    composite = round(_clamp(composite), 1)

    # Strengths and risks
    strengths = [d.explanation for d in dim_scores if d.raw_score >= 65]
    risks = [d.explanation for d in dim_scores if d.raw_score < 35]
    all_gaps = []
    for d in dim_scores:
        all_gaps.extend(d.data_gaps)

    return DealScoreResult(
        company_id=company.id,
        company_name=company.name,
        composite_score=composite,
        grade=_letter_grade(composite),
        dimensions=dim_scores,
        strengths=strengths,
        risks=risks,
        data_gaps=list(set(all_gaps)),
    )
