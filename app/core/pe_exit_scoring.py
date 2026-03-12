"""
PE Exit Readiness Scoring Engine.

Produces a composite 0-100 score across 6 weighted dimensions to assess
whether a portfolio company is ready for a successful exit.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PECompanyFinancials,
    PECompanyLeadership,
    PECompanyValuation,
    PECompetitorMapping,
    PEDeal,
    PEFund,
    PEFundInvestment,
    PEPortfolioCompany,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Grade helpers
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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    return float(val)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SubScore:
    """One dimension of the exit readiness score."""
    dimension: str
    label: str
    weight: float
    raw_score: float  # 0-100
    weighted_score: float  # raw * weight
    grade: str
    explanation: str
    recommendations: List[str] = field(default_factory=list)


@dataclass
class ExitReadinessResult:
    """Complete exit readiness assessment."""
    company_id: int
    company_name: str
    composite_score: float  # 0-100
    grade: str
    sub_scores: List[SubScore] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    confidence: str = "high"  # high, medium, low
    data_gaps: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Sub-score calculators
# ---------------------------------------------------------------------------

def _score_financial_health(
    db: Session, company_id: int
) -> SubScore:
    """Financial Health (30%): revenue growth, EBITDA margin, FCF, debt level."""
    weight = 0.30
    recs = []
    points = []

    # Get last 3 years of financials
    rows = db.execute(
        select(PECompanyFinancials)
        .where(
            PECompanyFinancials.company_id == company_id,
            PECompanyFinancials.fiscal_period == "FY",
        )
        .order_by(PECompanyFinancials.fiscal_year.desc())
        .limit(3)
    ).scalars().all()

    if not rows:
        return SubScore(
            dimension="financial_health", label="Financial Health",
            weight=weight, raw_score=25, weighted_score=25 * weight,
            grade="F", explanation="No financial data available.",
            recommendations=["Upload or collect financial data for at least 2 years."],
        )

    latest = rows[0]

    # Revenue growth (0-25 pts)
    growth = _safe_float(latest.revenue_growth_pct)
    if growth is not None:
        if growth >= 20:
            points.append(25)
        elif growth >= 10:
            points.append(20)
        elif growth >= 5:
            points.append(15)
        elif growth >= 0:
            points.append(10)
        else:
            points.append(5)
            recs.append("Revenue is declining — address top-line growth before exit.")
    else:
        points.append(10)

    # EBITDA margin (0-25 pts)
    margin = _safe_float(latest.ebitda_margin_pct)
    if margin is not None:
        if margin >= 25:
            points.append(25)
        elif margin >= 18:
            points.append(20)
        elif margin >= 12:
            points.append(15)
        elif margin >= 5:
            points.append(10)
        else:
            points.append(5)
            recs.append("EBITDA margin below 5% — focus on margin expansion initiatives.")
    else:
        points.append(10)

    # Free cash flow positive (0-25 pts)
    fcf = _safe_float(latest.free_cash_flow_usd)
    if fcf is not None:
        if fcf > 0:
            # Check FCF conversion (FCF / EBITDA)
            ebitda = _safe_float(latest.ebitda_usd)
            if ebitda and ebitda > 0:
                conversion = fcf / ebitda
                if conversion >= 0.6:
                    points.append(25)
                elif conversion >= 0.4:
                    points.append(20)
                else:
                    points.append(15)
            else:
                points.append(15)
        else:
            points.append(5)
            recs.append("Free cash flow is negative — address capex or working capital.")
    else:
        points.append(10)

    # Debt level (0-25 pts) — lower is better
    d2e = _safe_float(latest.debt_to_ebitda)
    if d2e is not None:
        if d2e <= 2:
            points.append(25)
        elif d2e <= 3.5:
            points.append(20)
        elif d2e <= 5:
            points.append(15)
        elif d2e <= 7:
            points.append(10)
        else:
            points.append(5)
            recs.append("Leverage above 7x EBITDA — consider deleveraging before exit.")
    else:
        points.append(10)

    raw = sum(points)
    # Bonus for multi-year consistency
    if len(rows) >= 3:
        growths = [_safe_float(r.revenue_growth_pct) for r in rows if _safe_float(r.revenue_growth_pct) is not None]
        if growths and all(g > 0 for g in growths):
            raw = min(100, raw + 5)

    explanation_parts = []
    if growth is not None:
        explanation_parts.append(f"Revenue growth {growth:.1f}%")
    if margin is not None:
        explanation_parts.append(f"EBITDA margin {margin:.1f}%")
    if d2e is not None:
        explanation_parts.append(f"Leverage {d2e:.1f}x")

    return SubScore(
        dimension="financial_health", label="Financial Health",
        weight=weight, raw_score=raw, weighted_score=raw * weight,
        grade=_letter_grade(raw),
        explanation="; ".join(explanation_parts) if explanation_parts else "Limited financial data.",
        recommendations=recs,
    )


def _score_market_position(
    db: Session, company_id: int
) -> SubScore:
    """Market Position (20%): industry, competitors, relative size."""
    weight = 0.20
    recs = []
    raw = 50  # default if no data

    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()

    if not company:
        return SubScore(
            dimension="market_position", label="Market Position",
            weight=weight, raw_score=25, weighted_score=25 * weight,
            grade="F", explanation="Company not found.",
            recommendations=[],
        )

    # Competitor analysis
    competitors = db.execute(
        select(PECompetitorMapping).where(PECompetitorMapping.company_id == company_id)
    ).scalars().all()

    explanation_parts = []

    if competitors:
        leader_count = sum(1 for c in competitors if c.market_position == "Leader")
        niche_count = sum(1 for c in competitors if c.market_position == "Niche")
        smaller_count = sum(1 for c in competitors if c.relative_size == "Smaller")

        # If most competitors are smaller, company is larger
        if len(competitors) >= 3 and smaller_count >= len(competitors) * 0.5:
            raw = 80
            explanation_parts.append(f"Larger than {smaller_count}/{len(competitors)} competitors")
        elif len(competitors) >= 2:
            raw = 60
            explanation_parts.append(f"{len(competitors)} mapped competitors")
        else:
            raw = 50
            explanation_parts.append("Limited competitive data")
    else:
        raw = 45
        recs.append("Map competitive landscape — buyers will ask for market position analysis.")
        explanation_parts.append("No competitor data")

    # Industry attractiveness bonus
    high_value_industries = {"Software", "Technology", "Healthcare", "Fintech"}
    if company.industry in high_value_industries:
        raw = min(100, raw + 10)
        explanation_parts.append(f"High-value sector: {company.industry}")

    # Platform company bonus
    if company.is_platform_company:
        raw = min(100, raw + 5)
        explanation_parts.append("Platform company")

    return SubScore(
        dimension="market_position", label="Market Position",
        weight=weight, raw_score=raw, weighted_score=raw * weight,
        grade=_letter_grade(raw),
        explanation="; ".join(explanation_parts),
        recommendations=recs,
    )


def _score_management_quality(
    db: Session, company_id: int
) -> SubScore:
    """Management Quality (15%): leadership depth, CEO tenure, key person risk."""
    weight = 0.15
    recs = []
    raw = 50

    leaders = db.execute(
        select(PECompanyLeadership).where(
            PECompanyLeadership.company_id == company_id,
            PECompanyLeadership.is_current == True,
        )
    ).scalars().all()

    explanation_parts = []

    if not leaders:
        raw = 30
        explanation_parts.append("No leadership data")
        recs.append("Collect and verify current management team — critical for buyer DD.")
    else:
        # C-suite completeness
        has_ceo = any(l.is_ceo for l in leaders)
        has_cfo = any(l.is_cfo for l in leaders)
        c_suite = [l for l in leaders if l.role_category == "C-Suite" or l.is_ceo or l.is_cfo]

        if has_ceo and has_cfo and len(c_suite) >= 3:
            raw = 85
            explanation_parts.append(f"Full C-suite ({len(c_suite)} executives)")
        elif has_ceo and has_cfo:
            raw = 70
            explanation_parts.append("CEO and CFO in place")
        elif has_ceo:
            raw = 55
            explanation_parts.append("CEO in place, no CFO identified")
            recs.append("Ensure a strong CFO is in place — buyers expect financial leadership.")
        else:
            raw = 40
            explanation_parts.append(f"{len(leaders)} leaders, no CEO identified")
            recs.append("Identify and document CEO and key executives.")

        # Board representation
        board_members = [l for l in leaders if l.is_board_member]
        if board_members:
            raw = min(100, raw + 5)
            explanation_parts.append(f"{len(board_members)} board members")

        # PE-appointed leadership (stability signal)
        pe_appointed = [l for l in leaders if l.appointed_by_pe]
        if pe_appointed:
            explanation_parts.append(f"{len(pe_appointed)} PE-appointed")

    return SubScore(
        dimension="management_quality", label="Management Quality",
        weight=weight, raw_score=raw, weighted_score=raw * weight,
        grade=_letter_grade(raw),
        explanation="; ".join(explanation_parts) if explanation_parts else "Unknown",
        recommendations=recs,
    )


def _score_data_room_readiness(
    db: Session, company_id: int
) -> SubScore:
    """Data Room Readiness (15%): coverage of financials, valuations, leadership, news."""
    weight = 0.15
    recs = []
    checks = 0
    total_checks = 6

    explanation_parts = []

    # Check: multi-year financials
    fin_count = db.execute(
        select(func.count(PECompanyFinancials.id)).where(
            PECompanyFinancials.company_id == company_id,
            PECompanyFinancials.fiscal_period == "FY",
        )
    ).scalar_one()
    if fin_count >= 3:
        checks += 1
        explanation_parts.append(f"{fin_count} years of financials")
    else:
        recs.append(f"Only {fin_count} years of financials — buyers expect 3-5 years.")

    # Check: audited financials
    audited = db.execute(
        select(func.count(PECompanyFinancials.id)).where(
            PECompanyFinancials.company_id == company_id,
            PECompanyFinancials.is_audited == True,
        )
    ).scalar_one()
    if audited >= 2:
        checks += 1
        explanation_parts.append(f"{audited} audited periods")
    else:
        recs.append("Get financials audited — unaudited statements reduce buyer confidence.")

    # Check: recent valuation
    val_count = db.execute(
        select(func.count(PECompanyValuation.id)).where(
            PECompanyValuation.company_id == company_id,
        )
    ).scalar_one()
    if val_count >= 1:
        checks += 1
        explanation_parts.append(f"{val_count} valuations on file")
    else:
        recs.append("Commission a third-party valuation to anchor pricing expectations.")

    # Check: leadership team documented
    leader_count = db.execute(
        select(func.count(PECompanyLeadership.id)).where(
            PECompanyLeadership.company_id == company_id,
            PECompanyLeadership.is_current == True,
        )
    ).scalar_one()
    if leader_count >= 3:
        checks += 1
        explanation_parts.append(f"{leader_count} leaders documented")
    else:
        recs.append("Document full management team with bios and tenure.")

    # Check: competitor mapping
    comp_count = db.execute(
        select(func.count(PECompetitorMapping.id)).where(
            PECompetitorMapping.company_id == company_id,
        )
    ).scalar_one()
    if comp_count >= 2:
        checks += 1
        explanation_parts.append(f"{comp_count} competitors mapped")
    else:
        recs.append("Map at least 3-5 competitors for buyer market sizing.")

    # Check: deal history
    deal_count = db.execute(
        select(func.count(PEDeal.id)).where(PEDeal.company_id == company_id)
    ).scalar_one()
    if deal_count >= 1:
        checks += 1
        explanation_parts.append(f"{deal_count} deals on record")
    else:
        recs.append("Document acquisition history and prior transaction details.")

    raw = round(checks / total_checks * 100)

    return SubScore(
        dimension="data_room_readiness", label="Data Room Readiness",
        weight=weight, raw_score=raw, weighted_score=raw * weight,
        grade=_letter_grade(raw),
        explanation=f"{checks}/{total_checks} checks passed. " + "; ".join(explanation_parts),
        recommendations=recs,
    )


def _score_market_timing(
    db: Session, company_id: int
) -> SubScore:
    """Market Timing (10%): sector deal volume, entry vs current multiples."""
    weight = 0.10
    recs = []
    raw = 55  # neutral default

    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()

    explanation_parts = []

    if not company:
        return SubScore(
            dimension="market_timing", label="Market Timing",
            weight=weight, raw_score=50, weighted_score=50 * weight,
            grade="C", explanation="Company not found.",
            recommendations=[],
        )

    # Check sector deal activity (deals in same industry, recent 2 years)
    recent_deals = db.execute(
        select(func.count(PEDeal.id))
        .join(PEPortfolioCompany, PEPortfolioCompany.id == PEDeal.company_id)
        .where(
            PEPortfolioCompany.industry == company.industry,
            PEDeal.announced_date >= func.current_date() - 730,  # ~2 years
        )
    ).scalar_one()

    if recent_deals >= 5:
        raw = 80
        explanation_parts.append(f"Active sector: {recent_deals} deals in 2yr")
    elif recent_deals >= 2:
        raw = 65
        explanation_parts.append(f"Moderate activity: {recent_deals} deals in 2yr")
    else:
        raw = 45
        explanation_parts.append(f"Low deal volume: {recent_deals} deals in 2yr")
        recs.append("Sector deal volume is low — consider waiting for a more active M&A cycle.")

    # Entry multiple vs current — check if multiples have expanded
    investment = db.execute(
        select(PEFundInvestment).where(
            PEFundInvestment.company_id == company_id,
            PEFundInvestment.status == "Active",
        ).order_by(PEFundInvestment.investment_date.desc()).limit(1)
    ).scalar_one_or_none()

    if investment and investment.entry_ev_ebitda_multiple:
        entry_mult = float(investment.entry_ev_ebitda_multiple)
        # Compare to latest sector deal multiples
        sector_mults = db.execute(
            select(PEDeal.ev_ebitda_multiple)
            .join(PEPortfolioCompany, PEPortfolioCompany.id == PEDeal.company_id)
            .where(
                PEPortfolioCompany.industry == company.industry,
                PEDeal.ev_ebitda_multiple.isnot(None),
                PEDeal.status == "Closed",
            )
            .order_by(PEDeal.closed_date.desc())
            .limit(5)
        ).scalars().all()

        if sector_mults:
            avg_current = sum(float(m) for m in sector_mults) / len(sector_mults)
            if avg_current > entry_mult * 1.1:
                raw = min(100, raw + 15)
                explanation_parts.append(f"Multiple expansion: entry {entry_mult:.1f}x → market {avg_current:.1f}x")
            elif avg_current < entry_mult * 0.9:
                raw = max(0, raw - 10)
                explanation_parts.append(f"Multiple compression: entry {entry_mult:.1f}x → market {avg_current:.1f}x")
                recs.append("Sector multiples have compressed — operational improvements needed to offset.")

    return SubScore(
        dimension="market_timing", label="Market Timing",
        weight=weight, raw_score=raw, weighted_score=raw * weight,
        grade=_letter_grade(raw),
        explanation="; ".join(explanation_parts) if explanation_parts else "Insufficient market data.",
        recommendations=recs,
    )


def _score_regulatory_risk(
    db: Session, company_id: int
) -> SubScore:
    """Regulatory Risk (10%): default to neutral, flag if data exists."""
    weight = 0.10
    recs = []
    raw = 70  # default to B — assume no major issues without data

    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()

    explanation_parts = ["No regulatory findings in database (default to favorable)"]

    if company:
        # Industries with higher regulatory scrutiny
        high_risk_industries = {"Healthcare", "Financial Services", "Energy", "Chemicals"}
        if company.industry in high_risk_industries:
            raw = 60
            explanation_parts = [f"Sector ({company.industry}) has elevated regulatory risk"]
            recs.append(f"Conduct regulatory DD — {company.industry} is a high-scrutiny sector.")

    return SubScore(
        dimension="regulatory_risk", label="Regulatory Risk",
        weight=weight, raw_score=raw, weighted_score=raw * weight,
        grade=_letter_grade(raw),
        explanation="; ".join(explanation_parts),
        recommendations=recs,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def score_exit_readiness(
    db: Session,
    company_id: int,
) -> Optional[ExitReadinessResult]:
    """
    Compute exit readiness score for a portfolio company.

    Returns ExitReadinessResult with composite score, sub-scores,
    letter grade, and actionable recommendations. Returns None if
    company not found.
    """
    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()
    if not company:
        return None

    # Compute all sub-scores
    sub_scores = [
        _score_financial_health(db, company_id),
        _score_market_position(db, company_id),
        _score_management_quality(db, company_id),
        _score_data_room_readiness(db, company_id),
        _score_market_timing(db, company_id),
        _score_regulatory_risk(db, company_id),
    ]

    # Composite score
    composite = sum(s.weighted_score for s in sub_scores)
    composite = max(0, min(100, round(composite, 1)))

    # Gather all recommendations, prioritizing from lowest-scoring dimensions
    sorted_subs = sorted(sub_scores, key=lambda s: s.raw_score)
    all_recs = []
    for s in sorted_subs:
        for r in s.recommendations:
            if r not in all_recs:
                all_recs.append(r)

    # Identify data gaps
    data_gaps = []
    for s in sub_scores:
        if s.grade in ("D", "F") and "data" in s.explanation.lower():
            data_gaps.append(f"{s.label}: {s.explanation}")

    # Confidence based on data coverage
    scores_with_data = sum(1 for s in sub_scores if s.grade not in ("D", "F"))
    if scores_with_data >= 5:
        confidence = "high"
    elif scores_with_data >= 3:
        confidence = "medium"
    else:
        confidence = "low"

    return ExitReadinessResult(
        company_id=company_id,
        company_name=company.name,
        composite_score=composite,
        grade=_letter_grade(composite),
        sub_scores=sub_scores,
        recommendations=all_recs,
        confidence=confidence,
        data_gaps=data_gaps,
    )
