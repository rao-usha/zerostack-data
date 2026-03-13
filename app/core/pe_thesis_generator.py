"""
PE Investment Thesis Generator.

Uses LLM to generate structured investment theses for portfolio companies.
Caches results in pe_investment_theses table with a 24-hour TTL.
Falls back to cached thesis if LLM is unavailable.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.pe_models import PEInvestmentThesis, PEPortfolioCompany

logger = logging.getLogger(__name__)

THESIS_TTL_HOURS = 24

THESIS_SYSTEM_PROMPT = """You are a senior private equity analyst writing investment theses.
Given company data, produce a structured JSON investment thesis with these exact keys:

{
  "executive_summary": "2-3 sentence investment thesis",
  "strengths": ["strength1", "strength2", ...],
  "risks": ["risk1", "risk2", ...],
  "value_creation_levers": ["lever1", "lever2", ...],
  "exit_strategy": {
    "recommended_path": "Strategic Sale | IPO | Secondary Buyout | Dividend Recap",
    "target_timeline": "12-18 months | 18-24 months | 24-36 months",
    "rationale": "Why this exit path"
  },
  "comparable_multiples": {
    "entry_multiple": "estimated EV/EBITDA at entry",
    "current_implied": "current implied EV/EBITDA",
    "target_exit": "target exit EV/EBITDA"
  },
  "key_metrics_to_watch": ["metric1", "metric2", ...],
  "investment_recommendation": "Strong Buy | Buy | Hold | Sell",
  "confidence_level": "High | Medium | Low"
}

Be specific and data-driven. Reference actual numbers from the provided data."""


@dataclass
class ThesisResult:
    """Result of thesis generation or retrieval."""
    company_id: int
    company_name: str
    thesis_data: Dict[str, Any]
    generated_at: datetime
    model_used: Optional[str]
    cost_usd: Optional[float]
    from_cache: bool


def _build_company_context(db: Session, company_id: int) -> Optional[str]:
    """Build a text summary of company data for the LLM prompt."""
    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()
    if not company:
        return None

    parts = [
        f"Company: {company.name}",
        f"Industry: {company.industry or 'Unknown'}",
        f"Sub-industry: {company.sub_industry or 'N/A'}",
        f"Status: {company.status or 'Unknown'}",
        f"HQ: {company.headquarters_city or '?'}, {company.headquarters_state or '?'}",
        f"Founded: {company.founded_year or 'Unknown'}",
        f"Employees: {company.employee_count or 'Unknown'}",
    ]

    # Add financials
    from app.core.pe_models import PECompanyFinancials
    financials = db.execute(
        select(PECompanyFinancials)
        .where(PECompanyFinancials.company_id == company_id)
        .order_by(PECompanyFinancials.fiscal_year.desc())
        .limit(3)
    ).scalars().all()

    if financials:
        parts.append("\nFinancials:")
        for f in financials:
            parts.append(
                f"  FY{f.fiscal_year}: Rev=${float(f.revenue_usd or 0)/1e6:.1f}M, "
                f"EBITDA=${float(f.ebitda_usd or 0)/1e6:.1f}M, "
                f"Margin={float(f.ebitda_margin_pct or 0):.1f}%, "
                f"Growth={float(f.revenue_growth_pct or 0):.1f}%"
            )

    # Add exit readiness if available
    try:
        from app.core.pe_exit_scoring import score_exit_readiness
        er = score_exit_readiness(db, company_id)
        if er:
            parts.append(f"\nExit Readiness: {er.composite_score:.0f}/100 ({er.grade})")
            for s in er.sub_scores:
                parts.append(f"  {s.label}: {s.raw_score:.0f} ({s.grade})")
    except Exception:
        pass

    # Add deal score if available
    try:
        from app.core.pe_deal_scorer import score_deal
        ds = score_deal(db, company_id)
        if ds:
            parts.append(f"\nDeal Score: {ds.composite_score:.0f}/100 ({ds.grade})")
    except Exception:
        pass

    # Add competitors
    from app.core.pe_models import PECompetitorMapping
    competitors = db.execute(
        select(PECompetitorMapping).where(PECompetitorMapping.company_id == company_id)
    ).scalars().all()
    if competitors:
        parts.append(f"\nCompetitors ({len(competitors)}):")
        for c in competitors:
            parts.append(f"  {c.competitor_name} — {c.competitor_type}, {c.relative_size}, {c.market_position}")

    # Add leadership
    from app.core.pe_models import PECompanyLeadership, PEPerson
    leaders = db.execute(
        select(PECompanyLeadership, PEPerson)
        .join(PEPerson, PEPerson.id == PECompanyLeadership.person_id)
        .where(
            PECompanyLeadership.company_id == company_id,
            PECompanyLeadership.is_current == True,
        )
    ).all()
    if leaders:
        parts.append(f"\nLeadership ({len(leaders)}):")
        for l, person in leaders:
            parts.append(f"  {person.full_name} — {l.title}")

    return "\n".join(parts)


def _get_cached_thesis(
    db: Session, company_id: int, max_age_hours: int = THESIS_TTL_HOURS
) -> Optional[PEInvestmentThesis]:
    """Return cached thesis if fresh enough."""
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    return db.execute(
        select(PEInvestmentThesis)
        .where(
            PEInvestmentThesis.company_id == company_id,
            PEInvestmentThesis.generated_at >= cutoff,
        )
        .order_by(PEInvestmentThesis.generated_at.desc())
        .limit(1)
    ).scalar_one_or_none()


async def generate_thesis(
    db: Session,
    company_id: int,
    force_refresh: bool = False,
) -> Optional[ThesisResult]:
    """
    Generate or retrieve an investment thesis for a company.

    Returns cached thesis if <24h old, unless force_refresh=True.
    Falls back to any cached thesis if LLM call fails.
    """
    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()
    if not company:
        return None

    # Check cache first (unless forced refresh)
    if not force_refresh:
        cached = _get_cached_thesis(db, company_id)
        if cached:
            return ThesisResult(
                company_id=company_id,
                company_name=company.name,
                thesis_data=cached.thesis_data,
                generated_at=cached.generated_at,
                model_used=cached.model_used,
                cost_usd=float(cached.cost_usd) if cached.cost_usd else None,
                from_cache=True,
            )

    # Build context for LLM
    context = _build_company_context(db, company_id)
    if not context:
        return None

    prompt = f"Generate an investment thesis for this PE portfolio company:\n\n{context}"

    # Try LLM generation
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    provider = "openai" if os.environ.get("OPENAI_API_KEY") else "anthropic"

    if api_key:
        try:
            from app.agentic.llm_client import LLMClient
            client = LLMClient(
                provider=provider,
                api_key=api_key,
                max_tokens=2000,
                temperature=0.3,
            )

            response = await client.complete(
                prompt=prompt,
                system_prompt=THESIS_SYSTEM_PROMPT,
                json_mode=True,
            )

            thesis_data = response.parse_json()
            if thesis_data:
                # Store in DB
                record = PEInvestmentThesis(
                    company_id=company_id,
                    thesis_data=thesis_data,
                    model_used=response.model,
                    input_tokens=response.input_tokens,
                    output_tokens=response.output_tokens,
                    cost_usd=response.cost_usd,
                    generated_at=datetime.utcnow(),
                )
                db.add(record)
                db.commit()

                # Track cost
                try:
                    from app.core.llm_cost_tracker import get_cost_tracker
                    tracker = get_cost_tracker()
                    await tracker.record(
                        model=response.model,
                        input_tokens=response.input_tokens,
                        output_tokens=response.output_tokens,
                        source="pe_thesis_generator",
                        company_id=company_id,
                    )
                except Exception:
                    pass

                return ThesisResult(
                    company_id=company_id,
                    company_name=company.name,
                    thesis_data=thesis_data,
                    generated_at=record.generated_at,
                    model_used=response.model,
                    cost_usd=response.cost_usd,
                    from_cache=False,
                )

        except Exception as e:
            logger.warning("LLM thesis generation failed for company %d: %s", company_id, e)

    # Fallback: return any cached thesis (even stale)
    fallback = db.execute(
        select(PEInvestmentThesis)
        .where(PEInvestmentThesis.company_id == company_id)
        .order_by(PEInvestmentThesis.generated_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if fallback:
        return ThesisResult(
            company_id=company_id,
            company_name=company.name,
            thesis_data=fallback.thesis_data,
            generated_at=fallback.generated_at,
            model_used=fallback.model_used,
            cost_usd=float(fallback.cost_usd) if fallback.cost_usd else None,
            from_cache=True,
        )

    return None
