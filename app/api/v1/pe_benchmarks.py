"""
PE Benchmarking & Exit Readiness API endpoints.

Endpoints for:
- Financial benchmarking (single company + portfolio heatmap)
- Exit readiness scoring
- Leadership network graph
- Demo data seeding
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import String, cast, delete, select
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.pe_models import (
    PECompanyFinancials,
    PECompanyLeadership,
    PECompanyNews,
    PECompetitorMapping,
    PEDeal,
    PEDealParticipant,
    PEFirm,
    PEFirmPeople,
    PEFund,
    PEFundInvestment,
    PEFundPerformance,
    PEPerson,
    PEPortfolioCompany,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pe", tags=["PE Intelligence - Benchmarks"])


# =============================================================================
# Response Models
# =============================================================================


class MetricBenchmarkResponse(BaseModel):
    metric: str
    label: str
    value: Optional[float] = None
    industry_median: Optional[float] = None
    portfolio_avg: Optional[float] = None
    top_quartile: Optional[float] = None
    bottom_quartile: Optional[float] = None
    percentile: Optional[int] = None
    trend: Optional[str] = None
    peer_count: int = 0


class CompanyBenchmarkResponse(BaseModel):
    company_id: int
    company_name: str
    industry: Optional[str] = None
    fiscal_year: int
    metrics: List[MetricBenchmarkResponse] = []
    overall_percentile: Optional[int] = None
    data_quality: str = "high"


class HeatmapCellResponse(BaseModel):
    company_id: int
    company_name: str
    industry: Optional[str] = None
    status: str
    metrics: Dict[str, Optional[int]] = {}


class SubScoreResponse(BaseModel):
    dimension: str
    label: str
    weight: float
    raw_score: float
    weighted_score: float
    grade: str
    explanation: str
    recommendations: List[str] = []


class ExitReadinessResponse(BaseModel):
    company_id: int
    company_name: str
    composite_score: float
    grade: str
    sub_scores: List[SubScoreResponse] = []
    recommendations: List[str] = []
    confidence: str = "high"
    data_gaps: List[str] = []


class SeedDemoResponse(BaseModel):
    status: str
    tables: Dict[str, int] = {}
    total_rows: int = 0


class GraphNodeResponse(BaseModel):
    id: str
    name: str
    type: str  # firm, pe_person, company, executive
    title: Optional[str] = None
    industry: Optional[str] = None


class GraphLinkResponse(BaseModel):
    source: str
    target: str
    type: str  # employment, board_seat, management


class LeadershipGraphResponse(BaseModel):
    firm_id: int
    firm_name: str
    nodes: List[GraphNodeResponse] = []
    links: List[GraphLinkResponse] = []


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/seed-demo", response_model=SeedDemoResponse)
async def seed_demo_data(db: Session = Depends(get_db)):
    """
    Seed PE demo data (3 firms, 6 funds, 24 companies, financials, people, deals).
    Idempotent — safe to run multiple times.
    """
    from app.sources.pe.demo_seeder import seed_pe_demo_data

    try:
        counts = await seed_pe_demo_data(db)
        return SeedDemoResponse(
            status="success",
            tables=counts,
            total_rows=sum(counts.values()),
        )
    except Exception as e:
        logger.exception("Demo seeder failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Seeder failed: {str(e)}")


@router.delete("/seed-demo")
async def cleanup_demo_data(db: Session = Depends(get_db)):
    """
    Remove all demo seeder data in correct FK order.
    Returns count of deleted rows per table.
    """
    counts: Dict[str, int] = {}

    try:
        # Identify demo entities by data_source tags
        demo_firm_ids = [
            r[0] for r in db.execute(
                select(PEFirm.id).where(
                    cast(PEFirm.data_sources, String).like("%demo_seeder%")
                )
            ).all()
        ]
        demo_fund_ids = [
            r[0] for r in db.execute(
                select(PEFund.id).where(PEFund.data_source == "demo_seeder")
            ).all()
        ]
        demo_company_ids = [
            r[0] for r in db.execute(
                select(PEPortfolioCompany.id).where(
                    PEPortfolioCompany.data_source == "demo_seeder"
                )
            ).all()
        ]
        demo_deal_ids = [
            r[0] for r in db.execute(
                select(PEDeal.id).where(PEDeal.company_id.in_(demo_company_ids))
            ).all()
        ] if demo_company_ids else []
        demo_person_ids = [
            r[0] for r in db.execute(
                select(PEPerson.id).where(
                    cast(PEPerson.data_sources, String).like("%demo_seeder%")
                )
            ).all()
        ]

        # Delete in FK order (children first)
        # 1. Fund investments
        if demo_fund_ids:
            r = db.execute(
                delete(PEFundInvestment).where(PEFundInvestment.fund_id.in_(demo_fund_ids))
            )
            counts["pe_fund_investments"] = r.rowcount

        # 2. Deal participants
        if demo_deal_ids:
            r = db.execute(
                delete(PEDealParticipant).where(PEDealParticipant.deal_id.in_(demo_deal_ids))
            )
            counts["pe_deal_participants"] = r.rowcount

        # 3. Deals
        if demo_company_ids:
            r = db.execute(
                delete(PEDeal).where(PEDeal.company_id.in_(demo_company_ids))
            )
            counts["pe_deals"] = r.rowcount

        # 4. Company news
        r = db.execute(
            delete(PECompanyNews).where(
                PECompanyNews.source_url.like("%example.com%")
            )
        )
        counts["pe_company_news"] = r.rowcount

        # 5. Competitor mappings
        r = db.execute(
            delete(PECompetitorMapping).where(
                PECompetitorMapping.data_source == "demo_seeder"
            )
        )
        counts["pe_competitor_mappings"] = r.rowcount

        # 6. Company leadership
        r = db.execute(
            delete(PECompanyLeadership).where(
                PECompanyLeadership.data_source == "demo_seeder"
            )
        )
        counts["pe_company_leadership"] = r.rowcount

        # 7. Firm-people links
        if demo_person_ids:
            r = db.execute(
                delete(PEFirmPeople).where(PEFirmPeople.person_id.in_(demo_person_ids))
            )
            counts["pe_firm_people"] = r.rowcount

        # 8. People
        if demo_person_ids:
            r = db.execute(
                delete(PEPerson).where(PEPerson.id.in_(demo_person_ids))
            )
            counts["pe_people"] = r.rowcount

        # 9. Company financials
        r = db.execute(
            delete(PECompanyFinancials).where(
                PECompanyFinancials.data_source == "demo_seeder"
            )
        )
        counts["pe_company_financials"] = r.rowcount

        # 10. Fund performance
        if demo_fund_ids:
            r = db.execute(
                delete(PEFundPerformance).where(
                    PEFundPerformance.fund_id.in_(demo_fund_ids)
                )
            )
            counts["pe_fund_performance"] = r.rowcount

        # 11. Portfolio companies
        if demo_company_ids:
            r = db.execute(
                delete(PEPortfolioCompany).where(
                    PEPortfolioCompany.id.in_(demo_company_ids)
                )
            )
            counts["pe_portfolio_companies"] = r.rowcount

        # 12. Funds
        if demo_fund_ids:
            r = db.execute(
                delete(PEFund).where(PEFund.id.in_(demo_fund_ids))
            )
            counts["pe_funds"] = r.rowcount

        # 13. Firms
        if demo_firm_ids:
            r = db.execute(
                delete(PEFirm).where(PEFirm.id.in_(demo_firm_ids))
            )
            counts["pe_firms"] = r.rowcount

        db.commit()

        total = sum(counts.values())
        logger.info("Demo cleanup complete: %d total rows deleted", total)
        return {"status": "cleaned", "tables": counts, "total_deleted": total}

    except Exception as e:
        db.rollback()
        logger.exception("Demo cleanup failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")


@router.get("/benchmarks/{company_id}", response_model=CompanyBenchmarkResponse)
async def get_company_benchmarks(
    company_id: int,
    fiscal_year: Optional[int] = Query(None, description="Fiscal year (defaults to most recent)"),
    db: Session = Depends(get_db),
):
    """
    Benchmark a portfolio company against industry peers.

    Returns percentile ranks for revenue growth, EBITDA margin,
    revenue per employee, debt/EBITDA, and more.
    """
    from app.core.pe_benchmarking import benchmark_company

    result = benchmark_company(db, company_id, fiscal_year)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    return CompanyBenchmarkResponse(
        company_id=result.company_id,
        company_name=result.company_name,
        industry=result.industry,
        fiscal_year=result.fiscal_year,
        metrics=[
            MetricBenchmarkResponse(
                metric=m.metric,
                label=m.label,
                value=m.value,
                industry_median=m.industry_median,
                portfolio_avg=m.portfolio_avg,
                top_quartile=m.top_quartile,
                bottom_quartile=m.bottom_quartile,
                percentile=m.percentile,
                trend=m.trend,
                peer_count=m.peer_count,
            )
            for m in result.metrics
        ],
        overall_percentile=result.overall_percentile,
        data_quality=result.data_quality,
    )


@router.get("/benchmarks/portfolio/{firm_id}", response_model=List[HeatmapCellResponse])
async def get_portfolio_heatmap(
    firm_id: int,
    fiscal_year: Optional[int] = Query(None, description="Fiscal year (defaults to most recent)"),
    db: Session = Depends(get_db),
):
    """
    Portfolio heatmap — percentile rank per metric for each company in a firm's portfolio.
    """
    from app.core.pe_benchmarking import benchmark_portfolio

    rows = benchmark_portfolio(db, firm_id, fiscal_year)
    return [
        HeatmapCellResponse(
            company_id=r.company_id,
            company_name=r.company_name,
            industry=r.industry,
            status=r.status,
            metrics=r.metrics,
        )
        for r in rows
    ]


@router.get("/exit-readiness/{company_id}", response_model=ExitReadinessResponse)
async def get_exit_readiness(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Exit readiness score for a portfolio company.

    Returns composite 0-100 score with 6 weighted sub-scores:
    Financial Health (30%), Market Position (20%), Management Quality (15%),
    Data Room Readiness (15%), Market Timing (10%), Regulatory Risk (10%).
    """
    from app.core.pe_exit_scoring import score_exit_readiness

    result = score_exit_readiness(db, company_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    return ExitReadinessResponse(
        company_id=result.company_id,
        company_name=result.company_name,
        composite_score=result.composite_score,
        grade=result.grade,
        sub_scores=[
            SubScoreResponse(
                dimension=s.dimension,
                label=s.label,
                weight=s.weight,
                raw_score=s.raw_score,
                weighted_score=s.weighted_score,
                grade=s.grade,
                explanation=s.explanation,
                recommendations=s.recommendations,
            )
            for s in result.sub_scores
        ],
        recommendations=result.recommendations,
        confidence=result.confidence,
        data_gaps=result.data_gaps,
    )


@router.get("/leadership-graph/{firm_id}", response_model=LeadershipGraphResponse)
async def get_leadership_graph(
    firm_id: int,
    db: Session = Depends(get_db),
):
    """
    Leadership network graph for a PE firm's portfolio.

    Returns a force-directed graph with 4 node types (firm, pe_person, company, executive)
    and 3 link types (employment, board_seat, management).
    """
    # Look up firm
    firm = db.execute(
        select(PEFirm).where(PEFirm.id == firm_id)
    ).scalar_one_or_none()
    if not firm:
        raise HTTPException(status_code=404, detail=f"Firm {firm_id} not found")

    nodes = []
    links = []
    seen_nodes = set()

    # Node: the firm itself
    firm_node_id = f"firm_{firm.id}"
    nodes.append(GraphNodeResponse(id=firm_node_id, name=firm.name, type="firm"))
    seen_nodes.add(firm_node_id)

    # PE team members at this firm
    firm_people = db.execute(
        select(PEFirmPeople, PEPerson)
        .join(PEPerson, PEPerson.id == PEFirmPeople.person_id)
        .where(PEFirmPeople.firm_id == firm_id, PEFirmPeople.is_current == True)
    ).all()

    pe_person_ids = set()
    for fp, person in firm_people:
        node_id = f"person_{person.id}"
        if node_id not in seen_nodes:
            nodes.append(GraphNodeResponse(
                id=node_id, name=person.full_name, type="pe_person", title=fp.title,
            ))
            seen_nodes.add(node_id)
        pe_person_ids.add(person.id)
        links.append(GraphLinkResponse(source=firm_node_id, target=node_id, type="employment"))

    # Find portfolio companies via funds → investments
    funds = db.execute(
        select(PEFund.id).where(PEFund.firm_id == firm_id)
    ).scalars().all()

    company_ids = set()
    if funds:
        investments = db.execute(
            select(PEFundInvestment.company_id)
            .where(PEFundInvestment.fund_id.in_(funds))
            .distinct()
        ).scalars().all()
        company_ids = set(investments)

    # Add company nodes
    for co_id in company_ids:
        company = db.execute(
            select(PEPortfolioCompany).where(PEPortfolioCompany.id == co_id)
        ).scalar_one_or_none()
        if not company:
            continue
        co_node_id = f"company_{company.id}"
        if co_node_id not in seen_nodes:
            nodes.append(GraphNodeResponse(
                id=co_node_id, name=company.name, type="company", industry=company.industry,
            ))
            seen_nodes.add(co_node_id)

        # Leadership records for this company
        leaders = db.execute(
            select(PECompanyLeadership, PEPerson)
            .join(PEPerson, PEPerson.id == PECompanyLeadership.person_id)
            .where(
                PECompanyLeadership.company_id == co_id,
                PECompanyLeadership.is_current == True,
            )
        ).all()

        for leadership, person in leaders:
            person_node_id = f"person_{person.id}" if person.id in pe_person_ids else f"exec_{person.id}"

            if person_node_id not in seen_nodes:
                node_type = "pe_person" if person.id in pe_person_ids else "executive"
                nodes.append(GraphNodeResponse(
                    id=person_node_id, name=person.full_name, type=node_type,
                    title=leadership.title,
                ))
                seen_nodes.add(person_node_id)

            # Link type depends on whether this is a board seat or management role
            if leadership.is_board_member:
                links.append(GraphLinkResponse(
                    source=person_node_id, target=co_node_id, type="board_seat",
                ))
            else:
                links.append(GraphLinkResponse(
                    source=co_node_id, target=person_node_id, type="management",
                ))

    return LeadershipGraphResponse(
        firm_id=firm.id,
        firm_name=firm.name,
        nodes=nodes,
        links=links,
    )


# =============================================================================
# Potential Buyers
# =============================================================================


class BuyerCandidate(BaseModel):
    name: str
    buyer_type: str  # Strategic, Financial, PE Platform
    fit_score: int = Field(ge=0, le=100)
    rationale: str
    is_public: bool = False
    ticker: Optional[str] = None
    estimated_capacity_usd: Optional[float] = None


class PotentialBuyersResponse(BaseModel):
    company_id: int
    company_name: str
    industry: Optional[str] = None
    strategic_buyers: List[BuyerCandidate] = []
    financial_buyers: List[BuyerCandidate] = []
    total_candidates: int = 0


@router.get("/buyer-analysis/{company_id}", response_model=PotentialBuyersResponse)
async def get_potential_buyers(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Identify potential buyers for a portfolio company.

    Generates strategic and financial buyer candidates based on:
    - Competitor mappings (larger competitors = strategic acquirers)
    - Industry vertical and deal history
    - PE firms active in the sector
    """
    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    strategic = []
    financial = []

    # 1. Competitors as strategic buyers (larger ones are acquirers)
    competitors = db.execute(
        select(PECompetitorMapping).where(PECompetitorMapping.company_id == company_id)
    ).scalars().all()

    for comp in competitors:
        if comp.relative_size in ("Larger", "Similar") and comp.market_position in ("Leader", "Challenger"):
            fit = 85 if comp.relative_size == "Larger" and comp.market_position == "Leader" else 70
            rationale_parts = [f"{comp.competitor_type} competitor"]
            if comp.relative_size == "Larger":
                rationale_parts.append("larger platform seeking tuck-in")
            if comp.is_public:
                rationale_parts.append(f"public ({comp.ticker}), acquisition currency available")
            strategic.append(BuyerCandidate(
                name=comp.competitor_name,
                buyer_type="Strategic",
                fit_score=fit,
                rationale="; ".join(rationale_parts),
                is_public=comp.is_public or False,
                ticker=comp.ticker,
            ))

    # 2. PE-backed competitors as "PE Platform" buyers
    for comp in competitors:
        if comp.is_pe_backed and comp.pe_owner:
            strategic.append(BuyerCandidate(
                name=f"{comp.competitor_name} (backed by {comp.pe_owner})",
                buyer_type="PE Platform",
                fit_score=65,
                rationale=f"PE-backed competitor seeking add-on in {company.industry or 'sector'}",
                is_public=False,
            ))

    # 3. Historical buyers in same industry (from past deals)
    industry_deals = db.execute(
        select(PEDeal).join(
            PEPortfolioCompany, PEPortfolioCompany.id == PEDeal.company_id
        ).where(
            PEPortfolioCompany.industry == company.industry,
            PEDeal.deal_type == "Exit",
            PEDeal.buyer_name.isnot(None),
        )
    ).scalars().all()

    seen_buyers = {s.name for s in strategic}
    for deal in industry_deals:
        if deal.buyer_name and deal.buyer_name not in seen_buyers:
            strategic.append(BuyerCandidate(
                name=deal.buyer_name,
                buyer_type="Strategic",
                fit_score=75,
                rationale=f"Prior acquirer in {company.industry} — bought {deal.deal_name.split(' Sale')[0] if ' Sale' in deal.deal_name else 'similar company'}",
            ))
            seen_buyers.add(deal.buyer_name)

    # 4. Financial buyers — PE firms active in same industry
    # Find current owner firm to exclude
    owner_firm_id = db.execute(
        select(PEFund.firm_id)
        .join(PEFundInvestment, PEFundInvestment.fund_id == PEFund.id)
        .where(PEFundInvestment.company_id == company_id)
        .limit(1)
    ).scalar_one_or_none()

    industry_firms = db.execute(
        select(PEFirm).where(
            PEFirm.id != owner_firm_id if owner_firm_id else True
        )
    ).scalars().all()

    for firm in industry_firms:
        # Check if firm has portfolio companies in same industry
        same_industry = db.execute(
            select(PEPortfolioCompany).join(
                PEFundInvestment, PEFundInvestment.company_id == PEPortfolioCompany.id
            ).join(
                PEFund, PEFund.id == PEFundInvestment.fund_id
            ).where(
                PEFund.firm_id == firm.id,
                PEPortfolioCompany.industry == company.industry,
            )
        ).scalars().first()

        if same_industry:
            financial.append(BuyerCandidate(
                name=firm.name,
                buyer_type="Financial",
                fit_score=70,
                rationale=f"Active PE investor in {company.industry} with existing platform ({same_industry.name})",
            ))

    # Sort by fit score
    strategic.sort(key=lambda x: x.fit_score, reverse=True)
    financial.sort(key=lambda x: x.fit_score, reverse=True)

    return PotentialBuyersResponse(
        company_id=company.id,
        company_name=company.name,
        industry=company.industry,
        strategic_buyers=strategic,
        financial_buyers=financial,
        total_candidates=len(strategic) + len(financial),
    )


# =============================================================================
# Data Room Package
# =============================================================================


class DataRoomFinancials(BaseModel):
    fiscal_year: int
    revenue_usd: Optional[float] = None
    ebitda_usd: Optional[float] = None
    ebitda_margin_pct: Optional[float] = None
    gross_margin_pct: Optional[float] = None
    revenue_growth_pct: Optional[float] = None
    employees: Optional[int] = None


class DataRoomLeader(BaseModel):
    name: str
    title: str
    role_category: Optional[str] = None
    is_ceo: bool = False
    is_cfo: bool = False
    is_board_member: bool = False
    appointed_by_pe: Optional[bool] = None
    tenure_years: Optional[float] = None


class DataRoomCompetitor(BaseModel):
    name: str
    competitor_type: Optional[str] = None
    relative_size: Optional[str] = None
    market_position: Optional[str] = None
    is_public: bool = False
    ticker: Optional[str] = None


class DataRoomNewsItem(BaseModel):
    title: str
    source: Optional[str] = None
    published_date: Optional[str] = None
    sentiment: Optional[str] = None
    news_type: Optional[str] = None
    summary: Optional[str] = None


class DataRoomPackageResponse(BaseModel):
    company_id: int
    company_name: str
    industry: Optional[str] = None
    status: Optional[str] = None
    headquarters: Optional[str] = None
    founded_year: Optional[int] = None
    employee_count: Optional[int] = None
    website: Optional[str] = None
    financials: List[DataRoomFinancials] = []
    leadership: List[DataRoomLeader] = []
    competitors: List[DataRoomCompetitor] = []
    recent_news: List[DataRoomNewsItem] = []
    benchmarks: Optional[Dict[str, Any]] = None
    exit_readiness: Optional[Dict[str, Any]] = None
    completeness_pct: int = 0
    missing_sections: List[str] = []


@router.get("/data-room/{company_id}", response_model=DataRoomPackageResponse)
async def get_data_room_package(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Assemble a complete data room package for a portfolio company.

    Combines financials, leadership, competitors, news, benchmarks,
    and exit readiness into a single response for buyer due diligence.
    """
    from app.core.pe_benchmarking import benchmark_company
    from app.core.pe_exit_scoring import score_exit_readiness

    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    sections_present = 0
    total_sections = 6
    missing = []

    # 1. Financials (last 5 years)
    financials_raw = db.execute(
        select(PECompanyFinancials)
        .where(PECompanyFinancials.company_id == company_id)
        .order_by(PECompanyFinancials.fiscal_year.desc())
        .limit(5)
    ).scalars().all()

    financials = []
    for f in financials_raw:
        financials.append(DataRoomFinancials(
            fiscal_year=f.fiscal_year,
            revenue_usd=float(f.revenue_usd) if f.revenue_usd else None,
            ebitda_usd=float(f.ebitda_usd) if f.ebitda_usd else None,
            ebitda_margin_pct=float(f.ebitda_margin_pct) if f.ebitda_margin_pct else None,
            gross_margin_pct=float(f.gross_margin_pct) if f.gross_margin_pct else None,
            revenue_growth_pct=float(f.revenue_growth_pct) if f.revenue_growth_pct else None,
            employees=None,
        ))
    if financials:
        sections_present += 1
    else:
        missing.append("financials")

    # 2. Leadership
    leaders_raw = db.execute(
        select(PECompanyLeadership, PEPerson)
        .join(PEPerson, PEPerson.id == PECompanyLeadership.person_id)
        .where(
            PECompanyLeadership.company_id == company_id,
            PECompanyLeadership.is_current == True,
        )
    ).all()

    leadership = []
    for l_rec, person in leaders_raw:
        tenure = None
        if l_rec.start_date:
            tenure = round((date.today() - l_rec.start_date).days / 365.25, 1)
        leadership.append(DataRoomLeader(
            name=person.full_name,
            title=l_rec.title,
            role_category=l_rec.role_category,
            is_ceo=l_rec.is_ceo or False,
            is_cfo=l_rec.is_cfo or False,
            is_board_member=l_rec.is_board_member or False,
            appointed_by_pe=l_rec.appointed_by_pe,
            tenure_years=tenure,
        ))
    if leadership:
        sections_present += 1
    else:
        missing.append("leadership")

    # 3. Competitors
    competitors_raw = db.execute(
        select(PECompetitorMapping).where(PECompetitorMapping.company_id == company_id)
    ).scalars().all()

    competitors = [
        DataRoomCompetitor(
            name=c.competitor_name,
            competitor_type=c.competitor_type,
            relative_size=c.relative_size,
            market_position=c.market_position,
            is_public=c.is_public or False,
            ticker=c.ticker,
        )
        for c in competitors_raw
    ]
    if competitors:
        sections_present += 1
    else:
        missing.append("competitive_landscape")

    # 4. Recent news
    news_raw = db.execute(
        select(PECompanyNews)
        .where(PECompanyNews.company_id == company_id)
        .order_by(PECompanyNews.published_date.desc())
        .limit(10)
    ).scalars().all()

    recent_news = [
        DataRoomNewsItem(
            title=n.title,
            source=n.source_name,
            published_date=n.published_date.strftime("%Y-%m-%d") if n.published_date else None,
            sentiment=n.sentiment,
            news_type=n.news_type,
            summary=n.summary,
        )
        for n in news_raw
    ]
    if recent_news:
        sections_present += 1
    else:
        missing.append("news_coverage")

    # 5. Benchmarks
    benchmarks_data = None
    try:
        bench = benchmark_company(db, company_id)
        if bench:
            benchmarks_data = {
                "overall_percentile": bench.overall_percentile,
                "metrics": {
                    m.metric: {"value": m.value, "percentile": m.percentile, "trend": m.trend}
                    for m in bench.metrics
                },
            }
            sections_present += 1
        else:
            missing.append("benchmarks")
    except Exception:
        missing.append("benchmarks")

    # 6. Exit readiness
    exit_data = None
    try:
        er = score_exit_readiness(db, company_id)
        if er:
            exit_data = {
                "composite_score": er.composite_score,
                "grade": er.grade,
                "confidence": er.confidence,
                "sub_scores": {
                    s.dimension: {"score": s.raw_score, "grade": s.grade}
                    for s in er.sub_scores
                },
                "top_recommendations": er.recommendations[:3],
            }
            sections_present += 1
        else:
            missing.append("exit_readiness")
    except Exception:
        missing.append("exit_readiness")

    completeness = int((sections_present / total_sections) * 100)

    return DataRoomPackageResponse(
        company_id=company.id,
        company_name=company.name,
        industry=company.industry,
        status=company.status,
        headquarters=f"{company.headquarters_city}, {company.headquarters_state}" if company.headquarters_city else None,
        founded_year=company.founded_year,
        employee_count=company.employee_count,
        website=company.website,
        financials=financials,
        leadership=leadership,
        competitors=competitors,
        recent_news=recent_news,
        benchmarks=benchmarks_data,
        exit_readiness=exit_data,
        completeness_pct=completeness,
        missing_sections=missing,
    )
