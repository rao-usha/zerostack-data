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
from sqlalchemy import String, cast, delete, func, select
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.pe_models import (
    PEAlert,
    PEAlertSubscription,
    PECashFlow,
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
    PEPortfolioSnapshot,
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


class FirmSummaryResponse(BaseModel):
    id: int
    name: str
    firm_type: Optional[str] = None
    primary_strategy: Optional[str] = None
    aum_usd_millions: Optional[float] = None
    headquarters_city: Optional[str] = None
    headquarters_state: Optional[str] = None
    sector_focus: Optional[List[str]] = None
    status: Optional[str] = None
    fund_count: int = 0
    company_count: int = 0


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
# Helpers
# =============================================================================


def _build_firm_summary(
    firm, fund_count: int, company_count: int,
) -> Dict[str, Any]:
    """Build a firm summary dict with counts."""
    return {
        "id": firm.id,
        "name": firm.name,
        "firm_type": firm.firm_type,
        "primary_strategy": firm.primary_strategy,
        "aum_usd_millions": float(firm.aum_usd_millions) if firm.aum_usd_millions else None,
        "headquarters_city": firm.headquarters_city,
        "headquarters_state": firm.headquarters_state,
        "sector_focus": firm.sector_focus,
        "status": firm.status,
        "fund_count": fund_count,
        "company_count": company_count,
    }


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/firms", response_model=List[FirmSummaryResponse], summary="List all PE firms")
async def list_firms(db: Session = Depends(get_db)) -> List[FirmSummaryResponse]:
    """Return all PE firms with their current IDs, fund counts, and company counts.

    Use this to discover firm IDs dynamically instead of hardcoding.
    """
    firms = db.execute(select(PEFirm).order_by(PEFirm.name)).scalars().all()
    results = []
    for firm in firms:
        fund_count = db.execute(
            select(func.count(PEFund.id)).where(PEFund.firm_id == firm.id)
        ).scalar() or 0
        # Count companies via fund investments
        company_count = db.execute(
            select(func.count(func.distinct(PEFundInvestment.company_id)))
            .join(PEFund, PEFundInvestment.fund_id == PEFund.id)
            .where(PEFund.firm_id == firm.id)
        ).scalar() or 0
        results.append(FirmSummaryResponse(**_build_firm_summary(firm, fund_count, company_count)))
    return results


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

            # 10b. Cash flows
            r = db.execute(
                delete(PECashFlow).where(
                    PECashFlow.fund_id.in_(demo_fund_ids)
                )
            )
            counts["pe_cash_flows"] = r.rowcount

        # 10c. Portfolio snapshots
        if demo_firm_ids:
            r = db.execute(
                delete(PEPortfolioSnapshot).where(
                    PEPortfolioSnapshot.firm_id.in_(demo_firm_ids)
                )
            )
            counts["pe_portfolio_snapshots"] = r.rowcount

        # 10d. Alerts
        if demo_firm_ids:
            r = db.execute(
                delete(PEAlert).where(
                    PEAlert.firm_id.in_(demo_firm_ids)
                )
            )
            counts["pe_alerts"] = r.rowcount

        # 10e. Alert subscriptions
        if demo_firm_ids:
            r = db.execute(
                delete(PEAlertSubscription).where(
                    PEAlertSubscription.firm_id.in_(demo_firm_ids)
                )
            )
            counts["pe_alert_subscriptions"] = r.rowcount

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


# =============================================================================
# Company 360 — Unified Intelligence View
# =============================================================================


class Company360SectionResponse(BaseModel):
    """Generic section wrapper for optional data."""
    pass


class Company360Response(BaseModel):
    company_id: int
    company_name: str
    profile: Optional[Dict[str, Any]] = None
    benchmarks: Optional[Dict[str, Any]] = None
    exit_readiness: Optional[Dict[str, Any]] = None
    deal_score: Optional[Dict[str, Any]] = None
    comparable_transactions: Optional[Dict[str, Any]] = None
    buyer_analysis: Optional[Dict[str, Any]] = None
    leadership: List[Dict[str, Any]] = []
    competitors: List[Dict[str, Any]] = []
    recent_alerts: List[Dict[str, Any]] = []
    pipeline_deals: List[Dict[str, Any]] = []
    snapshot_trend: Optional[Dict[str, Any]] = None
    thesis: Optional[Dict[str, Any]] = None
    data_completeness: int = 0


@router.get("/companies/{company_id}/360", response_model=Company360Response)
async def get_company_360_view(
    company_id: int,
    firm_id: Optional[int] = Query(None, description="Optional firm ID for firm-scoped data"),
    db: Session = Depends(get_db),
):
    """
    Unified Company 360 view — all intelligence about a PE target in a single response.

    Aggregates 12 data sections: profile, benchmarks, exit readiness, deal score,
    comparable transactions, buyer analysis, leadership, competitors, alerts,
    pipeline deals, snapshot trend, and investment thesis.

    Missing sections return null/empty — never error.
    """
    from app.core.pe_company_360 import get_company_360

    result = get_company_360(db, company_id, firm_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    return Company360Response(
        company_id=result.company_id,
        company_name=result.company_name,
        profile=result.profile,
        benchmarks=result.benchmarks,
        exit_readiness=result.exit_readiness,
        deal_score=result.deal_score,
        comparable_transactions=result.comparable_transactions,
        buyer_analysis=result.buyer_analysis,
        leadership=result.leadership,
        competitors=result.competitors,
        recent_alerts=result.recent_alerts,
        pipeline_deals=result.pipeline_deals,
        snapshot_trend=result.snapshot_trend,
        thesis=result.thesis,
        data_completeness=result.data_completeness,
    )


# =============================================================================
# Investment Thesis
# =============================================================================


class ThesisResponse(BaseModel):
    company_id: int
    company_name: str
    thesis: Optional[Dict[str, Any]] = None
    generated_at: Optional[str] = None
    model_used: Optional[str] = None
    cost_usd: Optional[float] = None
    from_cache: bool = False


@router.get("/companies/{company_id}/thesis", response_model=ThesisResponse)
async def get_investment_thesis(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Get the cached investment thesis for a company.

    Returns the most recent thesis if available.
    Use POST .../thesis/refresh to generate a new one.
    """
    from app.core.pe_thesis_generator import generate_thesis

    result = await generate_thesis(db, company_id, force_refresh=False)
    if result is None:
        # Company exists but no thesis — return empty
        company = db.execute(
            select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
        ).scalar_one_or_none()
        if not company:
            raise HTTPException(status_code=404, detail=f"Company {company_id} not found")
        return ThesisResponse(company_id=company_id, company_name=company.name)

    return ThesisResponse(
        company_id=result.company_id,
        company_name=result.company_name,
        thesis=result.thesis_data,
        generated_at=result.generated_at.isoformat() if result.generated_at else None,
        model_used=result.model_used,
        cost_usd=result.cost_usd,
        from_cache=result.from_cache,
    )


@router.post("/companies/{company_id}/thesis/refresh", response_model=ThesisResponse)
async def refresh_investment_thesis(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Force-regenerate the investment thesis for a company.

    Calls the LLM to produce a fresh thesis regardless of cache age.
    Requires OPENAI_API_KEY or ANTHROPIC_API_KEY to be set.
    """
    from app.core.pe_thesis_generator import generate_thesis

    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    result = await generate_thesis(db, company_id, force_refresh=True)
    if result is None:
        return ThesisResponse(
            company_id=company_id,
            company_name=company.name,
        )

    return ThesisResponse(
        company_id=result.company_id,
        company_name=result.company_name,
        thesis=result.thesis_data,
        generated_at=result.generated_at.isoformat() if result.generated_at else None,
        model_used=result.model_used,
        cost_usd=result.cost_usd,
        from_cache=result.from_cache,
    )


# =============================================================================
# Fragmentation Scoring Endpoints
# =============================================================================


class MarketDetail(BaseModel):
    county_fips: Optional[str] = None
    state_fips: Optional[str] = None
    geo_name: Optional[str] = None
    score: float = 0
    grade: str = "F"
    establishments: Optional[int] = None
    employees: Optional[int] = None
    hhi: Optional[float] = None
    small_biz_pct: Optional[float] = None
    avg_employees_per_estab: Optional[float] = None


class FragmentationResponse(BaseModel):
    naics_code: str
    naics_description: Optional[str] = None
    national_score: float = 0
    national_grade: str = "F"
    total_establishments: int = 0
    total_employees: int = 0
    county_count: int = 0
    avg_hhi: Optional[float] = None
    avg_small_biz_pct: Optional[float] = None
    avg_estab_size: Optional[float] = None
    year: int = 2021
    top_markets: List[MarketDetail] = []


class IndustryScanItem(BaseModel):
    naics_code: str
    naics_description: Optional[str] = None
    national_score: float = 0
    total_establishments: int = 0
    error: Optional[str] = None


class StateSummary(BaseModel):
    total_establishments: int = 0
    total_employees: int = 0
    county_count: int = 0
    state_score: float = 0
    state_grade: str = "F"
    avg_hhi: Optional[float] = None
    avg_small_biz_pct: Optional[float] = None


class RollUpTargetsResponse(BaseModel):
    naics_code: str
    naics_description: Optional[str] = None
    state: str
    year: int = 2021
    targets: List[MarketDetail] = []
    state_summary: Optional[StateSummary] = None


@router.get(
    "/fragmentation/scan",
    response_model=List[IndustryScanItem],
    summary="Scan multiple NAICS codes for fragmentation",
)
async def scan_fragmentation(
    naics_codes: str = Query(
        ...,
        description="Comma-separated NAICS codes (e.g. '621111,541330,238220')",
    ),
    year: int = Query(2021, description="Data year"),
    db: Session = Depends(get_db),
) -> List[IndustryScanItem]:
    """Scan multiple industries and rank by fragmentation score.

    Pass NAICS codes as comma-separated string. Returns ranked list
    from most to least fragmented.
    """
    from app.core.pe_fragmentation import FragmentationScorer

    codes = [c.strip() for c in naics_codes.split(",") if c.strip()]
    if not codes:
        return []

    scorer = FragmentationScorer(db)
    results = await scorer.scan_industries(codes, year=year)

    return [
        IndustryScanItem(
            naics_code=r["naics_code"],
            naics_description=r.get("naics_description"),
            national_score=r.get("national_score", 0),
            total_establishments=r.get("total_establishments", 0),
            error=r.get("error"),
        )
        for r in results
    ]


@router.get(
    "/fragmentation/{naics_code}",
    response_model=FragmentationResponse,
    summary="Score industry fragmentation for a NAICS code",
)
async def get_fragmentation_score(
    naics_code: str,
    year: int = Query(2021, description="Data year"),
    top_n: int = Query(20, description="Number of top markets to return"),
    db: Session = Depends(get_db),
) -> FragmentationResponse:
    """Score how fragmented an industry is nationally using Census CBP data.

    Higher score = more fragmented = better roll-up opportunity.
    Returns national score plus top county-level markets ranked by fragmentation.
    """
    from app.core.pe_fragmentation import FragmentationScorer

    scorer = FragmentationScorer(db)
    result = await scorer.score_industry(naics_code, year=year)

    # Trim top markets to requested count
    markets = result.get("top_markets", [])[:top_n]

    return FragmentationResponse(
        naics_code=result["naics_code"],
        naics_description=result.get("naics_description"),
        national_score=result.get("national_score", 0),
        national_grade=result.get("national_grade", "F"),
        total_establishments=result.get("total_establishments", 0),
        total_employees=result.get("total_employees", 0),
        county_count=result.get("county_count", 0),
        avg_hhi=result.get("avg_hhi"),
        avg_small_biz_pct=result.get("avg_small_biz_pct"),
        avg_estab_size=result.get("avg_estab_size"),
        year=result.get("year", year),
        top_markets=[MarketDetail(**m) for m in markets],
    )


@router.get(
    "/roll-up-targets/{naics_code}/{state}",
    response_model=RollUpTargetsResponse,
    summary="Find roll-up target markets in a state",
)
async def get_roll_up_targets(
    naics_code: str,
    state: str,
    year: int = Query(2021, description="Data year"),
    top_n: int = Query(20, description="Number of top targets"),
    min_establishments: int = Query(10, description="Min establishments per county"),
    db: Session = Depends(get_db),
) -> RollUpTargetsResponse:
    """Find counties in a state with high fragmentation for roll-up acquisitions.

    Identifies specific geographic markets where an industry is most fragmented,
    making them ideal targets for a platform acquisition strategy.
    """
    from app.core.pe_fragmentation import FragmentationScorer

    scorer = FragmentationScorer(db)

    # Ensure data is fetched for this state
    await scorer.collector.collect(naics_code, year=year, state=state)

    result = scorer.get_roll_up_targets(
        naics_code, state, year=year, top_n=top_n,
        min_establishments=min_establishments,
    )

    targets = [MarketDetail(**t) for t in result.get("targets", [])]
    summary = None
    if result.get("state_summary"):
        summary = StateSummary(**result["state_summary"])

    return RollUpTargetsResponse(
        naics_code=result["naics_code"],
        naics_description=result.get("naics_description"),
        state=result["state"],
        year=result.get("year", year),
        targets=targets,
        state_summary=summary,
    )


# =============================================================================
# Roll-Up Screener Endpoints
# =============================================================================


class ScreenerTarget(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    naics_code: Optional[str] = None
    headquarters_city: Optional[str] = None
    headquarters_state: Optional[str] = None
    ownership_status: Optional[str] = None
    current_pe_owner: Optional[str] = None
    employee_count: Optional[int] = None
    founded_year: Optional[int] = None
    website: Optional[str] = None
    description: Optional[str] = None
    revenue_usd: Optional[float] = None
    revenue_growth_pct: Optional[float] = None
    ebitda_margin_pct: Optional[float] = None
    ebitda_usd: Optional[float] = None
    target_score: float = 0
    acquisition_rationale: Optional[str] = None


class ScreenerResponse(BaseModel):
    naics_code: str
    naics_description: Optional[str] = None
    fragmentation_score: float = 0
    total_targets: int = 0
    targets: List[ScreenerTarget] = []


class StateCount(BaseModel):
    state: str
    count: int


class ScreenerSummaryResponse(BaseModel):
    naics_code: str
    naics_description: Optional[str] = None
    fragmentation_score: float = 0
    total_targets: int = 0
    total_addressable_revenue: float = 0
    avg_revenue: float = 0
    avg_employee_count: float = 0
    median_revenue: float = 0
    ownership_breakdown: Dict[str, int] = {}
    top_states: List[StateCount] = []


@router.get(
    "/rollup-screener/{naics_code}/summary",
    response_model=ScreenerSummaryResponse,
    summary="Market overview for roll-up screening",
)
async def get_rollup_screener_summary(
    naics_code: str,
    state: Optional[str] = Query(None, description="Filter to state (2-letter code)"),
    db: Session = Depends(get_db),
) -> ScreenerSummaryResponse:
    """Market overview: total addressable market, avg sizing, top geos, ownership mix."""
    from app.core.pe_rollup_screener import RollUpScreener

    screener = RollUpScreener(db)
    result = await screener.get_summary(naics_code, state=state)

    return ScreenerSummaryResponse(
        naics_code=result.get("naics_code", naics_code),
        naics_description=result.get("naics_description"),
        fragmentation_score=result.get("fragmentation_score", 0),
        total_targets=result.get("total_targets", 0),
        total_addressable_revenue=result.get("total_addressable_revenue", 0),
        avg_revenue=result.get("avg_revenue", 0),
        avg_employee_count=result.get("avg_employee_count", 0),
        median_revenue=result.get("median_revenue", 0),
        ownership_breakdown=result.get("ownership_breakdown", {}),
        top_states=[StateCount(**s) for s in result.get("top_states", [])],
    )


@router.get(
    "/rollup-screener/{naics_code}",
    response_model=ScreenerResponse,
    summary="Screen for roll-up acquisition targets",
)
async def get_rollup_screener(
    naics_code: str,
    state: Optional[str] = Query(None, description="Filter to state (2-letter code)"),
    min_revenue: Optional[float] = Query(None, description="Minimum revenue ($)"),
    max_revenue: Optional[float] = Query(None, description="Maximum revenue ($)"),
    exclude_pe_backed: bool = Query(False, description="Exclude PE-backed companies"),
    top_n: int = Query(20, description="Number of targets to return"),
    db: Session = Depends(get_db),
) -> ScreenerResponse:
    """Screen for acquisition targets in a fragmented industry.

    Combines fragmentation analysis with company discovery. Scores targets
    on size fit ($5-50M sweet spot), ownership status, geography, and growth.
    """
    from app.core.pe_rollup_screener import RollUpScreener

    screener = RollUpScreener(db)
    result = await screener.screen(
        naics_code,
        state=state,
        min_revenue=min_revenue,
        max_revenue=max_revenue,
        exclude_pe_backed=exclude_pe_backed,
        top_n=top_n,
    )

    return ScreenerResponse(
        naics_code=result["naics_code"],
        naics_description=result.get("naics_description"),
        fragmentation_score=result.get("fragmentation_score", 0),
        total_targets=result.get("total_targets", 0),
        targets=[ScreenerTarget(**t) for t in result.get("targets", [])],
    )


# =============================================================================
# Valuation Comparables Endpoints
# =============================================================================


class PeerCompany(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    enterprise_value: Optional[float] = None
    revenue: Optional[float] = None
    ebitda: Optional[float] = None
    ev_revenue: Optional[float] = None
    ev_ebitda: Optional[float] = None
    ownership_status: Optional[str] = None


class PeerStats(BaseModel):
    median: Optional[float] = None
    p25: Optional[float] = None
    p75: Optional[float] = None
    min: Optional[float] = None
    max: Optional[float] = None
    count: int = 0


class ValuationCompsResponse(BaseModel):
    company_id: int
    company_name: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    enterprise_value: Optional[float] = None
    revenue: Optional[float] = None
    ebitda: Optional[float] = None
    ev_revenue: Optional[float] = None
    ev_ebitda: Optional[float] = None
    peer_ev_revenue: Dict[str, Any] = {}
    peer_ev_ebitda: Dict[str, Any] = {}
    ev_revenue_percentile: Optional[int] = None
    ev_ebitda_percentile: Optional[int] = None
    peer_companies: List[PeerCompany] = []


@router.get(
    "/valuation-comps/{company_id}",
    response_model=ValuationCompsResponse,
    summary="Valuation comparables with peer benchmarks",
)
async def get_valuation_comps(
    company_id: int,
    db: Session = Depends(get_db),
) -> ValuationCompsResponse:
    """Get EV/Revenue and EV/EBITDA multiples for a company vs. peers.

    Returns company multiples, peer set stats (median, P25, P75),
    and percentile rank within the peer group.
    """
    from app.core.pe_valuation_comps import ValuationCompsService

    service = ValuationCompsService(db)
    result = service.get_comps(company_id)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return ValuationCompsResponse(
        company_id=result["company_id"],
        company_name=result.get("company_name"),
        industry=result.get("industry"),
        sub_industry=result.get("sub_industry"),
        enterprise_value=result.get("enterprise_value"),
        revenue=result.get("revenue"),
        ebitda=result.get("ebitda"),
        ev_revenue=result.get("ev_revenue"),
        ev_ebitda=result.get("ev_ebitda"),
        peer_ev_revenue=result.get("peer_ev_revenue", {}),
        peer_ev_ebitda=result.get("peer_ev_ebitda", {}),
        ev_revenue_percentile=result.get("ev_revenue_percentile"),
        ev_ebitda_percentile=result.get("ev_ebitda_percentile"),
        peer_companies=[PeerCompany(**p) for p in result.get("peer_companies", [])],
    )


# =============================================================================
# Comparable Transactions
# =============================================================================


class ComparableDealResponse(BaseModel):
    id: int
    deal_name: Optional[str] = None
    deal_type: Optional[str] = None
    deal_sub_type: Optional[str] = None
    buyer_name: Optional[str] = None
    seller_name: Optional[str] = None
    seller_type: Optional[str] = None
    enterprise_value_usd: Optional[float] = None
    ev_ebitda_multiple: Optional[float] = None
    ev_revenue_multiple: Optional[float] = None
    ltm_revenue_usd: Optional[float] = None
    ltm_ebitda_usd: Optional[float] = None
    announced_date: Optional[str] = None
    closed_date: Optional[str] = None
    status: Optional[str] = None


class MarketStatsResponse(BaseModel):
    total_deals: int = 0
    deals_with_multiples: int = 0
    ev_ebitda_median: Optional[float] = None
    ev_ebitda_p25: Optional[float] = None
    ev_ebitda_p75: Optional[float] = None
    ev_ebitda_min: Optional[float] = None
    ev_ebitda_max: Optional[float] = None
    ev_revenue_median: Optional[float] = None
    total_deal_value_usd: Optional[float] = None
    seller_type_breakdown: Dict[str, int] = {}
    multiple_trend: Optional[str] = None


class ComparableTransactionsResponse(BaseModel):
    company_id: int
    company_name: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    comparable_deals: List[ComparableDealResponse] = []
    deal_count: int = 0
    market_stats: MarketStatsResponse = MarketStatsResponse()


@router.get(
    "/comparable-transactions/{company_id}",
    response_model=ComparableTransactionsResponse,
    summary="Comparable exit transactions for valuation support",
)
async def get_comparable_transactions(
    company_id: int,
    years_back: int = Query(5, ge=1, le=20, description="Years of history"),
    include_pending: bool = Query(False, description="Include pending deals"),
    db: Session = Depends(get_db),
) -> ComparableTransactionsResponse:
    """Get comparable exit transactions in the same industry as a target company.

    Returns historical deal details with multiples plus aggregate market
    statistics (median multiple, volume, trend direction).
    """
    from app.core.pe_comparable_transactions import ComparableTransactionService

    service = ComparableTransactionService(db)
    result = service.get_comps(company_id, years_back=years_back, include_pending=include_pending)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return ComparableTransactionsResponse(
        company_id=result["company_id"],
        company_name=result.get("company_name"),
        industry=result.get("industry"),
        sub_industry=result.get("sub_industry"),
        comparable_deals=[ComparableDealResponse(**d) for d in result.get("comparable_deals", [])],
        deal_count=result.get("deal_count", 0),
        market_stats=MarketStatsResponse(**result.get("market_stats", {})),
    )


# =============================================================================
# Market Scanner & Intelligence Brief
# =============================================================================


class BuyerInfo(BaseModel):
    buyer_name: str
    deal_count: int


class EvEbitdaRange(BaseModel):
    min: Optional[float] = None
    max: Optional[float] = None


class SectorOverviewResponse(BaseModel):
    industry: str
    deal_count: int = 0
    total_deal_value_usd: float = 0
    median_ev_ebitda: Optional[float] = None
    median_ev_revenue: Optional[float] = None
    ev_ebitda_range: Optional[EvEbitdaRange] = None
    deal_type_breakdown: Dict[str, int] = {}
    seller_type_breakdown: Dict[str, int] = {}
    top_buyers: List[BuyerInfo] = []
    yoy_deal_count_change: Optional[float] = None
    yoy_multiple_change: Optional[float] = None


class IntelligenceBriefResponse(BaseModel):
    industry: str
    headline: str
    key_findings: List[str] = []
    recommendations: List[str] = []
    deal_count: int = 0
    total_deal_value_usd: float = 0
    median_ev_ebitda: Optional[float] = None


class NewsSentimentResponse(BaseModel):
    total_articles: int = 0
    avg_sentiment: Optional[float] = None
    positive: int = 0
    negative: int = 0
    neutral: int = 0


class MarketSignalResponse(BaseModel):
    industry: str
    recent_deal_count: int = 0
    prior_deal_count: int = 0
    current_median_ev_ebitda: Optional[float] = None
    prior_median_ev_ebitda: Optional[float] = None
    momentum: str = "neutral"
    momentum_score: int = 50
    deal_flow_change_pct: Optional[float] = None
    multiple_change_pct: Optional[float] = None
    news_sentiment: Optional[NewsSentimentResponse] = None


class SectorRankResponse(BaseModel):
    rank: int
    industry: str
    momentum_score: int = 50
    momentum: str = "neutral"
    recent_deal_count: int = 0
    current_median_ev_ebitda: Optional[float] = None
    news_sentiment: Optional[NewsSentimentResponse] = None


@router.get(
    "/market-scanner/signals",
    response_model=List[MarketSignalResponse],
    summary="Cross-sector momentum signals",
)
async def get_market_signals(
    db: Session = Depends(get_db),
) -> List[MarketSignalResponse]:
    """Get momentum signals across all active sectors.

    Compares recent deal flow and multiples vs prior period to identify
    bullish, bearish, or neutral momentum for each industry.
    """
    from app.core.pe_market_scanner import MarketScannerService

    service = MarketScannerService(db)
    signals = service.get_market_signals()
    return [MarketSignalResponse(**s) for s in signals]


@router.get(
    "/market-scanner/sector/{industry}",
    response_model=SectorOverviewResponse,
    summary="Sector deal flow overview",
)
async def get_sector_overview(
    industry: str,
    years_back: int = Query(3, ge=1, le=10, description="Years of history"),
    db: Session = Depends(get_db),
) -> SectorOverviewResponse:
    """Get sector overview with deal stats, multiples, and top buyers."""
    from app.core.pe_market_scanner import MarketScannerService

    service = MarketScannerService(db)
    result = service.get_sector_overview(industry, years_back=years_back)

    ev_range = result.get("ev_ebitda_range")
    return SectorOverviewResponse(
        industry=result["industry"],
        deal_count=result["deal_count"],
        total_deal_value_usd=result.get("total_deal_value_usd", 0),
        median_ev_ebitda=result.get("median_ev_ebitda"),
        median_ev_revenue=result.get("median_ev_revenue"),
        ev_ebitda_range=EvEbitdaRange(**ev_range) if ev_range else None,
        deal_type_breakdown=result.get("deal_type_breakdown", {}),
        seller_type_breakdown=result.get("seller_type_breakdown", {}),
        top_buyers=[BuyerInfo(**b) for b in result.get("top_buyers", [])],
        yoy_deal_count_change=result.get("yoy_deal_count_change"),
        yoy_multiple_change=result.get("yoy_multiple_change"),
    )


@router.get(
    "/market-scanner/intelligence-brief/{industry}",
    response_model=IntelligenceBriefResponse,
    summary="AI-generated sector intelligence brief",
)
async def get_intelligence_brief(
    industry: str,
    years_back: int = Query(3, ge=1, le=10, description="Years of history"),
    db: Session = Depends(get_db),
) -> IntelligenceBriefResponse:
    """Generate an intelligence brief with key findings and recommendations."""
    from app.core.pe_market_scanner import MarketScannerService

    service = MarketScannerService(db)
    brief = service.get_intelligence_brief(industry, years_back=years_back)
    return IntelligenceBriefResponse(**brief)


# New /market/ endpoints (aliases with momentum scoring)

@router.get(
    "/market/scan",
    response_model=List[MarketSignalResponse],
    summary="Market scan — signals across all sectors with momentum scores",
)
async def market_scan(
    db: Session = Depends(get_db),
) -> List[MarketSignalResponse]:
    """Current signals across all sectors with 0-100 momentum scoring."""
    from app.core.pe_market_scanner import MarketScannerService

    service = MarketScannerService(db)
    signals = service.get_market_signals()
    return [MarketSignalResponse(**s) for s in signals]


@router.get(
    "/market/brief/{sector}",
    response_model=IntelligenceBriefResponse,
    summary="Detailed sector intelligence brief",
)
async def market_brief(
    sector: str,
    years_back: int = Query(3, ge=1, le=10),
    db: Session = Depends(get_db),
) -> IntelligenceBriefResponse:
    """Detailed sector brief with key findings and recommendations."""
    from app.core.pe_market_scanner import MarketScannerService

    service = MarketScannerService(db)
    brief = service.get_intelligence_brief(sector, years_back=years_back)
    return IntelligenceBriefResponse(**brief)


@router.get(
    "/market/sectors",
    response_model=List[SectorRankResponse],
    summary="Sectors ranked by momentum score",
)
async def market_sectors(
    db: Session = Depends(get_db),
) -> List[SectorRankResponse]:
    """Return all active sectors ranked by momentum score (0-100)."""
    from app.core.pe_market_scanner import MarketScannerService

    service = MarketScannerService(db)
    ranked = service.get_sectors_ranked()
    return [SectorRankResponse(**s) for s in ranked]


# =============================================================================
# Deal Pipeline
# =============================================================================


class PipelineDealResponse(BaseModel):
    id: int
    company_id: Optional[int] = None
    deal_name: Optional[str] = None
    deal_type: Optional[str] = None
    deal_sub_type: Optional[str] = None
    status: Optional[str] = None
    enterprise_value_usd: Optional[float] = None
    ev_ebitda_multiple: Optional[float] = None
    ev_revenue_multiple: Optional[float] = None
    ltm_revenue_usd: Optional[float] = None
    ltm_ebitda_usd: Optional[float] = None
    buyer_name: Optional[str] = None
    seller_name: Optional[str] = None
    seller_type: Optional[str] = None
    announced_date: Optional[str] = None
    closed_date: Optional[str] = None
    expected_close_date: Optional[str] = None


class PipelineCreateRequest(BaseModel):
    company_id: int
    deal_name: str
    deal_type: str = "LBO"
    deal_sub_type: Optional[str] = None
    status: str = "Announced"
    enterprise_value_usd: Optional[float] = None
    ev_ebitda_multiple: Optional[float] = None
    ev_revenue_multiple: Optional[float] = None
    ltm_revenue_usd: Optional[float] = None
    ltm_ebitda_usd: Optional[float] = None
    buyer_name: Optional[str] = None
    seller_name: Optional[str] = None
    seller_type: Optional[str] = None
    announced_date: Optional[date] = None
    expected_close_date: Optional[date] = None


class PipelineUpdateRequest(BaseModel):
    status: Optional[str] = None
    deal_name: Optional[str] = None
    enterprise_value_usd: Optional[float] = None
    ev_ebitda_multiple: Optional[float] = None
    buyer_name: Optional[str] = None
    seller_name: Optional[str] = None
    expected_close_date: Optional[date] = None
    closed_date: Optional[date] = None


class UpcomingCloseResponse(BaseModel):
    deal_id: int
    expected_close_date: str


class PipelineInsightsResponse(BaseModel):
    total_pipeline_deals: int = 0
    active_deals: int = 0
    total_pipeline_value_usd: float = 0
    stage_breakdown: Dict[str, int] = {}
    deal_type_breakdown: Dict[str, int] = {}
    avg_deal_size_usd: float = 0
    upcoming_closes: List[UpcomingCloseResponse] = []


class FirmPipelineResponse(BaseModel):
    firm_id: int
    firm_name: str
    total_deals: int = 0
    stages: Dict[str, List[PipelineDealResponse]] = {}


class FirmPipelineCreateRequest(BaseModel):
    company_id: int
    deal_name: str
    deal_type: str = "LBO"
    deal_sub_type: Optional[str] = None
    status: str = "Screening"
    enterprise_value_usd: Optional[float] = None
    ev_ebitda_multiple: Optional[float] = None
    ev_revenue_multiple: Optional[float] = None
    ltm_revenue_usd: Optional[float] = None
    ltm_ebitda_usd: Optional[float] = None
    seller_name: Optional[str] = None
    seller_type: Optional[str] = None
    announced_date: Optional[date] = None
    expected_close_date: Optional[date] = None


class StageUpdateRequest(BaseModel):
    stage: str


class FirmInsightsResponse(BaseModel):
    firm_id: int = 0
    firm_name: str = ""
    total_deals: int = 0
    active_deals: int = 0
    won_deals: int = 0
    lost_deals: int = 0
    total_pipeline_value_usd: float = 0
    avg_deal_size_usd: float = 0
    win_rate_pct: Optional[float] = None
    stage_breakdown: Dict[str, int] = {}


@router.get(
    "/pipeline/insights",
    response_model=PipelineInsightsResponse,
    summary="Pipeline health and insights",
)
async def get_pipeline_insights(
    db: Session = Depends(get_db),
) -> PipelineInsightsResponse:
    """Get pipeline health summary: stage counts, total value, upcoming closes."""
    from app.core.pe_deal_pipeline import DealPipelineService

    service = DealPipelineService(db)
    insights = service.get_insights()
    return PipelineInsightsResponse(
        total_pipeline_deals=insights["total_pipeline_deals"],
        active_deals=insights["active_deals"],
        total_pipeline_value_usd=insights["total_pipeline_value_usd"],
        stage_breakdown=insights["stage_breakdown"],
        deal_type_breakdown=insights["deal_type_breakdown"],
        avg_deal_size_usd=insights["avg_deal_size_usd"],
        upcoming_closes=[UpcomingCloseResponse(**c) for c in insights.get("upcoming_closes", [])],
    )


@router.get(
    "/pipeline",
    response_model=List[PipelineDealResponse],
    summary="List pipeline deals",
)
async def list_pipeline_deals(
    status: Optional[str] = Query(None, description="Filter by status"),
    deal_type: Optional[str] = Query(None, description="Filter by deal type"),
    active_only: bool = Query(False, description="Only active (non-closed) deals"),
    db: Session = Depends(get_db),
) -> List[PipelineDealResponse]:
    """List all pipeline deals with optional status/type filters."""
    from app.core.pe_deal_pipeline import DealPipelineService

    service = DealPipelineService(db)
    deals = service.list_deals(status=status, deal_type=deal_type, active_only=active_only)
    return [PipelineDealResponse(**d) for d in deals]


@router.post(
    "/pipeline",
    response_model=PipelineDealResponse,
    summary="Create a pipeline deal",
    status_code=201,
)
async def create_pipeline_deal(
    request: PipelineCreateRequest,
    db: Session = Depends(get_db),
) -> PipelineDealResponse:
    """Create a new deal in the pipeline."""
    from app.core.pe_deal_pipeline import DealPipelineService

    service = DealPipelineService(db)
    result = service.create_deal(request.model_dump())

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return PipelineDealResponse(**result)


@router.patch(
    "/pipeline/{deal_id}",
    response_model=PipelineDealResponse,
    summary="Update a pipeline deal",
)
async def update_pipeline_deal(
    deal_id: int,
    request: PipelineUpdateRequest,
    db: Session = Depends(get_db),
) -> PipelineDealResponse:
    """Update a deal's status, value, or other fields."""
    from app.core.pe_deal_pipeline import DealPipelineService

    service = DealPipelineService(db)
    updates = {k: v for k, v in request.model_dump().items() if v is not None}
    result = service.update_deal(deal_id, updates)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return PipelineDealResponse(**result)


# =============================================================================
# Firm-Scoped Pipeline (v2)
# =============================================================================


@router.get(
    "/pipeline/{firm_id}",
    response_model=FirmPipelineResponse,
    summary="Firm pipeline by stage",
)
async def get_firm_pipeline(
    firm_id: int,
    db: Session = Depends(get_db),
) -> FirmPipelineResponse:
    """Get a firm's deal pipeline grouped by stage (Screening, DD, LOI, Closing, Won, Lost)."""
    from app.core.pe_deal_pipeline import DealPipelineService

    service = DealPipelineService(db)
    result = service.list_firm_deals(firm_id)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return FirmPipelineResponse(**result)


@router.post(
    "/pipeline/{firm_id}/deals",
    response_model=PipelineDealResponse,
    summary="Create firm pipeline deal",
    status_code=201,
)
async def create_firm_pipeline_deal(
    firm_id: int,
    request: FirmPipelineCreateRequest,
    db: Session = Depends(get_db),
) -> PipelineDealResponse:
    """Create a deal in a firm's pipeline. Links deal to firm via participant record."""
    from app.core.pe_deal_pipeline import DealPipelineService

    service = DealPipelineService(db)
    result = service.create_firm_deal(firm_id, request.model_dump())

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return PipelineDealResponse(**result)


@router.patch(
    "/deals/{deal_id}/stage",
    response_model=PipelineDealResponse,
    summary="Move deal to stage",
)
async def update_deal_stage(
    deal_id: int,
    request: StageUpdateRequest,
    db: Session = Depends(get_db),
) -> PipelineDealResponse:
    """Move a deal between pipeline stages (Screening, DD, LOI, Closing, Won, Lost)."""
    from app.core.pe_deal_pipeline import DealPipelineService

    service = DealPipelineService(db)
    result = service.update_deal_stage(deal_id, request.stage)

    if "error" in result:
        status = 400 if "Invalid stage" in result["error"] else 404
        raise HTTPException(status_code=status, detail=result["error"])

    return PipelineDealResponse(**result)


@router.get(
    "/pipeline/{firm_id}/insights",
    response_model=FirmInsightsResponse,
    summary="Firm pipeline health",
)
async def get_firm_pipeline_insights(
    firm_id: int,
    db: Session = Depends(get_db),
) -> FirmInsightsResponse:
    """Pipeline health for a specific firm: total value, conversion rates, stage breakdown."""
    from app.core.pe_deal_pipeline import DealPipelineService

    service = DealPipelineService(db)
    result = service.get_firm_insights(firm_id)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return FirmInsightsResponse(**result)


# =============================================================================
# Fund Performance Endpoints (Phase 3)
# =============================================================================


class CashFlowItem(BaseModel):
    id: int
    date: str
    amount: float
    type: str
    description: Optional[str] = None


class FundMetricsResponse(BaseModel):
    fund_id: int
    fund_name: str
    as_of_date: str
    irr_pct: Optional[float] = None
    moic: Optional[float] = None
    tvpi: Optional[float] = None
    dpi: Optional[float] = None
    rvpi: Optional[float] = None
    called_capital: float = 0
    distributed: float = 0
    nav: float = 0
    cash_flow_count: int = 0


class FundSummaryWithMetrics(BaseModel):
    fund_id: int
    fund_name: str
    vintage_year: Optional[int] = None
    strategy: Optional[str] = None
    target_size_usd_millions: Optional[float] = None
    status: Optional[str] = None
    metrics: FundMetricsResponse


class FirmFundsResponse(BaseModel):
    firm_id: int
    firm_name: str
    funds: List[FundSummaryWithMetrics] = []


class QuarterlySnapshot(BaseModel):
    quarter: str
    called_capital: float
    distributed: float
    nav: float
    tvpi: Optional[float] = None
    dpi: Optional[float] = None
    net_cash: float


class FundTimeseriesResponse(BaseModel):
    fund_id: int
    fund_name: str
    timeseries: List[QuarterlySnapshot] = []


class FundCashFlowsResponse(BaseModel):
    fund_id: int
    fund_name: str
    cash_flows: List[CashFlowItem] = []


class JCurvePoint(BaseModel):
    date: str
    cumulative_net: float
    type: str


class FundJCurveResponse(BaseModel):
    fund_id: int
    fund_name: str
    j_curve: List[JCurvePoint] = []


class FundComparisonResponse(BaseModel):
    funds: List[FundMetricsResponse] = []


@router.get(
    "/funds/{firm_id}",
    response_model=FirmFundsResponse,
    summary="All funds with live-calculated metrics",
)
async def get_firm_funds(
    firm_id: int,
    db: Session = Depends(get_db),
) -> FirmFundsResponse:
    """List all funds for a firm with IRR/MOIC/TVPI calculated from cash flows.

    Replaces hardcoded performance numbers with live calculations.
    """
    from app.core.pe_fund_performance import FundPerformanceService

    firm = db.execute(
        select(PEFirm).where(PEFirm.id == firm_id)
    ).scalar_one_or_none()
    if not firm:
        raise HTTPException(status_code=404, detail=f"Firm {firm_id} not found")

    funds = db.execute(
        select(PEFund).where(PEFund.firm_id == firm_id).order_by(PEFund.vintage_year)
    ).scalars().all()

    perf_service = FundPerformanceService(db)
    fund_list = []
    for fund in funds:
        # Get NAV from fund performance record if available
        perf = db.execute(
            select(PEFundPerformance)
            .where(PEFundPerformance.fund_id == fund.id)
            .order_by(PEFundPerformance.as_of_date.desc())
            .limit(1)
        ).scalar_one_or_none()
        nav = float(perf.remaining_value or 0) if perf else 0.0

        metrics = perf_service.calculate_fund_returns(fund.id, nav=nav)
        fund_list.append(FundSummaryWithMetrics(
            fund_id=fund.id,
            fund_name=fund.name,
            vintage_year=fund.vintage_year,
            strategy=fund.strategy,
            target_size_usd_millions=float(fund.target_size_usd_millions) if fund.target_size_usd_millions else None,
            status=fund.status,
            metrics=FundMetricsResponse(**metrics),
        ))

    return FirmFundsResponse(
        firm_id=firm.id,
        firm_name=firm.name,
        funds=fund_list,
    )


@router.get(
    "/fund-performance/compare",
    response_model=FundComparisonResponse,
    summary="Side-by-side fund comparison",
)
async def compare_funds(
    fund_ids: str = Query(..., description="Comma-separated fund IDs (e.g. 1,2,3)"),
    db: Session = Depends(get_db),
) -> FundComparisonResponse:
    """Compare multiple funds side-by-side with IRR, MOIC, TVPI, DPI, RVPI."""
    from app.core.pe_fund_performance import FundPerformanceService

    service = FundPerformanceService(db)
    results = []

    for fid_str in fund_ids.split(","):
        fid = int(fid_str.strip())
        # Get NAV
        perf = db.execute(
            select(PEFundPerformance)
            .where(PEFundPerformance.fund_id == fid)
            .order_by(PEFundPerformance.as_of_date.desc())
            .limit(1)
        ).scalar_one_or_none()
        nav = float(perf.remaining_value or 0) if perf else 0.0

        metrics = service.calculate_fund_returns(fid, nav=nav)
        if "error" not in metrics:
            results.append(FundMetricsResponse(**metrics))

    return FundComparisonResponse(funds=results)


@router.get(
    "/fund-performance/{fund_id}",
    response_model=FundTimeseriesResponse,
    summary="Quarterly performance time series",
)
async def get_fund_timeseries(
    fund_id: int,
    db: Session = Depends(get_db),
) -> FundTimeseriesResponse:
    """Quarterly performance snapshots from inception to present.

    Returns cumulative called capital, distributions, TVPI, DPI per quarter.
    """
    from app.core.pe_fund_performance import FundPerformanceService

    perf = db.execute(
        select(PEFundPerformance)
        .where(PEFundPerformance.fund_id == fund_id)
        .order_by(PEFundPerformance.as_of_date.desc())
        .limit(1)
    ).scalar_one_or_none()
    nav = float(perf.remaining_value or 0) if perf else 0.0

    service = FundPerformanceService(db)
    result = service.calculate_fund_timeseries(fund_id, nav=nav)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return FundTimeseriesResponse(
        fund_id=result["fund_id"],
        fund_name=result["fund_name"],
        timeseries=[QuarterlySnapshot(**s) for s in result["timeseries"]],
    )


@router.get(
    "/fund-performance/{fund_id}/cashflows",
    response_model=FundCashFlowsResponse,
    summary="Raw cash flow ledger",
)
async def get_fund_cashflows(
    fund_id: int,
    db: Session = Depends(get_db),
) -> FundCashFlowsResponse:
    """Raw cash flow ledger for a fund — every capital call and distribution."""
    from app.core.pe_fund_performance import FundPerformanceService

    service = FundPerformanceService(db)
    result = service.get_cash_flows(fund_id)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return FundCashFlowsResponse(
        fund_id=result["fund_id"],
        fund_name=result["fund_name"],
        cash_flows=[CashFlowItem(**cf) for cf in result["cash_flows"]],
    )


@router.get(
    "/fund-performance/{fund_id}/j-curve",
    response_model=FundJCurveResponse,
    summary="J-curve visualization data",
)
async def get_fund_j_curve(
    fund_id: int,
    db: Session = Depends(get_db),
) -> FundJCurveResponse:
    """J-curve data: cumulative net cash position over time.

    Shows the classic PE J-curve pattern — negative early (capital calls),
    then crossing zero and going positive as distributions flow back.
    """
    from app.core.pe_fund_performance import FundPerformanceService

    perf = db.execute(
        select(PEFundPerformance)
        .where(PEFundPerformance.fund_id == fund_id)
        .order_by(PEFundPerformance.as_of_date.desc())
        .limit(1)
    ).scalar_one_or_none()
    nav = float(perf.remaining_value or 0) if perf else 0.0

    service = FundPerformanceService(db)
    result = service.get_j_curve(fund_id, nav=nav)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return FundJCurveResponse(
        fund_id=result["fund_id"],
        fund_name=result["fund_name"],
        j_curve=[JCurvePoint(**p) for p in result["j_curve"]],
    )


# =============================================================================
# Deal Scoring Endpoint (Phase 4)
# =============================================================================


class DealDimensionResponse(BaseModel):
    dimension: str
    label: str
    weight: float
    raw_score: float
    weighted_score: float
    grade: str
    explanation: str
    data_gaps: List[str] = []


class DealScoreResponse(BaseModel):
    company_id: int
    company_name: str
    composite_score: float
    grade: str
    dimensions: List[DealDimensionResponse] = []
    strengths: List[str] = []
    risks: List[str] = []
    data_gaps: List[str] = []


@router.get(
    "/deal-score/{company_id}",
    response_model=DealScoreResponse,
    summary="Score acquisition target",
)
async def get_deal_score(
    company_id: int,
    db: Session = Depends(get_db),
) -> DealScoreResponse:
    """Score an acquisition target across 5 dimensions.

    Evaluates financial quality (35%), market position (20%), management (15%),
    growth trajectory (20%), and deal attractiveness (10%).
    """
    from app.core.pe_deal_scorer import score_deal

    result = score_deal(db, company_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    return DealScoreResponse(
        company_id=result.company_id,
        company_name=result.company_name,
        composite_score=result.composite_score,
        grade=result.grade,
        dimensions=[
            DealDimensionResponse(
                dimension=d.dimension,
                label=d.label,
                weight=d.weight,
                raw_score=d.raw_score,
                weighted_score=d.weighted_score,
                grade=d.grade,
                explanation=d.explanation,
                data_gaps=d.data_gaps,
            )
            for d in result.dimensions
        ],
        strengths=result.strengths,
        risks=result.risks,
        data_gaps=result.data_gaps,
    )


# =============================================================================
# Portfolio Monitoring & Alerts (Phase 2)
# =============================================================================


class AlertResponse(BaseModel):
    id: Optional[int] = None
    firm_id: int
    company_id: Optional[int] = None
    alert_type: str
    severity: str = "info"
    title: str
    detail: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    acknowledged_at: Optional[str] = None


class SubscriptionResponse(BaseModel):
    id: Optional[int] = None
    firm_id: int
    alert_type: str
    webhook_id: Optional[int] = None
    enabled: bool = True
    created_at: Optional[str] = None


class SubscribeRequest(BaseModel):
    alert_types: List[str] = Field(..., description="Alert types to subscribe to")
    webhook_id: Optional[int] = Field(None, description="Optional webhook to receive alerts")


class UnsubscribeRequest(BaseModel):
    alert_types: List[str] = Field(..., description="Alert types to unsubscribe from")


class CompanyHealthStatus(BaseModel):
    company_id: int
    company_name: str
    exit_score: Optional[float] = None
    exit_grade: Optional[str] = None
    revenue: Optional[float] = None
    ebitda_margin: Optional[float] = None
    leadership_count: int = 0
    alert_count: int = 0
    trend: str = "stable"


class PortfolioHealthResponse(BaseModel):
    firm_id: int
    firm_name: str
    check_date: str
    companies_checked: int = 0
    alerts_generated: int = 0
    company_statuses: List[CompanyHealthStatus] = []
    alerts: List[AlertResponse] = []


@router.get(
    "/alerts/{firm_id}",
    response_model=List[AlertResponse],
    summary="Recent alerts for firm",
)
async def get_firm_alerts(
    firm_id: int,
    limit: int = Query(50, ge=1, le=200),
    alert_type: Optional[str] = Query(None, description="Filter by alert type"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    db: Session = Depends(get_db),
) -> List[AlertResponse]:
    """Recent portfolio monitoring alerts for a PE firm."""
    from app.core.pe_alert_subscriptions import AlertSubscriptionService

    service = AlertSubscriptionService(db)
    alerts = service.get_alert_history(firm_id, limit=limit, alert_type=alert_type, severity=severity)
    return [AlertResponse(**a) for a in alerts]


@router.post(
    "/alerts/{firm_id}/subscribe",
    response_model=List[SubscriptionResponse],
    summary="Subscribe to alert types",
)
async def subscribe_to_alerts(
    firm_id: int,
    request: SubscribeRequest,
    db: Session = Depends(get_db),
) -> List[SubscriptionResponse]:
    """Subscribe a firm to PE alert types."""
    from app.core.pe_alert_subscriptions import AlertSubscriptionService

    firm = db.execute(select(PEFirm).where(PEFirm.id == firm_id)).scalar_one_or_none()
    if not firm:
        raise HTTPException(status_code=404, detail=f"Firm {firm_id} not found")

    service = AlertSubscriptionService(db)
    subs = service.subscribe(firm_id, request.alert_types, webhook_id=request.webhook_id)
    return [SubscriptionResponse(**s) for s in subs]


@router.delete(
    "/alerts/{firm_id}/subscribe",
    summary="Unsubscribe from alert types",
)
async def unsubscribe_from_alerts(
    firm_id: int,
    request: UnsubscribeRequest,
    db: Session = Depends(get_db),
):
    """Unsubscribe a firm from PE alert types."""
    from app.core.pe_alert_subscriptions import AlertSubscriptionService

    service = AlertSubscriptionService(db)
    count = service.unsubscribe(firm_id, request.alert_types)
    return {"status": "ok", "disabled": count}


@router.get(
    "/alerts/{firm_id}/subscriptions",
    response_model=List[SubscriptionResponse],
    summary="List active subscriptions",
)
async def list_alert_subscriptions(
    firm_id: int,
    db: Session = Depends(get_db),
) -> List[SubscriptionResponse]:
    """List active alert subscriptions for a PE firm."""
    from app.core.pe_alert_subscriptions import AlertSubscriptionService

    service = AlertSubscriptionService(db)
    subs = service.list_subscriptions(firm_id)
    return [SubscriptionResponse(**s) for s in subs]


@router.post(
    "/monitor/{firm_id}/run",
    response_model=PortfolioHealthResponse,
    summary="Run portfolio health check",
)
async def run_portfolio_health_check(
    firm_id: int,
    db: Session = Depends(get_db),
) -> PortfolioHealthResponse:
    """Manually trigger a full portfolio health check for a firm.

    Compares current state against snapshots, generates alerts for
    significant changes, stores new snapshots.
    """
    from app.core.pe_portfolio_monitor import PortfolioMonitorService

    firm = db.execute(select(PEFirm).where(PEFirm.id == firm_id)).scalar_one_or_none()
    if not firm:
        raise HTTPException(status_code=404, detail=f"Firm {firm_id} not found")

    service = PortfolioMonitorService(db)
    report = service.run_full_portfolio_check(firm_id)

    return PortfolioHealthResponse(
        firm_id=report.firm_id,
        firm_name=report.firm_name,
        check_date=report.check_date,
        companies_checked=report.companies_checked,
        alerts_generated=report.alerts_generated,
        company_statuses=[
            CompanyHealthStatus(
                company_id=s.company_id, company_name=s.company_name,
                exit_score=s.exit_score, exit_grade=s.exit_grade,
                revenue=s.revenue, ebitda_margin=s.ebitda_margin,
                leadership_count=s.leadership_count, alert_count=s.alert_count,
                trend=s.trend,
            )
            for s in report.company_statuses
        ],
        alerts=[AlertResponse(**a) for a in report.alerts],
    )


@router.get(
    "/monitor/{firm_id}/health",
    response_model=PortfolioHealthResponse,
    summary="Portfolio health dashboard",
)
async def get_portfolio_health(
    firm_id: int,
    db: Session = Depends(get_db),
) -> PortfolioHealthResponse:
    """Current portfolio health dashboard data.

    Returns latest snapshot data with company scores, trend arrows, and alert counts.
    Does NOT run a new check — uses cached snapshot data.
    """
    from app.core.pe_portfolio_monitor import PortfolioMonitorService, CompanyStatus
    from app.core.pe_models import PEPortfolioSnapshot, PEAlert as PEAlertModel

    firm = db.execute(select(PEFirm).where(PEFirm.id == firm_id)).scalar_one_or_none()
    if not firm:
        raise HTTPException(status_code=404, detail=f"Firm {firm_id} not found")

    monitor = PortfolioMonitorService(db)
    companies = monitor._get_portfolio_companies(firm_id)

    statuses = []
    for company in companies:
        snapshot = monitor._get_latest_snapshot(company.id)

        # Count recent alerts (last 30 days)
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(days=30)
        alert_count = db.execute(
            select(func.count(PEAlertModel.id)).where(
                PEAlertModel.company_id == company.id,
                PEAlertModel.created_at >= cutoff,
            )
        ).scalar() or 0

        statuses.append(CompanyHealthStatus(
            company_id=company.id,
            company_name=company.name,
            exit_score=float(snapshot.exit_score) if snapshot and snapshot.exit_score else None,
            exit_grade=snapshot.exit_grade if snapshot else None,
            revenue=float(snapshot.revenue) if snapshot and snapshot.revenue else None,
            ebitda_margin=float(snapshot.ebitda_margin) if snapshot and snapshot.ebitda_margin else None,
            leadership_count=snapshot.leadership_count if snapshot else 0,
            alert_count=alert_count,
            trend="stable",
        ))

    # Total alerts for this firm in last 30 days
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(days=30)
    total_alerts = db.execute(
        select(func.count(PEAlertModel.id)).where(
            PEAlertModel.firm_id == firm_id,
            PEAlertModel.created_at >= cutoff,
        )
    ).scalar() or 0

    return PortfolioHealthResponse(
        firm_id=firm.id,
        firm_name=firm.name,
        check_date=date.today().isoformat(),
        companies_checked=len(statuses),
        alerts_generated=total_alerts,
        company_statuses=statuses,
        alerts=[],
    )


# ---------------------------------------------------------------------------
# Deal Sourcing Endpoints
# ---------------------------------------------------------------------------

@router.post("/deal-sourcing/{firm_id}/run")
async def run_deal_sourcing(firm_id: int, db: Session = Depends(get_db)):
    """
    Manually trigger deal sourcing for a firm.

    Reads market signals, scores candidates, and auto-creates pipeline
    entries for B+ (>=70) companies not already in the pipeline.
    """
    from app.core.pe_deal_sourcing import source_deals_from_signals

    try:
        report = source_deals_from_signals(db, firm_id)
        return report.to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deal sourcing failed: {e}")


@router.get("/deal-sourcing/{firm_id}/history")
async def deal_sourcing_history(
    firm_id: int,
    days: int = 30,
    db: Session = Depends(get_db),
):
    """
    Recent auto-sourced deals with conversion stats.

    Returns deals created by market_scanner or acquisition_scorer
    for the given firm within the time window.
    """
    from app.core.pe_deal_sourcing import get_sourcing_history
    return get_sourcing_history(db, firm_id, days)


@router.get("/deal-sourcing/{firm_id}/candidates")
async def deal_sourcing_candidates(firm_id: int, db: Session = Depends(get_db)):
    """
    Current high-score candidates not yet in the pipeline.

    Runs the scoring pipeline but returns candidates without creating
    pipeline entries — useful for preview before committing.
    """
    from app.core.pe_deal_sourcing import (
        _find_candidates_by_sector,
        _get_pipeline_company_ids,
        SCORE_THRESHOLD,
    )
    from app.core.pe_market_signals import get_high_momentum_sectors
    from app.core.pe_deal_scorer import score_deal

    try:
        high_sectors = get_high_momentum_sectors(db, threshold=60)
    except Exception:
        high_sectors = []

    if not high_sectors:
        return {"candidates": [], "total": 0}

    sector_names = [s["sector"] for s in high_sectors]
    candidates = _find_candidates_by_sector(db, sector_names)
    existing_ids = _get_pipeline_company_ids(db)

    results = []
    for co in candidates:
        if co.id in existing_ids:
            continue
        try:
            result = score_deal(db, co.id)
            if result and result.composite_score >= SCORE_THRESHOLD:
                results.append({
                    "company_id": co.id,
                    "company_name": co.name,
                    "industry": co.industry,
                    "score": round(result.composite_score, 1),
                    "grade": result.grade,
                    "strengths": result.strengths[:3],
                    "risks": result.risks[:3],
                })
        except Exception:
            continue

    results.sort(key=lambda x: x["score"], reverse=True)
    return {"candidates": results, "total": len(results)}
