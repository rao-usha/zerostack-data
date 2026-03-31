"""
People Analytics API endpoints.

Provides endpoints for leadership analytics, trends, and aggregations:
- Industry stats and metrics for leadership data
- Talent flow analysis
- Change trends over time
- Hot roles and hiring patterns
- Company benchmark scores
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.core.database import get_db
from app.services.analytics_service import AnalyticsService


router = APIRouter(prefix="/people-analytics", tags=["People Analytics"])


# =============================================================================
# Response Models
# =============================================================================


class InstabilityFlag(BaseModel):
    """Company with high leadership turnover."""

    company_id: int
    company_name: str
    c_suite_changes: int
    flag: str


class IndustryStatsResponse(BaseModel):
    """Industry-wide statistics for leadership data."""

    industry: str
    period_days: int
    total_companies: int
    total_executives: int
    c_suite_count: int
    board_members: int
    changes_in_period: int
    changes_by_type: dict
    c_suite_changes: int
    board_changes: int
    avg_ceo_tenure_months: Optional[float] = None
    avg_cfo_tenure_months: Optional[float] = None
    avg_c_suite_tenure_months: Optional[float] = None
    instability_flags: List[InstabilityFlag] = []


class TalentFlowEntry(BaseModel):
    """Single company's talent flow."""

    company_id: int
    company_name: str
    hires: int
    departures: int
    net: int


class TalentFlowResponse(BaseModel):
    """Talent flow analysis response."""

    industry: str
    period_days: int
    net_importers: List[TalentFlowEntry]
    net_exporters: List[TalentFlowEntry]
    stable: List[TalentFlowEntry]


class TrendDataPoint(BaseModel):
    """Single month's trend data."""

    month: str
    total: int
    hires: int = 0
    departures: int = 0
    promotions: int = 0
    retirements: int = 0


class TrendsResponse(BaseModel):
    """Change trends over time."""

    industry: str
    months: int
    trends: List[TrendDataPoint]


class HotRoleEntry(BaseModel):
    """Hot role with hiring count."""

    role: str
    hires: int


class BenchmarkComponents(BaseModel):
    """Benchmark score components."""

    completeness: float
    depth: float
    tenure: float
    board: float


class BenchmarkDetails(BaseModel):
    """Benchmark detail metrics."""

    has_ceo: bool
    has_cfo: bool
    has_coo: bool
    c_suite_count: int
    vp_count: int
    board_count: int
    avg_c_suite_tenure_months: Optional[float] = None


class BenchmarkScoreResponse(BaseModel):
    """Company benchmark score."""

    company_id: int
    company_name: str
    team_score: float
    components: BenchmarkComponents
    details: BenchmarkDetails


class PortfolioAnalyticsResponse(BaseModel):
    """Portfolio analytics response."""

    portfolio_id: int
    portfolio_name: str
    total_companies: int
    total_executives: int
    c_suite_count: int = 0
    changes_in_period: int = 0
    changes_by_type: dict = {}
    period_days: int


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/companies-with-pedigrees")
async def list_companies_with_pedigrees(
    db: Session = Depends(get_db),
):
    """List companies that have pedigree scores computed."""
    from app.core.people_models import PersonPedigreeScore, CompanyPerson, IndustrialCompany
    from sqlalchemy import func

    rows = (
        db.query(IndustrialCompany.id, IndustrialCompany.name, func.count(PersonPedigreeScore.id))
        .join(CompanyPerson, CompanyPerson.company_id == IndustrialCompany.id)
        .join(PersonPedigreeScore, PersonPedigreeScore.person_id == CompanyPerson.person_id)
        .filter(CompanyPerson.is_current == True)
        .group_by(IndustrialCompany.id, IndustrialCompany.name)
        .order_by(IndustrialCompany.name)
        .all()
    )
    return [{"id": r[0], "name": r[1], "scored_count": r[2]} for r in rows]


@router.get("/industries", response_model=List[str])
async def list_industries(
    db: Session = Depends(get_db),
):
    """
    List all industries with leadership data.

    Returns distinct industry segments from the companies table.
    """
    from app.core.people_models import IndustrialCompany

    industries = (
        db.query(IndustrialCompany.industry_segment)
        .distinct()
        .filter(IndustrialCompany.industry_segment.isnot(None))
        .all()
    )

    return [i[0] for i in industries if i[0]]


@router.get("/industries/{industry}/stats", response_model=IndustryStatsResponse)
async def get_industry_stats(
    industry: str,
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get comprehensive leadership statistics for an industry.

    Includes executive counts, change stats, tenure averages, and instability flags.
    """
    service = AnalyticsService(db)
    stats = service.get_industry_stats(industry=industry, days=days)

    # Convert instability_flags to proper model
    flags = [InstabilityFlag(**f) for f in stats.get("instability_flags", [])]
    stats["instability_flags"] = flags

    return IndustryStatsResponse(**stats)


@router.get("/stats", response_model=IndustryStatsResponse)
async def get_overall_stats(
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get comprehensive leadership statistics across all industries.

    Useful for dashboard overview.
    """
    service = AnalyticsService(db)
    stats = service.get_industry_stats(industry=None, days=days)

    # Convert instability_flags to proper model
    flags = [InstabilityFlag(**f) for f in stats.get("instability_flags", [])]
    stats["instability_flags"] = flags

    return IndustryStatsResponse(**stats)


@router.get("/industries/{industry}/talent-flow", response_model=TalentFlowResponse)
async def get_talent_flow(
    industry: str,
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Analyze talent flow for an industry.

    Shows which companies are net importers vs exporters of executive talent.
    """
    service = AnalyticsService(db)
    flow = service.get_talent_flow(industry=industry, days=days)

    return TalentFlowResponse(
        industry=flow["industry"],
        period_days=flow["period_days"],
        net_importers=[TalentFlowEntry(**e) for e in flow["net_importers"]],
        net_exporters=[TalentFlowEntry(**e) for e in flow["net_exporters"]],
        stable=[TalentFlowEntry(**e) for e in flow["stable"]],
    )


@router.get("/talent-flow", response_model=TalentFlowResponse)
async def get_overall_talent_flow(
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Analyze talent flow across all industries.
    """
    service = AnalyticsService(db)
    flow = service.get_talent_flow(industry=None, days=days)

    return TalentFlowResponse(
        industry=flow["industry"],
        period_days=flow["period_days"],
        net_importers=[TalentFlowEntry(**e) for e in flow["net_importers"]],
        net_exporters=[TalentFlowEntry(**e) for e in flow["net_exporters"]],
        stable=[TalentFlowEntry(**e) for e in flow["stable"]],
    )


@router.get("/industries/{industry}/trends", response_model=TrendsResponse)
async def get_change_trends(
    industry: str,
    months: int = Query(12, ge=1, le=24, description="Months of history"),
    db: Session = Depends(get_db),
):
    """
    Get monthly leadership change trends for an industry.

    Returns time series of leadership changes by type.
    """
    service = AnalyticsService(db)
    trends = service.get_change_trends(industry=industry, months=months)

    return TrendsResponse(
        industry=trends["industry"],
        months=trends["months"],
        trends=[TrendDataPoint(**t) for t in trends["trends"]],
    )


@router.get("/trends", response_model=TrendsResponse)
async def get_overall_trends(
    months: int = Query(12, ge=1, le=24, description="Months of history"),
    db: Session = Depends(get_db),
):
    """
    Get monthly leadership change trends across all industries.
    """
    service = AnalyticsService(db)
    trends = service.get_change_trends(industry=None, months=months)

    return TrendsResponse(
        industry=trends["industry"],
        months=trends["months"],
        trends=[TrendDataPoint(**t) for t in trends["trends"]],
    )


@router.get("/industries/{industry}/hot-roles", response_model=List[HotRoleEntry])
async def get_hot_roles(
    industry: str,
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get most frequently hired leadership roles in an industry.

    Identifies roles with highest hiring activity.
    """
    service = AnalyticsService(db)
    roles = service.get_hot_roles(industry=industry, days=days)
    return [HotRoleEntry(**r) for r in roles]


@router.get("/hot-roles", response_model=List[HotRoleEntry])
async def get_overall_hot_roles(
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get most frequently hired leadership roles across all industries.
    """
    service = AnalyticsService(db)
    roles = service.get_hot_roles(industry=None, days=days)
    return [HotRoleEntry(**r) for r in roles]


@router.get("/companies/{company_id}/benchmark", response_model=BenchmarkScoreResponse)
async def get_company_benchmark(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Get team strength benchmark score for a company.

    Returns 0-100 score based on team completeness, tenure, and stability.
    Components:
    - Completeness (25 pts): Key roles filled (CEO, CFO, COO)
    - Depth (25 pts): Total C-suite and VP count
    - Tenure (25 pts): Average C-suite tenure
    - Board (25 pts): Board size and strength
    """
    service = AnalyticsService(db)
    result = service.get_company_benchmark_score(company_id)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return BenchmarkScoreResponse(
        company_id=result["company_id"],
        company_name=result["company_name"],
        team_score=result["team_score"],
        components=BenchmarkComponents(**result["components"]),
        details=BenchmarkDetails(**result["details"]),
    )


@router.get("/portfolios/{portfolio_id}", response_model=PortfolioAnalyticsResponse)
async def get_portfolio_analytics(
    portfolio_id: int,
    days: int = Query(90, ge=1, le=365, description="Days to analyze"),
    db: Session = Depends(get_db),
):
    """
    Get leadership analytics for a specific portfolio.

    Aggregates leadership stats across all portfolio companies.
    """
    service = AnalyticsService(db)
    result = service.get_portfolio_analytics(portfolio_id, days=days)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return PortfolioAnalyticsResponse(**result)


# ── Pedigree Scoring ──────────────────────────────────────────────────────────

@router.post("/companies/{company_id}/score-pedigree")
async def score_company_pedigree(
    company_id: int,
    db: Session = Depends(get_db),
):
    """Compute and cache pedigree scores for all current executives at a company."""
    from app.services.pedigree_scorer import PedigreeScorer
    scorer = PedigreeScorer()
    scores = scorer.score_company(company_id, db)
    if not scores:
        raise HTTPException(status_code=404, detail="No executives found for company")
    return {
        "company_id": company_id,
        "scored": len(scores),
        "team_avg_pedigree": round(sum(float(s.overall_pedigree_score or 0) for s in scores) / len(scores), 1),
        "pe_experienced": sum(1 for s in scores if s.pe_experience),
        "tier1_employers": sum(1 for s in scores if s.tier1_employer),
        "elite_educated": sum(1 for s in scores if s.elite_education),
        "members": [
            {
                "person_id": s.person_id,
                "overall": float(s.overall_pedigree_score or 0),
                "pe_experience": s.pe_experience,
                "exit_experience": s.exit_experience,
                "tier1_employer": s.tier1_employer,
                "elite_education": s.elite_education,
                "top_employers": s.top_employers or [],
                "mba_school": s.mba_school,
            }
            for s in sorted(scores, key=lambda x: float(x.overall_pedigree_score or 0), reverse=True)
        ],
    }


@router.post("/parse-bios")
async def parse_bios_batch(
    limit: int = 100,
    overwrite: bool = False,
    background_tasks = None,
    db: Session = Depends(get_db),
):
    """
    Trigger LLM bio parsing for all people with biography text.

    Parses bio → PersonExperience + PersonEducation rows.
    Runs synchronously (returns stats when done).
    Use limit to control batch size (default 100).
    """
    import asyncio
    from app.services.bio_parser_service import BioParserService

    service = BioParserService()
    stats = await service.parse_all(db, limit=limit, overwrite=overwrite)
    return {
        "status": "complete",
        "limit": limit,
        "overwrite": overwrite,
        **stats,
    }


@router.post("/score-all-pedigrees")
async def score_all_pedigrees(
    db: Session = Depends(get_db),
):
    """
    Run PedigreeScorer for all companies that have experience data.

    Returns summary: companies scored, people scored, avg pedigree score.
    """
    from app.core.people_models import PersonExperience, CompanyPerson, IndustrialCompany
    from app.services.pedigree_scorer import PedigreeScorer
    from sqlalchemy import distinct

    # Find companies where at least one current person has experience data
    company_ids_with_exp = [
        r[0] for r in
        db.query(distinct(CompanyPerson.company_id))
        .join(PersonExperience, PersonExperience.person_id == CompanyPerson.person_id)
        .filter(CompanyPerson.is_current == True)
        .all()
    ]

    if not company_ids_with_exp:
        return {
            "companies_scored": 0,
            "people_scored": 0,
            "avg_pedigree_score": None,
            "message": "No experience data found — run POST /parse-bios first",
        }

    scorer = PedigreeScorer()
    all_scores = []
    companies_scored = 0

    for company_id in company_ids_with_exp:
        scores = scorer.score_company(company_id, db)
        if scores:
            all_scores.extend(scores)
            companies_scored += 1

    avg = None
    if all_scores:
        avg = round(
            sum(float(s.overall_pedigree_score or 0) for s in all_scores) / len(all_scores), 1
        )

    return {
        "companies_scored": companies_scored,
        "people_scored": len(all_scores),
        "avg_pedigree_score": avg,
    }


@router.get("/companies-with-org-charts")
async def list_companies_with_org_charts(
    db: Session = Depends(get_db),
):
    """List all companies that have org chart snapshots in the DB."""
    from app.core.people_models import OrgChartSnapshot, IndustrialCompany
    from sqlalchemy import func

    # Subquery: latest snapshot per company
    latest = (
        db.query(
            OrgChartSnapshot.company_id,
            func.max(OrgChartSnapshot.snapshot_date).label("max_date"),
        )
        .group_by(OrgChartSnapshot.company_id)
        .subquery()
    )
    rows = (
        db.query(
            IndustrialCompany.id,
            IndustrialCompany.name,
            OrgChartSnapshot.total_people,
            OrgChartSnapshot.departments,
            OrgChartSnapshot.snapshot_date,
        )
        .join(latest, latest.c.company_id == IndustrialCompany.id)
        .join(
            OrgChartSnapshot,
            (OrgChartSnapshot.company_id == IndustrialCompany.id)
            & (OrgChartSnapshot.snapshot_date == latest.c.max_date),
        )
        .order_by(IndustrialCompany.name)
        .all()
    )
    return [
        {
            "id": r[0],
            "name": r[1],
            "total_people": r[2],
            "departments": r[3] or [],
            "snapshot_date": r[4].isoformat() if r[4] else None,
        }
        for r in rows
    ]


def _normalize_name(name: str) -> str:
    """Normalize a person's name for dedup comparison.

    Strips middle initials, suffixes, and extra whitespace so that
    'Andrew F. Sullivan' and 'Andrew Sullivan' compare as equal.
    """
    import re

    name = name.lower().strip()
    name = re.sub(r"\s+[a-z]\.\s+", " ", name)  # strip middle initial "F."
    name = re.sub(r"\b(jr|sr|ii|iii|iv|phd|md|esq|cpa|cfa|jd|mba)\.?\b", "", name)
    return re.sub(r"\s+", " ", name).strip()


def _count_desc(node: dict) -> int:
    """Count total descendants of a chart node."""
    return sum(1 + _count_desc(c) for c in node.get("children", []))


def _sanitize_org_chart(chart_data: dict) -> dict:
    """Dedup org chart nodes by person_id and normalized name.

    Multiple collection runs can create duplicate company_people rows for the
    same person (e.g., 'Andrew Sullivan' from SEC + 'Andrew F. Sullivan' from
    website). This pass removes duplicates, keeping the node with the most
    descendants (richest data).
    """
    import copy

    chart_data = copy.deepcopy(chart_data)
    root = chart_data.get("root")
    if not root:
        return chart_data

    # Pass 1: find the richest node per person_id
    best_by_pid: dict = {}

    def _collect(node: dict) -> None:
        pid = node.get("person_id")
        if pid is not None:
            if pid not in best_by_pid or _count_desc(node) > _count_desc(best_by_pid[pid]):
                best_by_pid[pid] = node
        for child in node.get("children", []):
            _collect(child)

    _collect(root)

    # Pass 2: also track best node per normalized name (catches middle-initial variants)
    best_by_name: dict = {}
    for node in best_by_pid.values():
        norm = _normalize_name(node.get("name", ""))
        if norm and (norm not in best_by_name or _count_desc(node) > _count_desc(best_by_name[norm])):
            best_by_name[norm] = node

    # Pass 3: dedup traversal — remove any node whose person_id or normalized
    # name has already been seen
    seen_pids: set = set()
    seen_names: set = set()

    def _dedup(node: dict):
        pid = node.get("person_id")
        norm = _normalize_name(node.get("name", ""))

        if pid is not None:
            if pid in seen_pids:
                return None
            if norm and norm in seen_names:
                return None
            seen_pids.add(pid)
            if norm:
                seen_names.add(norm)

        node["children"] = [
            c for c in [_dedup(c) for c in node.get("children", [])]
            if c is not None
        ]
        return node

    _dedup(root)
    chart_data["root"] = root
    return chart_data


@router.get("/companies/{company_id}/org-chart")
async def get_company_org_chart(
    company_id: int,
    db: Session = Depends(get_db),
):
    """Return the most recent org chart snapshot for a company."""
    from app.core.people_models import OrgChartSnapshot, IndustrialCompany

    company = db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    snapshot = (
        db.query(OrgChartSnapshot)
        .filter(OrgChartSnapshot.company_id == company_id)
        .order_by(OrgChartSnapshot.snapshot_date.desc())
        .first()
    )
    if not snapshot:
        raise HTTPException(status_code=404, detail="No org chart found for this company")

    chart_data = _sanitize_org_chart(snapshot.chart_data or {})

    return {
        "company_id": company_id,
        "company_name": company.name,
        "snapshot_date": snapshot.snapshot_date.isoformat() if snapshot.snapshot_date else None,
        "total_people": snapshot.total_people,
        "max_depth": snapshot.max_depth,
        "departments": snapshot.departments or [],
        "chart_data": chart_data,
    }


@router.get("/companies/{company_id}/pedigree-report")
async def get_company_pedigree_report(
    company_id: int,
    db: Session = Depends(get_db),
):
    """Return cached pedigree scores for a company's leadership team."""
    from app.core.people_models import PersonPedigreeScore, CompanyPerson, Person
    rows = (
        db.query(PersonPedigreeScore, Person, CompanyPerson)
        .join(Person, PersonPedigreeScore.person_id == Person.id)
        .join(CompanyPerson, CompanyPerson.person_id == Person.id)
        .filter(
            CompanyPerson.company_id == company_id,
            CompanyPerson.is_current == True,
        )
        .order_by(PersonPedigreeScore.overall_pedigree_score.desc().nullslast())
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No pedigree data — run POST /score-pedigree first")
    scores = [r[0] for r in rows]
    people = [r[1] for r in rows]
    cps = [r[2] for r in rows]
    return {
        "company_id": company_id,
        "scored_count": len(scores),
        "team_avg_pedigree": round(sum(float(s.overall_pedigree_score or 0) for s in scores) / len(scores), 1),
        "flags": {
            "pe_experienced_pct": round(sum(1 for s in scores if s.pe_experience) / len(scores) * 100),
            "tier1_employer_pct": round(sum(1 for s in scores if s.tier1_employer) / len(scores) * 100),
            "elite_education_pct": round(sum(1 for s in scores if s.elite_education) / len(scores) * 100),
        },
        "members": [
            {
                "person_id": s.person_id,
                "full_name": p.full_name,
                "title": cp.title,
                "overall_pedigree_score": float(s.overall_pedigree_score or 0),
                "employer_quality_score": float(s.employer_quality_score or 0),
                "career_velocity_score": float(s.career_velocity_score or 0),
                "pe_experience": s.pe_experience,
                "exit_experience": s.exit_experience,
                "tier1_employer": s.tier1_employer,
                "elite_education": s.elite_education,
                "top_employers": s.top_employers or [],
                "mba_school": s.mba_school,
                "avg_tenure_months": s.avg_tenure_months,
                "scored_at": s.scored_at.isoformat() if s.scored_at else None,
            }
            for s, p, cp in zip(scores, people, cps)
        ],
    }


# ---------------------------------------------------------------------------
# QA Endpoints (SPEC_030)
# ---------------------------------------------------------------------------


@router.get("/qa-report")
async def get_qa_report(
    limit: int = Query(100, ge=1, le=500, description="Max companies to evaluate"),
    db: Session = Depends(get_db),
):
    """Run rule-based QA checks across all active companies.

    Returns a list sorted by health_score ascending (worst first).
    Health score is 0-100: 100 = no issues, deducted per severity.
    """
    from app.services.people_qa_service import PeopleQAService

    reports = PeopleQAService().run_all(db, limit=limit)
    # Serialize QualityReport dataclasses; add legacy aliases so
    # the qa-dashboard.html frontend continues to work unchanged.
    return [
        {
            **r.to_dict(),
            "company_id": r.entity_id,
            "company_name": r.entity_name,
            "health_score": r.quality_score,
        }
        for r in reports
    ]


@router.get("/qa/merge-candidates")
async def list_merge_candidates(
    status: str = Query("pending", description="Filter by status: pending, approved, rejected, auto_merged"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Return merge candidates enriched with person + company details for review UI."""
    from app.core.people_models import PeopleMergeCandidate, Person, CompanyPerson, IndustrialCompany

    candidates = (
        db.query(PeopleMergeCandidate)
        .filter(PeopleMergeCandidate.status == status)
        .order_by(PeopleMergeCandidate.similarity_score.desc())
        .limit(limit)
        .all()
    )

    def _person_card(person_id: int) -> dict:
        person = db.query(Person).filter(Person.id == person_id).first()
        if not person:
            return {"id": person_id, "name": "Unknown", "title": None, "company": None, "sources": []}
        cp = (
            db.query(CompanyPerson)
            .filter(CompanyPerson.person_id == person_id, CompanyPerson.is_current.is_(True))
            .order_by(CompanyPerson.extraction_date.desc())
            .first()
        )
        return {
            "id": person_id,
            "name": person.full_name,
            "title": cp.title if cp else None,
            "company": (
                db.query(IndustrialCompany).filter(IndustrialCompany.id == cp.company_id).first()
            ).name if cp and cp.company_id else None,
            "company_id": cp.company_id if cp else None,
            "source": cp.source if cp else None,
            "confidence": cp.confidence if cp else None,
            "confidence_score": float(person.confidence_score) if person.confidence_score else None,
            "data_sources": person.data_sources or [],
        }

    return [
        {
            "id": c.id,
            "similarity_score": float(c.similarity_score) if c.similarity_score else None,
            "match_type": c.match_type,
            "status": c.status,
            "evidence_notes": c.evidence_notes,
            "shared_company_ids": c.shared_company_ids or [],
            "person_a": _person_card(c.person_id_a),
            "person_b": _person_card(c.person_id_b),
        }
        for c in candidates
    ]


class MergeResolveBody(BaseModel):
    action: str  # "approve" | "reject"


@router.post("/qa/merge-candidates/{candidate_id}/resolve")
async def resolve_merge_candidate(
    candidate_id: int,
    body: MergeResolveBody,
    db: Session = Depends(get_db),
):
    """Approve or reject a merge candidate.

    - approve: marks lower-confidence person as non-canonical, sets canonical_id → winner
    - reject: marks as rejected, no person record changes
    """
    from app.core.people_models import PeopleMergeCandidate, Person

    if body.action not in ("approve", "reject"):
        raise HTTPException(status_code=400, detail="action must be 'approve' or 'reject'")

    candidate = db.query(PeopleMergeCandidate).filter(PeopleMergeCandidate.id == candidate_id).first()
    if not candidate:
        raise HTTPException(status_code=404, detail="Merge candidate not found")
    if candidate.status not in ("pending",):
        raise HTTPException(status_code=409, detail=f"Candidate already resolved: {candidate.status}")

    if body.action == "approve":
        person_a = db.query(Person).filter(Person.id == candidate.person_id_a).first()
        person_b = db.query(Person).filter(Person.id == candidate.person_id_b).first()

        if person_a and person_b:
            # Winner = higher confidence_score; tie-break on lower id (earlier/more established)
            score_a = float(person_a.confidence_score or 0)
            score_b = float(person_b.confidence_score or 0)
            if score_a >= score_b:
                winner, loser = person_a, person_b
            else:
                winner, loser = person_b, person_a

            loser.canonical_id = winner.id
            loser.is_canonical = False
            candidate.canonical_person_id = winner.id

        candidate.status = "approved"
    else:
        candidate.status = "rejected"

    from datetime import datetime
    candidate.reviewed_at = datetime.utcnow()
    db.commit()

    return {"status": "ok", "action": body.action, "candidate_id": candidate_id}
