"""
PE Company 360 — Unified Company Intelligence.

Aggregates all available data about a PE target into a single view:
profile, benchmarks, exit readiness, deal score, comps, leadership,
competitors, alerts, pipeline status, and investment thesis.
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PEAlert,
    PECompanyLeadership,
    PECompetitorMapping,
    PEDeal,
    PEFund,
    PEFundInvestment,
    PEPortfolioCompany,
    PEPortfolioSnapshot,
)

logger = logging.getLogger(__name__)


@dataclass
class Company360Result:
    """Complete intelligence view for a single company."""
    company_id: int
    company_name: str
    profile: Optional[Dict[str, Any]] = None
    benchmarks: Optional[Dict[str, Any]] = None
    exit_readiness: Optional[Dict[str, Any]] = None
    deal_score: Optional[Dict[str, Any]] = None
    comparable_transactions: Optional[Dict[str, Any]] = None
    buyer_analysis: Optional[Dict[str, Any]] = None
    leadership: List[Dict[str, Any]] = field(default_factory=list)
    competitors: List[Dict[str, Any]] = field(default_factory=list)
    recent_alerts: List[Dict[str, Any]] = field(default_factory=list)
    pipeline_deals: List[Dict[str, Any]] = field(default_factory=list)
    snapshot_trend: Optional[Dict[str, Any]] = None
    thesis: Optional[Dict[str, Any]] = None
    data_completeness: int = 0


def get_company_360(
    db: Session,
    company_id: int,
    firm_id: Optional[int] = None,
) -> Optional[Company360Result]:
    """Aggregate all intelligence for a company into a single view.

    Each section is fetched independently — failures in one section
    do not affect others.
    """
    company = db.execute(
        select(PEPortfolioCompany).where(PEPortfolioCompany.id == company_id)
    ).scalar_one_or_none()
    if not company:
        return None

    result = Company360Result(company_id=company.id, company_name=company.name)
    sections_available = 0
    total_sections = 10

    # 1. Company profile
    result.profile = {
        "id": company.id,
        "name": company.name,
        "industry": company.industry,
        "sub_industry": company.sub_industry,
        "status": company.status,
        "headquarters_city": company.headquarters_city,
        "headquarters_state": company.headquarters_state,
        "founded_year": company.founded_year,
        "employee_count": company.employee_count,
        "website": company.website,
        "description": company.description,
    }
    sections_available += 1

    # 2. Financial benchmarks
    try:
        from app.core.pe_benchmarking import benchmark_company
        bench = benchmark_company(db, company_id)
        if bench:
            result.benchmarks = {
                "overall_percentile": bench.overall_percentile,
                "fiscal_year": bench.fiscal_year,
                "data_quality": bench.data_quality,
                "metrics": [
                    {
                        "metric": m.metric, "label": m.label,
                        "value": m.value, "percentile": m.percentile,
                        "industry_median": m.industry_median,
                        "trend": m.trend, "peer_count": m.peer_count,
                    }
                    for m in bench.metrics
                ],
            }
            sections_available += 1
    except Exception as e:
        logger.debug("Benchmarks unavailable for company %d: %s", company_id, e)

    # 3. Exit readiness
    try:
        from app.core.pe_exit_scoring import score_exit_readiness
        er = score_exit_readiness(db, company_id)
        if er:
            result.exit_readiness = {
                "composite_score": er.composite_score,
                "grade": er.grade,
                "confidence": er.confidence,
                "sub_scores": [
                    {"dimension": s.dimension, "label": s.label,
                     "raw_score": s.raw_score, "grade": s.grade,
                     "explanation": s.explanation}
                    for s in er.sub_scores
                ],
                "recommendations": er.recommendations[:5],
                "data_gaps": er.data_gaps,
            }
            sections_available += 1
    except Exception as e:
        logger.debug("Exit readiness unavailable for company %d: %s", company_id, e)

    # 4. Deal score
    try:
        from app.core.pe_deal_scorer import score_deal
        ds = score_deal(db, company_id)
        if ds:
            result.deal_score = {
                "composite_score": ds.composite_score,
                "grade": ds.grade,
                "dimensions": [
                    {"dimension": d.dimension, "label": d.label,
                     "raw_score": d.raw_score, "grade": d.grade,
                     "explanation": d.explanation}
                    for d in ds.dimensions
                ],
                "strengths": ds.strengths,
                "risks": ds.risks,
            }
            sections_available += 1
    except Exception as e:
        logger.debug("Deal score unavailable for company %d: %s", company_id, e)

    # 5. Comparable transactions
    try:
        from app.core.pe_comparable_transactions import ComparableTransactionService
        comps_service = ComparableTransactionService(db)
        comps = comps_service.get_comps(company_id)
        if comps and comps.get("deal_count", 0) > 0:
            result.comparable_transactions = {
                "deal_count": comps["deal_count"],
                "market_stats": comps.get("market_stats"),
                "comparable_deals": comps.get("comparable_deals", [])[:10],
            }
            sections_available += 1
    except Exception as e:
        logger.debug("Comps unavailable for company %d: %s", company_id, e)

    # 6. Leadership team
    try:
        from app.core.pe_models import PEPerson
        leaders = db.execute(
            select(PECompanyLeadership, PEPerson)
            .join(PEPerson, PEPerson.id == PECompanyLeadership.person_id)
            .where(
                PECompanyLeadership.company_id == company_id,
                PECompanyLeadership.is_current == True,
            )
        ).all()
        result.leadership = [
            {
                "name": person.full_name,
                "title": l.title,
                "is_ceo": l.is_ceo or False,
                "is_cfo": l.is_cfo or False,
                "is_board_member": l.is_board_member or False,
                "appointed_by_pe": l.appointed_by_pe,
                "role_category": l.role_category,
                "start_date": l.start_date.isoformat() if l.start_date else None,
            }
            for l, person in leaders
        ]
        if result.leadership:
            sections_available += 1
    except Exception as e:
        logger.debug("Leadership unavailable for company %d: %s", company_id, e)

    # 7. Competitor landscape
    try:
        competitors = db.execute(
            select(PECompetitorMapping)
            .where(PECompetitorMapping.company_id == company_id)
        ).scalars().all()
        result.competitors = [
            {
                "name": c.competitor_name,
                "competitor_type": c.competitor_type,
                "relative_size": c.relative_size,
                "market_position": c.market_position,
                "is_public": c.is_public or False,
                "ticker": c.ticker,
                "is_pe_backed": c.is_pe_backed or False,
                "pe_owner": c.pe_owner,
            }
            for c in competitors
        ]
        if result.competitors:
            sections_available += 1
    except Exception as e:
        logger.debug("Competitors unavailable for company %d: %s", company_id, e)

    # 8. Pipeline deals involving this company
    try:
        deals = db.execute(
            select(PEDeal)
            .where(PEDeal.company_id == company_id)
            .order_by(PEDeal.announced_date.desc())
        ).scalars().all()
        result.pipeline_deals = [
            {
                "id": d.id,
                "deal_name": d.deal_name,
                "deal_type": d.deal_type,
                "status": d.status,
                "pipeline_stage": d.pipeline_stage if hasattr(d, "pipeline_stage") else None,
                "enterprise_value_usd": float(d.enterprise_value_usd) if d.enterprise_value_usd else None,
                "announced_date": d.announced_date.isoformat() if d.announced_date else None,
                "buyer_name": d.buyer_name,
            }
            for d in deals
        ]
        if result.pipeline_deals:
            sections_available += 1
    except Exception as e:
        logger.debug("Pipeline deals unavailable for company %d: %s", company_id, e)

    # 9. Recent alerts
    try:
        alerts = db.execute(
            select(PEAlert)
            .where(PEAlert.company_id == company_id)
            .order_by(PEAlert.created_at.desc())
            .limit(10)
        ).scalars().all()
        result.recent_alerts = [
            {
                "alert_type": a.alert_type,
                "severity": a.severity,
                "title": a.title,
                "detail": a.detail,
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in alerts
        ]
        if result.recent_alerts:
            sections_available += 1
    except Exception as e:
        logger.debug("Alerts unavailable for company %d: %s", company_id, e)

    # 10. Snapshot trend
    try:
        snapshots = db.execute(
            select(PEPortfolioSnapshot)
            .where(PEPortfolioSnapshot.company_id == company_id)
            .order_by(PEPortfolioSnapshot.snapshot_date.desc())
            .limit(5)
        ).scalars().all()
        if snapshots:
            latest = snapshots[0]
            oldest = snapshots[-1] if len(snapshots) > 1 else None
            trend = "stable"
            if oldest and latest.exit_score and oldest.exit_score:
                delta = float(latest.exit_score) - float(oldest.exit_score)
                if delta > 3:
                    trend = "improving"
                elif delta < -3:
                    trend = "declining"

            result.snapshot_trend = {
                "latest_date": latest.snapshot_date.isoformat(),
                "latest_exit_score": float(latest.exit_score) if latest.exit_score else None,
                "latest_exit_grade": latest.exit_grade,
                "trend": trend,
                "snapshot_count": len(snapshots),
                "history": [
                    {
                        "date": s.snapshot_date.isoformat(),
                        "exit_score": float(s.exit_score) if s.exit_score else None,
                        "revenue": float(s.revenue) if s.revenue else None,
                        "ebitda_margin": float(s.ebitda_margin) if s.ebitda_margin else None,
                    }
                    for s in snapshots
                ],
            }
            sections_available += 1
    except Exception as e:
        logger.debug("Snapshots unavailable for company %d: %s", company_id, e)

    # 11. Thesis (if cached)
    try:
        from app.core.pe_models import PEInvestmentThesis
        thesis = db.execute(
            select(PEInvestmentThesis)
            .where(PEInvestmentThesis.company_id == company_id)
            .order_by(PEInvestmentThesis.generated_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if thesis:
            result.thesis = thesis.thesis_data
    except Exception:
        pass  # Table may not exist yet

    result.data_completeness = int((sections_available / total_sections) * 100)

    return result
