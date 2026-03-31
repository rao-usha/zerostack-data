"""
PE Fund Conviction API — LP conviction signals for PE funds.

Endpoints:
  POST /pe/conviction/score/{fund_id}   — compute/refresh conviction score
  GET  /pe/conviction/score/{fund_id}   — get latest score
  GET  /pe/conviction/lp-base/{firm_id} — LP investor base composition
  GET  /pe/conviction/signals           — market-wide conviction signals
  POST /pe/conviction/collect           — trigger LP commitment collection
"""
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import select, desc

from app.core.database import get_db

router = APIRouter(prefix="/pe/conviction", tags=["PE Conviction"])
logger = logging.getLogger(__name__)


@router.post("/score/{fund_id}")
async def compute_conviction_score(fund_id: int, db: Session = Depends(get_db)):
    """Compute or refresh the LP conviction score for a PE fund."""
    from app.core.pe_models import PEFund, PEFundConvictionScore
    from app.services.pe_fund_conviction_scorer import FundConvictionScorer

    fund = db.get(PEFund, fund_id)
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")

    scorer = FundConvictionScorer()

    # Gather available data from the fund record
    target_usd = (
        float(fund.target_size_usd_millions) * 1e6
        if fund.target_size_usd_millions else None
    )
    final_usd = (
        float(fund.final_close_usd_millions) * 1e6
        if fund.final_close_usd_millions else None
    )

    result = scorer.score_from_data(
        fund_id=fund_id,
        target_size_usd=target_usd,
        final_close_usd=final_usd,
        first_close_date=fund.first_close_date,
        final_close_date=fund.final_close_date,
    )

    # Enrich with LP relationship data if available
    from app.core.models import LpGpRelationship

    lp_stmt = select(LpGpRelationship).where(
        LpGpRelationship.gp_firm_id == fund.firm_id
    )
    lp_rels = db.execute(lp_stmt).scalars().all()

    if lp_rels:
        total_lps = len(lp_rels)
        repeat_lps = sum(1 for r in lp_rels if r.total_vintages_committed > 1)
        reup_rate = repeat_lps / total_lps if total_lps > 0 else None

        result = scorer.score_from_data(
            fund_id=fund_id,
            lp_count=total_lps,
            repeat_lp_count=repeat_lps,
            reup_rate_pct=reup_rate,
            target_size_usd=target_usd,
            final_close_usd=final_usd,
            first_close_date=fund.first_close_date,
            final_close_date=fund.final_close_date,
        )

    # Persist score
    score_record = PEFundConvictionScore(
        fund_id=fund_id,
        conviction_score=result.conviction_score,
        conviction_grade=result.conviction_grade,
        lp_quality_score=result.sub_scores.get("lp_quality"),
        reup_rate_score=result.sub_scores.get("reup_rate"),
        oversubscription_score=result.sub_scores.get("oversubscription"),
        lp_diversity_score=result.sub_scores.get("lp_diversity"),
        time_to_close_score=result.sub_scores.get("time_to_close"),
        gp_commitment_score=result.sub_scores.get("gp_commitment"),
        lp_count=result.lp_count,
        repeat_lp_count=result.repeat_lp_count,
        tier1_lp_count=result.tier1_lp_count,
        oversubscription_ratio=result.oversubscription_ratio,
        days_to_final_close=result.days_to_final_close,
        reup_rate_pct=result.reup_rate_pct,
        data_completeness=result.data_completeness,
        scoring_notes=result.scoring_notes,
        scored_at=datetime.utcnow(),
    )
    db.add(score_record)
    db.commit()
    db.refresh(score_record)

    return {
        "fund_id": fund_id,
        "fund_name": fund.name,
        "conviction_score": result.conviction_score,
        "conviction_grade": result.conviction_grade,
        "data_completeness": result.data_completeness,
        "signals": [
            {
                "name": s.name,
                "score": s.score,
                "weight": s.weight,
                "raw_value": s.raw_value,
                "explanation": s.explanation,
            }
            for s in result.signals
        ],
        "scoring_notes": result.scoring_notes,
        "scored_at": score_record.scored_at.isoformat(),
    }


@router.get("/score/{fund_id}")
async def get_conviction_score(fund_id: int, db: Session = Depends(get_db)):
    """Get the latest conviction score for a PE fund."""
    from app.core.pe_models import PEFundConvictionScore, PEFund

    fund = db.get(PEFund, fund_id)
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")

    stmt = (
        select(PEFundConvictionScore)
        .where(PEFundConvictionScore.fund_id == fund_id)
        .order_by(desc(PEFundConvictionScore.scored_at))
        .limit(1)
    )
    score = db.execute(stmt).scalar_one_or_none()

    if not score:
        raise HTTPException(
            status_code=404,
            detail="No conviction score computed yet. POST to /score/{fund_id} first.",
        )

    return {
        "fund_id": fund_id,
        "fund_name": fund.name,
        "conviction_score": score.conviction_score,
        "conviction_grade": score.conviction_grade,
        "sub_scores": {
            "lp_quality": score.lp_quality_score,
            "reup_rate": score.reup_rate_score,
            "oversubscription": score.oversubscription_score,
            "lp_diversity": score.lp_diversity_score,
            "time_to_close": score.time_to_close_score,
            "gp_commitment": score.gp_commitment_score,
        },
        "raw_signals": {
            "lp_count": score.lp_count,
            "repeat_lp_count": score.repeat_lp_count,
            "tier1_lp_count": score.tier1_lp_count,
            "oversubscription_ratio": score.oversubscription_ratio,
            "days_to_final_close": score.days_to_final_close,
            "reup_rate_pct": score.reup_rate_pct,
        },
        "data_completeness": score.data_completeness,
        "scoring_notes": score.scoring_notes,
        "scored_at": score.scored_at.isoformat(),
    }


@router.get("/lp-base/{firm_id}")
async def get_firm_lp_base(firm_id: int, db: Session = Depends(get_db)):
    """Get LP investor base composition and re-up history for a PE firm."""
    from app.core.pe_models import PEFirm
    from app.core.models import LpGpRelationship, LpFund

    firm = db.get(PEFirm, firm_id)
    if not firm:
        raise HTTPException(status_code=404, detail="Firm not found")

    # Get all LP relationships for this GP
    stmt = (
        select(LpGpRelationship, LpFund)
        .join(LpFund, LpGpRelationship.lp_id == LpFund.id)
        .where(LpGpRelationship.gp_firm_id == firm_id)
        .order_by(desc(LpGpRelationship.total_vintages_committed))
    )
    results = db.execute(stmt).all()

    lp_base = [
        {
            "lp_id": lp.id,
            "lp_name": lp.name,
            "lp_type": lp.lp_type,
            "first_vintage": rel.first_vintage,
            "last_vintage": rel.last_vintage,
            "total_vintages": rel.total_vintages_committed,
            "total_committed_usd": rel.total_committed_usd,
            "avg_commitment_usd": rel.avg_commitment_usd,
            "commitment_trend": rel.commitment_trend,
        }
        for rel, lp in results
    ]

    # Summary stats
    repeat_lps = sum(1 for lp in lp_base if lp["total_vintages"] > 1)
    total = len(lp_base)

    return {
        "firm_id": firm_id,
        "firm_name": firm.name,
        "total_lp_relationships": total,
        "repeat_lps": repeat_lps,
        "reup_rate": repeat_lps / total if total > 0 else None,
        "lp_base": lp_base,
    }


@router.get("/signals")
async def get_market_conviction_signals(
    strategy: Optional[str] = None,
    min_score: float = 0,
    db: Session = Depends(get_db),
):
    """Get market-wide LP conviction signals, optionally filtered by PE strategy."""
    from app.core.pe_models import PEFundConvictionScore, PEFund, PEFirm

    stmt = (
        select(PEFundConvictionScore, PEFund, PEFirm)
        .join(PEFund, PEFundConvictionScore.fund_id == PEFund.id)
        .join(PEFirm, PEFund.firm_id == PEFirm.id)
        .where(PEFundConvictionScore.conviction_score >= min_score)
        .order_by(desc(PEFundConvictionScore.scored_at))
    )
    if strategy:
        stmt = stmt.where(PEFirm.primary_strategy.ilike(f"%{strategy}%"))

    results = db.execute(stmt).all()

    signals = [
        {
            "fund_id": fund.id,
            "fund_name": fund.name,
            "firm_name": firm.name,
            "vintage_year": fund.vintage_year,
            "conviction_score": score.conviction_score,
            "conviction_grade": score.conviction_grade,
            "oversubscription_ratio": score.oversubscription_ratio,
            "reup_rate_pct": score.reup_rate_pct,
            "lp_count": score.lp_count,
        }
        for score, fund, firm in results
    ]

    return {"signals": signals, "count": len(signals)}


@router.post("/collect")
async def trigger_lp_collection(
    background_tasks: BackgroundTasks,
    sources: Optional[list] = None,
    db: Session = Depends(get_db),
):
    """Trigger LP commitment collection from all configured sources."""
    from app.agents.fund_lp_tracker_agent import FundLPTrackerAgent
    import asyncio

    def run_collection():
        agent = FundLPTrackerAgent(db)
        asyncio.run(agent.run(sources=sources))
        logger.info("LP collection complete")

    background_tasks.add_task(run_collection)

    return {
        "status": "started",
        "message": "LP commitment collection started in background",
        "sources": sources or ["cafr", "form_990", "form_d"],
    }


@router.post("/seed-public")
async def seed_public_lp_data(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Seed LP commitment data from public sources (Path B: hardcoded annual report data + Path C: HTML portals)."""
    from app.agents.fund_lp_tracker_agent import FundLPTrackerAgent
    import asyncio

    def run_seed():
        agent = FundLPTrackerAgent(db)
        asyncio.run(agent.run(sources=["public_seed", "html_portal"]))
        logger.info("Public LP seed complete")

    background_tasks.add_task(run_seed)

    return {
        "status": "started",
        "message": "Seeding LP commitments from public annual reports + HTML portals",
        "sources": ["public_seed", "html_portal"],
    }


@router.post("/classify-lps")
async def classify_lps(db: Session = Depends(get_db)):
    """Run LP tier classification on all lp_fund rows with null lp_tier."""
    try:
        from app.sources.lp_collection.lp_tier_classifier import classify_all_lps
        updated = classify_all_lps(db)
        return {"status": "ok", "lps_classified": updated}
    except Exception as e:
        logger.error(f"LP classification failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lp-commitments")
async def list_lp_commitments(
    lp_id: Optional[int] = None,
    gp_name: Optional[str] = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Browse raw LpGpCommitment records, filtered by LP or GP."""
    from app.core.models import LpGpCommitment, LpFund
    from sqlalchemy import select

    stmt = select(LpGpCommitment)
    if lp_id:
        stmt = stmt.where(LpGpCommitment.lp_id == lp_id)
    if gp_name:
        stmt = stmt.where(LpGpCommitment.gp_name.ilike(f"%{gp_name}%"))
    stmt = stmt.order_by(desc(LpGpCommitment.as_of_date)).limit(limit)

    commitments = db.execute(stmt).scalars().all()

    return {
        "total": len(commitments),
        "commitments": [
            {
                "id": c.id,
                "lp_id": c.lp_id,
                "gp_name": c.gp_name,
                "fund_name": c.fund_name,
                "fund_vintage": c.fund_vintage,
                "commitment_amount_usd": float(c.commitment_amount_usd) if c.commitment_amount_usd else None,
                "status": c.status,
                "data_source": c.data_source,
                "as_of_date": c.as_of_date.isoformat() if c.as_of_date else None,
            }
            for c in commitments
        ],
    }


@router.get("/coverage")
async def get_coverage(db: Session = Depends(get_db)):
    """Show data coverage stats: LPs with data, vintages covered, sources."""
    from app.core.models import LpGpCommitment, LpFund
    from sqlalchemy import select, func, distinct

    # Aggregate stats
    total_stmt = select(func.count(LpGpCommitment.id))
    total = db.execute(total_stmt).scalar() or 0

    unique_gps_stmt = select(func.count(distinct(LpGpCommitment.gp_name)))
    unique_gps = db.execute(unique_gps_stmt).scalar() or 0

    unique_lps_stmt = select(func.count(distinct(LpGpCommitment.lp_id)))
    unique_lps = db.execute(unique_lps_stmt).scalar() or 0

    vintage_stmt = select(
        func.min(LpGpCommitment.fund_vintage),
        func.max(LpGpCommitment.fund_vintage),
    )
    vintage_row = db.execute(vintage_stmt).one_or_none()
    vintages = (
        f"{vintage_row[0]}-{vintage_row[1]}"
        if vintage_row and vintage_row[0]
        else "none"
    )

    # By source breakdown
    source_stmt = select(
        LpGpCommitment.data_source,
        func.count(LpGpCommitment.id).label("count"),
    ).group_by(LpGpCommitment.data_source)
    source_rows = db.execute(source_stmt).all()
    by_source = {row.data_source: row.count for row in source_rows}

    # Per-LP detail
    lp_detail_stmt = (
        select(
            LpFund.name,
            func.count(LpGpCommitment.id).label("commitment_count"),
            func.min(LpGpCommitment.fund_vintage).label("earliest_vintage"),
            func.max(LpGpCommitment.fund_vintage).label("latest_vintage"),
        )
        .join(LpGpCommitment, LpFund.id == LpGpCommitment.lp_id)
        .group_by(LpFund.id, LpFund.name)
        .order_by(func.count(LpGpCommitment.id).desc())
    )
    lp_rows = db.execute(lp_detail_stmt).all()
    lps_with_data = [
        {
            "name": row.name,
            "commitment_count": row.commitment_count,
            "earliest_vintage": row.earliest_vintage,
            "latest_vintage": row.latest_vintage,
        }
        for row in lp_rows
    ]

    return {
        "total_lp_commitments": total,
        "unique_gps": unique_gps,
        "unique_lps": unique_lps,
        "vintages_covered": vintages,
        "by_source": by_source,
        "lps_with_data": lps_with_data,
    }
