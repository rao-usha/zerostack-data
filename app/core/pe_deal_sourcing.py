"""
PE Deal Sourcing Service.

Automated deal discovery engine that reads market signals, identifies
high-momentum sectors, scores candidate companies, and auto-creates
pipeline entries at "Screening" stage.
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PEDeal,
    PEPortfolioCompany,
    PECompanyFinancials,
    PEFirm,
)
from app.core.pe_market_signals import get_high_momentum_sectors
from app.core.pe_deal_scorer import score_deal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCORE_THRESHOLD = 70  # B+ or above
REVENUE_MIN = 10_000_000    # $10M
REVENUE_MAX = 500_000_000   # $500M


@dataclass
class DealSourcingReport:
    """Result of a deal sourcing run."""
    firm_id: int
    source_type: str  # "market_scanner" or "acquisition_scorer"
    candidates_found: int = 0
    candidates_scored: int = 0
    deals_created: int = 0
    deals_skipped_duplicate: int = 0
    deals_skipped_low_score: int = 0
    top_opportunities: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def source_deals_from_signals(db: Session, firm_id: int) -> DealSourcingReport:
    """
    Main deal sourcing orchestrator using market signals.

    1. Reads latest market signals (sectors with momentum > 60)
    2. For each high-momentum sector, queries matching companies
    3. Scores each candidate via DealScorer
    4. Creates pipeline entries for B+ candidates (>= 70)
    5. Skips companies already in pipeline (deduplication)
    """
    report = DealSourcingReport(firm_id=firm_id, source_type="market_scanner")

    # Validate firm
    firm = db.execute(
        select(PEFirm).where(PEFirm.id == firm_id)
    ).scalar_one_or_none()
    if not firm:
        report.errors.append(f"Firm {firm_id} not found")
        return report

    # Get high-momentum sectors
    try:
        high_sectors = get_high_momentum_sectors(db, threshold=60)
    except Exception as e:
        logger.warning("Failed to get market signals: %s", e)
        high_sectors = []

    if not high_sectors:
        logger.info("No high-momentum sectors found for deal sourcing")
        return report

    sector_names = [s["sector"] for s in high_sectors]

    # Find candidate companies in those sectors
    candidates = _find_candidates_by_sector(db, sector_names)
    report.candidates_found = len(candidates)

    if not candidates:
        return report

    # Get existing pipeline company_ids to avoid duplicates
    existing_ids = _get_pipeline_company_ids(db)

    # Score and create pipeline entries
    _score_and_create_deals(
        db, firm_id, firm.name, candidates, existing_ids, report,
        data_source="market_scanner",
        sector_signals={s["sector"]: s for s in high_sectors},
    )

    return report


def source_deals_from_targets(db: Session, firm_id: int) -> DealSourcingReport:
    """
    Deal sourcing using acquisition target scoring across all companies.

    Scores all portfolio-adjacent companies and creates pipeline entries
    for top-scored targets.
    """
    report = DealSourcingReport(firm_id=firm_id, source_type="acquisition_scorer")

    firm = db.execute(
        select(PEFirm).where(PEFirm.id == firm_id)
    ).scalar_one_or_none()
    if not firm:
        report.errors.append(f"Firm {firm_id} not found")
        return report

    # Get all companies with financials
    candidates = _find_all_scoreable_candidates(db)
    report.candidates_found = len(candidates)

    if not candidates:
        return report

    existing_ids = _get_pipeline_company_ids(db)

    _score_and_create_deals(
        db, firm_id, firm.name, candidates, existing_ids, report,
        data_source="acquisition_scorer",
    )

    return report


def get_sourcing_history(
    db: Session,
    firm_id: int,
    days: int = 30,
) -> Dict[str, Any]:
    """
    Recent auto-sourced deals with conversion stats.

    Returns deals created by market_scanner or acquisition_scorer
    for the given firm within the time window.
    """
    cutoff = datetime.utcnow() - timedelta(days=days)

    # Find deals sourced by automation
    deals = db.execute(
        select(PEDeal).where(
            and_(
                PEDeal.data_source.in_(["market_scanner", "acquisition_scorer"]),
                PEDeal.created_at >= cutoff,
            )
        ).order_by(PEDeal.created_at.desc())
    ).scalars().all()

    # Stats
    total = len(deals)
    by_source = {}
    by_stage = {}
    for d in deals:
        src = d.data_source or "unknown"
        by_source[src] = by_source.get(src, 0) + 1
        stage = d.status or "unknown"
        by_stage[stage] = by_stage.get(stage, 0) + 1

    converted = sum(1 for d in deals if d.status in ("DD", "LOI", "Closing", "Won"))

    return {
        "firm_id": firm_id,
        "period_days": days,
        "total_sourced": total,
        "by_source": by_source,
        "by_stage": by_stage,
        "conversion_rate": round(converted / total * 100, 1) if total > 0 else 0.0,
        "deals": [
            {
                "id": d.id,
                "deal_name": d.deal_name,
                "company_id": d.company_id,
                "status": d.status,
                "data_source": d.data_source,
                "created_at": d.created_at.isoformat() if d.created_at else None,
            }
            for d in deals[:20]  # limit to 20 most recent
        ],
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _find_candidates_by_sector(
    db: Session, sector_names: List[str],
) -> List[PEPortfolioCompany]:
    """Find companies in the given sectors with revenue in target range."""
    # Get companies with recent financials in target sectors
    companies = db.execute(
        select(PEPortfolioCompany).where(
            PEPortfolioCompany.industry.in_(sector_names)
        )
    ).scalars().all()

    # Filter by revenue range using latest financials
    candidates = []
    for co in companies:
        latest_fin = db.execute(
            select(PECompanyFinancials)
            .where(PECompanyFinancials.company_id == co.id)
            .order_by(PECompanyFinancials.fiscal_year.desc())
            .limit(1)
        ).scalar_one_or_none()

        if latest_fin and latest_fin.revenue_usd:
            rev = float(latest_fin.revenue_usd)
            if REVENUE_MIN <= rev <= REVENUE_MAX:
                candidates.append(co)

    return candidates


def _find_all_scoreable_candidates(db: Session) -> List[PEPortfolioCompany]:
    """Find all companies with financials suitable for scoring."""
    companies = db.execute(
        select(PEPortfolioCompany)
    ).scalars().all()

    candidates = []
    for co in companies:
        latest_fin = db.execute(
            select(PECompanyFinancials)
            .where(PECompanyFinancials.company_id == co.id)
            .order_by(PECompanyFinancials.fiscal_year.desc())
            .limit(1)
        ).scalar_one_or_none()

        if latest_fin and latest_fin.revenue_usd:
            rev = float(latest_fin.revenue_usd)
            if rev >= REVENUE_MIN:
                candidates.append(co)

    return candidates


def _get_pipeline_company_ids(db: Session) -> set:
    """Get company_ids already in the active pipeline."""
    rows = db.execute(
        select(PEDeal.company_id).where(
            PEDeal.status.in_(["Screening", "DD", "LOI", "Closing"])
        )
    ).scalars().all()
    return {cid for cid in rows if cid is not None}


def _score_and_create_deals(
    db: Session,
    firm_id: int,
    firm_name: str,
    candidates: List[PEPortfolioCompany],
    existing_ids: set,
    report: DealSourcingReport,
    data_source: str,
    sector_signals: Optional[Dict] = None,
) -> None:
    """Score candidates and create pipeline entries for qualifying ones."""
    scored_candidates = []

    for co in candidates:
        # Skip duplicates
        if co.id in existing_ids:
            report.deals_skipped_duplicate += 1
            continue

        # Score
        try:
            result = score_deal(db, co.id)
            report.candidates_scored += 1
        except Exception as e:
            report.errors.append(f"Scoring failed for {co.name}: {e}")
            continue

        if result is None:
            continue

        # Check threshold
        if result.composite_score < SCORE_THRESHOLD:
            report.deals_skipped_low_score += 1
            continue

        scored_candidates.append((co, result))

    # Sort by score descending and create deals
    scored_candidates.sort(key=lambda x: x[1].composite_score, reverse=True)

    for co, score_result in scored_candidates:
        try:
            deal_data = {
                "company_id": co.id,
                "deal_name": f"{co.name} — Auto-Sourced",
                "deal_type": "LBO",
                "status": "Screening",
                "data_source": data_source,
                "buyer_name": firm_name,
                "announced_date": date.today(),
            }

            deal = PEDeal(**deal_data)
            db.add(deal)
            db.flush()

            report.deals_created += 1
            report.top_opportunities.append({
                "company_id": co.id,
                "company_name": co.name,
                "score": round(score_result.composite_score, 1),
                "grade": score_result.grade,
                "sector": co.industry,
                "deal_id": deal.id,
            })

            # Track in existing_ids to prevent duplicates within same run
            existing_ids.add(co.id)

        except Exception as e:
            report.errors.append(f"Failed to create deal for {co.name}: {e}")
            db.rollback()

    try:
        db.commit()
    except Exception as e:
        logger.error("Failed to commit sourced deals: %s", e)
        db.rollback()

    logger.info(
        "Deal sourcing for firm %d: %d candidates, %d scored, %d created, %d duplicates, %d low-score",
        firm_id,
        report.candidates_found,
        report.candidates_scored,
        report.deals_created,
        report.deals_skipped_duplicate,
        report.deals_skipped_low_score,
    )
