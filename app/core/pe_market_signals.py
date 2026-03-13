"""
PE Market Signals Service.

Persists and retrieves market scanner results. Enables scheduled scanning,
historical trend tracking, and high-momentum sector identification for
automated deal sourcing.
"""

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select, func as sa_func, desc
from sqlalchemy.orm import Session

from app.core.pe_models import PEMarketSignal

logger = logging.getLogger(__name__)


def store_signals(db: Session, signals: List[Dict[str, Any]], batch_id: Optional[str] = None) -> int:
    """
    Persist market scanner output to pe_market_signals table.

    Args:
        db: Database session
        signals: List of signal dicts from MarketScannerService.get_market_signals()
        batch_id: Optional batch identifier (auto-generated if not provided)

    Returns:
        Number of signals stored.
    """
    if not signals:
        return 0

    if batch_id is None:
        batch_id = f"scan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    count = 0
    for sig in signals:
        try:
            record = PEMarketSignal(
                sector=sig.get("industry", "Unknown"),
                momentum_score=sig.get("momentum_score", 0),
                deal_count=sig.get("recent_deal_count", 0),
                avg_multiple=sig.get("current_median_ev_ebitda"),
                signal_type=sig.get("momentum", "neutral"),
                top_companies=sig.get("top_companies"),
                deal_flow_change_pct=sig.get("deal_flow_change_pct"),
                multiple_change_pct=sig.get("multiple_change_pct"),
                batch_id=batch_id,
            )
            db.add(record)
            count += 1
        except Exception as e:
            logger.warning("Failed to store signal for %s: %s", sig.get("industry"), e)

    try:
        db.commit()
        logger.info("Stored %d market signals (batch=%s)", count, batch_id)
    except Exception as e:
        logger.error("Failed to commit market signals: %s", e)
        db.rollback()
        return 0

    return count


def get_latest_signals(db: Session) -> List[Dict[str, Any]]:
    """
    Get the most recent scan results per sector.

    Returns signals from the latest batch_id.
    """
    # Find latest batch_id
    latest_batch = db.execute(
        select(PEMarketSignal.batch_id)
        .order_by(desc(PEMarketSignal.scanned_at))
        .limit(1)
    ).scalar()

    if not latest_batch:
        return []

    rows = (
        db.query(PEMarketSignal)
        .filter(PEMarketSignal.batch_id == latest_batch)
        .order_by(desc(PEMarketSignal.momentum_score))
        .all()
    )

    return [_signal_to_dict(r) for r in rows]


def get_high_momentum_sectors(db: Session, threshold: int = 60) -> List[Dict[str, Any]]:
    """
    Get sectors with momentum score above threshold from latest scan.

    Args:
        db: Database session
        threshold: Minimum momentum score (default 60)

    Returns:
        List of high-momentum signal dicts sorted by score descending.
    """
    # Find latest batch
    latest_batch = db.execute(
        select(PEMarketSignal.batch_id)
        .order_by(desc(PEMarketSignal.scanned_at))
        .limit(1)
    ).scalar()

    if not latest_batch:
        return []

    rows = (
        db.query(PEMarketSignal)
        .filter(
            PEMarketSignal.batch_id == latest_batch,
            PEMarketSignal.momentum_score >= threshold,
        )
        .order_by(desc(PEMarketSignal.momentum_score))
        .all()
    )

    return [_signal_to_dict(r) for r in rows]


def run_market_scan(db: Session) -> Dict[str, Any]:
    """
    Orchestrate a full market scan: scan all sectors, store results, fire webhooks.

    Returns summary with signals stored and high-momentum sectors.
    """
    from app.core.pe_market_scanner import MarketScannerService

    scanner = MarketScannerService(db)

    # Run the scan
    try:
        signals = scanner.get_market_signals()
    except Exception as e:
        logger.error("Market scan failed: %s", e)
        return {"status": "failed", "error": str(e), "signals_stored": 0}

    if not signals:
        logger.info("Market scan returned no signals")
        return {"status": "complete", "signals_stored": 0, "high_momentum": []}

    # Store results
    batch_id = f"scan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    count = store_signals(db, signals, batch_id=batch_id)

    # Identify high-momentum sectors
    high_momentum = [s for s in signals if s.get("momentum_score", 0) > 75]

    # Fire webhooks for high-momentum opportunities
    if high_momentum:
        _fire_opportunity_webhooks(high_momentum)

    return {
        "status": "complete",
        "signals_stored": count,
        "batch_id": batch_id,
        "total_sectors": len(signals),
        "high_momentum": [
            {"sector": s.get("industry"), "momentum_score": s.get("momentum_score")}
            for s in high_momentum
        ],
    }


def _fire_opportunity_webhooks(high_momentum_signals: List[Dict]) -> None:
    """Fire PE_NEW_MARKET_OPPORTUNITY webhook for high-momentum sectors."""
    try:
        import asyncio
        from app.core.webhook_service import trigger_webhooks

        for sig in high_momentum_signals:
            try:
                asyncio.get_event_loop().run_until_complete(
                    trigger_webhooks(
                        "pe_new_market_opportunity",
                        {
                            "sector": sig.get("industry"),
                            "momentum_score": sig.get("momentum_score"),
                            "deal_count": sig.get("recent_deal_count"),
                            "signal_type": sig.get("momentum"),
                        },
                    )
                )
            except RuntimeError:
                # No event loop — log and continue
                logger.info(
                    "Market opportunity: %s (momentum=%s)",
                    sig.get("industry"),
                    sig.get("momentum_score"),
                )
    except ImportError:
        logger.warning("Webhook service not available")


def _signal_to_dict(row: PEMarketSignal) -> Dict[str, Any]:
    """Convert a PEMarketSignal row to a dict."""
    return {
        "id": row.id,
        "sector": row.sector,
        "momentum_score": row.momentum_score,
        "deal_count": row.deal_count,
        "avg_multiple": float(row.avg_multiple) if row.avg_multiple else None,
        "signal_type": row.signal_type,
        "top_companies": row.top_companies,
        "deal_flow_change_pct": float(row.deal_flow_change_pct) if row.deal_flow_change_pct else None,
        "multiple_change_pct": float(row.multiple_change_pct) if row.multiple_change_pct else None,
        "batch_id": row.batch_id,
        "scanned_at": row.scanned_at.isoformat() if row.scanned_at else None,
    }
