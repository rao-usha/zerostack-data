"""
Market Scanner API (T49)

Endpoints for market intelligence scanning, trend detection,
opportunity identification, and brief generation.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.agents.market_scanner import MarketScannerAgent

router = APIRouter(prefix="/market", tags=["Market Scanner"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================


class TriggerScanRequest(BaseModel):
    """Request to trigger a market scan."""

    scan_type: str = Field(
        default="manual", description="Type of scan: manual, scheduled"
    )


class ScanResponse(BaseModel):
    """Response from market scan."""

    scan_id: str
    status: str
    scan_timestamp: str
    signals: List[Dict[str, Any]]
    total_signals: int
    by_type: Dict[str, int]
    by_category: Dict[str, int]


class TrendsResponse(BaseModel):
    """Response for trend analysis."""

    period: str
    trends: List[Dict[str, Any]]
    total_trends: int
    analysis_timestamp: str


class OpportunitiesResponse(BaseModel):
    """Response for opportunities."""

    opportunities: List[Dict[str, Any]]
    total_found: int
    generated_at: str


class BriefResponse(BaseModel):
    """Response for market brief."""

    brief_id: str
    period: Dict[str, str]
    brief_type: str
    summary: str
    sections: Dict[str, Any]
    stats: Optional[Dict[str, int]] = None
    generated_at: str


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.get("/scan", response_model=Dict[str, Any])
def get_current_signals(
    limit: int = Query(default=50, le=100, description="Max signals to return"),
    db: Session = Depends(get_db),
):
    """
    Get current market signals.

    Returns cached signals from the most recent scan (1 hour TTL).
    If no recent scan exists, triggers a new scan.
    """
    agent = MarketScannerAgent(db)
    return agent.get_current_signals(limit=limit)


@router.post("/scan/trigger", response_model=Dict[str, Any])
def trigger_scan(request: TriggerScanRequest = None, db: Session = Depends(get_db)):
    """
    Manually trigger a market scan.

    Runs a full scan across all data sources and detects signals.
    """
    scan_type = request.scan_type if request else "manual"
    agent = MarketScannerAgent(db)
    return agent.run_scan(scan_type=scan_type)


@router.get("/trends", response_model=Dict[str, Any])
def get_trends(
    period: int = Query(default=30, le=90, description="Period in days"),
    db: Session = Depends(get_db),
):
    """
    Get emerging market trends.

    Analyzes signals over the specified period to identify trends
    with momentum, stage classification, and supporting evidence.
    """
    agent = MarketScannerAgent(db)
    return agent.get_trends(period_days=period)


@router.get("/opportunities", response_model=Dict[str, Any])
def get_opportunities(db: Session = Depends(get_db)):
    """
    Get spotted investment opportunities.

    Identifies opportunities from market signals including:
    - Sector rotations
    - Contrarian plays
    - Momentum opportunities
    """
    agent = MarketScannerAgent(db)
    return agent.get_opportunities()


@router.get("/brief", response_model=Dict[str, Any])
def get_brief(
    period_type: str = Query(
        default="weekly", description="Brief type: daily, weekly, monthly"
    ),
    db: Session = Depends(get_db),
):
    """
    Get market intelligence brief.

    Generates a comprehensive market brief with:
    - Executive summary
    - Top signals
    - Emerging patterns
    - Sector spotlight
    - Geographic shifts
    - Early warnings
    """
    if period_type not in ["daily", "weekly", "monthly"]:
        raise HTTPException(
            status_code=400, detail="Invalid period_type. Use: daily, weekly, monthly"
        )

    agent = MarketScannerAgent(db)
    return agent.generate_brief(period_type=period_type)


@router.get("/history", response_model=Dict[str, Any])
def get_history(
    limit: int = Query(default=20, le=100, description="Max records per type"),
    db: Session = Depends(get_db),
):
    """
    Get historical scans and briefs.

    Returns recent scan history and generated briefs.
    """
    agent = MarketScannerAgent(db)
    return agent.get_history(limit=limit)


@router.get("/stats", response_model=Dict[str, Any])
def get_stats(db: Session = Depends(get_db)):
    """
    Get market scanner statistics.

    Returns metrics about scans, signals, and briefs.
    """
    agent = MarketScannerAgent(db)
    return agent.get_stats()


@router.get("/signals/{signal_id}", response_model=Dict[str, Any])
def get_signal(signal_id: str, db: Session = Depends(get_db)):
    """
    Get details for a specific signal.
    """
    try:
        from sqlalchemy import text

        query = text("""
            SELECT signal_id, signal_type, category, direction, strength, confidence,
                   description, data_points, first_detected, last_updated, status
            FROM market_signals
            WHERE signal_id = :signal_id
        """)

        result = db.execute(query, {"signal_id": signal_id}).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Signal not found")

        return {
            "signal_id": result[0],
            "signal_type": result[1],
            "category": result[2],
            "direction": result[3],
            "strength": result[4],
            "confidence": result[5],
            "description": result[6],
            "data_points": result[7],
            "first_detected": result[8].isoformat() if result[8] else None,
            "last_updated": result[9].isoformat() if result[9] else None,
            "status": result[10],
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/signals", response_model=Dict[str, Any])
def list_signals(
    signal_type: Optional[str] = Query(
        default=None, description="Filter by signal type"
    ),
    category: Optional[str] = Query(default=None, description="Filter by category"),
    direction: Optional[str] = Query(default=None, description="Filter by direction"),
    min_strength: float = Query(default=0, description="Minimum strength"),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
    db: Session = Depends(get_db),
):
    """
    List market signals with filters.
    """
    try:
        from sqlalchemy import text

        conditions = ["status = 'active'"]
        params = {"limit": limit, "offset": offset}

        if signal_type:
            conditions.append("signal_type = :signal_type")
            params["signal_type"] = signal_type

        if category:
            conditions.append("category = :category")
            params["category"] = category

        if direction:
            conditions.append("direction = :direction")
            params["direction"] = direction

        if min_strength > 0:
            conditions.append("strength >= :min_strength")
            params["min_strength"] = min_strength

        where_clause = " AND ".join(conditions)

        query = text(f"""
            SELECT signal_id, signal_type, category, direction, strength, confidence, description
            FROM market_signals
            WHERE {where_clause}
            ORDER BY strength DESC, last_updated DESC
            LIMIT :limit OFFSET :offset
        """)

        result = db.execute(query, params).fetchall()

        count_query = text(f"""
            SELECT COUNT(*) FROM market_signals WHERE {where_clause}
        """)
        total = db.execute(count_query, params).scalar()

        signals = [
            {
                "signal_id": r[0],
                "signal_type": r[1],
                "category": r[2],
                "direction": r[3],
                "strength": r[4],
                "confidence": r[5],
                "description": r[6],
            }
            for r in result
        ]

        return {"signals": signals, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
