"""
Prediction Market Intelligence API Endpoints.

Provides REST API for:
- Monitoring prediction markets (Kalshi, Polymarket)
- Querying market data and probabilities
- Managing alerts and notifications
- Dashboard data and analytics
"""
import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Optional, List
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.models import PredictionMarket, MarketObservation, MarketAlert, PredictionMarketJob
from app.sources.prediction_markets.ingest import (
    monitor_all_platforms,
    monitor_kalshi,
    monitor_polymarket,
    get_job_status,
    get_top_markets,
    get_market_history,
    get_active_alerts,
    get_dashboard_data,
    get_top_probability_movers,
)
from app.sources.prediction_markets.metadata import MARKET_CATEGORIES, get_high_priority_categories

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/prediction-markets", tags=["Prediction Markets"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class MonitorRequest(BaseModel):
    """Request to monitor prediction markets."""
    kalshi_categories: Optional[List[str]] = Field(
        default=None,
        description="Kalshi series to monitor (e.g., ['FED', 'CPI', 'PRES']). If None, monitors all high-priority series."
    )
    limit_per_platform: int = Field(
        default=50,
        ge=10,
        le=200,
        description="Maximum markets to fetch per platform"
    )


class MonitorResponse(BaseModel):
    """Response from monitoring job."""
    job_id: int
    status: str
    kalshi: dict
    polymarket: dict
    totals: dict
    errors: list


class MarketResponse(BaseModel):
    """Single market data."""
    id: int
    source: str
    question: str
    category: Optional[str]
    subcategory: Optional[str]
    yes_probability: Optional[float]
    volume_usd: Optional[float]
    close_date: Optional[str]
    market_url: Optional[str]
    last_updated: Optional[str]


class AlertResponse(BaseModel):
    """Market alert data."""
    id: int
    market_id: int
    market_question: Optional[str]
    market_source: Optional[str]
    alert_type: str
    alert_severity: str
    triggered_at: str
    probability_before: Optional[float]
    probability_after: Optional[float]
    probability_change: Optional[float]
    alert_message: Optional[str]


class DashboardResponse(BaseModel):
    """Dashboard summary data."""
    market_counts: dict
    total_markets: int
    alerts_24h: int
    unacknowledged_alerts: int
    top_movers: list
    high_priority_markets: list
    last_updated: str


# =============================================================================
# MONITORING ENDPOINTS
# =============================================================================

@router.post("/monitor/all", response_model=MonitorResponse)
async def trigger_monitor_all(
    request: MonitorRequest = MonitorRequest(),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """
    Monitor all prediction market platforms (Kalshi + Polymarket).
    
    This endpoint:
    1. Fetches current market data from all platforms
    2. Stores observations in the database
    3. Detects significant probability changes
    4. Generates alerts for large movements
    
    **Categories for Kalshi:**
    - Economics: FED, CPI, UNRATE, GDP
    - Politics: PRES, SENATE, HOUSE
    - Crypto: BTCUSD, ETHUSD
    - Climate: CLIMATE
    
    **Returns:** Job status with counts of markets checked, updated, and alerts generated.
    """
    try:
        results = await monitor_all_platforms(
            db=db,
            kalshi_categories=request.kalshi_categories,
            limit_per_platform=request.limit_per_platform,
        )
        return MonitorResponse(**results)
    except Exception as e:
        logger.error(f"Error in monitor_all: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/monitor/kalshi")
async def trigger_monitor_kalshi(
    categories: Optional[List[str]] = Query(
        default=None,
        description="Kalshi series to monitor (e.g., FED, CPI, PRES)"
    ),
    limit: int = Query(default=50, ge=10, le=200),
    db: Session = Depends(get_db),
):
    """
    Monitor Kalshi markets only.
    
    **Available Categories (series):**
    - FED: Federal Reserve rate decisions
    - CPI: Consumer Price Index / Inflation
    - UNRATE: Unemployment rate
    - GDP: GDP growth
    - INXD: Stock market indices
    - PRES: Presidential election
    - SENATE/HOUSE: Congressional control
    - BTCUSD/ETHUSD: Crypto prices
    """
    try:
        results = await monitor_kalshi(db, categories, limit)
        return {
            "platform": "kalshi",
            "results": results,
        }
    except Exception as e:
        logger.error(f"Error in monitor_kalshi: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/monitor/polymarket")
async def trigger_monitor_polymarket(
    limit: int = Query(default=50, ge=10, le=200),
    db: Session = Depends(get_db),
):
    """
    Monitor Polymarket markets only.
    
    Polymarket covers:
    - Politics (US and international)
    - Economics (Fed, recession, inflation)
    - Sports (NFL, NBA, etc.)
    - Crypto (Bitcoin, Ethereum)
    - Business (earnings, M&A)
    - World events (geopolitics)
    """
    try:
        results = await monitor_polymarket(db, limit)
        return {
            "platform": "polymarket",
            "results": results,
        }
    except Exception as e:
        logger.error(f"Error in monitor_polymarket: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# QUERY ENDPOINTS
# =============================================================================

@router.get("/markets/top", response_model=List[MarketResponse])
async def get_top_prediction_markets(
    platform: Optional[str] = Query(default=None, description="Filter by platform: kalshi, polymarket"),
    category: Optional[str] = Query(default=None, description="Filter by category: economics, politics, sports, crypto"),
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get top prediction markets by volume/activity.
    
    **Categories:**
    - economics: Fed rates, inflation, recession, unemployment
    - politics: Elections, legislation, cabinet
    - sports: NFL, NBA, MLB
    - crypto: Bitcoin, Ethereum
    - world: Geopolitics, international leaders
    - business: Earnings, M&A
    """
    try:
        markets = get_top_markets(db, platform, category, limit)
        return markets
    except Exception as e:
        logger.error(f"Error getting top markets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/markets/category/{category}")
async def get_markets_by_category(
    category: str,
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get all markets in a specific category.
    
    **Available categories:**
    - economics, politics, sports, crypto, world, business, entertainment, environment
    """
    try:
        markets = get_top_markets(db, category=category, limit=limit)
        return {
            "category": category,
            "count": len(markets),
            "markets": markets,
        }
    except Exception as e:
        logger.error(f"Error getting markets by category: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/markets/{market_id}")
async def get_market_details(
    market_id: int,
    db: Session = Depends(get_db),
):
    """
    Get detailed information about a specific market.
    """
    market = db.query(PredictionMarket).filter(PredictionMarket.id == market_id).first()
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    
    # Get latest observation
    latest_obs = db.query(MarketObservation).filter(
        MarketObservation.market_id == market_id
    ).order_by(MarketObservation.observation_timestamp.desc()).first()
    
    return {
        "id": market.id,
        "source": market.source,
        "market_id": market.market_id,
        "question": market.question,
        "description": market.description,
        "category": market.category,
        "subcategory": market.subcategory,
        "close_date": market.close_date.isoformat() if market.close_date else None,
        "is_active": market.is_active == 1,
        "market_url": market.market_url,
        "current_probability": float(market.last_yes_probability) if market.last_yes_probability else None,
        "volume_usd": float(market.last_volume_usd) if market.last_volume_usd else None,
        "last_updated": market.last_updated.isoformat() if market.last_updated else None,
        "first_observed": market.first_observed.isoformat() if market.first_observed else None,
        "latest_observation": {
            "timestamp": latest_obs.observation_timestamp.isoformat() if latest_obs else None,
            "probability": float(latest_obs.yes_probability) if latest_obs and latest_obs.yes_probability else None,
            "volume_24h": float(latest_obs.volume_24h_usd) if latest_obs and latest_obs.volume_24h_usd else None,
            "change_24h": float(latest_obs.probability_change_24h) if latest_obs and latest_obs.probability_change_24h else None,
        } if latest_obs else None,
    }


@router.get("/markets/{market_id}/history")
async def get_market_probability_history(
    market_id: int,
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """
    Get probability history for a market (time series).
    
    Returns observations over the specified number of days,
    useful for charting probability trends.
    """
    # Verify market exists
    market = db.query(PredictionMarket).filter(PredictionMarket.id == market_id).first()
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    
    try:
        history = get_market_history(db, market_id, days)
        return {
            "market_id": market_id,
            "question": market.question,
            "days": days,
            "observation_count": len(history),
            "history": history,
        }
    except Exception as e:
        logger.error(f"Error getting market history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ALERT ENDPOINTS
# =============================================================================

@router.get("/alerts", response_model=List[AlertResponse])
async def get_alerts(
    severity: Optional[str] = Query(
        default=None,
        description="Filter by severity: critical, high, medium, low"
    ),
    acknowledged: bool = Query(default=False, description="Show acknowledged alerts"),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Get recent market alerts.
    
    Alerts are generated when market probabilities change significantly:
    - Critical: >20% change
    - High: >15% change
    - Medium: >10% change
    - Low: >5% change (for high-priority markets)
    """
    try:
        alerts = get_active_alerts(db, severity, acknowledged, limit)
        return alerts
    except Exception as e:
        logger.error(f"Error getting alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: int,
    acknowledged_by: str = Query(default="api_user"),
    db: Session = Depends(get_db),
):
    """
    Mark an alert as acknowledged.
    """
    alert = db.query(MarketAlert).filter(MarketAlert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    alert.is_acknowledged = 1
    alert.acknowledged_at = datetime.utcnow()
    alert.acknowledged_by = acknowledged_by
    db.commit()
    
    return {"status": "acknowledged", "alert_id": alert_id}


# =============================================================================
# DASHBOARD & ANALYTICS
# =============================================================================

@router.get("/dashboard", response_model=DashboardResponse)
async def get_prediction_market_dashboard(
    db: Session = Depends(get_db),
):
    """
    Get dashboard summary data.
    
    Returns:
    - Market counts by platform
    - Recent alerts (24h)
    - Top probability movers
    - High-priority markets (economics, politics)
    """
    try:
        dashboard = get_dashboard_data(db)
        return dashboard
    except Exception as e:
        logger.error(f"Error getting dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/movers")
async def get_probability_movers(
    hours: int = Query(default=24, ge=1, le=168),
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """
    Get markets with largest probability changes.
    
    Useful for identifying significant market movements
    and potential trading signals.
    """
    try:
        movers = get_top_probability_movers(db, hours, limit)
        return {
            "time_period_hours": hours,
            "count": len(movers),
            "movers": movers,
        }
    except Exception as e:
        logger.error(f"Error getting movers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# JOB STATUS
# =============================================================================

@router.get("/jobs/{job_id}")
async def get_monitoring_job_status(
    job_id: int,
    db: Session = Depends(get_db),
):
    """
    Get status of a prediction market monitoring job.
    """
    job_status = get_job_status(db, job_id)
    if not job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_status


@router.get("/jobs")
async def list_recent_jobs(
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    List recent prediction market monitoring jobs.
    """
    jobs = db.query(PredictionMarketJob).order_by(
        PredictionMarketJob.created_at.desc()
    ).limit(limit).all()
    
    return [
        {
            "id": j.id,
            "job_type": j.job_type,
            "status": j.status,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            "markets_checked": j.markets_checked,
            "markets_updated": j.markets_updated,
            "alerts_generated": j.alerts_generated,
        }
        for j in jobs
    ]


# =============================================================================
# METADATA
# =============================================================================

@router.get("/categories")
async def list_market_categories():
    """
    List all supported market categories and their configuration.
    
    Shows:
    - Category name and display name
    - Keywords used for classification
    - Alert thresholds
    - Monitoring priority
    """
    return {
        "categories": {
            name: {
                "display_name": config["display_name"],
                "parent": config["parent"],
                "impact_level": config["impact_level"],
                "monitoring_priority": config["monitoring_priority"],
                "alert_threshold": config["alert_threshold"],
            }
            for name, config in MARKET_CATEGORIES.items()
        },
        "high_priority": get_high_priority_categories(),
    }


@router.get("/platforms")
async def list_supported_platforms():
    """
    List supported prediction market platforms.
    """
    return {
        "platforms": [
            {
                "name": "kalshi",
                "display_name": "Kalshi",
                "url": "https://kalshi.com",
                "focus": "US economic indicators, political events, crypto",
                "regulated": True,
                "regulator": "CFTC",
                "api_auth_required": False,
            },
            {
                "name": "polymarket",
                "display_name": "Polymarket",
                "url": "https://polymarket.com",
                "focus": "Global events, politics, sports, crypto, business",
                "regulated": False,
                "crypto_based": True,
                "api_auth_required": False,
            },
        ]
    }
