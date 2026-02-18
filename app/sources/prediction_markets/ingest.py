"""
Prediction Market Ingestion and Monitoring.

Orchestrates data collection from multiple prediction market platforms,
stores observations, detects significant changes, and generates alerts.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.models import (
    PredictionMarket,
    MarketObservation,
    MarketAlert,
    PredictionMarketJob,
)
from app.sources.prediction_markets.client import (
    KalshiClient,
    PolymarketClient,
    MarketData,
    get_kalshi_client,
    get_polymarket_client,
)
from app.sources.prediction_markets.metadata import (
    categorize_market,
    get_alert_threshold,
    KALSHI_SERIES,
)

logger = logging.getLogger(__name__)


# =============================================================================
# JOB MANAGEMENT
# =============================================================================


def create_job(
    db: Session,
    job_type: str,
    target_platforms: Optional[List[str]] = None,
    target_categories: Optional[List[str]] = None,
) -> PredictionMarketJob:
    """Create a new prediction market monitoring job."""
    job = PredictionMarketJob(
        job_type=job_type,
        target_platforms=target_platforms,
        target_categories=target_categories,
        status="pending",
        created_at=datetime.utcnow(),
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def start_job(db: Session, job: PredictionMarketJob) -> None:
    """Mark job as running."""
    job.status = "running"
    job.started_at = datetime.utcnow()
    db.commit()


def complete_job(
    db: Session,
    job: PredictionMarketJob,
    success: bool = True,
    markets_checked: int = 0,
    markets_updated: int = 0,
    new_markets_found: int = 0,
    observations_stored: int = 0,
    alerts_generated: int = 0,
    errors: Optional[List[Dict]] = None,
) -> None:
    """Mark job as completed."""
    job.status = "success" if success else "failed"
    job.completed_at = datetime.utcnow()
    job.markets_checked = markets_checked
    job.markets_updated = markets_updated
    job.new_markets_found = new_markets_found
    job.observations_stored = observations_stored
    job.alerts_generated = alerts_generated
    job.errors = errors
    db.commit()


def get_job_status(db: Session, job_id: int) -> Optional[Dict]:
    """Get job status and results."""
    job = db.query(PredictionMarketJob).filter(PredictionMarketJob.id == job_id).first()
    if not job:
        return None

    return {
        "id": job.id,
        "job_type": job.job_type,
        "status": job.status,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "markets_checked": job.markets_checked,
        "markets_updated": job.markets_updated,
        "new_markets_found": job.new_markets_found,
        "observations_stored": job.observations_stored,
        "alerts_generated": job.alerts_generated,
        "errors": job.errors,
    }


# =============================================================================
# MARKET STORAGE
# =============================================================================


def upsert_market(
    db: Session, market_data: MarketData
) -> Tuple[PredictionMarket, bool]:
    """
    Insert or update a prediction market.

    Returns:
        Tuple of (market, is_new)
    """
    existing = (
        db.query(PredictionMarket)
        .filter(
            PredictionMarket.source == market_data.source,
            PredictionMarket.market_id == market_data.market_id,
        )
        .first()
    )

    if existing:
        # Update existing market
        existing.question = market_data.question
        existing.description = market_data.description
        existing.category = market_data.category
        existing.subcategory = market_data.subcategory
        existing.close_date = market_data.close_date
        existing.market_url = market_data.market_url
        existing.last_yes_probability = str(market_data.yes_probability)
        existing.last_volume_usd = (
            str(market_data.volume_usd) if market_data.volume_usd else None
        )
        existing.last_updated = datetime.utcnow()
        existing.is_active = 1
        db.commit()
        return existing, False
    else:
        # Create new market
        market = PredictionMarket(
            source=market_data.source,
            market_id=market_data.market_id,
            question=market_data.question,
            description=market_data.description,
            category=market_data.category,
            subcategory=market_data.subcategory,
            outcome_type="binary",
            close_date=market_data.close_date,
            market_url=market_data.market_url,
            is_active=1,
            last_yes_probability=str(market_data.yes_probability),
            last_volume_usd=str(market_data.volume_usd)
            if market_data.volume_usd
            else None,
            last_updated=datetime.utcnow(),
            first_observed=datetime.utcnow(),
        )
        db.add(market)
        db.commit()
        db.refresh(market)
        return market, True


def store_observation(
    db: Session, market: PredictionMarket, market_data: MarketData
) -> MarketObservation:
    """Store a new market observation."""
    # Get previous observation for calculating changes
    prev_obs = (
        db.query(MarketObservation)
        .filter(MarketObservation.market_id == market.id)
        .order_by(MarketObservation.observation_timestamp.desc())
        .first()
    )

    # Calculate probability changes
    prob_change_24h = None
    if prev_obs:
        try:
            prev_prob = float(prev_obs.yes_probability)
            curr_prob = market_data.yes_probability
            # Only calculate if previous observation was within 24-48 hours
            time_diff = datetime.utcnow() - prev_obs.observation_timestamp
            if time_diff <= timedelta(hours=48):
                prob_change_24h = str(round(curr_prob - prev_prob, 4))
        except (ValueError, TypeError):
            pass

    observation = MarketObservation(
        market_id=market.id,
        observation_timestamp=datetime.utcnow(),
        yes_probability=str(market_data.yes_probability),
        no_probability=str(market_data.no_probability)
        if market_data.no_probability
        else None,
        volume_usd=str(market_data.volume_usd) if market_data.volume_usd else None,
        volume_24h_usd=str(market_data.volume_24h_usd)
        if market_data.volume_24h_usd
        else None,
        liquidity_usd=str(market_data.liquidity_usd)
        if market_data.liquidity_usd
        else None,
        probability_change_24h=prob_change_24h,
        data_source="api",
    )
    db.add(observation)
    db.commit()
    return observation


# =============================================================================
# ALERT GENERATION
# =============================================================================


def check_and_generate_alerts(
    db: Session,
    market: PredictionMarket,
    market_data: MarketData,
) -> Optional[MarketAlert]:
    """
    Check if market probability change exceeds threshold and generate alert.

    Returns:
        MarketAlert if generated, None otherwise
    """
    # Get recent observation for comparison
    recent_obs = (
        db.query(MarketObservation)
        .filter(
            MarketObservation.market_id == market.id,
            MarketObservation.observation_timestamp
            >= datetime.utcnow() - timedelta(hours=24),
        )
        .order_by(MarketObservation.observation_timestamp.asc())
        .first()
    )

    if not recent_obs:
        return None

    try:
        old_prob = float(recent_obs.yes_probability)
        new_prob = market_data.yes_probability
        change = abs(new_prob - old_prob)

        # Get threshold for this category
        threshold = get_alert_threshold(market.category, market.subcategory)

        if change >= threshold:
            # Determine alert type and severity
            if new_prob > old_prob:
                alert_type = "probability_spike"
            else:
                alert_type = "probability_drop"

            # Severity based on change magnitude
            if change >= 0.20:
                severity = "critical"
            elif change >= 0.15:
                severity = "high"
            elif change >= 0.10:
                severity = "medium"
            else:
                severity = "low"

            # Build alert message
            direction = "increased" if new_prob > old_prob else "decreased"
            alert_message = (
                f"{market.question[:100]} - Probability {direction} from "
                f"{old_prob:.1%} to {new_prob:.1%} ({change:+.1%}) in last 24h"
            )

            alert = MarketAlert(
                market_id=market.id,
                alert_type=alert_type,
                alert_severity=severity,
                triggered_at=datetime.utcnow(),
                probability_before=str(old_prob),
                probability_after=str(new_prob),
                probability_change=str(round(new_prob - old_prob, 4)),
                time_period="24h",
                alert_message=alert_message,
            )
            db.add(alert)
            db.commit()

            logger.info(
                f"Generated {severity} alert for market {market.id}: {alert_message}"
            )
            return alert

    except Exception as e:
        logger.error(f"Error checking alerts for market {market.id}: {e}")

    return None


# =============================================================================
# MONITORING FUNCTIONS
# =============================================================================


async def monitor_kalshi(
    db: Session,
    categories: Optional[List[str]] = None,
    limit: int = 100,
) -> Dict[str, int]:
    """
    Monitor Kalshi markets and store observations.

    Args:
        db: Database session
        categories: Kalshi series to fetch (e.g., ['FED', 'CPI', 'PRES'])
                   If None, fetches high-priority series + general markets
        limit: Max markets per category

    Returns:
        Dict with counts (markets_checked, new_markets, observations_stored, alerts_generated)
    """
    client = get_kalshi_client()

    # Default to high-priority + sports categories
    if categories is None:
        categories = list(KALSHI_SERIES.keys())[:10]  # Top 10 series

    results = {
        "markets_checked": 0,
        "new_markets": 0,
        "observations_stored": 0,
        "alerts_generated": 0,
    }

    try:
        # Fetch markets
        markets = await client.fetch_top_markets(categories=categories, limit=limit)
        results["markets_checked"] = len(markets)

        for market_data in markets:
            try:
                # Upsert market
                market, is_new = upsert_market(db, market_data)
                if is_new:
                    results["new_markets"] += 1

                # Store observation
                store_observation(db, market, market_data)
                results["observations_stored"] += 1

                # Check for alerts
                alert = check_and_generate_alerts(db, market, market_data)
                if alert:
                    results["alerts_generated"] += 1

            except Exception as e:
                logger.error(
                    f"Error processing Kalshi market {market_data.market_id}: {e}"
                )

        logger.info(f"Kalshi monitoring complete: {results}")

    except Exception as e:
        logger.error(f"Error monitoring Kalshi: {e}")
        raise

    return results


async def monitor_polymarket(
    db: Session,
    limit: int = 100,
) -> Dict[str, int]:
    """
    Monitor Polymarket markets and store observations.

    Args:
        db: Database session
        limit: Max markets to fetch

    Returns:
        Dict with counts (markets_checked, new_markets, observations_stored, alerts_generated)
    """
    client = get_polymarket_client()

    results = {
        "markets_checked": 0,
        "new_markets": 0,
        "observations_stored": 0,
        "alerts_generated": 0,
    }

    try:
        # Fetch markets
        markets = await client.fetch_top_markets(limit=limit, include_events=True)
        results["markets_checked"] = len(markets)

        for market_data in markets:
            try:
                # Upsert market
                market, is_new = upsert_market(db, market_data)
                if is_new:
                    results["new_markets"] += 1

                # Store observation
                store_observation(db, market, market_data)
                results["observations_stored"] += 1

                # Check for alerts
                alert = check_and_generate_alerts(db, market, market_data)
                if alert:
                    results["alerts_generated"] += 1

            except Exception as e:
                logger.error(
                    f"Error processing Polymarket market {market_data.market_id}: {e}"
                )

        logger.info(f"Polymarket monitoring complete: {results}")

    except Exception as e:
        logger.error(f"Error monitoring Polymarket: {e}")
        raise

    return results


async def monitor_all_platforms(
    db: Session,
    kalshi_categories: Optional[List[str]] = None,
    limit_per_platform: int = 50,
) -> Dict[str, Any]:
    """
    Monitor all prediction market platforms.

    Args:
        db: Database session
        kalshi_categories: Optional Kalshi series to fetch
        limit_per_platform: Max markets per platform

    Returns:
        Dict with results per platform and totals
    """
    # Create job
    job = create_job(
        db,
        job_type="monitor_all",
        target_platforms=["kalshi", "polymarket"],
        target_categories=kalshi_categories,
    )
    start_job(db, job)

    results = {
        "job_id": job.id,
        "kalshi": {},
        "polymarket": {},
        "totals": {
            "markets_checked": 0,
            "new_markets": 0,
            "observations_stored": 0,
            "alerts_generated": 0,
        },
        "errors": [],
    }

    try:
        # Monitor Kalshi
        try:
            kalshi_results = await monitor_kalshi(
                db, kalshi_categories, limit_per_platform
            )
            results["kalshi"] = kalshi_results
            for key in results["totals"]:
                results["totals"][key] += kalshi_results.get(key, 0)
        except Exception as e:
            logger.error(f"Kalshi monitoring failed: {e}")
            results["errors"].append({"platform": "kalshi", "error": str(e)})

        # Small delay between platforms
        await asyncio.sleep(1)

        # Monitor Polymarket
        try:
            polymarket_results = await monitor_polymarket(db, limit_per_platform)
            results["polymarket"] = polymarket_results
            for key in results["totals"]:
                results["totals"][key] += polymarket_results.get(key, 0)
        except Exception as e:
            logger.error(f"Polymarket monitoring failed: {e}")
            results["errors"].append({"platform": "polymarket", "error": str(e)})

        # Complete job
        success = len(results["errors"]) == 0
        complete_job(
            db,
            job,
            success=success,
            markets_checked=results["totals"]["markets_checked"],
            markets_updated=results["totals"]["markets_checked"]
            - results["totals"]["new_markets"],
            new_markets_found=results["totals"]["new_markets"],
            observations_stored=results["totals"]["observations_stored"],
            alerts_generated=results["totals"]["alerts_generated"],
            errors=results["errors"] if results["errors"] else None,
        )

        logger.info(f"All platforms monitoring complete: {results['totals']}")

    except Exception as e:
        logger.error(f"Error in monitor_all_platforms: {e}")
        complete_job(db, job, success=False, errors=[{"error": str(e)}])
        raise

    return results


# =============================================================================
# QUERY FUNCTIONS
# =============================================================================


def get_top_markets(
    db: Session,
    platform: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 20,
) -> List[Dict]:
    """Get top markets by volume with current probabilities."""
    query = db.query(PredictionMarket).filter(PredictionMarket.is_active == 1)

    if platform:
        query = query.filter(PredictionMarket.source == platform)
    if category:
        query = query.filter(PredictionMarket.category == category)

    # Order by volume (as numeric)
    markets = query.order_by(PredictionMarket.last_updated.desc()).limit(limit).all()

    return [
        {
            "id": m.id,
            "source": m.source,
            "question": m.question,
            "category": m.category,
            "subcategory": m.subcategory,
            "yes_probability": float(m.last_yes_probability)
            if m.last_yes_probability
            else None,
            "volume_usd": float(m.last_volume_usd) if m.last_volume_usd else None,
            "close_date": m.close_date.isoformat() if m.close_date else None,
            "market_url": m.market_url,
            "last_updated": m.last_updated.isoformat() if m.last_updated else None,
        }
        for m in markets
    ]


def get_market_history(
    db: Session,
    market_id: int,
    days: int = 30,
) -> List[Dict]:
    """Get probability history for a market."""
    since = datetime.utcnow() - timedelta(days=days)

    observations = (
        db.query(MarketObservation)
        .filter(
            MarketObservation.market_id == market_id,
            MarketObservation.observation_timestamp >= since,
        )
        .order_by(MarketObservation.observation_timestamp.asc())
        .all()
    )

    return [
        {
            "timestamp": o.observation_timestamp.isoformat(),
            "yes_probability": float(o.yes_probability) if o.yes_probability else None,
            "volume_usd": float(o.volume_usd) if o.volume_usd else None,
            "probability_change_24h": float(o.probability_change_24h)
            if o.probability_change_24h
            else None,
        }
        for o in observations
    ]


def get_active_alerts(
    db: Session,
    severity: Optional[str] = None,
    acknowledged: bool = False,
    limit: int = 50,
) -> List[Dict]:
    """Get recent alerts."""
    query = db.query(MarketAlert).filter(
        MarketAlert.is_acknowledged == (1 if acknowledged else 0)
    )

    if severity:
        query = query.filter(MarketAlert.alert_severity == severity)

    alerts = query.order_by(MarketAlert.triggered_at.desc()).limit(limit).all()

    # Get market info for each alert
    result = []
    for a in alerts:
        market = (
            db.query(PredictionMarket)
            .filter(PredictionMarket.id == a.market_id)
            .first()
        )
        result.append(
            {
                "id": a.id,
                "market_id": a.market_id,
                "market_question": market.question if market else None,
                "market_source": market.source if market else None,
                "alert_type": a.alert_type,
                "alert_severity": a.alert_severity,
                "triggered_at": a.triggered_at.isoformat(),
                "probability_before": float(a.probability_before)
                if a.probability_before
                else None,
                "probability_after": float(a.probability_after)
                if a.probability_after
                else None,
                "probability_change": float(a.probability_change)
                if a.probability_change
                else None,
                "alert_message": a.alert_message,
            }
        )

    return result


def get_dashboard_data(db: Session) -> Dict:
    """Get summary data for dashboard."""
    # Total markets by platform
    market_counts = {}
    for platform in ["kalshi", "polymarket"]:
        count = (
            db.query(PredictionMarket)
            .filter(
                PredictionMarket.source == platform,
                PredictionMarket.is_active == 1,
            )
            .count()
        )
        market_counts[platform] = count

    # Recent alerts (last 24h)
    recent_alerts = (
        db.query(MarketAlert)
        .filter(MarketAlert.triggered_at >= datetime.utcnow() - timedelta(hours=24))
        .count()
    )

    # Unacknowledged alerts
    unacked_alerts = (
        db.query(MarketAlert).filter(MarketAlert.is_acknowledged == 0).count()
    )

    # Top probability movers (24h)
    top_movers = get_top_probability_movers(db, hours=24, limit=10)

    # High-priority markets (economics, politics)
    high_priority = get_top_markets(db, category="economics", limit=5)
    high_priority.extend(get_top_markets(db, category="politics", limit=5))

    return {
        "market_counts": market_counts,
        "total_markets": sum(market_counts.values()),
        "alerts_24h": recent_alerts,
        "unacknowledged_alerts": unacked_alerts,
        "top_movers": top_movers,
        "high_priority_markets": high_priority[:10],
        "last_updated": datetime.utcnow().isoformat(),
    }


def get_top_probability_movers(
    db: Session,
    hours: int = 24,
    limit: int = 10,
) -> List[Dict]:
    """Get markets with largest probability changes in the last N hours."""
    since = datetime.utcnow() - timedelta(hours=hours)

    # Get latest observation per market with change data
    observations = (
        db.query(MarketObservation)
        .filter(
            MarketObservation.observation_timestamp >= since,
            MarketObservation.probability_change_24h.isnot(None),
        )
        .order_by(MarketObservation.observation_timestamp.desc())
        .all()
    )

    # Deduplicate by market_id, keeping latest
    seen_markets = {}
    for obs in observations:
        if obs.market_id not in seen_markets:
            seen_markets[obs.market_id] = obs

    # Sort by absolute change
    sorted_obs = sorted(
        seen_markets.values(),
        key=lambda o: abs(float(o.probability_change_24h or 0)),
        reverse=True,
    )[:limit]

    result = []
    for obs in sorted_obs:
        market = (
            db.query(PredictionMarket)
            .filter(PredictionMarket.id == obs.market_id)
            .first()
        )
        if market:
            result.append(
                {
                    "market_id": market.id,
                    "source": market.source,
                    "question": market.question,
                    "category": market.category,
                    "yes_probability": float(obs.yes_probability)
                    if obs.yes_probability
                    else None,
                    "probability_change_24h": float(obs.probability_change_24h)
                    if obs.probability_change_24h
                    else None,
                    "market_url": market.market_url,
                }
            )

    return result
