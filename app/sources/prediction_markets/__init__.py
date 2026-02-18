"""
Prediction Market Intelligence Source Module.

Monitors prediction markets (Kalshi, Polymarket, PredictIt) to track
market consensus on economic, political, sports, and business events.

Components:
- client.py: API clients for each platform
- ingest.py: Ingestion orchestration and job tracking
- metadata.py: Market categories and classification
"""

from app.sources.prediction_markets.client import (
    KalshiClient,
    PolymarketClient,
    get_kalshi_client,
    get_polymarket_client,
)
from app.sources.prediction_markets.ingest import (
    monitor_all_platforms,
    monitor_kalshi,
    monitor_polymarket,
    get_job_status,
)
from app.sources.prediction_markets.metadata import (
    MARKET_CATEGORIES,
    categorize_market,
)

__all__ = [
    # Clients
    "KalshiClient",
    "PolymarketClient",
    "get_kalshi_client",
    "get_polymarket_client",
    # Ingestion
    "monitor_all_platforms",
    "monitor_kalshi",
    "monitor_polymarket",
    "get_job_status",
    # Metadata
    "MARKET_CATEGORIES",
    "categorize_market",
]
