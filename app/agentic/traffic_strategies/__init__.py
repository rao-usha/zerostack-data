"""
Traffic collection strategies for the foot traffic agent.

Provides 5 data collection strategies:
1. SafeGraph - Weekly foot traffic from mobile location data (HIGH confidence)
2. Foursquare - POI enrichment and check-in data (MEDIUM confidence)
3. Placer.ai - Retail analytics and trade area data (HIGH confidence)
4. Google Popular Times - Peak hours scraping (MEDIUM confidence, ToS risk)
5. City Open Data - Public pedestrian counters (HIGH confidence, limited coverage)
"""

from app.agentic.traffic_strategies.base import (
    BaseTrafficStrategy,
    TrafficStrategyResult,
    LocationContext,
)
from app.agentic.traffic_strategies.safegraph_strategy import SafeGraphStrategy
from app.agentic.traffic_strategies.foursquare_strategy import FoursquareStrategy
from app.agentic.traffic_strategies.placer_strategy import PlacerStrategy
from app.agentic.traffic_strategies.google_strategy import GooglePopularTimesStrategy
from app.agentic.traffic_strategies.city_data_strategy import CityDataStrategy

__all__ = [
    # Base
    "BaseTrafficStrategy",
    "TrafficStrategyResult",
    "LocationContext",
    # Strategies
    "SafeGraphStrategy",
    "FoursquareStrategy",
    "PlacerStrategy",
    "GooglePopularTimesStrategy",
    "CityDataStrategy",
]
