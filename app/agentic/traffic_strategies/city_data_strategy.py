"""
City Open Data strategy for pedestrian counters.

CONFIDENCE: HIGH (actual sensor counts)
USE CASE: Street-level pedestrian traffic in supported cities
COST: Free (public data)

Supported cities: Seattle, NYC, San Francisco, Chicago
"""

import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List

from app.agentic.traffic_strategies.base import (
    BaseTrafficStrategy,
    TrafficStrategyResult,
    LocationContext,
)
from app.sources.foot_traffic.client import CityDataClient
from app.sources.foot_traffic.metadata import CITY_PEDESTRIAN_DATA_SOURCES

logger = logging.getLogger(__name__)


class CityDataStrategy(BaseTrafficStrategy):
    """
    City Open Data pedestrian counter strategy.

    Best for:
    - High-accuracy pedestrian counts (actual sensor data)
    - Street-level traffic analysis
    - Free public data
    - Historical data (varies by city)

    Limitations:
    - Limited to ~20-30 cities with data
    - Data is for specific street locations, not individual stores
    - Coverage varies significantly by city
    - Not store-specific (area-level data)
    """

    name = "city_data"
    display_name = "City Pedestrian Counters"
    source_type = "city_data"
    default_confidence = "high"
    requires_api_key = False

    # Free public data
    cost_per_request_usd = 0.0

    # Rate limiting
    max_requests_per_second = 1.0  # 60/minute (public APIs)
    max_concurrent_requests = 10

    # Supported cities
    SUPPORTED_CITIES = list(CITY_PEDESTRIAN_DATA_SOURCES.keys())

    def is_applicable(self, context: LocationContext) -> tuple[bool, str]:
        """Check if city data strategy can be used."""
        if not context.city:
            return False, "City is required for pedestrian counter data"

        # Normalize city name
        city_normalized = context.city.strip()

        # Check if city is supported
        for supported_city in self.SUPPORTED_CITIES:
            if city_normalized.lower() == supported_city.lower():
                config = CITY_PEDESTRIAN_DATA_SOURCES[supported_city]
                if config.get("endpoint"):
                    return (
                        True,
                        f"Pedestrian counter data available for {supported_city}",
                    )

        return (
            False,
            f"City '{context.city}' does not have public pedestrian counter data",
        )

    def calculate_priority(self, context: LocationContext) -> int:
        """Calculate priority for city data strategy."""
        applicable, _ = self.is_applicable(context)
        if not applicable:
            return 0

        # High accuracy but area-level, not store-specific
        return 6

    def _normalize_city_name(self, city: str) -> str:
        """Normalize city name to match supported cities."""
        city_lower = city.lower().strip()

        # Map variations to canonical names
        variations = {
            "new york": "New York",
            "nyc": "New York",
            "new york city": "New York",
            "san francisco": "San Francisco",
            "sf": "San Francisco",
            "seattle": "Seattle",
            "chicago": "Chicago",
            "los angeles": "Los Angeles",
            "la": "Los Angeles",
        }

        return variations.get(city_lower, city)

    async def execute(self, context: LocationContext) -> TrafficStrategyResult:
        """Execute city pedestrian data strategy."""
        started_at = datetime.utcnow()
        requests_made = 0

        if not context.city:
            return self._create_result(
                success=False,
                error_message="City is required",
                reasoning="Cannot fetch city data without city name",
            )

        city_name = self._normalize_city_name(context.city)

        if city_name not in self.SUPPORTED_CITIES:
            return self._create_result(
                success=False,
                error_message=f"City '{context.city}' not supported",
                reasoning=f"Supported cities: {', '.join(self.SUPPORTED_CITIES)}",
            )

        try:
            client = CityDataClient(city=city_name)

            # Default to last 30 days
            end_date = context.end_date or date.today()
            start_date = context.start_date or (end_date - timedelta(days=30))

            observations = await client.get_pedestrian_counts(
                start_date=start_date, end_date=end_date, limit=500
            )
            requests_made += 1

            await client.close()

            observations_found = []
            for obs in observations:
                observations_found.append(
                    {
                        "observation_date": str(obs.observation_date),
                        "observation_period": obs.observation_period,
                        "visit_count": obs.visit_count,
                        "source_type": "city_data",
                        "confidence": "high",
                        "note": f"City pedestrian counter data for {city_name}",
                    }
                )

            if observations_found:
                reasoning = f"Retrieved {len(observations_found)} pedestrian count records for {city_name}"
            else:
                reasoning = (
                    f"No pedestrian count data found for {city_name} in date range"
                )

            return self._create_result(
                success=bool(observations_found),
                observations=observations_found,
                metadata={
                    "city": city_name,
                    "date_range": {"start": str(start_date), "end": str(end_date)},
                    "data_source": CITY_PEDESTRIAN_DATA_SOURCES[city_name][
                        "description"
                    ],
                },
                reasoning=reasoning,
                requests_made=requests_made,
            )

        except Exception as e:
            logger.error(f"City data strategy failed: {e}", exc_info=True)
            return self._create_result(
                success=False,
                error_message=str(e),
                reasoning=f"Error fetching city data: {e}",
                requests_made=requests_made,
            )
