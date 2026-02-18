"""
Placer.ai API strategy for retail analytics.

CONFIDENCE: HIGH (similar data quality to SafeGraph)
USE CASE: Retail analytics, trade area analysis, competitive benchmarking
COST: $500-2,000+/month (enterprise pricing)
"""

import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

import httpx

from app.agentic.traffic_strategies.base import (
    BaseTrafficStrategy,
    TrafficStrategyResult,
    LocationContext,
)
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class PlacerStrategy(BaseTrafficStrategy):
    """
    Placer.ai API strategy.

    Best for:
    - Retail chain analytics
    - Trade area analysis (who lives/works nearby)
    - Competitive benchmarking
    - Mall and shopping center data
    - Monthly/weekly visit trends

    Limitations:
    - Expensive enterprise pricing ($500-2,000+/month)
    - Retail-focused (less coverage for offices, etc.)
    - Requires venue ID for traffic data
    """

    name = "placer"
    display_name = "Placer.ai Analytics"
    source_type = "placer"
    default_confidence = "high"
    requires_api_key = True

    # Placer pricing: enterprise
    cost_per_request_usd = 0.10

    # Rate limiting
    max_requests_per_second = 0.5  # 30/minute
    max_concurrent_requests = 3

    base_url = "https://api.placer.ai/v1"

    def is_applicable(self, context: LocationContext) -> tuple[bool, str]:
        """Check if Placer strategy can be used."""
        settings = get_settings()

        if not settings.get_placer_api_key():
            return False, "Placer.ai API key not configured"

        # Placer requires a venue ID for traffic data
        if context.placer_venue_id:
            return True, "Placer venue ID available for analytics"

        # Can search for retail locations
        if context.brand_name:
            # Placer is best for major retail chains
            return True, "Can search Placer for retail brand"

        return False, "Need Placer venue ID or brand name"

    def calculate_priority(self, context: LocationContext) -> int:
        """Calculate priority for Placer strategy."""
        applicable, _ = self.is_applicable(context)
        if not applicable:
            return 0

        # Placer excels at retail analytics
        if context.category in ["retail", "restaurant"]:
            return 9

        if context.placer_venue_id:
            return 8

        return 6

    async def execute(self, context: LocationContext) -> TrafficStrategyResult:
        """Execute Placer.ai strategy."""
        requests_made = 0

        settings = get_settings()
        api_key = settings.get_placer_api_key()

        if not api_key:
            return self._create_result(
                success=False,
                error_message="Placer.ai API key not configured",
                reasoning="Cannot execute without API key",
            )

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                headers = {"X-API-Key": api_key, "Accept": "application/json"}
                observations_found = []
                metadata = {}

                # Get insights for a venue
                if context.placer_venue_id:
                    url = f"{self.base_url}/venues/{context.placer_venue_id}/insights"

                    response = await client.get(url, headers=headers)
                    response.raise_for_status()
                    requests_made += 1

                    data = response.json()

                    # Extract monthly visits
                    for visit in data.get("monthly_visits", []):
                        observations_found.append(
                            {
                                "observation_date": visit.get("month"),
                                "observation_period": "monthly",
                                "visit_count": visit.get("visits"),
                                "source_type": "placer",
                                "confidence": "high",
                            }
                        )

                    # Extract trade area data
                    trade_area = data.get("trade_area", {})
                    if trade_area:
                        metadata["trade_area_5min"] = trade_area.get("5_min_drive", {})
                        metadata["trade_area_10min"] = trade_area.get(
                            "10_min_drive", {}
                        )

                    # Extract competitive set
                    competitors = data.get("competitive_set", [])
                    if competitors:
                        metadata["competitors"] = [
                            {
                                "name": c.get("competitor"),
                                "visits": c.get("visits"),
                                "distance_mi": c.get("distance_mi"),
                            }
                            for c in competitors
                        ]

                # Search for venues by brand name
                elif context.brand_name:
                    url = f"{self.base_url}/venues/search"
                    params = {"query": context.brand_name, "limit": 50}

                    if context.city:
                        params["city"] = context.city
                    if context.state:
                        params["state"] = context.state

                    response = await client.get(url, headers=headers, params=params)
                    response.raise_for_status()
                    requests_made += 1

                    data = response.json()

                    locations_found = []
                    for venue in data.get("venues", []):
                        locations_found.append(
                            {
                                "name": venue.get("venue_name"),
                                "brand_name": context.brand_name,
                                "address": venue.get("street_address"),
                                "city": venue.get("city"),
                                "state": venue.get("state"),
                                "placer_venue_id": venue.get("venue_id"),
                                "source_type": "placer",
                                "confidence": "high",
                            }
                        )

                    return self._create_result(
                        success=bool(locations_found),
                        locations=locations_found,
                        reasoning=f"Found {len(locations_found)} venues via Placer.ai",
                        requests_made=requests_made,
                    )

                reasoning = (
                    f"Retrieved {len(observations_found)} monthly observations"
                    if observations_found
                    else "No traffic data found"
                )

                return self._create_result(
                    success=bool(observations_found),
                    observations=observations_found,
                    metadata=metadata,
                    reasoning=reasoning,
                    requests_made=requests_made,
                )

        except httpx.HTTPStatusError as e:
            logger.error(f"Placer.ai API error: {e}")
            return self._create_result(
                success=False,
                error_message=f"HTTP {e.response.status_code}: {e.response.text}",
                reasoning=f"Placer.ai API error",
                requests_made=requests_made,
            )
        except Exception as e:
            logger.error(f"Placer strategy failed: {e}", exc_info=True)
            return self._create_result(
                success=False,
                error_message=str(e),
                reasoning=f"Placer.ai error: {e}",
                requests_made=requests_made,
            )
