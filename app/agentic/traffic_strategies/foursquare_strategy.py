"""
Foursquare Places API strategy for POI discovery and enrichment.

CONFIDENCE: MEDIUM (check-in data is opt-in, not representative)
USE CASE: Location discovery, POI metadata enrichment
COST: Free tier + $0.01-0.05/call for premium
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from app.agentic.traffic_strategies.base import (
    BaseTrafficStrategy,
    TrafficStrategyResult,
    LocationContext,
)
from app.core.config import get_settings
from app.sources.foot_traffic.client import FoursquareClient

logger = logging.getLogger(__name__)


class FoursquareStrategy(BaseTrafficStrategy):
    """
    Foursquare Places API strategy.

    Best for:
    - Discovering locations for a brand
    - Enriching location metadata (address, hours, categories)
    - Getting check-in data as a weak traffic signal

    Limitations:
    - Check-in data is opt-in (not representative of actual traffic)
    - No historical traffic patterns
    """

    name = "foursquare"
    display_name = "Foursquare Places"
    source_type = "foursquare"
    default_confidence = "medium"
    requires_api_key = True

    # Foursquare pricing: ~$0.01-0.05 per API call
    cost_per_request_usd = 0.01

    # Rate limiting
    max_requests_per_second = 0.8  # ~50/minute
    max_concurrent_requests = 5

    def is_applicable(self, context: LocationContext) -> tuple[bool, str]:
        """Check if Foursquare strategy can be used."""
        settings = get_settings()

        if not settings.get_foursquare_api_key():
            return False, "Foursquare API key not configured"

        # Foursquare is great for brand/location discovery
        if context.brand_name:
            return True, "Foursquare can discover locations for brand"

        # Also good for enriching existing locations
        if context.foursquare_fsq_id:
            return True, "Foursquare ID available for enrichment"

        # Can search by coordinates
        if context.latitude and context.longitude:
            return True, "Can search by geographic coordinates"

        return False, "Need brand name, Foursquare ID, or coordinates"

    def calculate_priority(self, context: LocationContext) -> int:
        """Calculate priority for Foursquare strategy."""
        applicable, _ = self.is_applicable(context)
        if not applicable:
            return 0

        # Foursquare is best for location discovery
        if context.brand_name and not context.location_id:
            return 9  # High priority for discovery

        # Good for enrichment
        if context.foursquare_fsq_id:
            return 7

        return 5  # Default medium priority

    async def execute(self, context: LocationContext) -> TrafficStrategyResult:
        """Execute Foursquare strategy."""
        requests_made = 0

        try:
            client = FoursquareClient()
            locations_found = []
            metadata = {}

            # Strategy 1: Discover locations for a brand
            if context.brand_name:
                results = await client.search_locations(
                    query=context.brand_name,
                    latitude=context.latitude,
                    longitude=context.longitude,
                    radius_meters=context.radius_meters,
                    limit=50,
                )
                requests_made += 1

                for loc in results:
                    locations_found.append(
                        {
                            "name": loc.name,
                            "brand_name": context.brand_name,
                            "address": loc.address,
                            "city": loc.city,
                            "state": loc.state,
                            "postal_code": loc.postal_code,
                            "latitude": loc.latitude,
                            "longitude": loc.longitude,
                            "category": loc.category,
                            "subcategory": loc.subcategory,
                            "foursquare_fsq_id": loc.external_id,
                            "phone": loc.phone,
                            "website": loc.website,
                            "hours": loc.hours,
                            "source_type": "foursquare",
                            "confidence": "medium",
                        }
                    )

            # Strategy 2: Enrich existing location with details
            elif context.foursquare_fsq_id:
                details = await client.get_place_details(context.foursquare_fsq_id)
                requests_made += 1

                metadata = {
                    "foursquare_rating": details.get("rating"),
                    "foursquare_price": details.get("price"),
                    "total_checkins": details.get("stats", {}).get("total_checkins", 0),
                    "total_users": details.get("stats", {}).get("total_users", 0),
                    "hours": details.get("hours"),
                    "popular_times": details.get("popular"),
                }

            await client.close()

            reasoning = (
                f"Found {len(locations_found)} locations for '{context.brand_name}'"
                if locations_found
                else f"Enriched location with Foursquare metadata"
            )

            return self._create_result(
                success=True,
                locations=locations_found,
                metadata=metadata,
                reasoning=reasoning,
                requests_made=requests_made,
            )

        except Exception as e:
            logger.error(f"Foursquare strategy failed: {e}", exc_info=True)
            return self._create_result(
                success=False,
                error_message=str(e),
                reasoning=f"Foursquare API error: {e}",
                requests_made=requests_made,
            )
