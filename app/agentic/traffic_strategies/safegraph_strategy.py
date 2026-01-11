"""
SafeGraph Patterns API strategy for foot traffic data.

CONFIDENCE: HIGH (absolute visitor counts from mobile location data)
USE CASE: Weekly traffic patterns, historical data, visitor demographics
COST: $100-500/month depending on tier
"""
import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List

from app.agentic.traffic_strategies.base import (
    BaseTrafficStrategy,
    TrafficStrategyResult,
    LocationContext,
)
from app.core.config import get_settings
from app.sources.foot_traffic.client import SafeGraphClient

logger = logging.getLogger(__name__)


class SafeGraphStrategy(BaseTrafficStrategy):
    """
    SafeGraph Patterns API strategy.
    
    Best for:
    - Absolute visitor counts (not relative)
    - Historical traffic data (2+ years)
    - Weekly patterns and trends
    - Visitor demographics and home locations
    - Dwell time analysis
    
    Limitations:
    - Requires paid subscription ($100-500/month)
    - Based on ~10-15% mobile location sample
    - Weekly granularity (not daily or hourly)
    """
    
    name = "safegraph"
    display_name = "SafeGraph Patterns"
    source_type = "safegraph"
    default_confidence = "high"
    requires_api_key = True
    
    # SafeGraph pricing: varies by tier
    cost_per_request_usd = 0.05
    
    # Rate limiting
    max_requests_per_second = 1.0  # 60/minute
    max_concurrent_requests = 5
    
    def is_applicable(self, context: LocationContext) -> tuple[bool, str]:
        """Check if SafeGraph strategy can be used."""
        settings = get_settings()
        
        if not settings.get_safegraph_api_key():
            return False, "SafeGraph API key not configured"
        
        # SafeGraph requires a placekey for traffic data
        if context.safegraph_placekey:
            return True, "SafeGraph placekey available for traffic patterns"
        
        # Can also search for locations
        if context.brand_name:
            return True, "Can search SafeGraph for brand locations"
        
        if context.latitude and context.longitude:
            return True, "Can search SafeGraph by coordinates"
        
        return False, "Need SafeGraph placekey, brand name, or coordinates"
    
    def calculate_priority(self, context: LocationContext) -> int:
        """Calculate priority for SafeGraph strategy."""
        applicable, _ = self.is_applicable(context)
        if not applicable:
            return 0
        
        # SafeGraph is best source for actual traffic data
        if context.safegraph_placekey:
            return 10  # Highest priority when we have the ID
        
        # Good for brand search too
        if context.brand_name:
            return 8
        
        return 7
    
    async def execute(self, context: LocationContext) -> TrafficStrategyResult:
        """Execute SafeGraph strategy."""
        started_at = datetime.utcnow()
        requests_made = 0
        
        try:
            client = SafeGraphClient()
            locations_found = []
            observations_found = []
            
            # Strategy 1: Get traffic patterns for known location
            if context.safegraph_placekey:
                # Default to last 90 days if not specified
                end_date = context.end_date or date.today()
                start_date = context.start_date or (end_date - timedelta(days=90))
                
                observations = await client.get_traffic_patterns(
                    placekey=context.safegraph_placekey,
                    start_date=start_date,
                    end_date=end_date
                )
                requests_made += 1
                
                for obs in observations:
                    observations_found.append({
                        "observation_date": str(obs.observation_date),
                        "observation_period": obs.observation_period,
                        "visit_count": obs.visit_count,
                        "visitor_count": obs.visitor_count,
                        "median_dwell_minutes": obs.median_dwell_minutes,
                        "hourly_traffic": obs.hourly_traffic,
                        "daily_traffic": obs.daily_traffic,
                        "visitor_demographics": obs.visitor_demographics,
                        "source_type": "safegraph",
                        "confidence": "high",
                    })
            
            # Strategy 2: Discover locations for a brand
            elif context.brand_name:
                results = await client.search_locations(
                    query=context.brand_name,
                    latitude=context.latitude,
                    longitude=context.longitude,
                    radius_meters=context.radius_meters,
                    limit=50
                )
                requests_made += 1
                
                for loc in results:
                    locations_found.append({
                        "name": loc.name,
                        "brand_name": context.brand_name,
                        "address": loc.address,
                        "city": loc.city,
                        "state": loc.state,
                        "postal_code": loc.postal_code,
                        "latitude": loc.latitude,
                        "longitude": loc.longitude,
                        "category": loc.category,
                        "safegraph_placekey": loc.external_id,
                        "source_type": "safegraph",
                        "confidence": "high",
                    })
            
            await client.close()
            
            if observations_found:
                reasoning = f"Retrieved {len(observations_found)} weekly traffic observations"
            elif locations_found:
                reasoning = f"Found {len(locations_found)} locations via SafeGraph"
            else:
                reasoning = "No data found in SafeGraph"
            
            return self._create_result(
                success=bool(observations_found or locations_found),
                locations=locations_found,
                observations=observations_found,
                reasoning=reasoning,
                requests_made=requests_made
            )
            
        except Exception as e:
            logger.error(f"SafeGraph strategy failed: {e}", exc_info=True)
            return self._create_result(
                success=False,
                error_message=str(e),
                reasoning=f"SafeGraph API error: {e}",
                requests_made=requests_made
            )
