"""
Foot Traffic Research Agent - Main orchestrator for foot traffic data collection.

This agent:
1. Analyzes location/brand context to plan which strategies to try
2. Executes strategies in priority order
3. Synthesizes findings from multiple sources
4. Stores results in the database
5. Logs full reasoning trail for debugging
"""

import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Type

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.agentic.traffic_strategies.base import (
    BaseTrafficStrategy,
    TrafficStrategyResult,
    LocationContext,
)
from app.agentic.traffic_strategies import (
    SafeGraphStrategy,
    FoursquareStrategy,
    PlacerStrategy,
    GooglePopularTimesStrategy,
    CityDataStrategy,
)

logger = logging.getLogger(__name__)


# All available strategies (5 strategies)
AVAILABLE_STRATEGIES: List[Type[BaseTrafficStrategy]] = [
    SafeGraphStrategy,  # Strategy 1: SafeGraph (HIGH confidence)
    FoursquareStrategy,  # Strategy 2: Foursquare POI (MEDIUM confidence)
    PlacerStrategy,  # Strategy 3: Placer.ai (HIGH confidence)
    CityDataStrategy,  # Strategy 4: City pedestrian data (HIGH confidence)
    GooglePopularTimesStrategy,  # Strategy 5: Google scraping (MEDIUM, ToS risk)
]


class FootTrafficAgent:
    """
    Agentic orchestrator for foot traffic data collection.

    The agent:
    1. DISCOVER: Finds locations for a brand using POI APIs
    2. ENRICH: Enriches locations with metadata
    3. COLLECT: Gathers foot traffic data from multiple sources
    4. VALIDATE: Cross-checks data across sources
    5. ANALYZE: Calculates trends and patterns
    6. LOG: Records full reasoning trail
    """

    # Default configuration
    DEFAULT_MAX_STRATEGIES = 5
    DEFAULT_MAX_TIME_SECONDS = 600  # 10 minutes
    DEFAULT_MIN_LOCATIONS_TARGET = 10
    DEFAULT_MIN_SOURCES = 2

    def __init__(
        self,
        db: Session,
        max_strategies: int = DEFAULT_MAX_STRATEGIES,
        max_time_seconds: int = DEFAULT_MAX_TIME_SECONDS,
        min_locations_target: int = DEFAULT_MIN_LOCATIONS_TARGET,
        min_sources: int = DEFAULT_MIN_SOURCES,
    ):
        """
        Initialize the foot traffic research agent.

        Args:
            db: Database session for storing results
            max_strategies: Maximum number of strategies to try
            max_time_seconds: Maximum execution time
            min_locations_target: Target number of locations to find
            min_sources: Minimum sources for validation
        """
        self.db = db
        self.max_strategies = max_strategies
        self.max_time_seconds = max_time_seconds
        self.min_locations_target = min_locations_target
        self.min_sources = min_sources

        # Initialize strategies
        self.strategies: List[BaseTrafficStrategy] = [
            strategy_class() for strategy_class in AVAILABLE_STRATEGIES
        ]

        logger.info(
            f"Initialized FootTrafficAgent with {len(self.strategies)} strategies: "
            f"{[s.name for s in self.strategies]}"
        )

    async def discover_locations(
        self,
        brand_name: str,
        city: Optional[str] = None,
        state: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        strategies_to_use: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Discover locations for a brand using available strategies.

        Args:
            brand_name: Brand name to search for
            city: Optional city filter
            state: Optional state filter
            latitude: Optional center latitude
            longitude: Optional center longitude
            strategies_to_use: Optional list of specific strategies

        Returns:
            Discovery results with locations and reasoning
        """
        started_at = datetime.utcnow()
        context = LocationContext(
            brand_name=brand_name,
            city=city,
            state=state,
            latitude=latitude,
            longitude=longitude,
        )

        # Track results
        all_locations: List[Dict[str, Any]] = []
        strategy_results: List[TrafficStrategyResult] = []
        errors: List[Dict[str, Any]] = []
        warnings: List[str] = []
        total_requests = 0
        total_cost = 0.0
        reasoning_log: List[Dict[str, Any]] = []

        logger.info(f"Starting location discovery for '{brand_name}'")

        try:
            # Plan strategies
            if strategies_to_use:
                planned_strategies = [
                    s for s in self.strategies if s.name in strategies_to_use
                ]
            else:
                planned_strategies = self._plan_strategies(context, job_type="discover")

            reasoning_log.append(
                {
                    "phase": "plan",
                    "strategies": [s.name for s in planned_strategies],
                    "reasoning": f"Planned {len(planned_strategies)} strategies for discovery",
                }
            )

            # Execute strategies
            for strategy in planned_strategies[: self.max_strategies]:
                # Check time limit
                elapsed = (datetime.utcnow() - started_at).total_seconds()
                if elapsed > self.max_time_seconds:
                    warnings.append(f"Time limit reached after {elapsed:.0f}s")
                    break

                # Execute
                logger.info(f"Executing strategy: {strategy.name}")
                try:
                    result = await strategy.execute(context)
                    strategy_results.append(result)
                    total_requests += result.requests_made
                    total_cost += result.cost_estimate_usd

                    if result.success:
                        all_locations.extend(result.locations_found)
                        reasoning_log.append(
                            {
                                "phase": "execute",
                                "strategy": strategy.name,
                                "result": "success",
                                "locations_found": len(result.locations_found),
                                "reasoning": result.reasoning,
                            }
                        )
                    else:
                        errors.append(
                            {"strategy": strategy.name, "error": result.error_message}
                        )
                        reasoning_log.append(
                            {
                                "phase": "execute",
                                "strategy": strategy.name,
                                "result": "failed",
                                "error": result.error_message,
                            }
                        )

                except Exception as e:
                    logger.error(f"Strategy {strategy.name} failed: {e}", exc_info=True)
                    errors.append({"strategy": strategy.name, "error": str(e)})

            # Deduplicate locations
            unique_locations = self._deduplicate_locations(all_locations)

            reasoning_log.append(
                {
                    "phase": "synthesize",
                    "raw_locations": len(all_locations),
                    "unique_locations": len(unique_locations),
                    "reasoning": f"Deduplicated {len(all_locations)} to {len(unique_locations)} unique locations",
                }
            )

            # Store locations
            stored = await self._store_locations(unique_locations, brand_name)

            return {
                "status": "success" if unique_locations else "no_data",
                "brand_name": brand_name,
                "locations_found": len(unique_locations),
                "new_locations": stored["new"],
                "updated_locations": stored["updated"],
                "strategies_used": [r.strategy_name for r in strategy_results],
                "reasoning_log": reasoning_log,
                "errors": errors,
                "warnings": warnings,
                "requests_made": total_requests,
                "cost_estimate_usd": total_cost,
                "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
                "locations": unique_locations[:20],  # Preview
            }

        except Exception as e:
            logger.error(f"Location discovery failed: {e}", exc_info=True)
            return {
                "status": "failed",
                "brand_name": brand_name,
                "error": str(e),
                "reasoning_log": reasoning_log,
            }

    async def collect_traffic(
        self,
        location_id: Optional[int] = None,
        brand_name: Optional[str] = None,
        city: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        strategies_to_use: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Collect foot traffic data for a location or brand.

        Args:
            location_id: Specific location ID to collect for
            brand_name: Brand name (will collect for all locations)
            city: City filter (when using brand_name)
            start_date: Start of date range
            end_date: End of date range
            strategies_to_use: Optional list of specific strategies

        Returns:
            Collection results with observations and reasoning
        """
        started_at = datetime.utcnow()

        # Default date range
        if not end_date:
            end_date = date.today()
        if not start_date:
            start_date = end_date - timedelta(days=90)

        # Get location(s)
        if location_id:
            locations = self.db.execute(
                text("SELECT * FROM locations WHERE id = :id"), {"id": location_id}
            ).fetchall()
        elif brand_name:
            query = "SELECT * FROM locations WHERE brand_name = :brand"
            params = {"brand": brand_name}
            if city:
                query += " AND city = :city"
                params["city"] = city
            locations = self.db.execute(text(query), params).fetchall()
        else:
            return {"status": "failed", "error": "Need location_id or brand_name"}

        if not locations:
            return {"status": "not_found", "error": "No locations found"}

        # Track results
        all_observations: List[Dict[str, Any]] = []
        strategy_results: List[TrafficStrategyResult] = []
        errors: List[Dict[str, Any]] = []
        total_requests = 0
        total_cost = 0.0

        logger.info(f"Collecting traffic for {len(locations)} locations")

        try:
            for loc in locations:
                loc_dict = dict(loc._mapping)

                context = LocationContext(
                    location_id=loc_dict["id"],
                    brand_name=loc_dict.get("brand_name"),
                    city=loc_dict.get("city"),
                    state=loc_dict.get("state"),
                    latitude=float(loc_dict["latitude"])
                    if loc_dict.get("latitude")
                    else None,
                    longitude=float(loc_dict["longitude"])
                    if loc_dict.get("longitude")
                    else None,
                    start_date=start_date,
                    end_date=end_date,
                    safegraph_placekey=loc_dict.get("safegraph_placekey"),
                    foursquare_fsq_id=loc_dict.get("foursquare_fsq_id"),
                    placer_venue_id=loc_dict.get("placer_venue_id"),
                    google_place_id=loc_dict.get("google_place_id"),
                )

                # Plan strategies for traffic collection
                if strategies_to_use:
                    planned_strategies = [
                        s for s in self.strategies if s.name in strategies_to_use
                    ]
                else:
                    planned_strategies = self._plan_strategies(
                        context, job_type="collect"
                    )

                # Execute strategies for this location
                for strategy in planned_strategies[:3]:  # Max 3 strategies per location
                    try:
                        result = await strategy.execute(context)
                        strategy_results.append(result)
                        total_requests += result.requests_made
                        total_cost += result.cost_estimate_usd

                        if result.success and result.observations_found:
                            for obs in result.observations_found:
                                obs["location_id"] = loc_dict["id"]
                                all_observations.append(obs)

                    except Exception as e:
                        errors.append(
                            {
                                "location_id": loc_dict["id"],
                                "strategy": strategy.name,
                                "error": str(e),
                            }
                        )

            # Store observations
            stored_count = await self._store_observations(all_observations)

            return {
                "status": "success" if all_observations else "no_data",
                "locations_processed": len(locations),
                "observations_collected": len(all_observations),
                "observations_stored": stored_count,
                "date_range": {"start": str(start_date), "end": str(end_date)},
                "strategies_used": list(set(r.strategy_name for r in strategy_results)),
                "errors": errors,
                "requests_made": total_requests,
                "cost_estimate_usd": total_cost,
                "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
            }

        except Exception as e:
            logger.error(f"Traffic collection failed: {e}", exc_info=True)
            return {"status": "failed", "error": str(e)}

    def _plan_strategies(
        self, context: LocationContext, job_type: str = "discover"
    ) -> List[BaseTrafficStrategy]:
        """Plan which strategies to use based on context and job type."""
        planned = []

        for strategy in self.strategies:
            applicable, reasoning = strategy.is_applicable(context)
            if applicable:
                priority = strategy.calculate_priority(context)
                planned.append(
                    {"strategy": strategy, "priority": priority, "reasoning": reasoning}
                )

        # Sort by priority
        planned.sort(key=lambda x: x["priority"], reverse=True)

        return [p["strategy"] for p in planned]

    def _deduplicate_locations(
        self, locations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Deduplicate locations by address/coordinates."""
        seen = set()
        unique = []

        for loc in locations:
            # Create a key from address components
            key = (
                loc.get("brand_name", "").lower(),
                loc.get("city", "").lower(),
                loc.get("address", "")[:50].lower() if loc.get("address") else "",
            )

            if key not in seen:
                seen.add(key)
                unique.append(loc)

        return unique

    async def _store_locations(
        self, locations: List[Dict[str, Any]], brand_name: str
    ) -> Dict[str, int]:
        """Store locations in the database."""
        new_count = 0
        updated_count = 0

        for loc in locations:
            # Check if exists
            existing = self.db.execute(
                text("""
                    SELECT id FROM locations 
                    WHERE brand_name = :brand 
                    AND city = :city 
                    AND street_address = :address
                    LIMIT 1
                """),
                {
                    "brand": brand_name,
                    "city": loc.get("city"),
                    "address": loc.get("address"),
                },
            ).fetchone()

            if existing:
                # Update
                self.db.execute(
                    text("""
                        UPDATE locations SET
                            latitude = COALESCE(:lat, latitude),
                            longitude = COALESCE(:lon, longitude),
                            foursquare_fsq_id = COALESCE(:fsq_id, foursquare_fsq_id),
                            safegraph_placekey = COALESCE(:placekey, safegraph_placekey),
                            last_updated = CURRENT_TIMESTAMP
                        WHERE id = :id
                    """),
                    {
                        "id": existing[0],
                        "lat": str(loc.get("latitude"))
                        if loc.get("latitude")
                        else None,
                        "lon": str(loc.get("longitude"))
                        if loc.get("longitude")
                        else None,
                        "fsq_id": loc.get("foursquare_fsq_id"),
                        "placekey": loc.get("safegraph_placekey"),
                    },
                )
                updated_count += 1
            else:
                # Insert
                self.db.execute(
                    text("""
                        INSERT INTO locations (
                            location_name, brand_name, street_address, city, state,
                            postal_code, latitude, longitude, category, subcategory,
                            foursquare_fsq_id, safegraph_placekey,
                            is_active, created_at, last_updated
                        ) VALUES (
                            :name, :brand, :address, :city, :state,
                            :postal, :lat, :lon, :category, :subcategory,
                            :fsq_id, :placekey,
                            1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        )
                    """),
                    {
                        "name": loc.get("name", brand_name),
                        "brand": brand_name,
                        "address": loc.get("address"),
                        "city": loc.get("city"),
                        "state": loc.get("state"),
                        "postal": loc.get("postal_code"),
                        "lat": str(loc.get("latitude"))
                        if loc.get("latitude")
                        else None,
                        "lon": str(loc.get("longitude"))
                        if loc.get("longitude")
                        else None,
                        "category": loc.get("category"),
                        "subcategory": loc.get("subcategory"),
                        "fsq_id": loc.get("foursquare_fsq_id"),
                        "placekey": loc.get("safegraph_placekey"),
                    },
                )
                new_count += 1

        self.db.commit()
        return {"new": new_count, "updated": updated_count}

    async def _store_observations(self, observations: List[Dict[str, Any]]) -> int:
        """Store traffic observations in the database."""
        stored_count = 0

        for obs in observations:
            try:
                self.db.execute(
                    text("""
                        INSERT INTO foot_traffic_observations (
                            location_id, observation_date, observation_period,
                            visit_count, visitor_count, visit_count_relative,
                            median_dwell_minutes, hourly_traffic, daily_traffic,
                            visitor_demographics, source_type, source_confidence,
                            collected_at
                        ) VALUES (
                            :location_id, :obs_date, :period,
                            :visit_count, :visitor_count, :relative,
                            :dwell, :hourly, :daily,
                            :demographics, :source, :confidence,
                            CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (location_id, observation_date, observation_period, source_type)
                        DO UPDATE SET
                            visit_count = COALESCE(:visit_count, foot_traffic_observations.visit_count),
                            collected_at = CURRENT_TIMESTAMP
                    """),
                    {
                        "location_id": obs.get("location_id"),
                        "obs_date": obs.get("observation_date"),
                        "period": obs.get("observation_period", "weekly"),
                        "visit_count": obs.get("visit_count"),
                        "visitor_count": obs.get("visitor_count"),
                        "relative": obs.get("visit_count_relative"),
                        "dwell": str(obs.get("median_dwell_minutes"))
                        if obs.get("median_dwell_minutes")
                        else None,
                        "hourly": obs.get("hourly_traffic"),
                        "daily": obs.get("daily_traffic"),
                        "demographics": obs.get("visitor_demographics"),
                        "source": obs.get("source_type"),
                        "confidence": obs.get("confidence", "medium"),
                    },
                )
                stored_count += 1
            except Exception as e:
                logger.warning(f"Failed to store observation: {e}")

        self.db.commit()
        return stored_count


async def quick_location_discovery(
    db: Session,
    brand_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Quick location discovery for a brand.

    Convenience function for simple discovery jobs.
    """
    agent = FootTrafficAgent(db)
    return await agent.discover_locations(brand_name=brand_name, city=city, state=state)


async def quick_traffic_collection(
    db: Session, brand_name: str, city: Optional[str] = None, days_back: int = 90
) -> Dict[str, Any]:
    """
    Quick traffic collection for a brand.

    Convenience function for simple collection jobs.
    """
    agent = FootTrafficAgent(db)
    return await agent.collect_traffic(
        brand_name=brand_name,
        city=city,
        start_date=date.today() - timedelta(days=days_back),
        end_date=date.today(),
    )
