"""
Ingestion orchestration for foot traffic data.

Handles the full workflow of:
1. Discovering locations for brands
2. Collecting foot traffic observations
3. Enriching location metadata
4. Storing results in the database
"""

import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.foot_traffic.client import (
    FootTrafficClient,
    FoursquareClient,
    SafeGraphClient,
    CityDataClient,
    LocationResult,
    TrafficObservation,
)
from app.sources.foot_traffic.metadata import (
    CITY_PEDESTRIAN_DATA_SOURCES,
)

logger = logging.getLogger(__name__)


async def discover_brand_locations(
    db: Session,
    brand_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    limit: int = 50,
    job_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Discover and store locations for a brand.

    Args:
        db: Database session
        brand_name: Brand name to search for (e.g., "Starbucks", "Chipotle")
        city: Optional city filter
        state: Optional state filter
        latitude: Optional center latitude for geographic search
        longitude: Optional center longitude
        limit: Maximum locations to discover
        job_id: Optional job ID for tracking

    Returns:
        Dictionary with discovery results
    """
    started_at = datetime.utcnow()
    reasoning_log = []
    errors = []

    client = FootTrafficClient()
    available_sources = client.get_available_sources()

    reasoning_log.append(
        {
            "phase": "init",
            "available_sources": available_sources,
            "reasoning": f"Initialized with {len(available_sources)} available sources",
        }
    )

    try:
        # Discover locations
        locations = await client.discover_locations(
            brand_name=brand_name,
            city=city,
            state=state,
            latitude=latitude,
            longitude=longitude,
            limit=limit,
        )

        reasoning_log.append(
            {
                "phase": "discover",
                "locations_found": len(locations),
                "reasoning": f"Found {len(locations)} locations for '{brand_name}'",
            }
        )

        # Store locations
        new_count = 0
        updated_count = 0

        for loc in locations:
            result = await _store_location(db, loc, brand_name)
            if result == "new":
                new_count += 1
            elif result == "updated":
                updated_count += 1

        db.commit()

        reasoning_log.append(
            {
                "phase": "store",
                "new_locations": new_count,
                "updated_locations": updated_count,
                "reasoning": f"Stored {new_count} new, updated {updated_count} existing",
            }
        )

        # Update job if provided
        if job_id:
            db.execute(
                text("""
                    UPDATE foot_traffic_collection_jobs 
                    SET status = 'success',
                        completed_at = :completed_at,
                        locations_found = :locations_found,
                        sources_checked = :sources_checked,
                        reasoning_log = :reasoning_log
                    WHERE id = :job_id
                """),
                {
                    "job_id": job_id,
                    "completed_at": datetime.utcnow(),
                    "locations_found": len(locations),
                    "sources_checked": available_sources,
                    "reasoning_log": reasoning_log,
                },
            )
            db.commit()

        return {
            "status": "success",
            "brand_name": brand_name,
            "locations_found": len(locations),
            "new_locations": new_count,
            "updated_locations": updated_count,
            "sources_used": available_sources,
            "reasoning_log": reasoning_log,
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
        }

    except Exception as e:
        logger.error(
            f"Error discovering locations for '{brand_name}': {e}", exc_info=True
        )
        errors.append({"phase": "discover", "error": str(e)})

        if job_id:
            db.execute(
                text("""
                    UPDATE foot_traffic_collection_jobs 
                    SET status = 'failed',
                        completed_at = :completed_at,
                        errors = :errors,
                        reasoning_log = :reasoning_log
                    WHERE id = :job_id
                """),
                {
                    "job_id": job_id,
                    "completed_at": datetime.utcnow(),
                    "errors": errors,
                    "reasoning_log": reasoning_log,
                },
            )
            db.commit()

        return {
            "status": "failed",
            "brand_name": brand_name,
            "errors": errors,
            "reasoning_log": reasoning_log,
        }
    finally:
        await client.close()


async def _store_location(db: Session, loc: LocationResult, brand_name: str) -> str:
    """
    Store or update a location in the database.

    Returns:
        'new' if inserted, 'updated' if updated, 'skipped' if no change
    """
    # Check if exists by external ID (Foursquare) or address
    existing = None

    if loc.external_id:
        result = db.execute(
            text("""
                SELECT id FROM locations 
                WHERE foursquare_fsq_id = :fsq_id
                LIMIT 1
            """),
            {"fsq_id": loc.external_id},
        ).fetchone()
        if result:
            existing = result[0]

    if not existing:
        # Try matching by address
        result = db.execute(
            text("""
                SELECT id FROM locations 
                WHERE brand_name = :brand_name 
                AND city = :city 
                AND street_address = :address
                LIMIT 1
            """),
            {
                "brand_name": brand_name,
                "city": loc.city,
                "address": loc.address,
            },
        ).fetchone()
        if result:
            existing = result[0]

    if existing:
        # Update existing
        db.execute(
            text("""
                UPDATE locations SET
                    location_name = :name,
                    latitude = :lat,
                    longitude = :lon,
                    category = :category,
                    subcategory = :subcategory,
                    phone = :phone,
                    website = :website,
                    hours_of_operation = :hours,
                    foursquare_fsq_id = COALESCE(:fsq_id, foursquare_fsq_id),
                    last_updated = CURRENT_TIMESTAMP
                WHERE id = :id
            """),
            {
                "id": existing,
                "name": loc.name,
                "lat": str(loc.latitude),
                "lon": str(loc.longitude),
                "category": loc.category,
                "subcategory": loc.subcategory,
                "phone": loc.phone,
                "website": loc.website,
                "hours": loc.hours,
                "fsq_id": loc.external_id,
            },
        )
        return "updated"
    else:
        # Insert new
        db.execute(
            text("""
                INSERT INTO locations (
                    location_name, brand_name, street_address, city, state,
                    postal_code, latitude, longitude, category, subcategory,
                    phone, website, hours_of_operation, foursquare_fsq_id,
                    is_active, created_at, last_updated
                ) VALUES (
                    :name, :brand_name, :address, :city, :state,
                    :postal_code, :lat, :lon, :category, :subcategory,
                    :phone, :website, :hours, :fsq_id,
                    1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
            """),
            {
                "name": loc.name,
                "brand_name": brand_name,
                "address": loc.address,
                "city": loc.city,
                "state": loc.state,
                "postal_code": loc.postal_code,
                "lat": str(loc.latitude),
                "lon": str(loc.longitude),
                "category": loc.category,
                "subcategory": loc.subcategory,
                "phone": loc.phone,
                "website": loc.website,
                "hours": loc.hours,
                "fsq_id": loc.external_id,
            },
        )
        return "new"


async def collect_traffic_for_location(
    db: Session,
    location_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    sources: Optional[List[str]] = None,
    job_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Collect foot traffic data for a specific location.

    Args:
        db: Database session
        location_id: Location ID to collect traffic for
        start_date: Start of date range (default: 90 days ago)
        end_date: End of date range (default: today)
        sources: List of sources to use (default: all available)
        job_id: Optional job ID for tracking

    Returns:
        Dictionary with collection results
    """
    started_at = datetime.utcnow()
    reasoning_log = []
    errors = []
    observations_collected = 0

    # Default date range: last 90 days
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=90)

    # Get location details
    location = db.execute(
        text("SELECT * FROM locations WHERE id = :id"), {"id": location_id}
    ).fetchone()

    if not location:
        return {"status": "failed", "error": f"Location {location_id} not found"}

    reasoning_log.append(
        {
            "phase": "init",
            "location": dict(location._mapping),
            "date_range": f"{start_date} to {end_date}",
        }
    )

    from app.core.config import get_settings

    settings = get_settings()

    try:
        # Collect from SafeGraph if available and configured
        if (not sources or "safegraph" in sources) and settings.get_safegraph_api_key():
            if location.safegraph_placekey:
                try:
                    client = SafeGraphClient()
                    observations = await client.get_traffic_patterns(
                        placekey=location.safegraph_placekey,
                        start_date=start_date,
                        end_date=end_date,
                    )

                    for obs in observations:
                        await _store_observation(db, location_id, obs)
                        observations_collected += 1

                    reasoning_log.append(
                        {
                            "phase": "collect",
                            "source": "safegraph",
                            "observations": len(observations),
                        }
                    )
                    await client.close()
                except Exception as e:
                    logger.warning(f"SafeGraph collection failed: {e}")
                    errors.append({"source": "safegraph", "error": str(e)})

        # Collect from city data if in a supported city
        if (
            not sources or "city_data" in sources
        ) and location.city in CITY_PEDESTRIAN_DATA_SOURCES:
            try:
                client = CityDataClient(city=location.city)
                # Note: City data is typically for specific counters, not specific businesses
                # This is included for completeness but may not match the exact location
                observations = await client.get_pedestrian_counts(
                    start_date=start_date, end_date=end_date, limit=100
                )

                # Only store if we got data
                if observations:
                    reasoning_log.append(
                        {
                            "phase": "collect",
                            "source": "city_data",
                            "note": f"Found {len(observations)} city pedestrian counts (may not be location-specific)",
                        }
                    )
                await client.close()
            except Exception as e:
                logger.warning(f"City data collection failed: {e}")

        db.commit()

        # Update job if provided
        if job_id:
            status = "success" if not errors else "partial_success"
            db.execute(
                text("""
                    UPDATE foot_traffic_collection_jobs 
                    SET status = :status,
                        completed_at = :completed_at,
                        observations_collected = :observations,
                        reasoning_log = :reasoning_log,
                        errors = :errors
                    WHERE id = :job_id
                """),
                {
                    "job_id": job_id,
                    "status": status,
                    "completed_at": datetime.utcnow(),
                    "observations": observations_collected,
                    "reasoning_log": reasoning_log,
                    "errors": errors if errors else None,
                },
            )
            db.commit()

        return {
            "status": "success" if not errors else "partial_success",
            "location_id": location_id,
            "observations_collected": observations_collected,
            "date_range": {"start": str(start_date), "end": str(end_date)},
            "reasoning_log": reasoning_log,
            "errors": errors,
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
        }

    except Exception as e:
        logger.error(
            f"Error collecting traffic for location {location_id}: {e}", exc_info=True
        )
        return {
            "status": "failed",
            "location_id": location_id,
            "error": str(e),
            "reasoning_log": reasoning_log,
        }


async def _store_observation(
    db: Session, location_id: int, obs: TrafficObservation
) -> None:
    """Store a traffic observation in the database."""
    db.execute(
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
                visitor_count = COALESCE(:visitor_count, foot_traffic_observations.visitor_count),
                median_dwell_minutes = COALESCE(:dwell, foot_traffic_observations.median_dwell_minutes),
                collected_at = CURRENT_TIMESTAMP
        """),
        {
            "location_id": location_id,
            "obs_date": obs.observation_date,
            "period": obs.observation_period,
            "visit_count": obs.visit_count,
            "visitor_count": obs.visitor_count,
            "relative": obs.visit_count_relative,
            "dwell": str(obs.median_dwell_minutes)
            if obs.median_dwell_minutes
            else None,
            "hourly": obs.hourly_traffic,
            "daily": obs.daily_traffic,
            "demographics": obs.visitor_demographics,
            "source": obs.source_type,
            "confidence": obs.confidence,
        },
    )


async def enrich_location_metadata(
    db: Session, location_id: int, job_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Enrich location with additional metadata (ratings, trade area, competitors).

    Args:
        db: Database session
        location_id: Location ID to enrich
        job_id: Optional job ID for tracking

    Returns:
        Dictionary with enrichment results
    """
    started_at = datetime.utcnow()
    reasoning_log = []

    # Get location details
    location = db.execute(
        text("SELECT * FROM locations WHERE id = :id"), {"id": location_id}
    ).fetchone()

    if not location:
        return {"status": "failed", "error": f"Location {location_id} not found"}

    from app.core.config import get_settings

    settings = get_settings()

    enrichments = {}

    try:
        # Enrich with Foursquare details (ratings, etc.)
        if settings.get_foursquare_api_key() and location.foursquare_fsq_id:
            try:
                client = FoursquareClient()
                details = await client.get_place_details(location.foursquare_fsq_id)

                # Extract useful fields
                if "rating" in details:
                    enrichments["foursquare_rating"] = details["rating"]
                if "stats" in details:
                    enrichments["foursquare_checkins"] = details["stats"].get(
                        "total_checkins", 0
                    )

                reasoning_log.append(
                    {
                        "phase": "enrich",
                        "source": "foursquare",
                        "fields_enriched": list(enrichments.keys()),
                    }
                )
                await client.close()
            except Exception as e:
                logger.warning(f"Foursquare enrichment failed: {e}")

        # Store metadata
        if enrichments:
            # Check if metadata exists
            existing = db.execute(
                text("SELECT id FROM location_metadata WHERE location_id = :id"),
                {"id": location_id},
            ).fetchone()

            if existing:
                # Update
                db.execute(
                    text("""
                        UPDATE location_metadata SET
                            google_rating = COALESCE(:google_rating, google_rating),
                            yelp_rating = COALESCE(:yelp_rating, yelp_rating),
                            last_updated = CURRENT_TIMESTAMP
                        WHERE location_id = :location_id
                    """),
                    {
                        "location_id": location_id,
                        "google_rating": enrichments.get("google_rating"),
                        "yelp_rating": enrichments.get("yelp_rating"),
                    },
                )
            else:
                # Insert
                db.execute(
                    text("""
                        INSERT INTO location_metadata (
                            location_id, google_rating, yelp_rating, last_updated
                        ) VALUES (
                            :location_id, :google_rating, :yelp_rating, CURRENT_TIMESTAMP
                        )
                    """),
                    {
                        "location_id": location_id,
                        "google_rating": enrichments.get("google_rating"),
                        "yelp_rating": enrichments.get("yelp_rating"),
                    },
                )

            db.commit()

        return {
            "status": "success",
            "location_id": location_id,
            "enrichments": enrichments,
            "reasoning_log": reasoning_log,
            "duration_seconds": (datetime.utcnow() - started_at).total_seconds(),
        }

    except Exception as e:
        logger.error(f"Error enriching location {location_id}: {e}", exc_info=True)
        return {
            "status": "failed",
            "location_id": location_id,
            "error": str(e),
        }


async def get_brand_traffic_summary(
    db: Session,
    brand_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Get aggregated traffic summary for a brand.

    Args:
        db: Database session
        brand_name: Brand name
        city: Optional city filter
        state: Optional state filter
        start_date: Start of date range
        end_date: End of date range

    Returns:
        Dictionary with aggregated traffic metrics
    """
    # Build query
    query = """
        SELECT 
            l.brand_name,
            COUNT(DISTINCT l.id) as location_count,
            COUNT(fto.id) as observation_count,
            AVG(fto.visit_count) as avg_visits,
            SUM(fto.visit_count) as total_visits,
            MIN(fto.observation_date) as earliest_date,
            MAX(fto.observation_date) as latest_date
        FROM locations l
        LEFT JOIN foot_traffic_observations fto ON l.id = fto.location_id
        WHERE l.brand_name = :brand_name
    """
    params = {"brand_name": brand_name}

    if city:
        query += " AND l.city = :city"
        params["city"] = city
    if state:
        query += " AND l.state = :state"
        params["state"] = state
    if start_date:
        query += " AND fto.observation_date >= :start_date"
        params["start_date"] = start_date
    if end_date:
        query += " AND fto.observation_date <= :end_date"
        params["end_date"] = end_date

    query += " GROUP BY l.brand_name"

    result = db.execute(text(query), params).fetchone()

    if not result:
        return {"status": "not_found", "brand_name": brand_name}

    return {
        "brand_name": result.brand_name,
        "location_count": result.location_count,
        "observation_count": result.observation_count,
        "avg_weekly_visits": float(result.avg_visits) if result.avg_visits else None,
        "total_visits": result.total_visits,
        "date_range": {
            "start": str(result.earliest_date) if result.earliest_date else None,
            "end": str(result.latest_date) if result.latest_date else None,
        },
    }
