"""
Foot Traffic & Location Intelligence Source Module.

Provides APIs for collecting and analyzing foot traffic data for physical locations
to evaluate retail/hospitality investments and real estate opportunities.

Data Sources:
- SafeGraph: Weekly foot traffic patterns from mobile location data
- Placer.ai: Retail analytics and competitive benchmarking
- Foursquare: POI data and enrichment
- Google Popular Times: Peak hours data (scraping, ToS risk)
- City Open Data: Pedestrian counters in select cities
"""

from app.sources.foot_traffic.client import (
    FootTrafficClient,
    FoursquareClient,
    SafeGraphClient,
)
from app.sources.foot_traffic.ingest import (
    discover_brand_locations,
    collect_traffic_for_location,
    enrich_location_metadata,
)
from app.sources.foot_traffic.metadata import (
    LOCATION_CATEGORIES,
    SOURCE_CONFIDENCE_LEVELS,
)

__all__ = [
    # Clients
    "FootTrafficClient",
    "FoursquareClient",
    "SafeGraphClient",
    # Ingestion
    "discover_brand_locations",
    "collect_traffic_for_location",
    "enrich_location_metadata",
    # Metadata
    "LOCATION_CATEGORIES",
    "SOURCE_CONFIDENCE_LEVELS",
]
