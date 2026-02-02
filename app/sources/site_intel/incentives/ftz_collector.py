"""
FTZ Board Collector.

Fetches Foreign Trade Zone data from FTZ Board:
- Zone locations and boundaries
- Grantee information
- Subzone designations

Data source: https://www.trade.gov/foreign-trade-zones-board
OFIS Portal: https://ofis.trade.gov/Zones

Note: The HIFLD ArcGIS endpoint was decommissioned in August 2025.
This collector now uses a built-in seed dataset of major FTZ zones
compiled from public FTZ Board records.
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import ForeignTradeZone
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)


# Major FTZ zones compiled from FTZ Board public records
# Source: https://ofis.trade.gov/Zones (January 2026)
FTZ_SEED_DATA = [
    # Texas
    {"ftz_number": 12, "zone_name": "McAllen Trade Zone", "grantee": "McAllen Economic Development Corporation", "state": "TX", "city": "McAllen", "latitude": 26.2034, "longitude": -98.2300},
    {"ftz_number": 36, "zone_name": "Galveston", "grantee": "Board of Trustees of Galveston Wharves", "state": "TX", "city": "Galveston", "latitude": 29.3013, "longitude": -94.7977},
    {"ftz_number": 39, "zone_name": "Dallas/Fort Worth", "grantee": "DFW International Airport Board", "state": "TX", "city": "Dallas", "latitude": 32.8998, "longitude": -97.0403},
    {"ftz_number": 62, "zone_name": "Brownsville", "grantee": "Brownsville Navigation District", "state": "TX", "city": "Brownsville", "latitude": 25.9017, "longitude": -97.4975},
    {"ftz_number": 68, "zone_name": "El Paso", "grantee": "City of El Paso", "state": "TX", "city": "El Paso", "latitude": 31.7619, "longitude": -106.4850},
    {"ftz_number": 80, "zone_name": "San Antonio", "grantee": "City of San Antonio", "state": "TX", "city": "San Antonio", "latitude": 29.4241, "longitude": -98.4936},
    {"ftz_number": 84, "zone_name": "Harris County", "grantee": "Port of Houston Authority", "state": "TX", "city": "Houston", "latitude": 29.7604, "longitude": -95.3698},
    {"ftz_number": 94, "zone_name": "Laredo", "grantee": "Laredo Development Foundation", "state": "TX", "city": "Laredo", "latitude": 27.5036, "longitude": -99.5075},
    {"ftz_number": 113, "zone_name": "Ellis County", "grantee": "Ellis County Trade Zone Corporation", "state": "TX", "city": "Midlothian", "latitude": 32.4821, "longitude": -96.9945},
    {"ftz_number": 196, "zone_name": "Fort Worth", "grantee": "Alliance Global Logistics Hub", "state": "TX", "city": "Fort Worth", "latitude": 32.9685, "longitude": -97.2898},
    # Virginia
    {"ftz_number": 20, "zone_name": "Suffolk", "grantee": "Virginia Port Authority", "state": "VA", "city": "Suffolk", "latitude": 36.7282, "longitude": -76.5836},
    {"ftz_number": 137, "zone_name": "Washington Dulles", "grantee": "Metropolitan Washington Airports Authority", "state": "VA", "city": "Dulles", "latitude": 38.9531, "longitude": -77.4565},
    {"ftz_number": 138, "zone_name": "Franklin County", "grantee": "County of Franklin", "state": "VA", "city": "Rocky Mount", "latitude": 36.9976, "longitude": -79.8920},
    {"ftz_number": 185, "zone_name": "Culpeper County", "grantee": "County of Culpeper", "state": "VA", "city": "Culpeper", "latitude": 38.4729, "longitude": -77.9966},
    {"ftz_number": 187, "zone_name": "Richmond", "grantee": "Greater Richmond Partnership", "state": "VA", "city": "Richmond", "latitude": 37.5407, "longitude": -77.4360},
    # California
    {"ftz_number": 3, "zone_name": "San Francisco", "grantee": "San Francisco Port Commission", "state": "CA", "city": "San Francisco", "latitude": 37.7749, "longitude": -122.4194},
    {"ftz_number": 50, "zone_name": "Long Beach", "grantee": "Board of Harbor Commissioners", "state": "CA", "city": "Long Beach", "latitude": 33.7701, "longitude": -118.1937},
    {"ftz_number": 56, "zone_name": "Oakland", "grantee": "City of Oakland", "state": "CA", "city": "Oakland", "latitude": 37.8044, "longitude": -122.2712},
    {"ftz_number": 143, "zone_name": "West Sacramento", "grantee": "Port of West Sacramento", "state": "CA", "city": "West Sacramento", "latitude": 38.5805, "longitude": -121.5302},
    {"ftz_number": 191, "zone_name": "Palmdale", "grantee": "City of Palmdale", "state": "CA", "city": "Palmdale", "latitude": 34.5794, "longitude": -118.1165},
    {"ftz_number": 202, "zone_name": "Los Angeles", "grantee": "Los Angeles World Airports", "state": "CA", "city": "Los Angeles", "latitude": 33.9425, "longitude": -118.4081},
    {"ftz_number": 205, "zone_name": "San Diego", "grantee": "San Diego Regional EDC", "state": "CA", "city": "San Diego", "latitude": 32.7157, "longitude": -117.1611},
    {"ftz_number": 236, "zone_name": "Riverside County", "grantee": "County of Riverside", "state": "CA", "city": "Riverside", "latitude": 33.9806, "longitude": -117.3755},
    # New York
    {"ftz_number": 1, "zone_name": "New York", "grantee": "City of New York", "state": "NY", "city": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"ftz_number": 23, "zone_name": "Buffalo", "grantee": "County of Erie", "state": "NY", "city": "Buffalo", "latitude": 42.8864, "longitude": -78.8784},
    {"ftz_number": 37, "zone_name": "Orange County", "grantee": "County of Orange", "state": "NY", "city": "Newburgh", "latitude": 41.5034, "longitude": -74.0104},
    {"ftz_number": 52, "zone_name": "Suffolk County", "grantee": "County of Suffolk", "state": "NY", "city": "Hauppauge", "latitude": 40.8251, "longitude": -73.2026},
    {"ftz_number": 109, "zone_name": "Watertown", "grantee": "Jefferson County Industrial Development Agency", "state": "NY", "city": "Watertown", "latitude": 43.9748, "longitude": -75.9108},
    {"ftz_number": 121, "zone_name": "Albany", "grantee": "Capital District Regional Planning Commission", "state": "NY", "city": "Albany", "latitude": 42.6526, "longitude": -73.7562},
    {"ftz_number": 141, "zone_name": "Monroe County", "grantee": "County of Monroe", "state": "NY", "city": "Rochester", "latitude": 43.1566, "longitude": -77.6088},
    # Florida
    {"ftz_number": 25, "zone_name": "Port Everglades", "grantee": "Port Everglades Authority", "state": "FL", "city": "Fort Lauderdale", "latitude": 26.0851, "longitude": -80.1173},
    {"ftz_number": 32, "zone_name": "Miami", "grantee": "Greater Miami FTZ Inc", "state": "FL", "city": "Miami", "latitude": 25.7617, "longitude": -80.1918},
    {"ftz_number": 42, "zone_name": "Orlando", "grantee": "Greater Orlando Aviation Authority", "state": "FL", "city": "Orlando", "latitude": 28.5383, "longitude": -81.3792},
    {"ftz_number": 64, "zone_name": "Jacksonville", "grantee": "Jacksonville Port Authority", "state": "FL", "city": "Jacksonville", "latitude": 30.3322, "longitude": -81.6557},
    {"ftz_number": 65, "zone_name": "Panama City", "grantee": "Panama City Port Authority", "state": "FL", "city": "Panama City", "latitude": 30.1588, "longitude": -85.6602},
    {"ftz_number": 79, "zone_name": "Tampa", "grantee": "Tampa Port Authority", "state": "FL", "city": "Tampa", "latitude": 27.9506, "longitude": -82.4572},
    {"ftz_number": 169, "zone_name": "Manatee County", "grantee": "Manatee County Port Authority", "state": "FL", "city": "Palmetto", "latitude": 27.5214, "longitude": -82.5723},
    {"ftz_number": 180, "zone_name": "Pinellas County", "grantee": "City of Clearwater", "state": "FL", "city": "Clearwater", "latitude": 27.9659, "longitude": -82.8001},
    {"ftz_number": 241, "zone_name": "Fort Pierce", "grantee": "St. Lucie County Board of County Commissioners", "state": "FL", "city": "Fort Pierce", "latitude": 27.4467, "longitude": -80.3256},
    # Illinois
    {"ftz_number": 22, "zone_name": "Chicago", "grantee": "Illinois International Port District", "state": "IL", "city": "Chicago", "latitude": 41.8781, "longitude": -87.6298},
    {"ftz_number": 31, "zone_name": "Granite City", "grantee": "Tri-City Regional Port District", "state": "IL", "city": "Granite City", "latitude": 38.7014, "longitude": -90.1487},
    {"ftz_number": 114, "zone_name": "Peoria", "grantee": "Greater Peoria FTZ Inc", "state": "IL", "city": "Peoria", "latitude": 40.6936, "longitude": -89.5890},
    {"ftz_number": 176, "zone_name": "Rockford", "grantee": "Greater Rockford Airport Authority", "state": "IL", "city": "Rockford", "latitude": 42.2711, "longitude": -89.0940},
    # Georgia
    {"ftz_number": 26, "zone_name": "Atlanta", "grantee": "Georgia Foreign Trade Zone Inc", "state": "GA", "city": "Atlanta", "latitude": 33.7490, "longitude": -84.3880},
    {"ftz_number": 104, "zone_name": "Savannah", "grantee": "Savannah Airport Commission", "state": "GA", "city": "Savannah", "latitude": 32.1279, "longitude": -81.2022},
    {"ftz_number": 144, "zone_name": "Brunswick", "grantee": "Brunswick and Glynn County Development Authority", "state": "GA", "city": "Brunswick", "latitude": 31.1499, "longitude": -81.4915},
    # Michigan
    {"ftz_number": 16, "zone_name": "Sault Ste. Marie", "grantee": "Economic Development Corporation of Sault Ste. Marie", "state": "MI", "city": "Sault Ste. Marie", "latitude": 46.4953, "longitude": -84.3453},
    {"ftz_number": 43, "zone_name": "Battle Creek", "grantee": "City of Battle Creek", "state": "MI", "city": "Battle Creek", "latitude": 42.3212, "longitude": -85.1797},
    {"ftz_number": 70, "zone_name": "Detroit", "grantee": "Greater Detroit Foreign Trade Zone Inc", "state": "MI", "city": "Detroit", "latitude": 42.3314, "longitude": -83.0458},
    {"ftz_number": 140, "zone_name": "Flint", "grantee": "City of Flint", "state": "MI", "city": "Flint", "latitude": 43.0125, "longitude": -83.6875},
    {"ftz_number": 189, "zone_name": "Kent/Ottawa Counties", "grantee": "Right Place Inc", "state": "MI", "city": "Grand Rapids", "latitude": 42.9634, "longitude": -85.6681},
    {"ftz_number": 210, "zone_name": "Lansing", "grantee": "Lansing Economic Development Corporation", "state": "MI", "city": "Lansing", "latitude": 42.7325, "longitude": -84.5555},
    # Ohio
    {"ftz_number": 8, "zone_name": "Toledo", "grantee": "Toledo-Lucas County Port Authority", "state": "OH", "city": "Toledo", "latitude": 41.6528, "longitude": -83.5379},
    {"ftz_number": 40, "zone_name": "Cleveland", "grantee": "Cleveland Cuyahoga County Port Authority", "state": "OH", "city": "Cleveland", "latitude": 41.4993, "longitude": -81.6944},
    {"ftz_number": 46, "zone_name": "Cincinnati", "grantee": "Greater Cincinnati FTZ Inc", "state": "OH", "city": "Cincinnati", "latitude": 39.1031, "longitude": -84.5120},
    {"ftz_number": 100, "zone_name": "Dayton", "grantee": "City of Dayton", "state": "OH", "city": "Dayton", "latitude": 39.7589, "longitude": -84.1916},
    {"ftz_number": 138, "zone_name": "Columbus", "grantee": "Columbus Regional Airport Authority", "state": "OH", "city": "Columbus", "latitude": 39.9612, "longitude": -82.9988},
    # Washington
    {"ftz_number": 5, "zone_name": "Seattle", "grantee": "Port of Seattle", "state": "WA", "city": "Seattle", "latitude": 47.6062, "longitude": -122.3321},
    {"ftz_number": 61, "zone_name": "Everett", "grantee": "Port of Everett", "state": "WA", "city": "Everett", "latitude": 47.9790, "longitude": -122.2021},
    {"ftz_number": 85, "zone_name": "Tacoma", "grantee": "Port of Tacoma", "state": "WA", "city": "Tacoma", "latitude": 47.2529, "longitude": -122.4443},
    {"ftz_number": 216, "zone_name": "Vancouver", "grantee": "Port of Vancouver USA", "state": "WA", "city": "Vancouver", "latitude": 45.6387, "longitude": -122.6615},
    # Arizona
    {"ftz_number": 48, "zone_name": "Tucson", "grantee": "Tucson Airport Authority", "state": "AZ", "city": "Tucson", "latitude": 32.1161, "longitude": -110.9375},
    {"ftz_number": 60, "zone_name": "Nogales", "grantee": "Border Industrial Development Inc", "state": "AZ", "city": "Nogales", "latitude": 31.3404, "longitude": -110.9343},
    {"ftz_number": 75, "zone_name": "Phoenix", "grantee": "City of Phoenix", "state": "AZ", "city": "Phoenix", "latitude": 33.4484, "longitude": -112.0740},
    {"ftz_number": 139, "zone_name": "Sierra Vista", "grantee": "City of Sierra Vista", "state": "AZ", "city": "Sierra Vista", "latitude": 31.5455, "longitude": -110.3035},
    # North Carolina
    {"ftz_number": 57, "zone_name": "Mecklenburg County", "grantee": "North Carolina Department of Transportation", "state": "NC", "city": "Charlotte", "latitude": 35.2271, "longitude": -80.8431},
    {"ftz_number": 67, "zone_name": "Morehead City", "grantee": "North Carolina State Ports Authority", "state": "NC", "city": "Morehead City", "latitude": 34.7229, "longitude": -76.7261},
    {"ftz_number": 93, "zone_name": "Raleigh-Durham", "grantee": "Triangle J Council of Governments", "state": "NC", "city": "Raleigh", "latitude": 35.7796, "longitude": -78.6382},
    {"ftz_number": 214, "zone_name": "Wilmington", "grantee": "City of Wilmington", "state": "NC", "city": "Wilmington", "latitude": 34.2257, "longitude": -77.9447},
    # New Jersey
    {"ftz_number": 44, "zone_name": "Morris County", "grantee": "Somerset County Improvement Authority", "state": "NJ", "city": "Mt. Olive", "latitude": 40.8618, "longitude": -74.7340},
    {"ftz_number": 49, "zone_name": "Newark/Elizabeth", "grantee": "Port Authority of NY and NJ", "state": "NJ", "city": "Newark", "latitude": 40.7357, "longitude": -74.1724},
    {"ftz_number": 142, "zone_name": "Salem/Gloucester Counties", "grantee": "South Jersey Port Corporation", "state": "NJ", "city": "Camden", "latitude": 39.9260, "longitude": -75.1195},
    # Pennsylvania
    {"ftz_number": 24, "zone_name": "Pittston", "grantee": "Luzerne County", "state": "PA", "city": "Pittston", "latitude": 41.3259, "longitude": -75.7891},
    {"ftz_number": 35, "zone_name": "Philadelphia", "grantee": "Philadelphia Regional Port Authority", "state": "PA", "city": "Philadelphia", "latitude": 39.9526, "longitude": -75.1652},
    {"ftz_number": 147, "zone_name": "Reading", "grantee": "Berks County Industrial Development Authority", "state": "PA", "city": "Reading", "latitude": 40.3356, "longitude": -75.9269},
    {"ftz_number": 148, "zone_name": "Harrisburg", "grantee": "Harrisburg International Airport Authority", "state": "PA", "city": "Harrisburg", "latitude": 40.2732, "longitude": -76.8867},
    # Massachusetts
    {"ftz_number": 27, "zone_name": "Boston", "grantee": "Massachusetts Port Authority", "state": "MA", "city": "Boston", "latitude": 42.3601, "longitude": -71.0589},
    {"ftz_number": 28, "zone_name": "New Bedford", "grantee": "New Bedford Industrial Development Financing Authority", "state": "MA", "city": "New Bedford", "latitude": 41.6362, "longitude": -70.9342},
    # Louisiana
    {"ftz_number": 2, "zone_name": "New Orleans", "grantee": "Board of Commissioners of Port of New Orleans", "state": "LA", "city": "New Orleans", "latitude": 29.9511, "longitude": -90.0715},
    {"ftz_number": 87, "zone_name": "Lake Charles", "grantee": "Lake Charles Harbor and Terminal District", "state": "LA", "city": "Lake Charles", "latitude": 30.2266, "longitude": -93.2174},
    {"ftz_number": 124, "zone_name": "Baton Rouge", "grantee": "Greater Baton Rouge Port Commission", "state": "LA", "city": "Baton Rouge", "latitude": 30.4515, "longitude": -91.1871},
    {"ftz_number": 145, "zone_name": "Shreveport", "grantee": "Regional Airport Authority of Shreveport", "state": "LA", "city": "Shreveport", "latitude": 32.5252, "longitude": -93.7502},
    # South Carolina
    {"ftz_number": 21, "zone_name": "Dorchester County", "grantee": "South Carolina State Ports Authority", "state": "SC", "city": "Charleston", "latitude": 32.7765, "longitude": -79.9311},
    {"ftz_number": 38, "zone_name": "Spartanburg County", "grantee": "South Carolina State Ports Authority", "state": "SC", "city": "Spartanburg", "latitude": 34.9496, "longitude": -81.9320},
    {"ftz_number": 127, "zone_name": "Columbia", "grantee": "Columbia Metropolitan Airport", "state": "SC", "city": "Columbia", "latitude": 34.0007, "longitude": -81.0348},
    # Tennessee
    {"ftz_number": 77, "zone_name": "Memphis", "grantee": "Memphis and Shelby County Port Commission", "state": "TN", "city": "Memphis", "latitude": 35.1495, "longitude": -90.0490},
    {"ftz_number": 78, "zone_name": "Nashville", "grantee": "Metropolitan Government of Nashville and Davidson County", "state": "TN", "city": "Nashville", "latitude": 36.1627, "longitude": -86.7816},
    {"ftz_number": 134, "zone_name": "Chattanooga", "grantee": "City of Chattanooga", "state": "TN", "city": "Chattanooga", "latitude": 35.0456, "longitude": -85.3097},
    # Indiana
    {"ftz_number": 72, "zone_name": "Indianapolis", "grantee": "Indianapolis Airport Authority", "state": "IN", "city": "Indianapolis", "latitude": 39.7684, "longitude": -86.1581},
    {"ftz_number": 125, "zone_name": "South Bend", "grantee": "St. Joseph County Airport Authority", "state": "IN", "city": "South Bend", "latitude": 41.6764, "longitude": -86.2520},
    {"ftz_number": 182, "zone_name": "Fort Wayne", "grantee": "Fort Wayne-Allen County Airport Authority", "state": "IN", "city": "Fort Wayne", "latitude": 41.0793, "longitude": -85.1394},
    # Colorado
    {"ftz_number": 123, "zone_name": "Denver", "grantee": "City and County of Denver", "state": "CO", "city": "Denver", "latitude": 39.7392, "longitude": -104.9903},
    {"ftz_number": 126, "zone_name": "Weld County", "grantee": "Colorado Springs Foreign Trade Zone Inc", "state": "CO", "city": "Greeley", "latitude": 40.4233, "longitude": -104.7091},
    # Missouri
    {"ftz_number": 15, "zone_name": "Kansas City", "grantee": "Greater Kansas City Foreign Trade Zone Inc", "state": "MO", "city": "Kansas City", "latitude": 39.0997, "longitude": -94.5786},
    {"ftz_number": 102, "zone_name": "St. Louis", "grantee": "St. Louis County Port Authority", "state": "MO", "city": "St. Louis", "latitude": 38.6270, "longitude": -90.1994},
    # Maryland
    {"ftz_number": 63, "zone_name": "Prince George's County", "grantee": "Prince George's County Economic Development Corporation", "state": "MD", "city": "Upper Marlboro", "latitude": 38.8159, "longitude": -76.7497},
    {"ftz_number": 73, "zone_name": "Baltimore", "grantee": "Maryland Department of Transportation", "state": "MD", "city": "Baltimore", "latitude": 39.2904, "longitude": -76.6122},
    # Kentucky
    {"ftz_number": 29, "zone_name": "Louisville", "grantee": "Louisville Jefferson County Metro Government", "state": "KY", "city": "Louisville", "latitude": 38.2527, "longitude": -85.7585},
    {"ftz_number": 47, "zone_name": "Boone County", "grantee": "Greater Cincinnati FTZ Inc", "state": "KY", "city": "Boone", "latitude": 38.9650, "longitude": -84.7194},
    # Wisconsin
    {"ftz_number": 41, "zone_name": "Milwaukee", "grantee": "Foreign Trade Zone of Wisconsin Ltd", "state": "WI", "city": "Milwaukee", "latitude": 43.0389, "longitude": -87.9065},
    # Minnesota
    {"ftz_number": 119, "zone_name": "Minneapolis", "grantee": "Greater Metropolitan FTZ Commission", "state": "MN", "city": "Minneapolis", "latitude": 44.9778, "longitude": -93.2650},
    # Alabama
    {"ftz_number": 82, "zone_name": "Mobile", "grantee": "City of Mobile", "state": "AL", "city": "Mobile", "latitude": 30.6954, "longitude": -88.0399},
    {"ftz_number": 83, "zone_name": "Huntsville", "grantee": "Huntsville-Madison County Airport Authority", "state": "AL", "city": "Huntsville", "latitude": 34.7304, "longitude": -86.5861},
    {"ftz_number": 98, "zone_name": "Birmingham", "grantee": "City of Birmingham", "state": "AL", "city": "Birmingham", "latitude": 33.5207, "longitude": -86.8025},
    # Oregon
    {"ftz_number": 45, "zone_name": "Portland", "grantee": "Port of Portland", "state": "OR", "city": "Portland", "latitude": 45.5152, "longitude": -122.6784},
]


@register_collector(SiteIntelSource.FTZ_BOARD)
class FTZBoardCollector(BaseCollector):
    """
    Collector for FTZ Board Foreign Trade Zone data.

    Uses built-in seed data compiled from FTZ Board public records
    since the HIFLD ArcGIS endpoint was decommissioned in August 2025.

    Fetches:
    - FTZ zones with grantee information
    - Zone locations
    """

    domain = SiteIntelDomain.INCENTIVES
    source = SiteIntelSource.FTZ_BOARD

    # Configuration
    default_timeout = 30.0
    rate_limit_delay = 0.1

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://ofis.trade.gov"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute FTZ data collection.

        Loads FTZ data from built-in seed dataset.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            logger.info("Collecting FTZ Board data from seed dataset...")
            ftz_result = await self._collect_ftz_zones(config)
            total_inserted += ftz_result.get("inserted", 0)
            total_processed += ftz_result.get("processed", 0)
            if ftz_result.get("error"):
                errors.append({"source": "ftz_zones", "error": ftz_result["error"]})

            status = CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"FTZ collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_ftz_zones(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect Foreign Trade Zones from seed data.
        """
        try:
            # Filter by state if specified
            zones = FTZ_SEED_DATA
            if config.states:
                zones = [z for z in zones if z.get("state") in config.states]

            logger.info(f"Processing {len(zones)} FTZ records from seed data")

            # Transform records
            records = []
            for zone in zones:
                transformed = self._transform_ftz_record(zone)
                if transformed:
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    ForeignTradeZone,
                    records,
                    unique_columns=["ftz_number"],
                    update_columns=[
                        "zone_name", "grantee", "operator", "state", "city",
                        "latitude", "longitude", "acreage", "status",
                        "activation_date", "source", "collected_at"
                    ],
                )
                logger.info(f"Inserted/updated {inserted} FTZ records")
                return {"processed": len(zones), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect FTZ zones: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_ftz_record(self, zone: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform FTZ seed record to database format."""
        ftz_number = zone.get("ftz_number")
        if not ftz_number:
            return None

        return {
            "ftz_number": int(ftz_number),
            "zone_name": zone.get("zone_name"),
            "grantee": zone.get("grantee"),
            "operator": zone.get("grantee"),  # Use grantee as operator if not specified
            "state": zone.get("state"),
            "city": zone.get("city"),
            "latitude": zone.get("latitude"),
            "longitude": zone.get("longitude"),
            "acreage": zone.get("acreage"),
            "status": "active",
            "activation_date": None,
            "source": "ftz_board_seed",
            "collected_at": datetime.utcnow(),
        }
