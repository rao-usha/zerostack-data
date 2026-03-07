"""
State EDO Certified Sites Collector.

Curated seed data from state Economic Development Organizations:
- Georgia GRAD (Georgia Ready for Accelerated Development)
- Virginia VBRSP (Virginia Business Ready Sites Program)
- North Carolina EDPNC
- Texas Governor's Office / regional EDOs

Populates the existing IndustrialSite model with shovel-ready
certified sites suitable for datacenter development.
No API key required — curated seed data.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import IndustrialSite
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
    CollectionStatus,
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)


# Curated shovel-ready datacenter sites from state EDOs
# These are real certified sites from public state databases
CERTIFIED_SITES: List[Dict[str, Any]] = [
    # Georgia GRAD Sites
    {
        "site_name": "Stanton Springs North",
        "site_type": "greenfield",
        "city": "Social Circle",
        "state": "GA",
        "county": "Newton",
        "latitude": 33.65,
        "longitude": -83.72,
        "acreage": 1200,
        "zoning": "Heavy Industrial",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": True, "gas": True},
        "rail_served": True,
        "highway_access": "I-20",
        "edo_name": "Georgia GRAD",
        "listing_url": "https://www.georgia.org/grad-sites",
    },
    {
        "site_name": "Bryan County Megasite",
        "site_type": "greenfield",
        "city": "Ellabell",
        "state": "GA",
        "county": "Bryan",
        "latitude": 32.13,
        "longitude": -81.47,
        "acreage": 2284,
        "zoning": "Heavy Industrial",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": True, "gas": True},
        "rail_served": True,
        "highway_access": "I-16",
        "edo_name": "Georgia GRAD",
        "listing_url": "https://www.georgia.org/grad-sites",
    },
    # Virginia VBRSP Sites
    {
        "site_name": "New River Valley Commerce Park",
        "site_type": "greenfield",
        "city": "Dublin",
        "state": "VA",
        "county": "Pulaski",
        "latitude": 37.10,
        "longitude": -80.72,
        "acreage": 394,
        "zoning": "Industrial",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": True, "gas": True},
        "rail_served": True,
        "highway_access": "I-81",
        "edo_name": "Virginia VBRSP",
        "listing_url": "https://www.vedp.org/virginia-business-ready-sites-program",
    },
    {
        "site_name": "Pocahontas Parkway Business Park",
        "site_type": "greenfield",
        "city": "Chester",
        "state": "VA",
        "county": "Chesterfield",
        "latitude": 37.35,
        "longitude": -77.40,
        "acreage": 325,
        "zoning": "Industrial / Technology",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": True, "gas": True},
        "rail_served": False,
        "highway_access": "I-95 / VA-288",
        "edo_name": "Virginia VBRSP",
        "listing_url": "https://www.vedp.org/virginia-business-ready-sites-program",
    },
    {
        "site_name": "Mecklenburg County Mega Site",
        "site_type": "greenfield",
        "city": "South Hill",
        "state": "VA",
        "county": "Mecklenburg",
        "latitude": 36.73,
        "longitude": -78.13,
        "acreage": 1500,
        "zoning": "Heavy Industrial",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": True, "gas": True},
        "rail_served": True,
        "highway_access": "I-85",
        "edo_name": "Virginia VBRSP",
        "listing_url": "https://www.vedp.org/virginia-business-ready-sites-program",
    },
    # North Carolina EDPNC
    {
        "site_name": "Chatham Advanced Manufacturing Site",
        "site_type": "greenfield",
        "city": "Siler City",
        "state": "NC",
        "county": "Chatham",
        "latitude": 35.72,
        "longitude": -79.46,
        "acreage": 1802,
        "zoning": "Industrial",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": True, "gas": True},
        "rail_served": True,
        "highway_access": "US-421 / US-64",
        "edo_name": "EDPNC",
        "listing_url": "https://edpnc.com/certified-sites/",
    },
    {
        "site_name": "Triangle North Industrial",
        "site_type": "greenfield",
        "city": "Butner",
        "state": "NC",
        "county": "Granville",
        "latitude": 36.13,
        "longitude": -78.75,
        "acreage": 350,
        "zoning": "Industrial",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": True, "gas": True},
        "rail_served": False,
        "highway_access": "I-85",
        "edo_name": "EDPNC",
        "listing_url": "https://edpnc.com/certified-sites/",
    },
    # Texas
    {
        "site_name": "Temple-Belton Industrial Park",
        "site_type": "greenfield",
        "city": "Temple",
        "state": "TX",
        "county": "Bell",
        "latitude": 31.10,
        "longitude": -97.34,
        "acreage": 500,
        "zoning": "Heavy Industrial",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": True, "gas": True},
        "rail_served": True,
        "highway_access": "I-35",
        "edo_name": "Texas Governor's Office",
        "listing_url": "https://gov.texas.gov/business/page/texas-enterprise-fund",
    },
    {
        "site_name": "Lubbock Business Park",
        "site_type": "greenfield",
        "city": "Lubbock",
        "state": "TX",
        "county": "Lubbock",
        "latitude": 33.57,
        "longitude": -101.85,
        "acreage": 680,
        "zoning": "Industrial",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": False, "gas": True},
        "rail_served": True,
        "highway_access": "I-27 / US-84",
        "edo_name": "Lubbock Economic Development Alliance",
        "listing_url": "https://lubbockeda.org/",
    },
    {
        "site_name": "Midlothian Business Park",
        "site_type": "greenfield",
        "city": "Midlothian",
        "state": "TX",
        "county": "Ellis",
        "latitude": 32.48,
        "longitude": -96.99,
        "acreage": 300,
        "zoning": "Industrial / Data Center",
        "utilities_available": {"electric": True, "water": True, "sewer": True, "fiber": True, "gas": True},
        "rail_served": False,
        "highway_access": "US-287 / US-67",
        "edo_name": "Midlothian Economic Development",
        "listing_url": "https://www.midlothian.tx.us/",
    },
]


@register_collector(SiteIntelSource.STATE_EDO_SITES)
class StateEDOSitesCollector(BaseCollector):
    """
    Collector for state EDO certified industrial sites.

    Inserts curated shovel-ready sites from GA GRAD, VA VBRSP,
    NC EDPNC, and Texas EDOs into the IndustrialSite model.
    """

    domain = SiteIntelDomain.INCENTIVES
    source = SiteIntelSource.STATE_EDO_SITES

    default_timeout = 30.0
    rate_limit_delay = 0.0  # No HTTP calls — seed data only

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://localhost"  # No real API — seed data

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Insert curated EDO certified sites."""
        try:
            sites = CERTIFIED_SITES
            if config.states:
                sites = [s for s in sites if s["state"] in config.states]

            logger.info(f"Inserting {len(sites)} state EDO certified sites...")

            records = []
            for site in sites:
                records.append({
                    "site_name": site["site_name"],
                    "site_type": site["site_type"],
                    "address": None,
                    "city": site["city"],
                    "state": site["state"],
                    "county": site["county"],
                    "latitude": site["latitude"],
                    "longitude": site["longitude"],
                    "acreage": site["acreage"],
                    "building_sqft": None,
                    "available_sqft": None,
                    "asking_price": None,
                    "asking_price_per_sqft": None,
                    "zoning": site["zoning"],
                    "utilities_available": site["utilities_available"],
                    "rail_served": site.get("rail_served"),
                    "highway_access": site.get("highway_access"),
                    "edo_name": site["edo_name"],
                    "contact_email": None,
                    "contact_phone": None,
                    "listing_url": site.get("listing_url"),
                    "source": "state_edo_sites",
                    "collected_at": datetime.utcnow(),
                })

            if records:
                inserted, updated = self.null_preserving_upsert(
                    IndustrialSite,
                    records,
                    unique_columns=["site_name", "state"],
                    update_columns=[
                        "site_type", "city", "county", "latitude", "longitude",
                        "acreage", "zoning", "utilities_available",
                        "rail_served", "highway_access", "edo_name",
                        "listing_url", "source", "collected_at",
                    ],
                )
                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(records),
                    processed=len(records),
                    inserted=inserted + updated,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=0,
                processed=0,
                inserted=0,
            )

        except Exception as e:
            logger.error(f"State EDO sites collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )
