"""
Warehouse Listing Collector.

Fetches active warehouse and industrial property listings from:
- LoopNet (commercial real estate)
- State EDO property databases

Data sources:
- LoopNet public listings (scraping)
- State economic development organizations

No API key required for public listings.
"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import hashlib

from sqlalchemy.orm import Session

from app.core.models_site_intel import WarehouseListing
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


@register_collector(SiteIntelSource.LOOPNET)
class WarehouseListingCollector(BaseCollector):
    """
    Collector for warehouse and industrial property listings.

    Fetches:
    - Active warehouse listings for lease/sale
    - Property specifications (size, dock doors, clear height)
    - Pricing information
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.LOOPNET

    # LoopNet configuration
    default_timeout = 60.0
    rate_limit_delay = 2.0  # Higher delay to avoid blocking

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://www.loopnet.com"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/html",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute warehouse listing collection.

        Collects active warehouse listings from LoopNet and EDO sites.
        """
        try:
            logger.info("Collecting warehouse/industrial listings...")

            all_listings = []

            # Collect from LoopNet
            listings_result = await self._collect_listings(config)
            all_listings.extend(listings_result.get("records", []))

            # No sample/random fallback: fabricated rows must never be stored (PLAN_082).
            if not all_listings:
                raise RuntimeError("No data returned from source; refusing to substitute sample data")

            # Transform and insert records
            records = []
            for listing in all_listings:
                transformed = self._transform_listing(listing)
                if transformed:
                    records.append(transformed)

            logger.info(f"Transformed {len(records)} warehouse listing records")

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    WarehouseListing,
                    records,
                    unique_columns=["listing_id"],
                    update_columns=[
                        "source",
                        "property_name",
                        "listing_type",
                        "property_type",
                        "address",
                        "city",
                        "state",
                        "zip",
                        "latitude",
                        "longitude",
                        "total_sqft",
                        "available_sqft",
                        "min_divisible_sqft",
                        "land_acres",
                        "clear_height_ft",
                        "dock_doors",
                        "drive_in_doors",
                        "column_spacing",
                        "floor_load_capacity",
                        "year_built",
                        "has_rail_spur",
                        "has_cold_storage",
                        "has_freezer",
                        "has_sprinkler",
                        "has_fenced_yard",
                        "trailer_parking_spaces",
                        "asking_rent_psf",
                        "asking_rent_nnn",
                        "asking_price",
                        "listing_date",
                        "broker_name",
                        "broker_company",
                        "broker_phone",
                        "listing_url",
                        "is_active",
                        "updated_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(all_listings),
                    processed=len(all_listings),
                    inserted=inserted,
                    sample=records[:3] if records else None,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(all_listings),
                processed=len(all_listings),
                inserted=0,
            )

        except Exception as e:
            logger.error(f"Warehouse listing collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_listings(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect warehouse listings from LoopNet and EDO sites.

        Note: LoopNet scraping may be blocked. For production, would need
        API access or data partnership.
        """
        try:
            await self.get_client()
            all_records = []

            # Would scrape LoopNet search results
            # For now, return empty and use sample data
            await self.apply_rate_limit()

            return {"records": all_records}

        except Exception as e:
            logger.error(f"Failed to collect listings: {e}", exc_info=True)
            return {"records": [], "error": str(e)}

    def _transform_listing(self, listing: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw listing data to database format."""
        listing_id = listing.get("listing_id")
        if not listing_id:
            return None

        # Parse listing date
        listing_date = listing.get("listing_date")
        if isinstance(listing_date, str):
            try:
                listing_date = datetime.strptime(listing_date[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                listing_date = None
        elif not isinstance(listing_date, date):
            listing_date = None

        return {
            "listing_id": listing_id,
            "source": listing.get("source", "loopnet"),
            "property_name": listing.get("property_name"),
            "listing_type": listing.get("listing_type"),
            "property_type": listing.get("property_type"),
            "address": listing.get("address"),
            "city": listing.get("city"),
            "state": listing.get("state"),
            "zip": listing.get("zip"),
            "latitude": self._safe_float(listing.get("latitude")),
            "longitude": self._safe_float(listing.get("longitude")),
            "total_sqft": self._safe_int(listing.get("total_sqft")),
            "available_sqft": self._safe_int(listing.get("available_sqft")),
            "min_divisible_sqft": self._safe_int(listing.get("min_divisible_sqft")),
            "land_acres": self._safe_float(listing.get("land_acres")),
            "clear_height_ft": self._safe_int(listing.get("clear_height_ft")),
            "dock_doors": self._safe_int(listing.get("dock_doors")),
            "drive_in_doors": self._safe_int(listing.get("drive_in_doors")),
            "column_spacing": listing.get("column_spacing"),
            "floor_load_capacity": listing.get("floor_load_capacity"),
            "year_built": self._safe_int(listing.get("year_built")),
            "has_rail_spur": listing.get("has_rail_spur"),
            "has_cold_storage": listing.get("has_cold_storage"),
            "has_freezer": listing.get("has_freezer"),
            "has_sprinkler": listing.get("has_sprinkler"),
            "has_fenced_yard": listing.get("has_fenced_yard"),
            "trailer_parking_spaces": self._safe_int(
                listing.get("trailer_parking_spaces")
            ),
            "asking_rent_psf": self._safe_float(listing.get("asking_rent_psf")),
            "asking_rent_nnn": listing.get("asking_rent_nnn"),
            "asking_price": self._safe_int(listing.get("asking_price")),
            "listing_date": listing_date,
            "broker_name": listing.get("broker_name"),
            "broker_company": listing.get("broker_company"),
            "broker_phone": listing.get("broker_phone"),
            "listing_url": listing.get("listing_url"),
            "is_active": listing.get("is_active", True),
            "collected_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None


# Need to import timedelta
from datetime import timedelta
