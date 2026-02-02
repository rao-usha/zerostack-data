"""
PeeringDB Collector.

Fetches internet peering data from PeeringDB API:
- Internet Exchange Points (IX)
- Colocation/Data Center Facilities
- Networks and peering relationships

API Documentation: https://www.peeringdb.com/apidocs/
Rate limit: 100 requests/minute for anonymous, higher with API key.

No API key required for basic access.
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.models_site_intel import InternetExchange, DataCenterFacility
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain, SiteIntelSource, CollectionConfig, CollectionResult, CollectionStatus
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)


@register_collector(SiteIntelSource.PEERINGDB)
class PeeringDBCollector(BaseCollector):
    """
    Collector for PeeringDB internet infrastructure data.

    Fetches:
    - Internet Exchange Points (IX) with participant counts
    - Data Center/Colocation facilities
    """

    domain = SiteIntelDomain.TELECOM
    source = SiteIntelSource.PEERINGDB

    # PeeringDB API configuration
    default_timeout = 60.0
    rate_limit_delay = 0.7  # ~85 req/min to stay under 100/min limit

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)
        if not self.api_key:
            settings = get_settings()
            self.api_key = getattr(settings, 'peeringdb_api_key', None)

    def get_default_base_url(self) -> str:
        return "https://www.peeringdb.com/api"

    def get_default_headers(self) -> Dict[str, str]:
        headers = {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Api-Key {self.api_key}"
        return headers

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute PeeringDB data collection.

        Collects IX points and data center facilities.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            # Collect Internet Exchanges
            logger.info("Collecting PeeringDB Internet Exchange data...")
            ix_result = await self._collect_internet_exchanges(config)
            total_inserted += ix_result.get("inserted", 0)
            total_processed += ix_result.get("processed", 0)
            if ix_result.get("error"):
                errors.append({"source": "internet_exchanges", "error": ix_result["error"]})

            # Collect Data Center Facilities
            logger.info("Collecting PeeringDB Data Center facility data...")
            fac_result = await self._collect_facilities(config)
            total_inserted += fac_result.get("inserted", 0)
            total_processed += fac_result.get("processed", 0)
            if fac_result.get("error"):
                errors.append({"source": "facilities", "error": fac_result["error"]})

            status = CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"PeeringDB collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_internet_exchanges(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect Internet Exchange points from PeeringDB.

        Uses /ix endpoint for exchange data.
        """
        try:
            # PeeringDB uses country codes, so we filter by US
            params = {
                "country": "US",
                "status": "ok",  # Only active IXs
            }

            response = await self.fetch_json("/ix", params=params)
            data = response.get("data", [])

            logger.info(f"Fetched {len(data)} IX records from PeeringDB")

            # Get network counts for each IX
            ix_net_counts = await self._get_ix_network_counts()

            # Transform records
            records = []
            for ix in data:
                transformed = self._transform_ix_record(ix, ix_net_counts)
                if transformed:
                    # Filter by state if specified
                    if config.states and transformed.get("state"):
                        if transformed["state"] not in config.states:
                            continue
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    InternetExchange,
                    records,
                    unique_columns=["peeringdb_id"],
                    update_columns=[
                        "name", "name_long", "city", "state", "country",
                        "latitude", "longitude", "website", "network_count",
                        "ipv4_prefixes", "ipv6_prefixes", "collected_at"
                    ],
                )
                return {"processed": len(data), "inserted": inserted}

            return {"processed": len(data), "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect internet exchanges: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    async def _get_ix_network_counts(self) -> Dict[int, int]:
        """Get network participant counts for each IX."""
        try:
            # Get network-IX relationships
            response = await self.fetch_json("/netixlan", params={"country": "US"})
            data = response.get("data", [])

            # Count networks per IX
            counts = {}
            for record in data:
                ix_id = record.get("ix_id")
                if ix_id:
                    counts[ix_id] = counts.get(ix_id, 0) + 1

            return counts
        except Exception as e:
            logger.warning(f"Could not fetch IX network counts: {e}")
            return {}

    def _transform_ix_record(self, record: Dict[str, Any], net_counts: Dict[int, int]) -> Optional[Dict[str, Any]]:
        """Transform PeeringDB IX record to database format."""
        pdb_id = record.get("id")
        if not pdb_id:
            return None

        # Parse city/state from location fields
        city = record.get("city")
        state = None
        region = record.get("region_continent")

        # PeeringDB doesn't always have state, try to extract from name or notes
        name = record.get("name", "")
        name_long = record.get("name_long", "")

        # Common IX naming patterns include state abbreviation
        state_abbrevs = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
        ]

        # Try to find state in city field (e.g., "Ashburn, VA")
        if city and "," in city:
            parts = city.split(",")
            if len(parts) >= 2:
                potential_state = parts[-1].strip().upper()
                if potential_state in state_abbrevs:
                    state = potential_state
                    city = parts[0].strip()

        return {
            "peeringdb_id": pdb_id,
            "name": name,
            "name_long": name_long,
            "city": city,
            "state": state,
            "country": record.get("country", "US"),
            "latitude": self._safe_float(record.get("latitude")),
            "longitude": self._safe_float(record.get("longitude")),
            "website": record.get("website"),
            "network_count": net_counts.get(pdb_id, 0),
            "ipv4_prefixes": record.get("proto_ipv4"),
            "ipv6_prefixes": record.get("proto_ipv6"),
            "source": "peeringdb",
            "collected_at": datetime.utcnow(),
        }

    async def _collect_facilities(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect data center/colocation facilities from PeeringDB.

        Uses /fac endpoint for facility data.
        """
        try:
            params = {
                "country": "US",
                "status": "ok",
            }

            response = await self.fetch_json("/fac", params=params)
            data = response.get("data", [])

            logger.info(f"Fetched {len(data)} facility records from PeeringDB")

            # Transform records
            records = []
            for fac in data:
                transformed = self._transform_facility_record(fac)
                if transformed:
                    # Filter by state if specified
                    if config.states and transformed.get("state"):
                        if transformed["state"] not in config.states:
                            continue
                    records.append(transformed)

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    DataCenterFacility,
                    records,
                    unique_columns=["peeringdb_fac_id"],
                    update_columns=[
                        "name", "city", "state", "country", "address",
                        "latitude", "longitude", "website", "owner_org",
                        "available_sqft", "colo_type", "collected_at"
                    ],
                )
                return {"processed": len(data), "inserted": inserted}

            return {"processed": len(data), "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect facilities: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_facility_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform PeeringDB facility record to database format."""
        pdb_id = record.get("id")
        if not pdb_id:
            return None

        # Get state from record or parse from address
        state = record.get("state")
        city = record.get("city")

        return {
            "peeringdb_fac_id": pdb_id,
            "name": record.get("name"),
            "city": city,
            "state": state,
            "country": record.get("country", "US"),
            "address": record.get("address1"),
            "latitude": self._safe_float(record.get("latitude")),
            "longitude": self._safe_float(record.get("longitude")),
            "website": record.get("website"),
            "owner_org": record.get("org_name"),
            "available_sqft": self._safe_int(record.get("available_voltage")),  # PeeringDB doesn't always have sqft
            "colo_type": self._determine_colo_type(record),
            "source": "peeringdb",
            "collected_at": datetime.utcnow(),
        }

    def _determine_colo_type(self, record: Dict[str, Any]) -> str:
        """Determine colocation type from facility data."""
        name = (record.get("name") or "").lower()
        notes = (record.get("notes") or "").lower()

        if "carrier" in name or "carrier" in notes:
            return "carrier_neutral"
        elif "enterprise" in name or "enterprise" in notes:
            return "enterprise"
        elif "hyperscale" in name or "hyperscale" in notes:
            return "hyperscale"
        else:
            return "colocation"

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
