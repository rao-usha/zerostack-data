"""
Drewry World Container Index (WCI) Collector.

Fetches container freight rate indices from Drewry:
- Weekly WCI composite index
- Route-specific rates

Data sources:
- Drewry WCI public data

No API key required for public index data.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import ContainerFreightIndex
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


@register_collector(SiteIntelSource.DREWRY)
class DrewryCollector(BaseCollector):
    """
    Collector for Drewry World Container Index (WCI).

    The Drewry WCI tracks container shipping rates on major trade routes.
    Published weekly, it provides a benchmark for container freight markets.
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.DREWRY

    # Drewry configuration
    default_timeout = 60.0
    rate_limit_delay = 1.0

    # Drewry WCI routes
    WCI_ROUTES = {
        "WCI_COMPOSITE": {
            "name": "Drewry WCI Composite",
            "origin_region": "Global",
            "origin_port": "Composite",
            "destination_region": "Global",
            "destination_port": "Composite",
            "container_type": "40ft",
        },
        "WCI_SHA_RTM": {
            "name": "Shanghai to Rotterdam",
            "origin_region": "China",
            "origin_port": "Shanghai",
            "destination_region": "North Europe",
            "destination_port": "Rotterdam",
            "container_type": "40ft",
        },
        "WCI_RTM_SHA": {
            "name": "Rotterdam to Shanghai",
            "origin_region": "North Europe",
            "origin_port": "Rotterdam",
            "destination_region": "China",
            "destination_port": "Shanghai",
            "container_type": "40ft",
        },
        "WCI_SHA_GEN": {
            "name": "Shanghai to Genoa",
            "origin_region": "China",
            "origin_port": "Shanghai",
            "destination_region": "Mediterranean",
            "destination_port": "Genoa",
            "container_type": "40ft",
        },
        "WCI_SHA_LAX": {
            "name": "Shanghai to Los Angeles",
            "origin_region": "China",
            "origin_port": "Shanghai",
            "destination_region": "North America",
            "destination_port": "Los Angeles",
            "container_type": "40ft",
        },
        "WCI_LAX_SHA": {
            "name": "Los Angeles to Shanghai",
            "origin_region": "North America",
            "origin_port": "Los Angeles",
            "destination_region": "China",
            "destination_port": "Shanghai",
            "container_type": "40ft",
        },
        "WCI_SHA_NYC": {
            "name": "Shanghai to New York",
            "origin_region": "China",
            "origin_port": "Shanghai",
            "destination_region": "North America",
            "destination_port": "New York",
            "container_type": "40ft",
        },
        "WCI_NYC_RTM": {
            "name": "New York to Rotterdam",
            "origin_region": "North America",
            "origin_port": "New York",
            "destination_region": "North Europe",
            "destination_port": "Rotterdam",
            "container_type": "40ft",
        },
        "WCI_RTM_NYC": {
            "name": "Rotterdam to New York",
            "origin_region": "North Europe",
            "origin_port": "Rotterdam",
            "destination_region": "North America",
            "destination_port": "New York",
            "container_type": "40ft",
        },
    }

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://www.drewry.co.uk"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (compatible; Nexdata-SiteIntel/1.0)",
            "Accept": "application/json, text/html",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute Drewry WCI data collection.

        Collects container freight rate indices for major trade routes.
        """
        try:
            logger.info("Collecting Drewry WCI container freight data...")

            all_rates = []

            # Try to fetch from Drewry public data
            rates_result = await self._collect_wci_rates(config)
            all_rates.extend(rates_result.get("records", []))

            # No sample/random fallback: fabricated rows must never be stored (PLAN_082).
            if not all_rates:
                raise RuntimeError("No data returned from source; refusing to substitute sample data")

            # Transform and insert records
            records = []
            for rate in all_rates:
                transformed = self._transform_rate(rate)
                if transformed:
                    records.append(transformed)

            logger.info(f"Transformed {len(records)} Drewry WCI rate records")

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    ContainerFreightIndex,
                    records,
                    unique_columns=["index_code", "rate_date"],
                    update_columns=[
                        "provider",
                        "route_origin_region",
                        "route_origin_port",
                        "route_destination_region",
                        "route_destination_port",
                        "container_type",
                        "rate_value",
                        "change_pct_wow",
                        "change_pct_mom",
                        "change_pct_yoy",
                        "source",
                        "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(all_rates),
                    processed=len(all_rates),
                    inserted=inserted,
                    sample=records[:3] if records else None,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(all_rates),
                processed=len(all_rates),
                inserted=0,
            )

        except Exception as e:
            logger.error(f"Drewry collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_wci_rates(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect WCI rates from Drewry.

        Note: Drewry public access may be limited.
        For production, would need Drewry subscription access.
        """
        try:
            client = await self.get_client()
            all_records = []

            await self.apply_rate_limit()

            try:
                # Try Drewry's public WCI page
                response = await client.get(
                    "/research/world-container-index-weekly-update",
                )

                if response.status_code == 200:
                    # Would parse HTML for rate data
                    # For now, return empty and use sample data
                    pass

            except Exception as e:
                logger.warning(f"Could not fetch from Drewry: {e}")

            return {"records": all_records}

        except Exception as e:
            logger.error(f"Failed to collect WCI rates: {e}", exc_info=True)
            return {"records": [], "error": str(e)}

    def _transform_rate(self, rate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw rate data to database format."""
        index_code = rate.get("index_code")
        if not index_code:
            return None

        rate_date = rate.get("rate_date")
        if isinstance(rate_date, str):
            try:
                rate_date = datetime.strptime(rate_date[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                rate_date = date.today()
        elif not isinstance(rate_date, date):
            rate_date = date.today()

        route_info = self.WCI_ROUTES.get(index_code, {})

        return {
            "index_code": index_code,
            "provider": "drewry",
            "route_origin_region": rate.get("route_origin_region")
            or route_info.get("origin_region"),
            "route_origin_port": rate.get("route_origin_port")
            or route_info.get("origin_port"),
            "route_destination_region": rate.get("route_destination_region")
            or route_info.get("destination_region"),
            "route_destination_port": rate.get("route_destination_port")
            or route_info.get("destination_port"),
            "container_type": rate.get("container_type")
            or route_info.get("container_type", "40ft"),
            "rate_value": self._parse_rate(rate.get("rate_value")),
            "rate_date": rate_date,
            "change_pct_wow": self._parse_rate(rate.get("change_pct_wow")),
            "change_pct_mom": self._parse_rate(rate.get("change_pct_mom")),
            "change_pct_yoy": self._parse_rate(rate.get("change_pct_yoy")),
            "source": "drewry_wci",
            "collected_at": datetime.utcnow(),
        }

    def _parse_rate(self, value: Any) -> Optional[float]:
        """Parse rate value to float."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                cleaned = (
                    value.replace("$", "").replace(",", "").replace("%", "").strip()
                )
                return float(cleaned)
            except (ValueError, TypeError):
                return None
        return None
