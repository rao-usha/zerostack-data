"""
Census Trade Gateway Collector.

Fetches import/export trade data by port and customs district from:
- Census Bureau International Trade API
- USA Trade Online

Data sources:
- Census Bureau API (api.census.gov)
- USA Trade Online statistics

API key required: Get from https://api.census.gov/data/key_signup.html
"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import TradeGatewayStats
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


@register_collector(SiteIntelSource.CENSUS_TRADE)
class CensusTradeCollector(BaseCollector):
    """
    Collector for Census Bureau international trade data.

    Fetches:
    - Import/export values by customs district
    - Top commodities by HS code
    - Trading partner breakdown
    - Transportation mode shares
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.CENSUS_TRADE

    # Census API configuration
    default_timeout = 60.0
    rate_limit_delay = 0.5

    # Census API endpoints
    CENSUS_API_BASE = "https://api.census.gov/data/timeseries/intltrade"

    # Major customs districts
    CUSTOMS_DISTRICTS = {
        "01": {"name": "Portland, ME", "state": "ME"},
        "02": {"name": "St. Albans, VT", "state": "VT"},
        "04": {"name": "Boston, MA", "state": "MA"},
        "05": {"name": "Providence, RI", "state": "RI"},
        "06": {"name": "Hartford, CT", "state": "CT"},
        "10": {"name": "New York, NY", "state": "NY"},
        "11": {"name": "Philadelphia, PA", "state": "PA"},
        "13": {"name": "Baltimore, MD", "state": "MD"},
        "14": {"name": "Norfolk, VA", "state": "VA"},
        "15": {"name": "Charlotte, NC", "state": "NC"},
        "16": {"name": "Charleston, SC", "state": "SC"},
        "17": {"name": "Savannah, GA", "state": "GA"},
        "18": {"name": "Tampa, FL", "state": "FL"},
        "20": {"name": "Miami, FL", "state": "FL"},
        "21": {"name": "San Juan, PR", "state": "PR"},
        "25": {"name": "New Orleans, LA", "state": "LA"},
        "26": {"name": "Houston, TX", "state": "TX"},
        "27": {"name": "Laredo, TX", "state": "TX"},
        "28": {"name": "Dallas-Fort Worth, TX", "state": "TX"},
        "29": {"name": "El Paso, TX", "state": "TX"},
        "30": {"name": "San Diego, CA", "state": "CA"},
        "31": {"name": "Nogales, AZ", "state": "AZ"},
        "32": {"name": "Great Falls, MT", "state": "MT"},
        "35": {"name": "Seattle, WA", "state": "WA"},
        "36": {"name": "Portland, OR", "state": "OR"},
        "33": {"name": "Anchorage, AK", "state": "AK"},
        "38": {"name": "San Francisco, CA", "state": "CA"},
        "37": {"name": "Los Angeles, CA", "state": "CA"},
        "41": {"name": "Milwaukee, WI", "state": "WI"},
        "39": {"name": "Chicago, IL", "state": "IL"},
        "40": {"name": "Cleveland, OH", "state": "OH"},
        "43": {"name": "Detroit, MI", "state": "MI"},
        "42": {"name": "Buffalo, NY", "state": "NY"},
        "45": {"name": "St. Louis, MO", "state": "MO"},
        "46": {"name": "Minneapolis, MN", "state": "MN"},
        "48": {"name": "Denver, CO", "state": "CO"},
    }

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return self.CENSUS_API_BASE

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute Census trade data collection.

        Collects import/export statistics by customs district.
        """
        try:
            logger.info("Collecting Census international trade data...")

            all_trade = []

            # Collect from Census API
            trade_result = await self._collect_trade_data(config)
            all_trade.extend(trade_result.get("records", []))

            # No sample/random fallback: fabricated rows must never be stored (PLAN_082).
            if not all_trade:
                raise RuntimeError("No data returned from source; refusing to substitute sample data")

            # Transform and insert records
            records = []
            for record in all_trade:
                transformed = self._transform_trade(record)
                if transformed:
                    records.append(transformed)

            logger.info(f"Transformed {len(records)} trade gateway records")

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    TradeGatewayStats,
                    records,
                    unique_columns=["customs_district", "period_year", "period_month"],
                    update_columns=[
                        "district_code",
                        "port_code",
                        "port_name",
                        "import_value_million",
                        "export_value_million",
                        "trade_balance_million",
                        "top_import_hs_codes",
                        "top_export_hs_codes",
                        "top_import_countries",
                        "top_export_countries",
                        "vessel_pct",
                        "air_pct",
                        "truck_pct",
                        "rail_pct",
                        "other_pct",
                        "source",
                        "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(all_trade),
                    processed=len(all_trade),
                    inserted=inserted,
                    sample=records[:3] if records else None,
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(all_trade),
                processed=len(all_trade),
                inserted=0,
            )

        except Exception as e:
            logger.error(f"Census trade collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_trade_data(self, config: CollectionConfig) -> Dict[str, Any]:
        """
        Collect trade data from Census API.
        """
        try:
            client = await self.get_client()
            all_records = []

            await self.apply_rate_limit()

            try:
                # Census International Trade API
                year = config.year or date.today().year - 1
                month = "12"  # Default to December for annual data

                params = {
                    "get": "CTY_CODE,CTY_NAME,DISTRICT,DIST_NAME,GEN_VAL_MO,CON_VAL_MO",
                    "time": f"{year}-{month}",
                }

                if self.api_key:
                    params["key"] = self.api_key

                response = await client.get(
                    f"{self.CENSUS_API_BASE}/imports/district",
                    params=params,
                )

                if response.status_code == 200:
                    data = response.json()
                    # Census API returns array of arrays with header row
                    if isinstance(data, list) and len(data) > 1:
                        headers = data[0]
                        for row in data[1:]:
                            record = dict(zip(headers, row))
                            all_records.append(record)

                    logger.info(f"Fetched {len(all_records)} records from Census API")

            except Exception as e:
                logger.warning(f"Could not fetch from Census API: {e}")

            return {"records": all_records}

        except Exception as e:
            logger.error(f"Failed to collect trade data: {e}", exc_info=True)
            return {"records": [], "error": str(e)}

    def _transform_trade(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw trade data to database format."""
        district = record.get("customs_district") or record.get("DIST_NAME")
        if not district:
            return None

        period_year = self._safe_int(record.get("period_year") or record.get("YEAR"))
        period_month = self._safe_int(
            record.get("period_month") or record.get("MONTH") or 12
        )

        if not period_year:
            return None

        return {
            "customs_district": district,
            "district_code": record.get("district_code") or record.get("DISTRICT"),
            "port_code": record.get("port_code"),
            "port_name": record.get("port_name"),
            "period_year": period_year,
            "period_month": period_month,
            "import_value_million": self._safe_float(
                record.get("import_value_million") or record.get("GEN_VAL_MO")
            ),
            "export_value_million": self._safe_float(
                record.get("export_value_million") or record.get("CON_VAL_MO")
            ),
            "trade_balance_million": self._safe_float(
                record.get("trade_balance_million")
            ),
            "top_import_hs_codes": record.get("top_import_hs_codes"),
            "top_export_hs_codes": record.get("top_export_hs_codes"),
            "top_import_countries": record.get("top_import_countries"),
            "top_export_countries": record.get("top_export_countries"),
            "vessel_pct": self._safe_float(record.get("vessel_pct")),
            "air_pct": self._safe_float(record.get("air_pct")),
            "truck_pct": self._safe_float(record.get("truck_pct")),
            "rail_pct": self._safe_float(record.get("rail_pct")),
            "other_pct": self._safe_float(record.get("other_pct")),
            "source": "census",
            "collected_at": datetime.utcnow(),
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
