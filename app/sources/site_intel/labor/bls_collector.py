"""
BLS Labor Market Collector.

Fetches labor market data from Bureau of Labor Statistics API v2:
- Employment and unemployment by area
- Occupational employment and wages (OES)
- Quarterly Census of Employment and Wages (QCEW)

API Documentation: https://www.bls.gov/developers/
Rate limits: 25 req/day without key, 500 req/day with key.

Requires BLS_API_KEY environment variable for higher limits.
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.models_site_intel import LaborMarketArea, OccupationalWage
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


# BLS Series ID prefixes
# LAUS = Local Area Unemployment Statistics
# OEUS = Occupational Employment Statistics
# ENUXXXXXX = QCEW by area

# State FIPS codes for BLS area codes
STATE_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
}


@register_collector(SiteIntelSource.BLS)
class BLSLaborCollector(BaseCollector):
    """
    Collector for BLS labor market data.

    Fetches:
    - State/MSA unemployment rates (LAUS)
    - Occupational employment and wages (OES)
    """

    domain = SiteIntelDomain.LABOR
    source = SiteIntelSource.BLS

    # BLS API configuration
    default_timeout = 60.0
    rate_limit_delay = 2.0  # Be conservative with BLS API

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)
        if not self.api_key:
            settings = get_settings()
            self.api_key = getattr(settings, "bls_api_key", None)

    def get_default_base_url(self) -> str:
        return "https://api.bls.gov/publicAPI/v2"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "User-Agent": "Nexdata-SiteIntel/1.0",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute BLS data collection.

        Collects labor market areas and occupational wages.
        """
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            # Collect labor market area statistics
            logger.info("Collecting BLS labor market area data...")
            lma_result = await self._collect_labor_market_areas(config)
            total_inserted += lma_result.get("inserted", 0)
            total_processed += lma_result.get("processed", 0)
            if lma_result.get("error"):
                errors.append(
                    {"source": "labor_market_areas", "error": lma_result["error"]}
                )

            # Collect occupational wages
            logger.info("Collecting BLS occupational wage data...")
            wage_result = await self._collect_occupational_wages(config)
            total_inserted += wage_result.get("inserted", 0)
            total_processed += wage_result.get("processed", 0)
            if wage_result.get("error"):
                errors.append(
                    {"source": "occupational_wages", "error": wage_result["error"]}
                )

            status = (
                CollectionStatus.SUCCESS if not errors else CollectionStatus.PARTIAL
            )

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"BLS collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _collect_labor_market_areas(
        self, config: CollectionConfig
    ) -> Dict[str, Any]:
        """
        Collect labor market area unemployment data from LAUS.

        Uses state-level unemployment series.
        """
        try:
            # Determine which states to collect
            states = config.states if config.states else list(STATE_FIPS.keys())

            # Build series IDs for each state
            # LAUS format: LAUSTxxxxxx00000003 (state unemployment rate)
            series_ids = []
            state_map = {}
            for state in states:
                fips = STATE_FIPS.get(state)
                if fips:
                    # Unemployment rate series
                    series_id = f"LASST{fips}0000000000003"
                    series_ids.append(series_id)
                    state_map[series_id] = state

            if not series_ids:
                return {"processed": 0, "inserted": 0}

            # BLS API limits to 50 series per request
            all_data = []
            current_year = datetime.now().year

            for i in range(0, len(series_ids), 50):
                batch = series_ids[i : i + 50]
                response = await self._fetch_bls_timeseries(
                    batch,
                    start_year=current_year - 2,
                    end_year=current_year,
                )

                series_data = response.get("Results", {}).get("series", [])
                for series in series_data:
                    series_id = series.get("seriesID")
                    state = state_map.get(series_id)
                    if state:
                        for data_point in series.get("data", []):
                            all_data.append(
                                {
                                    "series_id": series_id,
                                    "state": state,
                                    "data": data_point,
                                }
                            )

            logger.info(f"Fetched {len(all_data)} labor market data points")

            # Transform and deduplicate (keep latest per state)
            records_by_state = {}
            for item in all_data:
                state = item["state"]
                data = item["data"]

                period = data.get("period", "")

                # Only use annual averages (M13) or latest monthly
                if period == "M13" or (state not in records_by_state):
                    records_by_state[state] = self._transform_labor_market_area(
                        state, data
                    )

            records = list(records_by_state.values())

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    LaborMarketArea,
                    records,
                    unique_columns=["area_type", "area_code"],
                    update_columns=[
                        "area_name",
                        "state",
                        "unemployment_rate",
                        "labor_force",
                        "employment",
                        "collected_at",
                    ],
                )
                return {"processed": len(all_data), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect labor market areas: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_labor_market_area(
        self, state: str, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Transform BLS LAUS data to database format."""
        fips = STATE_FIPS.get(state, "00")

        return {
            "area_code": f"ST{fips}",
            "area_name": state,
            "area_type": "state",
            "state": state,
            "unemployment_rate": self._safe_float(data.get("value")),
            "labor_force": None,  # Would need separate series
            "employment": None,
            "source": "bls_laus",
            "collected_at": datetime.utcnow(),
        }

    async def _collect_occupational_wages(
        self, config: CollectionConfig
    ) -> Dict[str, Any]:
        """
        Collect occupational employment and wage data (OES).

        Focuses on key occupations for industrial/data center sites.
        """
        try:
            # Key occupation codes for site selection
            # SOC codes for relevant occupations
            key_occupations = [
                ("47-2111", "Electricians"),
                ("49-9071", "Maintenance Workers, Machinery"),
                ("49-1011", "Mechanics Supervisors"),
                ("53-7062", "Laborers, Freight/Stock"),
                ("43-5071", "Shipping Clerks"),
                ("15-1252", "Software Developers"),
                ("15-1244", "Network Administrators"),
                ("17-2071", "Electrical Engineers"),
                ("11-3021", "Computer/IS Managers"),
            ]

            # Determine which states to collect
            states = config.states if config.states else list(STATE_FIPS.keys())

            all_records = []

            for state in states:
                fips = STATE_FIPS.get(state)
                if not fips:
                    continue

                # Build series IDs for this state
                # OES series ID: OEUS + area(7) + industry(6) + occ(6) + datatype(2) = 25 chars
                # area = state_fips + "00000" (statewide), industry = "000000" (all)
                # datatype 13 = annual mean wage
                series_ids = []
                occ_map = {}

                for occ_code, occ_name in key_occupations:
                    # Mean annual wage series
                    clean_code = occ_code.replace("-", "")
                    series_id = (
                        f"OEUS{fips}00000000000{clean_code}13"
                    )
                    series_ids.append(series_id)
                    occ_map[series_id] = (occ_code, occ_name)

                if not series_ids:
                    continue

                try:
                    response = await self._fetch_bls_timeseries(
                        series_ids,
                        start_year=datetime.now().year - 3,  # OES releases with ~1yr lag
                        end_year=datetime.now().year,
                    )

                    series_data = response.get("Results", {}).get("series", [])
                    for series in series_data:
                        series_id = series.get("seriesID")
                        occ_info = occ_map.get(series_id)
                        if occ_info:
                            occ_code, occ_name = occ_info
                            for data_point in series.get("data", []):
                                record = self._transform_occupational_wage(
                                    state, occ_code, occ_name, data_point
                                )
                                if record:
                                    all_records.append(record)

                except Exception as e:
                    logger.warning(f"Failed to fetch OES data for {state}: {e}")
                    continue

            logger.info(f"Fetched {len(all_records)} occupational wage records")

            # Deduplicate by area/occupation/year (keep latest)
            unique_records = {}
            for record in all_records:
                key = f"{record['area_code']}_{record['occupation_code']}_{record['period_year']}"
                if key not in unique_records:
                    unique_records[key] = record

            records = list(unique_records.values())

            # Insert into database
            if records:
                inserted, _ = self.bulk_upsert(
                    OccupationalWage,
                    records,
                    unique_columns=["area_code", "occupation_code", "period_year"],
                    update_columns=[
                        "area_type",
                        "area_name",
                        "occupation_title",
                        "employment",
                        "mean_annual_wage",
                        "collected_at",
                    ],
                )
                return {"processed": len(all_records), "inserted": inserted}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.error(f"Failed to collect occupational wages: {e}", exc_info=True)
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_occupational_wage(
        self, state: str, occ_code: str, occ_name: str, data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Transform BLS OES data to database format."""
        fips = STATE_FIPS.get(state, "00")
        value = self._safe_float(data.get("value"))

        if value is None:
            return None

        return {
            "area_type": "state",
            "area_code": f"ST{fips}",
            "area_name": state,
            "occupation_code": occ_code,
            "occupation_title": occ_name,
            "employment": None,  # Would need separate series
            "mean_annual_wage": value,
            "period_year": int(data.get("year", 0)),
            "source": "bls_oes",
            "collected_at": datetime.utcnow(),
        }

    async def _fetch_bls_timeseries(
        self,
        series_ids: List[str],
        start_year: int,
        end_year: int,
    ) -> Dict[str, Any]:
        """
        Fetch time series data from BLS API.
        """
        client = await self.get_client()
        await self.apply_rate_limit()

        payload = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
        }

        if self.api_key:
            payload["registrationkey"] = self.api_key

        try:
            response = await client.post(
                f"{self.base_url}/timeseries/data/",
                json=payload,
            )
            response.raise_for_status()
            result = response.json()

            # Check for BLS API errors
            if result.get("status") != "REQUEST_SUCCEEDED":
                error_msg = result.get("message", ["Unknown error"])
                logger.warning(f"BLS API warning: {error_msg}")

            return result

        except Exception as e:
            logger.error(f"BLS API request failed: {e}")
            raise

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "" or value == "-":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
