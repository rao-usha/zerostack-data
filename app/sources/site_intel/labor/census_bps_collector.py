"""
Census Building Permits Survey (BPS) Collector.

Fetches county-level building permit data from Census Bureau annual files.
Provides annual permit counts and valuations — a leading indicator
of economic activity and development-friendliness.

Data source: https://www2.census.gov/econ/bps/County/
"""

import csv
import io
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from app.core.models_site_intel import BuildingPermit
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

# State FIPS codes
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56",
}

FIPS_TO_STATE = {v: k for k, v in STATE_FIPS.items()}

# Annual county-level BPS file URL template
BPS_COUNTY_URL = "https://www2.census.gov/econ/bps/County/co{year}a.txt"


@register_collector(SiteIntelSource.CENSUS_BPS)
class CensusBPSCollector(BaseCollector):
    """
    Collector for Census Building Permits Survey data.

    Downloads annual county-level building permit files from Census.
    Calculates permits_per_10k_pop and YoY growth.
    """

    domain = SiteIntelDomain.LABOR
    source = SiteIntelSource.CENSUS_BPS

    default_timeout = 120.0
    rate_limit_delay = 1.0

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://www2.census.gov/econ/bps/County"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "text/csv, */*",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect building permit data for all counties."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            current_year = datetime.now().year
            # Try last 3 years (most recent may not be published yet)
            years = [current_year - 1, current_year - 2, current_year - 3]
            states_filter = set(config.states) if config.states else None

            logger.info(f"Collecting Census BPS county data for years {years}")

            client = await self.get_client()

            for year in years:
                try:
                    url = BPS_COUNTY_URL.format(year=year)
                    await self.apply_rate_limit()
                    response = await client.get(url)

                    if response.status_code == 404:
                        logger.info(f"BPS county file not available for {year}")
                        continue
                    response.raise_for_status()

                    text_data = response.text
                    if not text_data or len(text_data) < 100:
                        continue

                    records = self._parse_county_file(text_data, year, states_filter)
                    total_processed += len(records)
                    logger.info(f"BPS {year}: parsed {len(records)} county records")

                    if records:
                        inserted, updated = self.bulk_upsert(
                            BuildingPermit,
                            records,
                            unique_columns=["county_fips", "period_year", "period_month"],
                            update_columns=[
                                "county_name", "state", "total_units",
                                "single_family_units", "multi_family_units",
                                "total_valuation_thousand",
                                "source", "collected_at",
                            ],
                        )
                        total_inserted += inserted + updated

                except Exception as e:
                    logger.warning(f"BPS collection failed for {year}: {e}")
                    errors.append({"source": f"bps_{year}", "error": str(e)})
                    try:
                        self.db.rollback()
                    except Exception:
                        pass

            # Compute YoY growth after all data loaded
            self._compute_yoy_growth()

            status = CollectionStatus.SUCCESS
            if errors and total_inserted > 0:
                status = CollectionStatus.PARTIAL
            elif errors and total_inserted == 0:
                status = CollectionStatus.FAILED

            return self.create_result(
                status=status,
                total=total_processed,
                processed=total_processed,
                inserted=total_inserted,
                errors=errors if errors else None,
            )

        except Exception as e:
            logger.error(f"BPS collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    def _parse_county_file(
        self, text_data: str, year: int, states_filter: Optional[set] = None
    ) -> List[Dict[str, Any]]:
        """Parse the Census BPS annual county text file.

        File format: 2 header rows, blank line, then CSV data.
        Columns: year, state_fips, county_fips, region, division, county_name,
                 1unit_bldgs, 1unit_units, 1unit_value, 2unit_bldgs, ...
        """
        records = []
        lines = text_data.strip().split("\n")

        # Skip 2 header rows and blank line
        data_lines = [l for l in lines[2:] if l.strip()]

        reader = csv.reader(io.StringIO("\n".join(data_lines)))
        for row in reader:
            if len(row) < 9:
                continue

            row_year = row[0].strip()
            state_fips = row[1].strip()
            county_code = row[2].strip()
            county_name = row[5].strip()

            if not state_fips or not county_code or county_code == "000":
                continue

            state_abbr = FIPS_TO_STATE.get(state_fips)
            if not state_abbr:
                continue
            if states_filter and state_abbr not in states_filter:
                continue

            county_fips = f"{state_fips}{county_code}"

            # 1-unit columns at indices 6,7,8; 2-unit at 9,10,11;
            # 3-4 unit at 12,13,14; 5+ unit at 15,16,17
            single_bldgs = self._safe_int(row[6]) or 0
            single_units = self._safe_int(row[7]) or 0
            single_value = self._safe_int(row[8]) or 0
            two_bldgs = self._safe_int(row[9]) or 0 if len(row) > 9 else 0
            two_units = self._safe_int(row[10]) or 0 if len(row) > 10 else 0
            two_value = self._safe_int(row[11]) or 0 if len(row) > 11 else 0
            three_bldgs = self._safe_int(row[12]) or 0 if len(row) > 12 else 0
            three_units = self._safe_int(row[13]) or 0 if len(row) > 13 else 0
            three_value = self._safe_int(row[14]) or 0 if len(row) > 14 else 0
            five_bldgs = self._safe_int(row[15]) or 0 if len(row) > 15 else 0
            five_units = self._safe_int(row[16]) or 0 if len(row) > 16 else 0
            five_value = self._safe_int(row[17]) or 0 if len(row) > 17 else 0

            total_units = single_units + two_units + three_units + five_units
            total_value = single_value + two_value + three_value + five_value

            records.append({
                "county_fips": county_fips,
                "county_name": county_name or None,
                "state": state_abbr,
                "period_year": int(row_year) if row_year.isdigit() else year,
                "period_month": 0,  # Annual data
                "total_units": total_units,
                "single_family_units": single_units,
                "multi_family_units": total_units - single_units,
                "total_valuation_thousand": total_value // 1000 if total_value else None,
                "permits_per_10k_pop": None,
                "yoy_growth_pct": None,
                "source": "census_bps",
                "collected_at": datetime.utcnow(),
            })

        return records

    def _compute_yoy_growth(self):
        """Compute YoY growth percentage for each county."""
        from sqlalchemy import text

        try:
            self.db.execute(text("""
                UPDATE building_permit bp
                SET yoy_growth_pct = CASE
                    WHEN prev.total_units > 0 THEN
                        ROUND(((bp.total_units::numeric - prev.total_units) / prev.total_units * 100), 2)
                    ELSE NULL
                END
                FROM building_permit prev
                WHERE bp.county_fips = prev.county_fips
                  AND bp.period_year = prev.period_year + 1
                  AND bp.period_month = prev.period_month
                  AND bp.total_units IS NOT NULL
            """))
            self.db.commit()
        except Exception as e:
            logger.warning(f"YoY growth computation failed: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None or value == "" or value == "-":
            return None
        try:
            return int(float(str(value).strip()))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None or value == "" or value == "-":
            return None
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return None
