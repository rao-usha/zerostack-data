"""
BLS QCEW (Quarterly Census of Employment and Wages) Collector.

Fetches industry employment data from BLS QCEW CSV API:
- Employment by industry and area
- Wage data by industry
- Establishment counts

API: https://data.bls.gov/cew/data/api/{year}/{quarter}/area/{area_fips}.csv
No API key required. Returns CSV data.

Rate limits: Be respectful — 1-2 req/sec.
"""

import csv
import io
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.orm import Session

from app.core.models_site_intel import IndustryEmployment
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


# State FIPS codes (2-digit) — area FIPS for QCEW is {fips}000 (statewide)
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

# Key NAICS supersectors to filter (own_code=5 = private sector)
# These are 2-digit NAICS codes that matter for site selection
KEY_INDUSTRY_CODES = {
    "10": "Total, All Industries",
    "1011": "Natural Resources and Mining",
    "1012": "Construction",
    "1013": "Manufacturing",
    "1021": "Trade, Transportation, and Utilities",
    "1022": "Information",
    "1023": "Financial Activities",
    "1024": "Professional and Business Services",
    "1025": "Education and Health Services",
    "1026": "Leisure and Hospitality",
    "1027": "Other Services",
    "1029": "Unclassified",
}


@register_collector(SiteIntelSource.BLS_QCEW)
class BLSQCEWCollector(BaseCollector):
    """
    Collector for BLS QCEW industry employment data.

    Fetches quarterly employment/wage data by state and industry via the
    QCEW CSV API. No API key required.
    """

    domain = SiteIntelDomain.LABOR
    source = SiteIntelSource.BLS_QCEW

    default_timeout = 60.0
    rate_limit_delay = 1.0  # Conservative — no API key

    def __init__(self, db: Session, api_key: Optional[str] = None, **kwargs):
        super().__init__(db, api_key, **kwargs)

    def get_default_base_url(self) -> str:
        return "https://data.bls.gov/cew/data/api"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "text/csv, application/csv, */*",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Collect QCEW data for configured states."""
        total_inserted = 0
        total_processed = 0
        errors = []

        try:
            states = config.states if config.states else list(STATE_FIPS.keys())
            logger.info(f"Collecting BLS QCEW data for {len(states)} states...")

            # Find latest available quarter
            year, quarter = await self._find_latest_quarter()
            if not year:
                return self.create_result(
                    status=CollectionStatus.FAILED,
                    error_message="Could not find available QCEW data for any recent quarter",
                )

            logger.info(f"Using QCEW data for {year} Q{quarter}")

            for state in states:
                fips = STATE_FIPS.get(state)
                if not fips:
                    continue

                area_fips = f"{fips}000"  # Statewide
                try:
                    result = await self._collect_state(
                        state, area_fips, year, quarter
                    )
                    total_inserted += result.get("inserted", 0)
                    total_processed += result.get("processed", 0)
                    if result.get("error"):
                        errors.append(
                            {"source": f"qcew_{state}", "error": result["error"]}
                        )
                except Exception as e:
                    logger.warning(f"Failed to collect QCEW for {state}: {e}")
                    errors.append({"source": f"qcew_{state}", "error": str(e)})

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
            logger.error(f"QCEW collection failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )

    async def _find_latest_quarter(self) -> tuple:
        """Find the latest available QCEW quarter by probing the API.

        Tries Q3→Q2→Q1 of current year-1, then falls back to year-2.
        Uses Texas (48000) as the probe state since it always has data.
        """
        current_year = datetime.now().year

        for year in [current_year - 1, current_year - 2]:
            for quarter in [3, 2, 1]:
                url = f"{self.base_url}/{year}/{quarter}/area/48000.csv"
                try:
                    client = await self.get_client()
                    response = await client.get(url)
                    if response.status_code == 200 and len(response.text) > 100:
                        return year, quarter
                except Exception:
                    continue
                await self.apply_rate_limit()

        return None, None

    async def _collect_state(
        self, state: str, area_fips: str, year: int, quarter: int
    ) -> Dict[str, Any]:
        """Fetch and process QCEW data for a single state."""
        try:
            url = f"{self.base_url}/{year}/{quarter}/area/{area_fips}.csv"
            client = await self.get_client()
            await self.apply_rate_limit()

            response = await client.get(url)
            response.raise_for_status()

            csv_text = response.text
            if not csv_text or len(csv_text) < 50:
                return {"processed": 0, "inserted": 0, "error": f"Empty CSV for {state}"}

            # Parse CSV
            reader = csv.DictReader(io.StringIO(csv_text))
            records = []

            for row in reader:
                transformed = self._transform_row(row, state)
                if transformed:
                    records.append(transformed)

            logger.info(f"QCEW {state}: parsed {len(records)} records from CSV")

            if records:
                inserted, updated = self.bulk_upsert(
                    IndustryEmployment,
                    records,
                    unique_columns=[
                        "area_fips", "industry_code", "ownership",
                        "period_year", "period_quarter",
                    ],
                    update_columns=[
                        "area_name", "industry_title", "establishments",
                        "avg_monthly_employment", "total_wages_thousand",
                        "avg_weekly_wage", "source", "collected_at",
                    ],
                )
                return {"processed": len(records), "inserted": inserted + updated}

            return {"processed": 0, "inserted": 0}

        except Exception as e:
            logger.warning(f"QCEW collection failed for {state}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return {"processed": 0, "inserted": 0, "error": str(e)}

    def _transform_row(self, row: Dict[str, str], state: str) -> Optional[Dict[str, Any]]:
        """Transform a QCEW CSV row to a database record.

        Filters to private sector (own_code=5) and key supersectors only.
        """
        own_code = row.get("own_code", "").strip()
        industry_code = row.get("industry_code", "").strip()

        # Filter: private sector only
        if own_code != "5":
            return None

        # Filter: key supersector codes only
        if industry_code not in KEY_INDUSTRY_CODES:
            return None

        area_fips = row.get("area_fips", "").strip()
        if not area_fips:
            return None

        return {
            "area_fips": area_fips,
            "area_name": row.get("area_title", "").strip() or state,
            "industry_code": industry_code,
            "industry_title": row.get("industry_title", "").strip()
            or KEY_INDUSTRY_CODES.get(industry_code, ""),
            "ownership": "private",
            "period_year": self._safe_int(row.get("year")),
            "period_quarter": self._safe_int(row.get("qtr")),
            "establishments": self._safe_int(row.get("qtrly_estabs")),
            "avg_monthly_employment": self._safe_int(
                row.get("month1_emplvl")  # Use month 1 as representative
            ),
            "total_wages_thousand": self._safe_bigint(row.get("total_qtrly_wages")),
            "avg_weekly_wage": self._safe_float(row.get("avg_wkly_wage")),
            "source": "bls_qcew",
            "collected_at": datetime.utcnow(),
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None or value == "" or value == "-":
            return None
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None or value == "" or value == "-":
            return None
        try:
            return int(float(str(value).strip()))
        except (ValueError, TypeError):
            return None

    def _safe_bigint(self, value: Any) -> Optional[int]:
        """Convert to int, dividing by 1000 for wages (stored as thousands)."""
        raw = self._safe_int(value)
        if raw is not None:
            return raw // 1000
        return None
