"""
CFTC Commitments of Traders (COT) client for downloading weekly position data.

Data sources:
- CFTC Public Reporting Environment: https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm
- Direct CSV downloads from CFTC servers

Report Types:
- Legacy (Futures Only, Combined)
- Disaggregated (Futures Only, Combined)
- Traders in Financial Futures (TFF)

No API key required - public data released every Tuesday.
"""

import logging
import io
import csv
import zipfile
from typing import Dict, List, Optional, Any
from datetime import datetime

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class CFTCCOTClient(BaseAPIClient):
    """
    HTTP client for CFTC Commitments of Traders data.

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "cftc_cot"
    BASE_URL = "https://www.cftc.gov/files/dea/history"

    # COT report download URLs
    REPORT_URLS = {
        "legacy_futures": "{base}/deacot{year}.zip",
        "legacy_combined": "{base}/deahistfo{year}.zip",
        "disaggregated_futures": "{base}/fut_disagg_txt_{year}.zip",
        "disaggregated_combined": "{base}/com_disagg_txt_{year}.zip",
        "tff_futures": "{base}/fut_fin_txt_{year}.zip",
        "tff_combined": "{base}/com_fin_txt_{year}.zip",
    }

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize CFTC COT client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("cftc_cot")

        super().__init__(
            api_key=None,  # No API key required
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=120.0,  # Longer timeout for large downloads
            connect_timeout=30.0,
            rate_limit_interval=config.get_rate_limit_interval(),
        )

    async def get_legacy_futures(self, year: int = None) -> List[Dict[str, Any]]:
        """Download Legacy COT report (Futures Only)."""
        if year is None:
            year = datetime.now().year
        return await self._download_cot_report("legacy_futures", year)

    async def get_legacy_combined(self, year: int = None) -> List[Dict[str, Any]]:
        """Download Legacy COT report (Futures + Options Combined)."""
        if year is None:
            year = datetime.now().year
        return await self._download_cot_report("legacy_combined", year)

    async def get_disaggregated_futures(self, year: int = None) -> List[Dict[str, Any]]:
        """Download Disaggregated COT report (Futures Only)."""
        if year is None:
            year = datetime.now().year
        return await self._download_cot_report("disaggregated_futures", year)

    async def get_disaggregated_combined(
        self, year: int = None
    ) -> List[Dict[str, Any]]:
        """Download Disaggregated COT report (Futures + Options Combined)."""
        if year is None:
            year = datetime.now().year
        return await self._download_cot_report("disaggregated_combined", year)

    async def get_tff_futures(self, year: int = None) -> List[Dict[str, Any]]:
        """Download Traders in Financial Futures (TFF) report (Futures Only)."""
        if year is None:
            year = datetime.now().year
        return await self._download_cot_report("tff_futures", year)

    async def get_tff_combined(self, year: int = None) -> List[Dict[str, Any]]:
        """Download TFF report (Futures + Options Combined)."""
        if year is None:
            year = datetime.now().year
        return await self._download_cot_report("tff_combined", year)

    async def _download_cot_report(
        self, report_type: str, year: int
    ) -> List[Dict[str, Any]]:
        """Download and parse a COT report."""
        if report_type not in self.REPORT_URLS:
            raise ValueError(f"Invalid report type: {report_type}")

        url_template = self.REPORT_URLS[report_type]
        url = url_template.format(base=self.BASE_URL, year=year)

        logger.info(f"Downloading CFTC COT {report_type} for {year}")

        # Note: This downloads binary content, need custom handling
        response = await self.get(url, resource_id=f"cot:{report_type}:{year}")

        # Parse ZIP file if response is bytes
        if isinstance(response, bytes):
            zip_data = io.BytesIO(response)
            return self._parse_cot_zip(zip_data, report_type, year)

        return []

    def _parse_cot_zip(
        self, zip_data: io.BytesIO, report_type: str, year: int
    ) -> List[Dict[str, Any]]:
        """Parse COT ZIP file containing CSV/TXT data."""
        records = []

        with zipfile.ZipFile(zip_data, "r") as zf:
            data_files = [f for f in zf.namelist() if f.endswith((".csv", ".txt"))]

            if not data_files:
                raise ValueError("No data files found in COT ZIP archive")

            main_file = data_files[0]
            logger.info(f"Parsing COT file: {main_file}")

            with zf.open(main_file) as data_file:
                text_wrapper = io.TextIOWrapper(
                    data_file, encoding="utf-8", errors="replace"
                )

                first_line = text_wrapper.readline()
                text_wrapper.seek(0)

                delimiter = "," if "," in first_line else "\t"

                reader = csv.DictReader(text_wrapper, delimiter=delimiter)

                for row in reader:
                    record = self._normalize_cot_record(row, report_type, year)
                    if record:
                        records.append(record)

        return records

    def _normalize_cot_record(
        self, row: Dict[str, str], report_type: str, year: int
    ) -> Optional[Dict[str, Any]]:
        """Normalize a COT record to standard format."""
        try:
            market_name = (
                row.get("Market_and_Exchange_Names")
                or row.get("Market and Exchange Names")
                or row.get("market_and_exchange_names")
                or ""
            )

            report_date_raw = (
                row.get("As_of_Date_In_Form_YYMMDD")
                or row.get("Report_Date_as_YYYY-MM-DD")
                or row.get("As of Date in Form YYMMDD")
                or ""
            )

            report_date = self._parse_cot_date(report_date_raw)

            record = {
                "report_type": report_type,
                "year": year,
                "report_date": report_date,
                "market_name": market_name.strip() if market_name else None,
                "cftc_contract_code": row.get("CFTC_Contract_Market_Code")
                or row.get("CFTC Contract Market Code"),
                "open_interest": self._safe_int(
                    row.get("Open_Interest_All") or row.get("Open Interest (All)")
                ),
                "noncomm_long": self._safe_int(
                    row.get("NonComm_Positions_Long_All")
                    or row.get("Noncommercial Positions-Long (All)")
                ),
                "noncomm_short": self._safe_int(
                    row.get("NonComm_Positions_Short_All")
                    or row.get("Noncommercial Positions-Short (All)")
                ),
                "comm_long": self._safe_int(
                    row.get("Comm_Positions_Long_All")
                    or row.get("Commercial Positions-Long (All)")
                ),
                "comm_short": self._safe_int(
                    row.get("Comm_Positions_Short_All")
                    or row.get("Commercial Positions-Short (All)")
                ),
            }

            return record

        except Exception as e:
            logger.warning(f"Failed to normalize COT record: {e}")
            return None

    def _parse_cot_date(self, date_str: str) -> Optional[str]:
        """Parse COT date formats to YYYY-MM-DD."""
        if not date_str:
            return None

        date_str = str(date_str).strip()

        if len(date_str) == 6 and date_str.isdigit():
            try:
                return datetime.strptime(date_str, "%y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                pass

        if "-" in date_str and len(date_str) == 10:
            return date_str

        return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert to int."""
        if value is None or value == "" or value == "." or value == "NA":
            return None
        try:
            return int(float(str(value).replace(",", "")))
        except (ValueError, TypeError):
            return None


# Common contract names for reference
MAJOR_CONTRACTS = {
    "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE": "WTI Crude",
    "NATURAL GAS - NEW YORK MERCANTILE EXCHANGE": "Natural Gas",
    "GOLD - COMMODITY EXCHANGE INC.": "Gold",
    "SILVER - COMMODITY EXCHANGE INC.": "Silver",
    "CORN - CHICAGO BOARD OF TRADE": "Corn",
    "SOYBEANS - CHICAGO BOARD OF TRADE": "Soybeans",
    "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "E-mini S&P 500",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": "EUR/USD",
}
