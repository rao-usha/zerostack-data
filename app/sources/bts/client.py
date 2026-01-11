"""
BTS API client supporting both Socrata API and CSV bulk downloads.

Data sources:
- Socrata API: https://data.transportation.gov (SODA API)
- CSV Downloads: https://www.bts.gov/ (FAF5 bulk data)

Rate limits (Socrata):
- Unauthenticated: ~1,000 requests/hour
- With app token: 4,000+ requests/hour (set BTS_APP_TOKEN env var)

No API key required for public datasets.
"""
import logging
import zipfile
import io
import csv
from typing import Dict, List, Optional, Any

from app.core.http_client import BaseAPIClient
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)


class BTSClient(BaseAPIClient):
    """
    HTTP client for BTS data with bounded concurrency and rate limiting.

    Supports:
    - Socrata SODA API for real-time queries
    - CSV bulk downloads for FAF5 freight data

    Inherits retry logic, backoff, and error handling from BaseAPIClient.
    """

    SOURCE_NAME = "bts"
    BASE_URL = "https://data.transportation.gov"

    # Key Socrata dataset identifiers (4x4 IDs)
    DATASETS = {
        "border_crossing": "keg4-3bc2",
        "vmt_monthly": "w96p-f2qv",
        "airline_fuel": "7wid-t3kd",
    }

    # FAF5 CSV download URLs (Freight Analysis Framework)
    FAF_URLS = {
        "regional_2018_2024": "https://www.bts.gov/sites/bts.dot.gov/files/2025-07/FAF5.7.1_2018-2024.zip",
        "regional_forecasts": "https://www.bts.gov/sites/bts.dot.gov/files/2025-07/FAF5.7.1.zip",
        "state_2018_2024": "https://www.bts.gov/sites/bts.dot.gov/files/2025-07/FAF5.7.1_State_2018-2024.zip",
    }

    def __init__(
        self,
        app_token: Optional[str] = None,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize BTS client.

        Args:
            app_token: Optional Socrata app token for higher rate limits
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
        """
        config = get_api_config("bts")

        super().__init__(
            api_key=app_token,  # App token is optional
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=120.0,  # Longer timeout for large downloads
            connect_timeout=30.0,
            rate_limit_interval=config.get_rate_limit_interval()
        )

        self.app_token = app_token

    # ========== Socrata API Methods ==========

    async def get_border_crossing_data(
        self,
        limit: int = 50000,
        offset: int = 0,
        port_name: Optional[str] = None,
        state: Optional[str] = None,
        border: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        measure: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch border crossing entry data from Socrata.

        Args:
            limit: Max records to return (Socrata max ~50000)
            offset: Pagination offset
            port_name: Filter by port name
            state: Filter by state code
            border: Filter by border (US-Canada, US-Mexico)
            start_date: Start date filter (YYYY-MM-DD or YYYY-MM)
            end_date: End date filter
            measure: Filter by measure type (Trucks, Containers, etc.)

        Returns:
            List of border crossing records
        """
        dataset_id = self.DATASETS["border_crossing"]

        where_clauses = []
        if port_name:
            where_clauses.append(f"port_name = '{port_name}'")
        if state:
            where_clauses.append(f"state = '{state}'")
        if border:
            where_clauses.append(f"border = '{border}'")
        if measure:
            where_clauses.append(f"measure = '{measure}'")
        if start_date:
            where_clauses.append(f"date >= '{start_date}'")
        if end_date:
            where_clauses.append(f"date <= '{end_date}'")

        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "date DESC"
        }

        if where_clauses:
            params["$where"] = " AND ".join(where_clauses)

        if self.app_token:
            params["$$app_token"] = self.app_token

        return await self.get(
            f"resource/{dataset_id}.json",
            params=params,
            resource_id="border_crossing"
        )

    async def get_vmt_data(
        self,
        limit: int = 50000,
        offset: int = 0,
        state: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch Vehicle Miles Traveled (VMT) data from Socrata.

        Args:
            limit: Max records to return
            offset: Pagination offset
            state: Filter by state name
            start_date: Start date filter (YYYY-MM)
            end_date: End date filter

        Returns:
            List of VMT records
        """
        dataset_id = self.DATASETS["vmt_monthly"]

        where_clauses = []
        if state:
            where_clauses.append(f"state = '{state}'")
        if start_date:
            where_clauses.append(f"date >= '{start_date}'")
        if end_date:
            where_clauses.append(f"date <= '{end_date}'")

        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "date DESC"
        }

        if where_clauses:
            params["$where"] = " AND ".join(where_clauses)

        if self.app_token:
            params["$$app_token"] = self.app_token

        return await self.get(
            f"resource/{dataset_id}.json",
            params=params,
            resource_id="vmt"
        )

    # ========== FAF5 CSV Download Methods ==========

    async def download_faf_data(
        self,
        version: str = "regional_2018_2024"
    ) -> List[Dict[str, Any]]:
        """
        Download and parse FAF5 Freight Analysis Framework data.

        Args:
            version: FAF version to download:
                - "regional_2018_2024": Regional database 2018-2024
                - "regional_forecasts": Regional with forecasts to 2050
                - "state_2018_2024": State-level 2018-2024

        Returns:
            List of parsed FAF records
        """
        if version not in self.FAF_URLS:
            raise ValueError(
                f"Invalid FAF version: {version}. "
                f"Valid options: {list(self.FAF_URLS.keys())}"
            )

        url = self.FAF_URLS[version]

        logger.info(f"Downloading FAF5 data from {url}")

        # Use full URL for external download
        response = await self.get(url, resource_id=f"faf:{version}")

        # For binary content, we need to handle differently
        # The base class returns JSON by default, so for FAF we might need custom handling
        return self._parse_faf_zip(io.BytesIO(response), version)

    def _parse_faf_zip(
        self,
        zip_data: io.BytesIO,
        version: str
    ) -> List[Dict[str, Any]]:
        """
        Parse FAF5 ZIP file containing CSV data.

        Args:
            zip_data: ZIP file as BytesIO
            version: FAF version for parsing hints

        Returns:
            List of parsed records
        """
        records = []

        with zipfile.ZipFile(zip_data, 'r') as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]

            if not csv_files:
                raise ValueError("No CSV files found in FAF ZIP archive")

            main_csv = max(csv_files, key=lambda f: zf.getinfo(f).file_size)
            logger.info(f"Parsing FAF CSV: {main_csv}")

            with zf.open(main_csv) as csv_file:
                text_wrapper = io.TextIOWrapper(csv_file, encoding='utf-8')
                reader = csv.DictReader(text_wrapper)

                for row in reader:
                    record = self._normalize_faf_record(row, version)
                    if record:
                        records.append(record)

        return records

    def _normalize_faf_record(
        self,
        row: Dict[str, str],
        version: str
    ) -> Optional[Dict[str, Any]]:
        """Normalize a FAF CSV record to standard format."""
        try:
            record = {
                "fr_orig": row.get("fr_orig") or row.get("dms_orig"),
                "dms_orig": row.get("dms_orig"),
                "dms_dest": row.get("dms_dest"),
                "fr_dest": row.get("fr_dest") or row.get("dms_dest"),
                "fr_inmode": row.get("fr_inmode"),
                "dms_mode": row.get("dms_mode"),
                "fr_outmode": row.get("fr_outmode"),
                "sctg2": row.get("sctg2"),
                "trade_type": self._safe_int(row.get("trade_type")),
                "tons": self._safe_float(row.get("tons_2017") or row.get("tons")),
                "value": self._safe_float(row.get("value_2017") or row.get("value")),
                "tmiles": self._safe_float(row.get("tmiles_2017") or row.get("tmiles")),
                "curval": self._safe_float(row.get("curval")),
            }

            if "year" in row:
                record["year"] = self._safe_int(row["year"])

            return record

        except Exception as e:
            logger.warning(f"Failed to normalize FAF record: {e}")
            return None

    def _safe_float(self, value: Optional[str]) -> Optional[float]:
        """Safely convert string to float."""
        if value is None or value == "" or value == "NA":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Optional[str]) -> Optional[int]:
        """Safely convert string to int."""
        if value is None or value == "" or value == "NA":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None


# Common filter values for reference
BORDER_TYPES = ["US-Canada Border", "US-Mexico Border"]

BORDER_MEASURES = [
    "Trucks",
    "Loaded Truck Containers",
    "Empty Truck Containers",
    "Trains",
    "Loaded Rail Containers",
    "Empty Rail Containers",
    "Train Passengers",
    "Buses",
    "Bus Passengers",
    "Personal Vehicles",
    "Personal Vehicle Passengers",
    "Pedestrians"
]

FAF_MODES = {
    "1": "Truck",
    "2": "Rail",
    "3": "Water",
    "4": "Air (include truck-air)",
    "5": "Multiple modes & mail",
    "6": "Pipeline",
    "7": "Other and unknown",
    "8": "No domestic mode"
}

FAF_TRADE_TYPES = {
    "1": "Domestic",
    "2": "Import",
    "3": "Export",
    "4": "Foreign Trade Zone"
}
