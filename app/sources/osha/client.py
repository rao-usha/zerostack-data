"""
OSHA Enforcement Data client.

Downloads bulk CSV files from the Department of Labor enforcement data catalog:
https://enforcedata.dol.gov/views/data_catalogs.php

The REST API at developer.dol.gov is unreliable, so this client uses
bulk CSV downloads which are updated regularly and contain comprehensive
inspection and violation records.

Download URLs:
- Inspections: https://enforcedata.dol.gov/views/data_summary/osha/osha_inspection_current.csv.zip
- Violations: https://enforcedata.dol.gov/views/data_summary/osha/osha_violation_current.csv.zip
"""

import asyncio
import io
import logging
import zipfile
from typing import Dict, List, Optional, Any

import httpx

from app.core.http_client import BaseAPIClient
from app.core.api_errors import RetryableError, FatalError

logger = logging.getLogger(__name__)

# Bulk CSV download URLs
INSPECTION_CSV_URL = (
    "https://enforcedata.dol.gov/views/data_summary/osha/"
    "osha_inspection_current.csv.zip"
)
VIOLATION_CSV_URL = (
    "https://enforcedata.dol.gov/views/data_summary/osha/"
    "osha_violation_current.csv.zip"
)


class OSHAClient(BaseAPIClient):
    """
    Client for downloading OSHA enforcement data via bulk CSV files.

    Inherits retry logic and error handling from BaseAPIClient.
    The CSV downloads do not require an API key.
    """

    SOURCE_NAME = "osha"
    BASE_URL = "https://enforcedata.dol.gov"

    def __init__(
        self,
        max_concurrency: int = 2,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize OSHA client.

        Args:
            max_concurrency: Maximum concurrent requests
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff multiplier
        """
        super().__init__(
            api_key=None,
            max_concurrency=max_concurrency,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            timeout=300.0,  # Large file downloads need longer timeout
            connect_timeout=30.0,
            rate_limit_interval=2.0,  # Be respectful to DOL servers
        )

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers for DOL downloads."""
        return {
            "Accept": "*/*",
            "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
        }

    async def download_csv_zip(self, url: str, resource_id: str = "csv") -> str:
        """
        Download a ZIP file containing a CSV and return the CSV content as string.

        Args:
            url: URL of the ZIP file to download
            resource_id: Identifier for logging

        Returns:
            CSV content as a string

        Raises:
            RetryableError: On network errors
            FatalError: On unrecoverable errors (e.g., 404)
        """
        headers = self._build_headers()
        last_error = None

        async with self.semaphore:
            await self._enforce_rate_limit()
            client = await self._get_client()

            for attempt in range(self.max_retries):
                try:
                    logger.info(
                        f"[{self.SOURCE_NAME}] Downloading {resource_id} "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )

                    response = await client.get(url, headers=headers)
                    response.raise_for_status()

                    # Extract CSV from ZIP
                    zip_bytes = response.content
                    logger.info(
                        f"[{self.SOURCE_NAME}] Downloaded {len(zip_bytes)} bytes "
                        f"for {resource_id}"
                    )

                    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                        csv_files = [
                            n for n in zf.namelist() if n.endswith(".csv")
                        ]
                        if not csv_files:
                            raise FatalError(
                                message="ZIP archive contains no CSV files",
                                source=self.SOURCE_NAME,
                            )

                        csv_name = csv_files[0]
                        csv_content = zf.read(csv_name).decode("utf-8", errors="replace")
                        logger.info(
                            f"[{self.SOURCE_NAME}] Extracted {csv_name} "
                            f"({len(csv_content)} chars)"
                        )
                        return csv_content

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        raise FatalError(
                            message=f"OSHA CSV not found at {url}",
                            source=self.SOURCE_NAME,
                            status_code=404,
                        )
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"[{self.SOURCE_NAME}] HTTP error downloading {resource_id}: {e}"
                        )
                        await self._backoff(attempt)
                        last_error = e
                        continue
                    raise RetryableError(
                        message=f"Failed to download {resource_id}: {e}",
                        source=self.SOURCE_NAME,
                    )

                except (httpx.RequestError, zipfile.BadZipFile) as e:
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"[{self.SOURCE_NAME}] Error downloading {resource_id}: {e}"
                        )
                        await self._backoff(attempt)
                        last_error = e
                        continue
                    raise RetryableError(
                        message=f"Failed to download {resource_id}: {e}",
                        source=self.SOURCE_NAME,
                    )

            raise RetryableError(
                message=f"Failed after {self.max_retries} attempts: {last_error}",
                source=self.SOURCE_NAME,
            )

    async def download_inspections(self) -> str:
        """
        Download the OSHA inspections CSV.

        Returns:
            CSV content as a string
        """
        return await self.download_csv_zip(
            INSPECTION_CSV_URL, resource_id="osha_inspections"
        )

    async def download_violations(self) -> str:
        """
        Download the OSHA violations CSV.

        Returns:
            CSV content as a string
        """
        return await self.download_csv_zip(
            VIOLATION_CSV_URL, resource_id="osha_violations"
        )
