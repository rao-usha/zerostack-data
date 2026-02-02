"""
Site Intelligence Platform - Base Collector.

Abstract base class for all site intelligence data collectors.
Provides common functionality for API calls, rate limiting, and database operations.
"""
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List, Dict, Any, Type

import httpx
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from app.core.models_site_intel import SiteIntelCollectionJob
from app.sources.site_intel.types import (
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
    CollectionStatus,
    CollectionProgress,
)

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """
    Abstract base class for site intelligence collectors.

    Subclasses must implement:
    - domain: The domain this collector belongs to
    - source: The data source identifier
    - collect(): Main collection logic
    """

    # Must be overridden by subclasses
    domain: SiteIntelDomain
    source: SiteIntelSource

    # Default configuration
    default_timeout: float = 30.0
    default_retries: int = 3
    rate_limit_delay: float = 0.5  # seconds between requests

    def __init__(
        self,
        db: Session,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        """
        Initialize the collector.

        Args:
            db: SQLAlchemy database session
            api_key: Optional API key for authenticated sources
            base_url: Optional base URL override
        """
        self.db = db
        self.api_key = api_key
        self.base_url = base_url or self.get_default_base_url()
        self._client: Optional[httpx.AsyncClient] = None
        self._job: Optional[SiteIntelCollectionJob] = None

    @abstractmethod
    def get_default_base_url(self) -> str:
        """Return the default base URL for this source."""
        pass

    @abstractmethod
    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """
        Execute the collection job.

        Args:
            config: Collection configuration

        Returns:
            CollectionResult with statistics and status
        """
        pass

    # =========================================================================
    # HTTP CLIENT MANAGEMENT
    # =========================================================================

    async def get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            headers = self.get_default_headers()
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=headers,
                timeout=self.default_timeout,
                follow_redirects=True,
            )
        return self._client

    async def close_client(self):
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def apply_rate_limit(self):
        """Apply rate limiting delay between requests."""
        import asyncio
        if self.rate_limit_delay > 0:
            await asyncio.sleep(self.rate_limit_delay)

    def get_default_headers(self) -> Dict[str, str]:
        """Get default headers for requests. Override for custom headers."""
        headers = {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    # =========================================================================
    # HTTP REQUEST METHODS
    # =========================================================================

    async def fetch_json(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch JSON data from an endpoint.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            method: HTTP method (GET, POST)
            json_body: JSON body for POST requests

        Returns:
            Parsed JSON response

        Raises:
            httpx.HTTPError: On request failure
        """
        client = await self.get_client()

        for attempt in range(self.default_retries):
            try:
                if method.upper() == "GET":
                    response = await client.get(endpoint, params=params)
                elif method.upper() == "POST":
                    response = await client.post(endpoint, params=params, json=json_body)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limited
                    import asyncio
                    wait_time = self.rate_limit_delay * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                    await asyncio.sleep(wait_time)
                    continue
                raise

            except httpx.RequestError as e:
                if attempt < self.default_retries - 1:
                    import asyncio
                    await asyncio.sleep(self.rate_limit_delay)
                    continue
                raise

        raise Exception(f"Failed after {self.default_retries} retries")

    async def fetch_all_pages(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_key: str = "page",
        per_page_key: str = "per_page",
        data_key: str = "data",
        per_page: int = 100,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages of a paginated API.

        Args:
            endpoint: API endpoint
            params: Base query parameters
            page_key: Parameter name for page number
            per_page_key: Parameter name for page size
            data_key: Key in response containing data array
            per_page: Items per page
            max_pages: Maximum pages to fetch (None for all)

        Returns:
            List of all items across pages
        """
        import asyncio

        all_items = []
        page = 1
        params = params or {}

        while True:
            params[page_key] = page
            params[per_page_key] = per_page

            response = await self.fetch_json(endpoint, params=params)
            items = response.get(data_key, [])

            if not items:
                break

            all_items.extend(items)
            logger.debug(f"Fetched page {page}, {len(items)} items, total: {len(all_items)}")

            if len(items) < per_page:
                break

            if max_pages and page >= max_pages:
                break

            page += 1
            await asyncio.sleep(self.rate_limit_delay)

        return all_items

    # =========================================================================
    # JOB MANAGEMENT
    # =========================================================================

    def create_job(self, config: CollectionConfig) -> SiteIntelCollectionJob:
        """Create a collection job record."""
        job = SiteIntelCollectionJob(
            domain=self.domain.value,
            source=self.source.value,
            job_type=config.job_type,
            status="pending",
            config=config.model_dump(mode='json'),
            created_at=datetime.utcnow(),
        )
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        self._job = job
        return job

    def start_job(self):
        """Mark job as started."""
        if self._job:
            self._job.status = "running"
            self._job.started_at = datetime.utcnow()
            self.db.commit()

    def complete_job(self, result: CollectionResult):
        """Mark job as completed with results."""
        if self._job:
            self._job.status = result.status.value
            self._job.completed_at = datetime.utcnow()
            self._job.total_items = result.total_items
            self._job.processed_items = result.processed_items
            self._job.inserted_items = result.inserted_items
            self._job.updated_items = result.updated_items
            self._job.failed_items = result.failed_items
            if result.error_message:
                self._job.error_message = result.error_message
            if result.errors:
                self._job.error_details = {"errors": result.errors}
            self.db.commit()

    def update_progress(
        self,
        processed: int,
        total: int,
        current_step: Optional[str] = None,
        errors: int = 0,
    ) -> CollectionProgress:
        """Update job progress."""
        if self._job:
            self._job.processed_items = processed
            self._job.total_items = total
            self._job.failed_items = errors
            self.db.commit()

        progress_pct = (processed / total * 100) if total > 0 else 0
        return CollectionProgress(
            job_id=self._job.id if self._job else 0,
            status=CollectionStatus.RUNNING,
            processed_items=processed,
            total_items=total,
            progress_pct=progress_pct,
            current_step=current_step,
            errors_so_far=errors,
        )

    # =========================================================================
    # DATABASE OPERATIONS
    # =========================================================================

    def bulk_upsert(
        self,
        model: Type,
        records: List[Dict[str, Any]],
        unique_columns: List[str],
        update_columns: Optional[List[str]] = None,
        batch_size: int = 1000,
    ) -> tuple[int, int]:
        """
        Bulk upsert records using PostgreSQL ON CONFLICT.

        Args:
            model: SQLAlchemy model class
            records: List of record dictionaries
            unique_columns: Columns that form the unique constraint
            update_columns: Columns to update on conflict (None = all non-unique)
            batch_size: Records per batch

        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if not records:
            return 0, 0

        inserted = 0
        updated = 0

        # Determine update columns
        if update_columns is None:
            all_columns = set(records[0].keys())
            update_columns = list(all_columns - set(unique_columns) - {'id'})

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            stmt = insert(model).values(batch)

            if update_columns:
                update_dict = {col: stmt.excluded[col] for col in update_columns}
                stmt = stmt.on_conflict_do_update(
                    index_elements=unique_columns,
                    set_=update_dict,
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=unique_columns)

            result = self.db.execute(stmt)
            self.db.commit()

            # PostgreSQL returns rowcount for affected rows
            batch_affected = result.rowcount
            inserted += batch_affected

        return inserted, updated

    def bulk_insert(
        self,
        model: Type,
        records: List[Dict[str, Any]],
        batch_size: int = 1000,
    ) -> int:
        """
        Bulk insert records (no upsert).

        Args:
            model: SQLAlchemy model class
            records: List of record dictionaries
            batch_size: Records per batch

        Returns:
            Number of inserted records
        """
        if not records:
            return 0

        inserted = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            self.db.bulk_insert_mappings(model, batch)
            self.db.commit()
            inserted += len(batch)

        return inserted

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def calculate_distance_miles(
        self,
        lat1: float,
        lng1: float,
        lat2: float,
        lng2: float,
    ) -> float:
        """
        Calculate distance between two points using Haversine formula.

        Args:
            lat1, lng1: First point coordinates
            lat2, lng2: Second point coordinates

        Returns:
            Distance in miles
        """
        import math

        R = 3959  # Earth's radius in miles

        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lng = math.radians(lng2 - lng1)

        a = (
            math.sin(delta_lat / 2) ** 2 +
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    def filter_by_bbox(
        self,
        records: List[Dict[str, Any]],
        bbox: Dict[str, float],
        lat_key: str = "latitude",
        lng_key: str = "longitude",
    ) -> List[Dict[str, Any]]:
        """
        Filter records by bounding box.

        Args:
            records: List of records with lat/lng
            bbox: Bounding box dict with min_lat, max_lat, min_lng, max_lng
            lat_key: Key for latitude in records
            lng_key: Key for longitude in records

        Returns:
            Filtered records within bounding box
        """
        return [
            r for r in records
            if (
                bbox["min_lat"] <= r.get(lat_key, 0) <= bbox["max_lat"] and
                bbox["min_lng"] <= r.get(lng_key, 0) <= bbox["max_lng"]
            )
        ]

    def filter_by_states(
        self,
        records: List[Dict[str, Any]],
        states: List[str],
        state_key: str = "state",
    ) -> List[Dict[str, Any]]:
        """
        Filter records by state codes.

        Args:
            records: List of records with state field
            states: List of state codes (e.g., ["CA", "TX"])
            state_key: Key for state in records

        Returns:
            Filtered records in specified states
        """
        states_upper = [s.upper() for s in states]
        return [r for r in records if r.get(state_key, "").upper() in states_upper]

    def create_result(
        self,
        status: CollectionStatus,
        total: int = 0,
        processed: int = 0,
        inserted: int = 0,
        updated: int = 0,
        failed: int = 0,
        errors: Optional[List[Dict[str, Any]]] = None,
        error_message: Optional[str] = None,
        sample: Optional[List[Dict[str, Any]]] = None,
    ) -> CollectionResult:
        """Create a CollectionResult with timing information."""
        now = datetime.utcnow()
        started = self._job.started_at if self._job else now
        duration = (now - started).total_seconds() if started else 0

        return CollectionResult(
            status=status,
            domain=self.domain,
            source=self.source,
            total_items=total,
            processed_items=processed,
            inserted_items=inserted,
            updated_items=updated,
            failed_items=failed,
            started_at=started,
            completed_at=now,
            duration_seconds=duration,
            errors=errors,
            error_message=error_message,
            sample_records=sample,
        )
