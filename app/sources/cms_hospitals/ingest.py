"""
CMS Hospital Provider ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor
from app.core.batch_operations import batch_insert
from app.sources.cms_hospitals.client import CmsHospitalClient
from app.sources.cms_hospitals import metadata

logger = logging.getLogger(__name__)


class CmsHospitalIngestor(BaseSourceIngestor):
    """
    Ingestor for CMS Hospital Provider Data.

    Handles ingestion of hospital quality ratings and provider information.
    """

    SOURCE_NAME = "cms_hospitals"

    def __init__(self, db: Session):
        """
        Initialize CMS Hospital ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest_hospitals(
        self,
        job_id: int,
        max_pages: int = 50,
        page_size: int = 500,
    ) -> Dict[str, Any]:
        """
        Ingest CMS hospital provider data.

        Fetches all hospital records from the CMS Provider Data API
        with automatic pagination and upserts into the database.

        Args:
            job_id: Ingestion job ID
            max_pages: Maximum pages to fetch (safety limit)
            page_size: Records per page

        Returns:
            Dictionary with ingestion results
        """
        client = CmsHospitalClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            logger.info("Ingesting CMS hospital provider data")

            # 2. Prepare table
            create_sql = metadata.generate_create_table_sql()

            self.prepare_table(
                dataset_id=metadata.DATASET_ID,
                table_name=metadata.TABLE_NAME,
                create_sql=create_sql,
                display_name=metadata.DISPLAY_NAME,
                description=metadata.DESCRIPTION,
                source_metadata={
                    "max_pages": max_pages,
                    "page_size": page_size,
                },
            )

            # 3. Fetch data from CMS API (all pages)
            raw_hospitals = await client.fetch_all_hospitals(
                max_pages=max_pages,
                page_size=page_size,
            )

            logger.info(f"Fetched {len(raw_hospitals)} raw hospital records")

            # 4. Parse data
            rows = metadata.parse_hospitals(raw_hospitals)

            if not rows:
                logger.warning("No parseable hospital records")
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "rows_fetched": len(raw_hospitals),
                    "rows_inserted": 0,
                }

            # 5. Insert data with upsert
            result = batch_insert(
                db=self.db,
                table_name=metadata.TABLE_NAME,
                rows=rows,
                columns=metadata.INSERT_COLUMNS,
                conflict_columns=metadata.CONFLICT_COLUMNS,
                update_columns=metadata.UPDATE_COLUMNS,
                batch_size=1000,
            )

            rows_inserted = result.rows_inserted + result.rows_updated

            # 6. Complete job
            self.complete_job(job_id, rows_inserted)

            return {
                "table_name": metadata.TABLE_NAME,
                "rows_fetched": len(raw_hospitals),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"CMS hospital ingestion failed: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


async def ingest_cms_hospitals(
    db: Session,
    job_id: int,
    max_pages: int = 50,
    page_size: int = 500,
) -> Dict[str, Any]:
    """
    Ingest CMS hospital provider data.

    Convenience wrapper around CmsHospitalIngestor.ingest_hospitals().
    """
    ingestor = CmsHospitalIngestor(db)
    return await ingestor.ingest_hospitals(
        job_id=job_id,
        max_pages=max_pages,
        page_size=page_size,
    )
