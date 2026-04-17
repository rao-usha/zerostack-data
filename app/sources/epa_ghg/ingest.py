"""
EPA GHGRP ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor
from app.core.batch_operations import batch_insert
from app.sources.epa_ghg.client import EpaGhgClient
from app.sources.epa_ghg import metadata

logger = logging.getLogger(__name__)


class EpaGhgIngestor(BaseSourceIngestor):
    """
    Ingestor for EPA Greenhouse Gas Reporting Program (GHGRP).

    Handles ingestion of facility-level greenhouse gas emissions data.
    """

    SOURCE_NAME = "epa_ghg"

    def __init__(self, db: Session):
        """
        Initialize EPA GHG ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest_facilities(
        self,
        job_id: int,
        max_pages: int = 200,
        page_size: int = 1000,
    ) -> Dict[str, Any]:
        """
        Ingest EPA GHGRP facility emissions data.

        Fetches all facility records from the Envirofacts API
        with automatic pagination and upserts into the database.

        Args:
            job_id: Ingestion job ID
            max_pages: Maximum pages to fetch (safety limit)
            page_size: Rows per page

        Returns:
            Dictionary with ingestion results
        """
        client = EpaGhgClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            logger.info("Ingesting EPA GHGRP facility emissions data")

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

            # 3. Fetch data from Envirofacts API (all pages)
            raw_facilities = await client.fetch_all_facilities(
                max_pages=max_pages,
                page_size=page_size,
            )

            logger.info(f"Fetched {len(raw_facilities)} raw GHGRP facility records")

            # 4. Parse data
            rows = metadata.parse_facilities(raw_facilities)

            if not rows:
                logger.warning("No parseable GHGRP facility records")
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "rows_fetched": len(raw_facilities),
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
                "rows_fetched": len(raw_facilities),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"EPA GHGRP ingestion failed: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


async def ingest_epa_ghg(
    db: Session,
    job_id: int,
    max_pages: int = 200,
    page_size: int = 1000,
) -> Dict[str, Any]:
    """
    Ingest EPA GHGRP facility emissions data.

    Convenience wrapper around EpaGhgIngestor.ingest_facilities().
    """
    ingestor = EpaGhgIngestor(db)
    return await ingestor.ingest_facilities(
        job_id=job_id,
        max_pages=max_pages,
        page_size=page_size,
    )
