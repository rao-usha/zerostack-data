"""
DOT Infrastructure Grants ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor
from app.core.batch_operations import batch_insert
from app.sources.dot_grants.client import DotGrantsClient
from app.sources.dot_grants import metadata

logger = logging.getLogger(__name__)


class DotGrantsIngestor(BaseSourceIngestor):
    """
    Ingestor for DOT Infrastructure Grants (via USAspending).

    Handles ingestion of state-level DOT grant spending data.
    """

    SOURCE_NAME = "dot_grants"

    def __init__(self, db: Session):
        """
        Initialize DOT Grants ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest_grants(
        self,
        job_id: int,
        agency: str = "Department of Transportation",
        start_year: int = 2021,
        end_year: int = 2026,
    ) -> Dict[str, Any]:
        """
        Ingest DOT infrastructure grant spending by state and year.

        Fetches grant data from USAspending for each fiscal year
        and upserts into the database.

        Args:
            job_id: Ingestion job ID
            agency: Top-tier awarding agency name
            start_year: First fiscal year to fetch
            end_year: Last fiscal year to fetch (inclusive)

        Returns:
            Dictionary with ingestion results
        """
        client = DotGrantsClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            logger.info(
                f"Ingesting DOT grants: agency={agency}, "
                f"years={start_year}-{end_year}"
            )

            # 2. Prepare table
            create_sql = metadata.generate_create_table_sql()

            self.prepare_table(
                dataset_id=metadata.DATASET_ID,
                table_name=metadata.TABLE_NAME,
                create_sql=create_sql,
                display_name=metadata.DISPLAY_NAME,
                description=metadata.DESCRIPTION,
                source_metadata={
                    "agency": agency,
                    "start_year": start_year,
                    "end_year": end_year,
                },
            )

            # 3. Fetch data from USAspending API (year by year)
            raw_records = await client.fetch_grants_by_year(
                agency=agency,
                start_year=start_year,
                end_year=end_year,
            )

            logger.info(f"Fetched {len(raw_records)} raw DOT grant records")

            # 4. Parse data
            rows = metadata.parse_grant_records(raw_records)

            if not rows:
                logger.warning("No parseable DOT grant records")
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "agency": agency,
                    "years": f"{start_year}-{end_year}",
                    "rows_fetched": len(raw_records),
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
                batch_size=500,
            )

            rows_inserted = result.rows_inserted + result.rows_updated

            # 6. Complete job
            self.complete_job(job_id, rows_inserted)

            return {
                "table_name": metadata.TABLE_NAME,
                "agency": agency,
                "years": f"{start_year}-{end_year}",
                "rows_fetched": len(raw_records),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"DOT grants ingestion failed: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


async def ingest_dot_grants(
    db: Session,
    job_id: int,
    agency: str = "Department of Transportation",
    start_year: int = 2021,
    end_year: int = 2026,
) -> Dict[str, Any]:
    """
    Ingest DOT infrastructure grant spending data.

    Convenience wrapper around DotGrantsIngestor.ingest_grants().
    """
    ingestor = DotGrantsIngestor(db)
    return await ingestor.ingest_grants(
        job_id=job_id,
        agency=agency,
        start_year=start_year,
        end_year=end_year,
    )
