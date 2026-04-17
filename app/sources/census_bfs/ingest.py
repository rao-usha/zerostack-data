"""
Census Business Formation Statistics (BFS) ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor
from app.core.batch_operations import batch_insert
from app.sources.census_bfs.client import CensusBFSClient
from app.sources.census_bfs import metadata

logger = logging.getLogger(__name__)


class CensusBFSIngestor(BaseSourceIngestor):
    """
    Ingestor for Census Business Formation Statistics.

    Handles ingestion of business application data by state and time period.
    """

    SOURCE_NAME = "census_bfs"

    def __init__(self, db: Session):
        """
        Initialize Census BFS ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest(
        self,
        job_id: int,
        time_from: str = "2020",
        state_fips: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ingest Census BFS business formation data.

        Args:
            job_id: Ingestion job ID
            time_from: Start year for time series (e.g., "2020")
            state_fips: Optional specific state FIPS code (all states if None)

        Returns:
            Dictionary with ingestion results
        """
        client = CensusBFSClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            logger.info(
                f"Ingesting Census BFS data: time_from={time_from}, "
                f"state={state_fips or 'all'}"
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
                    "time_from": time_from,
                    "state_fips": state_fips,
                },
            )

            # 3. Fetch data from Census BFS API
            raw_data = await client.fetch_business_formation(
                time_from=time_from,
                state_fips=state_fips,
            )

            raw_count = len(raw_data) - 1 if raw_data else 0
            logger.info(f"Fetched {raw_count} raw BFS records")

            # 4. Parse data
            rows = metadata.parse_response(raw_data)

            if not rows:
                logger.warning("No parseable BFS records")
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "time_from": time_from,
                    "state_fips": state_fips,
                    "rows_fetched": raw_count,
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
                "time_from": time_from,
                "state_fips": state_fips,
                "rows_fetched": raw_count,
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"Census BFS ingestion failed: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


async def ingest_census_bfs(
    db: Session,
    job_id: int,
    time_from: str = "2020",
    state_fips: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest Census BFS data.

    Convenience wrapper around CensusBFSIngestor.ingest().
    """
    ingestor = CensusBFSIngestor(db)
    return await ingestor.ingest(
        job_id=job_id,
        time_from=time_from,
        state_fips=state_fips,
    )
