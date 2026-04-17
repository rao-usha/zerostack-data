"""
Census County Business Patterns (CBP) ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor
from app.core.batch_operations import batch_insert
from app.sources.census_cbp.client import CensusCBPClient
from app.sources.census_cbp import metadata

logger = logging.getLogger(__name__)


class CensusCBPIngestor(BaseSourceIngestor):
    """
    Ingestor for Census County Business Patterns.

    Handles ingestion of establishment, employment, and payroll data
    by state and NAICS industry.
    """

    SOURCE_NAME = "census_cbp"

    def __init__(self, db: Session):
        """
        Initialize Census CBP ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest(
        self,
        job_id: int,
        year: int = 2022,
        state_fips: Optional[str] = None,
        naics_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ingest Census CBP business patterns data.

        Args:
            job_id: Ingestion job ID
            year: Data year (e.g., 2022)
            state_fips: Optional specific state FIPS code (all states if None)
            naics_code: Optional NAICS code filter (all industries if None)

        Returns:
            Dictionary with ingestion results
        """
        client = CensusCBPClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            logger.info(
                f"Ingesting Census CBP data: year={year}, "
                f"state={state_fips or 'all'}, naics={naics_code or 'all'}"
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
                    "year": year,
                    "state_fips": state_fips,
                    "naics_code": naics_code,
                },
            )

            # 3. Fetch data from Census CBP API
            raw_data = await client.fetch_business_patterns(
                year=year,
                state_fips=state_fips,
                naics_code=naics_code,
            )

            raw_count = len(raw_data) - 1 if raw_data else 0
            logger.info(f"Fetched {raw_count} raw CBP records")

            # 4. Parse data
            rows = metadata.parse_response(raw_data, year=year)

            if not rows:
                logger.warning("No parseable CBP records")
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "year": year,
                    "state_fips": state_fips,
                    "naics_code": naics_code,
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
                "year": year,
                "state_fips": state_fips,
                "naics_code": naics_code,
                "rows_fetched": raw_count,
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"Census CBP ingestion failed: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


async def ingest_census_cbp(
    db: Session,
    job_id: int,
    year: int = 2022,
    state_fips: Optional[str] = None,
    naics_code: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest Census CBP data.

    Convenience wrapper around CensusCBPIngestor.ingest().
    """
    ingestor = CensusCBPIngestor(db)
    return await ingestor.ingest(
        job_id=job_id,
        year=year,
        state_fips=state_fips,
        naics_code=naics_code,
    )
