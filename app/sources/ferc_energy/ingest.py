"""
FERC Energy Filings ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
Uses EIA electricity state profiles API as a reliable proxy for FERC data.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor, create_ingestion_job
from app.core.batch_operations import batch_insert
from app.sources.ferc_energy.client import FercEnergyClient
from app.sources.ferc_energy import metadata

logger = logging.getLogger(__name__)


class FercEnergyIngestor(BaseSourceIngestor):
    """
    Ingestor for FERC Energy Filings via EIA electricity profiles.

    Handles ingestion of state-level electricity data.
    """

    SOURCE_NAME = "ferc_energy"

    def __init__(self, db: Session):
        """
        Initialize FERC Energy ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest_state_profiles(
        self,
        job_id: int,
        period: Optional[str] = None,
        state: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ingest state electricity profile data.

        Args:
            job_id: Ingestion job ID
            period: Year filter (e.g., "2022")
            state: Two-letter state code filter (e.g., "TX")

        Returns:
            Dictionary with ingestion results
        """
        client = FercEnergyClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            logger.info(
                f"Ingesting FERC energy state profiles for "
                f"period={period}, state={state}"
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
                    "period": period,
                    "state": state,
                },
            )

            # 3. Fetch data from EIA API (all pages)
            raw_records = await client.fetch_all_state_profiles(
                period=period,
                state=state,
            )

            logger.info(
                f"Fetched {len(raw_records)} raw state profile records"
            )

            # 4. Parse data
            rows = metadata.parse_state_profiles(raw_records)

            if not rows:
                logger.warning(
                    f"No parseable state profiles for period={period}"
                )
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "period": period,
                    "state": state,
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
                batch_size=1000,
            )

            rows_inserted = result.rows_inserted + result.rows_updated

            # 6. Complete job
            self.complete_job(job_id, rows_inserted)

            return {
                "table_name": metadata.TABLE_NAME,
                "period": period,
                "state": state,
                "rows_fetched": len(raw_records),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"FERC energy ingestion failed for period={period}: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


async def ingest_ferc_energy(
    db: Session,
    job_id: int,
    period: Optional[str] = None,
    state: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest FERC energy state profile data.

    Convenience wrapper around FercEnergyIngestor.ingest_state_profiles().
    """
    ingestor = FercEnergyIngestor(db)
    return await ingestor.ingest_state_profiles(
        job_id=job_id,
        period=period,
        state=state,
    )
