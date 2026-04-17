"""
FFIEC Bank Call Reports ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor, create_ingestion_job
from app.core.batch_operations import batch_insert
from app.sources.ffiec_banks.client import FfiecBankClient
from app.sources.ffiec_banks import metadata

logger = logging.getLogger(__name__)


class FfiecBankIngestor(BaseSourceIngestor):
    """
    Ingestor for FFIEC Bank Call Reports via FDIC BankFind Suite.

    Handles ingestion of bank financial data.
    """

    SOURCE_NAME = "ffiec_banks"

    def __init__(self, db: Session):
        """
        Initialize FFIEC Bank ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest_financials(
        self,
        job_id: int,
        report_date: str = "20231231",
        state: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ingest bank financial data for a given report date.

        Args:
            job_id: Ingestion job ID
            report_date: Report date in YYYYMMDD format (e.g., "20231231")
            state: Optional state name filter (e.g., "Texas")

        Returns:
            Dictionary with ingestion results
        """
        client = FfiecBankClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)

            logger.info(
                f"Ingesting FFIEC bank financials for date={report_date}, "
                f"state={state}"
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
                    "report_date": report_date,
                    "state": state,
                },
            )

            # 3. Fetch data from FDIC API (all pages)
            raw_records = await client.fetch_all_financials(
                report_date=report_date,
                state=state,
            )

            logger.info(
                f"Fetched {len(raw_records)} raw bank records for date={report_date}"
            )

            # 4. Parse data
            rows = metadata.parse_bank_records(raw_records)

            if not rows:
                logger.warning(f"No parseable bank records for date={report_date}")
                self.complete_job(job_id, 0, warn_on_empty=True)
                return {
                    "table_name": metadata.TABLE_NAME,
                    "report_date": report_date,
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
                "report_date": report_date,
                "state": state,
                "rows_fetched": len(raw_records),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(
                f"FFIEC bank ingestion failed for date={report_date}: {e}",
                exc_info=True,
            )
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


async def ingest_ffiec_banks(
    db: Session,
    job_id: int,
    report_date: str = "20231231",
    state: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest FFIEC bank financial data.

    Convenience wrapper around FfiecBankIngestor.ingest_financials().
    """
    ingestor = FfiecBankIngestor(db)
    return await ingestor.ingest_financials(
        job_id=job_id,
        report_date=report_date,
        state=state,
    )
