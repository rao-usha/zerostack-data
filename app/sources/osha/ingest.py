"""
OSHA ingestion orchestration.

Coordinates downloading bulk CSV files from DOL enforcement data,
parsing inspection and violation records, and loading into PostgreSQL.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.ingest_base import BaseSourceIngestor
from app.sources.osha.client import OSHAClient
from app.sources.osha import metadata

logger = logging.getLogger(__name__)


# =========================================================================
# Standalone dispatch functions (called by SOURCE_DISPATCH in jobs.py)
# =========================================================================


async def ingest_osha_inspections(
    db: Session,
    job_id: int,
) -> Dict[str, Any]:
    """Dispatch-compatible wrapper for OSHA inspections ingestion."""
    ingestor = OSHAIngestor(db)
    return await ingestor.ingest_inspections(job_id=job_id)


async def ingest_osha_violations(
    db: Session,
    job_id: int,
) -> Dict[str, Any]:
    """Dispatch-compatible wrapper for OSHA violations ingestion."""
    ingestor = OSHAIngestor(db)
    return await ingestor.ingest_violations(job_id=job_id)


async def ingest_osha_all(
    db: Session,
    job_id: int,
    dataset: str = "all",
) -> Dict[str, Any]:
    """
    Dispatch-compatible wrapper for OSHA ingestion.

    Routes to the appropriate method based on dataset config value.
    """
    ingestor = OSHAIngestor(db)
    if dataset == "inspections":
        return await ingestor.ingest_inspections(job_id=job_id)
    elif dataset == "violations":
        return await ingestor.ingest_violations(job_id=job_id)
    else:
        return await ingestor.ingest_all(job_id=job_id)


class OSHAIngestor(BaseSourceIngestor):
    """
    Ingestor for OSHA enforcement data.

    Downloads bulk CSV files from the DOL enforcement data catalog
    and loads inspection and violation records into PostgreSQL.
    """

    SOURCE_NAME = "osha"

    def __init__(self, db: Session):
        """
        Initialize OSHA ingestor.

        Args:
            db: SQLAlchemy database session
        """
        super().__init__(db)
        self.settings = get_settings()

    async def ingest_inspections(self, job_id: int) -> Dict[str, Any]:
        """
        Download and ingest OSHA inspection records.

        Args:
            job_id: Ingestion job ID

        Returns:
            Dictionary with ingestion results
        """
        client = OSHAClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)
            logger.info("Starting OSHA inspections ingestion (bulk CSV download)")

            # 2. Prepare table
            self.prepare_table(
                dataset_id=metadata.INSPECTIONS_DATASET_ID,
                table_name=metadata.INSPECTIONS_TABLE_NAME,
                create_sql=metadata.CREATE_INSPECTIONS_SQL,
                display_name=metadata.INSPECTIONS_DISPLAY_NAME,
                description=metadata.INSPECTIONS_DESCRIPTION,
                source_metadata={"data_type": "inspections", "format": "csv_bulk"},
            )

            # 3. Download CSV
            logger.info("Downloading OSHA inspections CSV from DOL...")
            csv_content = await client.download_inspections()

            # 4. Parse CSV
            rows = metadata.parse_inspections_csv(csv_content)

            # 5. Insert data
            if not rows:
                logger.warning("No OSHA inspection records parsed from CSV")
                rows_inserted = 0
            else:
                result = self.insert_rows(
                    table_name=metadata.INSPECTIONS_TABLE_NAME,
                    rows=rows,
                    columns=metadata.INSPECTIONS_COLUMNS,
                    conflict_columns=metadata.INSPECTIONS_CONFLICT_COLUMNS,
                    update_columns=metadata.INSPECTIONS_UPDATE_COLUMNS,
                    batch_size=2000,
                )
                rows_inserted = result.rows_inserted

            # 6. Complete job
            self.complete_job(job_id, rows_inserted)

            return {
                "table_name": metadata.INSPECTIONS_TABLE_NAME,
                "data_type": "inspections",
                "rows_parsed": len(rows),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(f"OSHA inspections ingestion failed: {e}", exc_info=True)
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()

    async def ingest_violations(self, job_id: int) -> Dict[str, Any]:
        """
        Download and ingest OSHA violation records.

        Args:
            job_id: Ingestion job ID

        Returns:
            Dictionary with ingestion results
        """
        client = OSHAClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)
            logger.info("Starting OSHA violations ingestion (bulk CSV download)")

            # 2. Prepare table
            self.prepare_table(
                dataset_id=metadata.VIOLATIONS_DATASET_ID,
                table_name=metadata.VIOLATIONS_TABLE_NAME,
                create_sql=metadata.CREATE_VIOLATIONS_SQL,
                display_name=metadata.VIOLATIONS_DISPLAY_NAME,
                description=metadata.VIOLATIONS_DESCRIPTION,
                source_metadata={"data_type": "violations", "format": "csv_bulk"},
            )

            # 3. Download CSV
            logger.info("Downloading OSHA violations CSV from DOL...")
            csv_content = await client.download_violations()

            # 4. Parse CSV
            rows = metadata.parse_violations_csv(csv_content)

            # 5. Insert data
            if not rows:
                logger.warning("No OSHA violation records parsed from CSV")
                rows_inserted = 0
            else:
                result = self.insert_rows(
                    table_name=metadata.VIOLATIONS_TABLE_NAME,
                    rows=rows,
                    columns=metadata.VIOLATIONS_COLUMNS,
                    conflict_columns=metadata.VIOLATIONS_CONFLICT_COLUMNS,
                    update_columns=metadata.VIOLATIONS_UPDATE_COLUMNS,
                    batch_size=2000,
                )
                rows_inserted = result.rows_inserted

            # 6. Complete job
            self.complete_job(job_id, rows_inserted)

            return {
                "table_name": metadata.VIOLATIONS_TABLE_NAME,
                "data_type": "violations",
                "rows_parsed": len(rows),
                "rows_inserted": rows_inserted,
            }

        except Exception as e:
            logger.error(f"OSHA violations ingestion failed: {e}", exc_info=True)
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()

    async def ingest_all(self, job_id: int) -> Dict[str, Any]:
        """
        Download and ingest both OSHA inspections and violations.

        Creates the inspections table first, then violations.
        The job_id tracks the overall operation; individual tables
        are loaded sequentially.

        Args:
            job_id: Ingestion job ID

        Returns:
            Dictionary with combined ingestion results
        """
        client = OSHAClient(
            max_concurrency=self.settings.max_concurrency,
            max_retries=self.settings.max_retries,
            backoff_factor=self.settings.retry_backoff_factor,
        )

        try:
            # 1. Start job
            self.start_job(job_id)
            logger.info("Starting full OSHA ingestion (inspections + violations)")

            total_rows = 0

            # 2. Inspections
            self.prepare_table(
                dataset_id=metadata.INSPECTIONS_DATASET_ID,
                table_name=metadata.INSPECTIONS_TABLE_NAME,
                create_sql=metadata.CREATE_INSPECTIONS_SQL,
                display_name=metadata.INSPECTIONS_DISPLAY_NAME,
                description=metadata.INSPECTIONS_DESCRIPTION,
            )

            csv_inspections = await client.download_inspections()
            inspection_rows = metadata.parse_inspections_csv(csv_inspections)

            if inspection_rows:
                result = self.insert_rows(
                    table_name=metadata.INSPECTIONS_TABLE_NAME,
                    rows=inspection_rows,
                    columns=metadata.INSPECTIONS_COLUMNS,
                    conflict_columns=metadata.INSPECTIONS_CONFLICT_COLUMNS,
                    update_columns=metadata.INSPECTIONS_UPDATE_COLUMNS,
                    batch_size=2000,
                )
                total_rows += result.rows_inserted
                logger.info(f"Inspections: {result.rows_inserted} rows inserted")

            # 3. Violations
            self.prepare_table(
                dataset_id=metadata.VIOLATIONS_DATASET_ID,
                table_name=metadata.VIOLATIONS_TABLE_NAME,
                create_sql=metadata.CREATE_VIOLATIONS_SQL,
                display_name=metadata.VIOLATIONS_DISPLAY_NAME,
                description=metadata.VIOLATIONS_DESCRIPTION,
            )

            csv_violations = await client.download_violations()
            violation_rows = metadata.parse_violations_csv(csv_violations)

            if violation_rows:
                result = self.insert_rows(
                    table_name=metadata.VIOLATIONS_TABLE_NAME,
                    rows=violation_rows,
                    columns=metadata.VIOLATIONS_COLUMNS,
                    conflict_columns=metadata.VIOLATIONS_CONFLICT_COLUMNS,
                    update_columns=metadata.VIOLATIONS_UPDATE_COLUMNS,
                    batch_size=2000,
                )
                total_rows += result.rows_inserted
                logger.info(f"Violations: {result.rows_inserted} rows inserted")

            # 4. Complete job
            self.complete_job(job_id, total_rows)

            return {
                "tables": [
                    metadata.INSPECTIONS_TABLE_NAME,
                    metadata.VIOLATIONS_TABLE_NAME,
                ],
                "inspections_parsed": len(inspection_rows),
                "violations_parsed": len(violation_rows),
                "total_rows_inserted": total_rows,
            }

        except Exception as e:
            logger.error(f"OSHA full ingestion failed: {e}", exc_info=True)
            self.fail_job(job_id, e)
            raise

        finally:
            await client.close()
