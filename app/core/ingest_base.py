"""
Base ingestion orchestration class.

Provides reusable logic for:
- Table preparation (CREATE TABLE IF NOT EXISTS)
- Dataset registry management
- Job status tracking
- Common ingestion patterns
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Type
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.core.batch_operations import (
    batch_insert,
    BatchInsertResult,
    create_table_if_not_exists,
)

logger = logging.getLogger(__name__)


class BaseSourceIngestor(ABC):
    """
    Base class for all data source ingestors.

    Provides unified:
    - Table preparation and schema management
    - Dataset registry updates
    - Job status tracking
    - Batch insert operations

    Subclasses should:
    - Set SOURCE_NAME class attribute
    - Implement generate_table_name(), generate_create_table_sql()
    - Implement fetch_data() and parse_data()
    """

    SOURCE_NAME: str = "unknown"

    def __init__(self, db: Session):
        """
        Initialize the ingestor.

        Args:
            db: SQLAlchemy database session
        """
        self.db = db

    def prepare_table(
        self,
        dataset_id: str,
        table_name: str,
        create_sql: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Prepare database table for ingestion.

        Steps:
        1. Create table if not exists
        2. Register or update in dataset_registry

        Args:
            dataset_id: Unique identifier for this dataset
            table_name: PostgreSQL table name
            create_sql: CREATE TABLE SQL statement
            display_name: Human-readable name
            description: Dataset description
            source_metadata: Additional metadata to store

        Returns:
            Dict with table_name, created (bool), dataset_id
        """
        try:
            # 1. Create table
            logger.info(f"Preparing table {table_name} for {self.SOURCE_NAME}")
            created = create_table_if_not_exists(self.db, create_sql, table_name)

            # 2. Register in dataset_registry
            self._update_dataset_registry(
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=display_name,
                description=description,
                source_metadata=source_metadata,
            )

            return {
                "table_name": table_name,
                "dataset_id": dataset_id,
                "created": created,
                "source": self.SOURCE_NAME,
            }

        except Exception as e:
            logger.error(f"Failed to prepare table {table_name}: {e}")
            raise

    def _update_dataset_registry(
        self,
        dataset_id: str,
        table_name: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
    ) -> DatasetRegistry:
        """
        Create or update dataset registry entry.

        Args:
            dataset_id: Unique identifier
            table_name: Table name
            display_name: Human-readable name
            description: Description
            source_metadata: Additional metadata

        Returns:
            DatasetRegistry instance
        """
        existing = (
            self.db.query(DatasetRegistry)
            .filter(DatasetRegistry.table_name == table_name)
            .first()
        )

        if existing:
            logger.debug(f"Updating existing dataset registry: {dataset_id}")
            existing.last_updated_at = datetime.utcnow()
            if source_metadata:
                existing.source_metadata = source_metadata
            if display_name:
                existing.display_name = display_name
            if description:
                existing.description = description
            self.db.commit()
            return existing
        else:
            logger.info(f"Registering new dataset: {dataset_id}")
            entry = DatasetRegistry(
                source=self.SOURCE_NAME,
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=display_name or dataset_id,
                description=description,
                source_metadata=source_metadata or {},
            )
            self.db.add(entry)
            self.db.commit()
            return entry

    def update_job_status(
        self,
        job_id: int,
        status: JobStatus,
        rows_inserted: Optional[int] = None,
        error_message: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> Optional[IngestionJob]:
        """
        Update ingestion job status.

        Args:
            job_id: Job ID
            status: New status
            rows_inserted: Number of rows inserted
            error_message: Error message if failed
            error_details: Detailed error info

        Returns:
            Updated IngestionJob or None if not found
        """
        job = self.db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if not job:
            logger.warning(f"Job {job_id} not found")
            return None

        job.status = status

        if status == JobStatus.RUNNING:
            job.started_at = datetime.utcnow()
        elif status in (JobStatus.SUCCESS, JobStatus.FAILED):
            job.completed_at = datetime.utcnow()

        if rows_inserted is not None:
            job.rows_inserted = rows_inserted

        if error_message:
            job.error_message = error_message
            job.error_details = error_details

        self.db.commit()
        logger.info(f"Job {job_id} status updated to {status.value}")
        return job

    def start_job(self, job_id: int) -> Optional[IngestionJob]:
        """Mark job as running."""
        return self.update_job_status(job_id, JobStatus.RUNNING)

    def complete_job(
        self,
        job_id: int,
        rows_inserted: int,
        require_rows: bool = False,
        warn_on_empty: bool = True,
    ) -> Optional[IngestionJob]:
        """
        Mark job as successfully completed.

        Args:
            job_id: Job ID
            rows_inserted: Number of rows inserted
            require_rows: If True, fail job when rows_inserted is 0
            warn_on_empty: If True, log a warning when no rows inserted

        Returns:
            Updated IngestionJob or None if not found
        """
        if rows_inserted == 0:
            if require_rows:
                logger.error(f"Job {job_id}: No rows inserted, marking as failed")
                return self.update_job_status(
                    job_id,
                    JobStatus.FAILED,
                    rows_inserted=0,
                    error_message="Ingestion completed but no rows were inserted",
                )
            elif warn_on_empty:
                logger.warning(f"Job {job_id}: Completed with 0 rows inserted")

        return self.update_job_status(
            job_id, JobStatus.SUCCESS, rows_inserted=rows_inserted
        )

    def fail_job(
        self,
        job_id: int,
        error: Exception,
        error_details: Optional[Dict[str, Any]] = None,
    ) -> Optional[IngestionJob]:
        """Mark job as failed."""
        return self.update_job_status(
            job_id,
            JobStatus.FAILED,
            error_message=str(error),
            error_details=error_details or {"exception_type": type(error).__name__},
        )

    def insert_rows(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        columns: List[str],
        conflict_columns: Optional[List[str]] = None,
        update_columns: Optional[List[str]] = None,
        batch_size: int = 1000,
    ) -> BatchInsertResult:
        """
        Insert rows into table using batch operations.

        Args:
            table_name: Target table
            rows: Data rows
            columns: Column names
            conflict_columns: Columns for ON CONFLICT (upsert)
            update_columns: Columns to update on conflict
            batch_size: Rows per batch

        Returns:
            BatchInsertResult with statistics
        """
        if not rows:
            logger.warning(f"No rows to insert into {table_name}")
            result = BatchInsertResult()
            result.mark_complete()
            return result

        logger.info(f"Inserting {len(rows)} rows into {table_name}")

        return batch_insert(
            db=self.db,
            table_name=table_name,
            rows=rows,
            columns=columns,
            batch_size=batch_size,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
        )


class SimpleIngestor(BaseSourceIngestor):
    """
    Simplified ingestor for straightforward use cases.

    Provides a one-method ingestion pattern for sources with simple
    fetch -> parse -> insert workflows.
    """

    async def run_ingestion(
        self,
        job_id: int,
        dataset_id: str,
        table_name: str,
        create_sql: str,
        fetch_func,
        parse_func,
        columns: List[str],
        conflict_columns: Optional[List[str]] = None,
        update_columns: Optional[List[str]] = None,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        source_metadata: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        """
        Run a complete ingestion workflow.

        Args:
            job_id: Ingestion job ID
            dataset_id: Dataset identifier
            table_name: Target table name
            create_sql: CREATE TABLE SQL
            fetch_func: Async function that fetches raw data
            parse_func: Function that parses raw data into row dicts
            columns: Column names for insert
            conflict_columns: Columns for ON CONFLICT
            update_columns: Columns to update on conflict
            display_name: Human-readable name
            description: Dataset description
            source_metadata: Additional metadata
            batch_size: Rows per batch

        Returns:
            Dict with ingestion results
        """
        try:
            # 1. Start job
            self.start_job(job_id)

            # 2. Prepare table
            table_info = self.prepare_table(
                dataset_id=dataset_id,
                table_name=table_name,
                create_sql=create_sql,
                display_name=display_name,
                description=description,
                source_metadata=source_metadata,
            )

            # 3. Fetch data
            logger.info(f"Fetching data for {dataset_id}")
            raw_data = await fetch_func()

            # 4. Parse data
            logger.info(f"Parsing data for {dataset_id}")
            rows = parse_func(raw_data)

            # 5. Insert data
            result = self.insert_rows(
                table_name=table_name,
                rows=rows,
                columns=columns,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                batch_size=batch_size,
            )

            # 6. Complete job
            self.complete_job(job_id, result.rows_inserted)

            return {
                "status": "success",
                "table_name": table_name,
                "dataset_id": dataset_id,
                "rows_inserted": result.rows_inserted,
                "batches_processed": result.batches_processed,
                "duration_seconds": result.duration_seconds,
            }

        except Exception as e:
            logger.error(f"Ingestion failed for {dataset_id}: {e}", exc_info=True)
            self.fail_job(job_id, e)
            raise


def create_ingestion_job(
    db: Session, source: str, config: Dict[str, Any]
) -> IngestionJob:
    """
    Create a new ingestion job.

    Args:
        db: Database session
        source: Source name
        config: Job configuration

    Returns:
        Created IngestionJob
    """
    job = IngestionJob(source=source, status=JobStatus.PENDING, config=config)
    db.add(job)
    db.commit()
    db.refresh(job)
    logger.info(f"Created ingestion job {job.id} for {source}")
    return job
