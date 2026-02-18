"""
Batch database operations helper.

Provides reusable utilities for batch INSERT operations with:
- Configurable batch sizes
- ON CONFLICT handling (upsert)
- Progress tracking
- Error handling
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Callable
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


class BatchInsertResult:
    """Result of a batch insert operation."""

    def __init__(self):
        self.rows_inserted: int = 0
        self.rows_updated: int = 0
        self.batches_processed: int = 0
        self.errors: List[Dict[str, Any]] = []
        self.started_at: datetime = datetime.utcnow()
        self.completed_at: Optional[datetime] = None

    def mark_complete(self) -> None:
        """Mark the operation as complete."""
        self.completed_at = datetime.utcnow()

    @property
    def total_rows(self) -> int:
        """Total rows processed (inserted + updated)."""
        return self.rows_inserted + self.rows_updated

    @property
    def duration_seconds(self) -> Optional[float]:
        """Duration in seconds if complete."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "total_rows": self.total_rows,
            "batches_processed": self.batches_processed,
            "errors": self.errors,
            "duration_seconds": self.duration_seconds,
        }


def batch_insert(
    db: Session,
    table_name: str,
    rows: List[Dict[str, Any]],
    columns: List[str],
    batch_size: int = 1000,
    conflict_columns: Optional[List[str]] = None,
    update_columns: Optional[List[str]] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    commit_per_batch: bool = True,
) -> BatchInsertResult:
    """
    Batch insert rows into a table with optional upsert support.

    Args:
        db: SQLAlchemy session
        table_name: Target table name (will be quoted)
        rows: List of row dictionaries
        columns: Column names to insert
        batch_size: Number of rows per batch (default 1000)
        conflict_columns: Columns for ON CONFLICT (enables upsert)
        update_columns: Columns to update on conflict (defaults to all non-conflict columns)
        progress_callback: Optional callback(current, total) for progress updates
        commit_per_batch: Whether to commit after each batch (default True)

    Returns:
        BatchInsertResult with statistics

    Example:
        result = batch_insert(
            db=session,
            table_name="fred_series",
            rows=[{"series_id": "GDP", "date": "2024-01-01", "value": 100.0}],
            columns=["series_id", "date", "value"],
            conflict_columns=["series_id", "date"],
            update_columns=["value"]
        )
    """
    result = BatchInsertResult()

    if not rows:
        logger.warning("batch_insert called with empty rows list")
        result.mark_complete()
        return result

    if not columns:
        raise ValueError("columns list cannot be empty")

    # Build SQL
    sql = _build_insert_sql(
        table_name=table_name,
        columns=columns,
        conflict_columns=conflict_columns,
        update_columns=update_columns,
    )

    total_rows = len(rows)
    logger.info(
        f"Starting batch insert: {total_rows} rows into {table_name}, "
        f"batch_size={batch_size}"
    )

    try:
        for i in range(0, total_rows, batch_size):
            batch = rows[i : i + batch_size]
            batch_num = i // batch_size + 1

            try:
                db.execute(text(sql), batch)
                result.rows_inserted += len(batch)
                result.batches_processed += 1

                if commit_per_batch:
                    db.commit()

                if progress_callback:
                    progress_callback(min(i + batch_size, total_rows), total_rows)

                # Log progress every 5 batches or 5000 rows
                if result.batches_processed % 5 == 0 or i + batch_size >= total_rows:
                    logger.info(
                        f"Progress: {min(i + batch_size, total_rows)}/{total_rows} rows "
                        f"({result.batches_processed} batches)"
                    )

            except Exception as e:
                logger.error(f"Error in batch {batch_num}: {e}")
                result.errors.append(
                    {
                        "batch": batch_num,
                        "start_row": i,
                        "end_row": i + len(batch),
                        "error": str(e),
                    }
                )

                if commit_per_batch:
                    db.rollback()
                raise

        if not commit_per_batch:
            db.commit()

    except Exception as e:
        logger.error(f"Batch insert failed: {e}")
        raise

    finally:
        result.mark_complete()

    logger.info(
        f"Batch insert complete: {result.rows_inserted} rows in "
        f"{result.duration_seconds:.2f}s ({result.batches_processed} batches)"
    )

    return result


def batch_insert_with_returning(
    db: Session,
    table_name: str,
    rows: List[Dict[str, Any]],
    columns: List[str],
    returning_columns: List[str],
    batch_size: int = 100,
    conflict_columns: Optional[List[str]] = None,
    update_columns: Optional[List[str]] = None,
) -> Tuple[BatchInsertResult, List[Dict[str, Any]]]:
    """
    Batch insert with RETURNING clause to get inserted/updated row IDs.

    Args:
        db: SQLAlchemy session
        table_name: Target table name
        rows: List of row dictionaries
        columns: Column names to insert
        returning_columns: Columns to return after insert
        batch_size: Number of rows per batch (smaller for RETURNING)
        conflict_columns: Columns for ON CONFLICT
        update_columns: Columns to update on conflict

    Returns:
        Tuple of (BatchInsertResult, list of returned rows)
    """
    result = BatchInsertResult()
    returned_rows: List[Dict[str, Any]] = []

    if not rows:
        result.mark_complete()
        return result, returned_rows

    # Build SQL with RETURNING
    base_sql = _build_insert_sql(
        table_name=table_name,
        columns=columns,
        conflict_columns=conflict_columns,
        update_columns=update_columns,
    )
    returning_clause = f" RETURNING {', '.join(returning_columns)}"
    sql = base_sql + returning_clause

    total_rows = len(rows)

    try:
        for i in range(0, total_rows, batch_size):
            batch = rows[i : i + batch_size]

            cursor = db.execute(text(sql), batch)
            batch_returned = [dict(row._mapping) for row in cursor.fetchall()]
            returned_rows.extend(batch_returned)

            result.rows_inserted += len(batch)
            result.batches_processed += 1

            db.commit()

    except Exception as e:
        logger.error(f"Batch insert with returning failed: {e}")
        db.rollback()
        raise

    finally:
        result.mark_complete()

    return result, returned_rows


def _build_insert_sql(
    table_name: str,
    columns: List[str],
    conflict_columns: Optional[List[str]] = None,
    update_columns: Optional[List[str]] = None,
) -> str:
    """
    Build parameterized INSERT SQL with optional ON CONFLICT.

    Args:
        table_name: Table name
        columns: Column names
        conflict_columns: Columns for ON CONFLICT
        update_columns: Columns to update on conflict

    Returns:
        SQL string with :param placeholders
    """
    # Column list
    cols = ", ".join(columns)

    # Value placeholders (:column_name)
    values = ", ".join(f":{col}" for col in columns)

    # Basic INSERT
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({values})"

    # Add ON CONFLICT if specified
    if conflict_columns:
        conflict_cols = ", ".join(conflict_columns)

        # Determine update columns (all non-conflict columns by default)
        if update_columns is None:
            update_columns = [c for c in columns if c not in conflict_columns]

        if update_columns:
            # Build SET clause with EXCLUDED
            set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
            sql += f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
        else:
            # No columns to update, just ignore conflicts
            sql += f" ON CONFLICT ({conflict_cols}) DO NOTHING"

    return sql


def bulk_upsert(
    db: Session,
    table_name: str,
    rows: List[Dict[str, Any]],
    key_columns: List[str],
    value_columns: List[str],
    batch_size: int = 1000,
    add_timestamp_column: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> BatchInsertResult:
    """
    Convenience wrapper for upsert operations.

    Args:
        db: SQLAlchemy session
        table_name: Target table
        rows: Data rows
        key_columns: Primary key / unique columns for conflict detection
        value_columns: Value columns to insert/update
        batch_size: Rows per batch
        add_timestamp_column: Optional column name to set to NOW() on update
        progress_callback: Progress callback

    Returns:
        BatchInsertResult
    """
    all_columns = key_columns + value_columns
    update_cols = value_columns.copy()

    if add_timestamp_column:
        # Add timestamp to update clause but not to insert columns
        # (it should have a default)
        update_cols.append(add_timestamp_column)

    return batch_insert(
        db=db,
        table_name=table_name,
        rows=rows,
        columns=all_columns,
        batch_size=batch_size,
        conflict_columns=key_columns,
        update_columns=update_cols,
        progress_callback=progress_callback,
    )


def create_table_if_not_exists(db: Session, create_sql: str, table_name: str) -> bool:
    """
    Execute CREATE TABLE IF NOT EXISTS.

    Args:
        db: SQLAlchemy session
        create_sql: CREATE TABLE SQL statement
        table_name: Table name (for logging)

    Returns:
        True if created, False if already existed
    """
    try:
        # Check if table exists
        check_sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = :table_name
            )
        """
        result = db.execute(text(check_sql), {"table_name": table_name})
        exists = result.scalar()

        if not exists:
            db.execute(text(create_sql))
            db.commit()
            logger.info(f"Created table: {table_name}")
            return True
        else:
            logger.debug(f"Table already exists: {table_name}")
            return False

    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        db.rollback()
        raise
