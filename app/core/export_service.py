"""
Data Export Service.

Provides functionality to export table data to various file formats.
"""

import os
import gzip
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect

from app.core.models import ExportJob, ExportFormat, ExportStatus

logger = logging.getLogger(__name__)

# Export directory - can be configured via environment
EXPORT_DIR = os.environ.get("EXPORT_DIR", "/tmp/exports")
EXPORT_EXPIRY_HOURS = int(os.environ.get("EXPORT_EXPIRY_HOURS", "24"))

# ---------------------------------------------------------------------------
# Table list cache — avoids ~400 queries per request
# ---------------------------------------------------------------------------
_table_cache: List[Dict[str, Any]] = []
_table_cache_time: float = 0
_TABLE_CACHE_TTL = 300  # 5 minutes


# =============================================================================
# Export Service
# =============================================================================


class ExportService:
    """Service for exporting table data to files."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_export_dir()

    def _ensure_export_dir(self):
        """Ensure export directory exists."""
        Path(EXPORT_DIR).mkdir(parents=True, exist_ok=True)

    def list_tables(self) -> List[Dict[str, Any]]:
        """List all tables available for export. Uses a 5-minute TTL cache."""
        global _table_cache, _table_cache_time

        now = time.monotonic()
        if _table_cache and (now - _table_cache_time) < _TABLE_CACHE_TTL:
            return _table_cache

        inspector = inspect(self.db.get_bind())
        tables = []

        # Single query for all row counts via pg_stat — avoids N COUNT(*) queries
        row_counts: Dict[str, int] = {}
        try:
            rows = self.db.execute(
                text(
                    "SELECT relname, n_live_tup "
                    "FROM pg_stat_user_tables "
                    "ORDER BY relname"
                )
            )
            for r in rows:
                row_counts[r[0]] = int(r[1])
        except Exception:
            pass  # fall back to 0 if pg_stat not available

        for table_name in inspector.get_table_names():
            if table_name.startswith("_") or table_name in ("alembic_version",):
                continue

            try:
                columns = [col["name"] for col in inspector.get_columns(table_name)]
                tables.append(
                    {
                        "table_name": table_name,
                        "row_count": row_counts.get(table_name, 0),
                        "columns": columns,
                    }
                )
            except Exception as e:
                logger.warning(f"Could not inspect table {table_name}: {e}")

        tables.sort(key=lambda x: x["table_name"])

        _table_cache = tables
        _table_cache_time = now

        return tables

    def get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table."""
        inspector = inspect(self.db.get_bind())
        if table_name not in inspector.get_table_names():
            raise ValueError(f"Table not found: {table_name}")
        return [col["name"] for col in inspector.get_columns(table_name)]

    def create_export_job(
        self,
        table_name: str,
        format: ExportFormat,
        columns: Optional[List[str]] = None,
        row_limit: Optional[int] = None,
        filters: Optional[Dict] = None,
        compress: bool = False,
    ) -> ExportJob:
        """Create a new export job."""
        # Validate table exists
        inspector = inspect(self.db.get_bind())
        if table_name not in inspector.get_table_names():
            raise ValueError(f"Table not found: {table_name}")

        # Validate columns if specified
        if columns:
            valid_columns = self.get_table_columns(table_name)
            invalid = set(columns) - set(valid_columns)
            if invalid:
                raise ValueError(f"Invalid columns: {invalid}")

        job = ExportJob(
            table_name=table_name,
            format=format,
            status=ExportStatus.PENDING,
            columns=columns,
            row_limit=row_limit,
            filters=filters,
            compress=1 if compress else 0,
            expires_at=datetime.utcnow() + timedelta(hours=EXPORT_EXPIRY_HOURS),
        )
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)

        logger.info(f"Created export job {job.id} for table {table_name}")
        return job

    def execute_export(self, job_id: int) -> ExportJob:
        """Execute an export job."""
        job = self.db.query(ExportJob).filter(ExportJob.id == job_id).first()
        if not job:
            raise ValueError(f"Export job not found: {job_id}")

        if job.status != ExportStatus.PENDING:
            raise ValueError(f"Job {job_id} is not pending (status: {job.status})")

        # Mark as running
        job.status = ExportStatus.RUNNING
        job.started_at = datetime.utcnow()
        self.db.commit()

        try:
            # Build query
            columns = job.columns if job.columns else ["*"]
            columns_str = (
                ", ".join(f'"{c}"' for c in columns) if columns != ["*"] else "*"
            )

            query = f'SELECT {columns_str} FROM "{job.table_name}"'

            # Apply filters (parameterized to prevent SQL injection)
            where_clauses = []
            params = {}
            if job.filters:
                if job.filters.get("date_from"):
                    where_clauses.append("created_at >= :date_from")
                    params["date_from"] = job.filters["date_from"]
                if job.filters.get("date_to"):
                    where_clauses.append("created_at <= :date_to")
                    params["date_to"] = job.filters["date_to"]

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

            # Apply limit
            if job.row_limit:
                query += " LIMIT :row_limit"
                params["row_limit"] = job.row_limit

            # Execute query
            result = self.db.execute(text(query), params)
            rows = result.fetchall()
            column_names = result.keys()

            # Generate file
            file_name = f"export_{job.id}_{job.table_name}.{job.format.value}"
            if job.compress:
                file_name += ".gz"

            file_path = os.path.join(EXPORT_DIR, file_name)

            # Export based on format
            if job.format == ExportFormat.CSV:
                self._export_csv(rows, column_names, file_path, bool(job.compress))
            elif job.format == ExportFormat.JSON:
                self._export_json(rows, column_names, file_path, bool(job.compress))
            elif job.format == ExportFormat.PARQUET:
                self._export_parquet(rows, column_names, file_path)

            # Update job with results
            job.status = ExportStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.file_path = file_path
            job.file_name = file_name
            job.row_count = len(rows)
            job.file_size_bytes = os.path.getsize(file_path)
            self.db.commit()

            logger.info(
                f"Export job {job_id} completed: {len(rows)} rows, {job.file_size_bytes} bytes"
            )
            return job

        except Exception as e:
            job.status = ExportStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            self.db.commit()
            logger.error(f"Export job {job_id} failed: {e}")
            raise

    def _export_csv(self, rows, columns, file_path: str, compress: bool):
        """Export data to CSV format."""
        import csv

        def write_csv(f):
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in rows:
                writer.writerow(row)

        if compress:
            with gzip.open(file_path, "wt", encoding="utf-8") as f:
                write_csv(f)
        else:
            with open(file_path, "w", encoding="utf-8", newline="") as f:
                write_csv(f)

    def _export_json(self, rows, columns, file_path: str, compress: bool):
        """Export data to JSON format."""
        data = [dict(zip(columns, self._serialize_row(row))) for row in rows]

        if compress:
            with gzip.open(file_path, "wt", encoding="utf-8") as f:
                json.dump(data, f, default=str, indent=2)
        else:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, default=str, indent=2)

    def _export_parquet(self, rows, columns, file_path: str):
        """Export data to Parquet format."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for Parquet export")

        # Convert to DataFrame
        data = [dict(zip(columns, self._serialize_row(row))) for row in rows]
        df = pd.DataFrame(data)

        # Write to Parquet
        df.to_parquet(file_path, engine="pyarrow", index=False)

    def _serialize_row(self, row) -> List:
        """Serialize row values for JSON compatibility."""
        result = []
        for val in row:
            if isinstance(val, datetime):
                result.append(val.isoformat())
            elif hasattr(val, "__dict__"):
                result.append(str(val))
            else:
                result.append(val)
        return result

    def get_job(self, job_id: int) -> Optional[ExportJob]:
        """Get an export job by ID."""
        return self.db.query(ExportJob).filter(ExportJob.id == job_id).first()

    def list_jobs(
        self,
        status: Optional[ExportStatus] = None,
        table_name: Optional[str] = None,
        limit: int = 50,
    ) -> List[ExportJob]:
        """List export jobs with optional filtering."""
        query = self.db.query(ExportJob)

        if status:
            query = query.filter(ExportJob.status == status)
        if table_name:
            query = query.filter(ExportJob.table_name == table_name)

        return query.order_by(ExportJob.created_at.desc()).limit(limit).all()

    def delete_job(self, job_id: int) -> bool:
        """Delete an export job and its file."""
        job = self.get_job(job_id)
        if not job:
            return False

        # Delete file if exists
        if job.file_path and os.path.exists(job.file_path):
            try:
                os.remove(job.file_path)
                logger.info(f"Deleted export file: {job.file_path}")
            except Exception as e:
                logger.warning(f"Could not delete file {job.file_path}: {e}")

        self.db.delete(job)
        self.db.commit()
        return True

    def cleanup_expired(self) -> int:
        """Clean up expired export jobs and files."""
        now = datetime.utcnow()
        expired = (
            self.db.query(ExportJob)
            .filter(
                ExportJob.expires_at < now, ExportJob.status != ExportStatus.EXPIRED
            )
            .all()
        )

        count = 0
        for job in expired:
            # Delete file
            if job.file_path and os.path.exists(job.file_path):
                try:
                    os.remove(job.file_path)
                except Exception as e:
                    logger.warning(
                        f"Could not delete expired file {job.file_path}: {e}"
                    )

            job.status = ExportStatus.EXPIRED
            job.file_path = None
            count += 1

        self.db.commit()
        if count:
            logger.info(f"Cleaned up {count} expired export jobs")

        return count

    def get_file_path(self, job_id: int) -> Optional[str]:
        """Get the file path for a completed export job."""
        job = self.get_job(job_id)
        if not job:
            return None
        if job.status != ExportStatus.COMPLETED:
            return None
        if not job.file_path or not os.path.exists(job.file_path):
            return None
        return job.file_path
