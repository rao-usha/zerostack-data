"""
Data Export API endpoints.

Provides REST API for exporting table data to files.
"""
import logging
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import ExportJob, ExportFormat, ExportStatus
from app.core.export_service import ExportService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/export", tags=["export"])


# =============================================================================
# Pydantic Schemas
# =============================================================================

class ExportJobCreate(BaseModel):
    """Schema for creating an export job."""
    table_name: str = Field(..., min_length=1, max_length=255)
    format: str = Field(..., description="Export format: csv, json, parquet")
    columns: Optional[List[str]] = Field(default=None, description="Columns to export (null = all)")
    row_limit: Optional[int] = Field(default=None, ge=1, le=10000000, description="Max rows to export")
    filters: Optional[Dict[str, Any]] = Field(default=None, description="Filters: {date_from, date_to}")
    compress: bool = Field(default=False, description="Compress output with gzip")


class ExportJobResponse(BaseModel):
    """Response schema for an export job."""
    id: int
    table_name: str
    format: str
    status: str
    columns: Optional[List[str]]
    row_limit: Optional[int]
    filters: Optional[Dict[str, Any]]
    compress: bool
    file_name: Optional[str]
    file_size_bytes: Optional[int]
    row_count: Optional[int]
    error_message: Optional[str]
    created_at: str
    started_at: Optional[str]
    completed_at: Optional[str]
    expires_at: Optional[str]


class TableInfo(BaseModel):
    """Information about an exportable table."""
    table_name: str
    row_count: int
    columns: List[str]


class FormatInfo(BaseModel):
    """Information about an export format."""
    format: str
    description: str
    supports_compression: bool


# =============================================================================
# Helper Functions
# =============================================================================

def job_to_response(job: ExportJob) -> ExportJobResponse:
    """Convert ExportJob model to response schema."""
    return ExportJobResponse(
        id=job.id,
        table_name=job.table_name,
        format=job.format.value,
        status=job.status.value,
        columns=job.columns,
        row_limit=job.row_limit,
        filters=job.filters,
        compress=bool(job.compress),
        file_name=job.file_name,
        file_size_bytes=job.file_size_bytes,
        row_count=job.row_count,
        error_message=job.error_message,
        created_at=job.created_at.isoformat(),
        started_at=job.started_at.isoformat() if job.started_at else None,
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
        expires_at=job.expires_at.isoformat() if job.expires_at else None
    )


def run_export_job(job_id: int):
    """Background task to run an export job."""
    from app.core.database import get_session_factory
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        service = ExportService(db)
        service.execute_export(job_id)
    except Exception as e:
        logger.error(f"Background export job {job_id} failed: {e}")
    finally:
        db.close()


# =============================================================================
# Endpoints
# =============================================================================

@router.get("/formats", response_model=List[FormatInfo])
def list_formats():
    """List supported export formats."""
    return [
        FormatInfo(
            format="csv",
            description="Comma-separated values with headers",
            supports_compression=True
        ),
        FormatInfo(
            format="json",
            description="JSON array of objects",
            supports_compression=True
        ),
        FormatInfo(
            format="parquet",
            description="Apache Parquet columnar format (efficient for large data)",
            supports_compression=False  # Parquet has built-in compression
        )
    ]


@router.get("/tables", response_model=List[TableInfo])
def list_tables(db: Session = Depends(get_db)):
    """List tables available for export."""
    service = ExportService(db)
    tables = service.list_tables()
    return [TableInfo(**t) for t in tables]


@router.get("/tables/{table_name}/columns")
def get_table_columns(table_name: str, db: Session = Depends(get_db)):
    """Get column names for a specific table."""
    service = ExportService(db)
    try:
        columns = service.get_table_columns(table_name)
        return {"table_name": table_name, "columns": columns}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/jobs", response_model=ExportJobResponse, status_code=201)
def create_export_job(
    request: ExportJobCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Create a new export job.

    The job runs in the background. Check status with GET /export/jobs/{id}.
    Download the file when status is 'completed'.
    """
    service = ExportService(db)

    # Validate format
    try:
        format_enum = ExportFormat(request.format)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid format: {request.format}. Must be one of: csv, json, parquet"
        )

    try:
        job = service.create_export_job(
            table_name=request.table_name,
            format=format_enum,
            columns=request.columns,
            row_limit=request.row_limit,
            filters=request.filters,
            compress=request.compress
        )

        # Run export in background
        background_tasks.add_task(run_export_job, job.id)

        return job_to_response(job)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/jobs", response_model=List[ExportJobResponse])
def list_export_jobs(
    status: Optional[str] = Query(default=None, description="Filter by status"),
    table_name: Optional[str] = Query(default=None, description="Filter by table"),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db)
):
    """List export jobs with optional filtering."""
    service = ExportService(db)

    status_enum = None
    if status:
        try:
            status_enum = ExportStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    jobs = service.list_jobs(status=status_enum, table_name=table_name, limit=limit)
    return [job_to_response(j) for j in jobs]


@router.get("/jobs/{job_id}", response_model=ExportJobResponse)
def get_export_job(job_id: int, db: Session = Depends(get_db)):
    """Get an export job by ID."""
    service = ExportService(db)
    job = service.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Export job not found: {job_id}")

    return job_to_response(job)


@router.get("/jobs/{job_id}/download")
def download_export(job_id: int, db: Session = Depends(get_db)):
    """
    Download the exported file.

    Only available when job status is 'completed'.
    """
    service = ExportService(db)
    job = service.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Export job not found: {job_id}")

    if job.status != ExportStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Export not ready. Status: {job.status.value}"
        )

    file_path = service.get_file_path(job_id)
    if not file_path:
        raise HTTPException(status_code=404, detail="Export file not found or expired")

    # Determine media type
    media_type = "application/octet-stream"
    if job.format == ExportFormat.CSV:
        media_type = "text/csv"
    elif job.format == ExportFormat.JSON:
        media_type = "application/json"
    elif job.format == ExportFormat.PARQUET:
        media_type = "application/vnd.apache.parquet"

    if job.compress:
        media_type = "application/gzip"

    return FileResponse(
        path=file_path,
        filename=job.file_name,
        media_type=media_type
    )


@router.delete("/jobs/{job_id}")
def delete_export_job(job_id: int, db: Session = Depends(get_db)):
    """Delete an export job and its file."""
    service = ExportService(db)

    if not service.delete_job(job_id):
        raise HTTPException(status_code=404, detail=f"Export job not found: {job_id}")

    return {"message": f"Export job {job_id} deleted"}


@router.post("/cleanup")
def cleanup_expired_exports(db: Session = Depends(get_db)):
    """Clean up expired export jobs and files."""
    service = ExportService(db)
    count = service.cleanup_expired()
    return {"message": f"Cleaned up {count} expired exports", "count": count}
