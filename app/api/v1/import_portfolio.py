"""
Bulk Portfolio Import API endpoints.

Provides endpoints for uploading, validating, and importing
portfolio data from CSV/Excel files.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.import_data.portfolio import PortfolioImporter


router = APIRouter(prefix="/import", tags=["import"])


# Response Models


class UploadResponse(BaseModel):
    """Response after file upload."""

    import_id: int
    filename: str
    row_count: int
    status: str
    message: str


class ValidationErrorItem(BaseModel):
    """A single validation error."""

    row: int
    column: str
    error: str


class ValidationWarningItem(BaseModel):
    """A single validation warning."""

    row: int
    message: str


class ValidationSummary(BaseModel):
    """Validation summary."""

    total_rows: int
    valid_rows: int
    invalid_rows: int
    errors: List[ValidationErrorItem] = []
    warnings: List[ValidationWarningItem] = []


class PreviewRow(BaseModel):
    """A row in the preview."""

    row_num: int
    company_name: str
    investor_name: str
    investor_type: str
    status: str
    warning: Optional[str] = None


class PreviewResponse(BaseModel):
    """Preview response with validation and sample data."""

    import_id: int
    status: str
    validation: ValidationSummary
    preview_data: List[PreviewRow]


class ImportResultResponse(BaseModel):
    """Import execution result."""

    import_id: int
    status: str
    results: dict
    can_rollback: bool


class ImportStatusResponse(BaseModel):
    """Import job status."""

    import_id: int
    filename: str
    status: str
    row_count: int
    valid_rows: Optional[int] = None
    invalid_rows: Optional[int] = None
    imported_count: Optional[int] = None
    skipped_count: Optional[int] = None
    error_count: Optional[int] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None


class ImportHistoryItem(BaseModel):
    """Import history item."""

    id: int
    filename: str
    status: str
    row_count: int
    imported_count: Optional[int] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None


class RollbackResponse(BaseModel):
    """Rollback response."""

    import_id: int
    status: str
    message: str


# In-memory storage for parsed rows (in production, use Redis or database)
_parsed_rows_cache: dict = {}


# Endpoints


@router.post(
    "/upload",
    response_model=UploadResponse,
    summary="Upload portfolio file",
    description="""
    Upload a CSV or Excel file containing portfolio data.

    **Required columns:**
    - `company_name` - Name of the portfolio company
    - `investor_name` - Name of the investor (LP or Family Office)
    - `investor_type` - Either "lp" or "family_office"

    **Optional columns:**
    - `company_website`, `company_industry`, `company_stage`, `company_location`
    - `investment_date`, `investment_amount`, `shares_held`, `market_value`
    - `ownership_percentage`, `investment_type`

    After upload, use `/import/{id}/preview` to validate and preview the data.
    """,
)
async def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """Upload a portfolio file for import."""
    # Check file type
    filename = file.filename or "unknown"
    if not filename.lower().endswith((".csv", ".xlsx", ".xls")):
        raise HTTPException(
            status_code=400,
            detail="Unsupported file type. Please upload CSV or Excel (.xlsx/.xls)",
        )

    # Read file content
    content = await file.read()
    file_size = len(content)

    if file_size > 10 * 1024 * 1024:  # 10MB limit
        raise HTTPException(
            status_code=400, detail="File too large. Maximum size is 10MB"
        )

    importer = PortfolioImporter(db)

    try:
        # Parse file
        if filename.lower().endswith(".csv"):
            rows, columns = importer.parse_csv(content)
        else:
            rows, columns = importer.parse_excel(content)

        # Validate columns
        missing = importer.validate_columns(columns)
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {', '.join(missing)}",
            )

        # Create import job
        import_id = importer.create_import(filename, file_size, len(rows))

        # Cache parsed rows for preview/confirm
        _parsed_rows_cache[import_id] = rows

        return {
            "import_id": import_id,
            "filename": filename,
            "row_count": len(rows),
            "status": "pending",
            "message": f"File uploaded successfully. Use /import/{import_id}/preview to validate.",
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")


@router.get(
    "/{import_id}/preview",
    response_model=PreviewResponse,
    summary="Preview import with validation",
    description="""
    Validates the uploaded file and returns a preview of the data.

    Shows validation errors and warnings, plus a sample of the data
    that will be imported.
    """,
)
def get_preview(
    import_id: int,
    preview_limit: int = Query(
        10, ge=1, le=100, description="Number of rows to preview"
    ),
    db: Session = Depends(get_db),
):
    """Get preview of import with validation results."""
    importer = PortfolioImporter(db)

    # Check if import exists
    import_job = importer.get_import(import_id)
    if not import_job:
        raise HTTPException(status_code=404, detail="Import not found")

    # Get cached rows
    rows = _parsed_rows_cache.get(import_id)
    if not rows:
        raise HTTPException(
            status_code=400, detail="Import data not found. Please re-upload the file."
        )

    # Validate
    validation = importer.validate_file(import_id, rows)

    # Get preview
    preview_data = importer.get_preview(rows, preview_limit)

    return {
        "import_id": import_id,
        "status": "previewing",
        "validation": validation,
        "preview_data": preview_data,
    }


@router.post(
    "/{import_id}/confirm",
    response_model=ImportResultResponse,
    summary="Confirm and execute import",
    description="""
    Executes the import after preview/validation.

    Only valid rows will be imported. Invalid rows are skipped.
    The import can be rolled back using `/import/{id}/rollback`.
    """,
)
def confirm_import(
    import_id: int,
    db: Session = Depends(get_db),
):
    """Execute the confirmed import."""
    importer = PortfolioImporter(db)

    # Check if import exists
    import_job = importer.get_import(import_id)
    if not import_job:
        raise HTTPException(status_code=404, detail="Import not found")

    if import_job["status"] not in ("pending", "previewing"):
        raise HTTPException(
            status_code=400,
            detail=f"Import cannot be executed. Status: {import_job['status']}",
        )

    # Get cached rows
    rows = _parsed_rows_cache.get(import_id)
    if not rows:
        raise HTTPException(
            status_code=400, detail="Import data not found. Please re-upload the file."
        )

    # Execute import
    try:
        results = importer.import_rows(import_id, rows)

        # Clear cache
        _parsed_rows_cache.pop(import_id, None)

        return {
            "import_id": import_id,
            "status": "completed",
            "results": results,
            "can_rollback": True,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {e}")


@router.get(
    "/{import_id}/status",
    response_model=ImportStatusResponse,
    summary="Get import status",
    description="Returns the current status and details of an import job.",
)
def get_import_status(
    import_id: int,
    db: Session = Depends(get_db),
):
    """Get status of an import job."""
    importer = PortfolioImporter(db)

    import_job = importer.get_import(import_id)
    if not import_job:
        raise HTTPException(status_code=404, detail="Import not found")

    return {
        "import_id": import_job["id"],
        "filename": import_job["filename"],
        "status": import_job["status"],
        "row_count": import_job["row_count"],
        "valid_rows": import_job["valid_rows"],
        "invalid_rows": import_job["invalid_rows"],
        "imported_count": import_job["imported_count"],
        "skipped_count": import_job["skipped_count"],
        "error_count": import_job["error_count"],
        "created_at": import_job["created_at"].isoformat()
        if import_job.get("created_at")
        else None,
        "completed_at": import_job["completed_at"].isoformat()
        if import_job.get("completed_at")
        else None,
    }


@router.get(
    "/history",
    response_model=List[ImportHistoryItem],
    summary="List import history",
    description="Returns a list of past import jobs.",
)
def list_import_history(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List import history."""
    importer = PortfolioImporter(db)
    imports = importer.list_imports(limit=limit, offset=offset)

    return [
        {
            "id": imp["id"],
            "filename": imp["filename"],
            "status": imp["status"],
            "row_count": imp["row_count"],
            "imported_count": imp["imported_count"],
            "created_at": imp["created_at"].isoformat()
            if imp.get("created_at")
            else None,
            "completed_at": imp["completed_at"].isoformat()
            if imp.get("completed_at")
            else None,
        }
        for imp in imports
    ]


@router.post(
    "/{import_id}/rollback",
    response_model=RollbackResponse,
    summary="Rollback import",
    description="""
    Rolls back a completed import by deleting all records that were created.

    Only completed imports can be rolled back.
    """,
)
def rollback_import(
    import_id: int,
    db: Session = Depends(get_db),
):
    """Rollback a completed import."""
    importer = PortfolioImporter(db)

    # Check if import exists
    import_job = importer.get_import(import_id)
    if not import_job:
        raise HTTPException(status_code=404, detail="Import not found")

    if import_job["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Only completed imports can be rolled back. Status: {import_job['status']}",
        )

    success = importer.rollback_import(import_id)

    if success:
        return {
            "import_id": import_id,
            "status": "rolled_back",
            "message": "Import successfully rolled back. All imported records have been deleted.",
        }
    else:
        raise HTTPException(
            status_code=500, detail="Rollback failed. Please check logs for details."
        )
