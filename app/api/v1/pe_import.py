"""
PE Portfolio Import API endpoints.

Upload CSV/Excel files to import portfolio companies, financials, deals,
and leadership data with validation, preview, and rollback.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.pe_import import PEPortfolioImporter

logger = logging.getLogger(__name__)

router = APIRouter(tags=["PE Import"])


@router.get("/pe/import/templates")
def list_templates(db: Session = Depends(get_db)):
    """List available import templates with column definitions."""
    importer = PEPortfolioImporter(db)
    return importer.get_templates()


@router.post("/pe/import/upload")
async def upload_file(
    file: UploadFile = File(...),
    template_type: Optional[str] = Query(None, description="Template type (auto-detected if omitted)"),
    firm_name: Optional[str] = Query(None, description="PE firm name for auto-creation"),
    db: Session = Depends(get_db),
):
    """Upload a CSV/Excel file for import. Returns import_id for preview/confirm."""
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    importer = PEPortfolioImporter(db)
    record = importer.upload(content, file.filename or "upload.csv", template_type, firm_name)
    return record.to_dict()


@router.get("/pe/import/{import_id}/preview")
def preview_import(import_id: str, db: Session = Depends(get_db)):
    """Preview a pending import with sample rows and column mappings."""
    importer = PEPortfolioImporter(db)
    result = importer.preview(import_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Import {import_id} not found")
    return result


@router.post("/pe/import/{import_id}/execute")
def execute_import(import_id: str, db: Session = Depends(get_db)):
    """Execute a previewed import, inserting records into the database."""
    importer = PEPortfolioImporter(db)
    result = importer.execute(import_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Import {import_id} not found")
    if "error" in result and result.get("status") != "imported":
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/pe/import/{import_id}/rollback")
def rollback_import(import_id: str, db: Session = Depends(get_db)):
    """Rollback an executed import, deleting created records."""
    importer = PEPortfolioImporter(db)
    result = importer.rollback(import_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Import {import_id} not found")
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/pe/import/{import_id}")
def get_import_status(import_id: str, db: Session = Depends(get_db)):
    """Get import record status."""
    importer = PEPortfolioImporter(db)
    result = importer.get_import(import_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Import {import_id} not found")
    return result


@router.get("/pe/imports")
def list_imports(db: Session = Depends(get_db)):
    """List all import records."""
    importer = PEPortfolioImporter(db)
    return importer.list_imports()
