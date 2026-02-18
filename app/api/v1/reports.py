"""
Report Builder API Endpoints.

T25: Generate customizable PDF/Excel reports for sharing insights.
"""

from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.reports.builder import ReportBuilder

router = APIRouter(prefix="/reports", tags=["Reports"])


class GenerateReportRequest(BaseModel):
    """Request model for report generation."""

    template: str
    format: str = "excel"
    params: dict
    title: Optional[str] = None


@router.get("/templates")
def get_report_templates(
    db: Session = Depends(get_db),
):
    """
    List available report templates.

    Returns template names, descriptions, and supported formats.
    """
    builder = ReportBuilder(db)
    templates = builder.get_templates()

    return {
        "templates": templates,
        "total": len(templates),
    }


@router.post("/generate")
def generate_report(
    request: GenerateReportRequest,
    db: Session = Depends(get_db),
):
    """
    Generate a report.

    Supported templates:
    - investor_profile: One-pager with portfolio summary
    - portfolio_detail: Full portfolio breakdown

    Supported formats:
    - html: HTML file (viewable in browser)
    - excel: Excel workbook with multiple sheets

    Example params for investor_profile:
    {"investor_id": 1, "investor_type": "lp"}
    """
    builder = ReportBuilder(db)

    try:
        report = builder.generate(
            template_name=request.template,
            format=request.format,
            params=request.params,
            title=request.title,
        )
        return report

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Report generation failed: {str(e)}"
        )


@router.get("/{report_id}")
def get_report(
    report_id: int,
    db: Session = Depends(get_db),
):
    """
    Get report metadata and status.

    Returns report details including download URL if complete.
    """
    builder = ReportBuilder(db)
    report = builder.get_report(report_id)

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    return report


@router.get("/{report_id}/download")
def download_report(
    report_id: int,
    db: Session = Depends(get_db),
):
    """
    Download a generated report file.

    Returns the report file (HTML or Excel).
    """
    builder = ReportBuilder(db)
    report = builder.get_report(report_id)

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    if report["status"] != "complete":
        raise HTTPException(
            status_code=400, detail=f"Report is not ready: {report['status']}"
        )

    file_path = builder.get_download_path(report_id)
    if not file_path:
        raise HTTPException(status_code=404, detail="Report file not found")

    # Determine media type
    if report["format"] == "excel":
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = f"{report['title']}.xlsx"
    else:
        media_type = "text/html"
        filename = f"{report['title']}.html"

    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename,
    )


@router.get("")
def list_reports(
    template: Optional[str] = Query(None, description="Filter by template"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100, description="Max reports to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: Session = Depends(get_db),
):
    """
    List generated reports.

    Returns report history with pagination.
    """
    builder = ReportBuilder(db)
    return builder.list_reports(
        limit=limit,
        offset=offset,
        template=template,
        status=status,
    )


@router.delete("/{report_id}")
def delete_report(
    report_id: int,
    db: Session = Depends(get_db),
):
    """
    Delete a report.

    Removes the report record and associated file.
    """
    builder = ReportBuilder(db)

    if not builder.get_report(report_id):
        raise HTTPException(status_code=404, detail="Report not found")

    builder.delete_report(report_id)

    return {"message": "Report deleted", "id": report_id}
