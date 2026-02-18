"""
Report Generation API endpoints.

Provides AI-powered report generation:
- Generate comprehensive reports
- Multiple report types and templates
- Export to various formats
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

from app.core.database import get_db
from app.agents.report_writer import ReportWriterAgent

router = APIRouter(prefix="/ai-reports", tags=["Report Generation"])


# =============================================================================
# Request/Response Models
# =============================================================================


class GenerateReportRequest(BaseModel):
    """Request to generate a report."""

    report_type: str = Field(
        ..., description="Type: company_profile, due_diligence, competitive_landscape"
    )
    entity_name: str = Field(..., description="Target entity name")
    template: str = Field("full_report", description="Template name")
    options: Optional[Dict[str, Any]] = Field(None, description="Additional options")


class GenerateReportResponse(BaseModel):
    """Response for report generation."""

    report_id: str
    status: str
    error: Optional[str] = None


class ReportStatusResponse(BaseModel):
    """Response for report status."""

    report_id: str
    status: str
    progress: int
    error: Optional[str] = None


class CreateTemplateRequest(BaseModel):
    """Request to create a custom template."""

    name: str = Field(..., description="Template name")
    description: str = Field(..., description="Template description")
    sections: List[str] = Field(..., description="Sections to include")
    tone: str = Field(
        "formal", description="Tone: executive, formal, professional, casual"
    )
    detail_level: str = Field(
        "standard", description="Detail: summary, standard, detailed"
    )
    max_words: int = Field(2000, ge=100, le=10000, description="Maximum word count")
    report_type: Optional[str] = Field(None, description="Restrict to report type")


class TemplateResponse(BaseModel):
    """Response for template."""

    name: str
    description: str
    sections: List[str]
    tone: str
    detail_level: str
    max_words: int
    is_default: bool = False


# =============================================================================
# API Endpoints - Static routes first, then parameterized routes
# =============================================================================


@router.post("/generate", response_model=GenerateReportResponse)
async def generate_report(
    request: GenerateReportRequest, db: Session = Depends(get_db)
):
    """
    Generate a comprehensive report.

    Report types:
    - `company_profile`: Deep-dive company analysis
    - `due_diligence`: Investment DD report
    - `competitive_landscape`: Competitive analysis
    - `portfolio_summary`: Portfolio overview
    - `investor_profile`: Investor analysis
    - `market_overview`: Sector/market analysis
    """
    writer = ReportWriterAgent(db)

    result = writer.generate_report(
        report_type=request.report_type,
        entity_name=request.entity_name,
        template_name=request.template,
        options=request.options or {},
    )

    if "error" in result:
        return GenerateReportResponse(
            report_id=result.get("report_id", ""),
            status="failed",
            error=result["error"],
        )

    return GenerateReportResponse(
        report_id=result.get("report_id", ""), status=result.get("status", "completed")
    )


@router.get("/templates", response_model=List[TemplateResponse])
async def list_templates(db: Session = Depends(get_db)):
    """
    List available report templates.

    Includes both built-in and custom templates.
    """
    writer = ReportWriterAgent(db)
    templates = writer.get_templates()

    return [
        TemplateResponse(
            name=t["name"],
            description=t.get("description", ""),
            sections=t.get("sections", []),
            tone=t.get("tone", "formal"),
            detail_level=t.get("detail_level", "standard"),
            max_words=t.get("max_words", 2000),
            is_default=t.get("is_default", False),
        )
        for t in templates
    ]


@router.post("/templates")
async def create_template(
    request: CreateTemplateRequest, db: Session = Depends(get_db)
):
    """
    Create a custom report template.

    Templates define which sections to include, tone, and detail level.
    """
    writer = ReportWriterAgent(db)
    result = writer.create_template(
        name=request.name,
        description=request.description,
        sections=request.sections,
        tone=request.tone,
        detail_level=request.detail_level,
        max_words=request.max_words,
        report_type=request.report_type,
    )

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


@router.get("/templates/{name}")
async def get_template(name: str, db: Session = Depends(get_db)):
    """
    Get a specific template by name.
    """
    writer = ReportWriterAgent(db)
    template = writer.get_template(name)

    if not template:
        raise HTTPException(status_code=404, detail=f"Template not found: {name}")

    return template


@router.get("/list")
async def list_reports(
    report_type: Optional[str] = Query(None, description="Filter by report type"),
    entity_name: Optional[str] = Query(None, description="Filter by entity name"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
    db: Session = Depends(get_db),
):
    """
    List generated reports.

    Returns metadata for all matching reports.
    """
    writer = ReportWriterAgent(db)
    result = writer.list_reports(
        report_type=report_type, entity_name=entity_name, status=status, limit=limit
    )

    return result


@router.get("/{report_id}")
async def get_report(
    report_id: str,
    include_content: bool = Query(True, description="Include full content"),
    db: Session = Depends(get_db),
):
    """
    Get a generated report.

    Returns the full report with content in JSON, markdown, and HTML formats.
    """
    writer = ReportWriterAgent(db)
    report = writer.get_report(report_id)

    if not report:
        raise HTTPException(status_code=404, detail=f"Report not found: {report_id}")

    if not include_content:
        # Return metadata only
        return {
            "report_id": report["report_id"],
            "report_type": report["report_type"],
            "title": report["title"],
            "entity_name": report["entity_name"],
            "status": report["status"],
            "word_count": report["word_count"],
            "confidence": report["confidence"],
            "created_at": report["created_at"],
            "completed_at": report["completed_at"],
        }

    return report


@router.get("/{report_id}/status", response_model=ReportStatusResponse)
async def get_report_status(report_id: str, db: Session = Depends(get_db)):
    """
    Get report generation status.

    Use this to poll for completion during long-running generation.
    """
    writer = ReportWriterAgent(db)
    status = writer.get_report_status(report_id)

    if not status:
        raise HTTPException(status_code=404, detail=f"Report not found: {report_id}")

    return ReportStatusResponse(
        report_id=status["report_id"],
        status=status["status"],
        progress=status["progress"],
        error=status.get("error"),
    )


@router.get("/{report_id}/export")
async def export_report(
    report_id: str,
    format: str = Query("markdown", description="Export format: markdown, html, json"),
    db: Session = Depends(get_db),
):
    """
    Export report in specified format.

    Formats:
    - `markdown`: Plain markdown text
    - `html`: Rendered HTML document
    - `json`: Structured JSON content
    """
    writer = ReportWriterAgent(db)
    content = writer.export_report(report_id, format)

    if not content:
        raise HTTPException(status_code=404, detail=f"Report not found: {report_id}")

    # Set content type based on format
    content_types = {
        "markdown": "text/markdown",
        "html": "text/html",
        "json": "application/json",
    }
    content_type = content_types.get(format, "text/plain")

    # Set filename
    extensions = {
        "markdown": "md",
        "html": "html",
        "json": "json",
    }
    ext = extensions.get(format, "txt")

    return Response(
        content=content,
        media_type=content_type,
        headers={
            "Content-Disposition": f"attachment; filename=report_{report_id}.{ext}"
        },
    )
