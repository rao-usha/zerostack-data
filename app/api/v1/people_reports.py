"""
People Reports API endpoints.

Provides endpoints for generating and exporting reports:
- Management assessment reports
- Peer comparison reports
- Export to JSON/CSV
"""

from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
import io
import csv

from app.core.database import get_db
from app.services.report_service import ReportService


router = APIRouter(prefix="/people-reports", tags=["People Reports"])


# =============================================================================
# Request Models
# =============================================================================


class ManagementAssessmentRequest(BaseModel):
    """Request to generate a management assessment report."""

    company_id: int
    include_bios: bool = Field(True, description="Include executive bios")
    include_experience: bool = Field(True, description="Include work experience")
    include_education: bool = Field(True, description="Include education")


class PeerComparisonRequest(BaseModel):
    """Request to generate a peer comparison report."""

    company_id: int
    peer_set_id: Optional[int] = Field(None, description="Use existing peer set")
    peer_company_ids: Optional[List[int]] = Field(
        None, description="Or specify peer company IDs"
    )


# =============================================================================
# Response Models
# =============================================================================


class ReportMetadata(BaseModel):
    """Report metadata."""

    report_type: str
    generated_at: str
    company_id: int
    company_name: str


class TeamSummary(BaseModel):
    """Team summary stats."""

    total_executives: int
    c_suite_count: int
    vp_count: int
    director_count: int
    board_size: int


class TeamMetrics(BaseModel):
    """Team-level metrics."""

    avg_c_suite_tenure_months: Optional[float] = None
    min_c_suite_tenure_months: Optional[int] = None
    max_c_suite_tenure_months: Optional[int] = None
    has_ceo: bool = False
    has_cfo: bool = False
    has_coo: bool = False
    has_cto: bool = False
    has_cmo: bool = False
    has_chro: bool = False


class ExecutiveProfile(BaseModel):
    """Executive profile in report."""

    name: str
    title: str
    title_normalized: Optional[str] = None
    title_level: Optional[str] = None
    department: Optional[str] = None
    start_date: Optional[str] = None
    tenure_months: Optional[int] = None
    is_board_member: bool = False
    is_board_chair: bool = False
    linkedin_url: Optional[str] = None
    photo_url: Optional[str] = None
    bio: Optional[str] = None
    experience: Optional[List[dict]] = None
    education: Optional[List[dict]] = None


class RecentChange(BaseModel):
    """Recent leadership change."""

    person_name: str
    change_type: str
    old_title: Optional[str] = None
    new_title: Optional[str] = None
    date: Optional[str] = None


class ManagementAssessmentResponse(BaseModel):
    """Management assessment report response."""

    report_type: str
    generated_at: str
    company: dict
    team_summary: TeamSummary
    team_metrics: TeamMetrics
    c_suite: List[ExecutiveProfile]
    vp_level: List[ExecutiveProfile]
    directors: List[ExecutiveProfile]
    board: List[ExecutiveProfile]
    recent_changes: List[RecentChange]
    leadership_gaps: List[str]


class CompanyMetrics(BaseModel):
    """Company leadership metrics."""

    company_id: int
    company_name: str
    total_executives: int
    c_suite_count: int
    vp_count: int
    board_size: int
    avg_c_suite_tenure_months: Optional[float] = None
    changes_12m: int


class PeerAverages(BaseModel):
    """Peer group averages."""

    total_executives: Optional[float] = None
    c_suite_count: Optional[float] = None
    vp_count: Optional[float] = None
    board_size: Optional[float] = None
    avg_c_suite_tenure_months: Optional[float] = None
    changes_12m: Optional[float] = None


class ComparisonInsight(BaseModel):
    """Comparison insight."""

    metric: str
    insight: str
    diff: Optional[float] = None
    diff_months: Optional[float] = None


class Comparison(BaseModel):
    """Comparison results."""

    insights: List[ComparisonInsight]
    metrics_vs_peer_avg: dict


class PeerComparisonResponse(BaseModel):
    """Peer comparison report response."""

    report_type: str
    generated_at: str
    target_company: dict
    peer_companies: List[CompanyMetrics]
    peer_averages: PeerAverages
    comparison: Comparison


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/management-assessment", response_model=ManagementAssessmentResponse)
async def generate_management_assessment(
    request: ManagementAssessmentRequest,
    db: Session = Depends(get_db),
):
    """
    Generate a comprehensive management assessment report.

    Includes:
    - Team overview and structure
    - Executive profiles with optional bios, experience, education
    - Tenure analysis
    - Recent leadership changes
    - Leadership gaps (missing key roles)
    """
    service = ReportService(db)
    report = service.generate_management_assessment(
        company_id=request.company_id,
        include_bios=request.include_bios,
        include_experience=request.include_experience,
        include_education=request.include_education,
    )

    if "error" in report:
        raise HTTPException(status_code=404, detail=report["error"])

    return ManagementAssessmentResponse(
        report_type=report["report_type"],
        generated_at=report["generated_at"],
        company=report["company"],
        team_summary=TeamSummary(**report["team_summary"]),
        team_metrics=TeamMetrics(**report["team_metrics"]),
        c_suite=[ExecutiveProfile(**p) for p in report["c_suite"]],
        vp_level=[ExecutiveProfile(**p) for p in report["vp_level"]],
        directors=[ExecutiveProfile(**p) for p in report["directors"]],
        board=[ExecutiveProfile(**p) for p in report["board"]],
        recent_changes=[RecentChange(**c) for c in report["recent_changes"]],
        leadership_gaps=report["leadership_gaps"],
    )


@router.post("/peer-comparison", response_model=PeerComparisonResponse)
async def generate_peer_comparison(
    request: PeerComparisonRequest,
    db: Session = Depends(get_db),
):
    """
    Generate a peer comparison report.

    Compares a company's leadership against peer companies.
    Peers can be specified via peer_set_id or peer_company_ids.
    If neither is provided, auto-selects peers from same industry.

    Includes:
    - Target company metrics
    - Peer company metrics
    - Peer averages
    - Comparative insights
    """
    service = ReportService(db)
    report = service.generate_peer_comparison(
        company_id=request.company_id,
        peer_set_id=request.peer_set_id,
        peer_company_ids=request.peer_company_ids,
    )

    if "error" in report:
        raise HTTPException(
            status_code=404 if "not found" in report["error"].lower() else 400,
            detail=report["error"],
        )

    return PeerComparisonResponse(
        report_type=report["report_type"],
        generated_at=report["generated_at"],
        target_company=report["target_company"],
        peer_companies=[
            CompanyMetrics(**p) for p in report["peer_companies"] if "error" not in p
        ],
        peer_averages=PeerAverages(**report["peer_averages"]),
        comparison=Comparison(
            insights=[ComparisonInsight(**i) for i in report["comparison"]["insights"]],
            metrics_vs_peer_avg=report["comparison"]["metrics_vs_peer_avg"],
        ),
    )


@router.post("/management-assessment/export/json")
async def export_management_assessment_json(
    request: ManagementAssessmentRequest,
    db: Session = Depends(get_db),
):
    """
    Generate and export management assessment as JSON file.
    """
    service = ReportService(db)
    report = service.generate_management_assessment(
        company_id=request.company_id,
        include_bios=request.include_bios,
        include_experience=request.include_experience,
        include_education=request.include_education,
    )

    if "error" in report:
        raise HTTPException(status_code=404, detail=report["error"])

    service.export_to_json(report)

    # Generate filename
    company_name = report["company"]["name"].replace(" ", "_")[:30]
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    filename = f"management_assessment_{company_name}_{timestamp}.json"

    return JSONResponse(
        content=report,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/management-assessment/export/csv")
async def export_management_assessment_csv(
    request: ManagementAssessmentRequest,
    db: Session = Depends(get_db),
):
    """
    Generate and export management assessment as CSV file.
    """
    service = ReportService(db)
    report = service.generate_management_assessment(
        company_id=request.company_id,
        include_bios=request.include_bios,
        include_experience=request.include_experience,
        include_education=request.include_education,
    )

    if "error" in report:
        raise HTTPException(status_code=404, detail=report["error"])

    # Get CSV rows
    rows = service.export_to_csv_data(report)

    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    for row in rows:
        writer.writerow(row)

    # Generate filename
    company_name = report["company"]["name"].replace(" ", "_")[:30]
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    filename = f"management_assessment_{company_name}_{timestamp}.csv"

    # Return as streaming response
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/peer-comparison/export/json")
async def export_peer_comparison_json(
    request: PeerComparisonRequest,
    db: Session = Depends(get_db),
):
    """
    Generate and export peer comparison as JSON file.
    """
    service = ReportService(db)
    report = service.generate_peer_comparison(
        company_id=request.company_id,
        peer_set_id=request.peer_set_id,
        peer_company_ids=request.peer_company_ids,
    )

    if "error" in report:
        raise HTTPException(status_code=404, detail=report["error"])

    # Generate filename
    company_name = report["target_company"]["name"].replace(" ", "_")[:30]
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    filename = f"peer_comparison_{company_name}_{timestamp}.json"

    return JSONResponse(
        content=report,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/peer-comparison/export/csv")
async def export_peer_comparison_csv(
    request: PeerComparisonRequest,
    db: Session = Depends(get_db),
):
    """
    Generate and export peer comparison as CSV file.
    """
    service = ReportService(db)
    report = service.generate_peer_comparison(
        company_id=request.company_id,
        peer_set_id=request.peer_set_id,
        peer_company_ids=request.peer_company_ids,
    )

    if "error" in report:
        raise HTTPException(status_code=404, detail=report["error"])

    # Get CSV rows
    rows = service.export_to_csv_data(report)

    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    for row in rows:
        writer.writerow(row)

    # Generate filename
    company_name = report["target_company"]["name"].replace(" ", "_")[:30]
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    filename = f"peer_comparison_{company_name}_{timestamp}.csv"

    # Return as streaming response
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
