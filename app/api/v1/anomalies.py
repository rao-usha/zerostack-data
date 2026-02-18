"""
Anomaly Detection API endpoints.

Provides AI-powered anomaly detection:
- Scan for anomalies across data sources
- Get recent and company-specific anomalies
- Investigate anomalies for root causes
- View learned baseline patterns
"""

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

from app.core.database import get_db
from app.agents.anomaly_detector import AnomalyDetectorAgent

router = APIRouter(tags=["Anomaly Detection"])


# =============================================================================
# Request/Response Models
# =============================================================================


class ScanRequest(BaseModel):
    """Request to start anomaly scan."""

    scan_type: str = Field("full", description="Scan type: full, company, sector")
    target: Optional[str] = Field(None, description="Target company or sector name")
    force: bool = Field(False, description="Force re-scan even if recent")


class ScanResponse(BaseModel):
    """Response for scan operations."""

    scan_id: str
    status: str
    records_scanned: int = 0
    anomalies_found: int = 0
    anomalies: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None


class InvestigateRequest(BaseModel):
    """Request to investigate an anomaly."""

    anomaly_id: int
    depth: str = Field(
        "standard", description="Investigation depth: quick, standard, deep"
    )


class UpdateStatusRequest(BaseModel):
    """Request to update anomaly status."""

    status: str = Field(
        ..., description="New status: acknowledged, investigating, resolved"
    )
    resolution_notes: Optional[str] = Field(None, description="Notes for resolution")


class AnomalyResponse(BaseModel):
    """Single anomaly response."""

    id: int
    company_name: str
    anomaly_type: str
    description: Optional[str]
    previous_value: Optional[str]
    current_value: Optional[str]
    change_magnitude: Optional[float]
    severity_score: float
    severity_level: str
    confidence: Optional[float]
    data_source: Optional[str]
    status: str
    detected_at: Optional[str]


class RecentAnomaliesResponse(BaseModel):
    """Response for recent anomalies."""

    anomalies: List[Dict[str, Any]]
    total: int
    by_severity: Dict[str, int]
    by_type: Dict[str, int]


class CompanyAnomaliesResponse(BaseModel):
    """Response for company anomalies."""

    company: str
    anomalies: List[Dict[str, Any]]
    total: int
    unresolved: int
    risk_summary: Dict[str, Any]


class InvestigationResponse(BaseModel):
    """Response for anomaly investigation."""

    anomaly: Dict[str, Any]
    investigation: Dict[str, Any]


class PatternsResponse(BaseModel):
    """Response for baseline patterns."""

    patterns: List[Dict[str, Any]]
    total: int


# =============================================================================
# API Endpoints
# =============================================================================


@router.get("/anomalies/recent", response_model=RecentAnomaliesResponse)
async def get_recent_anomalies(
    hours: int = Query(24, ge=1, le=720, description="Time window in hours"),
    severity: Optional[str] = Query(None, description="Filter by severity level"),
    type: Optional[str] = Query(
        None, alias="type", description="Filter by anomaly type"
    ),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
    db: Session = Depends(get_db),
):
    """
    Get recent anomalies across all companies.

    Returns anomalies detected within the specified time window,
    sorted by severity score.
    """
    detector = AnomalyDetectorAgent(db)
    result = detector.get_recent_anomalies(
        hours=hours, severity=severity, anomaly_type=type, limit=limit
    )

    return RecentAnomaliesResponse(
        anomalies=result.get("anomalies", []),
        total=result.get("total", 0),
        by_severity=result.get("by_severity", {}),
        by_type=result.get("by_type", {}),
    )


@router.get("/anomalies/company/{name}", response_model=CompanyAnomaliesResponse)
async def get_company_anomalies(
    name: str,
    days: int = Query(30, ge=1, le=365, description="Time window in days"),
    status: Optional[str] = Query(None, description="Filter by status"),
    include_resolved: bool = Query(False, description="Include resolved anomalies"),
    db: Session = Depends(get_db),
):
    """
    Get anomalies for a specific company.

    Returns anomalies with risk summary and trend analysis.
    """
    detector = AnomalyDetectorAgent(db)
    result = detector.get_company_anomalies(
        company_name=name, days=days, status=status, include_resolved=include_resolved
    )

    return CompanyAnomaliesResponse(
        company=result.get("company", name),
        anomalies=result.get("anomalies", []),
        total=result.get("total", 0),
        unresolved=result.get("unresolved", 0),
        risk_summary=result.get("risk_summary", {}),
    )


@router.post("/anomalies/investigate", response_model=InvestigationResponse)
async def investigate_anomaly(
    request: InvestigateRequest, db: Session = Depends(get_db)
):
    """
    Deep investigation of an anomaly.

    Analyzes probable causes, correlated anomalies,
    historical context, and provides recommendations.
    """
    detector = AnomalyDetectorAgent(db)
    result = detector.investigate(anomaly_id=request.anomaly_id, depth=request.depth)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return InvestigationResponse(
        anomaly=result.get("anomaly", {}), investigation=result.get("investigation", {})
    )


@router.get("/anomalies/patterns", response_model=PatternsResponse)
async def get_patterns(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    entity_name: Optional[str] = Query(None, description="Filter by entity name"),
    metric: Optional[str] = Query(None, description="Filter by metric name"),
    limit: int = Query(100, ge=1, le=500, description="Max results"),
    db: Session = Depends(get_db),
):
    """
    Get learned baseline patterns.

    Shows normal value ranges per company/sector/metric
    used for anomaly detection.
    """
    detector = AnomalyDetectorAgent(db)
    result = detector.get_patterns(
        entity_type=entity_type, entity_name=entity_name, metric=metric, limit=limit
    )

    return PatternsResponse(
        patterns=result.get("patterns", []), total=result.get("total", 0)
    )


@router.post("/anomalies/scan", response_model=ScanResponse)
async def start_anomaly_scan(
    request: ScanRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Start an anomaly detection scan.

    Scans across all data sources to identify new anomalies.
    """
    detector = AnomalyDetectorAgent(db)

    # Run scan (synchronously for now - could be made async)
    result = detector.scan_for_anomalies(
        scan_type=request.scan_type, target=request.target, force=request.force
    )

    return ScanResponse(
        scan_id=result.get("scan_id", ""),
        status=result.get("status", "unknown"),
        records_scanned=result.get("records_scanned", 0),
        anomalies_found=result.get("anomalies_found", 0),
        anomalies=result.get("anomalies"),
        error=result.get("error"),
    )


@router.get("/anomalies/scan/{scan_id}")
async def get_scan_status(scan_id: str, db: Session = Depends(get_db)):
    """
    Get status of an anomaly scan.
    """
    detector = AnomalyDetectorAgent(db)
    result = detector.get_scan_status(scan_id)

    if not result:
        raise HTTPException(status_code=404, detail=f"Scan not found: {scan_id}")

    return result


@router.patch("/anomalies/{anomaly_id}")
async def update_anomaly(
    anomaly_id: int, request: UpdateStatusRequest, db: Session = Depends(get_db)
):
    """
    Update anomaly status (acknowledge, investigate, resolve).
    """
    detector = AnomalyDetectorAgent(db)
    result = detector.update_anomaly_status(
        anomaly_id=anomaly_id,
        status=request.status,
        resolution_notes=request.resolution_notes,
    )

    if not result:
        raise HTTPException(status_code=404, detail=f"Anomaly not found: {anomaly_id}")

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


@router.get("/anomalies/{anomaly_id}")
async def get_anomaly(anomaly_id: int, db: Session = Depends(get_db)):
    """
    Get a single anomaly by ID.
    """
    detector = AnomalyDetectorAgent(db)

    # Use investigate with quick depth to get anomaly details
    result = detector.investigate(anomaly_id=anomaly_id, depth="quick")

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return result.get("anomaly", {})


@router.post("/anomalies/learn-baseline")
async def learn_baseline(
    entity_type: str = Query(..., description="Entity type (company, sector)"),
    entity_name: str = Query(..., description="Entity name"),
    metric: str = Query(..., description="Metric name"),
    values: List[float] = [],
    db: Session = Depends(get_db),
):
    """
    Learn baseline pattern for a metric.

    Provide historical values to establish normal range.
    """
    if not values:
        raise HTTPException(status_code=400, detail="Values list required")

    detector = AnomalyDetectorAgent(db)
    result = detector.learn_baseline(
        entity_type=entity_type, entity_name=entity_name, metric=metric, values=values
    )

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result
