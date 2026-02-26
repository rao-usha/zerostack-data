"""
Data quality rules management endpoints.

Provides API for creating, managing, and evaluating data quality rules,
data profiling, anomaly detection, cross-source validation, and quality trending.
"""

import logging
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import (
    DataQualityRule,
    DataQualityResult,
    DataQualityReport,
    DataProfileSnapshot,
    DataProfileColumn,
    DQAnomalyAlert,
    DQAnomalyThreshold,
    DQCrossSourceValidation,
    DQCrossSourceResult,
    DQQualitySnapshot,
    DQSLATarget,
    AnomalyAlertType,
    RuleType,
    RuleSeverity,
    IngestionJob,
)
from app.core import data_quality_service
from app.core import data_profiling_service
from app.core import anomaly_detection_service
from app.core import cross_source_validation_service
from app.core import quality_trending_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data-quality", tags=["data-quality"])


# =============================================================================
# Pydantic Schemas
# =============================================================================


class RuleCreate(BaseModel):
    """Request schema for creating a data quality rule."""

    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    source: Optional[str] = Field(
        default=None, description="Data source (null = all sources)"
    )
    dataset_pattern: Optional[str] = Field(
        default=None, description="Regex pattern for dataset names"
    )
    column_name: Optional[str] = Field(
        default=None, description="Column to check (null = table-level)"
    )
    rule_type: str = Field(
        ...,
        description="Rule type: range, not_null, unique, regex, freshness, row_count, enum",
    )
    severity: str = Field(default="error", description="Severity: error, warning, info")
    parameters: Dict[str, Any] = Field(
        default_factory=dict, description="Rule-specific parameters"
    )
    priority: int = Field(
        default=5, ge=1, le=10, description="Priority (1=highest, 10=lowest)"
    )


class RuleUpdate(BaseModel):
    """Request schema for updating a data quality rule."""

    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = None
    source: Optional[str] = None
    dataset_pattern: Optional[str] = None
    column_name: Optional[str] = None
    severity: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    priority: Optional[int] = Field(default=None, ge=1, le=10)
    is_enabled: Optional[bool] = None


class RuleResponse(BaseModel):
    """Response schema for data quality rule."""

    id: int
    name: str
    description: Optional[str]
    source: Optional[str]
    dataset_pattern: Optional[str]
    column_name: Optional[str]
    rule_type: str
    severity: str
    parameters: Dict[str, Any]
    is_enabled: bool
    priority: int
    times_evaluated: int
    times_passed: int
    times_failed: int
    last_evaluated_at: Optional[str]
    created_at: str
    updated_at: str


class ResultResponse(BaseModel):
    """Response schema for rule evaluation result."""

    id: int
    rule_id: int
    job_id: Optional[int]
    source: str
    dataset_name: Optional[str]
    column_name: Optional[str]
    passed: bool
    severity: str
    message: Optional[str]
    actual_value: Optional[str]
    expected_value: Optional[str]
    sample_failures: Optional[List[Any]]
    rows_checked: Optional[int]
    rows_passed: Optional[int]
    rows_failed: Optional[int]
    execution_time_ms: Optional[int]
    evaluated_at: str


class ReportResponse(BaseModel):
    """Response schema for data quality report."""

    id: int
    job_id: Optional[int]
    source: Optional[str]
    report_type: str
    total_rules: int
    rules_passed: int
    rules_failed: int
    rules_warned: int
    errors: int
    warnings: int
    info: int
    overall_status: str
    failed_rules: Optional[List[Dict[str, Any]]]
    execution_time_ms: Optional[int]
    started_at: str
    completed_at: Optional[str]


class EvaluateRequest(BaseModel):
    """Request schema for manual rule evaluation."""

    table_name: str = Field(..., description="Table name to evaluate")
    source: str = Field(..., description="Data source")


# =============================================================================
# Rule CRUD Endpoints
# =============================================================================


@router.get("/rules", response_model=List[RuleResponse])
def list_rules(
    source: Optional[str] = Query(default=None, description="Filter by source"),
    rule_type: Optional[str] = Query(default=None, description="Filter by rule type"),
    enabled_only: bool = Query(default=False, description="Only show enabled rules"),
    db: Session = Depends(get_db),
) -> List[RuleResponse]:
    """
    List all data quality rules.
    """
    query = db.query(DataQualityRule)

    if source:
        query = query.filter(DataQualityRule.source == source)
    if rule_type:
        query = query.filter(DataQualityRule.rule_type == rule_type)
    if enabled_only:
        query = query.filter(DataQualityRule.is_enabled == 1)

    rules = query.order_by(DataQualityRule.priority, DataQualityRule.name).all()

    return [
        RuleResponse(
            id=r.id,
            name=r.name,
            description=r.description,
            source=r.source,
            dataset_pattern=r.dataset_pattern,
            column_name=r.column_name,
            rule_type=r.rule_type.value,
            severity=r.severity.value,
            parameters=r.parameters or {},
            is_enabled=bool(r.is_enabled),
            priority=r.priority,
            times_evaluated=r.times_evaluated,
            times_passed=r.times_passed,
            times_failed=r.times_failed,
            last_evaluated_at=r.last_evaluated_at.isoformat()
            if r.last_evaluated_at
            else None,
            created_at=r.created_at.isoformat(),
            updated_at=r.updated_at.isoformat(),
        )
        for r in rules
    ]


@router.get("/rules/types")
def list_rule_types():
    """
    List available rule types and their parameters.
    """
    return {
        "rule_types": [
            {
                "type": "range",
                "description": "Value must be within min/max range",
                "parameters": {"min": "number (optional)", "max": "number (optional)"},
                "requires_column": True,
                "example": {"min": 0, "max": 100},
            },
            {
                "type": "not_null",
                "description": "Value must not be null",
                "parameters": {},
                "requires_column": True,
                "example": {},
            },
            {
                "type": "unique",
                "description": "Values must be unique (no duplicates)",
                "parameters": {},
                "requires_column": True,
                "example": {},
            },
            {
                "type": "regex",
                "description": "Value must match regex pattern",
                "parameters": {"pattern": "regex string"},
                "requires_column": True,
                "example": {"pattern": "^[A-Z]{2}$"},
            },
            {
                "type": "freshness",
                "description": "Data must be recent (date/timestamp column)",
                "parameters": {
                    "max_age_hours": "number",
                    "max_age_days": "number (alternative)",
                },
                "requires_column": True,
                "example": {"max_age_hours": 24},
            },
            {
                "type": "row_count",
                "description": "Table must have min/max row count",
                "parameters": {"min": "number (optional)", "max": "number (optional)"},
                "requires_column": False,
                "example": {"min": 1, "max": 1000000},
            },
            {
                "type": "enum",
                "description": "Value must be in allowed list",
                "parameters": {"allowed": "array of values"},
                "requires_column": True,
                "example": {"allowed": ["A", "B", "C"]},
            },
        ],
        "severities": ["error", "warning", "info"],
    }


@router.post("/rules", response_model=RuleResponse, status_code=201)
def create_rule(
    rule_request: RuleCreate, db: Session = Depends(get_db)
) -> RuleResponse:
    """
    Create a new data quality rule.

    Example rules:
    - GDP must be positive: `{"name": "gdp_positive", "rule_type": "range", "column_name": "gdp", "parameters": {"min": 0}}`
    - State code format: `{"name": "state_format", "rule_type": "regex", "column_name": "state", "parameters": {"pattern": "^[A-Z]{2}$"}}`
    - Data freshness: `{"name": "recent_data", "rule_type": "freshness", "column_name": "created_at", "parameters": {"max_age_hours": 24}}`
    """
    # Validate rule type
    try:
        rule_type = RuleType(rule_request.rule_type)
    except ValueError:
        valid_types = [t.value for t in RuleType]
        raise HTTPException(
            status_code=400, detail=f"Invalid rule_type. Must be one of: {valid_types}"
        )

    # Validate severity
    try:
        severity = RuleSeverity(rule_request.severity)
    except ValueError:
        valid_severities = [s.value for s in RuleSeverity]
        raise HTTPException(
            status_code=400,
            detail=f"Invalid severity. Must be one of: {valid_severities}",
        )

    # Check for duplicate name
    existing = (
        db.query(DataQualityRule)
        .filter(DataQualityRule.name == rule_request.name)
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=400, detail="Rule with this name already exists"
        )

    rule = data_quality_service.create_rule(
        db=db,
        name=rule_request.name,
        rule_type=rule_type,
        severity=severity,
        source=rule_request.source,
        dataset_pattern=rule_request.dataset_pattern,
        column_name=rule_request.column_name,
        parameters=rule_request.parameters,
        description=rule_request.description,
        priority=rule_request.priority,
    )

    return RuleResponse(
        id=rule.id,
        name=rule.name,
        description=rule.description,
        source=rule.source,
        dataset_pattern=rule.dataset_pattern,
        column_name=rule.column_name,
        rule_type=rule.rule_type.value,
        severity=rule.severity.value,
        parameters=rule.parameters or {},
        is_enabled=bool(rule.is_enabled),
        priority=rule.priority,
        times_evaluated=rule.times_evaluated,
        times_passed=rule.times_passed,
        times_failed=rule.times_failed,
        last_evaluated_at=rule.last_evaluated_at.isoformat()
        if rule.last_evaluated_at
        else None,
        created_at=rule.created_at.isoformat(),
        updated_at=rule.updated_at.isoformat(),
    )


@router.get("/rules/{rule_id}", response_model=RuleResponse)
def get_rule(rule_id: int, db: Session = Depends(get_db)) -> RuleResponse:
    """Get a specific rule by ID."""
    rule = db.query(DataQualityRule).filter(DataQualityRule.id == rule_id).first()

    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    return RuleResponse(
        id=rule.id,
        name=rule.name,
        description=rule.description,
        source=rule.source,
        dataset_pattern=rule.dataset_pattern,
        column_name=rule.column_name,
        rule_type=rule.rule_type.value,
        severity=rule.severity.value,
        parameters=rule.parameters or {},
        is_enabled=bool(rule.is_enabled),
        priority=rule.priority,
        times_evaluated=rule.times_evaluated,
        times_passed=rule.times_passed,
        times_failed=rule.times_failed,
        last_evaluated_at=rule.last_evaluated_at.isoformat()
        if rule.last_evaluated_at
        else None,
        created_at=rule.created_at.isoformat(),
        updated_at=rule.updated_at.isoformat(),
    )


@router.patch("/rules/{rule_id}", response_model=RuleResponse)
def update_rule(
    rule_id: int, rule_update: RuleUpdate, db: Session = Depends(get_db)
) -> RuleResponse:
    """Update a data quality rule."""
    rule = db.query(DataQualityRule).filter(DataQualityRule.id == rule_id).first()

    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    # Update fields
    if rule_update.name is not None:
        rule.name = rule_update.name
    if rule_update.description is not None:
        rule.description = rule_update.description
    if rule_update.source is not None:
        rule.source = rule_update.source
    if rule_update.dataset_pattern is not None:
        rule.dataset_pattern = rule_update.dataset_pattern
    if rule_update.column_name is not None:
        rule.column_name = rule_update.column_name
    if rule_update.severity is not None:
        try:
            rule.severity = RuleSeverity(rule_update.severity)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid severity")
    if rule_update.parameters is not None:
        rule.parameters = rule_update.parameters
    if rule_update.priority is not None:
        rule.priority = rule_update.priority
    if rule_update.is_enabled is not None:
        rule.is_enabled = 1 if rule_update.is_enabled else 0

    db.commit()
    db.refresh(rule)

    return RuleResponse(
        id=rule.id,
        name=rule.name,
        description=rule.description,
        source=rule.source,
        dataset_pattern=rule.dataset_pattern,
        column_name=rule.column_name,
        rule_type=rule.rule_type.value,
        severity=rule.severity.value,
        parameters=rule.parameters or {},
        is_enabled=bool(rule.is_enabled),
        priority=rule.priority,
        times_evaluated=rule.times_evaluated,
        times_passed=rule.times_passed,
        times_failed=rule.times_failed,
        last_evaluated_at=rule.last_evaluated_at.isoformat()
        if rule.last_evaluated_at
        else None,
        created_at=rule.created_at.isoformat(),
        updated_at=rule.updated_at.isoformat(),
    )


@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    """Delete a data quality rule."""
    rule = db.query(DataQualityRule).filter(DataQualityRule.id == rule_id).first()

    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    db.delete(rule)
    db.commit()

    return {"message": f"Rule '{rule.name}' deleted"}


@router.post("/rules/{rule_id}/enable")
def enable_rule(rule_id: int, db: Session = Depends(get_db)):
    """Enable a data quality rule."""
    rule = db.query(DataQualityRule).filter(DataQualityRule.id == rule_id).first()

    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    rule.is_enabled = 1
    db.commit()

    return {"message": f"Rule '{rule.name}' enabled"}


@router.post("/rules/{rule_id}/disable")
def disable_rule(rule_id: int, db: Session = Depends(get_db)):
    """Disable a data quality rule."""
    rule = db.query(DataQualityRule).filter(DataQualityRule.id == rule_id).first()

    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    rule.is_enabled = 0
    db.commit()

    return {"message": f"Rule '{rule.name}' disabled"}


# =============================================================================
# Evaluation Endpoints
# =============================================================================


@router.post("/evaluate", response_model=ReportResponse)
def evaluate_table(
    request: EvaluateRequest, db: Session = Depends(get_db)
) -> ReportResponse:
    """
    Manually evaluate data quality rules against a table.

    This runs all applicable rules for the given source and table.
    """
    # Create a temporary job record for tracking
    job = IngestionJob(
        source=request.source,
        status="success",  # Pretend it's a completed job
        config={"table_name": request.table_name, "manual_evaluation": True},
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        report = data_quality_service.evaluate_rules_for_job(
            db=db, job=job, table_name=request.table_name
        )

        return ReportResponse(
            id=report.id,
            job_id=report.job_id,
            source=report.source,
            report_type=report.report_type,
            total_rules=report.total_rules,
            rules_passed=report.rules_passed,
            rules_failed=report.rules_failed,
            rules_warned=report.rules_warned,
            errors=report.errors,
            warnings=report.warnings,
            info=report.info,
            overall_status=report.overall_status,
            failed_rules=report.failed_rules,
            execution_time_ms=report.execution_time_ms,
            started_at=report.started_at.isoformat(),
            completed_at=report.completed_at.isoformat()
            if report.completed_at
            else None,
        )

    except Exception as e:
        logger.error(f"Error evaluating rules: {e}")
        raise HTTPException(status_code=500, detail=f"Error evaluating rules: {str(e)}")


@router.post("/rules/{rule_id}/evaluate")
def evaluate_single_rule(
    rule_id: int, request: EvaluateRequest, db: Session = Depends(get_db)
):
    """
    Evaluate a single rule against a table.
    """
    rule = db.query(DataQualityRule).filter(DataQualityRule.id == rule_id).first()

    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    try:
        result = data_quality_service.evaluate_rule(db, rule, request.table_name)

        return {
            "rule_id": result.rule_id,
            "rule_name": result.rule_name,
            "passed": result.passed,
            "severity": result.severity.value,
            "message": result.message,
            "actual_value": result.actual_value,
            "expected_value": result.expected_value,
            "rows_checked": result.rows_checked,
            "rows_passed": result.rows_passed,
            "rows_failed": result.rows_failed,
            "sample_failures": result.sample_failures,
            "execution_time_ms": result.execution_time_ms,
        }

    except Exception as e:
        logger.error(f"Error evaluating rule: {e}")
        raise HTTPException(status_code=500, detail=f"Error evaluating rule: {str(e)}")


# =============================================================================
# Results & Reports Endpoints
# =============================================================================


@router.get("/results", response_model=List[ResultResponse])
def list_results(
    rule_id: Optional[int] = Query(default=None, description="Filter by rule ID"),
    job_id: Optional[int] = Query(default=None, description="Filter by job ID"),
    passed: Optional[bool] = Query(default=None, description="Filter by passed status"),
    limit: int = Query(default=100, ge=1, le=1000),
    db: Session = Depends(get_db),
) -> List[ResultResponse]:
    """
    List rule evaluation results.
    """
    results = data_quality_service.get_rule_results(
        db=db, rule_id=rule_id, job_id=job_id, passed_only=passed, limit=limit
    )

    return [
        ResultResponse(
            id=r.id,
            rule_id=r.rule_id,
            job_id=r.job_id,
            source=r.source,
            dataset_name=r.dataset_name,
            column_name=r.column_name,
            passed=bool(r.passed),
            severity=r.severity.value,
            message=r.message,
            actual_value=r.actual_value,
            expected_value=r.expected_value,
            sample_failures=r.sample_failures,
            rows_checked=r.rows_checked,
            rows_passed=r.rows_passed,
            rows_failed=r.rows_failed,
            execution_time_ms=r.execution_time_ms,
            evaluated_at=r.evaluated_at.isoformat(),
        )
        for r in results
    ]


@router.get("/reports", response_model=List[ReportResponse])
def list_reports(
    job_id: Optional[int] = Query(default=None, description="Filter by job ID"),
    source: Optional[str] = Query(default=None, description="Filter by source"),
    status: Optional[str] = Query(default=None, description="Filter by status"),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> List[ReportResponse]:
    """
    List data quality reports.
    """
    query = db.query(DataQualityReport)

    if job_id:
        query = query.filter(DataQualityReport.job_id == job_id)
    if source:
        query = query.filter(DataQualityReport.source == source)
    if status:
        query = query.filter(DataQualityReport.overall_status == status)

    reports = query.order_by(DataQualityReport.started_at.desc()).limit(limit).all()

    return [
        ReportResponse(
            id=r.id,
            job_id=r.job_id,
            source=r.source,
            report_type=r.report_type,
            total_rules=r.total_rules,
            rules_passed=r.rules_passed,
            rules_failed=r.rules_failed,
            rules_warned=r.rules_warned,
            errors=r.errors,
            warnings=r.warnings,
            info=r.info,
            overall_status=r.overall_status,
            failed_rules=r.failed_rules,
            execution_time_ms=r.execution_time_ms,
            started_at=r.started_at.isoformat(),
            completed_at=r.completed_at.isoformat() if r.completed_at else None,
        )
        for r in reports
    ]


@router.get("/reports/{report_id}", response_model=ReportResponse)
def get_report(report_id: int, db: Session = Depends(get_db)) -> ReportResponse:
    """Get a specific data quality report."""
    report = data_quality_service.get_report(db, report_id)

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    return ReportResponse(
        id=report.id,
        job_id=report.job_id,
        source=report.source,
        report_type=report.report_type,
        total_rules=report.total_rules,
        rules_passed=report.rules_passed,
        rules_failed=report.rules_failed,
        rules_warned=report.rules_warned,
        errors=report.errors,
        warnings=report.warnings,
        info=report.info,
        overall_status=report.overall_status,
        failed_rules=report.failed_rules,
        execution_time_ms=report.execution_time_ms,
        started_at=report.started_at.isoformat(),
        completed_at=report.completed_at.isoformat() if report.completed_at else None,
    )


@router.get("/reports/job/{job_id}", response_model=ReportResponse)
def get_job_report(job_id: int, db: Session = Depends(get_db)) -> ReportResponse:
    """Get the data quality report for a specific job."""
    report = data_quality_service.get_job_report(db, job_id)

    if not report:
        raise HTTPException(status_code=404, detail="No report found for this job")

    return ReportResponse(
        id=report.id,
        job_id=report.job_id,
        source=report.source,
        report_type=report.report_type,
        total_rules=report.total_rules,
        rules_passed=report.rules_passed,
        rules_failed=report.rules_failed,
        rules_warned=report.rules_warned,
        errors=report.errors,
        warnings=report.warnings,
        info=report.info,
        overall_status=report.overall_status,
        failed_rules=report.failed_rules,
        execution_time_ms=report.execution_time_ms,
        started_at=report.started_at.isoformat(),
        completed_at=report.completed_at.isoformat() if report.completed_at else None,
    )


@router.get("/reports/{report_id}/results", response_model=List[ResultResponse])
def get_report_results(
    report_id: int, db: Session = Depends(get_db)
) -> List[ResultResponse]:
    """Get all rule results for a specific report."""
    report = data_quality_service.get_report(db, report_id)

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    results = (
        db.query(DataQualityResult)
        .filter(DataQualityResult.job_id == report.job_id)
        .all()
    )

    return [
        ResultResponse(
            id=r.id,
            rule_id=r.rule_id,
            job_id=r.job_id,
            source=r.source,
            dataset_name=r.dataset_name,
            column_name=r.column_name,
            passed=bool(r.passed),
            severity=r.severity.value,
            message=r.message,
            actual_value=r.actual_value,
            expected_value=r.expected_value,
            sample_failures=r.sample_failures,
            rows_checked=r.rows_checked,
            rows_passed=r.rows_passed,
            rows_failed=r.rows_failed,
            execution_time_ms=r.execution_time_ms,
            evaluated_at=r.evaluated_at.isoformat(),
        )
        for r in results
    ]


# =============================================================================
# Pydantic Schemas — Profiling
# =============================================================================


class ProfileSnapshotResponse(BaseModel):
    """Response schema for a data profile snapshot."""

    id: int
    table_name: str
    source: Optional[str]
    domain: Optional[str]
    job_id: Optional[int]
    row_count: int
    column_count: int
    total_null_count: int
    overall_completeness_pct: Optional[float]
    schema_snapshot: Optional[List[Dict[str, Any]]]
    profiled_at: str
    execution_time_ms: Optional[int]


class ProfileColumnResponse(BaseModel):
    """Response schema for column-level profile stats."""

    id: int
    snapshot_id: int
    column_name: str
    data_type: Optional[str]
    null_count: int
    null_pct: Optional[float]
    distinct_count: Optional[int]
    cardinality_ratio: Optional[float]
    stats: Optional[Dict[str, Any]]


# =============================================================================
# Pydantic Schemas — Anomalies
# =============================================================================


class AnomalyAlertResponse(BaseModel):
    """Response schema for an anomaly alert."""

    id: int
    table_name: str
    source: Optional[str]
    column_name: Optional[str]
    alert_type: str
    status: str
    severity: str
    message: Optional[str]
    current_value: Optional[str]
    baseline_value: Optional[str]
    deviation_sigma: Optional[float]
    details: Optional[Dict[str, Any]]
    snapshot_id: Optional[int]
    job_id: Optional[int]
    resolved_at: Optional[str]
    resolution_notes: Optional[str]
    detected_at: str


class AnomalyThresholdCreate(BaseModel):
    """Request schema for creating/updating anomaly thresholds."""

    source: Optional[str] = None
    table_pattern: Optional[str] = None
    row_count_sigma: float = 2.0
    null_rate_sigma: float = 2.0
    distribution_sigma: float = 3.0
    schema_drift_enabled: bool = True
    is_enabled: bool = True


class AnomalyThresholdResponse(BaseModel):
    """Response schema for anomaly thresholds."""

    id: int
    source: Optional[str]
    table_pattern: Optional[str]
    row_count_sigma: float
    null_rate_sigma: float
    distribution_sigma: float
    schema_drift_enabled: bool
    is_enabled: bool


class AnomalyResolveRequest(BaseModel):
    """Request schema for acknowledging/resolving an anomaly."""

    notes: Optional[str] = None


# =============================================================================
# Pydantic Schemas — Cross-Source Validation
# =============================================================================


class CrossSourceValidationCreate(BaseModel):
    """Request schema for creating a cross-source validation."""

    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    validation_type: str = Field(
        ..., description="Type: fips_consistency, identifier_match, geo_coherence, temporal_consistency"
    )
    config: Dict[str, Any] = Field(..., description="Validation config with left/right tables")
    severity: str = Field(default="warning")
    is_enabled: bool = True


class CrossSourceValidationUpdate(BaseModel):
    """Request schema for updating a cross-source validation."""

    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    severity: Optional[str] = None
    is_enabled: Optional[bool] = None


class CrossSourceValidationResponse(BaseModel):
    """Response schema for a cross-source validation."""

    id: int
    name: str
    description: Optional[str]
    validation_type: str
    config: Dict[str, Any]
    severity: str
    is_enabled: bool
    times_evaluated: int
    last_evaluated_at: Optional[str]
    last_pass_rate: Optional[float]


class CrossSourceResultResponse(BaseModel):
    """Response schema for a cross-source validation result."""

    id: int
    validation_id: int
    passed: bool
    left_count: Optional[int]
    right_count: Optional[int]
    matched_count: Optional[int]
    orphan_left: Optional[int]
    orphan_right: Optional[int]
    match_rate: Optional[float]
    sample_orphans: Optional[List[str]]
    message: Optional[str]
    execution_time_ms: Optional[int]
    evaluated_at: str


# =============================================================================
# Pydantic Schemas — Quality Trending
# =============================================================================


class QualitySnapshotResponse(BaseModel):
    """Response schema for a quality trend snapshot."""

    id: int
    snapshot_date: str
    source: Optional[str]
    table_name: Optional[str]
    domain: Optional[str]
    quality_score: Optional[float]
    completeness_score: Optional[float]
    freshness_score: Optional[float]
    validity_score: Optional[float]
    consistency_score: Optional[float]
    row_count: Optional[int]
    rule_pass_rate: Optional[float]
    anomaly_count: Optional[int]


class SLATargetCreate(BaseModel):
    """Request schema for creating/updating SLA targets."""

    source: Optional[str] = None
    table_pattern: Optional[str] = None
    target_quality_score: float = 80.0
    target_completeness: float = 85.0
    target_freshness: float = 90.0
    target_validity: float = 90.0
    consecutive_drops_threshold: int = 3
    is_enabled: bool = True


class SLATargetResponse(BaseModel):
    """Response schema for SLA target."""

    id: int
    source: Optional[str]
    table_pattern: Optional[str]
    target_quality_score: float
    target_completeness: float
    target_freshness: float
    target_validity: float
    consecutive_drops_threshold: int
    is_enabled: bool


# =============================================================================
# Profiling Endpoints (Phase 1)
# =============================================================================


def _snapshot_to_response(s: DataProfileSnapshot) -> ProfileSnapshotResponse:
    """Convert a DataProfileSnapshot to a response model."""
    return ProfileSnapshotResponse(
        id=s.id,
        table_name=s.table_name,
        source=s.source,
        domain=s.domain,
        job_id=s.job_id,
        row_count=s.row_count,
        column_count=s.column_count,
        total_null_count=s.total_null_count,
        overall_completeness_pct=s.overall_completeness_pct,
        schema_snapshot=s.schema_snapshot,
        profiled_at=s.profiled_at.isoformat() if s.profiled_at else None,
        execution_time_ms=s.execution_time_ms,
    )


def _column_to_response(c: DataProfileColumn) -> ProfileColumnResponse:
    """Convert a DataProfileColumn to a response model."""
    return ProfileColumnResponse(
        id=c.id,
        snapshot_id=c.snapshot_id,
        column_name=c.column_name,
        data_type=c.data_type,
        null_count=c.null_count,
        null_pct=c.null_pct,
        distinct_count=c.distinct_count,
        cardinality_ratio=c.cardinality_ratio,
        stats=c.stats,
    )


@router.post("/profile/{table_name}", response_model=ProfileSnapshotResponse)
def profile_table(
    table_name: str,
    source: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
) -> ProfileSnapshotResponse:
    """Profile a specific table on-demand."""
    snapshot = data_profiling_service.profile_table(db, table_name, source=source)
    if not snapshot:
        raise HTTPException(status_code=409, detail="Profiling already in progress or table not found")
    return _snapshot_to_response(snapshot)


@router.post("/profile/all")
def profile_all_tables(db: Session = Depends(get_db)):
    """Profile all registered tables."""
    snapshots = data_profiling_service.profile_all_tables(db)
    return {
        "message": f"Profiled {len(snapshots)} tables",
        "tables_profiled": [s.table_name for s in snapshots],
    }


@router.get("/profiles/{table_name}", response_model=ProfileSnapshotResponse)
def get_latest_profile(
    table_name: str, db: Session = Depends(get_db)
) -> ProfileSnapshotResponse:
    """Get the latest profile snapshot for a table."""
    snapshot = data_profiling_service.get_latest_profile(db, table_name)
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"No profile found for {table_name}")
    return _snapshot_to_response(snapshot)


@router.get("/profiles/{table_name}/history", response_model=List[ProfileSnapshotResponse])
def get_profile_history(
    table_name: str,
    limit: int = Query(default=30, ge=1, le=100),
    db: Session = Depends(get_db),
) -> List[ProfileSnapshotResponse]:
    """Get profile history for a table (last N snapshots)."""
    snapshots = data_profiling_service.get_profile_history(db, table_name, limit=limit)
    return [_snapshot_to_response(s) for s in snapshots]


@router.get("/profiles/{table_name}/columns", response_model=List[ProfileColumnResponse])
def get_profile_columns(
    table_name: str, db: Session = Depends(get_db)
) -> List[ProfileColumnResponse]:
    """Get column-level stats for the latest profile of a table."""
    snapshot = data_profiling_service.get_latest_profile(db, table_name)
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"No profile found for {table_name}")
    columns = data_profiling_service.get_column_stats(db, snapshot.id)
    return [_column_to_response(c) for c in columns]


# =============================================================================
# Anomaly Detection Endpoints (Phase 2)
# =============================================================================


def _anomaly_to_response(a: DQAnomalyAlert) -> AnomalyAlertResponse:
    """Convert a DQAnomalyAlert to a response model."""
    return AnomalyAlertResponse(
        id=a.id,
        table_name=a.table_name,
        source=a.source,
        column_name=a.column_name,
        alert_type=a.alert_type.value if a.alert_type else None,
        status=a.status.value if a.status else None,
        severity=a.severity.value if a.severity else None,
        message=a.message,
        current_value=a.current_value,
        baseline_value=a.baseline_value,
        deviation_sigma=a.deviation_sigma,
        details=a.details,
        snapshot_id=a.snapshot_id,
        job_id=a.job_id,
        resolved_at=a.resolved_at.isoformat() if a.resolved_at else None,
        resolution_notes=a.resolution_notes,
        detected_at=a.detected_at.isoformat() if a.detected_at else None,
    )


@router.get("/anomalies", response_model=List[AnomalyAlertResponse])
def list_anomalies(
    source: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    alert_type: Optional[str] = Query(default=None),
    table_name: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> List[AnomalyAlertResponse]:
    """List anomaly alerts with optional filters."""
    alerts = anomaly_detection_service.get_anomalies(
        db, source=source, status=status, alert_type=alert_type,
        table_name=table_name, limit=limit,
    )
    return [_anomaly_to_response(a) for a in alerts]


@router.get("/anomalies/{alert_id}", response_model=AnomalyAlertResponse)
def get_anomaly(
    alert_id: int, db: Session = Depends(get_db)
) -> AnomalyAlertResponse:
    """Get a single anomaly alert by ID."""
    alert = db.query(DQAnomalyAlert).filter(DQAnomalyAlert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Anomaly alert not found")
    return _anomaly_to_response(alert)


@router.post("/anomalies/{alert_id}/acknowledge", response_model=AnomalyAlertResponse)
def acknowledge_anomaly(
    alert_id: int,
    body: AnomalyResolveRequest = AnomalyResolveRequest(),
    db: Session = Depends(get_db),
) -> AnomalyAlertResponse:
    """Acknowledge an anomaly alert."""
    alert = anomaly_detection_service.acknowledge_anomaly(db, alert_id, notes=body.notes)
    if not alert:
        raise HTTPException(status_code=404, detail="Anomaly alert not found")
    return _anomaly_to_response(alert)


@router.post("/anomalies/{alert_id}/resolve", response_model=AnomalyAlertResponse)
def resolve_anomaly(
    alert_id: int,
    body: AnomalyResolveRequest = AnomalyResolveRequest(),
    db: Session = Depends(get_db),
) -> AnomalyAlertResponse:
    """Resolve an anomaly alert."""
    alert = anomaly_detection_service.resolve_anomaly(db, alert_id, notes=body.notes)
    if not alert:
        raise HTTPException(status_code=404, detail="Anomaly alert not found")
    return _anomaly_to_response(alert)


@router.get("/anomalies/thresholds", response_model=List[AnomalyThresholdResponse])
def list_anomaly_thresholds(db: Session = Depends(get_db)) -> List[AnomalyThresholdResponse]:
    """List all anomaly detection thresholds."""
    thresholds = db.query(DQAnomalyThreshold).all()
    return [
        AnomalyThresholdResponse(
            id=t.id, source=t.source, table_pattern=t.table_pattern,
            row_count_sigma=t.row_count_sigma, null_rate_sigma=t.null_rate_sigma,
            distribution_sigma=t.distribution_sigma,
            schema_drift_enabled=bool(t.schema_drift_enabled),
            is_enabled=bool(t.is_enabled),
        )
        for t in thresholds
    ]


@router.post("/anomalies/thresholds", response_model=AnomalyThresholdResponse, status_code=201)
def create_anomaly_threshold(
    body: AnomalyThresholdCreate, db: Session = Depends(get_db)
) -> AnomalyThresholdResponse:
    """Create or update an anomaly detection threshold."""
    threshold = DQAnomalyThreshold(
        source=body.source,
        table_pattern=body.table_pattern,
        row_count_sigma=body.row_count_sigma,
        null_rate_sigma=body.null_rate_sigma,
        distribution_sigma=body.distribution_sigma,
        schema_drift_enabled=1 if body.schema_drift_enabled else 0,
        is_enabled=1 if body.is_enabled else 0,
    )
    db.add(threshold)
    db.commit()
    db.refresh(threshold)
    return AnomalyThresholdResponse(
        id=threshold.id, source=threshold.source, table_pattern=threshold.table_pattern,
        row_count_sigma=threshold.row_count_sigma, null_rate_sigma=threshold.null_rate_sigma,
        distribution_sigma=threshold.distribution_sigma,
        schema_drift_enabled=bool(threshold.schema_drift_enabled),
        is_enabled=bool(threshold.is_enabled),
    )


@router.post("/anomalies/detect/{table_name}", response_model=List[AnomalyAlertResponse])
def detect_anomalies_for_table(
    table_name: str, db: Session = Depends(get_db)
) -> List[AnomalyAlertResponse]:
    """Run anomaly detection on a specific table (requires existing profile)."""
    snapshot = data_profiling_service.get_latest_profile(db, table_name)
    if not snapshot:
        raise HTTPException(
            status_code=404,
            detail=f"No profile found for {table_name}. Profile it first.",
        )
    alerts = anomaly_detection_service.detect_anomalies(db, snapshot, table_name)
    return [_anomaly_to_response(a) for a in alerts]


# =============================================================================
# Cross-Source Validation Endpoints (Phase 3)
# =============================================================================


def _validation_to_response(v: DQCrossSourceValidation) -> CrossSourceValidationResponse:
    """Convert a DQCrossSourceValidation to a response model."""
    return CrossSourceValidationResponse(
        id=v.id,
        name=v.name,
        description=v.description,
        validation_type=v.validation_type,
        config=v.config,
        severity=v.severity.value if hasattr(v.severity, "value") else str(v.severity),
        is_enabled=bool(v.is_enabled),
        times_evaluated=v.times_evaluated or 0,
        last_evaluated_at=v.last_evaluated_at.isoformat() if v.last_evaluated_at else None,
        last_pass_rate=v.last_pass_rate,
    )


def _xsrc_result_to_response(r: DQCrossSourceResult) -> CrossSourceResultResponse:
    """Convert a DQCrossSourceResult to a response model."""
    return CrossSourceResultResponse(
        id=r.id,
        validation_id=r.validation_id,
        passed=bool(r.passed),
        left_count=r.left_count,
        right_count=r.right_count,
        matched_count=r.matched_count,
        orphan_left=r.orphan_left,
        orphan_right=r.orphan_right,
        match_rate=r.match_rate,
        sample_orphans=r.sample_orphans,
        message=r.message,
        execution_time_ms=r.execution_time_ms,
        evaluated_at=r.evaluated_at.isoformat() if r.evaluated_at else None,
    )


@router.get("/cross-source/validations", response_model=List[CrossSourceValidationResponse])
def list_cross_source_validations(
    db: Session = Depends(get_db),
) -> List[CrossSourceValidationResponse]:
    """List all cross-source validation rules."""
    validations = db.query(DQCrossSourceValidation).all()
    return [_validation_to_response(v) for v in validations]


@router.post("/cross-source/validations", response_model=CrossSourceValidationResponse, status_code=201)
def create_cross_source_validation(
    body: CrossSourceValidationCreate, db: Session = Depends(get_db)
) -> CrossSourceValidationResponse:
    """Create a new cross-source validation rule."""
    validation = DQCrossSourceValidation(
        name=body.name,
        description=body.description,
        validation_type=body.validation_type,
        config=body.config,
        severity=body.severity,
        is_enabled=1 if body.is_enabled else 0,
    )
    db.add(validation)
    db.commit()
    db.refresh(validation)
    return _validation_to_response(validation)


@router.patch("/cross-source/validations/{validation_id}", response_model=CrossSourceValidationResponse)
def update_cross_source_validation(
    validation_id: int,
    body: CrossSourceValidationUpdate,
    db: Session = Depends(get_db),
) -> CrossSourceValidationResponse:
    """Update a cross-source validation rule."""
    validation = db.query(DQCrossSourceValidation).filter(
        DQCrossSourceValidation.id == validation_id
    ).first()
    if not validation:
        raise HTTPException(status_code=404, detail="Validation not found")

    if body.description is not None:
        validation.description = body.description
    if body.config is not None:
        validation.config = body.config
    if body.severity is not None:
        validation.severity = body.severity
    if body.is_enabled is not None:
        validation.is_enabled = 1 if body.is_enabled else 0

    db.commit()
    db.refresh(validation)
    return _validation_to_response(validation)


@router.post("/cross-source/run", response_model=List[CrossSourceResultResponse])
def run_all_cross_source_validations(
    db: Session = Depends(get_db),
) -> List[CrossSourceResultResponse]:
    """Run all enabled cross-source validations."""
    results = cross_source_validation_service.run_all_validations(db)
    return [_xsrc_result_to_response(r) for r in results]


@router.post("/cross-source/run/{validation_id}", response_model=CrossSourceResultResponse)
def run_single_cross_source_validation(
    validation_id: int, db: Session = Depends(get_db)
) -> CrossSourceResultResponse:
    """Run a single cross-source validation."""
    validation = db.query(DQCrossSourceValidation).filter(
        DQCrossSourceValidation.id == validation_id
    ).first()
    if not validation:
        raise HTTPException(status_code=404, detail="Validation not found")
    result = cross_source_validation_service.run_validation(db, validation)
    return _xsrc_result_to_response(result)


@router.get("/cross-source/results", response_model=List[CrossSourceResultResponse])
def get_cross_source_results(
    validation_id: Optional[int] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
) -> List[CrossSourceResultResponse]:
    """Get cross-source validation results."""
    query = db.query(DQCrossSourceResult)
    if validation_id:
        query = query.filter(DQCrossSourceResult.validation_id == validation_id)
    results = query.order_by(DQCrossSourceResult.evaluated_at.desc()).limit(limit).all()
    return [_xsrc_result_to_response(r) for r in results]


@router.post("/cross-source/seed-defaults")
def seed_default_validations(db: Session = Depends(get_db)):
    """Seed built-in cross-source validation rules."""
    created = cross_source_validation_service.create_default_validations(db)
    return {
        "message": f"Created {len(created)} default validations",
        "validations": [v.name for v in created],
    }


# =============================================================================
# Quality Trending Endpoints (Phase 4)
# =============================================================================


def _qsnapshot_to_response(s: DQQualitySnapshot) -> QualitySnapshotResponse:
    """Convert a DQQualitySnapshot to a response model."""
    return QualitySnapshotResponse(
        id=s.id,
        snapshot_date=str(s.snapshot_date) if s.snapshot_date else None,
        source=s.source,
        table_name=s.table_name,
        domain=s.domain,
        quality_score=s.quality_score,
        completeness_score=s.completeness_score,
        freshness_score=s.freshness_score,
        validity_score=s.validity_score,
        consistency_score=s.consistency_score,
        row_count=s.row_count,
        rule_pass_rate=s.rule_pass_rate,
        anomaly_count=s.anomaly_count,
    )


@router.get("/trends", response_model=List[QualitySnapshotResponse])
def get_quality_trends(
    source: Optional[str] = Query(default=None),
    table_name: Optional[str] = Query(default=None),
    window_days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
) -> List[QualitySnapshotResponse]:
    """Get quality trend data for a source/table over a time window."""
    snapshots = quality_trending_service.get_trend(
        db, source=source, table_name=table_name, window_days=window_days
    )
    return [_qsnapshot_to_response(s) for s in snapshots]


@router.get("/trends/weekly")
def get_weekly_trends(
    source: Optional[str] = Query(default=None),
    weeks: int = Query(default=12, ge=1, le=52),
    db: Session = Depends(get_db),
):
    """Get weekly aggregation of quality scores."""
    return quality_trending_service.get_weekly_aggregation(db, source=source, weeks=weeks)


@router.get("/trends/sla")
def get_sla_compliance(db: Session = Depends(get_db)):
    """Get SLA compliance status for all sources/tables."""
    results = quality_trending_service.check_sla_compliance(db)
    return {"compliance_results": results, "total": len(results)}


@router.post("/trends/sla-targets", response_model=SLATargetResponse, status_code=201)
def create_sla_target(
    body: SLATargetCreate, db: Session = Depends(get_db)
) -> SLATargetResponse:
    """Create or update an SLA target."""
    target = DQSLATarget(
        source=body.source,
        table_pattern=body.table_pattern,
        target_quality_score=body.target_quality_score,
        target_completeness=body.target_completeness,
        target_freshness=body.target_freshness,
        target_validity=body.target_validity,
        consecutive_drops_threshold=body.consecutive_drops_threshold,
        is_enabled=1 if body.is_enabled else 0,
    )
    db.add(target)
    db.commit()
    db.refresh(target)
    return SLATargetResponse(
        id=target.id, source=target.source, table_pattern=target.table_pattern,
        target_quality_score=target.target_quality_score,
        target_completeness=target.target_completeness,
        target_freshness=target.target_freshness,
        target_validity=target.target_validity,
        consecutive_drops_threshold=target.consecutive_drops_threshold,
        is_enabled=bool(target.is_enabled),
    )


@router.get("/trends/sla-targets", response_model=List[SLATargetResponse])
def list_sla_targets(db: Session = Depends(get_db)) -> List[SLATargetResponse]:
    """List all SLA targets."""
    targets = db.query(DQSLATarget).all()
    return [
        SLATargetResponse(
            id=t.id, source=t.source, table_pattern=t.table_pattern,
            target_quality_score=t.target_quality_score,
            target_completeness=t.target_completeness,
            target_freshness=t.target_freshness,
            target_validity=t.target_validity,
            consecutive_drops_threshold=t.consecutive_drops_threshold,
            is_enabled=bool(t.is_enabled),
        )
        for t in targets
    ]


@router.post("/trends/compute")
def compute_quality_snapshots(db: Session = Depends(get_db)):
    """Manually trigger daily quality snapshot computation."""
    snapshots = quality_trending_service.compute_daily_snapshots(db)
    return {
        "message": f"Computed {len(snapshots)} quality snapshots",
        "sources": list(set(s.source for s in snapshots if s.source)),
    }


@router.get("/trends/degradation-alerts", response_model=List[AnomalyAlertResponse])
def get_degradation_alerts(db: Session = Depends(get_db)) -> List[AnomalyAlertResponse]:
    """Get sustained quality degradation warnings."""
    alerts = (
        db.query(DQAnomalyAlert)
        .filter(DQAnomalyAlert.alert_type == AnomalyAlertType.QUALITY_DEGRADATION)
        .order_by(DQAnomalyAlert.detected_at.desc())
        .limit(50)
        .all()
    )
    return [_anomaly_to_response(a) for a in alerts]
