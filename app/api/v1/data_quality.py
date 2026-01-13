"""
Data quality rules management endpoints.

Provides API for creating, managing, and evaluating data quality rules.
"""
import logging
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from datetime import datetime

from app.core.database import get_db
from app.core.models import (
    DataQualityRule, DataQualityResult, DataQualityReport,
    RuleType, RuleSeverity, IngestionJob
)
from app.core import data_quality_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data-quality", tags=["data-quality"])


# =============================================================================
# Pydantic Schemas
# =============================================================================

class RuleCreate(BaseModel):
    """Request schema for creating a data quality rule."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    source: Optional[str] = Field(default=None, description="Data source (null = all sources)")
    dataset_pattern: Optional[str] = Field(default=None, description="Regex pattern for dataset names")
    column_name: Optional[str] = Field(default=None, description="Column to check (null = table-level)")
    rule_type: str = Field(..., description="Rule type: range, not_null, unique, regex, freshness, row_count, enum")
    severity: str = Field(default="error", description="Severity: error, warning, info")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Rule-specific parameters")
    priority: int = Field(default=5, ge=1, le=10, description="Priority (1=highest, 10=lowest)")


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
    db: Session = Depends(get_db)
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
            last_evaluated_at=r.last_evaluated_at.isoformat() if r.last_evaluated_at else None,
            created_at=r.created_at.isoformat(),
            updated_at=r.updated_at.isoformat()
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
                "example": {"min": 0, "max": 100}
            },
            {
                "type": "not_null",
                "description": "Value must not be null",
                "parameters": {},
                "requires_column": True,
                "example": {}
            },
            {
                "type": "unique",
                "description": "Values must be unique (no duplicates)",
                "parameters": {},
                "requires_column": True,
                "example": {}
            },
            {
                "type": "regex",
                "description": "Value must match regex pattern",
                "parameters": {"pattern": "regex string"},
                "requires_column": True,
                "example": {"pattern": "^[A-Z]{2}$"}
            },
            {
                "type": "freshness",
                "description": "Data must be recent (date/timestamp column)",
                "parameters": {"max_age_hours": "number", "max_age_days": "number (alternative)"},
                "requires_column": True,
                "example": {"max_age_hours": 24}
            },
            {
                "type": "row_count",
                "description": "Table must have min/max row count",
                "parameters": {"min": "number (optional)", "max": "number (optional)"},
                "requires_column": False,
                "example": {"min": 1, "max": 1000000}
            },
            {
                "type": "enum",
                "description": "Value must be in allowed list",
                "parameters": {"allowed": "array of values"},
                "requires_column": True,
                "example": {"allowed": ["A", "B", "C"]}
            }
        ],
        "severities": ["error", "warning", "info"]
    }


@router.post("/rules", response_model=RuleResponse, status_code=201)
def create_rule(
    rule_request: RuleCreate,
    db: Session = Depends(get_db)
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
            status_code=400,
            detail=f"Invalid rule_type. Must be one of: {valid_types}"
        )

    # Validate severity
    try:
        severity = RuleSeverity(rule_request.severity)
    except ValueError:
        valid_severities = [s.value for s in RuleSeverity]
        raise HTTPException(
            status_code=400,
            detail=f"Invalid severity. Must be one of: {valid_severities}"
        )

    # Check for duplicate name
    existing = db.query(DataQualityRule).filter(
        DataQualityRule.name == rule_request.name
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Rule with this name already exists")

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
        priority=rule_request.priority
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
        last_evaluated_at=rule.last_evaluated_at.isoformat() if rule.last_evaluated_at else None,
        created_at=rule.created_at.isoformat(),
        updated_at=rule.updated_at.isoformat()
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
        last_evaluated_at=rule.last_evaluated_at.isoformat() if rule.last_evaluated_at else None,
        created_at=rule.created_at.isoformat(),
        updated_at=rule.updated_at.isoformat()
    )


@router.patch("/rules/{rule_id}", response_model=RuleResponse)
def update_rule(
    rule_id: int,
    rule_update: RuleUpdate,
    db: Session = Depends(get_db)
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
        last_evaluated_at=rule.last_evaluated_at.isoformat() if rule.last_evaluated_at else None,
        created_at=rule.created_at.isoformat(),
        updated_at=rule.updated_at.isoformat()
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
    request: EvaluateRequest,
    db: Session = Depends(get_db)
) -> ReportResponse:
    """
    Manually evaluate data quality rules against a table.

    This runs all applicable rules for the given source and table.
    """
    # Create a temporary job record for tracking
    job = IngestionJob(
        source=request.source,
        status="success",  # Pretend it's a completed job
        config={"table_name": request.table_name, "manual_evaluation": True}
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        report = data_quality_service.evaluate_rules_for_job(
            db=db,
            job=job,
            table_name=request.table_name
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
            completed_at=report.completed_at.isoformat() if report.completed_at else None
        )

    except Exception as e:
        logger.error(f"Error evaluating rules: {e}")
        raise HTTPException(status_code=500, detail=f"Error evaluating rules: {str(e)}")


@router.post("/rules/{rule_id}/evaluate")
def evaluate_single_rule(
    rule_id: int,
    request: EvaluateRequest,
    db: Session = Depends(get_db)
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
            "execution_time_ms": result.execution_time_ms
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
    db: Session = Depends(get_db)
) -> List[ResultResponse]:
    """
    List rule evaluation results.
    """
    results = data_quality_service.get_rule_results(
        db=db,
        rule_id=rule_id,
        job_id=job_id,
        passed_only=passed,
        limit=limit
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
            evaluated_at=r.evaluated_at.isoformat()
        )
        for r in results
    ]


@router.get("/reports", response_model=List[ReportResponse])
def list_reports(
    job_id: Optional[int] = Query(default=None, description="Filter by job ID"),
    source: Optional[str] = Query(default=None, description="Filter by source"),
    status: Optional[str] = Query(default=None, description="Filter by status"),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db)
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
            completed_at=r.completed_at.isoformat() if r.completed_at else None
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
        completed_at=report.completed_at.isoformat() if report.completed_at else None
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
        completed_at=report.completed_at.isoformat() if report.completed_at else None
    )


@router.get("/reports/{report_id}/results", response_model=List[ResultResponse])
def get_report_results(
    report_id: int,
    db: Session = Depends(get_db)
) -> List[ResultResponse]:
    """Get all rule results for a specific report."""
    report = data_quality_service.get_report(db, report_id)

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    results = db.query(DataQualityResult).filter(
        DataQualityResult.job_id == report.job_id
    ).all()

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
            evaluated_at=r.evaluated_at.isoformat()
        )
        for r in results
    ]
