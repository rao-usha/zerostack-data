"""
Bulk Ingestion Template API endpoints.

Provides management and execution of reusable ingestion templates.
"""
import logging
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionTemplate, TemplateExecution, TemplateCategory
from app.core.template_service import (
    TemplateService,
    init_builtin_templates,
    BUILTIN_TEMPLATES
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/templates", tags=["templates"])


# =============================================================================
# Pydantic Schemas
# =============================================================================

class VariableDefinition(BaseModel):
    """Definition of a template variable."""
    type: str = Field(default="string", description="Variable type: string, integer, float, boolean")
    default: Optional[Any] = None
    required: bool = False
    description: Optional[str] = None


class JobDefinition(BaseModel):
    """Definition of a job within a template."""
    source: str = Field(..., min_length=1, max_length=50)
    config: Dict[str, Any] = Field(default_factory=dict)


class TemplateCreate(BaseModel):
    """Schema for creating a template."""
    name: str = Field(..., min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_]*$")
    display_name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None
    category: TemplateCategory = TemplateCategory.CUSTOM
    tags: Optional[List[str]] = None
    jobs_definition: List[JobDefinition]
    variables: Optional[Dict[str, VariableDefinition]] = None
    use_chain: bool = False
    parallel_execution: bool = True


class TemplateUpdate(BaseModel):
    """Schema for updating a template."""
    display_name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[TemplateCategory] = None
    tags: Optional[List[str]] = None
    jobs_definition: Optional[List[JobDefinition]] = None
    variables: Optional[Dict[str, VariableDefinition]] = None
    use_chain: Optional[bool] = None
    parallel_execution: Optional[bool] = None
    is_enabled: Optional[bool] = None


class TemplateResponse(BaseModel):
    """Response schema for a template."""
    id: int
    name: str
    display_name: Optional[str]
    description: Optional[str]
    category: str
    tags: Optional[List[str]]
    jobs_definition: List[Dict[str, Any]]
    variables: Optional[Dict[str, Any]]
    use_chain: bool
    parallel_execution: bool
    is_builtin: bool
    is_enabled: bool
    times_executed: int
    last_executed_at: Optional[str]
    created_at: str
    updated_at: str


class TemplateExecuteRequest(BaseModel):
    """Request schema for executing a template."""
    variables: Optional[Dict[str, Any]] = None


class ExecutionResponse(BaseModel):
    """Response schema for a template execution."""
    id: int
    template_id: int
    template_name: str
    parameters: Optional[Dict[str, Any]]
    status: str
    job_ids: List[int]
    chain_id: Optional[int]
    total_jobs: int
    completed_jobs: int
    successful_jobs: int
    failed_jobs: int
    started_at: str
    completed_at: Optional[str]
    errors: Optional[List[str]]


class BuiltinTemplateInfo(BaseModel):
    """Information about a built-in template."""
    name: str
    display_name: str
    description: str
    category: str
    tags: List[str]
    job_count: int
    variables: Dict[str, Any]
    use_chain: bool


# =============================================================================
# Helper Functions
# =============================================================================

def template_to_response(template: IngestionTemplate) -> TemplateResponse:
    """Convert a template model to response schema."""
    return TemplateResponse(
        id=template.id,
        name=template.name,
        display_name=template.display_name,
        description=template.description,
        category=template.category.value if template.category else "custom",
        tags=template.tags,
        jobs_definition=template.jobs_definition,
        variables=template.variables,
        use_chain=bool(template.use_chain),
        parallel_execution=bool(template.parallel_execution),
        is_builtin=bool(template.is_builtin),
        is_enabled=bool(template.is_enabled),
        times_executed=template.times_executed,
        last_executed_at=template.last_executed_at.isoformat() if template.last_executed_at else None,
        created_at=template.created_at.isoformat(),
        updated_at=template.updated_at.isoformat()
    )


def execution_to_response(execution: TemplateExecution) -> ExecutionResponse:
    """Convert an execution model to response schema."""
    return ExecutionResponse(
        id=execution.id,
        template_id=execution.template_id,
        template_name=execution.template_name,
        parameters=execution.parameters,
        status=execution.status,
        job_ids=execution.job_ids or [],
        chain_id=execution.chain_id,
        total_jobs=execution.total_jobs,
        completed_jobs=execution.completed_jobs,
        successful_jobs=execution.successful_jobs,
        failed_jobs=execution.failed_jobs,
        started_at=execution.started_at.isoformat(),
        completed_at=execution.completed_at.isoformat() if execution.completed_at else None,
        errors=execution.errors
    )


# =============================================================================
# Template Management Endpoints
# =============================================================================

@router.get("", response_model=List[TemplateResponse])
def list_templates(
    category: Optional[TemplateCategory] = Query(default=None, description="Filter by category"),
    tag: Optional[str] = Query(default=None, description="Filter by tag"),
    enabled_only: bool = Query(default=True, description="Only show enabled templates"),
    db: Session = Depends(get_db)
) -> List[TemplateResponse]:
    """
    List all ingestion templates.

    Returns templates with optional filtering by category or tags.
    """
    service = TemplateService(db)
    tags = [tag] if tag else None
    templates = service.list_templates(
        category=category,
        tags=tags,
        enabled_only=enabled_only
    )
    return [template_to_response(t) for t in templates]


@router.get("/builtins", response_model=List[BuiltinTemplateInfo])
def list_builtin_templates():
    """
    List available built-in template definitions.

    These are the pre-configured templates that can be initialized.
    """
    return [
        BuiltinTemplateInfo(
            name=name,
            display_name=config["display_name"],
            description=config["description"],
            category=config["category"].value,
            tags=config["tags"],
            job_count=len(config["jobs_definition"]),
            variables=config["variables"],
            use_chain=config["use_chain"]
        )
        for name, config in sorted(BUILTIN_TEMPLATES.items())
    ]


@router.get("/categories")
def list_categories():
    """List all available template categories."""
    return {
        "categories": [
            {"value": c.value, "name": c.name}
            for c in TemplateCategory
        ]
    }


@router.get("/{name}", response_model=TemplateResponse)
def get_template(name: str, db: Session = Depends(get_db)) -> TemplateResponse:
    """Get a template by name."""
    service = TemplateService(db)
    template = service.get_template(name)

    if not template:
        raise HTTPException(status_code=404, detail=f"Template not found: {name}")

    return template_to_response(template)


@router.post("", response_model=TemplateResponse, status_code=201)
def create_template(
    request: TemplateCreate,
    db: Session = Depends(get_db)
) -> TemplateResponse:
    """
    Create a new ingestion template.

    Template names must be lowercase with underscores (e.g., my_template).
    """
    service = TemplateService(db)

    # Check if template already exists
    if service.get_template(request.name):
        raise HTTPException(status_code=409, detail=f"Template already exists: {request.name}")

    # Convert Pydantic models to dicts
    jobs_def = [j.model_dump() for j in request.jobs_definition]
    variables = {k: v.model_dump() for k, v in request.variables.items()} if request.variables else None

    template = service.create_template(
        name=request.name,
        display_name=request.display_name,
        description=request.description,
        category=request.category,
        tags=request.tags,
        jobs_definition=jobs_def,
        variables=variables,
        use_chain=request.use_chain,
        parallel_execution=request.parallel_execution
    )

    return template_to_response(template)


@router.put("/{name}", response_model=TemplateResponse)
def update_template(
    name: str,
    request: TemplateUpdate,
    db: Session = Depends(get_db)
) -> TemplateResponse:
    """
    Update an existing template.

    Built-in templates cannot be modified.
    """
    service = TemplateService(db)

    # Build updates dict
    updates = {}
    if request.display_name is not None:
        updates["display_name"] = request.display_name
    if request.description is not None:
        updates["description"] = request.description
    if request.category is not None:
        updates["category"] = request.category
    if request.tags is not None:
        updates["tags"] = request.tags
    if request.jobs_definition is not None:
        updates["jobs_definition"] = [j.model_dump() for j in request.jobs_definition]
    if request.variables is not None:
        updates["variables"] = {k: v.model_dump() for k, v in request.variables.items()}
    if request.use_chain is not None:
        updates["use_chain"] = request.use_chain
    if request.parallel_execution is not None:
        updates["parallel_execution"] = request.parallel_execution
    if request.is_enabled is not None:
        updates["is_enabled"] = request.is_enabled

    try:
        template = service.update_template(name, **updates)
        if not template:
            raise HTTPException(status_code=404, detail=f"Template not found: {name}")
        return template_to_response(template)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{name}")
def delete_template(name: str, db: Session = Depends(get_db)):
    """
    Delete a template.

    Built-in templates cannot be deleted.
    """
    service = TemplateService(db)

    try:
        if not service.delete_template(name):
            raise HTTPException(status_code=404, detail=f"Template not found: {name}")
        return {"message": f"Template '{name}' deleted successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# =============================================================================
# Template Execution Endpoints
# =============================================================================

@router.post("/{name}/execute", response_model=ExecutionResponse, status_code=201)
def execute_template(
    name: str,
    request: Optional[TemplateExecuteRequest] = None,
    db: Session = Depends(get_db)
) -> ExecutionResponse:
    """
    Execute a template to create ingestion jobs.

    Provide variable values to customize the template execution.
    Jobs are created immediately and queued for processing.
    """
    service = TemplateService(db)

    variables = request.variables if request else None

    try:
        execution = service.execute_template(
            name=name,
            variables=variables
        )
        return execution_to_response(execution)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{name}/executions", response_model=List[ExecutionResponse])
def list_template_executions(
    name: str,
    status: Optional[str] = Query(default=None, description="Filter by status"),
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db)
) -> List[ExecutionResponse]:
    """List executions for a specific template."""
    service = TemplateService(db)

    # Verify template exists
    if not service.get_template(name):
        raise HTTPException(status_code=404, detail=f"Template not found: {name}")

    executions = service.list_executions(
        template_name=name,
        status=status,
        limit=limit
    )
    return [execution_to_response(e) for e in executions]


# =============================================================================
# Execution Management Endpoints
# =============================================================================

@router.get("/executions/all", response_model=List[ExecutionResponse])
def list_all_executions(
    status: Optional[str] = Query(default=None, description="Filter by status"),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db)
) -> List[ExecutionResponse]:
    """List all template executions across all templates."""
    service = TemplateService(db)
    executions = service.list_executions(status=status, limit=limit)
    return [execution_to_response(e) for e in executions]


@router.get("/executions/{execution_id}", response_model=ExecutionResponse)
def get_execution(
    execution_id: int,
    db: Session = Depends(get_db)
) -> ExecutionResponse:
    """Get a specific template execution."""
    service = TemplateService(db)
    execution = service.get_execution(execution_id)

    if not execution:
        raise HTTPException(status_code=404, detail=f"Execution not found: {execution_id}")

    return execution_to_response(execution)


@router.post("/executions/{execution_id}/refresh", response_model=ExecutionResponse)
def refresh_execution_status(
    execution_id: int,
    db: Session = Depends(get_db)
) -> ExecutionResponse:
    """
    Refresh execution status from job states.

    Updates the execution's completion counts based on current job statuses.
    """
    service = TemplateService(db)
    execution = service.update_execution_status(execution_id)

    if not execution:
        raise HTTPException(status_code=404, detail=f"Execution not found: {execution_id}")

    return execution_to_response(execution)


# =============================================================================
# Initialization Endpoints
# =============================================================================

@router.post("/init-builtins")
def initialize_builtin_templates(db: Session = Depends(get_db)):
    """
    Initialize database with built-in templates.

    Creates all pre-configured templates that don't already exist.
    """
    count = init_builtin_templates(db)

    return {
        "message": f"Initialized {count} built-in templates",
        "created": count,
        "available_builtins": len(BUILTIN_TEMPLATES)
    }
