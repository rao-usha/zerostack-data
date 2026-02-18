"""
Workflow Orchestration API (T50).

Endpoints for managing multi-agent workflow executions.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.agents.orchestrator import MultiAgentOrchestrator

router = APIRouter(prefix="/workflows", tags=["workflows"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================


class WorkflowStep(BaseModel):
    """Step definition for custom workflows."""

    id: str = Field(..., description="Unique step identifier")
    agent: str = Field(..., description="Agent type to execute")
    parallel_group: Optional[int] = Field(
        None, description="Group for parallel execution"
    )
    depends_on: List[str] = Field(
        default_factory=list, description="Step IDs this depends on"
    )


class StartWorkflowRequest(BaseModel):
    """Request to start a workflow."""

    workflow_type: str = Field(
        ..., description="Workflow template ID", examples=["full_due_diligence"]
    )
    entity_name: str = Field(..., description="Target entity name", examples=["Stripe"])
    entity_type: str = Field("company", description="Entity type", examples=["company"])
    domain: Optional[str] = Field(None, description="Company domain for enrichment")
    params: Optional[dict] = Field(None, description="Additional parameters")


class CreateCustomWorkflowRequest(BaseModel):
    """Request to create a custom workflow template."""

    name: str = Field(..., description="Workflow name", min_length=1, max_length=100)
    description: str = Field(..., description="Workflow description")
    steps: List[WorkflowStep] = Field(..., description="Workflow steps", min_length=1)


class WorkflowStepResponse(BaseModel):
    """Response for a workflow step."""

    step_id: str
    agent_type: str
    status: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    error: Optional[str] = None


class WorkflowResponse(BaseModel):
    """Response for workflow status."""

    workflow_id: str
    workflow_type: str
    workflow_name: Optional[str] = None
    entity_type: str
    entity_name: str
    status: str
    progress: float
    total_steps: int
    completed_steps: int
    failed_steps: int
    steps: List[WorkflowStepResponse]
    step_results: Optional[dict] = None
    aggregated_results: Optional[dict] = None
    error: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class WorkflowListItem(BaseModel):
    """Summary item for workflow list."""

    workflow_id: str
    workflow_type: str
    workflow_name: Optional[str] = None
    entity_name: str
    status: str
    progress: float
    total_steps: int
    completed_steps: int
    failed_steps: int
    created_at: Optional[str] = None
    completed_at: Optional[str] = None


class TemplateStepResponse(BaseModel):
    """Step in a workflow template."""

    id: str
    agent: str
    parallel_group: Optional[int] = None
    depends_on: List[str] = []


class TemplateResponse(BaseModel):
    """Response for workflow template."""

    id: str
    name: str
    description: Optional[str] = None
    steps: List[TemplateStepResponse]
    estimated_duration_minutes: Optional[int] = None
    is_custom: bool = False
    created_at: Optional[str] = None


class AgentResponse(BaseModel):
    """Response for available agent."""

    id: str
    name: str
    description: str


class StatsResponse(BaseModel):
    """Response for workflow statistics."""

    workflows: dict
    by_type: dict
    available_agents: List[str]
    template_count: int


# =============================================================================
# API ENDPOINTS
# =============================================================================


@router.post("/start", response_model=dict, summary="Start a workflow")
def start_workflow(
    request: StartWorkflowRequest,
    db: Session = Depends(get_db),
):
    """
    Start a multi-agent workflow execution.

    Workflows coordinate multiple agents to complete complex research tasks.
    Use predefined templates or create custom workflows.

    **Available workflow types:**
    - `full_due_diligence`: Company research + competitive + news + DD + report
    - `quick_company_scan`: Fast company research
    - `competitive_landscape`: Deep competitive analysis
    - `market_intelligence`: Market scanning with trends
    - `data_enrichment`: Find missing data and detect anomalies
    - `investor_brief`: Research + news + report

    Returns a workflow_id to track progress.
    """
    orchestrator = MultiAgentOrchestrator(db)

    # Prepare entity params
    entity_params = request.params or {}
    if request.domain:
        entity_params["domain"] = request.domain

    try:
        workflow_id = orchestrator.start_workflow(
            workflow_type=request.workflow_type,
            entity_name=request.entity_name,
            entity_type=request.entity_type,
            entity_params=entity_params,
        )

        return {
            "workflow_id": workflow_id,
            "message": f"Workflow '{request.workflow_type}' started for {request.entity_name}",
            "status": "pending",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/{workflow_id}", response_model=WorkflowResponse, summary="Get workflow status"
)
def get_workflow_status(
    workflow_id: str,
    db: Session = Depends(get_db),
):
    """
    Get the status and results of a workflow execution.

    Returns progress, step details, and aggregated results when complete.
    """
    orchestrator = MultiAgentOrchestrator(db)
    result = orchestrator.get_workflow_status(workflow_id)

    if not result:
        raise HTTPException(
            status_code=404, detail=f"Workflow not found: {workflow_id}"
        )

    return result


@router.get("", response_model=List[WorkflowListItem], summary="List workflows")
def list_workflows(
    status: Optional[str] = Query(None, description="Filter by status"),
    entity_name: Optional[str] = Query(None, description="Filter by entity name"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    db: Session = Depends(get_db),
):
    """
    List workflow executions with optional filters.

    **Status values:** pending, running, completed, partial, failed, cancelled
    """
    orchestrator = MultiAgentOrchestrator(db)
    return orchestrator.list_workflows(
        status=status, entity_name=entity_name, limit=limit
    )


@router.delete("/{workflow_id}", summary="Cancel workflow")
def cancel_workflow(
    workflow_id: str,
    db: Session = Depends(get_db),
):
    """
    Cancel a pending or running workflow.

    Only workflows in 'pending' or 'running' status can be cancelled.
    """
    orchestrator = MultiAgentOrchestrator(db)
    success = orchestrator.cancel_workflow(workflow_id)

    if not success:
        raise HTTPException(
            status_code=400,
            detail="Cannot cancel workflow - either not found or already completed",
        )

    return {"message": f"Workflow {workflow_id} cancelled", "status": "cancelled"}


@router.get(
    "/templates/list",
    response_model=List[TemplateResponse],
    summary="List workflow templates",
)
def get_templates(
    db: Session = Depends(get_db),
):
    """
    Get all available workflow templates.

    Includes both built-in and custom templates.
    """
    orchestrator = MultiAgentOrchestrator(db)
    templates = orchestrator.get_templates()

    # Transform steps for response
    result = []
    for t in templates:
        steps = []
        for step in t.get("steps", []):
            steps.append(
                TemplateStepResponse(
                    id=step.get("id"),
                    agent=step.get("agent"),
                    parallel_group=step.get("parallel_group"),
                    depends_on=step.get("depends_on", []),
                )
            )
        result.append(
            TemplateResponse(
                id=t["id"],
                name=t["name"],
                description=t.get("description"),
                steps=steps,
                estimated_duration_minutes=t.get("estimated_duration_minutes"),
                is_custom=t.get("is_custom", False),
                created_at=t.get("created_at"),
            )
        )

    return result


@router.post("/custom", summary="Create custom workflow")
def create_custom_workflow(
    request: CreateCustomWorkflowRequest,
    db: Session = Depends(get_db),
):
    """
    Create a custom workflow template.

    Define your own sequence of agents with dependencies and parallel groups.

    **Available agents:**
    - company_researcher
    - due_diligence
    - news_monitor
    - competitive_intel
    - data_hunter
    - anomaly_detector
    - report_writer
    - market_scanner

    **Example step:**
    ```json
    {
      "id": "research",
      "agent": "company_researcher",
      "parallel_group": 1
    }
    ```

    Steps with the same `parallel_group` run in parallel.
    Use `depends_on` to specify step dependencies.
    """
    orchestrator = MultiAgentOrchestrator(db)

    # Convert to dict format
    steps = [
        {
            "id": s.id,
            "agent": s.agent,
            "parallel_group": s.parallel_group,
            "depends_on": s.depends_on,
        }
        for s in request.steps
    ]

    try:
        template_id = orchestrator.create_custom_workflow(
            name=request.name,
            description=request.description,
            steps=steps,
        )

        return {
            "template_id": template_id,
            "name": request.name,
            "message": "Custom workflow template created",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/agents/available",
    response_model=List[AgentResponse],
    summary="List available agents",
)
def get_available_agents(
    db: Session = Depends(get_db),
):
    """
    Get list of available agents for workflow composition.

    Each agent has specific capabilities and can be combined in workflows.
    """
    orchestrator = MultiAgentOrchestrator(db)
    return orchestrator.get_available_agents()


@router.get(
    "/stats/summary", response_model=StatsResponse, summary="Get workflow statistics"
)
def get_stats(
    db: Session = Depends(get_db),
):
    """
    Get workflow execution statistics.

    Returns counts by status, popular workflow types, and average duration.
    """
    orchestrator = MultiAgentOrchestrator(db)
    return orchestrator.get_stats()
