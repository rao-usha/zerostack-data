"""
Job chain management endpoints.

Provides API for creating and executing job chains (DAG workflows).
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from datetime import datetime

from app.core.database import get_db
from app.core.models import JobChain, JobChainExecution, JobDependency, IngestionJob, JobStatus, DependencyCondition
from app.core import dependency_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chains", tags=["job-chains"])


# =============================================================================
# Pydantic Schemas
# =============================================================================

class JobDefinition(BaseModel):
    """Definition of a single job in a chain."""
    source: str = Field(..., description="Data source (e.g., 'fred', 'census')")
    config: dict = Field(default_factory=dict, description="Source-specific configuration")
    depends_on: List[int] = Field(default_factory=list, description="Indices of jobs this depends on")
    condition: str = Field(default="on_success", description="When dependency is satisfied: on_success, on_complete, on_failure")


class ChainCreate(BaseModel):
    """Request schema for creating a job chain."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    jobs: List[JobDefinition] = Field(..., min_items=1)


class ChainResponse(BaseModel):
    """Response schema for job chain information."""
    id: int
    name: str
    description: Optional[str]
    job_count: int
    is_active: bool
    times_executed: int
    last_executed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ExecutionResponse(BaseModel):
    """Response schema for chain execution information."""
    id: int
    chain_id: int
    status: str
    total_jobs: int
    completed_jobs: int
    successful_jobs: int
    failed_jobs: int
    started_at: datetime
    completed_at: Optional[datetime]


class DependencyCreate(BaseModel):
    """Request schema for creating a job dependency."""
    job_id: int = Field(..., description="The job that has the dependency")
    depends_on_job_id: int = Field(..., description="The job that must complete first")
    condition: str = Field(default="on_success", description="Condition: on_success, on_complete, on_failure")


# =============================================================================
# Chain Management Endpoints
# =============================================================================

@router.get("", response_model=List[ChainResponse])
def list_chains(
    active_only: bool = False,
    db: Session = Depends(get_db)
) -> List[ChainResponse]:
    """
    List all job chains.
    """
    query = db.query(JobChain)

    if active_only:
        query = query.filter(JobChain.is_active == 1)

    chains = query.order_by(JobChain.name).all()

    return [
        ChainResponse(
            id=c.id,
            name=c.name,
            description=c.description,
            job_count=len(c.chain_definition) if c.chain_definition else 0,
            is_active=bool(c.is_active),
            times_executed=c.times_executed,
            last_executed_at=c.last_executed_at,
            created_at=c.created_at,
            updated_at=c.updated_at
        )
        for c in chains
    ]


@router.post("", response_model=ChainResponse, status_code=201)
def create_chain(
    chain_request: ChainCreate,
    db: Session = Depends(get_db)
) -> ChainResponse:
    """
    Create a new job chain.

    A chain defines a sequence of jobs with dependencies between them.

    Example:
    ```json
    {
        "name": "Economic Indicators Update",
        "description": "Refresh key economic indicators",
        "jobs": [
            {"source": "fred", "config": {"series_id": "GDP"}, "depends_on": []},
            {"source": "fred", "config": {"series_id": "UNRATE"}, "depends_on": []},
            {"source": "fred", "config": {"series_id": "CPIAUCSL"}, "depends_on": [0, 1]}
        ]
    }
    ```
    """
    # Check for duplicate name
    existing = db.query(JobChain).filter(JobChain.name == chain_request.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Chain name already exists")

    # Convert to chain definition format
    chain_definition = [
        {
            "source": job.source,
            "config": job.config,
            "depends_on": job.depends_on,
            "condition": job.condition
        }
        for job in chain_request.jobs
    ]

    try:
        chain = dependency_service.create_chain(
            db=db,
            name=chain_request.name,
            chain_definition=chain_definition,
            description=chain_request.description
        )

        return ChainResponse(
            id=chain.id,
            name=chain.name,
            description=chain.description,
            job_count=len(chain.chain_definition),
            is_active=bool(chain.is_active),
            times_executed=chain.times_executed,
            last_executed_at=chain.last_executed_at,
            created_at=chain.created_at,
            updated_at=chain.updated_at
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{chain_id}", response_model=ChainResponse)
def get_chain(chain_id: int, db: Session = Depends(get_db)) -> ChainResponse:
    """
    Get a specific chain by ID.
    """
    chain = db.query(JobChain).filter(JobChain.id == chain_id).first()

    if not chain:
        raise HTTPException(status_code=404, detail="Chain not found")

    return ChainResponse(
        id=chain.id,
        name=chain.name,
        description=chain.description,
        job_count=len(chain.chain_definition) if chain.chain_definition else 0,
        is_active=bool(chain.is_active),
        times_executed=chain.times_executed,
        last_executed_at=chain.last_executed_at,
        created_at=chain.created_at,
        updated_at=chain.updated_at
    )


@router.get("/{chain_id}/definition")
def get_chain_definition(chain_id: int, db: Session = Depends(get_db)):
    """
    Get the full definition of a chain including all job configurations.
    """
    chain = db.query(JobChain).filter(JobChain.id == chain_id).first()

    if not chain:
        raise HTTPException(status_code=404, detail="Chain not found")

    return {
        "id": chain.id,
        "name": chain.name,
        "description": chain.description,
        "jobs": chain.chain_definition,
        "is_active": bool(chain.is_active)
    }


@router.delete("/{chain_id}")
def delete_chain(chain_id: int, db: Session = Depends(get_db)):
    """
    Delete a chain.
    """
    chain = db.query(JobChain).filter(JobChain.id == chain_id).first()

    if not chain:
        raise HTTPException(status_code=404, detail="Chain not found")

    db.delete(chain)
    db.commit()

    return {"message": f"Chain '{chain.name}' deleted"}


@router.post("/{chain_id}/activate")
def activate_chain(chain_id: int, db: Session = Depends(get_db)):
    """
    Activate a paused chain.
    """
    chain = db.query(JobChain).filter(JobChain.id == chain_id).first()

    if not chain:
        raise HTTPException(status_code=404, detail="Chain not found")

    chain.is_active = 1
    db.commit()

    return {"message": f"Chain '{chain.name}' activated"}


@router.post("/{chain_id}/pause")
def pause_chain(chain_id: int, db: Session = Depends(get_db)):
    """
    Pause a chain (prevent new executions).
    """
    chain = db.query(JobChain).filter(JobChain.id == chain_id).first()

    if not chain:
        raise HTTPException(status_code=404, detail="Chain not found")

    chain.is_active = 0
    db.commit()

    return {"message": f"Chain '{chain.name}' paused"}


# =============================================================================
# Chain Execution Endpoints
# =============================================================================

@router.post("/{chain_id}/execute", response_model=ExecutionResponse)
async def execute_chain(
    chain_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
) -> ExecutionResponse:
    """
    Execute a job chain.

    Creates all jobs defined in the chain with their dependencies.
    Jobs without dependencies start immediately; others wait for their
    dependencies to complete.

    Returns the execution ID for tracking progress.
    """
    try:
        execution = dependency_service.execute_chain(db, chain_id)

        # Start jobs that have no dependencies (they're in PENDING state)
        from app.api.v1.jobs import run_ingestion_job

        for job_id in execution.job_ids:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if job and job.status == JobStatus.PENDING:
                background_tasks.add_task(
                    run_ingestion_job,
                    job.id,
                    job.source,
                    job.config
                )

        return ExecutionResponse(
            id=execution.id,
            chain_id=execution.chain_id,
            status=execution.status,
            total_jobs=execution.total_jobs,
            completed_jobs=execution.completed_jobs,
            successful_jobs=execution.successful_jobs,
            failed_jobs=execution.failed_jobs,
            started_at=execution.started_at,
            completed_at=execution.completed_at
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{chain_id}/executions", response_model=List[ExecutionResponse])
def list_chain_executions(
    chain_id: int,
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db)
) -> List[ExecutionResponse]:
    """
    List recent executions of a chain.
    """
    chain = db.query(JobChain).filter(JobChain.id == chain_id).first()

    if not chain:
        raise HTTPException(status_code=404, detail="Chain not found")

    executions = db.query(JobChainExecution).filter(
        JobChainExecution.chain_id == chain_id
    ).order_by(
        JobChainExecution.started_at.desc()
    ).limit(limit).all()

    return [
        ExecutionResponse(
            id=e.id,
            chain_id=e.chain_id,
            status=e.status,
            total_jobs=e.total_jobs,
            completed_jobs=e.completed_jobs,
            successful_jobs=e.successful_jobs,
            failed_jobs=e.failed_jobs,
            started_at=e.started_at,
            completed_at=e.completed_at
        )
        for e in executions
    ]


@router.get("/executions/{execution_id}")
def get_execution_status(execution_id: int, db: Session = Depends(get_db)):
    """
    Get detailed status of a chain execution including all job statuses.
    """
    status = dependency_service.get_chain_execution_status(db, execution_id)

    if not status:
        raise HTTPException(status_code=404, detail="Execution not found")

    return status


# =============================================================================
# Direct Dependency Management Endpoints
# =============================================================================

@router.post("/dependencies", status_code=201)
def add_job_dependency(
    dependency: DependencyCreate,
    db: Session = Depends(get_db)
):
    """
    Add a dependency between two existing jobs.

    This allows creating ad-hoc dependencies without using a chain.

    Args:
        job_id: The job that should wait
        depends_on_job_id: The job that must complete first
        condition: When the dependency is satisfied
            - on_success: Parent must succeed (default)
            - on_complete: Parent must complete (success or failure)
            - on_failure: Parent must fail
    """
    # Validate jobs exist
    job = db.query(IngestionJob).filter(IngestionJob.id == dependency.job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {dependency.job_id} not found")

    parent = db.query(IngestionJob).filter(IngestionJob.id == dependency.depends_on_job_id).first()
    if not parent:
        raise HTTPException(status_code=404, detail=f"Parent job {dependency.depends_on_job_id} not found")

    try:
        condition = DependencyCondition(dependency.condition)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid condition. Must be: on_success, on_complete, on_failure"
        )

    try:
        dep = dependency_service.add_dependency(
            db=db,
            job_id=dependency.job_id,
            depends_on_job_id=dependency.depends_on_job_id,
            condition=condition
        )

        return {
            "message": f"Dependency added: job {dependency.job_id} depends on job {dependency.depends_on_job_id}",
            "dependency_id": dep.id,
            "job_status": job.status.value
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/dependencies/job/{job_id}")
def get_job_dependencies(job_id: int, db: Session = Depends(get_db)):
    """
    Get all dependencies for a job.
    """
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    deps = dependency_service.get_job_dependencies(db, job_id)

    dependencies = []
    for dep in deps:
        parent = db.query(IngestionJob).filter(IngestionJob.id == dep.depends_on_job_id).first()
        dependencies.append({
            "dependency_id": dep.id,
            "depends_on_job_id": dep.depends_on_job_id,
            "parent_source": parent.source if parent else None,
            "parent_status": parent.status.value if parent else None,
            "condition": dep.condition.value,
            "is_satisfied": dependency_service.is_dependency_satisfied(db, dep)
        })

    unsatisfied = dependency_service.get_unsatisfied_dependencies(db, job_id)

    return {
        "job_id": job_id,
        "job_status": job.status.value,
        "total_dependencies": len(deps),
        "satisfied": len(deps) - len(unsatisfied),
        "unsatisfied": len(unsatisfied),
        "can_run": dependency_service.are_all_dependencies_satisfied(db, job_id),
        "dependencies": dependencies
    }


@router.delete("/dependencies")
def remove_job_dependency(
    job_id: int,
    depends_on_job_id: int,
    db: Session = Depends(get_db)
):
    """
    Remove a dependency between two jobs.
    """
    success = dependency_service.remove_dependency(db, job_id, depends_on_job_id)

    if not success:
        raise HTTPException(status_code=404, detail="Dependency not found")

    return {"message": f"Dependency removed: job {job_id} -> {depends_on_job_id}"}
