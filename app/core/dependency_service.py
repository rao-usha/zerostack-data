"""
Job dependency service.

Provides functionality for managing job dependencies and executing job chains.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.core.models import (
    IngestionJob,
    JobStatus,
    JobDependency,
    DependencyCondition,
    JobChain,
    JobChainExecution,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Dependency Checking Functions
# =============================================================================


def get_job_dependencies(db: Session, job_id: int) -> List[JobDependency]:
    """
    Get all dependencies for a job.

    Args:
        db: Database session
        job_id: The job ID

    Returns:
        List of JobDependency objects
    """
    return db.query(JobDependency).filter(JobDependency.job_id == job_id).all()


def get_dependent_jobs(db: Session, job_id: int) -> List[JobDependency]:
    """
    Get all jobs that depend on a given job.

    Args:
        db: Database session
        job_id: The parent job ID

    Returns:
        List of JobDependency objects where depends_on_job_id == job_id
    """
    return (
        db.query(JobDependency).filter(JobDependency.depends_on_job_id == job_id).all()
    )


def is_dependency_satisfied(db: Session, dependency: JobDependency) -> bool:
    """
    Check if a single dependency is satisfied.

    Args:
        db: Database session
        dependency: The dependency to check

    Returns:
        True if the dependency is satisfied
    """
    parent_job = (
        db.query(IngestionJob)
        .filter(IngestionJob.id == dependency.depends_on_job_id)
        .first()
    )

    if not parent_job:
        logger.warning(
            f"Parent job {dependency.depends_on_job_id} not found for dependency"
        )
        return False

    if dependency.condition == DependencyCondition.ON_SUCCESS:
        return parent_job.status == JobStatus.SUCCESS

    elif dependency.condition == DependencyCondition.ON_COMPLETE:
        return parent_job.status in [JobStatus.SUCCESS, JobStatus.FAILED]

    elif dependency.condition == DependencyCondition.ON_FAILURE:
        return parent_job.status == JobStatus.FAILED

    return False


def are_all_dependencies_satisfied(db: Session, job_id: int) -> bool:
    """
    Check if all dependencies for a job are satisfied.

    Args:
        db: Database session
        job_id: The job ID

    Returns:
        True if all dependencies are satisfied (or no dependencies exist)
    """
    dependencies = get_job_dependencies(db, job_id)

    if not dependencies:
        return True

    for dep in dependencies:
        if not is_dependency_satisfied(db, dep):
            return False

    return True


def get_unsatisfied_dependencies(db: Session, job_id: int) -> List[Dict[str, Any]]:
    """
    Get list of unsatisfied dependencies for a job.

    Args:
        db: Database session
        job_id: The job ID

    Returns:
        List of unsatisfied dependency details
    """
    dependencies = get_job_dependencies(db, job_id)
    unsatisfied = []

    for dep in dependencies:
        if not is_dependency_satisfied(db, dep):
            parent_job = (
                db.query(IngestionJob)
                .filter(IngestionJob.id == dep.depends_on_job_id)
                .first()
            )

            unsatisfied.append(
                {
                    "dependency_id": dep.id,
                    "parent_job_id": dep.depends_on_job_id,
                    "parent_status": parent_job.status.value
                    if parent_job
                    else "not_found",
                    "condition": dep.condition.value,
                    "source": parent_job.source if parent_job else None,
                }
            )

    return unsatisfied


# =============================================================================
# Job Dependency Management
# =============================================================================


def add_dependency(
    db: Session,
    job_id: int,
    depends_on_job_id: int,
    condition: DependencyCondition = DependencyCondition.ON_SUCCESS,
) -> JobDependency:
    """
    Add a dependency between two jobs.

    Args:
        db: Database session
        job_id: The job that has the dependency
        depends_on_job_id: The job that must complete first
        condition: When the dependency is satisfied

    Returns:
        The created JobDependency
    """
    # Check if dependency already exists
    existing = (
        db.query(JobDependency)
        .filter(
            and_(
                JobDependency.job_id == job_id,
                JobDependency.depends_on_job_id == depends_on_job_id,
            )
        )
        .first()
    )

    if existing:
        logger.warning(
            f"Dependency already exists: job {job_id} -> {depends_on_job_id}"
        )
        return existing

    # Check for circular dependency
    if would_create_cycle(db, job_id, depends_on_job_id):
        raise ValueError(f"Adding dependency would create circular reference")

    dependency = JobDependency(
        job_id=job_id, depends_on_job_id=depends_on_job_id, condition=condition
    )

    db.add(dependency)
    db.commit()
    db.refresh(dependency)

    logger.info(
        f"Added dependency: job {job_id} depends on job {depends_on_job_id} ({condition.value})"
    )

    # If job is PENDING and has unsatisfied dependencies, mark as BLOCKED
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if job and job.status == JobStatus.PENDING:
        if not are_all_dependencies_satisfied(db, job_id):
            job.status = JobStatus.BLOCKED
            db.commit()
            logger.info(f"Job {job_id} marked as BLOCKED (waiting for dependencies)")

    return dependency


def remove_dependency(db: Session, job_id: int, depends_on_job_id: int) -> bool:
    """
    Remove a dependency between two jobs.

    Args:
        db: Database session
        job_id: The job that has the dependency
        depends_on_job_id: The job it depends on

    Returns:
        True if dependency was removed
    """
    dependency = (
        db.query(JobDependency)
        .filter(
            and_(
                JobDependency.job_id == job_id,
                JobDependency.depends_on_job_id == depends_on_job_id,
            )
        )
        .first()
    )

    if not dependency:
        return False

    db.delete(dependency)
    db.commit()

    logger.info(f"Removed dependency: job {job_id} -> {depends_on_job_id}")
    return True


def would_create_cycle(db: Session, job_id: int, depends_on_job_id: int) -> bool:
    """
    Check if adding a dependency would create a cycle.

    Args:
        db: Database session
        job_id: The job that would have the dependency
        depends_on_job_id: The job it would depend on

    Returns:
        True if adding the dependency would create a cycle
    """
    # Check if depends_on_job_id (directly or indirectly) depends on job_id
    visited = set()
    to_check = [depends_on_job_id]

    while to_check:
        current = to_check.pop()

        if current in visited:
            continue
        visited.add(current)

        if current == job_id:
            return True

        # Get dependencies of current job
        deps = db.query(JobDependency).filter(JobDependency.job_id == current).all()

        for dep in deps:
            to_check.append(dep.depends_on_job_id)

    return False


# =============================================================================
# Job Unblocking (Called when parent jobs complete)
# =============================================================================


def check_and_unblock_dependent_jobs(db: Session, completed_job_id: int) -> List[int]:
    """
    Check all jobs that depend on a completed job and unblock those that are ready.

    Called when a job completes (success or failure).

    Args:
        db: Database session
        completed_job_id: The job that just completed

    Returns:
        List of job IDs that were unblocked
    """
    # Get all jobs that depend on the completed job
    dependent_deps = get_dependent_jobs(db, completed_job_id)

    if not dependent_deps:
        return []

    unblocked = []

    for dep in dependent_deps:
        job = db.query(IngestionJob).filter(IngestionJob.id == dep.job_id).first()

        if not job:
            continue

        # Only check BLOCKED jobs
        if job.status != JobStatus.BLOCKED:
            continue

        # Check if ALL dependencies are now satisfied
        if are_all_dependencies_satisfied(db, job.id):
            job.status = JobStatus.PENDING
            db.commit()

            unblocked.append(job.id)
            logger.info(f"Job {job.id} unblocked (all dependencies satisfied)")

    return unblocked


async def process_unblocked_jobs(db: Session, job_ids: List[int]):
    """
    Process jobs that were just unblocked by starting them.

    Args:
        db: Database session
        job_ids: List of job IDs to process
    """
    from app.api.v1.jobs import run_ingestion_job

    for job_id in job_ids:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job and job.status == JobStatus.PENDING:
            logger.info(f"Starting unblocked job {job_id}")
            await run_ingestion_job(job.id, job.source, job.config)


# =============================================================================
# Job Chain Functions
# =============================================================================


def create_chain(
    db: Session,
    name: str,
    chain_definition: List[Dict[str, Any]],
    description: Optional[str] = None,
) -> JobChain:
    """
    Create a new job chain definition.

    Args:
        db: Database session
        name: Unique name for the chain
        chain_definition: List of job configs with dependencies
        description: Optional description

    Returns:
        The created JobChain

    Example chain_definition:
        [
            {"source": "fred", "config": {"series_id": "GDP"}, "depends_on": []},
            {"source": "fred", "config": {"series_id": "UNRATE"}, "depends_on": []},
            {"source": "census", "config": {...}, "depends_on": [0, 1]}  # Depends on first two
        ]
    """
    # Validate chain definition
    if not chain_definition:
        raise ValueError("Chain definition cannot be empty")

    for i, job_def in enumerate(chain_definition):
        if "source" not in job_def:
            raise ValueError(f"Job {i} missing 'source' field")
        if "config" not in job_def:
            raise ValueError(f"Job {i} missing 'config' field")

        # Validate dependencies reference valid indices
        depends_on = job_def.get("depends_on", [])
        for dep_idx in depends_on:
            if dep_idx < 0 or dep_idx >= i:
                raise ValueError(
                    f"Job {i} has invalid dependency index {dep_idx}. "
                    f"Dependencies must reference earlier jobs (0 to {i-1})"
                )

    chain = JobChain(
        name=name, description=description, chain_definition=chain_definition
    )

    db.add(chain)
    db.commit()
    db.refresh(chain)

    logger.info(f"Created job chain '{name}' with {len(chain_definition)} jobs")
    return chain


def execute_chain(db: Session, chain_id: int) -> JobChainExecution:
    """
    Execute a job chain, creating all jobs and dependencies.

    Args:
        db: Database session
        chain_id: The chain to execute

    Returns:
        The JobChainExecution tracking this execution
    """
    chain = db.query(JobChain).filter(JobChain.id == chain_id).first()

    if not chain:
        raise ValueError(f"Chain {chain_id} not found")

    if not chain.is_active:
        raise ValueError(f"Chain '{chain.name}' is not active")

    # Create execution record
    execution = JobChainExecution(
        chain_id=chain_id,
        status="running",
        job_ids=[],
        total_jobs=len(chain.chain_definition),
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)

    # Create jobs and track their IDs
    created_jobs = []  # List of (index, job_id) tuples

    for i, job_def in enumerate(chain.chain_definition):
        # Determine initial status
        depends_on = job_def.get("depends_on", [])
        initial_status = JobStatus.BLOCKED if depends_on else JobStatus.PENDING

        # Create the job
        job = IngestionJob(
            source=job_def["source"],
            status=initial_status,
            config=job_def.get("config", {}),
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        created_jobs.append((i, job.id))

        # Create dependencies
        for dep_idx in depends_on:
            # Find the job ID for the dependency index
            parent_job_id = next(
                (jid for idx, jid in created_jobs if idx == dep_idx), None
            )
            if parent_job_id:
                condition_str = job_def.get("condition", "on_success")
                condition = DependencyCondition(condition_str)
                add_dependency(db, job.id, parent_job_id, condition)

    # Update execution with job IDs
    execution.job_ids = [jid for _, jid in created_jobs]
    db.commit()

    # Update chain statistics
    chain.times_executed += 1
    chain.last_executed_at = datetime.utcnow()
    db.commit()

    logger.info(
        f"Executed chain '{chain.name}' (execution {execution.id}), created {len(created_jobs)} jobs"
    )

    return execution


def get_chain_execution_status(db: Session, execution_id: int) -> Dict[str, Any]:
    """
    Get detailed status of a chain execution.

    Args:
        db: Database session
        execution_id: The execution ID

    Returns:
        Dictionary with execution status and job details
    """
    execution = (
        db.query(JobChainExecution).filter(JobChainExecution.id == execution_id).first()
    )

    if not execution:
        return None

    # Get all jobs in this execution
    jobs = db.query(IngestionJob).filter(IngestionJob.id.in_(execution.job_ids)).all()

    job_details = []
    for job in jobs:
        deps = get_job_dependencies(db, job.id)
        job_details.append(
            {
                "job_id": job.id,
                "source": job.source,
                "status": job.status.value,
                "dependencies": [d.depends_on_job_id for d in deps],
                "rows_inserted": job.rows_inserted,
                "error_message": job.error_message[:200] if job.error_message else None,
            }
        )

    return {
        "execution_id": execution.id,
        "chain_id": execution.chain_id,
        "status": execution.status,
        "total_jobs": execution.total_jobs,
        "completed_jobs": execution.completed_jobs,
        "successful_jobs": execution.successful_jobs,
        "failed_jobs": execution.failed_jobs,
        "started_at": execution.started_at.isoformat(),
        "completed_at": execution.completed_at.isoformat()
        if execution.completed_at
        else None,
        "jobs": job_details,
    }


def update_chain_execution_status(db: Session, execution_id: int):
    """
    Update the status of a chain execution based on job statuses.

    Called after jobs complete to update overall chain status.

    Args:
        db: Database session
        execution_id: The execution ID to update
    """
    execution = (
        db.query(JobChainExecution).filter(JobChainExecution.id == execution_id).first()
    )

    if not execution:
        return

    # Get all jobs in this execution
    jobs = db.query(IngestionJob).filter(IngestionJob.id.in_(execution.job_ids)).all()

    # Count by status
    completed = sum(
        1 for j in jobs if j.status in [JobStatus.SUCCESS, JobStatus.FAILED]
    )
    successful = sum(1 for j in jobs if j.status == JobStatus.SUCCESS)
    failed = sum(1 for j in jobs if j.status == JobStatus.FAILED)

    execution.completed_jobs = completed
    execution.successful_jobs = successful
    execution.failed_jobs = failed

    # Determine overall status
    if completed == execution.total_jobs:
        if failed == 0:
            execution.status = "success"
        elif successful == 0:
            execution.status = "failed"
        else:
            execution.status = "partial_success"
        execution.completed_at = datetime.utcnow()

    db.commit()


def get_execution_for_job(db: Session, job_id: int) -> Optional[JobChainExecution]:
    """
    Find the chain execution that contains a job.

    Args:
        db: Database session
        job_id: The job ID

    Returns:
        The JobChainExecution if found, None otherwise
    """
    executions = (
        db.query(JobChainExecution).filter(JobChainExecution.status == "running").all()
    )

    for execution in executions:
        if job_id in execution.job_ids:
            return execution

    return None
