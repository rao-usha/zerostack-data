"""
Eval Builder API — SPEC_039 / PLAN_041.

Endpoints:
  Suites
    GET    /evals/suites                 — list all suites (filterable by domain/priority)
    POST   /evals/suites                 — create suite
    GET    /evals/suites/{suite_id}      — get suite detail + cases
    PATCH  /evals/suites/{suite_id}      — update suite metadata
    DELETE /evals/suites/{suite_id}      — soft-delete (is_active=false)

  Cases
    GET    /evals/suites/{suite_id}/cases        — list cases for a suite
    POST   /evals/suites/{suite_id}/cases        — add case to suite
    PATCH  /evals/suites/{suite_id}/cases/{id}   — edit case (non-destructive — saves previous_params)
    DELETE /evals/suites/{suite_id}/cases/{id}   — soft-delete case

  Runs
    GET    /evals/runs                   — list recent runs (filterable by suite/regression)
    GET    /evals/runs/{run_id}          — run detail + per-case results
    POST   /evals/suites/{suite_id}/run  — trigger a run for one suite
    POST   /evals/run-priority           — trigger all active suites at a given priority level
    POST   /evals/dry-run/{suite_id}     — capture output only, no DB persist

  Seeding
    POST   /evals/suites/{suite_id}/seed-from-db — auto-generate baseline cases from current DB state

  Health & Alerting (SPEC_040)
    GET    /evals/health                          — per-suite latest score summary
    GET    /evals/regressions/recent              — last N runs with is_regression=True
    POST   /evals/suites/{suite_id}/cases/{id}/dry-run — score one case without DB persist
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.eval_models import EvalCase, EvalResult, EvalRun, EvalSuite

router = APIRouter(prefix="/evals", tags=["Eval Builder"])


# ===========================================================================
# Pydantic schemas
# ===========================================================================

class SuiteCreate(BaseModel):
    name: str
    description: Optional[str] = None
    domain: Optional[str] = None
    binding_type: str = Field(..., description="agent | api | report | db")
    binding_target: str
    eval_mode: str = Field("db_snapshot", description="db_snapshot | api_response | agent_output | report_output")
    priority: int = Field(2, ge=1, le=3)
    schedule_cron: Optional[str] = None


class SuiteUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    domain: Optional[str] = None
    binding_type: Optional[str] = None
    binding_target: Optional[str] = None
    eval_mode: Optional[str] = None
    priority: Optional[int] = None
    schedule_cron: Optional[str] = None
    is_active: Optional[bool] = None


class CaseCreate(BaseModel):
    name: str
    description: Optional[str] = None
    entity_type: Optional[str] = None
    entity_id: Optional[int] = None
    entity_name: Optional[str] = None
    assertion_type: str
    assertion_params: dict = Field(default_factory=dict)
    tier: int = Field(1, ge=1, le=3)
    weight: float = Field(1.0, ge=0.0)
    regression_threshold_pct: float = Field(15.0, ge=0.0)


class CaseUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    assertion_type: Optional[str] = None
    assertion_params: Optional[dict] = None
    tier: Optional[int] = None
    weight: Optional[float] = None
    regression_threshold_pct: Optional[float] = None
    is_active: Optional[bool] = None
    edit_reason: Optional[str] = None


class ResolveAction(BaseModel):
    action: str = Field(..., description="approve | reject")


class RunPriorityRequest(BaseModel):
    priority: int = Field(..., ge=1, le=3, description="1=daily, 2=weekly, 3=monthly")


# ===========================================================================
# Suite endpoints
# ===========================================================================

@router.get("/suites", summary="List eval suites")
def list_suites(
    domain: Optional[str] = Query(None),
    priority: Optional[int] = Query(None),
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
):
    q = db.query(EvalSuite)
    if active_only:
        q = q.filter(EvalSuite.is_active == True)
    if domain:
        q = q.filter(EvalSuite.domain == domain)
    if priority is not None:
        q = q.filter(EvalSuite.priority == priority)
    suites = q.order_by(EvalSuite.priority, EvalSuite.name).all()

    result = []
    for s in suites:
        case_count = db.query(EvalCase).filter(
            EvalCase.suite_id == s.id, EvalCase.is_active == True
        ).count()
        last_run = (
            db.query(EvalRun)
            .filter(EvalRun.suite_id == s.id, EvalRun.status == "completed")
            .order_by(EvalRun.triggered_at.desc())
            .first()
        )
        result.append({
            "id": s.id,
            "name": s.name,
            "description": s.description,
            "domain": s.domain,
            "binding_type": s.binding_type,
            "binding_target": s.binding_target,
            "eval_mode": s.eval_mode,
            "priority": s.priority,
            "schedule_cron": s.schedule_cron,
            "is_active": s.is_active,
            "case_count": case_count,
            "last_run_at": last_run.triggered_at.isoformat() if last_run else None,
            "last_run_score": float(last_run.composite_score) if last_run and last_run.composite_score is not None else None,
            "last_run_regression": last_run.is_regression if last_run else False,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        })
    return result


@router.post("/suites", summary="Create eval suite", status_code=201)
def create_suite(body: SuiteCreate, db: Session = Depends(get_db)):
    suite = EvalSuite(
        name=body.name,
        description=body.description,
        domain=body.domain,
        binding_type=body.binding_type,
        binding_target=body.binding_target,
        eval_mode=body.eval_mode,
        priority=body.priority,
        schedule_cron=body.schedule_cron,
    )
    db.add(suite)
    db.commit()
    db.refresh(suite)
    return {"id": suite.id, "name": suite.name, "status": "created"}


@router.get("/suites/{suite_id}", summary="Get suite detail with cases")
def get_suite(suite_id: int, db: Session = Depends(get_db)):
    suite = db.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise HTTPException(status_code=404, detail="Suite not found")

    cases = (
        db.query(EvalCase)
        .filter(EvalCase.suite_id == suite_id)
        .order_by(EvalCase.tier, EvalCase.id)
        .all()
    )

    return {
        "id": suite.id,
        "name": suite.name,
        "description": suite.description,
        "domain": suite.domain,
        "binding_type": suite.binding_type,
        "binding_target": suite.binding_target,
        "eval_mode": suite.eval_mode,
        "priority": suite.priority,
        "schedule_cron": suite.schedule_cron,
        "is_active": suite.is_active,
        "created_at": suite.created_at.isoformat() if suite.created_at else None,
        "cases": [
            {
                "id": c.id,
                "name": c.name,
                "description": c.description,
                "entity_type": c.entity_type,
                "entity_id": c.entity_id,
                "entity_name": c.entity_name,
                "assertion_type": c.assertion_type,
                "assertion_params": c.assertion_params,
                "tier": c.tier,
                "weight": float(c.weight) if c.weight is not None else 1.0,
                "regression_threshold_pct": float(c.regression_threshold_pct) if c.regression_threshold_pct is not None else 15.0,
                "is_active": c.is_active,
                "edited_at": c.edited_at.isoformat() if c.edited_at else None,
                "edit_reason": c.edit_reason,
                "created_at": c.created_at.isoformat() if c.created_at else None,
            }
            for c in cases
        ],
    }


@router.patch("/suites/{suite_id}", summary="Update suite metadata")
def update_suite(suite_id: int, body: SuiteUpdate, db: Session = Depends(get_db)):
    suite = db.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise HTTPException(status_code=404, detail="Suite not found")
    for field, val in body.model_dump(exclude_none=True).items():
        setattr(suite, field, val)
    db.commit()
    return {"status": "updated", "id": suite_id}


@router.delete("/suites/{suite_id}", summary="Soft-delete suite (is_active=false)")
def delete_suite(suite_id: int, db: Session = Depends(get_db)):
    suite = db.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise HTTPException(status_code=404, detail="Suite not found")
    suite.is_active = False
    db.commit()
    return {"status": "deactivated", "id": suite_id}


# ===========================================================================
# Case endpoints
# ===========================================================================

@router.get("/suites/{suite_id}/cases", summary="List cases for a suite")
def list_cases(
    suite_id: int,
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
):
    suite = db.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise HTTPException(status_code=404, detail="Suite not found")

    q = db.query(EvalCase).filter(EvalCase.suite_id == suite_id)
    if active_only:
        q = q.filter(EvalCase.is_active == True)
    cases = q.order_by(EvalCase.tier, EvalCase.id).all()

    return [
        {
            "id": c.id,
            "name": c.name,
            "entity_type": c.entity_type,
            "entity_id": c.entity_id,
            "entity_name": c.entity_name,
            "assertion_type": c.assertion_type,
            "assertion_params": c.assertion_params,
            "tier": c.tier,
            "weight": float(c.weight) if c.weight is not None else 1.0,
            "is_active": c.is_active,
            "edited_at": c.edited_at.isoformat() if c.edited_at else None,
        }
        for c in cases
    ]


@router.post("/suites/{suite_id}/cases", summary="Add case to suite", status_code=201)
def create_case(suite_id: int, body: CaseCreate, db: Session = Depends(get_db)):
    suite = db.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise HTTPException(status_code=404, detail="Suite not found")

    from app.services.eval_scorer import EvalScorer
    if body.assertion_type not in EvalScorer.SUPPORTED_TYPES:
        supported = sorted(EvalScorer.SUPPORTED_TYPES)
        raise HTTPException(
            status_code=422,
            detail=f"Unknown assertion_type '{body.assertion_type}'. Supported: {', '.join(supported)}",
        )

    case = EvalCase(
        suite_id=suite_id,
        name=body.name,
        description=body.description,
        entity_type=body.entity_type,
        entity_id=body.entity_id,
        entity_name=body.entity_name,
        assertion_type=body.assertion_type,
        assertion_params=body.assertion_params,
        tier=body.tier,
        weight=body.weight,
        regression_threshold_pct=body.regression_threshold_pct,
    )
    db.add(case)
    db.commit()
    db.refresh(case)
    return {"id": case.id, "name": case.name, "status": "created"}


@router.patch("/suites/{suite_id}/cases/{case_id}", summary="Edit case (non-destructive)")
def update_case(
    suite_id: int,
    case_id: int,
    body: CaseUpdate,
    db: Session = Depends(get_db),
):
    case = db.query(EvalCase).filter(
        EvalCase.id == case_id, EvalCase.suite_id == suite_id
    ).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    # Non-destructive edit: save previous assertion_params before overwriting
    if body.assertion_params is not None and body.assertion_params != case.assertion_params:
        case.previous_params = case.assertion_params
        case.edited_at = datetime.utcnow()
        case.edit_reason = body.edit_reason

    for field, val in body.model_dump(exclude_none=True, exclude={"edit_reason"}).items():
        setattr(case, field, val)

    db.commit()
    return {"status": "updated", "id": case_id}


@router.delete("/suites/{suite_id}/cases/{case_id}", summary="Soft-delete case")
def delete_case(suite_id: int, case_id: int, db: Session = Depends(get_db)):
    case = db.query(EvalCase).filter(
        EvalCase.id == case_id, EvalCase.suite_id == suite_id
    ).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")
    case.is_active = False
    db.commit()
    return {"status": "deactivated", "id": case_id}


# ===========================================================================
# Run endpoints
# ===========================================================================

@router.get("/runs", summary="List recent eval runs")
def list_runs(
    suite_id: Optional[int] = Query(None),
    regression_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = db.query(EvalRun)
    if suite_id is not None:
        q = q.filter(EvalRun.suite_id == suite_id)
    if regression_only:
        q = q.filter(EvalRun.is_regression == True)
    runs = q.order_by(EvalRun.triggered_at.desc()).limit(limit).all()

    # Resolve suite names in one pass
    suite_ids = list({r.suite_id for r in runs})
    suite_map = {
        s.id: s.name
        for s in db.query(EvalSuite).filter(EvalSuite.id.in_(suite_ids)).all()
    } if suite_ids else {}

    return [
        {
            "id": r.id,
            "suite_id": r.suite_id,
            "suite_name": suite_map.get(r.suite_id),
            "status": r.status,
            "triggered_by": r.triggered_by,
            "triggered_at": r.triggered_at.isoformat() if r.triggered_at else None,
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "composite_score": float(r.composite_score) if r.composite_score is not None else None,
            "tier1_pass_rate": float(r.tier1_pass_rate) if r.tier1_pass_rate is not None else None,
            "tier2_avg_score": float(r.tier2_avg_score) if r.tier2_avg_score is not None else None,
            "tier3_avg_score": float(r.tier3_avg_score) if r.tier3_avg_score is not None else None,
            "cases_total": r.cases_total,
            "cases_passed": r.cases_passed,
            "cases_failed": r.cases_failed,
            "is_regression": r.is_regression,
            "regression_details": r.regression_details,
            "llm_cost_usd": float(r.llm_cost_usd) if r.llm_cost_usd is not None else 0.0,
        }
        for r in runs
    ]


@router.get("/runs/{run_id}", summary="Get run detail with per-case results")
def get_run(run_id: int, db: Session = Depends(get_db)):
    run = db.query(EvalRun).filter(EvalRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    results = (
        db.query(EvalResult).filter(EvalResult.run_id == run_id).all()
    )
    case_ids = [r.case_id for r in results]
    case_map = {
        c.id: c
        for c in db.query(EvalCase).filter(EvalCase.id.in_(case_ids)).all()
    } if case_ids else {}

    suite = db.query(EvalSuite).filter(EvalSuite.id == run.suite_id).first()

    return {
        "id": run.id,
        "suite_id": run.suite_id,
        "suite_name": suite.name if suite else None,
        "status": run.status,
        "triggered_by": run.triggered_by,
        "triggered_at": run.triggered_at.isoformat() if run.triggered_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "composite_score": float(run.composite_score) if run.composite_score is not None else None,
        "tier1_pass_rate": float(run.tier1_pass_rate) if run.tier1_pass_rate is not None else None,
        "tier2_avg_score": float(run.tier2_avg_score) if run.tier2_avg_score is not None else None,
        "tier3_avg_score": float(run.tier3_avg_score) if run.tier3_avg_score is not None else None,
        "cases_total": run.cases_total,
        "cases_passed": run.cases_passed,
        "cases_failed": run.cases_failed,
        "is_regression": run.is_regression,
        "regression_details": run.regression_details,
        "errors": run.errors,
        "llm_cost_usd": float(run.llm_cost_usd) if run.llm_cost_usd is not None else 0.0,
        "results": [
            {
                "id": r.id,
                "case_id": r.case_id,
                "case_name": case_map[r.case_id].name if r.case_id in case_map else None,
                "tier": case_map[r.case_id].tier if r.case_id in case_map else None,
                "assertion_type": case_map[r.case_id].assertion_type if r.case_id in case_map else None,
                "passed": r.passed,
                "score": float(r.score) if r.score is not None else None,
                "partial_credit": r.partial_credit,
                "actual_value": r.actual_value,
                "expected_value": r.expected_value,
                "failure_reason": r.failure_reason,
                "llm_judge_score": float(r.llm_judge_score) if r.llm_judge_score is not None else None,
                "llm_judge_reasoning": r.llm_judge_reasoning,
                "evaluated_at": r.evaluated_at.isoformat() if r.evaluated_at else None,
            }
            for r in results
        ],
    }


@router.post("/suites/{suite_id}/run", summary="Trigger eval run for one suite")
def run_suite(
    suite_id: int,
    background_tasks: BackgroundTasks,
    triggered_by: str = Query("api"),
    db: Session = Depends(get_db),
):
    suite = db.query(EvalSuite).filter(
        EvalSuite.id == suite_id, EvalSuite.is_active == True
    ).first()
    if not suite:
        raise HTTPException(status_code=404, detail="Suite not found or inactive")

    case_count = db.query(EvalCase).filter(
        EvalCase.suite_id == suite_id, EvalCase.is_active == True
    ).count()
    if case_count == 0:
        raise HTTPException(status_code=400, detail="Suite has no active cases")

    # Create a placeholder run record immediately so the caller gets a run_id
    run = EvalRun(
        suite_id=suite_id,
        status="running",
        triggered_by=triggered_by,
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    run_id = run.id

    # Execute in background — non-blocking
    background_tasks.add_task(_execute_run, suite_id, run_id)

    return {
        "status": "started",
        "run_id": run_id,
        "suite_id": suite_id,
        "suite_name": suite.name,
        "cases": case_count,
    }


@router.post("/run-priority", summary="Trigger all active suites at a priority level")
def run_priority(
    body: RunPriorityRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    suites = db.query(EvalSuite).filter(
        EvalSuite.priority == body.priority,
        EvalSuite.is_active == True,
    ).all()

    if not suites:
        return {"status": "no_suites", "priority": body.priority, "started": 0}

    started = []
    for suite in suites:
        case_count = db.query(EvalCase).filter(
            EvalCase.suite_id == suite.id, EvalCase.is_active == True
        ).count()
        if case_count == 0:
            continue

        run = EvalRun(
            suite_id=suite.id,
            status="running",
            triggered_by="priority_run",
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        background_tasks.add_task(_execute_run, suite.id, run.id)
        started.append({"suite_id": suite.id, "suite_name": suite.name, "run_id": run.id})

    return {
        "status": "started",
        "priority": body.priority,
        "started": len(started),
        "runs": started,
    }


@router.post("/dry-run/{suite_id}", summary="Capture output only — no DB persist")
def dry_run_suite(
    suite_id: int,
    entity_id: Optional[int] = Query(None, description="Override entity to capture; defaults to first case entity"),
    db: Session = Depends(get_db),
):
    """
    Runs the capture phase only and returns the raw captured output.
    Useful for inspecting what the agent/API currently produces before
    building assertion cases.  No EvalRun or EvalResult rows are created.
    """
    from app.services.eval_runner import EvalRunner

    suite = db.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise HTTPException(status_code=404, detail="Suite not found")

    cases = db.query(EvalCase).filter(
        EvalCase.suite_id == suite_id, EvalCase.is_active == True
    ).all()

    eid = entity_id or (cases[0].entity_id if cases else None)

    try:
        runner = EvalRunner()
        captured = runner._capture_output(suite, eid, db)
        raw = captured.raw
        return {
            "suite_id": suite_id,
            "suite_name": suite.name,
            "eval_mode": suite.eval_mode,
            "entity_id": captured.entity_id,
            "has_error": captured.error is not None,
            "error": captured.error,
            "status_code": captured.status_code,
            "latency_ms": captured.latency_ms,
            "raw_keys": list(raw.keys()) if isinstance(raw, dict) else None,
            "raw_preview": str(raw)[:500] if raw else None,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Capture failed: {exc}")


# ===========================================================================
# Seed-from-DB endpoint
# ===========================================================================

@router.post(
    "/suites/{suite_id}/seed-from-db",
    summary="Auto-generate baseline cases from current DB state",
    status_code=201,
)
def seed_from_db(suite_id: int, db: Session = Depends(get_db)):
    """
    Reads the current DB for entities relevant to this suite and auto-creates
    baseline eval cases.  Skips entities that already have active cases.

    Works for db_snapshot suites only.  For agent/api suites, you must
    create cases manually via POST /suites/{id}/cases.
    """
    suite = db.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise HTTPException(status_code=404, detail="Suite not found")

    if suite.eval_mode != "db_snapshot":
        raise HTTPException(
            status_code=400,
            detail="seed-from-db only works for db_snapshot suites",
        )

    existing_entity_ids = {
        c.entity_id
        for c in db.query(EvalCase.entity_id)
        .filter(EvalCase.suite_id == suite_id, EvalCase.is_active == True)
        .all()
        if c.entity_id is not None
    }

    created = _seed_cases_for_suite(suite, existing_entity_ids, db)
    db.commit()

    return {
        "status": "seeded",
        "suite_id": suite_id,
        "cases_created": len(created),
        "cases": created,
    }


# ===========================================================================
# Background task helpers
# ===========================================================================

def _execute_run(suite_id: int, run_id: int) -> None:
    """Background task: run EvalRunner.run_suite() for a pre-created EvalRun."""
    from app.core.database import get_session_factory
    from app.services.eval_runner import EvalRunner

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        runner = EvalRunner()
        runner.run_suite(suite_id=suite_id, db=db, existing_run_id=run_id)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error(
            "EvalRunner.run_suite failed for suite=%s run=%s: %s", suite_id, run_id, exc
        )
        # Mark run failed
        run = db.query(EvalRun).filter(EvalRun.id == run_id).first()
        if run:
            run.status = "failed"
            run.completed_at = datetime.utcnow()
            run.errors = [str(exc)]
            db.commit()
    finally:
        db.close()


def _seed_cases_for_suite(suite: EvalSuite, existing_entity_ids: set, db: Session) -> list:
    """Generate baseline cases based on the suite binding_target + domain."""
    created = []
    target = (suite.binding_target or "").lower()

    if "company" in target or suite.domain in ("people", "org"):
        from app.core.people_models import IndustrialCompany, OrgChartSnapshot
        companies = (
            db.query(IndustrialCompany)
            .join(OrgChartSnapshot, OrgChartSnapshot.company_id == IndustrialCompany.id)
            .filter(IndustrialCompany.id.notin_(existing_entity_ids))
            .distinct()
            .all()
        )
        for company in companies:
            baseline_cases = _baseline_people_cases(suite.id, company)
            for c in baseline_cases:
                db.add(c)
                created.append({"entity_id": company.id, "entity_name": company.name, "assertion_type": c.assertion_type})

    elif "pe_firm" in target or suite.domain == "pe":
        from app.core.pe_models import PEFirm
        firms = db.query(PEFirm).filter(PEFirm.id.notin_(existing_entity_ids)).all()
        for firm in firms:
            baseline_cases = _baseline_pe_cases(suite.id, firm)
            for c in baseline_cases:
                db.add(c)
                created.append({"entity_id": firm.id, "entity_name": firm.name, "assertion_type": c.assertion_type})

    elif "three_pl" in target or suite.domain == "3pl":
        from app.core.models import ThreePLCompany
        companies = db.query(ThreePLCompany).filter(ThreePLCompany.id.notin_(existing_entity_ids)).all()
        for company in companies:
            c = EvalCase(
                suite_id=suite.id,
                name=f"{company.company_name} — has_website",
                entity_type="three_pl",
                entity_id=company.id,
                entity_name=company.company_name,
                assertion_type="has_website",
                assertion_params={},
                tier=2,
                weight=1.0,
            )
            db.add(c)
            created.append({"entity_id": company.id, "entity_name": company.company_name, "assertion_type": "has_website"})

    return created


def _baseline_people_cases(suite_id: int, company) -> list:
    """Generate standard people/org baseline cases for a company."""
    return [
        EvalCase(
            suite_id=suite_id,
            name=f"{company.name} — ceo_exists",
            entity_type="company",
            entity_id=company.id,
            entity_name=company.name,
            assertion_type="ceo_exists",
            assertion_params={},
            tier=1,
            weight=1.0,
        ),
        EvalCase(
            suite_id=suite_id,
            name=f"{company.name} — headcount_min",
            entity_type="company",
            entity_id=company.id,
            entity_name=company.name,
            assertion_type="headcount_range",
            assertion_params={"min": 3},
            tier=2,
            weight=1.0,
        ),
        EvalCase(
            suite_id=suite_id,
            name=f"{company.name} — org_depth_range",
            entity_type="company",
            entity_id=company.id,
            entity_name=company.name,
            assertion_type="org_depth_range",
            assertion_params={"min": 1, "max": 10},
            tier=2,
            weight=0.5,
        ),
    ]


def _baseline_pe_cases(suite_id: int, firm) -> list:
    """Generate standard PE firm baseline cases."""
    return [
        EvalCase(
            suite_id=suite_id,
            name=f"{firm.name} — deal_count_range",
            entity_type="pe_firm",
            entity_id=firm.id,
            entity_name=firm.name,
            assertion_type="deal_count_range",
            assertion_params={"min": 1, "max": 9999},
            tier=2,
            weight=1.0,
        ),
        EvalCase(
            suite_id=suite_id,
            name=f"{firm.name} — has_deal_with_status",
            entity_type="pe_firm",
            entity_id=firm.id,
            entity_name=firm.name,
            assertion_type="has_deal_with_status",
            assertion_params={"status": "closed"},
            tier=1,
            weight=1.0,
        ),
    ]


def _baseline_lp_cases(suite_id: int, db: Session) -> list:
    """Generate baseline LP count case."""
    try:
        from app.core.lp_models import LP
        total = db.query(LP).count()
    except Exception:
        total = 0
    return [
        EvalCase(
            suite_id=suite_id,
            name="LP record count",
            entity_type="lp",
            entity_id=None,
            entity_name="LP universe",
            assertion_type="lp_count_range",
            assertion_params={"min": max(1, int(total * 0.8)), "max": int(total * 1.5) + 100},
            tier=2,
            weight=1.0,
        ),
    ]


# ===========================================================================
# SPEC_040 — Health summary, regression feed, per-case dry-run
# ===========================================================================

@router.get("/health", summary="Per-suite eval health summary")
def eval_health(
    domain: Optional[str] = Query(None),
    priority: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Returns one entry per active suite showing the latest completed run's composite
    score. Uses a single LEFT JOIN query — no N+1.
    """
    # Subquery: max completed run id per suite
    latest_run_subq = (
        db.query(
            EvalRun.suite_id.label("suite_id"),
            func.max(EvalRun.id).label("max_id"),
        )
        .filter(EvalRun.status == "completed")
        .group_by(EvalRun.suite_id)
        .subquery()
    )

    suite_q = db.query(EvalSuite).filter(EvalSuite.is_active.is_(True))
    if domain:
        suite_q = suite_q.filter(EvalSuite.domain == domain)
    if priority:
        suite_q = suite_q.filter(EvalSuite.priority == priority)
    suites = suite_q.all()

    # Fetch latest runs in one query
    run_ids = (
        db.query(latest_run_subq.c.max_id)
        .all()
    )
    run_id_set = {r[0] for r in run_ids}
    runs_by_suite: dict = {}
    if run_id_set:
        for run in db.query(EvalRun).filter(EvalRun.id.in_(run_id_set)).all():
            runs_by_suite[run.suite_id] = run

    rows = []
    total_composite = 0.0
    suites_with_data = 0
    suites_with_regressions = 0
    last_run_at = None

    for suite in suites:
        run = runs_by_suite.get(suite.id)
        composite = float(run.composite_score) if run and run.composite_score is not None else None
        is_regression = bool(run.is_regression) if run else False
        run_at = run.triggered_at.isoformat() if run and run.triggered_at else None

        rows.append({
            "suite_id": suite.id,
            "suite_name": suite.name,
            "priority": suite.priority,
            "domain": suite.domain,
            "last_run_id": run.id if run else None,
            "last_run_at": run_at,
            "composite_score": composite,
            "is_regression": is_regression,
            "cases_passed": run.cases_passed if run else None,
            "cases_failed": run.cases_failed if run else None,
        })

        if composite is not None:
            suites_with_data += 1
            total_composite += composite
        if is_regression:
            suites_with_regressions += 1
        if run_at and (last_run_at is None or run_at > last_run_at):
            last_run_at = run_at

    avg_composite = round(total_composite / suites_with_data, 1) if suites_with_data else None

    return {
        "summary": {
            "total_suites": len(suites),
            "suites_with_data": suites_with_data,
            "suites_with_regressions": suites_with_regressions,
            "avg_composite": avg_composite,
            "last_run_at": last_run_at,
        },
        "suites": sorted(rows, key=lambda r: (r["priority"] or 9, r["suite_name"])),
    }


@router.get("/regressions/recent", summary="Recent runs with regressions")
def recent_regressions(
    limit: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Returns last N eval runs where is_regression=True, newest first."""
    runs = (
        db.query(EvalRun)
        .filter(EvalRun.is_regression.is_(True))
        .order_by(EvalRun.triggered_at.desc())
        .limit(limit)
        .all()
    )
    suite_ids = {r.suite_id for r in runs}
    suites = {s.id: s.name for s in db.query(EvalSuite).filter(EvalSuite.id.in_(suite_ids)).all()}

    return [
        {
            "run_id": r.id,
            "suite_id": r.suite_id,
            "suite_name": suites.get(r.suite_id, "Unknown"),
            "composite_score": float(r.composite_score) if r.composite_score is not None else None,
            "triggered_by": r.triggered_by,
            "triggered_at": r.triggered_at.isoformat() if r.triggered_at else None,
            "regressions": r.regression_details or [],
        }
        for r in runs
    ]


@router.post(
    "/suites/{suite_id}/cases/{case_id}/dry-run",
    summary="Score one case without persisting to DB",
)
def dry_run_case(
    suite_id: int,
    case_id: int,
    params_override: Optional[dict] = Body(None),
    db: Session = Depends(get_db),
):
    """
    Captures current output and runs the scorer for one case.
    Optionally accepts assertion_params override in the request body to test
    param changes before committing the edit.
    No EvalRun or EvalResult rows are created.
    """
    from app.services.eval_runner import EvalRunner
    from app.services.eval_scorer import EvalScorer

    suite = db.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise HTTPException(status_code=404, detail="Suite not found")

    case = db.query(EvalCase).filter(
        EvalCase.id == case_id,
        EvalCase.suite_id == suite_id,
    ).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    # Apply optional param override (does not write to DB)
    if params_override:
        from copy import copy
        case = copy(case)
        case.assertion_params = {**(case.assertion_params or {}), **params_override}

    runner = EvalRunner()
    output = runner._capture_output(suite, case.entity_id, db)
    result = EvalScorer.score(case, output, db)

    return {
        "case_id": case_id,
        "assertion_type": case.assertion_type,
        "assertion_params": case.assertion_params,
        "passed": result.passed,
        "score": result.score,
        "partial_credit": result.partial_credit,
        "actual_value": result.actual_value,
        "expected_value": result.expected_value,
        "failure_reason": result.failure_reason,
        "capture_mode": output.mode,
        "capture_error": output.error,
    }
