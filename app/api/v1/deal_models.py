"""
Deal modeling API endpoints for PE roll-up analysis.

Provides scenario modeling, sensitivity analysis, deployment planning,
and executive summary generation.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.deal_engine import DEFAULT_ASSUMPTIONS, DealEngine, DealScenario

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Deal Models"])


# ──────────────────────────────────────────────────────────────────────
# Request models
# ──────────────────────────────────────────────────────────────────────

class DealModelRequest(BaseModel):
    """Run a deal model with custom assumptions."""
    state: Optional[str] = None
    min_grade: str = "A"
    benchmarks: Optional[Dict[str, Any]] = None
    assumptions: Optional[Dict[str, Any]] = None
    scenarios: Optional[Dict[str, Dict[str, Any]]] = None


class SensitivityRequest(BaseModel):
    """Run 2-way sensitivity analysis."""
    state: Optional[str] = None
    row_param: str = "exit_multiple"
    row_values: List[float] = Field(default=[6, 7, 8, 9, 10, 11, 12])
    col_param: str = "hold_years"
    col_values: List[float] = Field(default=[3, 4, 5, 6, 7])
    metric: str = "net_irr"
    base_assumptions: Optional[Dict[str, Any]] = None


class DeploymentPlanRequest(BaseModel):
    """Model phased acquisition timeline."""
    state: Optional[str] = None
    cohorts: Optional[List[Dict[str, Any]]] = None
    management_hires: Optional[List[Dict[str, Any]]] = None
    integration_cost_per_location: float = 50_000
    assumptions: Optional[Dict[str, Any]] = None


class ExecSummaryRequest(BaseModel):
    """Generate executive summary with narrative."""
    state: Optional[str] = None
    fund_size: Optional[float] = None
    target_close: str = "Q3 2026"
    assumptions: Optional[Dict[str, Any]] = None
    deployment: Optional[Dict[str, Any]] = None


class SaveScenarioRequest(BaseModel):
    """Save a scenario for later retrieval."""
    name: str
    description: Optional[str] = None
    state: Optional[str] = None
    benchmarks: Optional[Dict[str, Any]] = None
    assumptions: Optional[Dict[str, Any]] = None


# ──────────────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────────────

@router.post("/deal-models/run")
def run_deal_model(request: DealModelRequest, db: Session = Depends(get_db)):
    """Run ad-hoc deal model with custom assumptions. No persistence."""
    try:
        engine = DealEngine(db)
        result = engine.run_full_model(
            state=request.state,
            min_grade=request.min_grade,
            benchmarks=request.benchmarks,
            assumptions=request.assumptions,
            scenario_configs=request.scenarios,
        )
        return {"status": "success", "data": result}
    except Exception as e:
        logger.error(f"Deal model run failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deal-models/sensitivity")
def run_sensitivity(request: SensitivityRequest, db: Session = Depends(get_db)):
    """Run 2-way sensitivity analysis grid."""
    try:
        engine = DealEngine(db)
        portfolio = engine.get_target_portfolio(state=request.state)
        economics = engine.compute_tier_economics(portfolio["tier_counts"])

        result = engine.sensitivity_analysis(
            economics=economics,
            base_assumptions=request.base_assumptions,
            row_param=request.row_param,
            row_values=request.row_values,
            col_param=request.col_param,
            col_values=request.col_values,
            metric=request.metric,
        )
        return {"status": "success", "data": result}
    except Exception as e:
        logger.error(f"Sensitivity analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deal-models/deployment-plan")
def create_deployment_plan(request: DeploymentPlanRequest, db: Session = Depends(get_db)):
    """Model phased acquisition timeline with cohorts."""
    try:
        engine = DealEngine(db)
        portfolio = engine.get_target_portfolio(state=request.state)
        economics = engine.compute_tier_economics(portfolio["tier_counts"])

        result = engine.deployment_plan(
            economics=economics,
            cohorts=request.cohorts,
            management_hires=request.management_hires,
            integration_cost_per_location=request.integration_cost_per_location,
            assumptions=request.assumptions,
        )
        return {"status": "success", "data": result}
    except Exception as e:
        logger.error(f"Deployment plan failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deal-models/executive-summary")
def generate_executive_summary(request: ExecSummaryRequest, db: Session = Depends(get_db)):
    """Generate structured executive summary with narrative."""
    try:
        engine = DealEngine(db)
        a = {**DEFAULT_ASSUMPTIONS, **(request.assumptions or {})}

        portfolio = engine.get_target_portfolio(state=request.state)
        economics = engine.compute_tier_economics(portfolio["tier_counts"])
        scenarios = engine.run_scenarios(economics, a["scenarios"], a)

        if request.deployment:
            deployment = request.deployment
        else:
            deployment = engine.deployment_plan(economics=economics, assumptions=a)

        result = engine.executive_summary(
            portfolio=portfolio,
            economics=economics,
            scenario_results=scenarios,
            deployment=deployment,
            fund_size=request.fund_size,
            target_close=request.target_close,
            assumptions=a,
        )
        return {"status": "success", "data": result}
    except Exception as e:
        logger.error(f"Executive summary failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deal-models/scenarios")
def save_scenario(request: SaveScenarioRequest, db: Session = Depends(get_db)):
    """Save a named scenario for later retrieval."""
    try:
        engine = DealEngine(db)
        results = engine.run_full_model(
            state=request.state,
            benchmarks=request.benchmarks,
            assumptions=request.assumptions,
        )

        scenario = DealScenario(
            name=request.name,
            description=request.description,
            state_filter=request.state.upper() if request.state else None,
            benchmarks=request.benchmarks,
            assumptions=request.assumptions,
            results=results,
        )
        db.add(scenario)
        db.commit()
        db.refresh(scenario)

        return {
            "status": "success",
            "data": {
                "id": scenario.id,
                "name": scenario.name,
                "created_at": scenario.created_at.isoformat() if scenario.created_at else None,
            },
        }
    except Exception as e:
        db.rollback()
        logger.error(f"Save scenario failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deal-models/scenarios")
def list_scenarios(db: Session = Depends(get_db)):
    """List all saved scenarios."""
    try:
        scenarios = db.query(DealScenario).order_by(DealScenario.created_at.desc()).all()
        return {
            "status": "success",
            "data": [
                {
                    "id": s.id,
                    "name": s.name,
                    "description": s.description,
                    "state_filter": s.state_filter,
                    "created_at": s.created_at.isoformat() if s.created_at else None,
                    "updated_at": s.updated_at.isoformat() if s.updated_at else None,
                }
                for s in scenarios
            ],
        }
    except Exception as e:
        logger.error(f"List scenarios failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deal-models/scenarios/{scenario_id}")
def get_scenario(scenario_id: int, db: Session = Depends(get_db)):
    """Get scenario details + cached results."""
    scenario = db.query(DealScenario).filter(DealScenario.id == scenario_id).first()
    if not scenario:
        raise HTTPException(status_code=404, detail=f"Scenario {scenario_id} not found")

    return {
        "status": "success",
        "data": {
            "id": scenario.id,
            "name": scenario.name,
            "description": scenario.description,
            "state_filter": scenario.state_filter,
            "benchmarks": scenario.benchmarks,
            "assumptions": scenario.assumptions,
            "results": scenario.results,
            "created_at": scenario.created_at.isoformat() if scenario.created_at else None,
            "updated_at": scenario.updated_at.isoformat() if scenario.updated_at else None,
        },
    }


@router.delete("/deal-models/scenarios/{scenario_id}")
def delete_scenario(scenario_id: int, db: Session = Depends(get_db)):
    """Delete a saved scenario."""
    scenario = db.query(DealScenario).filter(DealScenario.id == scenario_id).first()
    if not scenario:
        raise HTTPException(status_code=404, detail=f"Scenario {scenario_id} not found")

    try:
        db.delete(scenario)
        db.commit()
        return {"status": "success", "message": f"Scenario {scenario_id} deleted"}
    except Exception as e:
        db.rollback()
        logger.error(f"Delete scenario failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
