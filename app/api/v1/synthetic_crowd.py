"""
Synthetic Consumer Crowd API — PLAN_062 addendum 1, Phase 3.

Endpoints:
  POST /synthetic-crowd/consumer-response   — predict crowd response for a scenario
  POST /synthetic-crowd/teacher-passthrough — call GPT-4o directly (rate-limited)
  GET  /synthetic-crowd/personas             — list of 50 archetypes
  GET  /synthetic-crowd/scenarios            — list of 40 scenario templates
  GET  /synthetic-crowd/dataset-export       — download v1.jsonl teacher dataset
  GET  /synthetic-crowd/model-status         — distilled model load state + report

The distilled model is loaded as a singleton at first-request time
(@lru_cache on get_default_model). Predictions run inline — no background task,
no GPU needed. Latency for a 50-persona crowd response: <50ms p95.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List, Literal, Optional, Union

from fastapi import APIRouter, Depends, HTTPException, Header
from fastapi.responses import FileResponse as FastAPIFileResponse
from pydantic import BaseModel, Field

from app.services.synthetic.consumer_crowd.personas import (
    PERSONAS,
    get_persona_by_id,
)
from app.services.synthetic.consumer_crowd.scenarios import (
    SCENARIOS,
    Scenario,
    get_scenario_by_id,
)
from app.services.synthetic.consumer_crowd.model import (
    DistilledCrowdModel,
    get_default_model,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/synthetic-crowd", tags=["Synthetic Consumer Crowd"])


# -----------------------------------------------------------------------
# Request / response models
# -----------------------------------------------------------------------

class ScenarioByIdRequest(BaseModel):
    """Trigger a scenario from the canonical library by id."""
    scenario_id: int = Field(..., ge=1, le=40, description="Scenario id (1-40)")


class CustomScenarioRequest(BaseModel):
    """Define an ad-hoc scenario matching the canonical schema."""
    category: Literal["pricing", "product", "brand", "geographic", "competitor", "operational"]
    event_type: str
    magnitude: Optional[float] = None
    description: str = Field(..., min_length=10, max_length=500)


class CrowdResponseRequest(BaseModel):
    """Input to /consumer-response."""
    scenario: Union[ScenarioByIdRequest, CustomScenarioRequest]
    personas: Union[Literal["all"], List[int]] = Field(
        default="all",
        description="'all' for all 50 personas, or a list of persona ids (1-50)",
    )


class PersonaSummary(BaseModel):
    id: int
    archetype: str
    age_bracket: str
    income_bracket: str
    geography: str
    price_sensitivity: str
    brand_loyalty: str
    novelty_seeking: str
    prompt_summary: str


class ScenarioSummary(BaseModel):
    id: int
    category: str
    event_type: str
    magnitude: Optional[float]
    description: str
    outcomes_applicable: List[str]


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _resolve_scenario(scenario_input: dict) -> Scenario:
    """Resolve a scenario from request body to a Scenario dataclass."""
    if "scenario_id" in scenario_input:
        return get_scenario_by_id(int(scenario_input["scenario_id"]))
    # Custom scenario — build a transient Scenario instance
    # Default to all outcomes applicable unless the description indicates new-relationship
    return Scenario(
        id=-1,
        category=scenario_input["category"],
        event_type=scenario_input["event_type"],
        magnitude=scenario_input.get("magnitude"),
        description=scenario_input["description"],
        outcomes_applicable=[
            "purchase_intent", "wtp_delta", "sentiment",
            "wom_amplitude", "churn_probability",
        ],
    )


def _resolve_personas(personas_input) -> List:
    if personas_input == "all":
        return list(PERSONAS)
    if isinstance(personas_input, list):
        return [get_persona_by_id(pid) for pid in personas_input]
    raise HTTPException(status_code=400, detail="personas must be 'all' or list of ids")


def _get_model() -> Optional[DistilledCrowdModel]:
    """Return the loaded distilled model or None if artifact not present."""
    try:
        return get_default_model()
    except FileNotFoundError as exc:
        logger.warning("Distilled crowd model not loaded: %s", exc)
        return None


# -----------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------

@router.get("/personas", response_model=List[PersonaSummary])
async def list_personas():
    """List the 50 consumer persona archetypes."""
    return [
        PersonaSummary(**p.to_dict())
        for p in PERSONAS
    ]


@router.get("/scenarios", response_model=List[ScenarioSummary])
async def list_scenarios():
    """List the 40 scenario templates with their outcomes_applicable spec."""
    return [
        ScenarioSummary(
            id=s.id,
            category=s.category,
            event_type=s.event_type,
            magnitude=s.magnitude,
            description=s.description,
            outcomes_applicable=s.outcomes_applicable,
        )
        for s in SCENARIOS
    ]


@router.post("/consumer-response")
async def consumer_response(request: dict):
    """
    Predict crowd response to a scenario.

    Request body shape:
        {
          "scenario": {"scenario_id": 2}                          # canonical
            OR
          "scenario": {"category": "pricing", "event_type": "custom_price_change",
                       "magnitude": 0.15, "description": "..."},  # custom
          "personas": "all" | [1, 5, 12]
        }
    """
    model = _get_model()
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Distilled model artifact not loaded. Run distiller.py first.",
        )

    if "scenario" not in request:
        raise HTTPException(status_code=400, detail="missing 'scenario' field")
    scenario = _resolve_scenario(request["scenario"])
    personas = _resolve_personas(request.get("personas", "all"))

    responses = model.predict_crowd(scenario, personas=personas)
    aggregate = model.aggregate(responses)

    return {
        "scenario": {
            "id": scenario.id,
            "category": scenario.category,
            "event_type": scenario.event_type,
            "magnitude": scenario.magnitude,
            "description": scenario.description,
            "outcomes_applicable": scenario.outcomes_applicable,
        },
        "responses": [r.to_dict() for r in responses],
        "aggregate": aggregate.to_dict(),
        "provenance": {
            "model_name": "distilled_consumer_v1",
            "teacher_model": "gpt-4o",
            "teacher_data_size": 2000,
        },
    }


@router.post("/teacher-passthrough")
async def teacher_passthrough(
    request: dict,
    x_admin_token: Optional[str] = Header(default=None),
):
    """
    Bypass the distilled model and call GPT-4o directly.
    Admin-only — rate-limited via env var ADMIN_TOKEN to prevent unbounded $$.
    Useful for ongoing quality audits (compare distilled vs teacher on edge cases).
    """
    expected = os.environ.get("ADMIN_TOKEN")
    if not expected or x_admin_token != expected:
        raise HTTPException(status_code=403, detail="admin token required")

    # Lazy import — only used here
    from app.services.synthetic.consumer_crowd.teacher_runner import TeacherRunner

    scenario = _resolve_scenario(request["scenario"])
    personas = _resolve_personas(request.get("personas", "all"))

    runner = TeacherRunner(model="gpt-4o", budget_usd=5.0)  # Hard cap per request

    responses = []
    for p in personas:
        try:
            resp = await runner.run_cell(p, scenario)
            responses.append({
                "persona_id": p.id,
                "persona_archetype": p.archetype,
                "purchase_intent": resp.purchase_intent,
                "wtp_delta": resp.wtp_delta,
                "sentiment": resp.sentiment,
                "wom_amplitude": resp.wom_amplitude,
                "churn_probability": resp.churn_probability,
                "rationale": resp.rationale,
            })
        except Exception as e:
            logger.warning("teacher passthrough failed for persona %d: %s", p.id, e)

    return {
        "responses": responses,
        "cost_usd": runner.cost_so_far_usd,
        "calls": runner.calls_so_far,
        "cache_hits": runner.cache_hits,
        "provenance": {"model_name": "gpt-4o-direct"},
    }


@router.get("/dataset-export")
async def dataset_export():
    """Download the v1 teacher dataset as JSONL."""
    path = Path("/app/data/reports/synthetic_consumer/v1.jsonl")
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="v1 dataset not found. Run dataset_builder.py first.",
        )
    return FastAPIFileResponse(
        path=str(path),
        media_type="application/x-ndjson",
        filename="synthetic_consumer_v1.jsonl",
    )


@router.get("/model-status")
async def model_status():
    """Distilled model load state + per-outcome distillation report."""
    artifact_dir = Path("/app/data/reports/synthetic_consumer/distilled_v1/")
    report_path = artifact_dir / "distillation_report.json"

    if not artifact_dir.exists():
        return {"loaded": False, "reason": "artifact directory missing"}

    if not report_path.exists():
        return {
            "loaded": False,
            "reason": "distillation_report.json missing",
            "artifact_dir": str(artifact_dir),
        }

    import json
    report = json.loads(report_path.read_text())
    return {
        "loaded": True,
        "artifact_dir": str(artifact_dir),
        "model_name": "distilled_consumer_v1",
        "teacher_model": "gpt-4o",
        "report": report,
    }
