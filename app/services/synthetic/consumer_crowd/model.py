"""
Distilled consumer-crowd inference model (PLAN_062 addendum 1, Phase 2.2).

Loads the 5 LightGBM artifacts from the distilled_v1/ directory and exposes
a clean predict() API for the Phase 3 FastAPI router (PLAN_062 addendum 1
Phase 3 — not yet shipped).

Singleton pattern: load once at app startup, reuse for all requests.
Predict latency target: <5ms for a 50-persona crowd response.

Usage:
    model = DistilledCrowdModel.load(Path("/app/data/reports/synthetic_consumer/distilled_v1/"))
    response = model.predict_one(persona, scenario)  # single (persona, scenario)
    crowd = model.predict_crowd(scenario, personas=PERSONAS)  # whole crowd

Inference returns null for outcome variables that the scenario marks
not-applicable (e.g., churn_probability for a new-relationship scenario),
matching the teacher's null-handling.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import lightgbm as lgb
import numpy as np
import pandas as pd

from app.services.synthetic.consumer_crowd.distiller import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    OUTCOMES,
)
from app.services.synthetic.consumer_crowd.personas import (
    PERSONAS,
    Persona,
    get_persona_by_id,
)
from app.services.synthetic.consumer_crowd.scenarios import (
    Scenario,
    get_scenario_by_id,
)

logger = logging.getLogger(__name__)


@dataclass
class CrowdResponse:
    """Per-persona predicted response for a given scenario."""
    persona_id: int
    persona_archetype: str
    purchase_intent: Optional[float]
    wtp_delta: Optional[float]
    sentiment: Optional[float]
    wom_amplitude: Optional[float]
    churn_probability: Optional[float]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AggregateStats:
    """Aggregated stats across a crowd response."""
    purchase_intent_mean: float
    purchase_intent_std: float
    wtp_delta_mean: float
    wtp_delta_std: float
    sentiment_mean: float
    sentiment_std: float
    wom_amplitude_mean: float
    wom_amplitude_std: float
    churn_probability_mean: Optional[float]  # None if no churn-applicable personas
    churn_probability_std: Optional[float]
    n_personas: int
    n_with_churn: int

    def to_dict(self) -> dict:
        return asdict(self)


class DistilledCrowdModel:
    """
    Wrapper around the 5 LightGBM artifacts. Built by Phase 2 distiller;
    loaded at API startup; serves the Phase 3 endpoint.
    """

    def __init__(self, model_dir: Path):
        self.model_dir = model_dir
        self.models: Dict[str, lgb.Booster] = {}
        self.feature_spec: Dict = {}

    @classmethod
    def load(cls, model_dir: Path) -> "DistilledCrowdModel":
        m = cls(model_dir)

        # Load feature spec
        spec_path = model_dir / "feature_spec.json"
        if not spec_path.exists():
            raise FileNotFoundError(f"feature_spec.json missing in {model_dir}")
        m.feature_spec = json.loads(spec_path.read_text())

        # Load each outcome model
        for outcome in OUTCOMES:
            model_path = model_dir / f"{outcome}.txt"
            if not model_path.exists():
                raise FileNotFoundError(f"Missing LGB artifact: {model_path}")
            m.models[outcome] = lgb.Booster(model_file=str(model_path))

        logger.info("Loaded DistilledCrowdModel from %s (%d outcomes)", model_dir, len(m.models))
        return m

    # ------------------------------------------------------------------
    # Single-row prediction (used internally and by the smoke tests)
    # ------------------------------------------------------------------

    def predict_one(
        self,
        persona: Persona,
        scenario: Scenario,
    ) -> CrowdResponse:
        """Predict response for one (persona, scenario) cell."""
        df = self._feature_row(persona, scenario)
        results = {}
        for outcome in OUTCOMES:
            if outcome in scenario.outcomes_applicable:
                # LightGBM expects categorical cols typed as category
                pred = self.models[outcome].predict(df)[0]
                # Clip predictions to the outcome's natural range
                pred = self._clip_outcome(outcome, float(pred))
                results[outcome] = pred
            else:
                results[outcome] = None

        return CrowdResponse(
            persona_id=persona.id,
            persona_archetype=persona.archetype,
            **results,
        )

    # ------------------------------------------------------------------
    # Crowd prediction — vectorized batch over all personas
    # ------------------------------------------------------------------

    def predict_crowd(
        self,
        scenario: Scenario,
        personas: Optional[List[Persona]] = None,
    ) -> List[CrowdResponse]:
        """
        Predict response for an entire crowd given one scenario.
        Vectorized: builds one DataFrame and runs each model once.
        """
        target_personas = personas if personas is not None else PERSONAS
        df = self._feature_batch(target_personas, scenario)

        # Run each outcome model once across the whole batch
        preds_by_outcome: Dict[str, np.ndarray] = {}
        for outcome in OUTCOMES:
            if outcome in scenario.outcomes_applicable:
                preds_by_outcome[outcome] = self.models[outcome].predict(df)
            else:
                preds_by_outcome[outcome] = None  # type: ignore

        responses: List[CrowdResponse] = []
        for i, p in enumerate(target_personas):
            responses.append(CrowdResponse(
                persona_id=p.id,
                persona_archetype=p.archetype,
                purchase_intent=self._clip_outcome("purchase_intent", float(preds_by_outcome["purchase_intent"][i])) if preds_by_outcome["purchase_intent"] is not None else None,
                wtp_delta=self._clip_outcome("wtp_delta", float(preds_by_outcome["wtp_delta"][i])) if preds_by_outcome["wtp_delta"] is not None else None,
                sentiment=self._clip_outcome("sentiment", float(preds_by_outcome["sentiment"][i])) if preds_by_outcome["sentiment"] is not None else None,
                wom_amplitude=self._clip_outcome("wom_amplitude", float(preds_by_outcome["wom_amplitude"][i])) if preds_by_outcome["wom_amplitude"] is not None else None,
                churn_probability=self._clip_outcome("churn_probability", float(preds_by_outcome["churn_probability"][i])) if preds_by_outcome["churn_probability"] is not None else None,
            ))
        return responses

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def aggregate(self, responses: List[CrowdResponse]) -> AggregateStats:
        """Compute mean+std across a crowd response for each outcome."""
        arrs = {
            outcome: np.array(
                [getattr(r, outcome) for r in responses if getattr(r, outcome) is not None],
                dtype=float,
            )
            for outcome in OUTCOMES
        }

        def _mean_std(a):
            if len(a) == 0:
                return None, None
            return float(np.mean(a)), float(np.std(a))

        pi_mean, pi_std = _mean_std(arrs["purchase_intent"])
        wtp_mean, wtp_std = _mean_std(arrs["wtp_delta"])
        sent_mean, sent_std = _mean_std(arrs["sentiment"])
        wom_mean, wom_std = _mean_std(arrs["wom_amplitude"])
        churn_mean, churn_std = _mean_std(arrs["churn_probability"])

        return AggregateStats(
            purchase_intent_mean=pi_mean if pi_mean is not None else 0.0,
            purchase_intent_std=pi_std if pi_std is not None else 0.0,
            wtp_delta_mean=wtp_mean if wtp_mean is not None else 0.0,
            wtp_delta_std=wtp_std if wtp_std is not None else 0.0,
            sentiment_mean=sent_mean if sent_mean is not None else 0.0,
            sentiment_std=sent_std if sent_std is not None else 0.0,
            wom_amplitude_mean=wom_mean if wom_mean is not None else 0.0,
            wom_amplitude_std=wom_std if wom_std is not None else 0.0,
            churn_probability_mean=churn_mean,
            churn_probability_std=churn_std,
            n_personas=len(responses),
            n_with_churn=len(arrs["churn_probability"]),
        )

    # ------------------------------------------------------------------
    # Internals — feature construction
    # ------------------------------------------------------------------

    def _feature_row(self, persona: Persona, scenario: Scenario) -> pd.DataFrame:
        row = {
            "age_bracket": persona.age_bracket,
            "income_bracket": persona.income_bracket,
            "geography": persona.geography,
            "price_sensitivity": persona.price_sensitivity,
            "brand_loyalty": persona.brand_loyalty,
            "novelty_seeking": persona.novelty_seeking,
            "scenario_category": scenario.category,
            "scenario_event_type": scenario.event_type,
            "scenario_magnitude": scenario.magnitude,
        }
        df = pd.DataFrame([row])
        return self._coerce_dtypes(df)

    def _feature_batch(self, personas: List[Persona], scenario: Scenario) -> pd.DataFrame:
        rows = [
            {
                "age_bracket": p.age_bracket,
                "income_bracket": p.income_bracket,
                "geography": p.geography,
                "price_sensitivity": p.price_sensitivity,
                "brand_loyalty": p.brand_loyalty,
                "novelty_seeking": p.novelty_seeking,
                "scenario_category": scenario.category,
                "scenario_event_type": scenario.event_type,
                "scenario_magnitude": scenario.magnitude,
            }
            for p in personas
        ]
        df = pd.DataFrame(rows)
        return self._coerce_dtypes(df)

    @staticmethod
    def _coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        for col in CATEGORICAL_FEATURES:
            df[col] = df[col].astype("category")
        # scenario_magnitude can be None → pandas infers object; LightGBM needs numeric
        df["scenario_magnitude"] = pd.to_numeric(df["scenario_magnitude"], errors="coerce")
        return df

    @staticmethod
    def _clip_outcome(outcome: str, value: float) -> float:
        """Clip predictions to the outcome's natural range."""
        if outcome in ("purchase_intent", "wom_amplitude", "churn_probability"):
            return max(0.0, min(1.0, value))
        elif outcome in ("wtp_delta", "sentiment"):
            return max(-1.0, min(1.0, value))
        return value


# -----------------------------------------------------------------------
# Singleton accessor (used by Phase 3 API)
# -----------------------------------------------------------------------

_DEFAULT_MODEL_DIR = Path("/app/data/reports/synthetic_consumer/distilled_v1/")


@lru_cache(maxsize=1)
def get_default_model() -> DistilledCrowdModel:
    """Lazy-load the default distilled model. Cached as a singleton."""
    return DistilledCrowdModel.load(_DEFAULT_MODEL_DIR)
