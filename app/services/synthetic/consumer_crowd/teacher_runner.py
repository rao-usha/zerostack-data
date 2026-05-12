"""
GPT-4o teacher orchestrator for the synthetic consumer crowd
(SPEC_050 / PLAN_062 addendum 1, Phase 1).

Responsibilities:
- Build the structured prompt (persona context + scenario + optional brand context)
- Call OpenAI's chat.completions API with JSON-schema response format
- Retry on 429 / 5xx with exponential backoff
- Validate outcome ranges (purchase_intent ∈ [0,1], wtp_delta ∈ [-1,1], etc.)
- Track cumulative cost; raise BudgetExceededError before exceeding budget
- Cache responses on disk keyed by (persona_id, scenario_id, magnitude, brand_context_hash)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from openai import AsyncOpenAI, APIError, RateLimitError, APITimeoutError

from app.services.synthetic.consumer_crowd.personas import Persona
from app.services.synthetic.consumer_crowd.scenarios import Scenario

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------
# Pricing — GPT-4o (per OpenAI public pricing as of 2026-05)
# -----------------------------------------------------------------------

GPT4O_INPUT_USD_PER_1M = 2.50
GPT4O_OUTPUT_USD_PER_1M = 10.00


# -----------------------------------------------------------------------
# Response schema
# -----------------------------------------------------------------------

# Strict JSON Schema for OpenAI structured output. Every field is required;
# nulls represented as null (structured-output mode handles null in unions).
RESPONSE_JSON_SCHEMA = {
    "name": "consumer_crowd_response",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "purchase_intent",
            "purchase_intent_confidence",
            "wtp_delta",
            "wtp_delta_confidence",
            "sentiment",
            "sentiment_confidence",
            "wom_amplitude",
            "wom_amplitude_confidence",
            "churn_probability",
            "churn_probability_confidence",
            "rationale",
        ],
        "properties": {
            "purchase_intent": {
                "type": "number",
                "description": "Likelihood of buying within 3 months given the scenario. 0 = will not buy, 1 = will definitely buy.",
            },
            "purchase_intent_confidence": {"type": "number"},
            "wtp_delta": {
                "type": "number",
                "description": "Change in willingness to pay relative to baseline. -1 = will only pay 0% of baseline, 0 = no change, +1 = will pay 2x baseline.",
            },
            "wtp_delta_confidence": {"type": "number"},
            "sentiment": {
                "type": "number",
                "description": "Emotional reaction. -1 = very negative, 0 = neutral, +1 = very positive.",
            },
            "sentiment_confidence": {"type": "number"},
            "wom_amplitude": {
                "type": "number",
                "description": "Likelihood of telling others (positive or negative). 0 = will not mention, 1 = will mention to many.",
            },
            "wom_amplitude_confidence": {"type": "number"},
            "churn_probability": {
                "type": ["number", "null"],
                "description": "Likelihood of switching away from this brand. Null if there's no existing relationship to churn from.",
            },
            "churn_probability_confidence": {"type": ["number", "null"]},
            "rationale": {
                "type": "string",
                "description": "One sentence explaining the most important driver of this response.",
            },
        },
    },
}


SYSTEM_PROMPT = """You simulate a single consumer's response to a business scenario for market-research purposes.

Return ONLY a JSON object matching the requested schema. No prose outside JSON.

Outcome variables — interpret each as follows:
- purchase_intent (0 to 1): likelihood of buying within 3 months given the scenario
- wtp_delta (-1 to +1): change in willingness to pay; -1 = will only pay 0% of baseline (won't buy at any price), 0 = no change, +1 = will pay 2x baseline
- sentiment (-1 to +1): emotional reaction
- wom_amplitude (0 to 1): likelihood of telling others
- churn_probability (0 to 1, or null): likelihood of switching away from this brand. Return null if the persona has no existing relationship to churn from in this scenario.

For each numeric outcome, also return a *_confidence (0 to 1) reflecting your certainty.

Reason from the persona's stated demographics + psychographics. Be consistent: a persona with high price_sensitivity should respond more negatively to price increases than one with low price_sensitivity, etc."""


# -----------------------------------------------------------------------
# Data classes
# -----------------------------------------------------------------------

@dataclass(frozen=True)
class CellKey:
    """Cache + identity key for one (persona, scenario, brand_context) cell."""
    persona_id: int
    scenario_id: int
    magnitude: Optional[float]
    brand_context_hash: str  # sha256 of brand_context (or empty string if none)

    def to_hash(self) -> str:
        s = f"{self.persona_id}|{self.scenario_id}|{self.magnitude}|{self.brand_context_hash}"
        return hashlib.sha256(s.encode()).hexdigest()


@dataclass
class TeacherResponse:
    # Outcome variables (some may be None if not applicable per scenario)
    purchase_intent: Optional[float]
    wtp_delta: Optional[float]
    sentiment: Optional[float]
    wom_amplitude: Optional[float]
    churn_probability: Optional[float]
    # Confidences
    purchase_intent_confidence: Optional[float]
    wtp_delta_confidence: Optional[float]
    sentiment_confidence: Optional[float]
    wom_amplitude_confidence: Optional[float]
    churn_probability_confidence: Optional[float]
    # Provenance
    rationale: str
    teacher_model: str
    teacher_call_id: str
    generated_at: str  # ISO 8601
    cost_usd: float
    input_tokens: int
    output_tokens: int
    from_cache: bool = False

    def to_dict(self) -> Dict:
        return asdict(self)


class BudgetExceededError(Exception):
    """Raised when next teacher call would push past the configured budget."""


# -----------------------------------------------------------------------
# Validation helpers
# -----------------------------------------------------------------------

def _clamp_value(value: Optional[float], lo: float, hi: float, field_name: str) -> Optional[float]:
    """Return value if in [lo, hi]; clamp + warn if slightly out; raise if wildly out."""
    if value is None:
        return None
    if lo <= value <= hi:
        return float(value)
    # Allow small overshoots (within 0.05) by clamping
    if lo - 0.05 <= value <= hi + 0.05:
        clamped = max(lo, min(hi, value))
        logger.warning("Clamped %s: %.4f → %.4f", field_name, value, clamped)
        return clamped
    raise ValueError(f"{field_name}={value} is wildly out of expected range [{lo}, {hi}]")


def _validate_response_ranges(raw: Dict) -> Dict:
    """T8: validate outcome ranges; raise on wild values, clamp on small overshoot."""
    return {
        "purchase_intent": _clamp_value(raw.get("purchase_intent"), 0.0, 1.0, "purchase_intent"),
        "purchase_intent_confidence": _clamp_value(raw.get("purchase_intent_confidence"), 0.0, 1.0, "purchase_intent_confidence"),
        "wtp_delta": _clamp_value(raw.get("wtp_delta"), -1.0, 1.0, "wtp_delta"),
        "wtp_delta_confidence": _clamp_value(raw.get("wtp_delta_confidence"), 0.0, 1.0, "wtp_delta_confidence"),
        "sentiment": _clamp_value(raw.get("sentiment"), -1.0, 1.0, "sentiment"),
        "sentiment_confidence": _clamp_value(raw.get("sentiment_confidence"), 0.0, 1.0, "sentiment_confidence"),
        "wom_amplitude": _clamp_value(raw.get("wom_amplitude"), 0.0, 1.0, "wom_amplitude"),
        "wom_amplitude_confidence": _clamp_value(raw.get("wom_amplitude_confidence"), 0.0, 1.0, "wom_amplitude_confidence"),
        "churn_probability": _clamp_value(raw.get("churn_probability"), 0.0, 1.0, "churn_probability"),
        "churn_probability_confidence": _clamp_value(raw.get("churn_probability_confidence"), 0.0, 1.0, "churn_probability_confidence"),
        "rationale": str(raw.get("rationale", ""))[:500],
    }


# -----------------------------------------------------------------------
# Cache
# -----------------------------------------------------------------------

class _DiskCache:
    """Simple file-per-key JSON cache for teacher responses."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, key_hash: str) -> Path:
        # Shard into 2-char prefix dirs to avoid one huge directory
        return self.cache_dir / key_hash[:2] / f"{key_hash}.json"

    def get(self, key_hash: str) -> Optional[Dict]:
        p = self._path(key_hash)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Cache read failed for %s: %s", key_hash, exc)
            return None

    def put(self, key_hash: str, value: Dict) -> None:
        p = self._path(key_hash)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(value), encoding="utf-8")


# -----------------------------------------------------------------------
# Teacher Runner
# -----------------------------------------------------------------------

class TeacherRunner:
    def __init__(
        self,
        model: str = "gpt-4o",
        budget_usd: float = 100.0,
        cache_dir: str = ".cache/consumer_crowd/teacher_v1/",
        max_retries: int = 4,
        request_timeout_sec: float = 60.0,
        api_key: Optional[str] = None,
    ):
        self.model = model
        self.budget_usd = budget_usd
        self.cache = _DiskCache(Path(cache_dir))
        self.max_retries = max_retries
        self.request_timeout_sec = request_timeout_sec

        self._client = AsyncOpenAI(
            api_key=api_key or os.environ.get("OPENAI_API_KEY"),
            timeout=request_timeout_sec,
        )

        self.cost_so_far_usd: float = 0.0
        self.calls_so_far: int = 0
        self.cache_hits: int = 0

    # ----- public API -----

    async def run_cell(
        self,
        persona: Persona,
        scenario: Scenario,
        brand_context: Optional[str] = None,
    ) -> TeacherResponse:
        """Run one cell. Returns parsed + validated response. Caches on disk."""

        brand_hash = (
            hashlib.sha256(brand_context.encode()).hexdigest()[:16]
            if brand_context
            else ""
        )
        key = CellKey(
            persona_id=persona.id,
            scenario_id=scenario.id,
            magnitude=scenario.magnitude,
            brand_context_hash=brand_hash,
        )
        key_hash = key.to_hash()

        cached = self.cache.get(key_hash)
        if cached is not None:
            self.cache_hits += 1
            cached["from_cache"] = True
            return TeacherResponse(**cached)

        # Build prompts
        system_prompt = SYSTEM_PROMPT
        user_prompt = self._build_user_prompt(persona, scenario, brand_context)

        # Pre-check budget (estimated worst-case cost)
        est_cost = self._estimate_call_cost(user_prompt)
        if self.cost_so_far_usd + est_cost > self.budget_usd:
            raise BudgetExceededError(
                f"Cost ${self.cost_so_far_usd:.2f} + est ${est_cost:.4f} "
                f"would exceed budget ${self.budget_usd:.2f}"
            )

        # Call with retry
        raw_response, usage, call_id = await self._call_with_retry(system_prompt, user_prompt)

        # Validate ranges
        validated = _validate_response_ranges(raw_response)

        # Honor scenario.outcomes_applicable — null out non-applicable outcomes
        for outcome in ["purchase_intent", "wtp_delta", "sentiment", "wom_amplitude", "churn_probability"]:
            if outcome not in scenario.outcomes_applicable:
                validated[outcome] = None
                validated[f"{outcome}_confidence"] = None

        # Compute cost
        call_cost = self._compute_call_cost(usage)
        self.cost_so_far_usd += call_cost
        self.calls_so_far += 1

        response = TeacherResponse(
            purchase_intent=validated["purchase_intent"],
            wtp_delta=validated["wtp_delta"],
            sentiment=validated["sentiment"],
            wom_amplitude=validated["wom_amplitude"],
            churn_probability=validated["churn_probability"],
            purchase_intent_confidence=validated["purchase_intent_confidence"],
            wtp_delta_confidence=validated["wtp_delta_confidence"],
            sentiment_confidence=validated["sentiment_confidence"],
            wom_amplitude_confidence=validated["wom_amplitude_confidence"],
            churn_probability_confidence=validated["churn_probability_confidence"],
            rationale=validated["rationale"],
            teacher_model=self.model,
            teacher_call_id=call_id,
            generated_at=datetime.utcnow().isoformat() + "Z",
            cost_usd=call_cost,
            input_tokens=usage.get("prompt_tokens", 0),
            output_tokens=usage.get("completion_tokens", 0),
            from_cache=False,
        )

        # Persist to cache
        self.cache.put(key_hash, response.to_dict())

        return response

    # ----- internals -----

    def _build_user_prompt(
        self,
        persona: Persona,
        scenario: Scenario,
        brand_context: Optional[str],
    ) -> str:
        ctx = brand_context or "A generic consumer brand in the relevant category."
        return (
            f"Persona: {persona.archetype}. {persona.prompt_summary} "
            f"Demographics — age: {persona.age_bracket}, income: {persona.income_bracket}, "
            f"geography: {persona.geography}. Psychographics — price sensitivity: "
            f"{persona.price_sensitivity}, brand loyalty: {persona.brand_loyalty}, "
            f"novelty-seeking: {persona.novelty_seeking}.\n\n"
            f"Scenario: {scenario.description}\n\n"
            f"Brand context: {ctx}\n\n"
            f"Predict this persona's response across the outcomes specified in the schema. "
            f"For outcomes that don't apply in this scenario (e.g., churn for a brand you "
            f"don't currently buy), return null."
        )

    def _estimate_call_cost(self, user_prompt: str) -> float:
        # Very rough estimate; actual cost computed from API response.
        approx_input_tokens = (len(SYSTEM_PROMPT) + len(user_prompt)) / 4 + 200
        approx_output_tokens = 400
        return (
            approx_input_tokens * GPT4O_INPUT_USD_PER_1M / 1_000_000
            + approx_output_tokens * GPT4O_OUTPUT_USD_PER_1M / 1_000_000
        )

    def _compute_call_cost(self, usage: Dict) -> float:
        return (
            usage.get("prompt_tokens", 0) * GPT4O_INPUT_USD_PER_1M / 1_000_000
            + usage.get("completion_tokens", 0) * GPT4O_OUTPUT_USD_PER_1M / 1_000_000
        )

    async def _call_with_retry(self, system_prompt: str, user_prompt: str):
        """Call GPT-4o with exponential backoff on 429/5xx. Returns (raw_json, usage_dict, call_id)."""
        for attempt in range(self.max_retries + 1):
            try:
                resp = await self._client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    response_format={"type": "json_schema", "json_schema": RESPONSE_JSON_SCHEMA},
                    temperature=1.0,
                )
                content = resp.choices[0].message.content
                raw_json = json.loads(content)
                usage = {
                    "prompt_tokens": resp.usage.prompt_tokens,
                    "completion_tokens": resp.usage.completion_tokens,
                }
                return raw_json, usage, resp.id
            except (RateLimitError, APITimeoutError) as exc:
                wait = 2 ** attempt
                logger.warning("Retryable OpenAI error (attempt %d): %s — sleeping %ds", attempt + 1, exc, wait)
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(wait)
            except APIError as exc:
                # 5xx are retryable; 4xx generally not
                if 500 <= getattr(exc, "status_code", 0) < 600:
                    wait = 2 ** attempt
                    logger.warning("Server error (attempt %d): %s — sleeping %ds", attempt + 1, exc, wait)
                    if attempt == self.max_retries:
                        raise
                    await asyncio.sleep(wait)
                else:
                    raise
        raise RuntimeError("unreachable")
