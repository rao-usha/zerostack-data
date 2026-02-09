"""
Centralized LLM cost tracking service.

Records every LLM API call to the database with model, tokens, cost,
and source context. Provides in-memory session totals and DB persistence.
"""
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Pricing per 1M tokens — mirrors llm_client.py MODEL_PRICING
MODEL_PRICING = {
    # OpenAI
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4-turbo": {"input": 10.00, "output": 30.00},
    "gpt-4-turbo-preview": {"input": 10.00, "output": 30.00},
    "gpt-3.5-turbo": {"input": 0.50, "output": 1.50},
    # Anthropic
    "claude-3-5-sonnet-20241022": {"input": 3.00, "output": 15.00},
    "claude-3-5-haiku-20241022": {"input": 0.80, "output": 4.00},
    "claude-3-haiku-20240307": {"input": 0.25, "output": 1.25},
    "claude-3-sonnet-20240229": {"input": 3.00, "output": 15.00},
    "claude-3-opus-20240229": {"input": 15.00, "output": 75.00},
}

# Provider detection from model name
ANTHROPIC_MODELS = {"claude-3-5-sonnet", "claude-3-5-haiku", "claude-3-haiku", "claude-3-sonnet", "claude-3-opus"}


def _detect_provider(model: str) -> str:
    """Detect provider from model name."""
    model_lower = model.lower()
    if "claude" in model_lower:
        return "anthropic"
    return "openai"


def _calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate cost in USD for token usage."""
    pricing = MODEL_PRICING.get(model, {"input": 0.0, "output": 0.0})
    input_cost = (input_tokens / 1_000_000) * pricing["input"]
    output_cost = (output_tokens / 1_000_000) * pricing["output"]
    return input_cost + output_cost


class LLMCostTracker:
    """
    Singleton service that records every LLM call to the database.

    Thread-safe via simple append pattern — each record() call writes
    immediately to DB in its own session.
    """

    def __init__(self):
        self._total_input_tokens = 0
        self._total_output_tokens = 0
        self._total_cost_usd = 0.0
        self._total_calls = 0

    async def record(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
        source: str = "unknown",
        company_id: Optional[int] = None,
        job_id: Optional[int] = None,
        prompt_chars: int = 0,
        provider: Optional[str] = None,
        cost_usd: Optional[float] = None,
    ) -> None:
        """
        Record an LLM call. Writes to DB immediately.

        Args:
            model: Model name (e.g., "gpt-4o", "claude-3-5-sonnet-20241022")
            input_tokens: Number of input/prompt tokens
            output_tokens: Number of output/completion tokens
            source: Caller context (e.g., "people_collection", "org_chart")
            company_id: Optional company ID for context
            job_id: Optional job ID for context
            prompt_chars: Number of characters in the prompt
            provider: Optional provider override ("openai" or "anthropic")
            cost_usd: Optional pre-calculated cost; auto-calculated if None
        """
        if provider is None:
            provider = _detect_provider(model)

        if cost_usd is None:
            cost_usd = _calculate_cost(model, input_tokens, output_tokens)

        # Update in-memory totals
        self._total_input_tokens += input_tokens
        self._total_output_tokens += output_tokens
        self._total_cost_usd += cost_usd
        self._total_calls += 1

        logger.info(
            f"[LLMCostTracker] {source} | {model} | "
            f"in={input_tokens} out={output_tokens} | "
            f"${cost_usd:.6f} | session_total=${self._total_cost_usd:.4f}"
        )

        # Persist to DB
        try:
            from app.core.database import get_session_factory
            from app.core.models import LLMUsage

            SessionLocal = get_session_factory()
            db = SessionLocal()
            try:
                usage = LLMUsage(
                    created_at=datetime.utcnow(),
                    model=model,
                    provider=provider,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cost_usd=cost_usd,
                    source=source,
                    company_id=company_id,
                    job_id=job_id,
                    prompt_chars=prompt_chars,
                )
                db.add(usage)
                db.commit()
            finally:
                db.close()
        except Exception as e:
            logger.warning(f"[LLMCostTracker] Failed to persist LLM usage to DB: {e}")

    def get_session_totals(self) -> dict:
        """Return in-memory running totals for current process."""
        return {
            "total_calls": self._total_calls,
            "total_input_tokens": self._total_input_tokens,
            "total_output_tokens": self._total_output_tokens,
            "total_tokens": self._total_input_tokens + self._total_output_tokens,
            "total_cost_usd": round(self._total_cost_usd, 6),
        }


# Module-level singleton
_tracker: Optional[LLMCostTracker] = None


def get_cost_tracker() -> LLMCostTracker:
    """Get the global LLM cost tracker singleton."""
    global _tracker
    if _tracker is None:
        _tracker = LLMCostTracker()
    return _tracker
