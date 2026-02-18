"""
LLM Client - Unified async wrapper for OpenAI and Anthropic.

Provides:
- Async API calls with retry logic
- Structured JSON output parsing
- Token counting and cost tracking
- Support for both OpenAI and Anthropic
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Try to import LLM libraries
try:
    from openai import AsyncOpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AsyncOpenAI = None

try:
    from anthropic import AsyncAnthropic

    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    AsyncAnthropic = None


# Pricing per 1M tokens (as of 2024)
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


@dataclass
class LLMResponse:
    """Response from LLM call."""

    content: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    model: str
    cost_usd: float
    raw_response: Any = None

    def parse_json(self) -> Optional[Dict]:
        """Parse content as JSON, handling markdown code blocks."""
        text = self.content.strip()

        # Remove markdown code blocks if present
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first line (```json or ```)
            lines = lines[1:]
            # Remove last line (```)
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines)

        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse LLM response as JSON: {e}")
            return None


class LLMClient:
    """
    Unified async LLM client supporting OpenAI and Anthropic.

    Usage:
        client = LLMClient(provider="openai", api_key="sk-...")
        response = await client.complete("Extract companies from this text...")
        data = response.parse_json()
    """

    def __init__(
        self,
        provider: str = "openai",
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        max_tokens: int = 500,
        temperature: float = 0.1,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize LLM client.

        Args:
            provider: "openai" or "anthropic"
            api_key: API key (falls back to environment variable)
            model: Model name (uses sensible defaults)
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature (0-1)
            max_retries: Number of retries on failure
            retry_delay: Base delay between retries (exponential backoff)
        """
        self.provider = provider.lower()
        self.api_key = api_key
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Set default model based on provider
        if model:
            self.model = model
        elif self.provider == "openai":
            self.model = "gpt-4o-mini"
        else:
            self.model = "claude-3-5-haiku-20241022"

        self._client = None
        self._total_tokens_used = 0
        self._total_cost_usd = 0.0

    @property
    def is_available(self) -> bool:
        """Check if the selected provider is available."""
        if self.provider == "openai":
            return OPENAI_AVAILABLE and bool(self.api_key)
        elif self.provider == "anthropic":
            return ANTHROPIC_AVAILABLE and bool(self.api_key)
        return False

    def _get_client(self):
        """Get or create the API client."""
        if self._client is None:
            if self.provider == "openai" and OPENAI_AVAILABLE:
                self._client = AsyncOpenAI(api_key=self.api_key)
            elif self.provider == "anthropic" and ANTHROPIC_AVAILABLE:
                self._client = AsyncAnthropic(api_key=self.api_key)
        return self._client

    def _calculate_cost(
        self, model: str, input_tokens: int, output_tokens: int
    ) -> float:
        """Calculate cost in USD for token usage."""
        pricing = MODEL_PRICING.get(model, {"input": 0.0, "output": 0.0})
        input_cost = (input_tokens / 1_000_000) * pricing["input"]
        output_cost = (output_tokens / 1_000_000) * pricing["output"]
        return input_cost + output_cost

    async def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        json_mode: bool = False,
    ) -> LLMResponse:
        """
        Send a completion request to the LLM.

        Args:
            prompt: User prompt/message
            system_prompt: Optional system message
            json_mode: Request JSON output format (OpenAI only)

        Returns:
            LLMResponse with content and usage stats

        Raises:
            ValueError: If provider not available
            Exception: After all retries exhausted
        """
        if not self.is_available:
            raise ValueError(
                f"LLM provider '{self.provider}' not available. "
                f"Check that the package is installed and API key is set."
            )

        client = self._get_client()
        last_error = None

        for attempt in range(self.max_retries):
            try:
                if self.provider == "openai":
                    response = await self._openai_complete(
                        client, prompt, system_prompt, json_mode
                    )
                else:
                    response = await self._anthropic_complete(
                        client, prompt, system_prompt
                    )

                # Track totals
                self._total_tokens_used += response.total_tokens
                self._total_cost_usd += response.cost_usd

                # Persist to DB via cost tracker
                try:
                    from app.core.llm_cost_tracker import get_cost_tracker

                    tracker = get_cost_tracker()
                    await tracker.record(
                        model=response.model,
                        input_tokens=response.input_tokens,
                        output_tokens=response.output_tokens,
                        source="llm_client",
                        provider=self.provider,
                        cost_usd=response.cost_usd,
                        prompt_chars=len(prompt),
                    )
                except Exception as track_err:
                    logger.debug(f"[LLMClient] Cost tracking failed: {track_err}")

                return response

            except Exception as e:
                last_error = e
                logger.warning(
                    f"LLM request failed (attempt {attempt + 1}/{self.max_retries}): {e}"
                )

                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    await asyncio.sleep(delay)

        raise last_error or Exception("LLM request failed after all retries")

    async def _openai_complete(
        self,
        client: "AsyncOpenAI",
        prompt: str,
        system_prompt: Optional[str],
        json_mode: bool,
    ) -> LLMResponse:
        """Send request to OpenAI API."""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        kwargs = {
            "model": self.model,
            "messages": messages,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
        }

        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = await client.chat.completions.create(**kwargs)

        content = response.choices[0].message.content or ""
        input_tokens = response.usage.prompt_tokens if response.usage else 0
        output_tokens = response.usage.completion_tokens if response.usage else 0

        return LLMResponse(
            content=content,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=input_tokens + output_tokens,
            model=self.model,
            cost_usd=self._calculate_cost(self.model, input_tokens, output_tokens),
            raw_response=response,
        )

    async def _anthropic_complete(
        self,
        client: "AsyncAnthropic",
        prompt: str,
        system_prompt: Optional[str],
    ) -> LLMResponse:
        """Send request to Anthropic API."""
        kwargs = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }

        if system_prompt:
            kwargs["system"] = system_prompt

        response = await client.messages.create(**kwargs)

        content = response.content[0].text if response.content else ""
        input_tokens = response.usage.input_tokens if response.usage else 0
        output_tokens = response.usage.output_tokens if response.usage else 0

        return LLMResponse(
            content=content,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=input_tokens + output_tokens,
            model=self.model,
            cost_usd=self._calculate_cost(self.model, input_tokens, output_tokens),
            raw_response=response,
        )

    @property
    def total_tokens_used(self) -> int:
        """Total tokens used across all requests."""
        return self._total_tokens_used

    @property
    def total_cost_usd(self) -> float:
        """Total cost in USD across all requests."""
        return self._total_cost_usd

    def reset_stats(self):
        """Reset token and cost tracking."""
        self._total_tokens_used = 0
        self._total_cost_usd = 0.0


def get_llm_client(
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> Optional[LLMClient]:
    """
    Get an LLM client using settings from config.

    Args:
        provider: Override provider (openai/anthropic)
        model: Override model name

    Returns:
        LLMClient if API key available, None otherwise
    """
    from app.core.config import get_settings

    settings = get_settings()

    # Determine provider and API key
    if provider is None:
        # Try OpenAI first, then Anthropic
        if settings.get_openai_api_key():
            provider = "openai"
        elif settings.get_anthropic_api_key():
            provider = "anthropic"
        else:
            logger.warning(
                "No LLM API key configured (OPENAI_API_KEY or ANTHROPIC_API_KEY)"
            )
            return None

    if provider == "openai":
        api_key = settings.get_openai_api_key()
    else:
        api_key = settings.get_anthropic_api_key()

    if not api_key:
        logger.warning(f"No API key configured for {provider}")
        return None

    # Use configured model or default
    if model is None:
        model = settings.agentic_llm_model

    return LLMClient(
        provider=provider,
        api_key=api_key,
        model=model,
        max_tokens=settings.agentic_llm_max_tokens,
    )
