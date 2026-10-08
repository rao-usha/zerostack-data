"""Model provider registry and adapters for the ontology bake-off (SPEC_164).

The registry is data (``providers.json``): one entry per *route to a model*
(``<provider>/<model>``), listing everything the runner needs to call it and
to bound and price the call. It lists candidates neutrally; nothing here
chooses a roster. The owner selects names (PLAN_100 D1) through
``ONTOLOGY_ROSTER`` or an explicit list.

Two adapters:

* ``OpenAICompatAdapter``: one ``AsyncOpenAI(base_url=...)`` client covering
  OpenAI, the Gemini OpenAI-compatible endpoint, xAI, OpenRouter, DeepSeek,
  LM Studio and llama.cpp ``llama-server``.
* ``AnthropicAdapter``: the ``anthropic`` SDK, imported lazily and only built
  when an API key is present.

Both use explicit timeouts and ``max_retries=0`` in the SDK. Our own retry
loop retries **only** HTTP 429 and 5xx responses (no output was produced, so
nothing was billed), with exponential backoff and jitter, honouring
``Retry-After``. Timeouts and connection errors are never retried: a retried
timeout can bill the same long generation two or three times.

This module never imports the shared people/PE LLM client.

CLI (no network calls; lists configuration only)::

    python -m app.ontology.providers --check
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Sequence

import httpx

from app.ontology.jsonschema_lite import validate as validate_schema

logger = logging.getLogger(__name__)

REGISTRY_PATH = Path(__file__).with_name("providers.json")

ADAPTERS = ("openai_compat", "anthropic")
CLASSES = ("closed", "open_hosted", "open_local")
ROUTES = ("direct", "openrouter", "vertex", "local")
GENERATION_MODES = ("single", "split")
STRUCTURED_MODES = ("json_schema_strict", "json_schema", "json_object", "tool_schema", "prompt_only")
REASONING_BOUNDED = ("yes", "no", "unverified", "n/a")

DEFAULT_CONNECT_TIMEOUT_S = 10.0
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504, 529})
MAX_BACKOFF_S = 60.0


# ---------------------------------------------------------------- errors


class RegistryError(ValueError):
    """A registry entry is malformed."""


class RosterError(ValueError):
    """A requested model name is not in the registry."""


class ProviderError(RuntimeError):
    """Base class for adapter failures."""


class ProviderNotConfigured(ProviderError):
    """No API key (or SDK) for this provider; nothing was sent."""


class ProviderTimeout(ProviderError):
    """The request timed out. Never retried; billing is unknown."""


class ProviderConnectionError(ProviderError):
    """Transport failure. ``sent`` is False only when the connection was never made."""

    def __init__(self, message: str, sent: bool):
        super().__init__(message)
        self.sent = sent


class ProviderRequestError(ProviderError):
    """The provider answered with a non-retryable HTTP error."""

    def __init__(self, message: str, status: Optional[int]):
        super().__init__(message)
        self.status = status


class ProviderRetryExhausted(ProviderRequestError):
    """429/5xx persisted after the allowed retries."""


class _Retryable(Exception):
    def __init__(self, status: int, retry_after: Optional[float], message: str):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


# ---------------------------------------------------------------- registry


@dataclass(frozen=True)
class ProviderSpec:
    """One route to one model. Field meanings: see SPEC_164 "Registry fields"."""

    name: str
    adapter: str
    route: str
    model_id: str
    max_output_tokens: int
    klass: str
    base_url: Optional[str] = None
    docker_base_url: Optional[str] = None
    base_url_env: Optional[str] = None
    api_key_env: Optional[str] = None
    max_tokens_param: str = "max_tokens"
    structured_output: str = "json_object"
    reasoning_control: Dict[str, Any] = field(default_factory=dict)
    usage_reasoning_field: Optional[str] = None
    usage_cached_field: Optional[str] = None
    reasoning_in_output: bool = True
    generation_mode: str = "single"
    context_window: Optional[int] = None
    tokenizer: str = "fallback"
    timeout_s: float = 900.0
    terms_url: Optional[str] = None
    terms_checked_at: Optional[str] = None
    jurisdiction: Optional[str] = None
    extra_body: Dict[str, Any] = field(default_factory=dict)
    notes: Optional[str] = None

    @property
    def provider(self) -> str:
        """The first path segment of the registry name (openai, xai, lmstudio, ...)."""
        return self.name.split("/", 1)[0]

    @property
    def is_local(self) -> bool:
        return self.klass == "open_local"

    @classmethod
    def from_dict(cls, name: str, d: Mapping[str, Any]) -> "ProviderSpec":
        d = dict(d)
        d.pop("name", None)
        if "class" in d:
            d["klass"] = d.pop("class")
        known = set(cls.__dataclass_fields__)
        unknown = set(d) - known
        if unknown:
            raise RegistryError(f"{name}: unknown fields {sorted(unknown)}")
        for key in ("adapter", "route", "model_id", "max_output_tokens", "klass"):
            if d.get(key) in (None, ""):
                raise RegistryError(f"{name}: missing {key}")
        d["reasoning_control"] = dict(d.get("reasoning_control") or {})
        d["extra_body"] = dict(d.get("extra_body") or {})
        spec = cls(name=name, **d)
        checks = (
            ("adapter", spec.adapter, ADAPTERS),
            ("class", spec.klass, CLASSES),
            ("route", spec.route, ROUTES),
            ("generation_mode", spec.generation_mode, GENERATION_MODES),
            ("structured_output", spec.structured_output, STRUCTURED_MODES),
            ("reasoning_control.bounded", spec.reasoning_control.get("bounded", "n/a"), REASONING_BOUNDED),
        )
        for label, value, allowed in checks:
            if value not in allowed:
                raise RegistryError(f"{name}: {label}={value!r} not in {allowed}")
        if int(spec.max_output_tokens) <= 0:
            raise RegistryError(f"{name}: max_output_tokens must be positive")
        if not spec.is_local and not spec.api_key_env:
            raise RegistryError(f"{name}: hosted entries need api_key_env")
        return spec

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["class"] = d.pop("klass")
        d.pop("name")
        return d


def load_registry(path: Optional[os.PathLike] = None) -> Dict[str, ProviderSpec]:
    """Load the registry file (default ``providers.json`` next to this module)."""
    raw = json.loads(Path(path or REGISTRY_PATH).read_text(encoding="utf-8"))
    entries = raw.get("providers", raw)
    return {name: ProviderSpec.from_dict(name, d) for name, d in sorted(entries.items())}


def select_roster(
    names: Optional[Sequence[str]] = None,
    registry: Optional[Mapping[str, ProviderSpec]] = None,
    env: Optional[Mapping[str, str]] = None,
) -> List[ProviderSpec]:
    """Resolve the owner's roster. No names (and no ``ONTOLOGY_ROSTER``) → empty."""
    registry = registry if registry is not None else load_registry()
    env = os.environ if env is None else env
    if names is None:
        names = [n.strip() for n in (env.get("ONTOLOGY_ROSTER") or "").split(",") if n.strip()]
    missing = [n for n in names if n not in registry]
    if missing:
        raise RosterError(f"not in registry: {missing}")
    return [registry[n] for n in names]


def running_in_docker(env: Optional[Mapping[str, str]] = None) -> bool:
    env = os.environ if env is None else env
    return os.path.exists("/.dockerenv") or env.get("RUNNING_IN_DOCKER", "") in ("1", "true")


def resolve_base_url(
    spec: ProviderSpec, env: Optional[Mapping[str, str]] = None, in_docker: Optional[bool] = None
) -> Optional[str]:
    """Env override, then the Docker host alias when in a container, then base_url."""
    env = os.environ if env is None else env
    if spec.base_url_env and env.get(spec.base_url_env):
        return env[spec.base_url_env]
    if in_docker is None:
        in_docker = running_in_docker(env)
    if in_docker and spec.docker_base_url:
        return spec.docker_base_url
    return spec.base_url


def api_key_present(spec: ProviderSpec, env: Optional[Mapping[str, str]] = None) -> Optional[bool]:
    """True/False for hosted entries; None when the entry needs no key."""
    env = os.environ if env is None else env
    if not spec.api_key_env:
        return None
    return bool(env.get(spec.api_key_env))


# ---------------------------------------------------------------- results


@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    cached_input_tokens: int = 0
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CallResult:
    text: str
    parsed: Any
    valid: bool
    validation_errors: List[str]
    usage: Usage
    truncated: bool
    finish_reason: Optional[str]
    model_reported: Optional[str]
    request_id: Optional[str]
    system_fingerprint: Optional[str]
    latency_ms: int
    attempts: int
    # filled in by budget.BudgetGuard.guarded_call
    cost_usd: Optional[float] = None
    reserved_usd: Optional[float] = None
    cost_basis: Optional[str] = None


def _dig(d: Any, dotted: Optional[str]) -> Optional[int]:
    if not dotted or not isinstance(d, dict):
        return None
    node: Any = d
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return int(node) if isinstance(node, (int, float)) else None


def _parse_and_validate(text: str, schema: Optional[dict], truncated: bool):
    parsed: Any = None
    errors: List[str] = []
    try:
        parsed = json.loads(text) if text else None
        if parsed is None:
            errors.append("empty output")
    except (json.JSONDecodeError, TypeError) as e:
        errors.append(f"invalid JSON: {e}")
    if parsed is not None and schema is not None:
        errors.extend(validate_schema(parsed, schema))
    if truncated:
        errors.append("truncated: hit max output tokens")
    return parsed, (not errors), errors


def _retry_after_seconds(headers: Optional[Mapping[str, str]]) -> Optional[float]:
    if not headers:
        return None
    value = headers.get("retry-after")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


# ---------------------------------------------------------------- adapters


SleepFn = Callable[[float], Awaitable[None]]


class _BaseAdapter:
    def __init__(
        self,
        spec: ProviderSpec,
        api_key: Optional[str],
        base_url: Optional[str],
        http_client: Optional[httpx.AsyncClient] = None,
        sleep: Optional[SleepFn] = None,
        max_retries: int = 2,
        backoff_base_s: float = 2.0,
    ):
        self.spec = spec
        self.api_key = api_key
        self.base_url = base_url
        self.http_client = http_client
        self.max_retries = max(0, int(max_retries))
        self.backoff_base_s = backoff_base_s
        self._sleep = sleep or asyncio.sleep
        self.timeout = httpx.Timeout(float(spec.timeout_s), connect=DEFAULT_CONNECT_TIMEOUT_S)

    def _map_sdk_error(self, e: Exception, sdk: Any) -> Exception:
        """Translate an SDK exception to ours (or a _Retryable)."""
        if isinstance(e, sdk.APITimeoutError):
            return ProviderTimeout(f"{self.spec.name}: timed out after {self.spec.timeout_s}s")
        if isinstance(e, sdk.APIConnectionError):
            cause = e.__cause__
            never_sent = isinstance(cause, (httpx.ConnectError, httpx.ConnectTimeout))
            return ProviderConnectionError(f"{self.spec.name}: connection error: {cause or e}", sent=not never_sent)
        if isinstance(e, sdk.APIStatusError):
            status = e.status_code
            if status in RETRYABLE_STATUS:
                return _Retryable(status, _retry_after_seconds(e.response.headers), str(e))
            return ProviderRequestError(f"{self.spec.name}: HTTP {status}: {e}", status)
        return e

    async def _with_retries(self, fn: Callable[[], Awaitable[Any]]):
        attempt = 0
        while True:
            attempt += 1
            try:
                return await fn(), attempt
            except _Retryable as r:
                if attempt > self.max_retries:
                    raise ProviderRetryExhausted(
                        f"{self.spec.name}: HTTP {r.status} after {attempt} attempts", r.status
                    ) from r
                delay = self.backoff_base_s * (2 ** (attempt - 1)) + random.uniform(0, self.backoff_base_s)
                if r.retry_after is not None:
                    delay = max(delay, r.retry_after)
                delay = min(delay, MAX_BACKOFF_S)
                logger.warning(
                    "[ontology] %s: HTTP %s, retry %d/%d in %.1fs",
                    self.spec.name, r.status, attempt, self.max_retries, delay,
                )
                await self._sleep(delay)

    async def complete(self, messages: List[Dict[str, Any]], schema: Optional[dict] = None,
                       schema_name: str = "output", temperature: Optional[float] = None,
                       seed: Optional[int] = None) -> CallResult:  # pragma: no cover - abstract
        raise NotImplementedError


class OpenAICompatAdapter(_BaseAdapter):
    """Chat Completions over any OpenAI-compatible base URL."""

    def __init__(self, spec: ProviderSpec, api_key: Optional[str], base_url: Optional[str], **kw):
        super().__init__(spec, api_key, base_url, **kw)
        from openai import AsyncOpenAI

        self._client = AsyncOpenAI(
            api_key=api_key or "not-needed-local",
            base_url=base_url,
            timeout=self.timeout,
            max_retries=0,
            http_client=self.http_client,
        )

    def build_body(self, messages, schema=None, schema_name="output", temperature=None, seed=None) -> Dict[str, Any]:
        """Everything except model/messages; sent as the JSON body's other keys."""
        spec = self.spec
        body: Dict[str, Any] = {spec.max_tokens_param: int(spec.max_output_tokens)}
        mode = spec.structured_output
        if schema is not None and mode in ("json_schema_strict", "json_schema"):
            body["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": schema_name, "schema": schema, "strict": mode == "json_schema_strict"},
            }
        elif mode == "json_object" or (schema is not None and mode == "tool_schema"):
            body["response_format"] = {"type": "json_object"}
        rc = spec.reasoning_control
        if rc.get("param") and rc.get("value") is not None:
            body[rc["param"]] = rc["value"]
        if temperature is not None:
            body["temperature"] = temperature
        if seed is not None:
            body["seed"] = seed
        body.update(spec.extra_body)
        return body

    async def complete(self, messages, schema=None, schema_name="output", temperature=None, seed=None) -> CallResult:
        import openai

        body = self.build_body(messages, schema, schema_name, temperature, seed)

        async def once():
            try:
                return await self._client.chat.completions.with_raw_response.create(
                    model=self.spec.model_id, messages=messages, extra_body=body
                )
            except openai.OpenAIError as e:
                raise self._map_sdk_error(e, openai) from e

        t0 = time.monotonic()
        raw, attempts = await self._with_retries(once)
        latency_ms = int((time.monotonic() - t0) * 1000)
        completion = raw.parse()
        data = completion.model_dump()
        choice = (data.get("choices") or [{}])[0]
        text = ((choice.get("message") or {}).get("content")) or ""
        finish = choice.get("finish_reason")
        truncated = finish == "length"
        u = data.get("usage") or {}
        usage = Usage(
            input_tokens=int(u.get("prompt_tokens") or 0),
            output_tokens=int(u.get("completion_tokens") or 0),
            reasoning_tokens=_dig(u, self.spec.usage_reasoning_field) or 0,
            cached_input_tokens=_dig(u, self.spec.usage_cached_field) or 0,
            raw=u,
        )
        parsed, valid, errors = _parse_and_validate(text, schema, truncated)
        return CallResult(
            text=text, parsed=parsed, valid=valid, validation_errors=errors, usage=usage,
            truncated=truncated, finish_reason=finish, model_reported=data.get("model"),
            request_id=raw.headers.get("x-request-id") or data.get("id"),
            system_fingerprint=data.get("system_fingerprint"), latency_ms=latency_ms, attempts=attempts,
        )


class AnthropicAdapter(_BaseAdapter):
    """Anthropic Messages API. Built only when a key exists (guarded import)."""

    def __init__(self, spec: ProviderSpec, api_key: Optional[str], base_url: Optional[str], **kw):
        super().__init__(spec, api_key, base_url, **kw)
        try:
            from anthropic import AsyncAnthropic
        except ImportError as e:  # pragma: no cover - depends on environment
            raise ProviderNotConfigured(f"{spec.name}: the anthropic SDK is not installed") from e
        kwargs: Dict[str, Any] = dict(api_key=api_key, timeout=self.timeout, max_retries=0)
        if base_url:
            kwargs["base_url"] = base_url
        if self.http_client is not None:
            kwargs["http_client"] = self.http_client
        self._client = AsyncAnthropic(**kwargs)

    @staticmethod
    def _split_system(messages):
        system = "\n\n".join(m["content"] for m in messages if m.get("role") == "system")
        rest = [m for m in messages if m.get("role") != "system"]
        return system, rest

    def build_body(self, messages, schema=None, schema_name="output", temperature=None, seed=None) -> Dict[str, Any]:
        spec = self.spec
        system, _ = self._split_system(messages)
        body: Dict[str, Any] = {}
        if system:
            body["system"] = system
        if schema is not None and spec.structured_output == "tool_schema":
            body["tools"] = [{
                "name": schema_name,
                "description": "Return the output object that matches this schema.",
                "input_schema": schema,
            }]
            body["tool_choice"] = {"type": "tool", "name": schema_name}
        rc = spec.reasoning_control
        if rc.get("param") and rc.get("value") is not None:
            body[rc["param"]] = rc["value"]
        if temperature is not None:
            body["temperature"] = temperature
        body.update(spec.extra_body)
        return body

    async def complete(self, messages, schema=None, schema_name="output", temperature=None, seed=None) -> CallResult:
        import anthropic

        body = self.build_body(messages, schema, schema_name, temperature, seed)
        _, rest = self._split_system(messages)

        async def once():
            try:
                return await self._client.messages.with_raw_response.create(
                    model=self.spec.model_id, max_tokens=int(self.spec.max_output_tokens),
                    messages=rest, extra_body=body,
                )
            except anthropic.AnthropicError as e:
                raise self._map_sdk_error(e, anthropic) from e

        t0 = time.monotonic()
        raw, attempts = await self._with_retries(once)
        latency_ms = int((time.monotonic() - t0) * 1000)
        msg = raw.parse()
        data = msg.model_dump()
        text_parts, tool_input = [], None
        for block in data.get("content") or []:
            if block.get("type") == "text":
                text_parts.append(block.get("text") or "")
            elif block.get("type") == "tool_use" and tool_input is None:
                tool_input = block.get("input")
        text = json.dumps(tool_input) if tool_input is not None else "".join(text_parts)
        finish = data.get("stop_reason")
        truncated = finish == "max_tokens"
        u = data.get("usage") or {}
        usage = Usage(
            input_tokens=int(u.get("input_tokens") or 0)
            + int(u.get("cache_read_input_tokens") or 0) + int(u.get("cache_creation_input_tokens") or 0),
            output_tokens=int(u.get("output_tokens") or 0),
            reasoning_tokens=_dig(u, self.spec.usage_reasoning_field) or 0,
            cached_input_tokens=_dig(u, self.spec.usage_cached_field) or 0,
            raw=u,
        )
        parsed, valid, errors = _parse_and_validate(text, schema, truncated)
        return CallResult(
            text=text, parsed=parsed, valid=valid, validation_errors=errors, usage=usage,
            truncated=truncated, finish_reason=finish, model_reported=data.get("model"),
            request_id=raw.headers.get("request-id") or data.get("id"), system_fingerprint=None,
            latency_ms=latency_ms, attempts=attempts,
        )

    async def count_tokens(self, messages, schema=None, schema_name="output") -> int:
        """Provider count endpoint (free; makes a network call — never used in tests)."""
        body = self.build_body(messages, schema, schema_name)
        _, rest = self._split_system(messages)
        kwargs: Dict[str, Any] = {"model": self.spec.model_id, "messages": rest}
        for key in ("system", "tools", "tool_choice"):
            if key in body:
                kwargs[key] = body[key]
        res = await self._client.messages.count_tokens(**kwargs)
        return int(res.input_tokens)


def make_adapter(
    spec: ProviderSpec,
    api_key: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    in_docker: Optional[bool] = None,
    **kw: Any,
) -> _BaseAdapter:
    """Build the adapter for one registry entry. Raises ProviderNotConfigured without a key."""
    env = os.environ if env is None else env
    key = api_key or (env.get(spec.api_key_env) if spec.api_key_env else None)
    if not key and not spec.is_local:
        raise ProviderNotConfigured(f"{spec.name}: {spec.api_key_env} is not set")
    base_url = resolve_base_url(spec, env, in_docker)
    if spec.adapter == "anthropic":
        return AnthropicAdapter(spec, key, base_url, **kw)
    return OpenAICompatAdapter(spec, key, base_url, **kw)


# ---------------------------------------------------------------- CLI


def check_report(
    registry: Optional[Mapping[str, ProviderSpec]] = None,
    env: Optional[Mapping[str, str]] = None,
    pricing: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Configuration only: key presence and price status. Makes no network calls."""
    from app.ontology.pricing import load_pricing, price_status

    registry = registry if registry is not None else load_registry()
    env = os.environ if env is None else env
    pricing = pricing if pricing is not None else load_pricing()
    rows = []
    for name, spec in sorted(registry.items()):
        key = api_key_present(spec, env)
        rows.append({
            "name": name,
            "class": spec.klass,
            "adapter": spec.adapter,
            "route": spec.route,
            "model_id": spec.model_id,
            "base_url": resolve_base_url(spec, env),
            "key": "n/a" if key is None else ("yes" if key else "no"),
            "key_env": spec.api_key_env,
            "price": price_status(spec, pricing),
            "generation_mode": spec.generation_mode,
            "reasoning_bounded": spec.reasoning_control.get("bounded", "n/a"),
            "jurisdiction": spec.jurisdiction,
        })
    return rows


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.ontology.providers")
    parser.add_argument("--check", action="store_true", help="list configured providers (no network calls)")
    parser.add_argument("--json", action="store_true", help="print JSON instead of text")
    args = parser.parse_args(argv)
    if not args.check:
        parser.print_help()
        return 0
    rows = check_report()
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    print("# ontology provider registry (configuration only; no calls made; listing order is alphabetical)")
    for r in rows:
        print(
            f"{r['name']}  class={r['class']} route={r['route']} adapter={r['adapter']} "
            f"key={r['key']} price={r['price']} mode={r['generation_mode']} "
            f"reasoning_bounded={r['reasoning_bounded']} jurisdiction={r['jurisdiction']} "
            f"base_url={r['base_url']}"
        )
    roster = os.environ.get("ONTOLOGY_ROSTER", "")
    print(f"# ONTOLOGY_ROSTER={roster or '(not set; no roster selected)'}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
