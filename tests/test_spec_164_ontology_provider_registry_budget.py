"""
Tests for SPEC 164 — ontology provider registry, pricing gate, budget guard.

No test makes a network call: adapters run against httpx.MockTransport, and
the Postgres tests need TEST_PG_URL pointing at a DISPOSABLE database (the
budget DDL is applied there).
"""
import asyncio
import json
import logging
import os
import socket
import threading
import uuid
from datetime import date, timedelta
from pathlib import Path

import httpx
import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]
TODAY = date(2026, 10, 8)


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------- fixtures


def _spec(**over):
    from app.ontology.providers import ProviderSpec

    name = over.pop("name", "example/model-1")
    d = {
        "adapter": "openai_compat",
        "route": "direct",
        "base_url": "https://api.example.test/v1",
        "api_key_env": "EXAMPLE_API_KEY",
        "model_id": "example-model-1",
        "max_output_tokens": 64000,
        "max_tokens_param": "max_completion_tokens",
        "structured_output": "json_schema_strict",
        "reasoning_control": {"param": "reasoning_effort", "value": "low", "bounded": "yes"},
        "usage_reasoning_field": "completion_tokens_details.reasoning_tokens",
        "usage_cached_field": "prompt_tokens_details.cached_tokens",
        "reasoning_in_output": True,
        "generation_mode": "single",
        "context_window": 1000000,
        "tokenizer": "fallback",
        "timeout_s": 30,
        "terms_url": "https://example.test/terms",
        "terms_checked_at": None,
        "jurisdiction": "US",
        "class": "closed",
    }
    d.update(over)
    return ProviderSpec.from_dict(name, d)


def _price(**over):
    from app.ontology.pricing import PriceEntry

    d = {
        "input_per_m": 2.0,
        "output_per_m": 10.0,
        "cached_input_per_m": 0.5,
        "reasoning_per_m": None,
        "long_context": [],
        "promo_end": None,
        "as_of": TODAY.isoformat(),
        "source_url": "https://example.test/pricing",
        "verified": True,
    }
    d.update(over)
    return PriceEntry.from_dict("example/model-1", d)


SCHEMA = {
    "type": "object",
    "properties": {"a": {"type": "integer"}},
    "required": ["a"],
    "additionalProperties": False,
}


def _chat_response(content='{"a": 1}', finish="stop", usage=None):
    return {
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "created": 0,
        "model": "example-model-1-2026",
        "system_fingerprint": "fp_x",
        "choices": [
            {"index": 0, "message": {"role": "assistant", "content": content},
             "finish_reason": finish}
        ],
        "usage": usage
        or {
            "prompt_tokens": 1000,
            "completion_tokens": 300,
            "total_tokens": 1300,
            "completion_tokens_details": {"reasoning_tokens": 100},
            "prompt_tokens_details": {"cached_tokens": 200},
        },
    }


class Recorder:
    """Captures requests made through an httpx.MockTransport."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request):
        self.requests.append(request)
        r = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(r, Exception):
            raise r
        if callable(r):
            return r(request)
        return r


def _adapter(spec, responses, **kw):
    from app.ontology.providers import make_adapter

    rec = Recorder(responses)
    client = httpx.AsyncClient(transport=httpx.MockTransport(rec))
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    a = make_adapter(spec, api_key="sk-test", http_client=client, sleep=fake_sleep, **kw)
    return a, rec, sleeps


MSGS = [{"role": "system", "content": "brief"}, {"role": "user", "content": "go"}]


# ---------------------------------------------------------------- T1-T3


def test_package_minimal():
    import app.ontology as onto

    pkg = Path(onto.__file__).parent
    init = (pkg / "__init__.py").read_text(encoding="utf-8")
    # minimal: a docstring only, no imports, so parallel specs can add subpackages
    assert "import " not in init
    for mod in ("providers.py", "pricing.py", "budget.py", "tokens.py"):
        text = (pkg / mod).read_text(encoding="utf-8")
        assert "llm_client" not in text, mod


def test_registry_loads_and_fields():
    from app.ontology.providers import ADAPTERS, CLASSES, GENERATION_MODES, STRUCTURED_MODES, load_registry

    reg = load_registry()
    assert len(reg) >= 10
    classes = set()
    for name, s in reg.items():
        assert s.name == name
        assert s.adapter in ADAPTERS
        assert s.klass in CLASSES
        assert s.generation_mode in GENERATION_MODES
        assert s.structured_output in STRUCTURED_MODES
        assert s.max_output_tokens > 0
        assert s.model_id
        assert s.terms_url is not None or s.klass == "open_local"
        assert "jurisdiction" in s.__dict__ or hasattr(s, "jurisdiction")
        assert s.reasoning_control.get("bounded") in ("yes", "no", "unverified", "n/a")
        if s.klass != "open_local":
            assert s.api_key_env
        classes.add(s.klass)
        # neutral registry: nothing marks a model as chosen/recommended
        assert not getattr(s, "recommended", False)
    assert classes == {"closed", "open_hosted", "open_local"}
    raw = json.loads((REPO / "app/ontology/providers.json").read_text(encoding="utf-8"))
    text = json.dumps(raw).lower()
    assert "recommend" not in text
    # local entries cover LM Studio (localhost + docker) and llama.cpp
    local = [s for s in reg.values() if s.klass == "open_local"]
    assert any("1234" in (s.base_url or "") and s.docker_base_url for s in local)
    assert any("llama" in s.name for s in local)


def test_roster_selection(monkeypatch):
    from app.ontology.providers import RosterError, load_registry, select_roster

    reg = load_registry()
    monkeypatch.delenv("ONTOLOGY_ROSTER", raising=False)
    assert select_roster(registry=reg) == []  # no roster is decided by default
    names = sorted(reg)[:2]
    monkeypatch.setenv("ONTOLOGY_ROSTER", ",".join(names))
    assert [s.name for s in select_roster(registry=reg)] == names
    with pytest.raises(RosterError):
        select_roster(["nope/not-a-model"], registry=reg)


# ---------------------------------------------------------------- adapters


def test_openai_compat_request_shape_strict():
    spec = _spec()
    a, rec, _ = _adapter(spec, [httpx.Response(200, json=_chat_response())])
    res = run(a.complete(MSGS, schema=SCHEMA, schema_name="onto", temperature=0.7, seed=7))
    assert len(rec.requests) == 1
    req = rec.requests[0]
    assert str(req.url) == "https://api.example.test/v1/chat/completions"
    assert req.headers["authorization"] == "Bearer sk-test"
    body = json.loads(req.content)
    assert body["model"] == "example-model-1"
    assert body["max_completion_tokens"] == 64000
    assert "max_tokens" not in body
    assert body["response_format"]["type"] == "json_schema"
    assert body["response_format"]["json_schema"]["strict"] is True
    assert body["response_format"]["json_schema"]["schema"] == SCHEMA
    assert body["response_format"]["json_schema"]["name"] == "onto"
    assert body["reasoning_effort"] == "low"
    assert body["temperature"] == 0.7 and body["seed"] == 7
    assert res.parsed == {"a": 1} and res.valid and not res.truncated
    assert res.model_reported == "example-model-1-2026"
    assert res.system_fingerprint == "fp_x"


def test_openai_compat_request_shape_json_object_and_extra_body():
    spec = _spec(
        structured_output="json_object",
        max_tokens_param="max_tokens",
        reasoning_control={"param": None, "bounded": "unverified"},
        extra_body={"provider": {"require_parameters": True}},
    )
    a, rec, _ = _adapter(spec, [httpx.Response(200, json=_chat_response('{"a": "x"}'))])
    res = run(a.complete(MSGS, schema=SCHEMA))
    body = json.loads(rec.requests[0].content)
    assert body["response_format"] == {"type": "json_object"}
    assert body["max_tokens"] == 64000
    assert body["provider"] == {"require_parameters": True}
    assert "reasoning_effort" not in body
    # local validation still runs and flags the type error
    assert res.parsed == {"a": "x"}
    assert res.valid is False and res.validation_errors


def test_prompt_only_invalid_json_flagged():
    spec = _spec(structured_output="prompt_only")
    a, rec, _ = _adapter(spec, [httpx.Response(200, json=_chat_response("not json"))])
    res = run(a.complete(MSGS, schema=SCHEMA))
    body = json.loads(rec.requests[0].content)
    assert "response_format" not in body
    assert res.parsed is None and res.valid is False


def test_no_retry_on_timeout():
    from app.ontology.providers import ProviderTimeout

    spec = _spec()
    a, rec, sleeps = _adapter(spec, [httpx.ReadTimeout("slow")])
    with pytest.raises(ProviderTimeout):
        run(a.complete(MSGS, schema=SCHEMA))
    assert len(rec.requests) == 1
    assert sleeps == []


def test_no_retry_on_connection_error():
    from app.ontology.providers import ProviderConnectionError

    a, rec, sleeps = _adapter(_spec(), [httpx.ConnectError("boom")])
    with pytest.raises(ProviderConnectionError):
        run(a.complete(MSGS))
    assert len(rec.requests) == 1


def test_timeout_is_explicit():
    a, _, _ = _adapter(_spec(timeout_s=42), [httpx.Response(200, json=_chat_response())])
    t = a.timeout
    assert t.read == 42 and t.connect is not None


def test_retry_on_429_and_5xx_then_success():
    spec = _spec()
    a, rec, sleeps = _adapter(
        spec,
        [
            httpx.Response(429, headers={"retry-after": "3"}, json={"error": {"message": "slow"}}),
            httpx.Response(503, json={"error": {"message": "down"}}),
            httpx.Response(200, json=_chat_response()),
        ],
        max_retries=2,
    )
    res = run(a.complete(MSGS, schema=SCHEMA))
    assert res.valid
    assert len(rec.requests) == 3
    assert len(sleeps) == 2 and sleeps[0] >= 3


def test_retry_exhausted():
    from app.ontology.providers import ProviderRetryExhausted

    a, rec, sleeps = _adapter(_spec(), [httpx.Response(500, json={"error": {}})], max_retries=2)
    with pytest.raises(ProviderRetryExhausted):
        run(a.complete(MSGS))
    assert len(rec.requests) == 3


def test_no_retry_on_400():
    from app.ontology.providers import ProviderRequestError

    a, rec, sleeps = _adapter(_spec(), [httpx.Response(400, json={"error": {"message": "bad schema"}})])
    with pytest.raises(ProviderRequestError) as ei:
        run(a.complete(MSGS, schema=SCHEMA))
    assert ei.value.status == 400
    assert len(rec.requests) == 1 and sleeps == []


def test_truncation_flag():
    a, _, _ = _adapter(_spec(), [httpx.Response(200, json=_chat_response('{"a": 1', finish="length"))])
    res = run(a.complete(MSGS, schema=SCHEMA))
    assert res.truncated is True
    assert res.valid is False


def test_usage_reasoning_and_cached_fields():
    a, _, _ = _adapter(_spec(), [httpx.Response(200, json=_chat_response())])
    res = run(a.complete(MSGS))
    u = res.usage
    assert (u.input_tokens, u.output_tokens, u.reasoning_tokens, u.cached_input_tokens) == (1000, 300, 100, 200)


def test_anthropic_requires_key(monkeypatch):
    from app.ontology.providers import ProviderNotConfigured, make_adapter

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    spec = _spec(adapter="anthropic", api_key_env="ANTHROPIC_API_KEY", base_url=None,
                 structured_output="prompt_only", max_tokens_param="max_tokens")
    with pytest.raises(ProviderNotConfigured):
        make_adapter(spec, env={})


def test_anthropic_request_shape():
    pytest.importorskip("anthropic")
    spec = _spec(
        adapter="anthropic", api_key_env="ANTHROPIC_API_KEY", base_url=None,
        structured_output="tool_schema", max_tokens_param="max_tokens",
        reasoning_control={"param": None, "bounded": "yes"},
        usage_reasoning_field=None, usage_cached_field="cache_read_input_tokens",
    )
    resp = {
        "id": "msg_1", "type": "message", "role": "assistant", "model": "example-model-1",
        "content": [{"type": "tool_use", "id": "t1", "name": "onto", "input": {"a": 2}}],
        "stop_reason": "tool_use", "stop_sequence": None,
        "usage": {"input_tokens": 50, "output_tokens": 20, "cache_read_input_tokens": 10},
    }
    a, rec, _ = _adapter(spec, [httpx.Response(200, json=resp)])
    res = run(a.complete(MSGS, schema=SCHEMA, schema_name="onto"))
    body = json.loads(rec.requests[0].content)
    assert rec.requests[0].url.path.endswith("/v1/messages")
    assert body["max_tokens"] == 64000
    assert body["system"] == "brief"
    assert body["messages"] == [{"role": "user", "content": "go"}]
    assert body["tool_choice"] == {"type": "tool", "name": "onto"}
    assert body["tools"][0]["input_schema"] == SCHEMA
    assert res.parsed == {"a": 2} and res.valid
    # normalised: input = uncached + cache reads, like OpenAI prompt_tokens
    assert res.usage.input_tokens == 60 and res.usage.cached_input_tokens == 10


def test_local_base_url_resolution():
    from app.ontology.providers import resolve_base_url

    spec = _spec(base_url="http://localhost:1234/v1", docker_base_url="http://host.docker.internal:1234/v1",
                 base_url_env="LMSTUDIO_BASE_URL", api_key_env=None, **{"class": "open_local"})
    assert resolve_base_url(spec, env={}, in_docker=False) == "http://localhost:1234/v1"
    assert resolve_base_url(spec, env={}, in_docker=True) == "http://host.docker.internal:1234/v1"
    assert resolve_base_url(spec, env={"LMSTUDIO_BASE_URL": "http://h:9/v1"}, in_docker=True) == "http://h:9/v1"


def test_local_adapter_needs_no_key():
    from app.ontology.providers import make_adapter

    spec = _spec(base_url="http://localhost:1234/v1", api_key_env=None, **{"class": "open_local"})
    a = make_adapter(spec, env={}, in_docker=False)
    assert a.base_url == "http://localhost:1234/v1"


# ---------------------------------------------------------------- pricing


def test_pricing_seed_all_unverified_and_covers_hosted():
    from app.ontology.pricing import load_pricing
    from app.ontology.providers import load_registry

    pricing = load_pricing()
    reg = load_registry()
    for name, e in pricing.items():
        assert e.verified is False, name
        assert e.as_of and e.source_url, name
    for name, s in reg.items():
        if s.klass != "open_local":
            assert name in pricing, f"hosted {name} has no price entry"


def test_price_gate_unverified_blocks():
    from app.ontology.pricing import PriceGateError, check_price_gate

    with pytest.raises(PriceGateError, match="unverified"):
        check_price_gate(_spec(), {"example/model-1": _price(verified=False)}, today=TODAY)


def test_price_gate_stale_blocks():
    from app.ontology.pricing import PriceGateError, check_price_gate

    old = (TODAY - timedelta(days=5)).isoformat()
    with pytest.raises(PriceGateError, match="older"):
        check_price_gate(_spec(), {"example/model-1": _price(as_of=old)}, today=TODAY, max_age_days=1)
    # a wider window lets it through
    check_price_gate(_spec(), {"example/model-1": _price(as_of=old)}, today=TODAY, max_age_days=7)


def test_price_gate_missing_promo_premium():
    from app.ontology.pricing import PriceGateError, check_price_gate

    with pytest.raises(PriceGateError, match="no price"):
        check_price_gate(_spec(), {}, today=TODAY)
    with pytest.raises(PriceGateError, match="promo"):
        check_price_gate(_spec(), {"example/model-1": _price(promo_end="2026-10-01")}, today=TODAY)
    prem = {"example/model-1": _price(output_per_m=50.0)}
    with pytest.raises(PriceGateError, match="premium"):
        check_price_gate(_spec(), prem, today=TODAY)
    assert check_price_gate(_spec(), prem, today=TODAY, allow_premium=True).premium


def test_price_gate_local_exempt():
    from app.ontology.pricing import check_price_gate

    spec = _spec(api_key_env=None, base_url="http://localhost:1234/v1", **{"class": "open_local"})
    assert check_price_gate(spec, {}, today=TODAY) is None


def test_worst_case_math_base_and_reasoning_price():
    from app.ontology.pricing import worst_case_cost

    e = _price()
    # 35k in at $2/M + 128k out at $10/M
    assert worst_case_cost(e, 35_000, 128_000) == pytest.approx(0.07 + 1.28)
    e2 = _price(reasoning_per_m=12.0)
    assert worst_case_cost(e2, 35_000, 128_000) == pytest.approx(0.07 + 128_000 * 12 / 1e6)


def test_worst_case_math_long_context_tier():
    from app.ontology.pricing import worst_case_cost

    e = _price(input_per_m=4.0, output_per_m=20.0,
               long_context=[{"above_input_tokens": 272_000, "input_per_m": 8.0, "output_per_m": 30.0}])
    assert worst_case_cost(e, 100_000, 64_000) == pytest.approx(0.4 + 1.28)
    assert worst_case_cost(e, 300_000, 64_000) == pytest.approx(2.4 + 1.92)


def test_worst_case_math_unpriced_tier_refuses():
    from app.ontology.pricing import UnpricedTier, worst_case_cost

    e = _price(long_context=[{"above_input_tokens": 200_000, "input_per_m": None, "output_per_m": None}])
    worst_case_cost(e, 150_000, 64_000)
    with pytest.raises(UnpricedTier):
        worst_case_cost(e, 250_000, 64_000)


def test_worst_case_math_band():
    from app.ontology.pricing import worst_case_cost

    e = _price()
    assert worst_case_cost(e, 100_000, 0, band=True) == pytest.approx(0.26)
    # the band can push a prompt over a tier threshold
    e2 = _price(long_context=[{"above_input_tokens": 120_000, "input_per_m": 4.0, "output_per_m": 20.0}])
    assert worst_case_cost(e2, 100_000, 0, band=True) == pytest.approx(130_000 * 4 / 1e6)


def test_actual_cost_math():
    from app.ontology.pricing import actual_cost
    from app.ontology.providers import Usage

    e = _price(reasoning_per_m=12.0)
    u = Usage(input_tokens=1000, output_tokens=300, reasoning_tokens=100, cached_input_tokens=200)
    exp = (800 * 2.0 + 200 * 0.5 + 200 * 10.0 + 100 * 12.0) / 1e6
    assert actual_cost(e, u, reasoning_in_output=True) == pytest.approx(exp)
    exp2 = (800 * 2.0 + 200 * 0.5 + 300 * 10.0 + 100 * 12.0) / 1e6
    assert actual_cost(e, u, reasoning_in_output=False) == pytest.approx(exp2)


# ---------------------------------------------------------------- budget (memory)


def _guard(store=None, spec=None, price=None, caps=None, recorder=None):
    from app.ontology.budget import BudgetConfig, BudgetGuard, MemoryBudgetStore

    spec = spec or _spec()
    cfg = BudgetConfig(
        pilot_cap_usd=20, round_cap_usd=100, month_cap_usd=(caps or {}).get("month", 150),
        call_cap_usd=(caps or {}).get("call", 10), price_max_age_days=1, premium_output_per_m=40.0,
    )
    store = store or MemoryBudgetStore()
    pricing = {spec.name: price or _price()} if price is not False else {}
    calls = []

    async def rec(**kw):
        calls.append(kw)

    g = BudgetGuard(store, {spec.name: spec}, pricing, cfg, recorder=recorder or rec,
                    today_fn=lambda: TODAY)
    return g, store, calls


def test_budget_defaults_from_d2(monkeypatch):
    from app.ontology.budget import BudgetConfig

    for k in ("ONTOLOGY_PILOT_BUDGET_USD", "ONTOLOGY_ROUND_BUDGET_USD", "ONTOLOGY_MONTHLY_BUDGET_USD"):
        monkeypatch.delenv(k, raising=False)
    c = BudgetConfig.from_env()
    assert (c.pilot_cap_usd, c.round_cap_usd, c.month_cap_usd) == (20.0, 100.0, 150.0)
    monkeypatch.setenv("ONTOLOGY_MONTHLY_BUDGET_USD", "90")
    assert BudgetConfig.from_env().month_cap_usd == 90.0


def test_budget_refuses_before_send_round_cap():
    from app.ontology.budget import BudgetExceeded

    g, store, _ = _guard()
    rid = store.create_budget("pilot", 0.5)
    a, rec, _ = _adapter(_spec(), [httpx.Response(200, json=_chat_response())])
    with pytest.raises(BudgetExceeded) as ei:  # worst case 0.71 > 0.5
        run(g.guarded_call(a, rid, MSGS, schema=SCHEMA, in_tok=35_000))
    assert ei.value.scope == "pilot"
    assert rec.requests == []
    assert store.get(rid).reserved_usd == 0


def test_budget_refuses_before_send_call_cap():
    from app.ontology.budget import BudgetExceeded

    g, store, _ = _guard(caps={"call": 0.5})
    rid = store.create_budget("round", 100)
    a, rec, _ = _adapter(_spec(), [httpx.Response(200, json=_chat_response())])
    with pytest.raises(BudgetExceeded) as ei:
        run(g.guarded_call(a, rid, MSGS, in_tok=35_000))
    assert ei.value.scope == "call" and rec.requests == []


def test_budget_refuses_before_send_month_cap():
    from app.ontology.budget import BudgetExceeded

    g, store, _ = _guard(caps={"month": 0.3})
    rid = store.create_budget("round", 100)
    a, rec, _ = _adapter(_spec(), [httpx.Response(200, json=_chat_response())])
    with pytest.raises(BudgetExceeded) as ei:
        run(g.guarded_call(a, rid, MSGS, in_tok=35_000))
    assert ei.value.scope == "month" and rec.requests == []
    assert store.get(rid).reserved_usd == 0  # nothing half-reserved


def test_unverified_price_blocks_run():
    from app.ontology.pricing import PriceGateError

    g, store, _ = _guard(price=_price(verified=False))
    rid = store.create_budget("round", 100)
    a, rec, _ = _adapter(_spec(), [httpx.Response(200, json=_chat_response())])
    with pytest.raises(PriceGateError):
        run(g.guarded_call(a, rid, MSGS, in_tok=1000))
    assert rec.requests == []


def test_unbounded_reasoning_refused():
    from app.ontology.budget import ReasoningUnbounded

    spec = _spec(reasoning_control={"param": "reasoning_effort", "value": "high", "bounded": "no"})
    g, store, _ = _guard(spec=spec)
    rid = store.create_budget("round", 100)
    with pytest.raises(ReasoningUnbounded):
        g.reserve(spec.name, rid, in_tok=1000)
    spec2 = _spec(reasoning_control={"param": "reasoning_effort", "bounded": "no",
                                     "reservation_ceiling_tokens": 200_000})
    g2, store2, _ = _guard(spec=spec2)
    rid2 = store2.create_budget("round", 100)
    r = g2.reserve(spec2.name, rid2, in_tok=0)
    assert r.amount_usd == pytest.approx(200_000 * 10 / 1e6)


def test_budget_settle_and_release():
    g, store, calls = _guard()
    rid = store.create_budget("round", 100)
    a, rec, _ = _adapter(_spec(), [httpx.Response(200, json=_chat_response())])
    res = run(g.guarded_call(a, rid, MSGS, schema=SCHEMA, in_tok=35_000, label="pilot"))
    b = store.get(rid)
    assert b.reserved_usd == pytest.approx(0)
    assert b.spent_usd == pytest.approx(res.cost_usd)
    assert res.cost_basis == "priced"
    assert res.reserved_usd == pytest.approx(0.71)
    assert calls and calls[0]["cost_usd"] == pytest.approx(res.cost_usd)
    assert calls[0]["source"] == "ontology:pilot"
    assert calls[0]["provider"] == "example"
    # failure (400) → released, nothing spent
    a2, _, _ = _adapter(_spec(), [httpx.Response(400, json={"error": {}})])
    from app.ontology.providers import ProviderRequestError

    with pytest.raises(ProviderRequestError):
        run(g.guarded_call(a2, rid, MSGS, in_tok=35_000))
    b = store.get(rid)
    assert b.reserved_usd == pytest.approx(0) and b.spent_usd == pytest.approx(res.cost_usd)


def test_guarded_call_timeout_settles_worst_case():
    from app.ontology.providers import ProviderTimeout

    g, store, calls = _guard()
    rid = store.create_budget("round", 100)
    a, rec, _ = _adapter(_spec(), [httpx.ReadTimeout("slow")])
    with pytest.raises(ProviderTimeout):
        run(g.guarded_call(a, rid, MSGS, in_tok=35_000))
    assert len(rec.requests) == 1
    b = store.get(rid)
    assert b.reserved_usd == pytest.approx(0)
    assert b.spent_usd == pytest.approx(0.71)
    assert store.reservations()[-1]["cost_basis"] == "timeout_worst_case"


def test_guarded_call_connect_error_releases():
    from app.ontology.providers import ProviderConnectionError

    g, store, calls = _guard()
    rid = store.create_budget("round", 100)
    a, rec, _ = _adapter(_spec(), [httpx.ConnectError("refused")])
    with pytest.raises(ProviderConnectionError) as ei:
        run(g.guarded_call(a, rid, MSGS, in_tok=35_000))
    assert ei.value.sent is False
    b = store.get(rid)
    assert b.reserved_usd == pytest.approx(0) and b.spent_usd == pytest.approx(0)
    assert calls == []


def test_local_model_zero_cost_logged():
    spec = _spec(name="lmstudio/x", api_key_env=None, base_url="http://localhost:1234/v1",
                 **{"class": "open_local"})
    g, store, calls = _guard(spec=spec, price=False)
    rid = store.create_budget("round", 0.0)  # even a $0 budget admits local calls
    a, rec, _ = _adapter(spec, [httpx.Response(200, json=_chat_response())])
    res = run(g.guarded_call(a, rid, MSGS, in_tok=5000))
    assert res.cost_usd == 0 and res.cost_basis == "local"
    assert len(calls) == 1 and calls[0]["cost_usd"] == 0
    assert calls[0]["provider"] == "lmstudio"


def test_fallback_count_uses_band(monkeypatch):
    g, store, _ = _guard()
    rid = store.create_budget("round", 100)
    r = g.reserve("example/model-1", rid, messages=[{"role": "user", "content": "x" * 40_000}])
    # 40,000 chars / 4 = 10k tokens, banded to 13k
    assert r.in_tok_estimated is True
    assert r.amount_usd == pytest.approx(13_000 * 2 / 1e6 + 64_000 * 10 / 1e6)


def test_preflight_confirm_worst_case():
    from app.ontology.budget import PlannedCall, WorstCaseNotConfirmed

    g, store, _ = _guard()
    rid = store.create_budget("pilot", 2.0)
    plan = [PlannedCall("example/model-1", in_tok=35_000, expected_out_tok=40_000) for _ in range(3)]
    with pytest.raises(WorstCaseNotConfirmed) as ei:
        g.preflight(plan, rid)
    est = ei.value.estimate
    assert est["worst_case_usd"] == pytest.approx(3 * (0.07 + 0.64))
    assert est["expected_usd"] == pytest.approx(3 * (0.07 + 0.40))
    ok = g.preflight(plan, rid, confirm_worst_case=True)
    assert ok["warnings"] and ok["remaining_usd"] == pytest.approx(2.0)
    assert store.get(rid).reserved_usd == 0  # preflight reserves nothing


def test_preflight_reports_refusals():
    from app.ontology.budget import PlannedCall

    g, store, _ = _guard(price=_price(verified=False))
    rid = store.create_budget("round", 100)
    est = g.preflight([PlannedCall("example/model-1", in_tok=1000)], rid)
    assert est["refused"] and "unverified" in est["refused"][0]["reason"]


# ---------------------------------------------------------------- tokens


def test_token_counter_fallback_band():
    from app.ontology.tokens import count_tokens

    tc = count_tokens("abcd" * 1000, _spec(tokenizer="fallback"))
    assert tc.tokens == 1000 and tc.exact is False and tc.band == 0.3
    tc2 = count_tokens("abcd" * 1000, _spec(tokenizer="tiktoken:o200k_base"))
    assert tc2.tokens > 0
    if tc2.method == "fallback":
        assert tc2.band == 0.3
    else:
        assert tc2.exact and tc2.band == 0.0


# ---------------------------------------------------------------- CLI


def test_check_cli_no_network(monkeypatch, capsys):
    from app.ontology import providers

    def no_net(*a, **k):
        raise AssertionError("network call attempted")

    monkeypatch.setattr(socket.socket, "connect", no_net)
    monkeypatch.setattr(socket, "create_connection", no_net)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-secret-value")
    monkeypatch.delenv("XAI_API_KEY", raising=False)
    rc = providers.main(["--check"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "sk-secret-value" not in out
    lines = [ln for ln in out.splitlines() if ln.startswith("openai/")]
    assert lines and all("key=yes" in ln for ln in lines)
    assert any(ln.startswith("xai/") and "key=no" in ln for ln in out.splitlines())
    assert "price=unverified" in out


# ---------------------------------------------------------------- D12


def test_cost_tracker_warns_once_for_unknown_model(caplog):
    from app.core import llm_cost_tracker as t

    t._WARNED_UNKNOWN_MODELS.clear()
    with caplog.at_level(logging.WARNING, logger="app.core.llm_cost_tracker"):
        assert t._calculate_cost("mystery-model-9", 1000, 1000) == 0.0
        t._calculate_cost("mystery-model-9", 10, 10)
        t._calculate_cost("gpt-4o", 1000, 1000)
    warns = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warns) == 1 and "mystery-model-9" in warns[0].getMessage()
    assert t._calculate_cost("gpt-4o", 1_000_000, 0) == pytest.approx(2.50)


# ---------------------------------------------------------------- Postgres


@pytest.fixture
def pg_store():
    from sqlalchemy import create_engine

    from app.ontology.budget import BUDGET_DDL, PostgresBudgetStore

    eng = create_engine(PG_URL, pool_size=5)
    with eng.begin() as c:
        c.exec_driver_sql(BUDGET_DDL)
    yield PostgresBudgetStore(eng)
    eng.dispose()


@pg
def test_pg_ddl_idempotent(pg_store):
    from app.ontology.budget import BUDGET_DDL

    with pg_store.engine.begin() as c:
        c.exec_driver_sql(BUDGET_DDL)


@pg
def test_pg_concurrent_reservations(pg_store):
    from app.ontology.budget import BudgetExceeded

    rid = pg_store.create_budget("round", 10.0, name=f"t-{uuid.uuid4()}")
    mid = pg_store.get_or_create_budget("month", f"test-{uuid.uuid4().hex[:8]}", 150.0)
    barrier = threading.Barrier(2)
    results = []

    def hold(_):
        import time

        time.sleep(0.3)  # widen the race window while the row locks are held

    def worker():
        barrier.wait()
        try:
            pg_store.reserve([rid, mid], 6.0, {"model": "m"}, _after_lock=hold)
            results.append("ok")
        except BudgetExceeded as e:
            results.append(e.scope)

    ts = [threading.Thread(target=worker) for _ in range(2)]
    [t.start() for t in ts]
    [t.join(20) for t in ts]
    assert sorted(results) == ["ok", "round"]
    assert pg_store.get(rid).reserved_usd == pytest.approx(6.0)
    assert pg_store.get(mid).reserved_usd == pytest.approx(6.0)


@pg
def test_pg_settle_release(pg_store):
    from app.ontology.budget import BudgetExceeded

    rid = pg_store.create_budget("pilot", 20.0)
    mid = pg_store.get_or_create_budget("month", f"test-{uuid.uuid4().hex[:8]}", 150.0)
    assert pg_store.get_or_create_budget("month", pg_store.get(mid).period_key, 999) == mid
    r1 = pg_store.reserve([rid, mid], 5.0, {"model": "m"})
    r2 = pg_store.reserve([rid, mid], 4.0, {"model": "m"})
    pg_store.settle(r1, 1.25, "priced")
    pg_store.release(r2)
    b, m = pg_store.get(rid), pg_store.get(mid)
    assert b.reserved_usd == pytest.approx(0) and b.spent_usd == pytest.approx(1.25)
    assert m.spent_usd == pytest.approx(1.25)
    pg_store.settle(r1, 9.0, "priced")  # settling twice is a no-op
    assert pg_store.get(rid).spent_usd == pytest.approx(1.25)
    with pytest.raises(BudgetExceeded):
        pg_store.reserve([rid, mid], 18.8, {"model": "m"})
    assert pg_store.get(mid).reserved_usd == pytest.approx(0)
