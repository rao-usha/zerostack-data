# SPEC 164 — Ontology model provider registry, pricing gate, budget guard

**Status:** Implemented (branch `spec-164`)
**Task type:** service
**Date:** 2026-10-08
**Plan:** `docs/plans/PLAN_100_industry_ontologies.md` §4, §8.1, §8.4 (row SPEC_164), §9, §9.3, §12 D2/D12
**Test file:** tests/test_spec_164_ontology_provider_registry_budget.py
**Runs in parallel with:** SPEC_162 (`app/ontology/gold/`, Alembic 0022 for CMS), SPEC_163 (`app/ontology/standards/`)
**Hands off to:** SPEC_165 (runner, store, endpoints, Alembic migration for the `onto` schema)

## Goal

Give the ontology bake-off its own model-calling layer, separate from
`app/agentic/llm_client.py` (which the people/PE pipelines depend on and which
retries timeouts 3x, has `max_tokens=500` and no `base_url`). The layer is a
data-driven registry of candidate models and routes, a pricing file that must be
verified and fresh before a model can run, and a budget guard that reserves the
worst-case cost of every call atomically before anything is sent.

No model is called in this spec. The roster (D1) is not decided: the registry
lists candidates and the owner selects names later (`ONTOLOGY_ROSTER` or an
explicit list passed by the SPEC_165 runner). The registry lists vendors
neutrally; listing order is alphabetical by registry name and implies nothing.

## Owner decisions applied

| Decision | Applied as |
|---|---|
| D2 budget caps | Defaults pilot **$20**, round **$100**, month **$150** (`ONTOLOGY_PILOT_BUDGET_USD`, `ONTOLOGY_ROUND_BUDGET_USD`, `ONTOLOGY_MONTHLY_BUDGET_USD`). `allow_premium=False` by default. |
| D12 | `app/core/llm_cost_tracker.py::_calculate_cost` logs one WARNING per unknown model when it falls back to $0. **No** new columns on `llm_usage` (reasoning tokens / cost basis go to the future `onto.run`). |
| D1 | Not decided. Registry is data (`app/ontology/providers.json`); no roster is hard-coded. |

## Design

```
app/ontology/
  __init__.py      minimal (SPEC_162/163 add gold/ and standards/ subpackages)
  providers.json   registry data: name -> ProviderSpec fields
  providers.py     ProviderSpec, load_registry, roster selection, adapters
                   (OpenAICompatAdapter, AnthropicAdapter), structured output,
                   retry policy, `python -m app.ontology.providers --check`
  pricing.json     per registry name: $/M in/out/cached/reasoning, long-context
                   tiers, promo_end, premium (derived), as_of, source_url, verified
  pricing.py       loader, cost maths (worst case + actual), price gate
  budget.py        BudgetStore interface, MemoryBudgetStore, PostgresBudgetStore,
                   BUDGET_DDL (for SPEC_165's migration), BudgetGuard
                   (gates, preflight, reserve/settle/release, guarded call)
  tokens.py        per-model token counting with ±30% fallback band
  jsonschema_lite.py  local validation (jsonschema if installed, else subset)
```

### Registry fields (`ProviderSpec`)

`name`, `adapter` (`openai_compat` | `anthropic`), `route` (direct / openrouter /
vertex / local), `base_url`, `docker_base_url` (local only), `base_url_env`,
`api_key_env`, `model_id`, `max_output_tokens`, `max_tokens_param`,
`structured_output` (`json_schema_strict` | `json_schema` | `json_object` |
`tool_schema` | `prompt_only`), `reasoning_control` {param, value, bounded:
yes/no/unverified, reservation_ceiling_tokens}, `usage_reasoning_field`,
`usage_cached_field`, `reasoning_in_output` (vendor counts reasoning inside
completion tokens), `generation_mode` (single | split), `context_window`,
`tokenizer`, `timeout_s`, `terms_url`, `terms_checked_at`, `jurisdiction`,
`class` (closed | open_hosted | open_local), `extra_body`, `notes`.

Model ids and terms are copied from PLAN_100 §4 and are **unchecked**
(`terms_checked_at: null`); the owner/pilot confirms them on run day alongside
prices.

### Price gate (plan §9.3 step 1, "hard prerequisite")

A hosted model is refused (`PriceGateError`) when its `pricing.json` entry is
missing, `verified` is false, `as_of` is missing or older than
`ONTOLOGY_PRICE_MAX_AGE_DAYS` (default 1, i.e. re-entered on run day),
`source_url` is missing, `promo_end` has passed, or it is premium
(output ≥ `ONTOLOGY_PREMIUM_OUTPUT_PER_M`, default $40/M) without
`allow_premium=True`. Every seeded entry is `verified: false` because the plan
says none of the per-model prices were re-checked at first party. Local
(`open_local`) models are $0 and exempt, but still logged.

A long-context tier whose prices are `null` means "unpriced above this
threshold": a prompt that crosses it is refused.

### Reasoning bound gate (§9.3 step 2)

`bounded: "no"` refuses the model unless `reservation_ceiling_tokens` is set
(then the reservation uses that ceiling). `"unverified"` is allowed (the pilot
verifies it) and is reported as a warning in the preflight.

### Cost maths

- Worst case per call = `in_tok × p_in + max_out × max(p_out, p_reasoning)`, at
  the long-context tier selected by `in_tok`; `max_out` is
  `max(max_output_tokens, reservation_ceiling_tokens)`. When `in_tok` comes from
  the fallback counter the worst case uses `in_tok × 1.3`.
- Actual = `(in − cached) × p_in + cached × p_cached + visible_out × p_out +
  reasoning × p_reasoning` (visible_out = out − reasoning when the vendor
  counts reasoning inside completion tokens).

### Budget guard (§9.3 steps 3-6)

- Scopes: **call** (`ONTOLOGY_CALL_CAP_USD`, default $10, not an owner figure),
  **round/pilot** (budget row), **month** (budget row keyed `YYYY-MM`).
- `reserve()` locks the round and month rows (`SELECT ... FOR UPDATE`, ordered
  by id), checks `spent + reserved + r <= cap` for each, then increments both
  and inserts a reservation row, in one transaction. Two concurrent reservers
  cannot both pass. Refusal raises `BudgetExceeded` before any request is sent.
- `settle()` moves the reservation to actual usage in one transaction;
  `release()` drops it.
- `guarded_call()`: gates → count/accept `in_tok` → reserve → adapter call →
  settle (and log to `llm_cost_tracker` with `provider=` and `cost_usd=`
  overrides, `source="ontology:<label>"`). On an error that cannot have been
  billed (exception before send, 4xx, retry exhaustion on 429/5xx) the
  reservation is released. **On a timeout** billing is unknown, so the
  reservation is settled at the worst case with `cost_basis="timeout_worst_case"`
  (conservative; the ledger is the primary control). A connection error is
  released only when the connection was never established (`httpx.ConnectError`);
  otherwise it is settled at the worst case too.
- Usage is normalised across adapters: `input_tokens` includes cached tokens
  (Anthropic `cache_read_input_tokens` and `cache_creation_input_tokens` are
  added to its `input_tokens`); `output_tokens` includes reasoning when
  `reasoning_in_output` is true.
- `preflight()` returns expected and whole-round worst case per model and in
  total, refusals and warnings; a worst case above the remaining budget needs
  `confirm_worst_case=True` (else `WorstCaseNotConfirmed`). It makes no calls.

### Adapters

- One `OpenAICompatAdapter` (AsyncOpenAI with `base_url`, `max_retries=0`,
  explicit `httpx.Timeout`) for OpenAI, Gemini OpenAI-compat endpoint, xAI,
  OpenRouter, DeepSeek, Vertex where OpenAI-compatible, LM Studio and
  llama.cpp `llama-server`. LM Studio resolves `base_url_env` →
  `docker_base_url` (`http://host.docker.internal:1234/v1`) when running in a
  container → `base_url` (`http://localhost:1234/v1`).
- `AnthropicAdapter`: `anthropic` imported lazily; constructing it without
  `ANTHROPIC_API_KEY` raises `ProviderNotConfigured`.
- Structured output: `json_schema_strict` sends `response_format=json_schema,
  strict=true`; `json_schema` sends it non-strict; `json_object` sends
  `{"type": "json_object"}`; `prompt_only`/`tool_schema` as named. Every
  response is parsed and validated locally regardless.
- Retry policy: retry only HTTP 429 and 5xx responses (no output was produced),
  with exponential backoff + jitter, honouring `Retry-After`, at most
  `max_retries` (default 2). **Timeouts and connection errors are never
  retried.** A `finish_reason` of `length`/`max_tokens` marks the result
  `truncated=True`.
- `app/agentic/llm_client.py` is not imported or modified.

### Storage and DDL

No Alembic migration in this spec (SPEC_165 owns 0022+ for `onto`; SPEC_162
takes 0022 for CMS). `budget.BUDGET_DDL` holds the `onto.budget` and
`onto.budget_reservation` DDL for SPEC_165 to put in its migration. Tests apply
it to a disposable database.

## Acceptance Criteria

- [x] `app/ontology/` package exists with a minimal `__init__.py`; no `gold/` or `standards/`.
- [x] Registry loads from `providers.json`; every entry has the required fields; roster is selectable by name and none is hard-coded.
- [x] OpenAI-compat adapter shapes requests correctly (model, max-token param, response_format per mode, reasoning param, extra_body) — verified with `httpx.MockTransport`.
- [x] Timeout is not retried (one request), 429/5xx are retried with backoff then succeed or give up, 400 is not retried.
- [x] Anthropic adapter is refused without a key and shapes requests with MockTransport when a key is given.
- [x] `pricing.json` entries all carry `as_of`, `source_url`, `verified=false`; unverified, stale, past-promo, missing or premium-without-flag prices block a run.
- [x] Worst-case maths includes long-context tier, reasoning price, fallback band; unpriced tier refuses.
- [x] Budget reservation refuses before send when any cap would be exceeded; settle/release adjust the ledger; local models cost $0 and are still logged.
- [x] Two concurrent reservers on Postgres (`FOR UPDATE`) cannot both pass.
- [x] Preflight requires `confirm_worst_case` when the whole-round worst case exceeds the remaining budget.
- [x] `python -m app.ontology.providers --check` lists providers and key presence without network calls.
- [x] D12: `_calculate_cost` logs a WARNING once per unknown model.
- [x] No test makes a network call.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_package_minimal | `__init__` minimal, no gold/standards created, llm_client not imported |
| T2 | test_registry_loads_and_fields | all fields, valid enums, neutral (no roster/recommended flag) |
| T3 | test_roster_selection | names selected from env/list, unknown name refused |
| T4 | test_openai_compat_request_shape_strict | json_schema strict, max_completion_tokens, reasoning_effort |
| T5 | test_openai_compat_request_shape_json_object_and_extra_body | json_object mode + OpenRouter extra_body + local validation failure flagged |
| T6 | test_no_retry_on_timeout | exactly one request on timeout, ProviderTimeout raised |
| T7 | test_retry_on_429_and_5xx_then_success | retried, Retry-After honoured, success |
| T8 | test_no_retry_on_400 | single request, ProviderRequestError |
| T9 | test_truncation_flag | finish_reason=length → truncated |
| T10 | test_usage_reasoning_and_cached_fields | dotted usage paths parsed |
| T11 | test_anthropic_requires_key / test_anthropic_request_shape | guarded import + shaping |
| T12 | test_local_base_url_resolution | env > docker > localhost |
| T13 | test_pricing_seed_all_unverified | every entry verified=false, as_of, source_url |
| T14 | test_price_gate_* | unverified / stale / promo_end / missing / premium |
| T15 | test_worst_case_math_* | base, long-context tier, unpriced tier, band, reasoning price, ceiling |
| T16 | test_actual_cost_math | cached and reasoning split |
| T17 | test_budget_refuses_before_send | adapter never called when cap exceeded (call/round/month) |
| T18 | test_budget_settle_and_release | ledger arithmetic |
| T19 | test_guarded_call_timeout_settles_worst_case | timeout → no retry, worst case spent |
| T20 | test_local_model_zero_cost_logged | $0 and recorder called |
| T21 | test_preflight_confirm_worst_case | warning requires confirm |
| T22 | test_pg_concurrent_reservations | two threads, one passes (PG) |
| T23 | test_pg_settle_release | PG ledger arithmetic |
| T24 | test_check_cli_no_network | `--check` output, no sockets |
| T25 | test_cost_tracker_warns_once_for_unknown_model | D12 |
| T26 | test_token_counter_fallback_band | fallback flags ±30% band |

## Follow-ups

- SPEC_165: put `budget.BUDGET_DDL` into its Alembic revision; add `onto.run`
  with reasoning tokens and `cost_basis`; wire `BudgetGuard` into the runner and
  `/runs/estimate`; reconcile the month scope with `llm_usage` (`source LIKE
  'ontology:%'`).
- Owner/pilot on run day: verify each roster model's price, model id and terms
  (`verified: true`, `as_of`, `terms_checked_at`); confirm reasoning bounding per
  adapter (`bounded`).
- Optional: add `tiktoken` (and `jsonschema`) to the eval image; the code falls
  back without them. Neither is installed in the API image today, so every
  count is the ±30% fallback until then.
- Registry gaps the owner may fill when choosing D1 routes: Vertex AI rows
  (separate prices and regional premiums), OpenRouter rows for closed models,
  OpenRouter rows for DeepSeek. Each needs its own `pricing.json` entry.
- Default reasoning settings in the registry (`reasoning_effort` medium for
  OpenAI, low for Gemini, unset for xAI, thinking off for Anthropic) are
  placeholders for D8-style run parameters; set them per round.
- A sweep for reservations left `reserved` by a crashed worker (release after
  N hours, or settle at worst case) belongs with the SPEC_165 runner.
- Seeded `long_context` tiers with null prices (OpenAI non-Sol above 272k,
  Gemini 3.1 Pro and Grok 4.7 above 200k) refuse such prompts until priced.
- Optional (D12): the same $0 fallback exists at `app/agentic/llm_client.py:160`
  and is left untouched here.
