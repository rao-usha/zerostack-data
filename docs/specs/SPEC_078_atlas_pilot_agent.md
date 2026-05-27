# SPEC 078 — Phase B: Atlas Pilot agent (tool dispatcher + narration UI)

**Status:** Draft (overnight build)
**Task type:** service + frontend + LLM integration
**Date:** 2026-05-27
**Plan:** PLAN_073 rev_01 Phase B
**Test file:** `tests/test_spec_078_atlas_pilot.py`
**Builds on:** Anthropic SDK (already in deps), Phase A tract data, all existing SPEC_065-075 endpoints.

## Goal

The category-defining differentiator: an LLM that pilots the Atlas
in service of a user's question, narrating each step and citing
every numerical claim. v0 ships a **non-streaming, constrained-tool
agent** with a minimal-but-honest tool surface.

## Scope (v0 — overnight build)

### What ships
- `app/services/atlas/pilot_tools.py` — 6 tools wrapping existing endpoints
- `app/services/atlas/pilot.py` — agent loop using Anthropic tool-use API
- `POST /atlas/pilot` — endpoint that takes a question, returns a full
  agent response (plan + tool calls + narration + citations)
- Frontend: **header question box** + **response side panel** that
  renders the agent's plan, tool calls, narration, and citations
- Constrained tool set: agent picks from 6 known tools, no free-form
  code execution

### What does NOT ship in v0
- **SSE streaming** — non-streaming first; client waits for full response
- **Per-claim citation enforcement** — agent encouraged via system
  prompt; post-hoc validator is a follow-up
- **Cost / latency caps** — soft limits via system prompt; hard
  server-side limits are a follow-up
- **Map manipulation tools** (focal node, zoom, layer toggle) —
  Phase C/D introduce those; v0 tools are read-only over existing data
- **Conversational follow-up** — single question / single response v0

## The v0 tool surface

```
1. list_layers() → registry of all 22 active layers
2. query_place(geo_id) → multi-layer aggregate for a county / tract
3. compare_places([geo_id, geo_id, ...]) → side-by-side layer values
4. get_recent_events(limit, sources) → recent FEMA + SEC activity
5. get_top_migration_flows(top_n) → IRS county-to-county migration
6. cite(claim, source) → log a citation (rendered in evidence panel)
```

Constrained by design: these all wrap existing endpoints. v1 (Phase C+)
adds `plant_focal_node`, `compute_competition`, `compute_demand_surface`,
etc. — the tools that *mutate* the UI.

## Acceptance Criteria

- [ ] `app/services/atlas/pilot_tools.py` exists with 6 tools defined
      as Anthropic-tool-use-compatible JSON schemas + Python callables
- [ ] `app/services/atlas/pilot.py` runs the agent loop: send question
      → call Claude with tools → execute tools server-side →
      feed results back → return final narration + tool-call log
- [ ] `POST /atlas/pilot` endpoint with body `{question, session_id?}`
      returns `{plan, steps, narration, citations, tool_calls, model_used}`
- [ ] Frontend: header "Ask the Pilot" input + side panel that renders
      agent state
- [ ] An end-to-end demo question works:
      "Compare Austin and Houston on income and broadband"
      → agent calls `query_place(48453)` + `query_place(48201)` + cites
      → narration: "Austin median income is $X, Houston $Y; broadband
      subscription Austin Z%, Houston W%."
- [ ] Telemetry events: `pilot_question_submitted`, `pilot_completed`,
      `pilot_tool_called`
- [ ] No regression on existing smoke harness

## Design Notes

### Tool definition pattern
```python
TOOLS = [
    {
        "name": "query_place",
        "description": "Read all available layer values for one place …",
        "input_schema": {
            "type": "object",
            "properties": {
                "geo_id": {"type": "string", "pattern": "^\\d{2,11}$"},
            },
            "required": ["geo_id"],
        },
        "callable": _tool_query_place,   # Python fn(db, **kwargs)
    },
    ...
]
```

### Agent loop
```python
def run_pilot(db, question, session_id=None, model="claude-sonnet-4-6"):
    messages = [{"role": "user", "content": question}]
    tool_calls = []; citations = []; loop_n = 0; MAX_LOOPS = 8
    system = build_system_prompt()
    while loop_n < MAX_LOOPS:
        resp = client.messages.create(model=model, system=system,
                                       tools=TOOL_DEFS, messages=messages,
                                       max_tokens=4000)
        if resp.stop_reason == "end_turn":
            return synthesize(resp, tool_calls, citations)
        for block in resp.content:
            if block.type == "tool_use":
                result = dispatch(db, block.name, block.input)
                tool_calls.append({"name": block.name, "input": block.input, "result": result})
                if block.name == "cite":
                    citations.append({"claim": block.input.get("claim"),
                                       "source": block.input.get("source")})
                messages.append({"role": "assistant", "content": resp.content})
                messages.append({"role": "user", "content": [{"type": "tool_result",
                                  "tool_use_id": block.id, "content": str(result)}]})
        loop_n += 1
    return synthesize(resp, tool_calls, citations, truncated=True)
```

### System prompt skeleton
- "You are Atlas Pilot, an expert public-data analyst."
- "Plan briefly before calling tools."
- "Every numerical claim in your final answer must be backed by a `cite()` call."
- "If a tool returns no data, say so; don't fabricate numbers."
- "Be concise — analyst-grade prose, not chat."
- Tool descriptions baked into the prompt.

### Frontend UX (v0 minimal)
```
HEADER (existing chrome):
  [Question box: "Ask the Pilot…"]  [Ask →]
  Compare layers · Scatter · Recent · Share

When user submits → response panel slides in from right (replaces
place panel temporarily) showing:
  PLAN              (LLM's stated plan)
  STEPS             (each tool call + result preview)
  NARRATION         (final analyst summary)
  CITATIONS         (clickable list of sources)
```

### Cost / model
- Model: `claude-sonnet-4-6` (good balance for v0)
- Max tokens per response: 4000
- Max loop iterations: 8 tool calls per question
- Per-question cost target: <$0.50 (Sonnet at ~$3/M input, $15/M output)
- No hard server-side cap in v0; relies on max_tokens + loop cap

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_078_atlas_pilot_agent.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_078_atlas_pilot_agent` |
| `app/services/atlas/pilot_tools.py` | Create | 6 tool definitions + callables |
| `app/services/atlas/pilot.py` | Create | Agent loop |
| `app/api/v1/atlas.py` | Modify | Add `POST /atlas/pilot` endpoint |
| `frontend/atlas.html` | Modify | Header question box + response panel + JS |
| `tests/test_spec_078_atlas_pilot.py` | Create | Endpoint smoke + tool dispatch tests |

## Verification

1. `curl -X POST http://localhost:8001/api/v1/atlas/pilot -H 'Content-Type: application/json' -d '{"question":"What is the median income in Harris County, TX?"}'`
   → returns JSON with plan, tool_calls (at least one `query_place(48201)`), narration, citations
2. Open `/atlas.html` → type "Compare Austin and Houston on income and broadband" → response panel renders within ~15s
3. `python scripts/smoke_atlas.py` → all existing scenarios pass

## Feedback History

_No corrections yet._
