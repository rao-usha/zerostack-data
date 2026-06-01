# SPEC 094 — Planner → Executor (generalised walkthrough)

**Status:** Draft
**Task type:** service (backend + frontend)
**Date:** 2026-06-01
**Plan:** PLAN_077_planner_executor (Phase D1)
**Test file:** tests/test_spec_094_planner_executor.py
**Builds on:** SPEC_087 (fit-score) · SPEC_088 (chips) · SPEC_089
(trade-area) · SPEC_090 (Pilot tools) · SPEC_091 (competition) ·
SPEC_092 (story demo) · SPEC_093 (CoT)

## User decisions (locked from PLAN_077)

- Rationales **pre-baked** in the plan (single LLM call, deterministic)
- Hardcoded **SPEC_092 furniture sequence** as fallback if planner fails
- Cache by **thesis hash, 24h TTL** (in-process)
- ⚡ Demo with no thesis → **auto-load FURNITURE_DEMO** then plan

## Goal

Replace the hardcoded SPEC_092/093 furniture-only runner with a
`Plan → Execute` pattern that works for **any** thesis. One LLM call
returns a structured `Plan` (4-8 beats × {title, rationale, tool,
args}); the frontend executor walks it deterministically.

## Acceptance Criteria

- [ ] `POST /atlas/plan` accepts `{thesis_context?, prompt?}`,
      returns `{plan, error?, cache_hit}`. Always 200; fallback path
      returns the frozen furniture plan with `error: "..."`.
- [ ] `PlanModel`/`BeatModel` Pydantic schemas validate the shape.
- [ ] Tool whitelist: `setup_thesis`, `add_constraint`,
      `remove_constraint`, `recommend_candidates`, `enter_trade_area`,
      `exit_trade_area`, `find_competition`, `no_op`.
- [ ] Constraint `dimension` validated against `_VALID_DIMENSIONS`
      from `pilot_tools`.
- [ ] Beats count clamped [3, 12]; titles ≤80 chars; rationales ≤500.
- [ ] In-process `_PLAN_CACHE` keyed by SHA256 of the JSON-sorted
      thesis_context + prompt; 24h TTL.
- [ ] Frontend `fetchAndRunPlan(thesis, prompt)` opens the banner in
      a "Planning your walkthrough…" pre-state, POSTs `/atlas/plan`,
      then runs `runStoryFromPlan(plan)`.
- [ ] `runStoryFromPlan(plan)` walks `plan.beats`: banner title +
      chat rationale + `applyPlanBeat(beat)`.
- [ ] `applyPlanBeat(beat)` dispatches each tool to the existing
      helpers. `enter_trade_area` with `top_pick: true` reads the
      current top fit-score candidate.
- [ ] `appendStaticCoT(label, text)` renders a non-streaming
      assistant.cot message (sibling of `streamCoT`).
- [ ] ⚡ Demo button wired to `fetchAndRunPlan(FURNITURE_DEMO, null)`.
      The SPEC_092 runner stays in code as the fallback path.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_validate_plan_happy_path | well-formed plan validates |
| T2 | test_validate_plan_unknown_tool | unknown tool → rejected |
| T3 | test_validate_plan_bad_dimension | bad constraint dimension → rejected |
| T4 | test_validate_plan_too_many_beats | beats > 12 → rejected |
| T5 | test_validate_plan_short_strings | over-length title/rationale truncated or rejected |
| T6 | test_generate_plan_uses_fallback_when_llm_fails | OpenAI mock raises → fallback plan returned with error string |
| T7 | test_generate_plan_cache_hit | same thesis twice → second call returns cache_hit=True without LLM call |
| T8 | test_plan_body_schema | PlanBody accepts optional thesis + prompt |
| T9 | test_frontend_fetch_and_run_plan_present | atlas.html declares fetchAndRunPlan + runStoryFromPlan + applyPlanBeat + appendStaticCoT |
| T10 | test_frontend_demo_wired_to_planner | thesis-demo onClick = fetchAndRunPlan |
| T11 | test_frontend_apply_plan_beat_dispatches | every whitelisted tool name appears in applyPlanBeat |

## Design

### Pydantic models

```python
class BeatModel(BaseModel):
    title: str = Field(..., min_length=1, max_length=80)
    rationale: str = Field(..., min_length=1, max_length=500)
    tool: Literal["setup_thesis", "add_constraint", "remove_constraint",
                   "recommend_candidates", "enter_trade_area",
                   "exit_trade_area", "find_competition", "no_op"]
    args: Optional[Dict[str, Any]] = None
    wait_ms: Optional[int] = Field(default=1600, ge=200, le=5000)

class PlanModel(BaseModel):
    title: str = Field(..., min_length=1, max_length=80)
    summary: str = Field("", max_length=240)
    thesis_recap: Optional[str] = Field(None, max_length=240)
    beats: List[BeatModel] = Field(..., min_length=3, max_length=12)
```

### Validator

After Pydantic, walk beats and check:
- `tool == "add_constraint"` → args has `dimension ∈ _VALID_DIMENSIONS`
  and numeric `value`
- `tool == "remove_constraint"` → args has `dimension ∈ _VALID_DIMENSIONS`
- `tool == "enter_trade_area"` → args has `geo_id` (5 digit FIPS) OR
  `top_pick: true`; optional `radius_mi` ∈ [1, 250]
- `tool == "find_competition"` → args has `term` (str ≤80) and either
  `geo_id` OR `top_pick: true`
- First beat MUST be `setup_thesis` (auto-prepended if missing)

### Planner LLM call

OpenAI Chat Completions, `response_format={"type":"json_object"}`,
single user message containing the thesis_context + optional user
prompt. System prompt teaches valid schema + the tools.

### Cache

```python
_PLAN_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_TTL_SEC = 24 * 3600

def _cache_key(thesis, prompt):
    return hashlib.sha256(
        json.dumps([thesis or {}, prompt or ""],
                    sort_keys=True).encode()).hexdigest()
```

### Fallback plan (frozen SPEC_092 sequence as a Plan dict)

```python
_FALLBACK_PLAN: Dict[str, Any] = {
  "title": "Furniture-store walkthrough (default)",
  "summary": "Default walkthrough used when the planner couldn't author one.",
  "beats": [
    {"title": "Loading thesis", "rationale": "...", "tool": "setup_thesis"},
    {"title": "Filter HHI ≥ $75K", "rationale": "...",
     "tool": "add_constraint", "args": {"dimension":"hhi_min","value":75000}},
    {"title": "Exclude high-NRI", "rationale": "...",
     "tool": "add_constraint", "args": {"dimension":"exclude_nri","value":50}},
    {"title": "Score 3,248 counties", "rationale": "...",
     "tool": "recommend_candidates", "args": {"top_n":10}},
    {"title": "Top candidates", "rationale": "...", "tool": "no_op"},
    {"title": "Open trade area", "rationale": "...",
     "tool": "enter_trade_area", "args": {"top_pick":True, "radius_mi":50}},
  ],
}
```

### Frontend

`fetchAndRunPlan(thesis, prompt)`:
1. Show story banner with `Step 0 / ? · Planning your walkthrough…`
2. POST `/atlas/plan`, get `{plan, error, cache_hit}`
3. If error, toast.
4. Call `runStoryFromPlan(plan)`.

`runStoryFromPlan(plan)`:
- For each beat: set banner (title + step counter), append
  `appendStaticCoT(beat.title, beat.rationale)`, call
  `applyPlanBeat(beat)`, sleep `beat.wait_ms` honouring cancel token.

`applyPlanBeat`:
```js
const t = beat.tool;
const args = beat.args || {};
if (t === 'setup_thesis')     { fillThesisForm(); /* ... */ }
else if (t === 'add_constraint')     addChip(args.dimension, args.value);
else if (t === 'remove_constraint')  { const c = CONSTRAINTS.find(...); if (c) removeChip(c.id); }
else if (t === 'recommend_candidates') await fetchAndPaintFitScore(loadThesis());
else if (t === 'enter_trade_area')   { let gid = args.geo_id; if (args.top_pick && CURRENT_FIT_RESULT) gid = CURRENT_FIT_RESULT.top_n[0].geo_id; if (gid) await enterTradeArea(gid, args.radius_mi || 50); }
else if (t === 'exit_trade_area')    exitTradeArea();
else if (t === 'find_competition')   { /* POST /atlas/competition, append summary to chat */ }
// no_op: nothing
```

## Files

| File | Action |
|------|--------|
| `app/services/atlas/planner.py` | Create |
| `app/api/v1/atlas.py` | PlanBody + POST /atlas/plan |
| `frontend/atlas.html` | fetchAndRunPlan + runStoryFromPlan + applyPlanBeat + appendStaticCoT; rewire ⚡ Demo |
| `tests/test_spec_094_planner_executor.py` | T1-T11 |

## Verification

1. `POST /atlas/plan {"thesis_context":{"industry_label":"Furniture stores"}}`
   → 200, `plan.beats` non-empty, first beat `setup_thesis`, no error.
2. Same call again → 200, `cache_hit: true`, sub-100ms.
3. Modify thesis (e.g. industry → "Coffee shops") → 200, cache miss,
   fresh plan; beats reference coffee-oriented thresholds.
4. With OpenAI key removed → 200, `plan` = fallback, `error: "..."`.
5. Browser ⚡ Demo → banner "Planning…" ~3s → 4-8 beats run, each with
   banner title + chat rationale + map mutation.
