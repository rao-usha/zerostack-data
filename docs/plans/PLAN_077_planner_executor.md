# PLAN 077 — Planner → Executor (generalised walkthrough)

**Date:** 2026-06-01
**Status:** Draft (awaiting direction)
**Builds on:** SPEC_092 (storytelling demo) + SPEC_093 (CoT) + SPEC_090
(Pilot tools) + PLAN_076 (CoT framework, Phase D placeholder)

## Trigger

> "plan the planner→executor next"

PLAN_076 sketched four ways to add reasoning to the demo. We picked
**Option B** (live CoT alongside hardcoded beats) and shipped it as
SPEC_093. PLAN_076 itself flagged **Option D — Planner → Executor**
as the natural follow-on: replace the hardcoded furniture sequence
with a structured *plan* the LLM authors on the fly so the walkthrough
works for **any** thesis.

## Goal

Make ⚡ Demo (and a future "Walk me through this") work for an
*arbitrary* investment thesis by:
1. Asking the Pilot to **plan** the walkthrough (one LLM call → a
   structured `Plan` JSON).
2. Validating the plan against a whitelist of tools/args.
3. **Executing** the plan deterministically: each beat shows its
   title in the story banner, its rationale in the chat, and runs
   the named tool against the Decision Map.

Today the demo only works for the furniture thesis (hardcoded
prompts, hardcoded actions). After this, it works for *whatever*
the user typed into the Thesis form.

## The contract — `Plan` schema

```json
{
  "title": "string ≤ 80 chars (banner title for the run)",
  "summary": "string ≤ 240 chars (chat opener — one paragraph)",
  "thesis_recap": "string ≤ 240 chars (optional)",
  "beats": [
    {
      "title": "string ≤ 60 chars (banner step label)",
      "rationale": "string ≤ 500 chars (chat narration for this beat)",
      "tool": "add_constraint | remove_constraint | recommend_candidates | enter_trade_area | exit_trade_area | find_competition | setup_thesis | no_op",
      "args": { "...": "..." } | null,
      "wait_ms": 1600
    }
  ]
}
```

`beats.length` ∈ [3, 12]. Tool names from a closed whitelist. Args
validated against each tool's existing schema (already declared in
`pilot_tools.TOOL_DEFS`). Anything else → reject and fall back.

## Architecture — backend

**New service:** `app/services/atlas/planner.py`

```python
PLANNER_SYSTEM_PROMPT = """You are a site-selection consultant
authoring a guided walkthrough plan for a Decision Map app.

Output ONLY valid JSON matching this Plan schema:
  {title, summary, thesis_recap?, beats:[{title, rationale, tool, args, wait_ms?}]}

Available tools and their effect:
  setup_thesis(args=null)                          — fills the thesis form, no-op visually
  add_constraint(dimension, value)                 — pushes a filter chip; dimensions:
                                                     hhi_min, hhi_max, establishments_min,
                                                     broadband_min, exclude_nri
  remove_constraint(dimension)                     — pops a chip
  recommend_candidates(top_n)                      — surfaces top-N pins (silent on map; informs you)
  enter_trade_area(geo_id?, top_pick?, radius_mi)  — opens trade-area card; pass top_pick:true to
                                                     auto-pick from the most recent recommendation
  exit_trade_area()                                — closes the trade-area card
  find_competition(geo_id, radius_mi, term)        — counts competing businesses (Yelp)
  no_op                                            — used for context-only beats

Rules:
  - 4-8 beats total. The first beat MUST be setup_thesis with no args.
  - Pick chip thresholds that match the user's thesis (HHI from
    target_hhi_min if set, exclude_nri if user said to avoid risk).
  - End with enter_trade_area(top_pick:true) unless explicitly told otherwise.
  - Each rationale is 2-3 sentences, analyst-grade, no preambles.
  - Honest about uncertainty. No hallucinated layer ids or dimensions.
"""

def generate_plan(db, thesis_context, user_prompt=None, model=MODEL):
    """One LLM call → validated Plan dict. Falls back to a safe
    'default' plan if generation/validation fails."""
    raw = _call_openai_json(...)
    plan, err = validate_plan(raw)
    if err: return _default_plan_for(thesis_context), err
    return plan, None
```

**Validation** (`validate_plan(raw_dict) -> (plan, error)`):
- Schema-check via Pydantic (`PlanModel`, `BeatModel`).
- Tool whitelist: only the 8 names above.
- Args schema: reuse `pilot_tools.TOOL_DEFS` JSON Schemas.
- Beats count ∈ [3, 12].
- String length caps enforced.
- Returns either `(plan, None)` or `(stub_plan, "reason")`.

**New endpoint:** `POST /atlas/plan`

```python
class PlanBody(BaseModel):
    thesis_context: Optional[Dict[str, Any]] = None
    prompt: Optional[str] = None       # e.g. "walk me through where to open this"

@router.post("/plan")
def atlas_plan(body, db=Depends(get_db)):
    plan, err = generate_plan(db, body.thesis_context, body.prompt)
    return {"plan": plan, "error": err}
```

Returns 200 in both happy and fallback paths — the frontend always
gets *a* plan it can run.

## Architecture — frontend

**Replace `runFurnitureStoryDemo` with `runStoryFromPlan(plan)`:**

```js
async function fetchAndRunPlan(thesis, userPrompt) {
  // Show "Planning…" pre-state on the banner immediately so the
  // 1-3s wait doesn't read as a hang.
  openStoryBanner();
  setStoryBeat(0, 'Planning your walkthrough…');
  try {
    const r = await fetch(API + '/plan', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        thesis_context: thesis || null,
        prompt: userPrompt || null,
      }),
    }).then(x => x.json());
    if (r.error) toast('Plan fell back to default: ' + r.error);
    return runStoryFromPlan(r.plan, thesis);
  } catch (e) {
    closeStoryBanner();
    toast('Walkthrough failed: ' + e.message);
  }
}

async function runStoryFromPlan(plan, thesis) {
  cancelAnyRunningStory();
  const tok = { cancelled: false };
  RUNNING_STORY = tok;

  // Plan summary streamed once as a "kicker" CoT message
  if (plan.summary) appendStaticCoT('PLAN', plan.title + ': ' + plan.summary);

  const total = plan.beats.length;
  for (let i = 0; i < total; i++) {
    if (tok.cancelled) return;
    const beat = plan.beats[i];
    // 1. Show beat title in banner
    setStoryBeat(i + 1, beat.title);
    paintStoryDots(i + 1);
    // 2. Stream rationale into chat
    appendStaticCoT(`STEP ${i + 1} · ${beat.title}`, beat.rationale);
    // 3. Execute tool
    if (beat.tool && beat.tool !== 'no_op') {
      await applyPlanBeat(beat);
    }
    // 4. Pause
    await sleep(beat.wait_ms || STORY_DELAY_MS, tok);
  }
  setTimeout(closeStoryBanner, 4000);
}

async function applyPlanBeat(beat) {
  const { tool, args } = beat;
  if (tool === 'setup_thesis') {
    fillThesisForm(); refreshThesisPreviews();
    document.querySelectorAll('#view-thesis details.block').forEach(d => {
      d.open = ['industry', 'demographics'].includes(d.dataset.block);
    });
    return;
  }
  if (tool === 'add_constraint')    return addChip(args.dimension, args.value);
  if (tool === 'remove_constraint') {
    const c = CONSTRAINTS.find(x => x.dimension === args.dimension);
    if (c) removeChip(c.id);
    return;
  }
  if (tool === 'recommend_candidates') {
    await fetchAndPaintFitScore(loadThesis()); return;
  }
  if (tool === 'enter_trade_area') {
    let gid = args.geo_id;
    if (args.top_pick && CURRENT_FIT_RESULT?.top_n?.length)
      gid = CURRENT_FIT_RESULT.top_n[0].geo_id;
    if (gid) await enterTradeArea(gid, args.radius_mi || 50);
    return;
  }
  if (tool === 'exit_trade_area') return exitTradeArea();
  if (tool === 'find_competition') {
    // Could just render to chat as a tool-call summary
    const r = await fetch(API + '/competition', { ... });
    appendStaticCoT('COMPETITION', `${r.count} ${args.term} within ${args.radius_mi} mi`);
    return;
  }
}
```

**`appendStaticCoT(label, text)`** — sibling of SPEC_093's
`streamCoT`, but for pre-authored text (no streaming, no LLM call,
just append + scroll). Reuses `.msg.assistant.cot` styling.

**⚡ Demo button** → `fetchAndRunPlan(FURNITURE_DEMO, null)`. The
furniture preset still works; the path is now generic.

**New button: "✨ Walk me through this"** on the Thesis panel,
disabled until at least an `industry_label` is set. Clicking calls
`fetchAndRunPlan(loadThesis(), null)` — works for whatever the user
typed in.

## Decision points (need your direction)

### 1. Rationales — pre-baked in the plan, or streamed live per beat?

- **Pre-baked (Option D-static):** one LLM call, ~3s wait up front,
  then instant per-beat narration on replay. Plan is fully self-
  contained / saveable / shareable.
- **Live per beat (Option D-live):** the plan only has *titles* + a
  tool list; each beat's rationale is streamed live the way SPEC_093
  does it now. ~6 small LLM calls per run on top of the plan call.
  Richer per-beat text, but plans aren't replayable verbatim.
- **Hybrid:** plan carries a one-sentence rationale; an "expand"
  affordance asks for more depth on demand.

### 2. Fallback when planner fails

- Hardcoded fallback plan (the SPEC_092/093 furniture sequence) so
  the user always gets *something*.
- Show an error and offer Retry.
- Retry once silently with a stricter prompt.

### 3. Plan caching

- Cache by hash(thesis_context) so re-runs are instant (~$0 ongoing).
- Always fresh (consistent with SPEC_093's "fresh every run" call).
- Cache with a 24-hour TTL.

### 4. Behaviour when no thesis is saved

- Click ⚡ Demo → auto-load FURNITURE_DEMO and plan against it
  (current behaviour).
- Disable the button until a thesis exists; surface a tooltip.
- Click ⚡ Demo → prompt: "What kind of business?" → planner uses
  that text as user_prompt.

## Phasing

| Phase | Scope |
|---|---|
| **D1** | Backend planner + validator + /atlas/plan endpoint. Frontend runStoryFromPlan + applyPlanBeat. ⚡ Demo wired to fetch+run. Hardcoded furniture fallback if planner errors. |
| **D2** | "✨ Walk me through this" button for arbitrary saved theses. Plan caching (if picked). |
| **D3** | Save/share/replay plans — store recent plans in localStorage, render a "Past walkthroughs" view, "Replay this" button. |
| **D4** | Pilot drives the planner from chat: "walk me through where to open a coffee shop in Brooklyn" → /atlas/plan → executor. |

## Files (when approved)

| File | Action |
|------|--------|
| `app/services/atlas/planner.py` | Create — PLANNER_SYSTEM_PROMPT, generate_plan, validate_plan |
| `app/services/atlas/pilot.py` | Reuse _call_openai_json helper (extract if needed) |
| `app/api/v1/atlas.py` | PlanBody + POST /atlas/plan |
| `frontend/atlas.html` | fetchAndRunPlan, runStoryFromPlan, applyPlanBeat, appendStaticCoT, "Walk me through this" button, replace ⚡ Demo wiring |
| `tests/test_spec_094_planner_executor.py` | Schema validation, tool whitelist, fallback path, frontend wiring |
| `docs/specs/SPEC_094_planner_executor.md` | Companion |

## Open questions for the user

See AskUserQuestion that follows.

## Revisions

- [PLAN_077 rev_01](./PLAN_077_planner_executor_rev_01.md) — pins
  the b86bafb bug fixes (LatLng + planning UX) and flags the
  bigger gap surfaced by user feedback: the Pilot has no session
  context, so *"can you see what just happened?"* falls back to
  generic recent-events tools. Hands off to PLAN_078. 2026-06-01.
