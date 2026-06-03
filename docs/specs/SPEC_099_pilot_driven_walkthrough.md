# SPEC 099 — Pilot-driven walkthrough (unify ⚡ Demo with the chat)

**Status:** Draft
**Date:** 2026-06-03
**Builds on:** SPEC_087-098 (Decision Map + context-aware copilot)
**Test file:** tests/test_spec_099_pilot_driven_walkthrough.py

## Trigger

> "The planning model all that should happen in the pilot. It should
> be as if it's running as a human on chat. This still needs work."

After SPEC_098, the ⚡ Demo button still bypasses the Pilot entirely
— it calls `/atlas/plan` and runs a banner-driven script
(`runStoryFromPlan` + `appendStaticCoT` + `applyPlanBeat`). The chat
panel only receives static text drops. The thinking chips, receipts,
and streaming narration from SPEC_098 never fire during the demo
because handlePilotEvent is never invoked.

## Goal

Make the walkthrough a real Pilot conversation:
1. Click ⚡ Demo → the thesis loads → the Pilot receives a
   "walk me through this thesis" prompt and **does the walkthrough
   itself**, streaming narration and calling tools that mutate the map.
2. No top-of-map banner. No `/atlas/plan` round trip.
3. SPEC_098's thinking chips + receipts fire naturally for every tool
   call.
4. The user can interrupt at any point with a follow-up.

## Acceptance Criteria

- [ ] New frontend `runPilotWalkthrough(thesis, prompt?)`:
      - Persists `thesis` to localStorage so session-state sees it.
      - Fills the form + opens the Thesis blocks so the user sees the
        seeding take effect.
      - Submits a structured walkthrough prompt as a Pilot question
        via `askPilot()` (which uses the existing streaming pipeline
        + tools + thinking chips + receipts).
- [ ] ⚡ Demo button wired to `runPilotWalkthrough(loadThesis() ||
      FURNITURE_DEMO)`. The old `fetchAndRunPlan` path is kept in
      code as `runPlannerWalkthroughFallback` but is no longer the
      default ⚡ Demo wiring.
- [ ] System prompt gains a `9f. WALKTHROUGH MODE` rule teaching the
      Pilot to work deliberately when asked to walk through a thesis:
      one tool at a time, 1-2 sentences of reasoning BEFORE each
      tool, end with `present_options`.
- [ ] No top-of-map story banner during ⚡ Demo.

## Test cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_run_pilot_walkthrough_declared | atlas.html declares `function runPilotWalkthrough` |
| T2 | test_demo_button_routes_to_walkthrough | `thesis-demo` onClick now invokes `runPilotWalkthrough` |
| T3 | test_walkthrough_persists_thesis | `runPilotWalkthrough` writes to atlas_thesis_v1 |
| T4 | test_walkthrough_calls_askpilot | body contains `askPilot(` |
| T5 | test_walkthrough_prompt_present | a `WALKTHROUGH_PROMPT` constant exists with key tools mentioned |
| T6 | test_system_prompt_walkthrough_rule | SYSTEM_PROMPT contains "WALKTHROUGH MODE" |
| T7 | test_planner_path_kept_as_fallback | `runStoryFromPlan` and `fetchAndRunPlan` still exist (resilience) |

## Design

### Walkthrough prompt

```
Walk me through this thesis like a site-selection consultant. Work
through it step by step, ONE TOOL AT A TIME with 1-2 sentences of
reasoning BEFORE each tool call:

1. Add the must-have constraints from my thesis (HHI floor, hazard
   exclusion if appropriate) — explain each threshold's logic.
2. Score the candidates (recommend_candidates) — narrate what the
   top picks have in common.
3. Open the trade area around the top candidate (enter_trade_area)
   — explain what the neighbour summary tells us.
4. Check competition (find_competition) — quote the count.

End with present_options offering 2-3 concrete next moves.
```

### System prompt rule 9f

> When the user sends a walkthrough request ("walk me through",
> "show me how to evaluate this", "demo this thesis", "guide me"),
> work through it DELIBERATELY:
> - One tool call at a time. Never batch.
> - 1-2 sentences of reasoning narration BEFORE each tool call,
>   not after. The user wants to see "I'm going to X because Y" then
>   the tool fire.
> - Use add_constraint, recommend_candidates, enter_trade_area,
>   find_competition as the spine.
> - Always end with present_options offering 2-3 concrete next moves.

### Files

- `app/services/atlas/pilot.py` — system prompt rule 9f
- `frontend/atlas.html` — `WALKTHROUGH_PROMPT`, `runPilotWalkthrough`,
  rewire `thesis-demo`
- `tests/test_spec_099_pilot_driven_walkthrough.py` — T1-T7
