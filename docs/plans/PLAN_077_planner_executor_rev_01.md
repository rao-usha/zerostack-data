# PLAN 077 rev_01 — Planner gap surfaced: the Pilot has no session context

**Date:** 2026-06-01
**Parent:** PLAN_077_planner_executor.md (SPEC_094 shipped at commit cfdee16)
**Successor:** PLAN_078_context_aware_copilot (planned with this revision)

## Revision 01

Two threads of feedback after SPEC_094 + the b86bafb fixes:

1. **Tactical bug fixes** (already shipped at b86bafb): LatLng crash
   when `CENTROIDS[gid]` array was read as `{lat, lon}`; silent
   "Planning your walkthrough" wait with no spinner.
2. **Strategic gap** (real driver of this revision): asking the
   Atlas Pilot *"can you see what just happened?"* returned a
   generic FEMA + SEC-filings dump. The agent had **zero
   awareness** of the user's session: which thesis is loaded,
   which chips are active, what candidates are pinned, which
   trade area is open, what the executor just played.

## What Was Wrong

The Pilot is built like a tool-using analyst with **no memory of the
visible app**. It treats every question as cold-start:

- No session-state block in the system prompt. The agent does NOT
  know thesis content, chip set, fit-score result, top-N pin list,
  current trade area, current map view, or recent actions.
- No action history. When the planner executor adds a chip, paints
  a fit-score, opens a trade area, the Pilot has no record that any
  of this happened.
- "Recent events" tools (FEMA, SEC) shadow the real intent of
  *"what just happened?"*. The agent falls back to those because
  they're the only "recent things" it can see.
- Tool coverage is incomplete for full chat-driven map navigation.
  Some primitives exist (`zoom_to`, `toggle_layer`,
  `enter_trade_area`, `add_constraint`), but there's no
  `select_pin`, no `clear_chips`, no `change_recipe_weight`, no
  `pan_to_focal`, no `describe_current_view`.

The user named the bar correctly: *"the best AI experience of all
time with a map."* That requires a copilot that **sees what the user
sees and can act on the whole surface**. We haven't built that yet.

## What Was Fixed (in this revision window)

Shipped at commit b86bafb (before this rev doc):

- Top-N pin LatLng crash (`CENTROIDS[gid]` is `[lon, lat]` array,
  not `{lat, lon}` object). Guarded + destructured.
- "Planning…" silence → indigo spinner + rotating hint cycle
  ("Asking the Pilot to author your walkthrough…" → "Reading your
  thesis…" → "Choosing constraints and beats…" → "Drafting the
  rationales…").

The strategic gap is **not** fixed in this revision — it needs a new
plan (PLAN_078) and a new spec stack. This rev_01 exists to:
- Acknowledge the gap honestly.
- Pin the bug fixes that already shipped.
- Hand off to PLAN_078 for the real work.

## Lessons Learned

- A "Pilot" without session state isn't a copilot — it's a chatbot
  glued onto a map. The distance between those two products is
  measured in *how much of the user's current view the agent sees
  before it speaks.*
- Tools that *do things* are necessary but insufficient. The agent
  also needs tools that **report state** — and, more importantly,
  needs that state injected into the system prompt every turn so it
  can reference it without an explicit query.
- "What just happened?" is the test question. If your copilot
  can't answer it from the visible session, no amount of tool-call
  cleverness will make it feel context-aware.
- Defaulting to off-topic tools (recent FEMA events) when the real
  intent is *recap of MY actions* is a UX smell that flags the
  missing state layer.
- For arbitrary `CENTROIDS[gid]` shape: GeoJSON convention is
  `[lon, lat]`. Earlier code stored it that way for the migration-
  arc renderer; later code (fit pins) assumed `{lat, lon}`. Mixed
  conventions for the same data inside one file. Lock the shape
  on first use and respect it everywhere.

## Hand-off

See `PLAN_078_context_aware_copilot.md` for the full architecture
of:
- Session-state injection block (`<session_state>` in the system
  prompt every turn)
- Action history log (server-side or localStorage-resident)
- Tool-call expansion for full chat-driven map control
- Prompt rules that route "what just happened?" intent to a
  session recap instead of generic recent-events tools
- "Streaming reasoning" surface so the agent's plan-of-attack is
  visible
