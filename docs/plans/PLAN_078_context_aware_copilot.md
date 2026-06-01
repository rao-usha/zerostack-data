# PLAN 078 — Context-aware Copilot (the "best AI experience with a map")

**Date:** 2026-06-01
**Status:** Draft (awaiting direction)
**Triggered by:** PLAN_077 rev_01 + user feedback —
> "I want to have the best AI experience of all time with a map. I
> want something that's next level. It should have context about the
> users actions. It should also be able to navigate the whole map.
> all through chat."

## The test question

> *"Can you see what just happened?"*

A context-aware copilot answers something like:
> *"Yes — about 2 minutes ago you loaded the Furniture-store demo
> (NAICS 442110, Austin metro, HHI ≥ $75K). The walkthrough added a
> $75K income chip and an exclude-NRI chip, painted the fit-score
> across 3,248 counties (716 surviving filters), and opened a
> 50-mile trade area around Loudoun VA (fit 70/100). 25 neighbour
> counties showed avg income $107K. Yelp tried to fetch competing
> furniture stores around Loudoun but the trial key expired. Want me
> to dig into a different candidate or relax the chips?"*

Today the Pilot answers with a FEMA-disaster + SEC-filings dump.
Closing that gap is the whole point of PLAN_078.

## The four layers

The copilot needs four things to feel "next level," in order of
cost-to-build.

### Layer 1 — Session-state injection (table stakes)

Every Pilot call prepends a `<session_state>` block to the system
prompt, generated fresh from the frontend's current view. Shape:

```
<session_state>
Thesis:
  industry: Furniture stores (NAICS 442110)
  region: Austin, TX metro
  HHI ≥ $75K  ·  age band 25-44  ·  density min 1000

Active chips: HHI ≥ $75K · Exclude NRI > 50
Counter: 716 of 3,248 candidates

Active layer: Decision Map fit-score (retail recipe — 55% income, 30% commercial, 15% broadband)

Top pins on map:
  1. Loudoun County, VA  (fit 70)
  2. Los Angeles County, CA (fit 67)
  3. Santa Clara County, CA (fit 67)
  4. Fairfax County, VA (fit 63)
  5. San Mateo County, CA (fit 63)

Current trade area: Loudoun County VA · 50 mi radius · 25 neighbours
  Avg neighbour income $107K · avg broadband 91.5% · max NRI 97.6
  Closest: Jefferson WV (12.9 mi), Frederick MD (18.5 mi), Clarke VA (21.6 mi)
  Competition: Yelp TRIAL_EXPIRED — count unknown

Map view: zoom 8, center 39.09N -77.64W

Recent actions (last 5):
  T-30s    Pilot     opened trade area for 51107 (radius 50 mi)
  T-32s    Pilot     painted fit-score (3,248 scored, 716 filtered)
  T-34s    Pilot     added constraint exclude_nri ≤ 50
  T-36s    Pilot     added constraint hhi_min ≥ 75000
  T-42s    User      clicked ⚡ Demo → planner returned 4-beat plan
</session_state>
```

Without this, every other layer is wallpaper.

### Layer 2 — Action history log

Every state change in the app writes a line to an in-memory ring
buffer (`SESSION_ACTIONS`) keyed by the session id. The session-state
block reads the last 5-10 actions. Action types:

- `user` actions: chip add/remove, layer toggle, pin click, thesis
  edit, trade-area exit, map pan/zoom, pilot question.
- `pilot` actions: every UI action the agent queued + every read tool
  it called (with summary).
- `planner` actions: each beat executed by the planner runner.

Persisted to localStorage so a refresh doesn't wipe the recap.

### Layer 3 — Full chat-driven navigation (close the tool gap)

Add the tools that are still missing so the agent can drive the entire
map from chat. Inventory of gaps vs. what the UI can do:

| User can today | Agent can call today | Gap |
|---|---|---|
| Click a top-N pin | (no) | `select_pin(rank|geo_id)` |
| Drag chip × to remove | `remove_constraint(dimension)` | ✓ |
| Click + Add chip | `add_constraint(dim, value)` | ✓ |
| Click ↺ Reset thesis | (no) | `reset_thesis()` |
| Set a thesis field | (no) | `set_thesis_field(key, value)` |
| Drag opacity slider | (no) | `set_opacity(value)` |
| Drag boundary toggle | (no) | `toggle_boundaries()` |
| Type pan / zoom / fast travel | partial via `zoom_to(lat,lon,z)` | also need `pan_to_place(geo_id)`, `fit_bounds_of(geo_id)` |
| Open Trade Area | `enter_trade_area(...)` | ✓ |
| Close Trade Area | `exit_trade_area()` | ✓ |
| Edit recipe weights | (no) | `set_fit_weights({income,broadband,...})` |
| Ask "what just happened?" | currently routes to FEMA tool | `describe_session()` returns the same block we inject, formatted for narration |

### Layer 4 — Visible reasoning surface

The agent's tool calls + intermediate thinking already stream into the
bottom-inspector Tool Log (SPEC_085). For a "next level" feel, we add:

- A live "agent is thinking…" chip in the chat thread that shows the
  current tool call (e.g. *"Reading 168 fit-score candidates…"*).
- Inline action receipts — when the agent calls `enter_trade_area`,
  show a one-line "→ opened trade area for Loudoun (50 mi)" in the
  chat directly under its message, so the user can see the chain of
  cause and effect.
- The agent gets to NARRATE *while* tools run, not only after — small
  pre-tool-call sentences like *"Let me check the broadband layer for
  Loudoun first…"*

## Phasing

Each phase is a shippable spec; later phases assume earlier ones.

| Phase | Scope | Approx |
|---|---|---|
| **A — SPEC_095** | Layer 1: `/atlas/session_snapshot` backend (no-op, just echoes); frontend `gatherSessionState()` that walks the UI and builds the block; PilotBody gains `session_state` field; pilot.py prepends it to the system prompt. New `<session_state>` rule in the system prompt teaches the agent to use it. *"What just happened?"* test should now return a recap. | 1 spec |
| **B — SPEC_096** | Layer 2: SESSION_ACTIONS ring buffer (frontend), wired into every state-change site, surfaced in the session-state block as "Recent actions." | 1 spec |
| **C — SPEC_097** | Layer 3: add the missing tools (`select_pin`, `reset_thesis`, `set_thesis_field`, `set_opacity`, `pan_to_place`, `set_fit_weights`, `describe_session`). Each is a small UI tool. | 1 spec |
| **D — SPEC_098** | Layer 4: visible reasoning surface — live "thinking…" chips in the chat thread, inline action receipts, mid-tool narration. | 1 spec |

## Open questions

1. **Layer 1 scope** — should the session-state block include the
   *entire* action history every turn (token-heavy but rich) or just
   the last N events?
2. **Layer 2 location** — frontend localStorage only, or also write
   server-side so the agent can query history with a tool?
3. **Tool surface bloat** — full inventory (~12 new tools) all at
   once, or only the 4-5 highest-impact?
4. **Layer 4 ambition** — full streaming "thinking" panel, or just
   inline action receipts under each assistant turn?

(AskUserQuestion follows.)

## What this *replaces* / changes about earlier specs

- The Pilot's `<thesis>` block (SPEC_082) becomes a sub-section of the
  new `<session_state>` block.
- The "recent events" tool (FEMA/SEC) keeps existing but is *not* the
  default route for "what just happened?" — the agent learns from the
  prompt to recap from `<session_state>` first.
- The story-banner walkthrough (SPEC_092 + SPEC_094) still works
  unchanged — it just becomes one more entry in the action history
  the Pilot can reference.
