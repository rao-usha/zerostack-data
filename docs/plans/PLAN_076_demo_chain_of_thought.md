# PLAN 076 — Decision Map demo: automated chain-of-thought reasoning

**Date:** 2026-06-01
**Status:** Draft (awaiting direction)
**Builds on:** PLAN_075 (Decision Map) + SPEC_092 (storytelling demo)
**Triggered by:** User feedback —
> "I do like this, this is a great improvement, but I would like to see
> as part of this is an automated chain of thought reasoning. Can you
> create a plan to make this work better."

## What today's demo does — and doesn't

SPEC_092 ships a 6-beat scripted walkthrough. Each beat shows a short
banner label ("Step 4 / 6 · Scoring 3,248 counties…") and runs one
deliberate action. That made the *what* visible.

It does **not** show the **why**:
- "Why HHI ≥ $75K and not $60K?" → silent threshold pick.
- "Why exclude NRI > 50?" → silent.
- "Why is Loudoun #1 and Santa Clara #3?" → silent.
- "Why this trade-area radius?" → silent.

The user can see the system moving but can't see it *thinking*. The
ask: surface a real reasoning stream as part of the walkthrough.

## Goal

Make each demo step show **the reasoning behind the action**, not just
a label. Stretch goal: the reasoning is actually authored by the LLM
in real time, not pre-written.

## Four framework options

The space is well-defined. Pick one (or a hybrid).

### Option A — Pre-written rationales

Each scripted beat gets a hand-authored 2-3 sentence rationale (e.g.,
"Furniture is a discretionary purchase, so I'm filtering for HHI ≥ $75K
— that's roughly the 60th US household-income percentile, the
threshold where home-furnishing purchases of $1K+ become routine.").
The banner gets a `▾ Reasoning` toggle that expands the rationale.

- **Pros:** Fast, deterministic, $0 per demo run, predictable timing,
  copywritten to be sharp.
- **Cons:** Not actually "automated" reasoning. Same rationale every
  time. Doesn't generalise to user-authored theses — only the
  furniture demo.

### Option B — Pilot CoT streamed alongside the scripted beats

The 6 scripted beats keep their timing. In parallel, the Atlas Pilot
streams narration into the chat panel, one paragraph per beat,
authored live by the LLM with the active thesis as context. The story
banner stays short ("Step 4 · Scoring") and the chat shows the
running CoT.

- **Pros:** Real LLM reasoning. Predictable rhythm (scripted spine).
  Works for any user-authored thesis on later iterations. Adds value
  to the chat panel.
- **Cons:** LLM token cost per demo (~6 short generations). Two
  surfaces to read (banner + chat) — slight cognitive load.

### Option C — Fully Pilot-driven (demo = prompt the Pilot)

⚡ Demo loads the thesis silently, then submits a fixed prompt to the
Pilot — *"Walk me through evaluating this thesis step by step,
narrating your reasoning as you go and using add_constraint /
recommend_candidates / enter_trade_area to drive the map."* — and the
Pilot does **everything**: picks the chips, the order, the
candidates, the radius. The scripted beats go away.

- **Pros:** Most authentic — actual automated reasoning. Same code
  path that handles any user question. Generalises perfectly.
- **Cons:** Less predictable rhythm; the agent might call too many
  tools, or pick odd thresholds, or take 40+ seconds. Harder to
  guarantee the demo always looks crisp. Tighter prompt engineering
  required.

### Option D — Planner + Executor split

Two-phase:
1. **Plan** — one LLM call returns a structured `Plan` JSON:
   ```json
   { "steps": [
     { "title": "...", "rationale": "...", "tool": "add_constraint",
       "args": {"dimension":"hhi_min","value":75000} },
     ... ]
   }
   ```
2. **Execute** — the demo runner walks the steps, showing each step's
   `title` in the banner and the `rationale` in a CoT panel, then
   invoking the named tool.

- **Pros:** Real LLM reasoning AND predictable rhythm (the plan is
  fully known before execution starts). The plan itself becomes a
  reusable artifact (savable, replayable, shareable). Lets us show
  the user *the whole reasoning tree at a glance* if we want.
- **Cons:** One extra LLM round-trip up front (~3-6s wait before any
  visible motion). New schema to maintain. More moving pieces.

## Recommendation

**Option B (live CoT alongside scripted beats)** for v1, with **D**
(plan-then-execute) as the long-term pattern.

Rationale:
- B is the cheapest way to get *real* reasoning in front of the user
  with the lowest risk of "the demo looks weird this time."
- B uses the existing Pilot streaming infrastructure (SPEC_079) and
  the existing chat-thread render — no new chrome to design.
- B preserves the scripted-beat rhythm SPEC_092 just shipped (which
  the user explicitly said they like).
- The path from B → D is incremental: once we have the streaming
  CoT working, swapping to a planner-first execution is a 1-day
  refactor.

## What B would look like

```
                ┌─────────────────────────────────────────┐
                │  Step 2 / 6 · Filtering: HHI ≥ $75K    │
                │  ●●○○○○                       Skip ▶︎   │
                └─────────────────────────────────────────┘
                                  MAP
   ┌──────────────────────────────────────────────────────────┐
   │ THESIS panel              MAP             CHAT (right)   │
   │   Furniture stores                        Pilot:          │
   │                                           Furniture is a  │
   │  [HHI ≥ $75K ×]  …                        discretionary   │
   │                                           purchase. The   │
   │                                           60th-percentile │
   │                                           US HHI cut-off  │
   │                                           is ~$75K — below│
   │                                           that, $1K+      │
   │                                           home-furnishing │
   │                                           purchases drop  │
   │                                           sharply.        │
   └──────────────────────────────────────────────────────────┘
```

The chat panel becomes the CoT log: each beat appends a streamed
paragraph from the Pilot, anchored to that beat's action.

## Implementation sketch (Option B)

1. **New Pilot tool** `narrate_demo_step(step, total, title)` (read,
   no side-effects). The runner calls it before each scripted action;
   the Pilot's reply IS the rationale. We get the streaming events
   for free.
2. **Demo runner change** — `storyBeat()` gets an `narrate` flag.
   When set, the runner POSTs to `/pilot/stream` with a context-aware
   prompt and pipes the streamed narration into the chat as a new
   assistant turn. The scripted action runs once the LLM's narration
   completes (or after a 4 s ceiling — whichever first).
3. **No new UI** — uses the existing chat thread (SPEC_086).
4. **Cancellation** — Skip ▶︎ aborts the in-flight Pilot fetch via
   the same AbortController pattern askPilot already uses.
5. **Token budget** — system prompt addition that caps each
   per-step narration at ~80 words. LLM call cost: ~6 small calls.

### Phasing

| Phase | Scope |
|---|---|
| **B1** | Wire pre-step narration to a short Pilot prompt; render in chat thread; keep scripted beats. |
| **B2** | Tune prompts per beat (chip rationale, score rationale, trade-area rationale). |
| **B3** | Add a `▾ Reasoning` collapse on the banner that mirrors the chat paragraph (for users not looking at chat). |
| **D**  | Future — swap to planner-first (one big LLM call returns a Plan, then execute). |

## Open questions (need user direction)

1. **Which option** — A, B, C, or D?
2. **Where should reasoning render** — only in the chat thread, only
   below the banner, or both?
3. **Token spend** — fine to call the LLM 6× per demo run? Or want
   it cached after the first run?

(See AskUserQuestion that follows.)

## Files (when approved)

- `app/services/atlas/pilot_tools.py` — new `narrate_demo_step` tool
- `app/services/atlas/pilot.py` — system-prompt CoT block + word cap
- `frontend/atlas.html` — runner change, chat-thread plumbing,
  optional banner collapse
- `tests/test_spec_093_demo_cot.py`
- `docs/specs/SPEC_093_demo_chain_of_thought.md`
