# SPEC 098 — Visible reasoning surface (PLAN_078 Layer 4)

**Status:** Draft
**Date:** 2026-06-01
**Builds on:** SPEC_086 (chat) · SPEC_090/091/097 (tool calls) · SPEC_093 (CoT)
**Test file:** tests/test_spec_098_visible_reasoning.py

## Goal

Make the Pilot's chain of cause-and-effect legible inside the chat
thread:
- A live "thinking…" chip appears under the active assistant turn
  while a tool call is running (e.g. *"🔧 Reading 168 fit-score
  candidates…"*).
- Inline action receipts append under the assistant turn after each
  tool completes (e.g. *"→ opened trade area for Loudoun (50 mi)"*).
- Receipts stay visible after the run finishes so the user sees the
  whole chain of moves the agent made.

## Acceptance

- [ ] `.msg.assistant .thinking-chip` element renders during
      `tool_call_started`; updates with the tool name + truncated args.
- [ ] On `tool_call_completed`, the chip is replaced by a permanent
      `.cause-receipt` line under the narration.
- [ ] Receipts have an icon column: `🔧` for read tools, `🎬` for UI.
- [ ] Tooltip on hover shows the full args + result summary.
- [ ] Thinking chip cleared on `done`/`error` if still present.
- [ ] No regression to existing narration/options rendering.

## Test cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_thinking_chip_class_present | CSS `.thinking-chip` declared |
| T2 | test_cause_receipt_class_present | CSS `.cause-receipt` declared |
| T3 | test_handler_creates_thinking_chip | handlePilotEvent on tool_call_started appends a `.thinking-chip` to `c.narrEl`'s parent |
| T4 | test_handler_replaces_chip_with_receipt | on tool_call_completed the chip is replaced with `.cause-receipt` |
| T5 | test_handler_clears_chip_on_done | done removes any lingering chip |

## Design

- Reuse existing `tool_call_started` / `tool_call_completed` /
  `done` events from SPEC_079.
- New per-turn refs on `collected`: `thinkingEl` (current chip),
  `receiptsEl` (container for receipts under the narration).
- `appendPilotTurn` adds a `.receipts` div inside the assistant
  message and captures it.
- handlePilotEvent:
  - `tool_call_started`: create / update `c.thinkingEl` with
    "🔧 (or 🎬) tool_name(args…)" + soft pulse animation.
  - `tool_call_completed`: clear `c.thinkingEl`; append a `.cause-
    receipt` row to `c.receiptsEl` with the tool name + summary.
  - `done`: ensure `c.thinkingEl` is gone (already cleared by last
    completion; defensive only).

## Files

- `frontend/atlas.html` — CSS + handler changes + appendPilotTurn
- `tests/test_spec_098_visible_reasoning.py`
