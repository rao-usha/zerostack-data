# SPEC 096 — Action history ring buffer (PLAN_078 Layer 2)

**Status:** Draft
**Date:** 2026-06-01
**Builds on:** SPEC_095
**Test file:** tests/test_spec_096_action_history.py

## Goal

A live ring buffer of the last 50 actions (user / pilot / planner)
sent as part of the `<session_state>` block so the agent can recap
*timeline* questions like "what did we do 2 minutes ago?" or "show me
my history."

## Acceptance

- [ ] Frontend `SESSION_ACTIONS = []` global, cap 50; persists to
      localStorage `atlas_session_actions_v1`.
- [ ] `logAction(actor, kind, detail)` helper appends
      `{ts, actor, kind, detail}` and saves.
- [ ] State-change sites wired: addChip, removeChip, clearAllChips,
      enterTradeArea, exitTradeArea, saveThesis, doResetThesis,
      loadFurnitureDemo / fetchAndRunPlan, askPilot,
      applyPlanBeat, applyOnePilotAction.
- [ ] `gatherSessionState()` includes `recent_actions: last 10`.
- [ ] Backend `_format_session_state_block` renders a `Recent actions:`
      section with relative timestamps ("30s ago", "2 min ago").

## Test cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_format_session_block_recent_actions | block contains "Recent actions:" with relative ts |
| T2 | test_format_session_block_action_truncated | overlong detail truncated |
| T3 | test_format_session_block_no_actions_no_section | empty `recent_actions` omits the section |
| T4 | test_relative_ts_under_minute | `ts ≈ now-30s` → "30s ago" |
| T5 | test_relative_ts_minutes | `ts ≈ now-180s` → "3 min ago" |
| T6 | test_frontend_log_action_present | atlas.html declares `logAction` + `SESSION_ACTIONS` |
| T7 | test_frontend_action_log_persists | localStorage key `atlas_session_actions_v1` referenced |
| T8 | test_frontend_actions_wired_into_chips | addChip + removeChip both call logAction |

## Design

Action record:
```js
{ ts: Date.now(), actor: 'user'|'pilot'|'planner', kind: 'chip_added',
  detail: 'HHI ≥ $75K' }
```

Backend rendering (relative timestamp via `(now - ts_ms) // 1000`):
```
Recent actions (last 10):
  T-15s    user      asked: "what just happened?"
  T-1 min  pilot     called enter_trade_area for 51107
  T-1 min  pilot     called recommend_candidates (top_n=10)
  T-2 min  planner   beat 4 add_constraint exclude_nri 50
  T-2 min  planner   beat 3 add_constraint hhi_min 75000
```

Backend treats incoming actions as `[{ts, actor, kind, detail}]`.
Caps each detail at 120 chars. Skips malformed entries silently.
