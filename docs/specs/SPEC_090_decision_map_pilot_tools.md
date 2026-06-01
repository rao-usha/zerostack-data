# SPEC 090 — Decision Map Phase E: Pilot tools

**Status:** Draft
**Task type:** service (backend + frontend)
**Date:** 2026-05-31
**Plan:** PLAN_075 (Phase E — final)
**Test file:** tests/test_spec_090_decision_map_pilot_tools.py
**Builds on:** SPEC_081 (Pilot) + SPEC_087/088/089 (Decision Map)

## Goal

Give the Pilot tools that drive the Decision Map end-to-end so the
chat *is* the workflow: ask, watch the map shift, react.

New tools:

| Tool | Kind | Purpose |
|---|---|---|
| `recommend_candidates(top_n=5)` | read | Return top-N candidates from the current fit-score (uses the same recipe + thesis + active constraints the frontend has). |
| `add_constraint(dimension, value)` | UI | Push a new pill chip and re-fit. |
| `remove_constraint(dimension)` | UI | Pop a chip by dimension and re-fit. |
| `enter_trade_area(geo_id, radius_mi=50)` | UI | Click a candidate programmatically — opens the Trade Area card. |
| `exit_trade_area()` | UI | Close the Trade Area card. |

## Acceptance Criteria

- [ ] Each tool is registered in `app/services/atlas/pilot_tools.py`
      `TOOLS` with a proper OpenAI tool schema.
- [ ] `recommend_candidates` is a **read** tool: dispatch calls
      `compute_fit_score(db, thesis_context, top_n=N)` and returns
      `{candidates: [...], recipe, weights, total}` (no UI action).
- [ ] `add_constraint`, `remove_constraint`, `enter_trade_area`,
      `exit_trade_area` are **UI** tools: dispatch returns an action
      descriptor `{name, args}` queued for the frontend.
- [ ] Frontend `applyOnePilotAction` knows how to apply each new UI
      action (calls `addChip`, `removeChip`, `enterTradeArea`,
      `exitTradeArea`).
- [ ] System prompt teaches the agent when to use them.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_tools_registered | `TOOLS` includes all 5 by name |
| T2 | test_recommend_candidates_returns_list | with mocked compute_fit_score, dispatch returns a candidates list |
| T3 | test_add_constraint_returns_ui_action | returns `{ok, action: {name: "add_constraint", args}}` |
| T4 | test_remove_constraint_returns_ui_action | likewise |
| T5 | test_enter_trade_area_returns_ui_action | likewise |
| T6 | test_exit_trade_area_returns_ui_action | likewise |
| T7 | test_add_constraint_validates_dimension | unknown dimension → error result |
| T8 | test_frontend_applies_new_ui_actions | atlas.html `applyOnePilotAction` handles all 4 new action names |

## Design

### Backend tool implementations (in `pilot_tools.py`)

```python
def _tool_recommend_candidates(db, top_n=5, thesis_context=None):
    from app.services.atlas.fit_score import compute_fit_score
    top_n = max(1, min(20, int(top_n or 5)))
    r = compute_fit_score(db, thesis=thesis_context or {}, top_n=top_n)
    return {
      "candidates": r.get("top_n", []),
      "recipe": r.get("recipe"),
      "weights": r.get("weights", []),
      "total_candidates": r.get("total_candidates", 0),
    }

_VALID_DIMENSIONS = {
  "hhi_min","hhi_max","establishments_min","broadband_min","exclude_nri"
}
def _tool_add_constraint(db, dimension, value):
    if dimension not in _VALID_DIMENSIONS:
        return {"error": f"unknown dimension {dimension!r}; "
                          f"valid: {sorted(_VALID_DIMENSIONS)}"}
    try: v = float(value)
    except (TypeError, ValueError):
        return {"error": "value must be numeric"}
    return {"ok": True,
            "action": {"name": "add_constraint",
                        "args": {"dimension": dimension, "value": v}},
            "note": f"Constraint {dimension}={v} queued."}

def _tool_remove_constraint(db, dimension):
    if dimension not in _VALID_DIMENSIONS:
        return {"error": f"unknown dimension {dimension!r}"}
    return {"ok": True,
            "action": {"name": "remove_constraint",
                        "args": {"dimension": dimension}},
            "note": f"Removed constraint on {dimension}."}

def _tool_enter_trade_area(db, geo_id, radius_mi=50):
    rm = max(1.0, min(250.0, float(radius_mi or 50)))
    return {"ok": True,
            "action": {"name": "enter_trade_area",
                        "args": {"geo_id": str(geo_id), "radius_mi": rm}},
            "note": f"Trade-area mode queued for {geo_id} ({rm} mi)."}

def _tool_exit_trade_area(db):
    return {"ok": True,
            "action": {"name": "exit_trade_area", "args": {}},
            "note": "Exit trade-area queued."}
```

`recommend_candidates` is **read** kind; the rest are **ui**.

Recall: the streaming dispatcher passes `thesis_context` through to
read tools via the existing pipeline — confirm this is wired (else
add a thesis_context arg path).

### Frontend `applyOnePilotAction` extension

```js
case 'add_constraint':
  if (typeof addChip === 'function')
    addChip(args.dimension, args.value);
  break;
case 'remove_constraint':
  if (typeof removeChip === 'function') {
    const c = CONSTRAINTS.find(x => x.dimension === args.dimension);
    if (c) removeChip(c.id);
  }
  break;
case 'enter_trade_area':
  if (typeof enterTradeArea === 'function')
    enterTradeArea(args.geo_id, args.radius_mi || 50);
  break;
case 'exit_trade_area':
  if (typeof exitTradeArea === 'function') exitTradeArea();
  break;
```

### System prompt addition

> The map now operates as a Decision Map. You have these tools to drive
> it: `recommend_candidates`, `add_constraint`, `remove_constraint`,
> `enter_trade_area`, `exit_trade_area`. When the user asks "where
> should I open this?", prefer `recommend_candidates` + then
> `enter_trade_area(top_candidate.geo_id)` over raw `toggle_layer`.
> Use `add_constraint` to apply must-haves from the user's words
> ("only counties above $80K" → `add_constraint("hhi_min", 80000)`).

## Files

| File | Action |
|------|--------|
| `app/services/atlas/pilot_tools.py` | Add 5 tools + schemas |
| `app/services/atlas/pilot.py` | Append the new system-prompt block |
| `frontend/atlas.html` | Extend `applyOnePilotAction` with 4 new cases |
| `tests/test_spec_090_decision_map_pilot_tools.py` | T1-T8 |

## Verification

1. Ask Pilot: "Show me where to open a furniture store, only above $80K
   HHI and skip high-NRI counties."
2. Expected: agent calls `add_constraint(hhi_min=80000)` +
   `add_constraint(exclude_nri=50)`, then `recommend_candidates(5)`,
   then `enter_trade_area(top.geo_id)`. The chip strip fills, the
   counter shrinks, the trade-area card opens — all driven by chat.
