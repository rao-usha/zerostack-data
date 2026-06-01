# SPEC 095 — Pilot session-state injection (Layer 1 of PLAN_078)

**Status:** Draft
**Task type:** service (backend + frontend)
**Date:** 2026-06-01
**Plan:** PLAN_078_context_aware_copilot (Layer 1 / 4)
**Test file:** tests/test_spec_095_session_state_injection.py
**Builds on:** SPEC_081/082/086 (Pilot) · SPEC_087-094 (Decision Map)

## User decisions (locked from PLAN_078)

- Layer 1 only — session-state injection.
- **Aggressive snapshot** (~800 tokens): full thesis, all chips with
  thresholds, top-10 pin list with name + score, current trade area
  focal + summary + 5 closest neighbours, current map view.
- Action history deferred to Layer 2 (SPEC_096).

## Goal

Every Pilot call ships with a `<session_state>` block prepended to the
system prompt, so the agent sees exactly what the user is looking at
before it speaks. Closes the *"can you see what just happened?"* gap.

## Acceptance Criteria

- [ ] `PilotBody.session_state: Optional[Dict[str, Any]]` accepted by
      both `/pilot` and `/pilot/stream`.
- [ ] `app/services/atlas/pilot.py`: new `_format_session_state_block(
      session_state)` returns `<session_state>...\n</session_state>\n\n`
      or `""` when empty/None.
- [ ] Both `run_pilot` and `run_pilot_streaming` prepend the session-
      state block BEFORE the existing `<thesis>` block (which becomes a
      nested sub-section of state going forward, but for v1 they're
      sibling blocks).
- [ ] System prompt gains a `9d. SESSION STATE` rule teaching the agent
      to recap from `<session_state>` first when asked recap-flavoured
      questions ("what just happened?", "what am I looking at?",
      "what did we do?", etc.).
- [ ] Frontend `gatherSessionState()` returns the aggressive snapshot:
      `{thesis, chips, top_pins, trade_area, map_view}`.
- [ ] `askPilot` (and the streaming fetch) include `session_state` in
      the body on every call.
- [ ] Sanitiser caps every string field (length); rejects non-dict
      input; returns empty block when no usable state.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_pilot_body_session_state_optional | absent field defaults to None |
| T2 | test_pilot_body_session_state_present | populated dict round-trips |
| T3 | test_format_session_state_block_empty | None / `{}` → "" |
| T4 | test_format_session_state_block_thesis | thesis fields rendered |
| T5 | test_format_session_state_block_chips | chip list rendered with thresholds |
| T6 | test_format_session_state_block_top_pins | top-N rendered with rank + name + score |
| T7 | test_format_session_state_block_trade_area | focal + summary + 5 neighbours rendered |
| T8 | test_format_session_state_block_map_view | zoom + center coords rendered |
| T9 | test_format_session_state_block_caps_strings | overlong fields truncated |
| T10 | test_system_prompt_has_session_state_rule | SYSTEM_PROMPT contains "session_state" guidance |
| T11 | test_frontend_gather_session_state_present | atlas.html declares gatherSessionState |
| T12 | test_frontend_pilot_calls_send_session_state | both pilot fetches include session_state in body |

## Design

### Backend — session-state block format

```
<session_state>
Thesis:
  Industry: Furniture stores (NAICS 442110)
  Region focus: Austin, TX metro
  Target HHI: $75,000+
  Target age band: 25-44
  Notes: Mid-to-high-end home furnishings...

Active filters (chips): HHI ≥ $75K · Exclude high-NRI (NRI ≤ 50)
Counter: 716 of 3,248 candidates passing the filters.

Active layer: Decision Map fit-score
  Recipe: retail (55% income · 30% commercial · 15% broadband)

Top 10 candidates on map:
  1. Loudoun County, VA — fit 70
  2. Los Angeles County, CA — fit 67
  3. Santa Clara County, CA — fit 67
  4. Fairfax County, VA — fit 63
  5. San Mateo County, CA — fit 63
  6. ...

Current trade area: Loudoun County, VA · 50 mi radius
  Focal stats: income $178,707 · 7,500 establishments · broadband 92% · NRI 25
  Aggregate of 25 neighbour counties: avg income $107,586 · avg broadband 91.5% · max NRI 97.6
  Closest neighbours:
    - Jefferson County, WV (12.9 mi) — income $95,523
    - Frederick County, MD (18.5 mi) — income $120,458
    - Clarke County, VA (21.6 mi) — income $114,185
    - Montgomery County, MD (23.7 mi) — income $128,733
    - Fairfax City, VA (28.4 mi) — income $132,774

Map view: zoom 8, center 39.09°N -77.64°W
</session_state>
```

Each section is omitted when empty. Aggressive but bounded.

### System prompt addition (9d)

> **9d. SESSION STATE.** Every turn, you are given a `<session_state>`
> block that describes EXACTLY what the user is currently looking at:
> their thesis, active filters, top map pins, current trade area,
> current view. When the user asks "what just happened?", "what am I
> looking at?", "what did we do?", "summarize", or any similar recap
> intent, your FIRST move is to narrate from `<session_state>` — do
> NOT call `get_recent_events`, `get_migration_flows`, or any other
> generic "recent data" tool for that intent. Those tools serve
> external-world questions ("what disasters happened in the US
> recently?"), not session recap.
>
> When a user says "this" or "here", resolve it against the active
> trade area focal or the top pin. Don't ask for clarification you
> can derive from the state.

### Frontend `gatherSessionState()` shape

```js
function gatherSessionState() {
  const t = loadThesis();
  return {
    thesis: t,                                 // SPEC_082 shape
    chips: (CONSTRAINTS || []).map(c => ({
      dimension: c.dimension, value: c.value, label: c.label,
    })),
    fit_result: CURRENT_FIT_RESULT ? {
      recipe: CURRENT_FIT_RESULT.recipe,
      weights: CURRENT_FIT_RESULT.weights,
      total_candidates: CURRENT_FIT_RESULT.total_candidates,
      filtered_candidates: CURRENT_FIT_RESULT.filtered_candidates,
    } : null,
    top_pins: (CURRENT_FIT_RESULT && CURRENT_FIT_RESULT.top_n || [])
      .slice(0, 10).map((p, i) => ({
        rank: i + 1, geo_id: p.geo_id, name: nameForGeo(p.geo_id),
        score: p.score,
      })),
    trade_area: TRADE_AREA_STATE && CURRENT_TRADE_AREA_DATA ? {
      focal:    CURRENT_TRADE_AREA_DATA.focal,
      summary:  CURRENT_TRADE_AREA_DATA.summary,
      radius_mi: TRADE_AREA_STATE.radius_mi,
      neighbors: (CURRENT_TRADE_AREA_DATA.neighbors || []).slice(0, 5),
    } : null,
    map_view: MAP ? {
      zoom: MAP.getZoom(),
      lat:  MAP.getCenter().lat,
      lon:  MAP.getCenter().lng,
    } : null,
  };
}
```

`CURRENT_TRADE_AREA_DATA` is new — cached on `enterTradeArea`.

### Sanitiser

```python
def _format_session_state_block(s):
    if not isinstance(s, dict):
        return ""
    parts = []
    # Thesis (delegate to existing helper)
    t = s.get("thesis")
    if t: parts.append(_format_thesis_for_session(t))   # short form
    # Chips
    chips = s.get("chips") or []
    if chips:
        labels = [str(c.get("label") or c.get("dimension"))[:60] for c in chips[:8]]
        parts.append("Active filters (chips): " + " · ".join(labels))
    # Counter
    fit = s.get("fit_result") or {}
    if fit.get("filtered_candidates") is not None:
        parts.append(f"Counter: {fit['filtered_candidates']:,} of "
                      f"{fit.get('total_candidates', 0):,} candidates "
                      "passing the filters.")
    # ...top pins / trade area / map view similar
    if not parts:
        return ""
    return "<session_state>\n" + "\n\n".join(parts) + "\n</session_state>\n\n"
```

All strings capped (name ≤80, label ≤60); numeric formats use locale-
free comma separators; floats rounded to sane precision.

## Files

| File | Action |
|------|--------|
| `app/services/atlas/pilot.py` | `_format_session_state_block`; both run_pilot and run_pilot_streaming accept + inject; new prompt rule 9d |
| `app/api/v1/atlas.py` | `PilotBody.session_state: Optional[Dict[str, Any]]` |
| `frontend/atlas.html` | `gatherSessionState()`; `CURRENT_TRADE_AREA_DATA`; both pilot fetches include the state |
| `tests/test_spec_095_session_state_injection.py` | T1-T12 |

## Verification

1. Click ⚡ Demo, let it run (top pins land, trade area opens).
2. Type *"can you see what just happened?"* into the Pilot chat.
3. Expected: narration references YOUR thesis, YOUR chips, YOUR top
   pins (by name), YOUR trade area (Loudoun + neighbour counts), NOT
   a FEMA disaster recap.

## Out of scope (deferred)

- Action history log (SPEC_096 — Layer 2)
- New tools: select_pin, set_fit_weights, etc. (SPEC_097 — Layer 3)
- Live "thinking…" chips + inline receipts (SPEC_098 — Layer 4)
