# SPEC 086 — Map data-presentation system + Cursor-style Pilot chat

**Status:** Draft
**Task type:** bug_fix + service (frontend)
**Date:** 2026-05-28
**Plan:** PLAN_074_atlas_vscode_shell_rev_04
**Test file:** tests/test_spec_086_atlas_map_presentation.py
**Builds on:** SPEC_080 (zoom-adaptive) · SPEC_082 (shell) · SPEC_085 (Pilot)

## Goal

Make map data legible and Pilot a real Cursor-style chat:
- Fix the overlay-kills-fill bug; taper fill on zoom instead of cutting
  to 0; lighter default opacity; semantic per-layer color ramps; lighter
  chrome.
- Rework the right-column Pilot into a Cursor-style chat: message thread
  that grows downward, composer pinned at the bottom, header "New chat".

## User decisions (locked)

- **Color ramps:** semantic per-layer (income/"more" → teal→gold;
  risk → green→amber→red diverging; population → violet sequential).
- **Chat:** "just like Cursor" — right panel, bottom composer, message
  thread, minimal (no avatars/heavy bubbles).

## Acceptance Criteria

### Map
- [ ] **Overlay coexistence:** adding/removing a point overlay never
      makes the choropleth fill vanish. Both render together.
- [ ] **Zoom taper, not cut:** fill opacity tapers with zoom to a floor
      (~0.15), never hard 0; outline sharpens at high zoom.
- [ ] **One owner for fillOpacity:** the load transition fades *to* the
      zoom-correct opacity (styleFeature owns it; transition scales it).
- [ ] **Lighter default:** base fill opacity default ~0.40 (slider still
      overrides).
- [ ] **Semantic ramps:** `rampFor(spec)` returns a 5-color ramp by
      layer polarity/domain; legend swatches reflect the active ramp.
- [ ] **Lighter chrome:** thinner default borders; calmer hover (no
      jarring full-cyan flash).

### Chat
- [ ] Composer (textarea + send) pinned to the BOTTOM of `#right-col`;
      Enter sends, Shift+Enter newlines.
- [ ] Conversation renders as a thread in `#pilot-thread`: user turn
      (subtle boxed) then assistant turn (plain prose), newest at the
      bottom, auto-scroll.
- [ ] Options render as buttons under the latest assistant turn; clicking
      continues the conversation (keep_history) as a new thread turn.
- [ ] Header shows "Atlas Pilot" + "New chat" (clears thread + history
      conversation).
- [ ] Global-header Pilot input (`#pilot-input`/`#pilot-go` in
      `#search-wrap`) removed; fast-travel search stays.
- [ ] Tool log still streams to the bottom inspector (SPEC_085); chat
      stays clean.

## Test Cases (HTML/JS structure + backend untouched)

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_ramp_function_present | `function rampFor` exists; defines teal/risk/population ramps |
| T2 | test_default_opacity_lighter | base opacity default ≤ 0.45 (not 0.55) |
| T3 | test_zoom_taper_has_floor | high-zoom branch no longer sets fillOpacity to 0 |
| T4 | test_transition_single_owner | transition no longer hardcodes `state.opacity * eased` over styleFeature |
| T5 | test_composer_at_bottom | `#pilot-composer` exists in `#right-col`; contains `#pilot-input` + `#pilot-go` |
| T6 | test_header_input_removed | `#search-wrap` no longer contains `#pilot-input` |
| T7 | test_thread_container | `#pilot-thread` present; renderPilotTurn / appendUserMsg helpers exist |
| T8 | test_new_chat_control | "New chat" control wired to clear thread + conversation |

## Design Notes

### Semantic ramps

```js
const RAMPS = {
  more:  ['#0b3d4d','#14708a','#1e9bb5','#56c1c9','#ffd166'], // teal→gold
  risk:  ['#2a9d8f','#8ab17d','#e9c46a','#f4a261','#e76f51'], // green→red
  pop:   ['#2e1065','#5b21b6','#7c3aed','#a78bfa','#ddd6fe'], // violet
};
function rampFor(spec) {
  const id = (spec?.layer_id || spec?.id || '').toLowerCase();
  const dom = (spec?.domain || '').toLowerCase();
  if (/nri|fema|disaster|risk|hazard|flood/.test(id + dom)) return RAMPS.risk;
  if (/pop|population|density/.test(id + dom))             return RAMPS.pop;
  return RAMPS.more;
}
```

`ACTIVE_RAMP` is set in `applyLayerToBoundaries`; `bucketColor` reads it.
Legend swatches render from `ACTIVE_RAMP`.

### Zoom taper (styleFeature)

```js
// base from slider; taper toward a floor as we zoom in, sharpen outline
const z = MAP.getZoom();
let fill = state.opacity;          // default ~0.40
let weight = 0.5, color = 'rgba(255,255,255,.10)';
if (z >= 10)      { fill = Math.max(0.15, state.opacity * 0.45); weight = 1.6; color = outlineTint; }
else if (z >= 8)  { fill = state.opacity * 0.7;  weight = 0.8; }
layer.setStyle({ fillColor, fillOpacity: fill * opacityScale, weight, color });
```

`styleFeature(feature, layer, opacityScale=1)`; the load transition calls
`styleFeature(f, fl, eased)` instead of overwriting fillOpacity itself.

### Cursor-style chat DOM

```html
<aside id="right-col">
  <div id="pilot-panel" class="show">
    <div id="pilot-head">
      <span class="title">Atlas Pilot</span>
      <button id="pilot-newchat">New chat</button>
    </div>
    <div id="pilot-thread"></div>          <!-- grows down, scrolls -->
    <div id="pilot-tools"  style="display:none"></div>  <!-- mirror src -->
    <div id="pilot-cites"  style="display:none"></div>
    <div id="pilot-meta"   style="display:none"></div>
    <div id="pilot-composer">
      <textarea id="pilot-input" rows="1"
                placeholder="Ask the Atlas Pilot…"></textarea>
      <button id="pilot-cancel">Cancel</button>
      <button id="pilot-go" title="Send">↑</button>
    </div>
  </div>
</aside>
```

Per turn appended to `#pilot-thread`:
```html
<div class="msg user"><div class="bubble"></div></div>
<div class="msg assistant">
  <div class="loading">…</div>
  <div class="narration"></div>
  <div class="options"></div>
  <div class="meta"></div>
</div>
```

`askPilot` builds the user + assistant turn, stores element refs on
`collected` (`c.narrEl`, `c.optionsEl`, `c.loadingEl`, `c.metaEl`);
`handlePilotEvent` writes to those refs (not global ids). Auto-scroll
`#pilot-thread` to bottom after each event.

## Files to Modify

| File | Action |
|------|--------|
| `frontend/atlas.html` | ramps, opacity, taper, transition fix, overlay coexistence, chrome, chat thread + composer, remove header input |
| `docs/specs/SPEC_086_atlas_map_presentation.md` | this file |
| `tests/test_spec_086_atlas_map_presentation.py` | T1-T8 |

## Verification

1. Load income layer; basemap stays readable (lighter fill); ramp reads
   teal→gold.
2. Switch to NRI → green→red diverging ramp.
3. Zoom into a metro → fill tapers (still visible), outlines sharpen.
4. Add a point overlay → choropleth fill stays; points sit on top.
5. Pilot: composer at the bottom; ask a question → user bubble then
   assistant reply appear in-thread; map moves; options under the reply;
   click one → next turn appends below; "New chat" clears it.

## Feedback History

- 2026-05-28 — Driving feedback (overlay bug, colors/opacity, "create a
  plan on data presentation", "chat like Cursor, input not at top, tool
  calls navigate map"). See `memory/feedback/corrections.md` 2026-05-28.
