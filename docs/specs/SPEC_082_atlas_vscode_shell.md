# SPEC 082 — Atlas VS-Code-style workspace shell (Phase A)

**Status:** Draft
**Task type:** service (mostly frontend) + minor backend
**Date:** 2026-05-27
**Plan:** PLAN_074_atlas_vscode_shell
**Test file:** tests/test_spec_082_atlas_vscode_shell.py
**Builds on:** SPEC_078 · SPEC_079 · SPEC_080 · SPEC_081

## Goal

Replace the Atlas page's fixed three-column grid with a VS-Code-style
workspace: 40-px activity bar + left panel + map + right chat +
bottom inspector. Move the floating Pilot into a dedicated right
column. Add four left views (Thesis / Layers / Places / History).
Make all gutters draggable and panel widths persistent.

## Acceptance Criteria

- [ ] `#app` grid replaced with new 5-region layout
      (`activity left map right`, with optional bottom row).
- [ ] Activity bar shows 4 icons: Thesis · Layers · Places · History.
      Click switches the left panel view; click the active icon
      collapses the left panel.
- [ ] Right column is always-visible Pilot chat (replaces floating
      `#pilot-panel`). All existing Pilot features still work
      (streaming, guided-tour options, cancel, history, citations).
- [ ] Bottom panel toggleable with 3 tabs: Tool Log · Citations ·
      Queries. The latter shows each tool call's args+result as
      expandable JSON rows.
- [ ] All three gutters (activity-left / left-map / map-right /
      top-bottom) draggable. Widths persist in localStorage key
      `atlas_shell_v1`.
- [ ] **Thesis view** — form with NAICS code, target HHI min/max,
      target population density, target age band, exclude layers.
      Saves to localStorage `atlas_thesis_v1`. A "Save" button
      flashes confirmation.
- [ ] Pilot endpoints accept optional `thesis_context: Dict` body
      field; frontend sends it on every Pilot call when populated;
      backend prepends a compact `<thesis>...</thesis>` block to
      the system prompt.
- [ ] **Layers view** — same layer toggles as today, grouped by
      domain headings (Demographics / Industry / Federal $ / Risk /
      Climate / Logistics).
- [ ] **Places view** — when user clicks a county, it's added to a
      pinned list (capped at 20). Click a pinned place to re-zoom
      and re-render its facts. Delete + star actions.
- [ ] **History view** — same data as the floating history strip,
      formatted as a list of past questions with timestamp +
      re-ask button.
- [ ] **Status bar** at bottom: toggle-left | toggle-bottom |
      last-Pilot-summary | active-layer-count.
- [ ] After any sash drag, `MAP.invalidateSize()` fires so the map
      reflows.
- [ ] Mobile (`<1000px`): activity bar collapses to top tab strip;
      bottom panel hidden by default; right chat is a bottom sheet.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_pilot_accepts_thesis_context | `/atlas/pilot` POST with `thesis_context` returns 200; doesn't crash |
| T2 | test_thesis_context_in_system_prompt | When `thesis_context` is non-empty, the prompt includes a `<thesis>` block |
| T3 | test_thesis_context_omitted_when_empty | Empty / None thesis → no `<thesis>` block, no token waste |
| T4 | test_pilot_body_thesis_optional | `PilotBody` accepts no `thesis_context` (backwards compat) |
| T5 | test_thesis_context_sanitized | Excessively long fields are truncated; non-string values rejected |

Frontend behaviour is verified manually (see Verification below) —
DOM tests would need a headless browser harness not currently set up.

## Design Notes

### Grid layout

```css
#app {
  display: grid;
  height: 100vh;
  grid-template-rows: 48px 1fr auto 24px;
  grid-template-columns: 40px var(--left-w, 300px) 1fr var(--right-w, 380px);
  grid-template-areas:
    "header   header  header  header"
    "activity left    map     right"
    "activity bottom  bottom  right"
    "status   status  status  status";
}
#app.no-left   { grid-template-columns: 40px 0 1fr var(--right-w); }
#app.no-bottom { grid-template-rows: 48px 1fr 0 24px; }
```

CSS variables let drag handlers mutate widths live; values are
written to localStorage on drag-end.

### Activity bar

Single column of 4 buttons (vertical icons). Active state = the
view currently rendered. Click active → collapse. Bottom of bar
has a small "?" help button.

```html
<aside id="activity-bar">
  <button data-view="thesis"  class="act on">📋</button>
  <button data-view="layers"  class="act">▦</button>
  <button data-view="places"  class="act">📍</button>
  <button data-view="history" class="act">🕘</button>
</aside>
```

### Thesis context shape (sent to Pilot)

```json
{
  "industry_naics": "442110",
  "industry_label": "Furniture stores",
  "target_hhi_min": 75000,
  "target_hhi_max": null,
  "target_pop_density_min": null,
  "target_age_band": "25-44",
  "exclude_layers": ["nri_risk"],
  "notes": "Looking for affluent urban tracts in TX metros."
}
```

Backend prepends (only populated fields):
```
<thesis>
Industry: Furniture stores (NAICS 442110)
Target HHI: $75K+
Target age band: 25-44
Notes: Looking for affluent urban tracts in TX metros.
</thesis>
```

Sanitisation:
- Numeric fields cast to int or dropped
- String fields stripped + capped at 200 chars
- Notes capped at 800 chars
- Whole block dropped if every field is empty/None

### Sash resize

```js
const SASHES = [
  {id:'sash-left',  axis:'x', cssVar:'--left-w',  min:240, max:480},
  {id:'sash-right', axis:'x', cssVar:'--right-w', min:320, max:560},
  {id:'sash-bottom',axis:'y', cssVar:'--bottom-h',min:120, max:400},
];
```

Each sash binds `pointerdown` → captures, listens for `pointermove`
to compute new size, writes CSS var. On `pointerup`, saves to
`atlas_shell_v1` and calls `MAP.invalidateSize()`.

### Views are stateless renders

`renderLeftView(viewId)` clears `#left-panel` and re-renders. View
state (e.g., thesis form values) is read from localStorage on each
render — no in-memory shadow state.

## Rubric Checklist (service)

- [ ] Async-safe (no blocking calls in handlers)
- [ ] Bounded — input sizes capped, no unbounded loops
- [ ] Parameterised SQL only (none added here)
- [ ] Errors raised, not silently swallowed
- [ ] Tests cover happy path + boundary + error paths
- [ ] No PII collection
- [ ] Honest data — thesis is user-supplied, not LLM-fabricated

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `frontend/atlas.html` | Modify | Shell rewrite, 4 views, sashes, status bar, thesis form |
| `app/services/atlas/pilot.py` | Modify | Accept `thesis_context`; prepend to system prompt |
| `app/api/v1/atlas.py` | Modify | `PilotBody.thesis_context: Optional[Dict[str,Any]]` |
| `docs/plans/PLAN_074_atlas_vscode_shell.md` | Create | (already done) |
| `docs/specs/SPEC_082_atlas_vscode_shell.md` | Create | this file |
| `tests/test_spec_082_atlas_vscode_shell.py` | Create | skeleton + T1-T5 |

## Verification (manual, in browser)

1. Reload `/atlas.html` → see new shell. Old layout gone.
2. Click each activity-bar icon → left panel shows that view.
3. Click active icon → left panel collapses; map widens; status bar
   toggle reflects state.
4. Drag each gutter → widths change live; release → persist; reload
   restores them.
5. Fill thesis form: industry=furniture, HHI min=$75K, save. Reload
   → form retains values.
6. Ask Pilot "where should I open my store?" → narration explicitly
   references furniture + $75K+ context.
7. Click 3 different counties → all appear in Places list.
8. Open bottom panel → switch tabs → see live tool calls during a
   Pilot run.

## Feedback History

_No corrections yet._
