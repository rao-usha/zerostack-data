# PLAN 074 — Atlas VS-Code-style workspace shell

**Date:** 2026-05-27
**Phase:** PLAN_073 rev_01 Phase B v1.3 (between SPEC_081 and Phase C)
**Builds on:** SPEC_078 Pilot · SPEC_079 streaming · SPEC_080 zoom-adaptive · SPEC_081 guided tour
**Companion spec:** SPEC_082_atlas_vscode_shell

## Why

Today's Atlas page is a fixed three-column grid with the Pilot
floating over the map. As we add thesis-context, saved places,
session history, and a tool-log inspector, the page can't grow.
Users have asked for the IDE pattern they already know — VS Code's
activity bar + dockable panels — where chat lives on the right and
working state lives on the left.

This plan implements the **shell rewrite only** (Phase A). Backend
thesis persistence is Phase B (separate spec, optional).

## Target shell

```
┌┬──────────────────────────────┬───────────┐
│▤│ THESIS               MAP    │ CHAT      │  ← right column
│△│ ──────             ───      │ ───────── │    (Pilot, fixed)
│○│ Industry             [TX]   │ "I want   │
│◍│ Target HHI          [+]     │  to open  │
│ │                             │  a chair  │
│ │ [thesis editor]             │  store…"  │
│ ├─────────────────────────────│           │
│ │ TOOL LOG  CITES  SQL        │ [options] │
│ │ [15:42] zoom_to…            │           │
└┴──────────────────────────────┴───────────┘
 ↑ activity   ↑ left panel       ↑ bottom    ↑ chat (fixed)
   bar 40px     280-360px          150-300px   360-440px
```

- **Activity bar (40px):** 4 icons — Thesis · Layers · Places · History.
  Clicking switches the left panel view; clicking the active icon
  collapses the panel. Bottom of activity bar: status indicator.
- **Left panel (resizable 240-400px):** renders one of four views.
- **Map (flex):** unchanged Leaflet container, now bigger.
- **Right panel (resizable 320-480px):** Pilot chat replaces the
  current floating `#pilot-panel`. Same chat behaviour, fixed slot.
- **Bottom panel (collapsible 150-300px):** tabbed inspector —
  Tool Log · Citations · Queries (the tool-call-args/result inspector).
- **Status bar (24px):** left-panel toggle, bottom-panel toggle,
  current view info.

All four sashes (activity-left, left-map, map-right, top-bottom)
draggable via gutters. State persists in localStorage.

## Phasing

### Phase A — shell + 4 views (this iteration)

1. **Shell skeleton** — replace `#app` grid with the new 5-region grid.
2. **Activity bar + view switching** — 4 icon buttons, one active at
   a time, click-again collapses.
3. **Resizable gutters** — vertical between left panel & map and
   map & chat; horizontal between map and bottom panel. Mouse-down
   to drag, persists to localStorage.
4. **Thesis view (new)** — form: industry NAICS, target HHI range,
   target population density, target age band, must-have layers
   list. Saves to localStorage `atlas_thesis_v1`. Pilot reads this
   on every question and prepends to the message context.
5. **Layers view** — port existing `#layer-panel` content into the
   new shell. Group by domain (Demographics / Industry / Federal $
   / Risk / Climate / Logistics). Keep search.
6. **Places view (new)** — list of pinned places (county clicks +
   focal nodes). Click to re-zoom. Star, delete, group actions.
   Saves to localStorage `atlas_places_v1`.
7. **History view** — current localStorage `nexdata.pilot.hist`
   reformatted as searchable list with re-ask button.
8. **Right chat panel** — move pilot UI out of floating div into
   the right column. Same JS handlers; new container ID
   `#chat-panel`. Drop the floating-panel close button (always
   visible) but keep cancel + clear-history.
9. **Bottom inspector** — three tabs:
   - **Tool Log:** existing `#pilot-tools` stream → moved here
   - **Citations:** existing `#pilot-cites` → moved here
   - **Queries:** new — shows each tool call's args + result JSON
     in expandable rows. Useful for power users.
10. **Status bar** — bottom strip with: ⌧ left | ⌧ bottom |
    last-Pilot-status | layer-count indicator.
11. **Telemetry** — log view-switch / panel-toggle / sash-drag.

### Phase B — backend persistence (deferred)

- New table `atlas_thesis` (user_id, thesis_json, updated_at)
- New table `atlas_saved_place` (user_id, geo_id, label, group)
- Endpoint POST `/atlas/thesis`, GET `/atlas/thesis`
- Endpoint POST `/atlas/saved-place`, GET `/atlas/saved-places`
- Migration of localStorage → server on next visit (idempotent)

NOT in scope this iteration. Phase A ships with localStorage only.

## Files affected (Phase A)

| File | Change |
|---|---|
| `frontend/atlas.html` | shell grid rewrite, 4 view templates, sash drag, status bar |
| `frontend/atlas.html` (JS) | view-switcher, resize manager, thesis form, places list |
| `app/services/atlas/pilot.py` | accept optional `thesis_context` body field, prepend to system prompt |
| `app/api/v1/atlas.py` | `PilotBody.thesis_context: Optional[Dict]` field |
| `docs/specs/SPEC_082_atlas_vscode_shell.md` | new (companion) |
| `tests/test_spec_082_atlas_vscode_shell.py` | skeleton |

## Risk / open questions

- **Single-file HTML is getting heavy** — atlas.html is now >2.5K
  lines. Phase A may push it past 3K. Acceptable for now; a future
  refactor to ES modules / Vite is its own project.
- **Leaflet `invalidateSize()`** — after every sash drag we must
  call `MAP.invalidateSize()` or tiles won't fill the new area.
- **Mobile** — `<1000px` will collapse activity bar + bottom panel.
  Right chat panel becomes a bottom drawer. Phase A covers it but
  it's not the primary target.
- **Thesis context size** — to keep token cost manageable, only
  send thesis to Pilot when non-empty AND only the populated fields.

## Acceptance

- [ ] Page loads with new shell; old grid removed.
- [ ] All 4 left views render and switch.
- [ ] Right panel always shows Pilot chat.
- [ ] Bottom panel toggleable; 3 tabs work.
- [ ] Sash resize works for all 3 gutters; persists across reload.
- [ ] Thesis fields persist; Pilot reads them.
- [ ] Layer toggles still affect the map identically to today.
- [ ] Live test: ask "I want to open a chair store" with a populated
      thesis → narration references the thesis explicitly.

## Revisions

- [PLAN_074 rev_01](./PLAN_074_atlas_vscode_shell_rev_01.md) —
  visual polish (SVG icon set, refined palette, soft shadows,
  micro-transitions) + default-layer correction (NRI → median
  household income). 2026-05-27.
- [PLAN_074 rev_02](./PLAN_074_atlas_vscode_shell_rev_02.md) —
  Thesis form recast as Notion-style collapsible blocks with
  summary previews + sticky Save bar. 2026-05-27.
