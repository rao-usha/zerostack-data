# PLAN_058 — Revision 01: Cascade Explorer Visualization Overhaul

**Date:** 2026-04-08  
**Triggered by:** User feedback on cascade-explorer.html UX

---

## Revision 01

Replace the D3 force graph with a **Layered DAG (Option 2)** as primary view and **Sankey Flow (Option 1)** as alternative view. Fix all reported UX issues.

## What Was Wrong

1. **Slider misalignment** — slider ball position didn't match displayed value (e.g., ball at -1 but label shows 0). The range input's visual position was decoupled from the simulation value.

2. **Slider too prominent** — takes too much horizontal space and is locked to FFR only. Should be compact inline controls on any node, supporting Building Permits, deregulation benchmarks, or any macro variable.

3. **Node impact labels don't track** — the `-0.5%` labels on nodes don't update smoothly with slider movement. Debounce is too slow or label update doesn't sync with simulation response.

4. **Force graph recentering** — when dragging nodes, the graph snaps back to a northwest offset. The force simulation's `forceCenter` and `forceX/Y` keep pulling nodes to a computed midpoint that doesn't match the visual center. This fights the user constantly.

5. **Force graph is wrong metaphor** — for a causal cascade, you need to see directional flow (cause → effect), not a physics cloud. A force graph doesn't communicate "A causes B causes C" — it shows "A is near B."

## What Was Fixed

### New Primary View: Layered DAG (4 columns)
- Fixed positions by causal distance: **Input → Transmission → Downstream → Companies**
- No physics simulation — nodes stay where placed
- Click any node in column 1 or 2 to make it the shock input
- Compact inline +/- stepper appears on the selected input node (not a separate slider bar)
- Edges colored green/red by impact direction, thickness by confidence
- Impact labels update immediately on each node

### New Alternative View: Sankey Flow
- Toggle between DAG and Sankey via view selector
- Flow width = impact magnitude, color = direction
- Left-to-right: Input → intermediaries → companies
- Beautiful for demo/presentation — shows "shock energy" flowing and splitting

### Other Fixes
- Any macro variable can be the input (not just FFR)
- Scenario presets work with both views
- Chat panel works with both views (graph refreshes on mutations)
- Company search+add works with both views
- No recentering, no physics fighting

## Lessons Learned

1. **Force graphs are wrong for causal flow** — they show proximity, not causation. Use fixed layouts (DAG, Sankey, tree) for directional relationships.
2. **Sliders should be inline on the element they control** — a global slider bar is disconnected from what it modifies. Attach controls to the node itself.
3. **Physics simulations fight the user** — if users drag things, the graph shouldn't pull them back. Either use fixed positions or disable recentering forces entirely.
4. **Test with actual user interaction** — the slider/label mismatch would have been caught by clicking through the UI once.
