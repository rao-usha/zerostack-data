# SPEC 066b — Atlas Dynamic UI Layer (coupling, motion, surprise)

**Status:** Draft
**Task type:** report (frontend / UI surface; SPEC_058 / 063 convention)
**Date:** 2026-05-23
**Plan:** PLAN_070 (Dynamic UI design source-of-truth)
**Test file:** _none_ — static HTML + JS, e2e verification
**Builds on:** SPEC_066 (the map canvas — must ship first); SPEC_065 (layer API)

> **Numbering:** this is `066b`, not `067`, because it layers on top of
> SPEC_066's same `frontend/atlas.html` file. It does not stand alone — it
> presupposes the SPEC_066 map exists.

## Goal

Add the 🥇 "unique-to-Atlas" and 🥈 "table-stakes" dynamic-UI features from
PLAN_070 §3 to the SPEC_066 map, so the explorer becomes *interesting to
use*, not just functional. This is the difference between "competent
dashboard" and "I can't stop clicking" — and the Phase-2 loop measurement
depends on it.

## Acceptance Criteria

Each feature below ships and works against the SPEC_065 layer API.

### Coupling (the addictive loop)

- [ ] **Bivariate choropleth mode** — toggle "compare layers →" exposes a
      second layer picker; both layers' choropleth values are encoded into a
      single 3×3 bivariate color scale (D3 custom scale). Works for any two
      county-grain layers.
- [ ] **Brushed histogram ↔ map** — when a single choropleth layer is
      active, a histogram appears in the side panel. Dragging a brush range
      on the histogram dims counties outside that range on the map.
      Conversely, hovering a county highlights its bar on the histogram.
- [ ] **Distribution-with-my-position panel** — when a county is clicked,
      the place panel shows a small histogram per choropleth layer (for that
      layer) with the clicked county's value marked, plus a "this county is
      in the Nth percentile" label.
- [ ] **Scatter-plot pair view** — a "🔬 explore correlations" mode opens a
      scatter of every county for two chosen layers; clicking a point
      highlights that county on the map; map clicks highlight the scatter
      point. Linked both ways.

### Motion (animate to teach)

- [ ] **Migration flow arcs** — when the `demo_irs_migration_net_agi` layer
      is active, animated arcs render origin→destination county pairs for
      the top-N migration flows (width = $$ flow). D3 + Leaflet SVG overlay.
      Hover an arc → tooltip with the pair.
- [ ] **Smooth layer transitions** — switching layers fades/recolors the
      choropleth over ~400ms instead of redrawing instantly. D3 `.transition()`
      on the fill color.
- [ ] **Animated counter ticks** — on landing, the command-center counters
      count up from 0 over ~800ms (D3 `.tween("text", ...)`).

### Coupling (read-only, sparklines)

- [ ] **Sparklines in the place panel** — for layers with a time series
      (FRED rates, FEMA decl-per-year for that county, FDIC quarterly), a
      tiny sparkline renders inline next to the value. Observable Plot.

### Coupling (control)

- [ ] **Layer opacity slider** — each active layer has a 0-100 slider that
      adjusts its fill alpha; lets users mix bivariate-style without leaving
      single-layer mode.

### Telemetry (PLAN_070 §6 — every interaction is signal)
- [ ] `bivariate_pair_selected: {layer_a, layer_b}`
- [ ] `histogram_brushed: {layer, range_min, range_max}`
- [ ] `scatter_explored: {x_layer, y_layer}`
- [ ] `migration_arc_clicked: {origin, dest}`
- [ ] `place_percentile_viewed: {place_id, layer, percentile}`
- [ ] All recorded via the existing `POST /atlas/events` (SPEC_064 telemetry).

### Quality
- [ ] **No regression** — SPEC_066 verification still passes; the dynamic
      layer is purely additive.
- [ ] **Honest motion** — every animation reveals something the static state
      didn't. No decorative motion (PLAN_070 §2 Pillar 2).
- [ ] **Performance** — choropleth recolor under ~200ms; brushing produces
      smooth 60fps highlight on the map. Use the Leaflet canvas renderer +
      throttle hover.

## Verification (manual / e2e)

1. SPEC_066 verification still passes.
2. Toggle "compare layers" → pick `demo_irs_county_agi_per_return` × `disaster_nri`
   → see a 9-cell legend, map recolored bivariate. Verify the legend cells
   are clickable and explain "high wealth × high risk" etc.
3. With a single layer active, drag a range on its histogram → counties
   outside the range visibly dim. Release brush → all counties return.
4. Click Harris County (48201) → place panel shows percentile markers
   ("78th percentile for AGI per return") and ≥4 sparklines.
5. Switch to `demo_irs_migration_net_agi` → animated arcs render across the
   map; hover an arc → tooltip.
6. Toggle between two layers → fade transition, no flash.
7. Move the opacity slider on a layer → fill alpha visibly changes.
8. Open browser DevTools network tab → `/atlas/events` POSTs fire for each
   interaction.
9. No console errors. Bundle size: <200KB JS (Leaflet + D3 + Plot + heat).

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_066b_atlas_dynamic_ui.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_066b_atlas_dynamic_ui` (after SPEC_066 lands) |
| `frontend/atlas.html` | **Modify** | Add D3 + Observable Plot CDN scripts; add the 8 features above into the existing SPEC_066 page (panel sections, event wiring, telemetry calls) |

## Design Notes

### Tech additions over SPEC_066
- **D3.js v7** (CDN) — bivariate scales, brushed histograms, migration arcs,
  custom transitions, color interpolations
- **Observable Plot** (CDN) — sparklines, scatter plot, distribution
  histograms (one-line declarative)

Both via unpkg, single-file, no build step. PLAN_070 §4 tech stack.

### Bivariate color scale (D3 sketch)
A 3×3 grid: x-axis = layer A quintile (low/mid/high), y-axis = layer B
quintile. Cell colors interpolate two hues — e.g. blue (high A) × red
(high B) → purple (high both). D3 `d3.interpolateLab` between the corners.

### Brushed histogram (D3 sketch)
`d3.brushX()` over an `<svg>` histogram of layer values. The brush selection
sets a `[min, max]` filter; the map iterates choropleth values, dims any
outside the range via canvas-fill alpha multiplier. Brushing emits the
`histogram_brushed` event.

### Migration flow arcs (D3 + Leaflet SVG overlay)
Top-N flows per active view (e.g. 100). Each rendered as a quadratic Bezier
arc from origin centroid → destination centroid. Path `stroke-width`
proportional to $$, `stroke-opacity` damped at the ends. Animation:
`stroke-dasharray` interpolated for the "flowing" effect (subtle, on hover).

### Sparkline shape (Observable Plot)
```js
Plot.lineY(values, {x: "date", y: "value"})
    .plot({width: 80, height: 24, axis: null})
```
Tiny, inline, no axes. One line per place-panel row that has a time series.

### Scatter-plot pair (Observable Plot)
```js
Plot.dot(counties, {x: layerA, y: layerB, fill: "currentColor"})
    .plot({width: 240, height: 240})
```
With a `Plot.frame()` and a callback that maps point hover → map highlight
(linked-views).

## Sequencing within SPEC_066b
If the spec ships in waves:
1. **Wave 1** — sparklines + opacity sliders + animated counters + smooth
   transitions. The cheap dynamic. ~half-day.
2. **Wave 2** — brushed histogram + distribution-position panel. The core
   coupled loop. ~day.
3. **Wave 3** — bivariate choropleth + scatter pair. The novel comparisons.
   ~day.
4. **Wave 4** — migration flow arcs. The signature animation.

Ship as one commit when all four are working, or wave-by-wave with each
acceptance checked, depending on focus budget.

## Non-goals (deferred to SPEC_066c)
- FEMA time-cascade scrubber (Pillar 2; the biggest "wow" but biggest spec)
- EPA kernel-density heatmap (Leaflet.heat — drop-in but its own spec entry)
- Calendar heatmap for daily series
- Side-by-side synced compare / swipe compare
- Lasso-select / custom-region
- Surprise/anomaly engine (gated on telemetry signal)

## Feedback History

_No corrections yet._
