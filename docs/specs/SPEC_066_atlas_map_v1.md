# SPEC 066 — Atlas Map v1 (the explorer canvas)

**Status:** Draft
**Task type:** report (frontend / UI surface; same convention as SPEC_058 / 063)
**Date:** 2026-05-23
**Plan:** PLAN_066 v3 (the general explorer); PLAN_070 (dynamic UI design)
**Test file:** _none_ — static HTML, e2e verification (SPEC_058 convention)
**Builds on:** SPEC_064 (Atlas engine + cards) and SPEC_065 (layer-data + boundaries API, shipped `b893c2b`)

## Goal

Ship the **explorer canvas** — the Leaflet map, layer panel, choropleth +
point rendering, click→drill-down, hover, command counters, and the deep-
linkable URL state — that the layer-data API (SPEC_065) and the SPEC_064
engine combine to power. This is the *foundation* surface; the dynamic-UI
features (bivariate, brushed histograms, migration arcs, sparklines,
transitions) are SPEC_066b and ship alongside, not folded in here, so the
critical-path map is reviewable as a single concern.

**Intentionally NOT in this spec:** every PLAN_070 §3 "🥇 unique" and
"🥈 table-stakes" dynamic feature. Those are SPEC_066b. The split is
deliberate (PLAN_070 §5).

## Acceptance Criteria

- [ ] `frontend/atlas.html` rewritten as the **map-first** explorer (replaces
      the search-first v0 page from SPEC_064).
- [ ] **Zero-query landing** — page opens on the default layer
      (`disaster_nri` per PLAN_066 v3 §5.1) over the county boundary
      collection, with command-center counters populated. No query required.
- [ ] **Layer panel** — domain-grouped from `GET /atlas/layers`; toggling a
      layer fetches `GET /atlas/layer/{id}` and re-renders the choropleth
      (for choropleth layers) or overlays points (for point layers).
- [ ] **Hover** — a tooltip on each county shows `geo_name` + the current
      layer's value at that county, with the layer's `unit` label.
- [ ] **Click → drill-down** — clicking a county fires `GET /atlas/place/{geo_id}`
      and renders the multi-layer aggregate in a side panel. (The SPEC_064
      cross-dataset cards via `/atlas/explore` are also reachable from the
      panel via a "Deeper analysis →" link but are not the default click target —
      simple `/place/{geo_id}` is faster and shows more layer breadth.)
- [ ] **Command-center counters** in the chrome — honest values from the API
      (e.g. "17 layers · 3,143 counties · 1.07M EPA facilities tracked").
- [ ] **Deep-linkable URL** — `?lat=&lon=&zoom=&layer=&place=` round-trips
      the map state; share button copies the link.
- [ ] **Boundary geometry loaded once** — `GET /atlas/boundaries?geo_level=county`
      fetched on landing, cached in memory; layer toggles only fetch the
      layer values, not the geometry.
- [ ] Telemetry: every `card_viewed`-style interaction (`layer_toggled`,
      `place_clicked`, `tooltip_hovered` throttled) records to
      `POST /atlas/events` (reuses SPEC_064 telemetry).
- [ ] **No regression** — existing 11 SPEC_065 tests + the rest of the suite
      stay green.

## Verification (manual / e2e — SPEC_058 convention)

1. Open `http://localhost:3001/atlas.html` in a real browser
2. Map renders with county boundaries + the disaster_nri default layer color-
   ramped on top, within ~3s of landing
3. Layer panel shows 10 domains; toggling `energy_power_plants` overlays 14k
   point markers
4. Hovering a county shows `{name} · NRI risk: {value}`
5. Clicking Harris County (48201) opens the side panel with ≥4 layer values
6. URL updates to `?layer=disaster_nri&place=48201`; reload restores it
7. Share button copies the link
8. No console errors in DevTools; payload sizes reasonable (<5MB total)

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_066_atlas_map_v1.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_066_atlas_map_v1` |
| `frontend/atlas.html` | **Rewrite** | The SPEC_064-era search-first page becomes the map-first canvas (single self-contained file; Leaflet via CDN) |

## Design Notes

### Tech stack (PLAN_070 §4)
Leaflet v1.9 via unpkg CDN; canvas renderer (`L.canvas()`) for the 3,279
county polygons. No D3 in v1 — it lives in SPEC_066b. No basemap tiles in v1
either (boundaries on a dark background); a muted dark raster basemap is a
small follow-on. ~50KB of JS total.

### Page anatomy
```
┌─────────────────────────────────────────────────────────────────┐
│ HEADER       Nexdata Atlas · 17 layers · 3,143 counties · 1.07M EPA facilities       [Search ⌕]
├──────────────┬──────────────────────────────────────────┬───────┤
│              │                                          │       │
│  LAYER PANEL │             MAP CANVAS                   │ PLACE │
│  (domain     │   (Leaflet choropleth + point overlays)  │ PANEL │
│   groups,    │                                          │ (when │
│   from       │            tooltip on hover              │ a     │
│   /atlas/    │                                          │ county│
│   layers)    │                                          │ is    │
│              │                                          │ clkd) │
│              │                                          │       │
└──────────────┴──────────────────────────────────────────┴───────┘
                              [Share]  [Reset view]
```

The panel layout is intentionally simple; SPEC_066b layers brush bars,
sparklines, and the bivariate-mode toggle into the same chrome.

### State + URL
```js
const state = {
  layerId: 'disaster_nri',     // current single layer (v1 — one at a time)
  pointOverlayId: null,        // optional point layer on top
  placeId: null,               // clicked county/state FIPS
  view: { lat: 38.5, lon: -96, zoom: 4 },  // map view
};
// URL reflects state — ?layer=&overlay=&place=&lat=&lon=&zoom=
// History.replaceState on every change; parsed on load.
```

### Endpoint use (SPEC_065 — shipped)
- `GET /atlas/boundaries?geo_level=county` — once on landing, cached
- `GET /atlas/layers` — once on landing
- `GET /atlas/layer/{id}` — on every layer toggle
- `GET /atlas/place/{geo_id}` — on every county click
- `POST /atlas/events` — telemetry, fire-and-forget

### What v1 explicitly omits (PLAN_070 / SPEC_066b owns)
- Bivariate choropleth (one layer at a time in v1)
- Brushed histogram + distribution panel
- Migration flow arcs
- Sparklines
- Smooth animated transitions (v1 redraws on toggle — fine)
- Layer opacity sliders
- Scatter-plot pair view

## Feedback History

_No corrections yet._
