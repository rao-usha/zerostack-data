# PLAN_054 — Signal Chain Analytics Dashboard (D3 Interactive)

## Context

All 8 PLAN_052 signal chains are live with API endpoints. 4 of 8 have no frontend visualization. This plan creates a single-page D3-powered interactive analytics dashboard (`frontend/signal-chains.html`) that lets investors explore all 8 intelligence products through connected, filterable visualizations.

**Design system:** Inherits Nexdata dark-slate theme (--bg: #0f172a, --primary: #6366f1, --accent: #06b6d4). Single self-contained HTML file (~2000-3000 lines). D3 v7 from CDN. No build tools.

---

## Page Architecture

### Layout: Tab-based with 8 signal panels + overview

```
┌──────────────────────────────────────────────────────────┐
│  HEADER: ← Back | Signal Chain Intelligence | [Refresh]  │
│  STATS BAR: 9 sectors | 43 GPs | 431 holdings | 5.4K...  │
├──────────────────────────────────────────────────────────┤
│  TABS: Overview | Macro | Diligence | LP-GP | Exec |     │
│        Site | Stress | Healthcare | Roll-Up              │
├──────────────────────────────────────────────────────────┤
│                                                          │
│                   ACTIVE PANEL                           │
│                 (D3 visualization)                        │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

---

## 9 Panels (1 overview + 8 chains)

### Panel 0: Overview — Signal Summary Matrix
**Viz type:** D3 grid heatmap — 8 rows (chains) × key metric columns
- Each cell is a colored rectangle (green/yellow/red gradient)
- Shows: chain name, primary score, key metric, data freshness
- Click any row → navigates to that chain's tab
- **API:** Calls all 8 chain summary endpoints in parallel on load
- **D3 pattern:** `d3.scaleBand()` for grid, `d3.scaleSequential(d3.interpolateRdYlGn)` for color

### Panel 1: Macro Environment — Sector Radar + Factor Heatmap
**Viz type:** D3 radar chart (9 sectors overlaid) + factor heatmap grid
- **Left:** Interactive radar with 7 axes (Rate, Yield, Labor, Sentiment, CPI, Energy, GDP). Each sector is a polygon. Hover = highlight one sector, dim others.
- **Right:** 9×7 heatmap grid (sectors × factors). Color = factor contribution. Click cell = show reading.
- **Bottom:** Macro ticker — live values: FFR 3.64% | 10Y 4.44% | CPI 2.7% | Oil $89 | NatGas $2.94
- **API:** `GET /pe/macro/deal-scores`
- **D3 pattern:** `d3.lineRadial()` for radar, `d3.scaleSequential()` for heatmap cells

### Panel 2: Company Diligence — Search + Scorecard
**Viz type:** Search bar → animated scorecard with gauge charts
- **Top:** Search input with autocomplete (fetches on type)
- **Center:** 6 radial gauge charts (one per factor) arranged in 2×3 grid. Each gauge is a D3 arc (0-100) with color gradient (red→yellow→green). Score number in center.
- **Right sidebar:** Red flags list (pulsing red dots), sources matched vs empty (check/X icons), confidence meter
- **Bottom:** Raw details accordion (expandable JSON-like breakdown)
- **API:** `POST /diligence/score` on search submit
- **D3 pattern:** `d3.arc()` for gauges, `d3.transition()` for animated fill

### Panel 3: LP→GP Network — Force-Directed Bipartite Graph
**Viz type:** D3 force simulation — THE showpiece visualization
- **Nodes:** LPs (left cluster, blue circles) and GPs (right cluster, indigo circles). Size = total committed USD. Tier-1 LPs get gold ring.
- **Links:** LP→GP edges. Width = relationship strength. Color = commitment trend (green=growing, gray=stable, red=declining). Dashed = first vintage only.
- **Interactions:** Drag nodes, zoom/pan, hover = highlight all connections for that node + tooltip with LP/GP details. Click GP = show pipeline score panel.
- **Right panel (slide-in):** GP Pipeline Score card when a GP node is clicked — 5 signal gauges + LP base table
- **Controls:** Filter by min strength, toggle tier-1 only, search for GP/LP
- **API:** `GET /pe/gp-pipeline/graph` + `GET /pe/gp-pipeline/scores/{id}` on click
- **D3 pattern:** Full force simulation from `network.html` — `forceLink`, `forceManyBody`, `forceCenter`, `forceCollide`, drag + zoom

### Panel 4: Executive Signals — Transition Leaderboard + Treemap
**Viz type:** Sortable leaderboard table + D3 treemap of hiring by company
- **Left:** Ranked table — company name, transition score (color-coded bar), flags (badges), C-suite/VP/Dir counts. Click row = drill into treemap.
- **Right:** D3 treemap — rectangles sized by total_open postings. Color = transition score (red=high, blue=low). Nested: company → seniority level breakdown.
- **Hover:** Treemap cell → tooltip with signal breakdown
- **API:** `GET /exec-signals/scan?limit=30`
- **D3 pattern:** `d3.treemap()` with `d3.hierarchy()`, squarify layout

### Panel 5: Site Intelligence — Interactive Map + Score Card
**Viz type:** D3 US map (AlbersUSA projection) + click-to-score
- **Map:** US states choropleth (colored by NRI risk or electricity price). Markers at scored locations.
- **Click map:** Drops a pin at click location, fires unified-score API, shows floating score card.
- **Score card:** 5 factor bars (horizontal, animated fill), overall score badge, use-case toggle (datacenter/manufacturing/warehouse/general — changes weights live).
- **Compare mode:** Pin up to 4 locations, show side-by-side bar chart comparison.
- **API:** `POST /site-intel/sites/unified-score` on map click, `POST /site-intel/sites/unified-compare` for comparison
- **D3 pattern:** `d3.geoAlbersUsa()` + `d3.geoPath()` from `map.html`, TopoJSON states

### Panel 6: Portfolio Stress — Heatmap + Bubble Chart
**Viz type:** Holdings stress heatmap + zoomable bubble chart
- **Top:** Firm selector dropdown (PE firms list)
- **Center:** Bubble chart — each holding is a circle. X-axis = rate stress, Y-axis = margin stress, size = revenue (if available), color = sector. Quadrants labeled: "Safe Harbor" (low/low), "Rate Exposed" (high rate/low margin), "Margin Squeeze" (low rate/high margin), "Critical" (high/high).
- **Left sidebar:** Distribution bars — critical/elevated/moderate/low counts. Portfolio stress gauge.
- **Hover:** Bubble → company name, all 3 components, leverage, EBITDA margin
- **Click:** Bubble → slide-in detail panel with full component breakdown
- **API:** `GET /pe/stress/{firm_id}`, `GET /pe/stress/holding/{id}` on click
- **D3 pattern:** `d3.scaleLinear()` for axes, `d3.forceSimulation()` for bubble packing (avoid overlap), `d3.zoom()` for drill-down

### Panel 7: Healthcare Profiles — Scatter + Filter Panel
**Viz type:** D3 scatter plot (acquisition score vs revenue) + filter sidebar
- **Main:** Scatter plot — X = estimated revenue, Y = acquisition score. Color = grade (A=green, B=blue, C=yellow, D/F=red). Size = review count. Shape: circle=independent, diamond=multi-site.
- **Filter sidebar:** State dropdown, min score slider, physician oversight toggle, min locations slider
- **Hover:** Practice name, city, ZIP, all 5 factor scores, revenue estimate
- **Click:** Expand to full profile card (factor gauges + details)
- **API:** `GET /healthcare/profiles?state=&min_score=&limit=100`
- **D3 pattern:** `d3.scaleLinear()` / `d3.scaleLog()` for axes, `d3.brush()` for selection, animated transitions on filter change

### Panel 8: Roll-Up Market — County Choropleth + NAICS Selector
**Viz type:** D3 US county choropleth map + NAICS industry selector
- **Top:** NAICS code input/search (with autocomplete for common industries)
- **Map:** County-level fill color = roll-up market score (green=high, red=low). Only counties with data colored; others gray.
- **Hover:** County name, state, score, grade, fragmentation, market size, affluence
- **Click county:** Slide-in detail with 5 sub-score breakdown
- **Right panel:** Top 10 counties table, state filter dropdown
- **API:** `GET /rollup-intel/rankings/{naics}?limit=100`, `GET /rollup-intel/market/{naics}/{fips}`
- **D3 pattern:** `d3.geoPath()` with county-level TopoJSON, `d3.scaleSequential()` for choropleth

---

## Shared Components

### Stats Bar (dynamic, updates per tab)
```
Overview: 8 chains active | 47 sources | 9 sectors | 43 GPs | 1.8K companies
Macro:    FFR 3.64% | 10Y 4.44% | CPI 2.7% | Oil $89.33
LP-GP:    43 GPs | 564 LPs | 164 edges | 8 tier-1 LPs
```

### Tooltip (shared)
- Positioned at mouse, clamped to viewport
- Dark card style with border, 200ms fade
- Content generated per-panel

### Loading States
- Skeleton shimmer animation while fetching
- Spinner in stats bar during load

### Color Scales (shared across panels)
- Grade: A=#22c55e, B=#6366f1, C=#f59e0b, D=#ef4444, F=#6b7280
- Stress: 0=#22c55e → 50=#f59e0b → 100=#ef4444
- Strength: `d3.interpolateViridis` (0-100)
- Sector: 9 distinct colors from `d3.schemeTableau10`

---

## Implementation Phases

### Phase 1: Shell + Overview + Macro (Panels 0-1)
- HTML skeleton with tab system, header, stats bar
- Overview heatmap grid (calls all 8 endpoints)
- Macro radar + factor heatmap
- **~800 lines**

### Phase 2: LP-GP Network + Diligence (Panels 2-3)  
- Force-directed bipartite graph (the showpiece)
- Company search with gauge scorecard
- **~700 lines**

### Phase 3: Exec + Site Map (Panels 4-5)
- Treemap for executive signals
- Interactive US map with click-to-score
- **~600 lines**

### Phase 4: Stress + Healthcare + Roll-Up (Panels 6-8)
- Bubble chart for portfolio stress
- Scatter plot for healthcare
- County choropleth for roll-up
- **~700 lines**

### Phase 5: Polish + Gallery Card
- Loading states, error handling, responsive tweaks
- Add gallery card to `frontend/index.html`
- **~100 lines**

---

## Files

- **NEW:** `frontend/signal-chains.html` (~2800 lines, self-contained)
- **MODIFY:** `frontend/index.html` (add gallery card)

## CDN Dependencies
```html
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script src="https://cdn.jsdelivr.net/npm/topojson-client@3"></script>
```

## Verification

1. Open `http://localhost:8001/signal-chains.html`
2. Overview tab: all 8 chains show colored cells
3. Macro tab: radar chart with 9 sector polygons
4. LP-GP tab: drag/zoom network graph with 164 edges
5. Site tab: click US map → score card appears
6. Stress tab: select Blackstone → 431 bubbles render
7. Healthcare tab: scatter plot with 5.4K points filterable by state
8. All interactions: hover tooltips, click drill-downs, filter controls
