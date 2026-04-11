# D3.js Visualization Research for Nexdata PE Intelligence Platform

**Date:** 2026-03-09
**Purpose:** Identify the highest-impact D3.js visualization types for a Private Equity intelligence platform, ranked by value to PE users.

---

## Executive Summary

This document catalogs 20 D3.js visualization types mapped to Nexdata's PE intelligence use cases. Each is ranked by **impact** (value to PE decision-makers), with data requirements, implementation complexity, and the D3 modules/extensions needed. The recommendations are informed by patterns used in Bloomberg Terminal, PitchBook, Carta, and modern PE analytics platforms.

---

## Visualization Catalog (Ranked by Impact)

### Tier 1 -- Must-Have (Highest Impact)

---

#### 1. Sankey Diagram -- Deal Flow Pipeline

**What it shows:** The flow of deals through pipeline stages (Sourced -> Screened -> LOI -> DD -> Closed -> Portfolio), with width proportional to deal count or value at each stage. Drop-off between stages is immediately visible.

**Data needed:**
- `pe_deals` table: deal stage, deal value, timestamps, sector, source
- Stage transition history (when deals moved between stages)

**Why it's valuable for PE:** This is the single most requested visualization in PE. Partners want to see pipeline velocity, conversion rates, and where deals stall. A Sankey immediately answers "how healthy is our pipeline?" and "where are we losing deals?" -- questions that currently require spreadsheet analysis.

**D3 module:** `d3-sankey` (npm install d3-sankey)

**Complexity:** Medium. The d3-sankey plugin handles layout; main work is transforming deal stage data into nodes/links format with proper flow values.

**Nexdata endpoints:** `pe_deals`, `pe_collection`

---

#### 2. Force-Directed Network Graph -- Portfolio Company Relationships

**What it shows:** An interactive network where nodes are companies, PE firms, people, and sectors. Links represent ownership, board seats, co-investments, supply chain relationships, and shared executives. Node size = revenue or deal value. Color = sector or health score.

**Data needed:**
- `pe_firms`, `pe_companies`, `pe_deals` -- ownership/investment links
- `people`, `company_people` -- shared executives, board members
- `industrial_companies` -- supply chain relationships
- Sector/industry classification

**Why it's valuable for PE:** Reveals hidden connections that drive deal sourcing. "Company X's CFO used to be at Company Y, which is in our portfolio" or "Three of our portfolio companies share the same logistics provider." Network visualization is a key differentiator vs. PitchBook, which shows relationships as flat tables.

**D3 module:** `d3-force` (built into D3 core)

**Complexity:** Medium-High. Force simulation needs tuning (charge strength, link distance, collision detection). Interactive features (drag, zoom, click-to-expand, hover tooltips) add complexity. Consider WebGL rendering via d3fc for large graphs (500+ nodes).

**Nexdata endpoints:** `pe_firms`, `pe_companies`, `pe_people`, `people`, `rollup_intel`

---

#### 3. Radar/Spider Chart -- Deal Scoring & Company Assessment

**What it shows:** Multi-axis comparison of a company or deal across 6-10 dimensions: financial health, market position, management quality, growth trajectory, data quality/coverage, risk score, labor arbitrage potential, location score.

**Data needed:**
- Composite scores from various Nexdata modules (normalized to 0-100 scale)
- `pe_companies` scores, `zip_scores`, labor arbitrage metrics, competitive intel
- Benchmark data for peer comparison (overlay multiple companies on same chart)

**Why it's valuable for PE:** Deal committees evaluate companies across many dimensions simultaneously. A radar chart instantly shows strengths and weaknesses vs. peers. Overlaying a target company against the portfolio average reveals fit. This replaces the "deal scoring spreadsheet" that every PE firm maintains manually.

**D3 module:** No official D3 module; use community `radar-chart-d3` (npm) or build custom with `d3-scale`, `d3-line`, `d3-area`.

**Complexity:** Medium. Well-documented community implementations exist. Main work is normalizing heterogeneous scores to comparable scales.

**Nexdata endpoints:** `pe_companies`, `zip_scores`, `labor_arbitrage`, `competitive`, `rollup_intel`

---

#### 4. Choropleth Map -- Geographic/Location Intelligence

**What it shows:** US map (or world) with regions colored by intensity metrics: market density, labor cost, utility rates, tax incentives, foot traffic, site scores. Click a state/county to drill down. Overlay markers for portfolio company locations, competitor locations, and potential acquisition targets.

**Data needed:**
- GeoJSON boundaries (US states, counties, MSAs, ZIP codes)
- `zip_scores` -- composite location scores
- Site intel data: labor rates, utility costs, incentives, risk scores
- Portfolio company and target locations (lat/lng)

**Why it's valuable for PE:** Location intelligence is critical for industrials-focused PE (Nexdata's sweet spot). Site selection, market expansion planning, and geographic concentration risk all require spatial analysis. A PE firm evaluating a logistics company needs to see "where are their facilities vs. optimal locations?" instantly.

**D3 module:** `d3-geo` (built into D3 core), `d3-geo-projection` for additional projections, `topojson-client` for efficient boundary data.

**Complexity:** Medium-High. Requires TopoJSON boundary files, proper projection setup (AlbersUSA for US maps), color scale calibration, and zoom/drill-down interaction. The geographic data pipeline is the hard part, not the D3 rendering.

**Nexdata endpoints:** `zip_scores`, `location_diligence`, `site_intel_*` (labor, power, transport, logistics, water, telecom, risk, incentives), `foot_traffic`

---

#### 5. Multi-Line Time Series with Brushable Zoom -- Fund/Company Performance

**What it shows:** Revenue, EBITDA, employee count, or other KPIs plotted over time for one or more companies. A brush control at the bottom allows selecting a time range to zoom into. Multiple series can be toggled. Annotations mark key events (acquisition date, add-on, management change).

**Data needed:**
- Time-series financial data from `pe_companies` (quarterly/annual)
- Event data: deal dates, leadership changes, add-on acquisitions
- Benchmark indices for comparison overlays

**Why it's valuable for PE:** Tracking portfolio company performance over hold period is the core of PE operations. Brushable zoom lets analysts focus on specific periods (e.g., "what happened in Q3?"). Event annotations connect performance changes to operational decisions. This is table-stakes for any PE analytics platform.

**D3 module:** `d3-scale` (scaleTime, scaleLinear), `d3-axis`, `d3-brush`, `d3-zoom`, `d3-shape` (line, area). Or use `d3fc` / `TechanJS` for pre-built financial chart components.

**Complexity:** Medium. D3's brush and zoom modules are well-documented. The main challenge is handling multiple Y-axis scales and responsive design. `d3fc` significantly reduces boilerplate.

**Nexdata endpoints:** `pe_companies`, `pe_deals`, `pe_collection`

---

### Tier 2 -- High Impact

---

#### 6. Treemap -- Portfolio Composition & Sector Allocation

**What it shows:** Nested rectangles where size = investment value (or revenue), color = performance (green = above target, red = below), and nesting = sector -> sub-sector -> company. Click to zoom into a sector.

**Data needed:**
- `pe_companies`: company value, sector, sub-sector
- `pe_deals`: investment amount, current valuation
- Performance metrics (revenue growth, EBITDA margin) for color coding

**Why it's valuable for PE:** Instantly shows portfolio concentration risk. "70% of our capital is in two sectors" jumps off the screen. Treemaps encode two variables simultaneously (size + color), making them more information-dense than pie charts. PitchBook uses treemaps extensively for market composition views.

**D3 module:** `d3-hierarchy` (treemap layout, built into D3 core). Use `treemapSquarify` for best readability.

**Complexity:** Low-Medium. D3's treemap layout is mature and well-documented. Zoomable treemaps add moderate complexity.

**Nexdata endpoints:** `pe_companies`, `pe_deals`, `pe_firms`, `market`

---

#### 7. Heatmap Matrix -- Financial Benchmarking & Data Coverage

**What it shows:** Two primary uses:

**(a) Financial Benchmarking:** Rows = companies, columns = metrics (revenue growth, EBITDA margin, employee growth, etc.). Cell color intensity = percentile rank within peer set. Instantly identify outperformers and laggards across multiple dimensions.

**(b) Data Coverage Dashboard:** Rows = companies, columns = data fields. Cell color = data completeness (green = populated, yellow = stale, red = missing). Shows collection gaps at a glance.

**Data needed:**
- (a): Normalized financial metrics across peer sets from `pe_companies`, `peer_sets`
- (b): Field-level completeness metadata from all tables

**Why it's valuable for PE:** Benchmarking heatmaps replace the "comp table" spreadsheet that analysts build manually for every deal. Data coverage heatmaps are critical for platform operations -- they show where collection efforts should focus. Both encode large amounts of data in minimal space.

**D3 module:** `d3-scale` (scaleSequential with interpolateRdYlGn or similar), `d3-axis`. No special plugin needed.

**Complexity:** Low-Medium. Heatmaps are essentially colored grids. The complexity is in data normalization and color scale selection, not rendering.

**Nexdata endpoints:** `pe_companies`, `peer_sets`, `people_data_quality`, `competitive`

---

#### 8. Bubble Chart (Scatter + Size) -- Company Comparison Matrix

**What it shows:** Each bubble is a company. X-axis = one metric (e.g., revenue), Y-axis = another (e.g., growth rate), bubble size = a third (e.g., employee count), color = sector or deal stage. Interactive: hover for details, click to drill into company profile.

**Data needed:**
- `pe_companies`: revenue, growth rate, employee count, EBITDA, sector
- Any three comparable numeric metrics

**Why it's valuable for PE:** The "magic quadrant" view. Instantly segments companies into categories (high-growth/high-revenue vs. small/stagnant). PE analysts use this to identify acquisition targets ("show me fast-growing companies in the $20-50M revenue range") and to position portfolio companies against competitors.

**D3 module:** `d3-scale` (scaleSqrt for bubble sizing -- critical to use area, not radius), `d3-axis`, `d3-transition` for animated filtering.

**Complexity:** Low-Medium. Standard scatter plot with circle sizing. Use `scaleSqrt()` (not `scaleLinear()`) so bubble area is proportional to value.

**Nexdata endpoints:** `pe_companies`, `competitive`, `market`, `peer_sets`

---

#### 9. Organizational Hierarchy / Tree Diagram -- Org Charts

**What it shows:** Top-down or radial tree showing company leadership hierarchy. CEO at root, direct reports as children, their reports below. Node cards show name, title, tenure, source confidence. Expandable/collapsible branches.

**Data needed:**
- `people`, `company_people`: person name, title, reports_to
- `org_chart_snapshots`: historical org structure
- Title hierarchy inference from deep collection pipeline

**Why it's valuable for PE:** Management assessment is a key part of PE due diligence. Visualizing the org chart reveals span of control, depth of management, key-person risk, and open positions. Comparing org charts over time shows organizational stability. This directly leverages Nexdata's deep collection pipeline.

**D3 module:** `d3-hierarchy` (tree layout), `d3-zoom` for pan/zoom on large orgs.

**Complexity:** Medium. D3's tree layout handles positioning. The challenge is rendering attractive node cards (not just circles), handling large orgs with collapse/expand, and the reporting-line data itself (often incomplete, requiring inference).

**Nexdata endpoints:** `people`, `pe_people`, `people_analytics`, `people_jobs`

---

#### 10. Waterfall Chart -- Value Creation Bridge

**What it shows:** A stepped bar chart showing how value changed from entry to exit: Starting EBITDA -> Revenue Growth -> Margin Expansion -> Multiple Expansion -> Add-on Acquisitions -> Debt Paydown -> Exit Value. Each step shows positive (green, going up) or negative (red, going down) contribution.

**Data needed:**
- Entry valuation, exit valuation
- EBITDA at entry/exit, revenue at entry/exit
- Entry/exit multiples
- Add-on acquisition values
- Debt structure changes

**Why it's valuable for PE:** The "value creation bridge" is THE chart in every PE fund's investor presentation. It decomposes returns into operational improvement vs. financial engineering vs. multiple arbitrage. LPs scrutinize this to evaluate GP skill. Automating this from data (vs. manually building in PowerPoint) is a major time-saver.

**D3 module:** No official plugin; built with `d3-scale`, `d3-axis`, custom rectangle positioning (each bar starts where the previous one ended).

**Complexity:** Medium. The layout logic (calculating cumulative offsets for floating bars) is the main challenge. Connecting lines between bars and proper labeling require attention.

**Nexdata endpoints:** `pe_deals`, `pe_companies`

---

### Tier 3 -- Differentiation & Delight

---

#### 11. Chord Diagram -- Sector/Fund Flow Relationships

**What it shows:** Circular layout showing capital flow between sectors, funds, or geographic regions. Arc width = flow volume. Reveals which sectors are net sources vs. destinations of capital, or which PE firms frequently co-invest.

**Data needed:**
- `pe_deals`: source sector, target sector, deal value
- `pe_firms`: co-investment relationships
- Capital flow data between regions/sectors

**Why it's valuable for PE:** Reveals market-level patterns invisible in tabular data. "Healthcare is the biggest net source of PE exits flowing into tech" or "These three firms co-invest on 40% of deals." Visually striking for LP reports and presentations.

**D3 module:** `d3-chord` (built into D3 core)

**Complexity:** Medium. D3's chord layout handles the math; the challenge is preparing the adjacency matrix and making it readable with good color coding and interactive tooltips.

**Nexdata endpoints:** `pe_deals`, `pe_firms`, `market`

---

#### 12. Zoomable Sunburst -- Industry/Sector Drill-Down

**What it shows:** Concentric rings representing hierarchy levels: Sector -> Sub-sector -> Industry -> Company. Ring width = market size or deal count. Click to zoom into a sector, showing its sub-sectors as the new outer ring.

**Data needed:**
- Industry classification hierarchy (NAICS codes, SIC codes)
- `pe_companies`: industry, revenue/deal count at each level
- `market`: sector size data

**Why it's valuable for PE:** More intuitive than treemaps for deep hierarchies (4+ levels). The zoom interaction lets analysts start broad ("show me all industrials") and drill down ("show me the packaging sub-sector") naturally. The radial layout accommodates many categories without the squished rectangles of a treemap.

**D3 module:** `d3-hierarchy` (partition layout), `d3-transition` for zoom animation.

**Complexity:** Medium. Well-documented on Observable. The partition layout does the heavy lifting. Smooth zoom transitions require careful arc interpolation.

**Nexdata endpoints:** `pe_companies`, `market`, `pe_deals`

---

#### 13. Bullet Chart -- KPI Status Indicators

**What it shows:** A compact horizontal bar showing: actual value (dark bar), target value (thin line marker), and performance ranges (background bands: poor/satisfactory/good). Multiple bullet charts stacked vertically create a KPI dashboard.

**Data needed:**
- Current metric values (revenue, EBITDA, headcount, etc.)
- Target values (budget, plan, prior year)
- Threshold ranges for RAG (Red/Amber/Green) status

**Why it's valuable for PE:** Bullet charts are the most space-efficient way to show "actual vs. target" for many KPIs simultaneously. A portfolio dashboard showing 10 companies x 5 KPIs as bullet charts fits on one screen -- impossible with traditional bar charts. Stephen Few (information visualization expert) designed this format specifically for executive dashboards.

**D3 module:** No official plugin; `d3fc` includes a bullet chart component. Otherwise built with `d3-scale`, basic rectangles and lines.

**Complexity:** Low. Simple geometry (nested rectangles + a line marker). The design challenge is choosing appropriate ranges and colors.

**Nexdata endpoints:** `pe_companies`, `pe_deals`

---

#### 14. Sparkline Grid -- Portfolio Health at a Glance

**What it shows:** A table where each row is a portfolio company, and columns include tiny inline charts (sparklines) showing 12-month trends for key metrics: revenue trend (line), headcount trend (line), sentiment (area), web traffic (bar). No axes -- just the shape of the trend.

**Data needed:**
- Monthly time-series data for each metric
- `pe_companies`: financial time series
- `web_traffic`, `job_postings`: alternative data time series

**Why it's valuable for PE:** Sparklines (coined by Edward Tufte) pack maximum information into minimum space. A portfolio manager can scan 30 companies and immediately spot which ones are trending up vs. down across multiple dimensions. This is the "Bloomberg Terminal feel" -- dense, informative, designed for experts who scan data quickly.

**D3 module:** `d3-shape` (line generator, area generator), `d3-scale`. Sparklines are just tiny line/area charts without axes.

**Complexity:** Low. Sparklines are the simplest D3 charts. The challenge is embedding them properly in table cells with responsive sizing.

**Nexdata endpoints:** `pe_companies`, `web_traffic`, `job_postings`, `foot_traffic`

---

#### 15. Stacked Area Chart -- Market Composition Over Time

**What it shows:** Time on X-axis, stacked colored areas showing how market share or deal volume is distributed across sectors/players over time. Reveals sector rotation, emerging trends, and declining segments.

**Data needed:**
- Time-series deal data by sector from `pe_deals`
- Market share data from `market`
- Job posting volume by sector over time

**Why it's valuable for PE:** Shows market dynamics that point data (bar charts) misses. "Industrial automation deal volume has been steadily eating into traditional manufacturing since 2020" is a strategic insight that drives thesis development.

**D3 module:** `d3-shape` (area, stack), `d3-scale`, `d3-axis`.

**Complexity:** Low-Medium. D3's stack layout handles the stacking math. Interactive features (hover to highlight one series, tooltip with values) add moderate complexity.

**Nexdata endpoints:** `pe_deals`, `market`, `job_postings`

---

#### 16. Gauge / Speedometer -- Score Displays

**What it shows:** Semi-circular or circular gauge showing a single score (0-100) with colored zones (red/yellow/green). Used for: overall deal score, data quality score, location score, risk score.

**Data needed:**
- Single composite score value
- Threshold definitions for zones

**Why it's valuable for PE:** Instant executive-level read on "how good is this?" Works well as the hero element on a company profile page or deal summary. Less information-dense than other charts but high emotional impact.

**D3 module:** `d3-arc` (part of `d3-shape`), or use `d3-kpi-gauge` npm package.

**Complexity:** Low. Essentially a single arc with color stops. Several plug-and-play D3 gauge libraries exist.

**Nexdata endpoints:** `pe_companies` (scores), `zip_scores`, `rollup_intel`

---

#### 17. Horizontal Bar Chart with Diverging Scale -- Competitive Positioning

**What it shows:** Companies listed vertically, with bars extending left (below benchmark) or right (above benchmark) from a center axis. Each bar shows deviation from peer median on a selected metric. Immediately shows leaders and laggards.

**Data needed:**
- `pe_companies` or `competitive`: metric values per company
- Peer set median/mean for the chosen metric

**Why it's valuable for PE:** More readable than standard bar charts for comparison. The diverging layout creates a natural "winners vs. losers" narrative. Useful in IC memos: "Target company is 15% above peer median on margins but 20% below on growth."

**D3 module:** `d3-scale`, `d3-axis`. Standard D3, no plugins needed.

**Complexity:** Low. Standard bar chart with a centered axis.

**Nexdata endpoints:** `pe_companies`, `peer_sets`, `competitive`

---

#### 18. Parallel Coordinates -- Multi-Dimensional Screening

**What it shows:** Each vertical axis represents a different metric (revenue, margins, growth, headcount, location score, etc.). Each company is a line connecting its values across all axes. Lines can be colored by cluster or filtered by brushing on any axis.

**Data needed:**
- `pe_companies`: 5-10 comparable numeric metrics per company
- Large enough dataset (20+ companies) to show patterns

**Why it's valuable for PE:** The most powerful screening visualization. Brushing on one axis (e.g., "EBITDA margin > 20%") immediately highlights which companies pass and filters the display. Combining brushes across multiple axes is visual SQL. Analysts can explore "show me companies with >$50M revenue AND >15% growth AND <5x leverage" without writing queries.

**D3 module:** `d3-scale`, `d3-axis`, `d3-brush`. No special plugin, but implementation requires careful axis management.

**Complexity:** High. Managing multiple parallel brushes, line highlighting, axis reordering, and performance with many lines is challenging. But the payoff is enormous for power users.

**Nexdata endpoints:** `pe_companies`, `peer_sets`, `competitive`, `market`

---

#### 19. Adjacency Matrix / Connection Grid -- Relationship Density

**What it shows:** A grid where both rows and columns are entities (PE firms, companies, people). Cell color intensity = strength of relationship (co-investments, shared board members, deal history). Rows/columns can be reordered by cluster to reveal groups.

**Data needed:**
- Relationship data from `pe_firms`, `pe_deals`, `people`, `company_people`
- Relationship strength scoring

**Why it's valuable for PE:** An alternative to force-directed graphs for dense networks. Matrices scale better (hundreds of entities) and reveal clusters more clearly. Useful for LP relationship mapping: "which GPs have the most co-investment overlap?"

**D3 module:** `d3-scale`, basic grid rendering. No special plugin.

**Complexity:** Medium. The rendering is simple (colored cells); the challenge is computing relationship scores and finding good row/column orderings (consider hierarchical clustering).

**Nexdata endpoints:** `pe_firms`, `pe_deals`, `pe_people`, `people`

---

#### 20. Candlestick / OHLC -- Public Comp Trading Data

**What it shows:** Standard financial candlestick chart showing open/high/low/close for publicly traded comparable companies. Include volume bars below and moving average overlays.

**Data needed:**
- Public market price data (from SEC/market data sources)
- Volume data
- Moving average calculations

**Why it's valuable for PE:** Relevant for public comp analysis during valuation and for tracking public portfolio companies post-IPO. Less central to PE than the other visualizations (PE is private markets), but expected by financial professionals and useful for public-market context.

**D3 module:** `TechanJS` (best option -- built specifically for this on D3), or `d3fc` (includes candlestick components).

**Complexity:** Medium. TechanJS provides pre-built components. Building from scratch with raw D3 is complex (proper wick/body rendering, volume alignment, crosshair interaction).

**Nexdata endpoints:** SEC data, market data feeds

---

## D3 Libraries & Extensions Reference

| Library | npm Package | Use Case | Notes |
|---------|------------|----------|-------|
| **d3-sankey** | `d3-sankey` | Deal flow, capital flow | Official D3 module for flow diagrams |
| **d3-force** | Built into D3 | Network graphs, relationships | Velocity Verlet integration; tune charge/distance |
| **d3-hierarchy** | Built into D3 | Treemaps, sunbursts, org charts, trees | Includes tree, treemap, partition, pack layouts |
| **d3-chord** | Built into D3 | Sector flow, co-investment | Requires adjacency matrix input |
| **d3-geo** | Built into D3 | Choropleth maps, location intel | Pair with `topojson-client` for boundaries |
| **d3-geo-projection** | `d3-geo-projection` | Additional map projections | AlbersUSA for US-focused maps |
| **d3-brush** | Built into D3 | Range selection, filtering | Key for time series zoom and parallel coords |
| **d3-zoom** | Built into D3 | Pan and zoom | Essential for maps and network graphs |
| **d3-transition** | Built into D3 | Animated transitions | Smooth state changes on data updates |
| **d3fc** | `d3fc` | Financial charts, WebGL rendering | Candlestick, OHLC, discontinuous scales, annotations |
| **TechanJS** | `techan` | Stock charts, technical analysis | Built on D3; candlestick, OHLC, indicators |
| **radar-chart-d3** | `radar-chart-d3` | Spider/radar charts | Community package; reusable, configurable |
| **d3-kpi-gauge** | `d3-kpi-gauge` | Gauge/speedometer displays | Plug-and-play KPI gauges |
| **Crossfilter** | `crossfilter2` | Real-time multi-dimensional filtering | Handles 1M+ records; pairs with any D3 chart |
| **topojson-client** | `topojson-client` | Efficient geo boundary data | 80% smaller than GeoJSON for same boundaries |

---

## Implementation Priority Roadmap

### Phase 1: Core Dashboard (Weeks 1-3)
1. **Sparkline Grid** (Low complexity, high density) -- portfolio health overview
2. **Radar Chart** (Medium complexity) -- deal/company scoring
3. **Heatmap Matrix** (Low-Medium complexity) -- benchmarking + data coverage
4. **Gauge Charts** (Low complexity) -- score displays on company profiles

### Phase 2: Strategic Visualizations (Weeks 4-6)
5. **Sankey Diagram** (Medium complexity) -- deal flow pipeline
6. **Choropleth Map** (Medium-High complexity) -- location intelligence
7. **Multi-Line Time Series** (Medium complexity) -- performance tracking
8. **Bubble Chart** (Low-Medium complexity) -- company comparison

### Phase 3: Advanced Analytics (Weeks 7-10)
9. **Force-Directed Network** (Medium-High complexity) -- relationship mapping
10. **Treemap** (Low-Medium complexity) -- portfolio composition
11. **Org Chart Tree** (Medium complexity) -- management assessment
12. **Waterfall Chart** (Medium complexity) -- value creation bridge

### Phase 4: Power User & Differentiation (Weeks 11-14)
13. **Parallel Coordinates** (High complexity) -- multi-dimensional screening
14. **Chord Diagram** (Medium complexity) -- sector/fund flows
15. **Sunburst** (Medium complexity) -- industry drill-down
16. **Stacked Area** (Low-Medium complexity) -- market composition trends

---

## Architecture Recommendation

Given Nexdata is a Python/FastAPI backend, the D3 visualizations would live in a frontend layer. Two approaches:

### Option A: React + D3 (Recommended)
- Use React for component lifecycle, state management, layout
- Use D3 for scales, layouts, and data transformations only (let React handle DOM)
- Libraries: `@visx/visx` (Airbnb's React+D3 library) wraps D3 primitives in React components
- Pros: Component reusability, React ecosystem, server-side rendering
- Cons: Learning curve for D3-in-React patterns

### Option B: D3 Direct (Simpler Start)
- Embed D3 charts directly in HTML pages served by FastAPI
- D3 manages its own DOM via `d3.select().append()`
- Pros: Faster prototyping, full D3 control, many examples to copy
- Cons: Harder to maintain, no component reuse, manual state management

### Data Flow
```
Nexdata API (FastAPI) --> JSON endpoints --> D3 visualization layer
                                           |
                     /api/v1/pe_companies --+--> Radar, Bubble, Heatmap
                     /api/v1/pe_deals ------+--> Sankey, Waterfall, Timeline
                     /api/v1/zip_scores ----+--> Choropleth, Gauge
                     /api/v1/people --------+--> Force Graph, Org Chart
                     /api/v1/market --------+--> Treemap, Sunburst, Stacked Area
```

---

## Impressive Financial D3 Dashboard Examples

1. **Bloomberg Terminal Web** -- Dense sparkline grids, real-time streaming candlesticks, heatmap market overview
2. **Observable D3 Gallery** (observablehq.com/@d3) -- Official examples including zoomable sunbursts, chord diagrams, and force layouts
3. **D3 Graph Gallery** (d3-graph-gallery.com) -- 400+ reproducible examples with code
4. **d3fc Financial Charts** (d3fc.io) -- WebGL-accelerated candlestick charts handling 100K+ data points
5. **Visual Cinnamon** (visualcinnamon.com) -- Award-winning D3 radar charts and creative layouts by Nadieh Bremer
6. **Sisense D3 Dashboards** -- Enterprise D3 integration for KPI dashboards with real-time filtering via Crossfilter

---

## Key Takeaways

1. **Start with information density, not flash.** PE professionals value sparklines, heatmaps, and bullet charts that pack maximum data into minimum space over animated 3D charts.

2. **The Sankey deal pipeline and Radar scoring chart are the two visualizations that will most differentiate Nexdata** from spreadsheet-based workflows. Build these first.

3. **Geographic visualization (choropleth) directly leverages Nexdata's unique data** -- ZIP scores, site intel, location diligence. No competitor has this for PE.

4. **Use `d3fc` for financial time series** rather than building candlestick/OHLC from scratch. It adds WebGL rendering, discontinuous time scales (skip weekends), and annotation support.

5. **Crossfilter + D3 is the "secret weapon"** for interactive dashboards. It enables real-time filtering across linked charts with millisecond response times on datasets up to 1M records.

6. **Force-directed graphs are visually impressive but require careful performance tuning.** Start with smaller networks (<200 nodes) and add WebGL rendering later for scale.

Sources:
- [D3.js in Financial Analytics - Real-World Examples](https://moldstud.com/articles/p-d3js-in-financial-analytics-real-world-examples-and-best-practices-for-data-visualization)
- [Building a Complex Financial Chart with D3 and d3fc](https://blog.scottlogic.com/2018/09/21/d3-financial-chart.html)
- [TechanJS - Financial Charting on D3](https://techanjs.org/)
- [D3FC - Financial Chart Components](https://d3fc.io/)
- [D3 Graph Gallery](https://d3-graph-gallery.com/)
- [D3.js Official - d3-force](https://d3js.org/d3-force)
- [D3.js Official - d3-hierarchy](https://d3js.org/d3-hierarchy)
- [D3.js Official - d3-chord](https://d3js.org/d3-chord)
- [D3 Sankey on GitHub](https://github.com/d3/d3-sankey)
- [Radar Chart D3 by Nadieh Bremer](https://www.visualcinnamon.com/2015/10/different-look-d3-radar-chart/)
- [Observable - Zoomable Sunburst](https://observablehq.com/@d3/zoomable-sunburst)
- [From Pipeline to Portfolio - VC Investment Visualization](https://creately.com/blog/diagrams/from-pipeline-to-portfolio-using-visualization-to-track-and-improve-vc-investment-processes/)
- [JavaScript Chart Libraries in 2026](https://www.luzmo.com/blog/javascript-chart-libraries)
- [D3 KPI Gauge](https://www.npmjs.com/package/d3-kpi-gauge)
- [Choropleth Maps - D3 Graph Gallery](https://d3-graph-gallery.com/choropleth.html)
- [Supply Chain Visualization with Sankey](https://github.com/csuavet/supply-chain-visualization)
- [Awesome D3 - Curated List](https://github.com/wbkd/awesome-d3)
- [KDnuggets - Creating Interactive Dashboards with D3.js](https://www.kdnuggets.com/creating-interactive-dashboards-with-d3-js)
