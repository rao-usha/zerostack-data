# PLAN 070 — Atlas Dynamic UI: Coupling, Motion, Surprise

**Status:** Draft — awaiting approval
**Date:** 2026-05-23
**Phase:** 1b (sits between Phase 1 — *build the map* — and Phase 2 — *prove the loop*)
**Builds on:** PLAN_066 v3 (the general explorer), SPEC_065 (the layer + boundaries API, shipped `b893c2b`)
**Note on numbering:** previously placeholder-numbered PLAN_070 was the
"Learning Layer" (telemetry-driven card ranking). That moves to PLAN_071;
this slot now holds the Dynamic UI design — it's the more immediate need.

---

## 1 · Why this plan

PLAN_066 v3 ships the *explorer* (map + layers + click-to-cards). That makes
Atlas **functional.** But functional is not *interesting to use* — and an
exploration product that nobody wants to keep clicking has no loop, no
telemetry, no wedge to discover. The Phase-2 gate (PLAN_066 §9) reads
"does Dana — er, does *anyone* — return and follow?" The answer turns on UX
charisma as much as data quality.

This plan codifies what "dynamic UI" means for Atlas, the **feature catalog**
that earns it, and the **sequencing** that ships incrementally without
boiling the ocean. It is the design source-of-truth for SPEC_066 (map v1),
SPEC_066b (the dynamic-UI layer), and any later SPEC_066c.

## 2 · The principle — three pillars

A wall of D3 charts is not dynamic; it's decorated tables. Dynamic UI means
three things, in priority:

### Pillar 1 — Coupling (the addictive loop)
The map and the charts move *together.* Brush a histogram → counties on the
map highlight. Click a county → its sparklines fill in alongside. Scrub a
time slider → the map recolors. This is the "I can't stop clicking" effect
in Observable notebooks, Pudding pieces, FT/NYT graphics. It's the single
most important thing.

### Pillar 2 — Motion (animate to teach, not to decorate)
Transitions, scrubbers, animated arrivals. **Done well, motion teaches;
done badly, motion distracts.** Hard rule: every animation must reveal
something the static state didn't — change over time, relationships between
states, where a value sits in a distribution. *No decorative motion.* No
sparkles.

### Pillar 3 — Surprise (discovery > query)
The product offers things the user didn't ask for: anomaly highlights, "did
you know," correlations between layers a user would never have paired,
telemetry-driven "users who explored X also looked at Y." Surprise drives
return visits; query alone does not.

## 3 · The feature catalog

Tiered by *how well each feature plays to Atlas's actual data + breadth.*

### 🥇 Unique-to-Atlas — the distinctive moves

| Feature | Pillar | What it is | Why it's ours |
|---|---|---|---|
| **Bivariate choropleth** | coupling | One map, two layers, color encodes both (e.g., wealth × disaster risk) via a 3×3 or 9-cell color scale | We have many county layers worth pairing. No public-data aggregator does this. |
| **Brushed histogram ↔ map** | coupling | Pick a layer → histogram appears → drag a range → counties in range highlight on the map (and vice versa) | The core addictive loop. Works for every choropleth layer. |
| **Migration flow arcs** | coupling + motion | Animated arcs from origin → destination county, width = $$ migration | IRS SOI migration is *natively* flow data. Genuinely novel. |
| **FEMA time-cascade** | motion | Scrub 1999–2026, watch declarations appear on the map | We have 50k+ dated, county-located events. Time scrubber + map = period-piece compelling. |
| **EPA enforcement heatmap** | motion (subtle) | Kernel-density heatmap of 1.07M geocoded facilities; intensity = local density of high-violation sites | Volume is the *only* reason this is possible. |

### 🥈 Strong, standard, table-stakes for "dynamic"

| Feature | Pillar |
|---|---|
| Hover tooltips with mini-summary | coupling |
| Distribution-with-my-position panel (small histogram showing where my click sits) | coupling |
| Scatter-plot any two layers (correlation discovery) | coupling + surprise |
| Sparklines in the place panel for time-series layers | coupling |
| Calendar heatmap (daily intensity) for time-series | motion |
| Smooth animated transitions between layer toggles | motion |
| Layer opacity sliders | coupling |
| Command-center counters that tick up on arrival | motion (minimal) |

### 🥉 Nice but expensive — defer to SPEC_066c

| Feature | Why deferred |
|---|---|
| **3D extrusion** (heights = magnitude) | Requires deck.gl + a build step; breaks no-build constraint |
| **Side-by-side synced maps** | Genuinely useful for compare, but heavy frontend lift |
| **Swipe compare** (vertical line splits two layers) | Slick but not essential v1 |
| **Lasso-select counties** | Power-user feature; gate on telemetry signal |
| **"Surprise me" / anomaly highlighting / suggestion engine** | Gated on telemetry — only useful once usage signal exists |

### 🚫 Don't ship — ever
- Bespoke 3D globe / particle effects / WebGL eye candy with no information payload
- Animation that decorates without teaching
- Anything that looks "designer-y" but doesn't reveal information

## 4 · Tech stack — no-build, CDN drop-ins

| Library | Role | Size | Why |
|---|---|---|---|
| **Leaflet** | Map canvas, choropleth, point layers, hover, basic interactivity | ~42 KB | Already in PLAN_066 v3. Canvas renderer handles 3,279 polygons. |
| **D3.js v7** | Custom viz: bivariate scales, migration arcs, brushed histograms, SVG overlays | ~70 KB | Gold standard. Powers the *unique* features. |
| **Observable Plot** | Standard charts (histograms, scatter, sparklines) — declarative one-liners | ~30 KB | D3 + Plot is the modern combo. Plot for fast, D3 for bespoke. |
| **Leaflet.heat** | EPA kernel-density heatmap | ~5 KB | Single drop-in plugin. |

**Total: ~150 KB.** All CDN drop-ins, no build step, no bundler, no
npm. Honors the PLAN_066 v3 "single self-contained HTML, vanilla JS"
convention.

**Explicitly NOT v1:**
- deck.gl, MapLibre GL — vector / WebGL — bigger commit, build step
- React / Vue / Svelte — no framework
- Custom bundler — keep the single-file convention

## 5 · Sequencing across SPEC_066 / 066b / 066c

```
SPEC_066  (v1, must-have)        →  the explorer exists. Foundation.
       ↓
SPEC_066b (dynamic UI layer)     →  the explorer becomes interesting to use.
       ↓                             Ship this BEFORE Phase-2 measurement
                                     so the loop has a chance to retain.
       ↓
SPEC_066c (heatmaps, compare, time-cascade)  → after early telemetry signal,
                                                add the heavier features that
                                                wedge-discovery shows are wanted.
```

**The discipline:** SPEC_066 alone is *not enough* to fairly measure the
loop. Atlas is a UX product; an "explorer with no dynamism" undersells the
data and produces a misleading Phase-2 read. Ship SPEC_066 + 066b together
as the v1 surface; defer SPEC_066c to after first telemetry.

## 6 · Connection to wedge discovery

PLAN_066 v3 §9 says: *let telemetry name the wedge.* The dynamic-UI features
are also **telemetry surfaces** — every interaction is an event:
- `bivariate_pair_selected: {layer_a, layer_b}` — which pairings users actually try
- `histogram_brushed: {layer, range_min, range_max}` — what value ranges they care about
- `time_scrubbed: {layer, from_year, to_year}` — which time spans they watch
- `migration_arc_clicked: {origin, dest}` — which flows are interesting
- `scatter_explored: {x_layer, y_layer}` — which correlations they look for

This data **becomes the wedge discovery signal.** What people *do* with the
dynamic UI tells us which domains, comparisons, and patterns matter — the
input to PLAN_068 targeted-outreach and PLAN_069 monetization.

## 7 · Honest cuts + non-goals
- No deck.gl in v1 (means no 3D extrusion, no big-data particle clouds)
- No build pipeline
- No charts that don't reveal information ("decorative" rejected)
- No "AI-generated" insight prose — the moat is *visualization + provenance*, not LLM commentary
- No mobile-app shell — responsive web is enough

## 8 · Success metrics — telemetry-instrumented
Beyond PLAN_066 §9's base loop metrics:
- **Coupling engagement** — % of sessions that use brushing or layer toggling
- **Comparison engagement** — % that try bivariate or scatter
- **Time-engagement** — sessions that use the FEMA scrubber
- **Per-feature retention** — return rate of users who try feature X vs those who don't (does the bivariate map *make* people come back?)
- **The shareables** — which dynamic views get screenshotted/shared

## 9 · Risks
| Risk | Mitigation |
|---|---|
| D3 lift larger than estimated | Lean on Observable Plot for everything *standard*; reserve D3 for the unique features (bivariate, flow arcs) |
| Performance — 3,279 polygons + D3 overlays + transitions | Canvas renderer; throttle hover; lazy-load layers; measure |
| Feature creep | This plan + the spec split is the discipline. SPEC_066c features genuinely wait. |
| Bivariate / brushed-histogram is novel UX — users may not "get it" | Default to one-layer view; bivariate is an opt-in mode with a one-line affordance ("compare layers →"). |

## 10 · The 066/066b/066c handoff

| Spec | Ships | What user can do |
|---|---|---|
| **SPEC_066** | Leaflet map, layer panel, choropleth + point rendering, click→drill-down, hover, command counters, deep-link URL | Browse, toggle layers, see places. Functional. |
| **SPEC_066b** | Bivariate choropleth, brushed histogram, distribution position, migration arcs, sparklines, layer opacity, smooth transitions, scatter-plot pair | The *coupled* loop. Interesting to use. |
| **SPEC_066c (later)** | Time scrubber + cascade, EPA kernel-density heatmap, calendar heatmap, swipe-compare, lasso-select, surprise/anomaly | Power-user + curiosity-driven; ship after first telemetry signal informs which are wanted. |
