# PLAN 073 — Atlas Commerce Simulator (the pivot)

**Status:** Draft — awaiting approval
**Date:** 2026-05-26
**Phase:** New program — supersedes PLAN_066 v3 as the headline product
**Replaces / demotes:** PLAN_066 v3 ("general explorer"), PLAN_068 ("growth & distribution" as currently scoped), PLAN_072 §future-monetization assumptions
**Preserves:** all SPEC_064 / 065 / 066 / 066b / 066c / 066e / 067 / 070-075 infrastructure — the engine stays, the product reframes

---

## 1 · The honest read of what we have

The Atlas as built (22 layers, 14 endpoints, 40/40 smoke, FEMA scrubber,
Recent Activity, etc.) is an impressive *tool*, but it is a **weak
product for any audience we can articulate**. It works at county grain,
has no basemap, no competition data, no demand surface at sub-county
detail, and offers no workflow beyond "browse layers."

For the "general curious citizen" hypothesis, the explorer is mildly
interesting but boring after 5 minutes — no narrative, no
personalization, no comparison engine. PLAN_066 v3's bet was *"let
telemetry name the wedge"* — but a tool that produces no return-visit
behavior produces no telemetry to learn from. We were building the
measurement instrument before building something worth measuring.

This plan **replaces that strategy** with a specific audience, a
specific job-to-be-done, and a product that does it.

---

## 2 · The audience and the job

**Audience: solo / boutique site-selection consultants for retail.**

Their daily job:
> "Evaluate whether a specific street address is a good location for
> a specific retail concept (e.g., a chair store, a coffee shop, a
> tutoring center, a pet supplies store)."

What they *actually* do today:
- Cobble together Census Reporter (demographics by ZIP), Google Maps
  (competing businesses), client-provided traffic studies, occasional
  Yelp pulls, and a pile of municipal data
- The mid-market tools they can't afford: Placer.ai ($50-150k/yr),
  Esri Business Analyst (enterprise), TradeArea Systems ($1-5k/mo),
  Buxton (enterprise)
- The output of their work: a PDF deck for the client showing trade-
  area demographics, competition map, traffic estimates, demand
  projections

**Their unmet need:** a single tool that gives a complete picture of
a specific address for a specific retail concept — demographics +
competition + supply chain + macro stress — in a sharable, free,
fast-to-use interface.

We're not competing with Placer.ai on data depth. We're owning
**unified visibility on free public data with cross-domain joins**
that nobody else publishes in one place.

---

## 3 · The product

### Core workflow (the focal-node loop)

1. User opens the Atlas (or follows a shared deep-link).
2. Header has a clear **"Plant a focal node"** action: pick a NAICS
   sector + drop a pin at a street address.
3. The map auto-zooms to that address at neighborhood scale (~zoom 14).
4. A **real basemap** (Carto dark or OpenStreetMap) renders below the
   choropleth so the user sees actual streets, parks, anchors.
5. The right panel transforms from "place values" into
   **"your business: NAICS 442110 chair retail at 1234 Lamar Ave,
   Austin TX"**, organized into four collapsible sections:
   - **Competition** — nearby retailers in the same NAICS family;
     count within 1, 3, 5 mile radius; nearest five named
   - **Customers** — demographic surface within the trade area;
     household income / age / housing density relevant to this NAICS
   - **Suppliers** — upstream NAICS sectors per the BEA Input-Output
     Use Table; nearest geographic representatives plotted as points;
     arcs flowing into the focal node, weighted by sector $-flow
   - **Logistics** — nearest freight nodes (rail, port, airport,
     distribution warehouses); driving distance + macro context
     (fuel-cost-sensitive sectors get a fuel-price overlay)
6. **Macro stress sliders** in a tray: interest rate, fuel cost,
   freight rate, employment. Each one re-weights the supplier arcs
   and the customer-demand surface in real time.
7. **The share URL** captures: focal NAICS, lat/lon, active macro
   overlays, basemap style, zoom. Consultant copies the URL into a
   deck for a client — client opens it, lands on the exact view.

### What the user sees, end-to-end, in one sentence

"I'm thinking about a chair store at this corner — here's who's
already selling chairs nearby, here's the demographic of buyers
within 3 miles, here are my likely suppliers and where they cluster,
here's the freight time + cost to get from them to me, and here's
how all of that shifts if interest rates rise 200bp."

---

## 4 · What we kill, demote, reuse

### Reused as-is
- All 14 API endpoints (the infrastructure for layers, places,
  series, events, recent activity, migration, cascades, telemetry)
- The boundary-serving system (extends to ZCTA + tract)
- The smoke harness pattern (every new feature gets a scenario)
- Deep-link URL state and the share-view loop
- Bivariate compare → repurposed as competition × demand overlay
- Migration arcs → the arc engine becomes supplier→focal +
  focal→customer arcs
- Brushed histogram → trade-area population filtering
- Calendar heatmap → "openings/closures in your area by year"
- Recent Activity → "recent business permits + openings in your trade
  area," with FEMA + SEC demoted
- 22 existing layers → most relevant; about half stay surfaced

### Demoted / removed from the headline UX
- **NRI as the default layer.** It's actuarial expected-annual-loss,
  not operational disruption risk. Site-selection consultants don't
  care about 100-year storm tail events; they care about how many
  days per year the location is closed. Replaced as default by a
  competition-density map at the focal NAICS.
- **The FEMA scrubber as a signature feature.** Demoted. It stays as
  an optional layer for the small subset of consultants doing
  hurricane-corridor work (FL gulf coast, NJ shore, LA), but is not
  the headline animation.
- **The general 22-layer panel** as the entry UX. Layers become
  *inputs to the simulator* rather than browsable objects. The
  layer panel still exists for power users but is no longer the
  primary surface.
- **The county-default landing.** Replaced by a focal-node-first
  workflow: empty map → "plant a focal node to begin."

### Wholly new
- ZCTA + census-tract boundary ingest + serving
- A real **basemap tile layer** under the choropleth
- Sub-county ACS layers (median income, age, household composition
  at tract grain)
- **Business-location point data by NAICS** — OSM extracts +
  USAspending POP geocodes + ZBP density (honest hybrid: points
  where we have them, density where we don't)
- **BEA Input-Output Use Table** ingest — the sector flow matrix
  that drives supplier arcs
- A **simulator service** — given (NAICS, lat/lon, macro state),
  returns the supplier nodes, customer surface, logistics nodes
- **Macro stress sliders** — small elasticity models tying
  FRED/EIA macro variables to the simulator's flow weights
- **Operational disruption risk** (replaces NRI as the risk layer) —
  derived from FEMA declaration frequency per county + utility outage
  data where available + flood-zone shape data
- A focal-node-first **onboarding flow**

---

## 5 · Phases

Each phase ships independently and is end-to-end usable; you stop at
any phase boundary and the product is coherent.

### Phase A — Hyper-local foundation (3 days)
**Goal:** the map finally looks like a map and zooms to neighborhoods.
- A.1 — Add a Carto-dark or OSM basemap tile layer under the
  choropleth. Three lines of Leaflet. **Instant "looks real."**
- A.2 — Ingest ZCTA boundaries from Census TIGER (~33k features).
  Ingest census-tract boundaries (~74k features). Same pattern as
  SPEC_065 county boundaries.
- A.3 — Add tract-grain ACS layers for demand surface: median income,
  median age, population density, owner-occupied housing rate. Same
  pattern as SPEC_070 county_acs.py.
- A.4 — Re-tune zoom defaults: clicking a place auto-zooms to 14
  (neighborhood scale). Boundary serving honors `geo_level=tract` /
  `zcta` query params.

**Acceptance:** map shows real streets; tract-grain demographic
layers render; clicking a place zooms to neighborhood.

### Phase B — Focal-node UX (4 days)
**Goal:** "plant your business" replaces "browse layers" as the
default workflow.
- B.1 — Header gains a **"Plant focal node"** primary action; opens
  a two-step picker: NAICS family (with sub-NAICS dropdown) + click-
  to-place on the map.
- B.2 — Focal node renders as a distinct glyph (pin + ring with
  NAICS label).
- B.3 — Right panel transforms to the focal-node panel with placeholder
  sections (competition / customers / suppliers / logistics — empty
  for now, populated in C and D).
- B.4 — Deep-link state extended:
  `?focal_naics=442110&focal_lat=30.2672&focal_lon=-97.7431`.
- B.5 — Telemetry: `focal_node_planted`, `focal_node_moved`.

**Acceptance:** user can pick "chair retail" + drop a pin in Austin
and see the focal-node UX activate, with empty supplier/customer/
logistics sections.

### Phase C — Competition + customer surface (6 days)
**Goal:** the first two of four focal-node sections work.
- C.1 — Ingest a baseline business-location dataset:
  - OSM POIs via Geofabrik US extract (~6M POIs, NAICS-mappable)
  - USAspending POP geocodes (federal contractors with real points)
  - Census ZIP Business Patterns (ZBP) → ZIP-grain counts where we
    lack points
- C.2 — Competition section: query "businesses in same NAICS family
  within N miles of focal." Show count at 1/3/5 mi radii; render
  nearest five as named pins; draw concentric trade-area rings.
- C.3 — Customer surface: query the demand-relevant ACS variables
  for the focal NAICS (per a small NAICS → demographic-profile
  lookup we hand-curate for retail families). Render as a tract-
  grain choropleth around the focal.
- C.4 — Honest data badges: "Y point estimates + Z density estimates"
  on each section so the user knows what's solid vs interpolated.
- C.5 — Telemetry: `competition_inspected`, `demand_surface_viewed`.

**Acceptance:** for a chair retailer at any Austin address, the user
sees competing furniture stores within 5 miles + the demographic
demand surface at tract grain.

### Phase D — Supplier + logistics arcs (7 days)
**Goal:** the supply chain visualization that's the actual product
moat.
- D.1 — Ingest the **BEA Input-Output Use Table** (CSV from BEA, ~400
  sectors × 400 sectors). Becomes our supplier-relationship graph.
- D.2 — For a focal NAICS, look up the top 5-10 upstream sectors and
  the $-flow weight per the IO table.
- D.3 — For each upstream sector, find nearest geographic
  representatives (use the OSM + ZBP data from Phase C, filtered to
  the upstream sector's NAICS).
- D.4 — Render arcs from those representatives into the focal node,
  width = sector flow $ × distance-discount factor. Reuses the SPEC_066b
  migration-arc rendering engine.
- D.5 — Logistics section: query nearest representatives of freight
  nodes (existing layers: airports, ports, rail; new ingest needed
  for FHWA distribution centers + intermodal facilities).
- D.6 — Tooltip on arc: "Upholstery wholesale ($X typical flow,
  N miles, ~Yh truck transit, fuel-cost-elastic by Z%)."
- D.7 — Telemetry: `supplier_node_clicked`, `logistics_node_clicked`.

**Acceptance:** the chair-store focal node shows arcs from upholstery
wholesalers + lumber yards + freight forwarding nodes, with realistic
geographic distribution and tooltips that explain the relationship.

### Phase E — Macro stress overlays (5 days)
**Goal:** the macro→logistics connection the user asked for.
- E.1 — Pull from existing FRED ingest (we have it): interest rates,
  unemployment, GDP growth. Add EIA fuel prices.
- E.2 — Build a small elasticity model: for each macro variable, how
  much does it shift supplier arc weights and customer-demand
  intensity? Hand-curated coefficients per retail NAICS family, with
  honest "model assumption" badges.
- E.3 — UI tray with macro sliders: each slider has a baseline (live
  current value) and a stressed value (drag to ±100% range).
- E.4 — Dragging a slider re-runs the simulator: arc widths re-render,
  demand surface re-colors, an inline narrative updates
  ("at +200bp interest rates, big-ticket retail demand drops 11%
  in your trade area").
- E.5 — Telemetry: `macro_stressed`, `macro_reset`.

**Acceptance:** a consultant can show their client "here's the chair
store today, here's the same chair store under a 200bp rate hike +
$1.50/gal fuel-price increase."

### Phase F — Sharing / storytelling layer (ongoing)
**Goal:** consultants share URLs that become Atlas distribution.
- F.1 — Server-side rendered preview snapshot per focal-node URL so
  social-card previews look right (Cloudflare Workers or Cloud Run job).
- F.2 — A small library of pre-baked "case study" focal-node URLs that
  demonstrate the product — these become the marketing surface.
- F.3 — Per-focal-node LLM-generated insight: "the 3 most unusual
  things about this site for chair retail" (Anthropic API call with
  the layer values + comparison to peer sites, cached by URL).

---

## 6 · Operational disruption risk — the NRI replacement

This is its own deliverable inside the plan because it's user-flagged
and warrants its own thinking.

**Why NRI fails for this audience:**
- NRI = expected annual loss × population vulnerability across 18
  hazard types
- It composites 100-year tail events (Cat-5 hurricane making landfall)
  with frequent-low-impact events
- For Ocean County NJ: storm-surge expected loss dominates the score
  because of Sandy-scale risk → high NRI
- But the *lived experience* for a chair store on the boardwalk is
  "wind storms 2x/year close us for a day, water damage every 10 yr,
  catastrophic event every 30 yr"
- A site-selection consultant doesn't need actuarial expected loss —
  they need **how-many-days-closed-per-year + how-often-supply-chain-
  disrupted**

**What we build instead:**
- `risk_operational_disruption_county` layer:
  - 30% weight: FEMA declarations frequency (already ingested,
    SPEC_073 / 074 / 075 expose it as time-series)
  - 30% weight: utility outage frequency (Department of Energy
    publishes; needs ingest)
  - 20% weight: flood-zone exposure (FEMA Q3 Flood Hazard Layer;
    needs ingest)
  - 10% weight: heat extremes ≥95°F days per year (NOAA)
  - 10% weight: severe-weather days (NOAA SPC)
- Output: "expected business-closure days per year" — a number a
  consultant can put in a deck

Honestly framed: "this is operational disruption *frequency*, not
catastrophic-loss expected value. For insurance pricing, use NRI;
for site selection, use this."

The existing NRI layer **stays** for users who want it, with an
explicit note: *"NRI scores actuarial expected annual loss across
hazard types. It is not a measure of how often you will be closed."*

---

## 7 · The data gap and the hybrid approach

The single hardest sourcing problem: **business-location points by
NAICS at sub-county grain.**

| Source | Coverage | Granularity | Free? |
|---|---|---|---|
| OSM Geofabrik US extract | ~6M US POIs; ~60% of retail | Lat/lon + tag-derivable NAICS | ✓ |
| USAspending POP geocodes | Federal contractors only | Lat/lon | ✓ |
| Census ZIP Business Patterns | All US businesses | ZIP × NAICS counts (no points) | ✓ |
| Census Economic Census | All US businesses | County × NAICS counts | ✓ |
| OpenCorporates | Most registered businesses | Address (geocoding needed) | ✓ (rate-limited) |
| Yelp Fusion API | Strong retail/restaurant | Lat/lon + rich attributes | Rate-limited free |
| SafeGraph Places | All US POIs | Lat/lon + rich attributes | Was free until 2023; paid now |
| Google Places | All US POIs | Lat/lon + rich attributes | $$$ at scale |

**Honest hybrid approach:**
- **Tier 1 (points):** OSM + USAspending POP — for ~50% of retail
  NAICS we get real coordinates
- **Tier 2 (density):** ZBP + Economic Census — where we don't have
  points, we render density choropleth at ZIP grain so the user sees
  "there are 18 furniture-related businesses in this ZIP" without
  pretending we know exactly where
- **Honest UI badges:** every section shows "N points + M density"
  so the user knows what's solid

A consultant doesn't need 100% point accuracy — they need *enough
to write a defensible deck*. Density + points hybrid does that.

A later paid tier (if/when monetization gates clear) could pipe in
SafeGraph or Yelp data with attribution, but the free tier never
requires it.

---

## 8 · What this plan refuses to do

- **No paid tier in v1.** Free, period. Monetization gates clear only
  after the loop proves itself with consultants.
- **No real-time data.** The bet is governed, dated, public data with
  honest provenance. Not "live foot traffic."
- **No mobile-native experience.** Desktop-first; site-selection
  workflow is a desk job.
- **No competitor-tier feature parity with Placer.ai.** Different
  product, different data, different audience.
- **No premature integrations.** No CRM hooks, no Slack bot, no
  PDF export of the deck. The deck output is "the consultant
  screenshots / shares the URL." Workflows come after audience.
- **No AI-chat-over-data.** Commoditizing fast; not the moat. We use
  LLM only for per-focal-node insight generation (a small surface
  with honest "model-generated" labels).

---

## 9 · Sequencing relative to existing work

**Stop:**
- The launch sequencing in `LAUNCH_READINESS.md` §10 as currently
  scoped (soft-launching the general-explorer Atlas). The Atlas is
  still going public, but not until Phase C-D ships so the audience
  who sees it can actually do something useful.
- The PLAN_068 "growth" thinking as scoped — those tactics (embeddable
  widget, social cards, EDO outreach) don't fit a site-selection-
  consultant audience. Replaced by F.1-F.3.
- PLAN_069 monetization remains explicitly gated on Phase-2 loop proof,
  but the loop measurement now happens against the commerce simulator,
  not the general explorer.

**Keep running in parallel:**
- PLAN_067 data backfills — already done; the layers we ingested
  (county wealth, federal $, broadband, SEC filers, CBP multi-year)
  are still useful, just demoted from headline UX to *inputs to the
  simulator*.

**New gate:**
- **GATE 0 — Phase C-D ship + 10 real consultants try it.** Before
  any public push, 10 actual site-selection consultants (recruit via
  LinkedIn, professional associations, /r/CRE etc.) walk through the
  simulator on a real site they're working on. Feedback collected.
  Only then does soft launch happen.

---

## 10 · Specs to draft under this plan

Numbered tentatively; each gets its own SPEC doc once approved:

| Phase | Spec | Title |
|---|---|---|
| A | SPEC_077 | Basemap + sub-county boundary ingest + tract ACS layers |
| B | SPEC_078 | Focal-node UX + deep-link state |
| C | SPEC_079 | Business-location point ingest (OSM + USAspending) |
| C | SPEC_080 | Competition + customer demand surface engine |
| D | SPEC_081 | BEA Input-Output Use Table ingest |
| D | SPEC_082 | Supplier + logistics arc renderer |
| E | SPEC_083 | Macro stress overlays + elasticity model |
| (any) | SPEC_084 | Operational disruption risk layer (NRI replacement) |
| F | SPEC_085 | Server-side preview + LLM per-site insight |

That's ~9 specs, ~25 working days, ~5-6 calendar weeks at a
reasonable pace. The product is end-to-end usable after Phase D
(~3 weeks); E-F are polish that drives the share/loop behavior.

---

## 11 · Definition of done

This plan closes when:
- All five phases A-E ship
- GATE 0 passes: 10 site-selection consultants try the simulator on
  real work and either *pay attention to it* or give feedback that
  shifts the next iteration
- The replaced "general explorer" framing is fully retired from the
  product surface (it remains in archived docs)
- The honest-data principles from the original Atlas survive into
  the new product: every section says where its data came from,
  what's a point vs a density estimate, what's a model assumption

---

## 12 · The honest unknowns

| Unknown | What to do about it |
|---|---|
| Will site-selection consultants actually use a free tool? | GATE 0 — talk to 10 |
| Is OSM business coverage good enough? | Spike Phase C.1 first, measure point-coverage % across 10 retail NAICS in 5 metros |
| Is the BEA IO table specific enough to be useful at the sector level retail consultants care about? | Spike Phase D.1: pick chair retail (442110), see if upstream sectors map to plottable NAICS |
| Will the macro stress sliders feel like a gimmick or like a real differentiator? | Phase E ships latest, gets first-round feedback at GATE 0 |
| Are there enough free public datasets to displace Placer.ai for the boutique-consultant tier? | If GATE 0 says "close but missing X," we add X in v1.5; if it says "no, missing too much," we restructure |

The biggest risk is the audience risk, not the engineering risk.
**Build phases A-C first, talk to consultants, then decide D-F.**

---

## 13 · What stays from the old Atlas, explicitly

To be clear: **none of the engineering from PLAN_064-072 is wasted.**

- Layer API, place API, series API, events API, recent API, migration
  API, cascade APIs — all reused
- Boundary serving — extends to ZCTA + tract
- Dynamic UI library (D3, Observable Plot, Leaflet.heat) — all reused
- Telemetry table + Phase-2 measurement framework — reused; what
  *changes* is what the metrics are measuring (focal nodes planted,
  supplier arcs clicked, macro stress events)
- 22 layers — most become inputs to the simulator
- Smoke harness, tests, deep-link URL state — all reused
- The honest-data principle — strengthened, not abandoned

What changes is the **product framing on top of all that engineering**.
The Atlas was a city. The commerce simulator turns the city into a
specific neighborhood you can live in.

---

## 14 · Refuses to do

- **Pre-build all 9 specs before talking to consultants.** GATE 0
  exists for a reason.
- **Re-debate the audience choice.** This plan picks site-selection
  consultants for retail. If feedback says wrong, we replan, not
  half-pivot mid-build.
- **Add features that compete with Placer.ai head-on.** Our wedge is
  the cross-domain join + macro stress + free, not foot-traffic depth.
- **Skip the Phase-2 measurement loop.** Discipline survives the pivot.
