# Nexdata Atlas — Launch Guide

**Date:** 2026-05-27 *(refreshed during PLAN_073 Phase A)*
**Audience:** you, the launcher. Click through every URL, see every feature, verify everything works.
**Length:** ~15 min read · ~30 min clickthrough
**Status:** PLAN_073 (Atlas Pilot commerce simulator) Phase A largely shipped. The map now has a **real basemap** and **tract-grain demographic layers** as the foundation for the focal-node UX. Phases B-G follow.

## What changed since last edit

- **Real basemap** — Carto Dark Matter tiles now render under the choropleth. The map looks like a map. ([SPEC_077 A.1, commit 9e16920](http://localhost:3001/atlas.html))
- **Default choropleth opacity dropped 85% → 55%** so streets are visible underneath.
- **County click auto-zooms to neighborhood scale (~zoom 14)** instead of zoom 8 — trade-area scale. [SPEC_077 A.4]
- **78,383 census tracts ingested** into `geojson_boundaries` ([SPEC_077 A.2, tracts shipped, ZCTAs deferred to SPEC_077b])
- **4 new tract-grain demographic layers** registered: median income, median age, population, owner-occupied housing. The frontend auto-switches boundary grain (county ↔ tract) when you toggle these. [SPEC_077 A.3 + A.4]
- **Boundary "glass shards" bug fixed** — adjacent counties now share clean borders. [SPEC_076, commit e15653c]
- **PLAN_073 rev_01 committed** — pivot to LLM-piloted commerce simulator for retail site-selection consultants. Phase A complete; Phases B (agent shell), C (focal-node + competition + demand), D (suppliers + logistics), E (Atlas Pilot agent quality), F (macro stress), G (sharing) remain.

> **Companion docs**
> - `docs/LAUNCH_READINESS.md` — the strategic "should I launch?" doc
> - `docs/ATLAS_TOUR.md` — the developer/QA runbook
> - **This doc** — the user-facing click-through showcase. If you only read one, read this one.

---

## How to use this guide

Three modes:

| Time | Mode | What you do |
|---|---|---|
| 5 min | **First look** | Paste 6 URLs, see 6 wow moments |
| 15 min | **Layer tour** | Click through all 22 layers grouped by domain |
| 30 min | **Feature tour** | Every dynamic-UI feature with how to trigger and what it teaches |

Each URL is clickable. Paste into the browser at `http://localhost:3001/atlas.html` (local) or your production URL once deployed.

---

## Mode 1 — Five-minute first look

Six URLs. Paste, look, get the gist of what we built.

### 1. Default landing
[`/atlas.html`](http://localhost:3001/atlas.html)

**What you should see:**
- Counters in header tween up from 0: **22 layers · 10 domains · 3,143 counties · 1.07M EPA facilities · 51,093 FEMA decls**
- Disaster risk choropleth (NRI) colors every US county
- Brushed histogram of NRI values at the bottom of the layer panel — drag a range, watch out-of-range counties dim
- Bottom-right legend shows the active layer

### 2. Compelling compare: wealth × disaster risk
[`/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri&place=48201`](http://localhost:3001/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri&place=48201)

**What you should see:**
- Map recolored with a 3×3 bivariate scale (the legend grid bottom-right)
- Harris Co (Houston) panel opens on the right showing 8 layer values
- Each layer card has a mini distribution histogram with a cyan marker at Harris's value + percentile readout
- A calendar heatmap under FEMA shows 27 declarations across years

### 3. Time-travel: FEMA scrubber
[`/atlas.html?layer=disaster_fema_declarations`](http://localhost:3001/atlas.html?layer=disaster_fema_declarations)

**What you should see:**
- A year slider docked at the top center: "FEMA YEAR · ALL"
- Drag it through 1999 → 2026 — Katrina (2005) lights up the Gulf, Sandy (2012) lights up NY/NJ, Beryl/Helene (2024) light up the South

### 4. Time-travel: CBP scrubber 🆕
[`/atlas.html?layer=econ_cbp_establishments_county`](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_county)

**What you should see:**
- Same year slider, now labeled "CBP YEAR"
- Drag through 2018 → 2022 — establishment counts shift, 2020 COVID hits visible, 2022 recovery patterns
- Same UI pattern as FEMA scrubber — proves the scrubber generalizes to any cascade-capable layer

### 5. Migration arcs
[`/atlas.html?layer=demo_irs_migration_net_agi`](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi)

**What you should see:**
- Choropleth auto-dims to 22% so arcs read clearly
- ~80 animated amber arcs flow across the map (IRS county-to-county top migration $-flows)
- The NYC → Miami corridor is the most visible thing
- LA → Orange County cluster on the West Coast
- Hover an arc → origin / dest / AGI / return-count tooltip

### 6. Recent Activity feed 🆕
[`/atlas.html?recent=1`](http://localhost:3001/atlas.html?recent=1)

**What you should see:**
- Floating panel auto-opens top-right: "Recent activity"
- Mixed feed of FEMA + SEC items (5 each by per-source allocation)
- Each item: source chip (FEMA cyan, SEC indigo) · date · type chip · title · place
- FEMA items: click → opens place panel + map fits to county
- SEC items: ↗ arrow indicates external link; click opens the EDGAR filing URL in a new tab

If all six look right, the system is working end-to-end.

---

## Mode 2 — Fifteen-minute layer tour

All 22 layers grouped by domain. Click each — the map recolors / overlays / shows a slider.

### Demographics (3 layers · all county)
| Layer | Why it's interesting |
|---|---|
| [`demo_acs_median_income`](http://localhost:3001/atlas.html?layer=demo_acs_median_income) | ACS B19013 2023 5-year — true county median income, $16k-$179k range |
| [`demo_irs_county_agi_per_return`](http://localhost:3001/atlas.html?layer=demo_irs_county_agi_per_return) | IRS SOI — what filers actually earn (excludes non-filers, complementary signal) |
| [`demo_irs_migration_net_agi`](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi) | Auto-shows the migration arcs (Wave 4 feature) |

### Disaster (2 layers · all county)
| Layer | Why it's interesting |
|---|---|
| [`disaster_nri`](http://localhost:3001/atlas.html?layer=disaster_nri) (default) | FEMA composite risk score 0-100 |
| [`disaster_fema_declarations`](http://localhost:3001/atlas.html?layer=disaster_fema_declarations) | Auto-shows the year scrubber 1999-2026 |

### Economy (4 layers — was 2, **+2 from SPEC_074/075**)
| Layer | Grain | Why it's interesting |
|---|---|---|
| [`econ_federal_dollars`](http://localhost:3001/atlas.html?layer=econ_federal_dollars&place=51013) | county | Federal contract dollars FY24; Arlington VA tops at $13.5B |
| [`econ_cbp_establishments_county`](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_county) 🆕 | county | Multi-year CBP with scrubber 2018-2022; LA Co tops at 304k establishments |
| [`econ_cbp_establishments_state`](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_state) | state | Coarser state rollup (kept for compat) |
| [`econ_sec_active_filers`](http://localhost:3001/atlas.html?layer=econ_sec_active_filers) 🆕 | state | SEC-registered companies by HQ state; CA tops at 322 |

### Energy (2 layers · both point)
| Layer | Count | Best viewed with |
|---|---|---|
| [`energy_power_plants`](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=energy_power_plants) | 14k | Overlaid on disaster_nri |
| [`energy_substations`](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=energy_substations) | 8.7k | Overlaid on disaster_nri |

### Environment (3 layers · all point)
| Layer | Count | Try the heat toggle |
|---|---|---|
| [`env_epa_facilities`](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=env_epa_facilities) | up to 10k | 🔥 Heat button reveals industrial corridors |
| [`env_brownfields`](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=env_brownfields) | up to 10k | Heat reveals legacy-industrial regions |
| [`env_water_monitoring`](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=env_water_monitoring) | up to 10k | Dots dominate near major rivers |

### Finance (1 layer · county)
| Layer | Why it's interesting |
|---|---|
| [`finance_fdic_county_deposits`](http://localhost:3001/atlas.html?layer=finance_fdic_county_deposits&place=48201) | Avg deposits per bank, latest quarter; Harris panel shows 168 quarterly sparkline points back to 1984 |

### Infrastructure (3 layers)
| Layer | Grain | Why it's interesting |
|---|---|---|
| [`infra_broadband_subscription`](http://localhost:3001/atlas.html?layer=infra_broadband_subscription) | county | ACS B28002 demand-side: 35% (rural) → 100% (rich urban) |
| [`infra_data_centers`](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=infra_data_centers) | point | PeeringDB inventory |
| [`infra_fcc_providers_state`](http://localhost:3001/atlas.html?layer=infra_fcc_providers_state) | state | FCC supply-side (state-only; county is SPEC_072b deferred) |

### Real Estate (1 layer · county)
| Layer | Why it's interesting |
|---|---|
| [`realestate_building_permits`](http://localhost:3001/atlas.html?layer=realestate_building_permits) | Where construction is happening |

### Trade (1 layer · state)
| Layer | Why it's interesting |
|---|---|
| [`trade_exports_state`](http://localhost:3001/atlas.html?layer=trade_exports_state) | State exports |

### Transport (2 layers)
| Layer | Grain | Why it's interesting |
|---|---|---|
| [`transport_airports`](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=transport_airports) | point | Airport inventory |
| [`transport_rail_density`](http://localhost:3001/atlas.html?layer=transport_rail_density) | state | Rail segment count by state |

---

## Mode 3 — Thirty-minute feature tour

Every dynamic UI feature with how to trigger.

### Coupling — the addictive loop

#### Bivariate compare (3×3 color scale)
- Click **"Compare layers"** in header → message asks for a county-grain B-axis
- Click any county-grain layer in the left panel
- Legend bottom-right swaps to a 9-cell grid
- Tooltip on counties shows both values

Try these pairings:
- [Wealth × disaster risk](http://localhost:3001/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri) — rich counties in high-risk zones
- [Broadband × income](http://localhost:3001/atlas.html?layer=infra_broadband_subscription&compare=demo_acs_median_income) — digital divide visible
- [Federal $ × wealth](http://localhost:3001/atlas.html?layer=econ_federal_dollars&compare=demo_acs_median_income) — contractor-rich DC metro
- [CBP × broadband](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_county&compare=infra_broadband_subscription) — business density vs connectivity

#### Brushed histogram (drag to dim)
- With any choropleth active, the histogram appears in the bottom of the layer panel
- Click-drag a range across it
- Outside-range counties dim to 10% opacity on the map
- Release to restore

#### Distribution + percentile (per place)
- Click any county on the map
- Right panel opens with all layer values
- Each layer card shows a mini histogram with a cyan vertical marker at the clicked county's value
- Plus an "Nth percentile across counties" readout

#### Scatter pair
- Click **"🔬 Scatter"** in header
- Floating panel opens with two layer pickers
- Pick x and y → scatter renders (3,000 county dots)
- Hover dots → county FIPS tooltip

### Motion — animate to teach

#### Migration arcs
- Auto-shows when [`demo_irs_migration_net_agi`](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi) is active
- 80 quadratic-Bezier arcs flow with a `stroke-dasharray` animation
- NYC → Miami most visible
- Choropleth auto-dims to 22% so arcs read

#### Time-cascade scrubber (FEMA — SPEC_066c)
- Auto-shows when [`disaster_fema_declarations`](http://localhost:3001/atlas.html?layer=disaster_fema_declarations) is active
- Year slider 1999 → 2026
- "ALL" button restores aggregate

#### Time-cascade scrubber (CBP — SPEC_066e generalized) 🆕
- Auto-shows when [`econ_cbp_establishments_county`](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_county) is active
- Year slider 2018 → 2022
- Same UI, generalized via SCRUBBER_LAYERS config

#### Smooth color transitions
- Any layer switch fades the choropleth over 400ms (cubicInOut easing)
- No flash, no jarring color jumps

#### Counter tween (landing only)
- Reload the page; header counters tween from 0 over 800-1300ms

### Coupling — control

#### Opacity slider
- The bottom of the layer panel has a 0-100% slider
- Drag → every county re-alphas instantly
- Migration view auto-sets it to 22%

### Coupling — read-only

#### Sparklines (place-panel only)
- Open [`/atlas.html?layer=finance_fdic_county_deposits&place=48201`](http://localhost:3001/atlas.html?layer=finance_fdic_county_deposits&place=48201)
- Three sparklines appear in the right panel:
  - FEMA Disaster History → declarations-per-year since 1999
  - Bank Deposits → 168 quarterly points since 1984
  - Net Migration → 2018-2021 net AGI flow

#### Calendar heatmap (place-panel only)
- For any county with FEMA events, a 28-year × 12-month grid below the FEMA card
- Hover cells → year/month/count tooltip
- Best example: [Harris Co TX](http://localhost:3001/atlas.html?place=48201) (27 events with Hurricane Beryl 2024-07 + 2021-02 ice storms most visible)

### Point-overlay heat toggle 🔥

- With any point overlay active, the **🔥 Heat** button shows in top-right map actions
- Click → cyan dots become a KDE blob layer
- Click again → return to dots
- Best example: [EPA facilities heat mode](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=env_epa_facilities) then click 🔥

### Recent Activity feed 📰 🆕

- Click **📰 Recent** in header (or [direct link](http://localhost:3001/atlas.html?recent=1))
- Floating panel: 50 most-recent items mixed FEMA + SEC (10 each by per-source allocation)
- FEMA items: click → fit map to county + open place panel
- SEC items: ↗ icon → click opens EDGAR filing URL in new tab

---

## Compelling place deep-dives

Stories the data tells when you click a specific county:

### [Arlington County VA (51013) — Pentagon-adjacent federal capital](http://localhost:3001/atlas.html?layer=econ_federal_dollars&place=51013)
- Federal contracts FY24: **$13.54B** (99th percentile)
- 8+ layer values populated

### [Harris County TX (48201) — Houston-metro everything](http://localhost:3001/atlas.html?place=48201)
- 8 layer values; most data-rich county we cover
- Federal $: $5.66B (DoD + NASA Johnson)
- Median income: $73,104
- Broadband: 91%
- FDIC: $1.47M avg deposits/bank
- 27 FEMA declarations · CBP 109,874 establishments (2022, up from 103k in 2018)
- Calendar heatmap reveals disaster clustering

### [LA County CA (06037) — the biggest county](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_county&place=06037)
- 304,305 business establishments — top in the country
- $11B+ federal contracts
- 24,198-return migration outflow to Orange Co per year

### [New York County NY (36061) — migration source](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi&place=36061)
- Origin of the thickest arcs in the migration view
- Top single flow: Manhattan → Miami-Dade ($3B AGI/year)

---

## API reference (for verification without the browser)

```bash
# Registry — 22 layers across 10 domains
curl -s http://localhost:8001/api/v1/atlas/layers \
  | jq '[.layers_by_domain | to_entries[] | {(.key): (.value|length)}]'

# Boundaries — county geometry, ~1.2MB compressed
curl -s "http://localhost:8001/api/v1/atlas/boundaries?geo_level=county"

# Choropleth values for one layer
curl -s http://localhost:8001/api/v1/atlas/layer/demo_acs_median_income \
  | jq '{grain, count: (.values|length), legend}'

# Per-place multi-layer aggregate
curl -s http://localhost:8001/api/v1/atlas/place/48201 \
  | jq '.layers | to_entries | map({layer:.key, value:.value.value, unit:.value.unit})'

# Series for sparkline (FEMA per year)
curl -s 'http://localhost:8001/api/v1/atlas/place/48201/series?layer=disaster_fema_declarations' \
  | jq '{kind, n: (.points|length), first: .points[0], last: .points[-1]}'

# Calendar events (raw FEMA declarations)
curl -s 'http://localhost:8001/api/v1/atlas/place/48201/events?source=fema&limit=5' \
  | jq '.events'

# Migration top-5 flows
curl -s 'http://localhost:8001/api/v1/atlas/migration?top_n=5' | jq

# FEMA time-cascade (the scrubber data)
curl -s http://localhost:8001/api/v1/atlas/layer/disaster_fema_declarations/cascade \
  | jq '{years, non_zero: .meta.non_zero_cells, harris_2024: .values_by_year["2024"]["48201"]}'

# CBP time-cascade 🆕
curl -s http://localhost:8001/api/v1/atlas/layer/econ_cbp_establishments_county/cascade \
  | jq '{years, harris_growth: [.values_by_year["2018"]["48201"], .values_by_year["2022"]["48201"]]}'

# Recent Activity (merged FEMA + SEC)
curl -s 'http://localhost:8001/api/v1/atlas/recent?sources=fema,sec&limit=10' \
  | jq '.items | map({date, source, type, title: (.title[0:40])})'
```

---

## Pre-launch verification checklist

Walk this once before flipping the public DNS.

### Local smoke (every commit-ish)
```bash
python scripts/smoke_atlas.py           # 40/40 in ~90s
docker exec nexdata-api-1 python -m pytest tests/test_spec_06*.py tests/test_spec_07*.py -q
```

### Cloud DB connectivity
- [ ] `/health` returns 200
- [ ] `/atlas/layers` returns 22 layers
- [ ] `/atlas/layer/disaster_nri` returns ≥3,000 values
- [ ] `/atlas/place/48201` returns 8+ layer values

### Critical user-paths (browser, manual)
- [ ] Landing renders with map + counters within 3s
- [ ] Toggle 3 different layers, each recolors
- [ ] Click a county → place panel populates
- [ ] Click 📰 Recent → panel opens with items
- [ ] Drag the FEMA scrubber → choropleth recolors per year
- [ ] Drag the CBP scrubber → choropleth recolors per year
- [ ] Bivariate compare → 3×3 grid in legend
- [ ] Share view → link copies; opening the link reproduces the exact state

### Pre-deploy chores
- [ ] `frontend/atlas.html` `const API` constant points to production URL
- [ ] CORS configured on the API for the production frontend domain
- [ ] Robots.txt allows crawling; sitemap includes all 22 layer URLs
- [ ] OG meta tags present for social-card previews
- [ ] Uptime monitor watching `/health`
- [ ] One `python scripts/smoke_atlas.py --base PROD_URL --api PROD_API_URL` pass

### Post-deploy
- [ ] Visit at least one URL on a mobile phone — graceful, not perfect
- [ ] Open one URL from an incognito window — no cached-auth surprises
- [ ] Check `atlas_events` table is receiving telemetry within 5 minutes

---

## What's in v1 vs what's not

### In v1 (now)
- 22 layers across 10 domains, all queryable
- 14 API endpoints, telemetry on every interaction
- 10+ dynamic UI features (bivariate, scatter, brushed histogram, sparklines, calendar heatmap, FEMA + CBP scrubbers, migration arcs, heat-mode toggle, smooth transitions, opacity)
- Recent Activity feed (FEMA + SEC)
- Deep-link URLs for every state
- Honest data citations (vintage, grain, coverage per layer)
- Headless smoke harness (40/40 scenarios) for regression defense

### Not in v1 (clean follow-ons)
- Real FCC BDC county broadband (currently using ACS B28002 subscription as proxy) — SPEC_072b
- Per-NAICS-2 CBP slicing (currently NAICS=00 total only)
- Sub-awards from USAspending — SPEC_071-stretch
- 8-K filings in Recent Activity (sec_8k table is empty)
- Swipe compare + lasso select — SPEC_066d (deferred)
- Mobile-optimized layout
- CSV export, API keys, paywall — all gated on Phase-2 loop measurement

---

## tl;dr

If you can paste 6 URLs in your browser, see 6 wow moments, and the API responds 200 across the board, **you're launch-ready**.

The 30-minute mode walks you through every feature so you can confidently show this to other people. The pre-launch checklist is the final-check before flipping public DNS.

Per `LAUNCH_READINESS.md` §9: ship as a **soft public beta**, no announcement. Let telemetry tell you who the wedge audience is over the 30-day Phase-2 measurement window.
