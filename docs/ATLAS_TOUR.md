# Nexdata Atlas — click-through tour

A runbook for actually opening the Atlas and seeing what's there.
Every step is a URL you can paste into a browser, plus what to look
for and what the data is honestly saying. No login.

**Base URL** (local): `http://localhost:3001/atlas.html`
**API base**: `http://localhost:8001/api/v1/atlas`

> Prereqs — `docker-compose up -d` running; the cloud DB proxy is up
> on `127.0.0.1:5435`; API responds 200 to `/health`.

---

## 60-second first look

Three URLs. Paste, look, get the gist.

### 1. The landing — zero-query default
[`/atlas.html`](http://localhost:3001/atlas.html)

You should see:
- The **command-center counters tween up from zero** over ~1 second
  (22 layers · 10 domains · 3,143 counties · 1.07M EPA facilities ·
  51,093 FEMA decls).
- **Disaster risk choropleth** across every US county (the default
  `disaster_nri` layer), light blue = low risk → dark blue = high.
- The **brushed histogram of disaster-risk values** in the bottom of
  the layer panel — drag a range on it; outside-range counties dim.
- **Legend bottom-right** with the layer name + min/max.

### 2. The migration story — animated arcs
[`/atlas.html?layer=demo_irs_migration_net_agi`](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi)

You should see:
- Choropleth auto-dims to 22% (migration is an arc-first story).
- **80 animated amber arcs** flow across the map — top IRS county-to-
  county migration $-flows for the latest tax year.
- The **NYC → Miami corridor** is the most obvious thing on screen.
- **LA → Orange County** cluster on the West Coast.
- Hover an arc → tooltip with origin/dest/AGI/return-count.

### 3. The bivariate story — wealth × disaster risk
[`/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri&place=48201`](http://localhost:3001/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri&place=48201)

You should see:
- A **3×3 bivariate color grid** in the legend (high AGI × high risk
  = bright cyan corner; low AGI × low risk = dark gray corner).
- The map recolored by both layers simultaneously.
- **Harris County, TX** open in the right panel showing 8 layer
  values, each with a mini distribution histogram + a vertical
  marker at Harris's value + a percentile readout.
- Below the FEMA Disaster History card: a **28-year × 12-month
  calendar heatmap** of Harris's disaster declarations.

### 4. The time-cascade story — animate FEMA history
[`/atlas.html?layer=disaster_fema_declarations`](http://localhost:3001/atlas.html?layer=disaster_fema_declarations)

You should see:
- A **year slider docked at top-center of the map** ("FEMA YEAR · ALL").
- Drag the slider — the choropleth recolors to show only that year's
  declarations. 2005 lights up Hurricane Katrina; 2024 lights up
  Hurricane Beryl + Helene.
- "ALL" button restores the all-years aggregate.

### 5. The "what's new" story — Recent Activity
[`/atlas.html?recent=1`](http://localhost:3001/atlas.html?recent=1)

You should see:
- The **📰 Recent panel** auto-opens top-right with ~50 most-recent
  FEMA declarations.
- Each item: date · type chip (fire amber, hurricane cyan, winter
  storm light-blue) · title · place name.
- Click any item → map fits to that county, place panel populates,
  recent panel closes.

If those five look right, the system is working.

---

## Tour the layer registry — 20 layers, 10 domains

Click each layer in the left panel. Or use the URL `?layer={id}` to
jump directly. **County-grain layers recolor the choropleth; point
layers overlay cyan dots; state-grain layers are coarser.**

### Demographics (3 layers — all county)
| Layer ID | What it shows | Vintage | Honest about |
|---|---|---|---|
| `demo_acs_median_income` | Median household income | 2023 ACS 5-year | Single-point estimate from ACS; ±margin of error |
| `demo_irs_county_agi_per_return` | Avg AGI per tax return | 2018-2021 latest | Excludes non-filers (typically the lowest-income) |
| `demo_irs_migration_net_agi` | Net AGI inflow minus outflow (FY) | 2018-2021 | Two-county-pair flows only; doesn't count international |

[try income](http://localhost:3001/atlas.html?layer=demo_acs_median_income) ·
[try AGI/return](http://localhost:3001/atlas.html?layer=demo_irs_county_agi_per_return) ·
[try migration with arcs](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi)

### Disaster (2 layers — both county)
| Layer ID | What it shows | Vintage | Honest about |
|---|---|---|---|
| `disaster_nri` (default) | FEMA National Risk Index composite | periodic | Composite — masks hazard-mix differences |
| `disaster_fema_declarations` | Declared disaster count since 1999 | 1999-2026 | Includes territorial/aggregations; some non-5-digit FIPS |

[NRI default](http://localhost:3001/atlas.html?layer=disaster_nri) ·
[FEMA declarations](http://localhost:3001/atlas.html?layer=disaster_fema_declarations)

### Economy (4 layers)
| Layer ID | What it shows | Grain | Honest about |
|---|---|---|---|
| `econ_federal_dollars` | Federal contract dollars FY (prime) | county | Prime contracts only; sub-awards deferred |
| `econ_cbp_establishments_county` | CBP business establishments (latest yr) | county | **Has a time-scrubber** (5-year cascade 2018-2022) |
| `econ_cbp_establishments_state` | CBP establishments (state rollup) | state | Superseded by the county layer; kept for compat |
| `econ_sec_active_filers` | SEC-registered companies by HQ state | state | State-only — SEC carries no county address |

[federal dollars — Arlington VA in panel](http://localhost:3001/atlas.html?layer=econ_federal_dollars&place=51013) ·
[CBP county + scrubber](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_county) ·
[SEC active filers](http://localhost:3001/atlas.html?layer=econ_sec_active_filers)

### Energy (2 layers — both point overlays)
| Layer ID | What it shows | Count | |
|---|---|---|---|
| `energy_power_plants` | EIA power generation facilities | 14k | full coverage |
| `energy_substations` | HIFLD substation inventory | up to 10k | sampled cap |

[Power plants overlaid on NRI](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=energy_power_plants)

### Environment (3 layers — all point)
| Layer ID | What it shows | Honest about |
|---|---|---|
| `env_epa_facilities` | EPA regulated facilities, top violations | Sample of 1.07M; "top violations" filter |
| `env_brownfields` | EPA brownfield site inventory | Up to 10k of ~45k |
| `env_water_monitoring` | EPA water-quality monitoring sites | Up to 10k of ~33k |

[EPA facilities overlay](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=env_epa_facilities)

### Finance (1 layer — county)
| Layer ID | What it shows | Honest about |
|---|---|---|
| `finance_fdic_county_deposits` | Avg bank deposits per institution, latest quarter | Per-institution average — concentrated counties may look small |

[FDIC deposits](http://localhost:3001/atlas.html?layer=finance_fdic_county_deposits&place=48201)
— Harris Co opens with **168 quarters** of FDIC history as a sparkline
in the place panel.

### Infrastructure (3 layers)
| Layer ID | What it shows | Grain | Honest about |
|---|---|---|---|
| `infra_broadband_subscription` | % households with broadband (ACS) | county | Demand-side (who subscribes), not supply (who can) |
| `infra_data_centers` | PeeringDB data-center inventory | point | Listed in PeeringDB only |
| `infra_fcc_providers_state` | Distinct broadband providers | state | County coverage = SPEC_072b future work |

[broadband subscription](http://localhost:3001/atlas.html?layer=infra_broadband_subscription) ·
[data centers overlay](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=infra_data_centers)

### Real Estate (1 layer — county)
| Layer ID | What it shows | Honest about |
|---|---|---|
| `realestate_building_permits` | Building permits per county | Permit issuance ≠ completion |

[building permits](http://localhost:3001/atlas.html?layer=realestate_building_permits)

### Trade (1 layer — state)
| Layer ID | What it shows | Honest about |
|---|---|---|
| `trade_exports_state` | State exports, latest year | State-only |

[state exports](http://localhost:3001/atlas.html?layer=trade_exports_state)

### Transport (2 layers)
| Layer ID | What it shows | Grain |
|---|---|---|
| `transport_airports` | Airport inventory | point |
| `transport_rail_density` | Rail segment count | state |

[airports overlay](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=transport_airports)

---

## Tour the dynamic features

These layer on top of any choropleth — they're how the explorer
becomes *interesting to use*, not just functional.

### Coupling — the addictive loop

#### Bivariate compare
1. Open [`/atlas.html`](http://localhost:3001/atlas.html).
2. Click **"Compare layers"** in the header.
3. Click any other county-grain layer in the left panel (e.g. `demo_irs_county_agi_per_return`).
4. Watch the **map recolor with a 3×3 bivariate scale**. Legend bottom-right is now a 3×3 color grid.
5. Hover counties — tooltip shows **both** layer values.
6. Click "Compare layers" again to clear.

Try: [wealth × risk](http://localhost:3001/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri) ·
[broadband × income](http://localhost:3001/atlas.html?layer=infra_broadband_subscription&compare=demo_acs_median_income) ·
[federal $ × wealth](http://localhost:3001/atlas.html?layer=econ_federal_dollars&compare=demo_acs_median_income)

#### Brushed histogram → map dim
1. With any choropleth layer active, look at the **bottom of the layer panel**.
2. There's a small histogram of layer values.
3. **Click-drag a range** across it. Counties **outside the range dim
   to 10% opacity** on the map.
4. Release the brush → counties restore.

Best on: [federal dollars](http://localhost:3001/atlas.html?layer=econ_federal_dollars) — drag the brush to "very-high spending" and only the DC-metro / defense corridor stays lit.

#### Distribution + percentile (per-place)
1. With a choropleth active, click any county.
2. The **right panel** opens with each layer the place has values for.
3. **Each layer card has a mini distribution histogram with a vertical cyan marker** at the clicked county's value.
4. Below: **"Nth percentile across counties"** for the active layer.

Try [Harris Co TX, all layers](http://localhost:3001/atlas.html?place=48201) (Harris is the most populated county we have data for; 8 layer values).

#### Scatter pair
1. Click **"🔬 Scatter"** in the header.
2. A floating panel opens with two layer pickers (x and y).
3. Picks default to the active layer + a sensible second.
4. Change either — the scatter re-renders.
5. Hover dots — tip shows county geo_id.

### Motion — animate to teach

#### Migration arcs
- Active automatically when `demo_irs_migration_net_agi` is the base layer.
- 80 quadratic-Bezier arcs flow with a `stroke-dasharray` animation.
- Hover an arc → tooltip with origin / dest / AGI / return count.
- Click an arc → telemetry fires + toast.

[migration arcs](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi)

#### Smooth transitions
- Any layer switch fades the choropleth in over 400ms with `cubicInOut` easing.
- No flash, no jarring color jumps.

#### Counter tween (landing only)
- Reload the page. The header counters tween from 0 to their values over 800-1300ms.

### Control — opacity slider
- The bottom of the layer panel has an **Opacity slider 0-100%**.
- Drag it — every county on the choropleth re-alphas instantly.
- The migration view auto-sets it to 22%.

### Sparklines (place-panel only)
- Open [`/atlas.html?layer=finance_fdic_county_deposits&place=48201`](http://localhost:3001/atlas.html?layer=finance_fdic_county_deposits&place=48201).
- Scroll the right panel — three rows have **inline cyan sparklines**:
  - FEMA Disaster History → declarations-per-year since 1999
  - Bank Deposits per County → 168 quarterly points since 1984
  - Net Migration → 2018-2021 net AGI flow

### SPEC_066c additions (Wave 2 — "loop-compelling")

#### Calendar heatmap in place panel
- For any clicked county, beneath the FEMA layer card a
  **28-year × 12-month grid** colors cells by event count.
- Hover → tooltip `{year, month, count}`.
- Best example: [Harris Co TX](http://localhost:3001/atlas.html?place=48201)
  — 27 declarations span the grid; Hurricane Beryl 2024-07 + ice
  storms 2021-02 are the brightest cells.

#### EPA kernel-density heat toggle
- Open [`/atlas.html?layer=disaster_nri&overlay=env_epa_facilities`](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=env_epa_facilities).
- The "🔥 Heat" button appears in the top-right map actions.
- Click → cyan dots become a KDE blob layer (industrial corridors
  visible as bright clusters). Click again to return to dots.
- Works for any point overlay.

#### FEMA time-cascade scrubber
- Open [`/atlas.html?layer=disaster_fema_declarations`](http://localhost:3001/atlas.html?layer=disaster_fema_declarations).
- A **year slider docks at the top center**: "FEMA YEAR · ALL".
- Drag through 1999 → 2026. Watch the choropleth recolor per-year.
  Highlights: 2005 (Katrina), 2012 (Sandy), 2017 (Harvey/Maria),
  2024 (Beryl/Helene).
- "ALL" button restores the all-years aggregate.

#### CBP time-scrubber (SPEC_066e — generalized) 🆕
- Open [`/atlas.html?layer=econ_cbp_establishments_county`](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_county).
- Same year-slider docked at the top, labeled "CBP YEAR" instead.
- Drag through 2018 → 2022. Watch industry density shift — 2020 hits
  show COVID's establishment churn; 2022 shows recovery patterns.
- The scrubber is the same UI as FEMA, generalized via SCRUBBER_LAYERS
  config — any layer with a `/cascade` endpoint can plug in.

### SPEC_067 addition (Wave 3 — "Recent Activity")

#### 📰 Recent Activity panel
- Header button **📰 Recent** opens a floating right-side panel.
- Lists the 50 most-recent FEMA declarations across all counties.
- Each item: date · type chip (color-coded by category) · title ·
  place name.
- Click an item → **map fits to that county** + place panel opens + recent panel closes.
- Direct-link: [`?recent=1`](http://localhost:3001/atlas.html?recent=1).
- SEC + USAspending lanes arrive when PLAN_067 SPEC_074 + SPEC_071-stretch land.

---

## Compelling place deep-dives — stories the data tells

### Arlington County, VA (51013) — federal money capital
[click](http://localhost:3001/atlas.html?layer=econ_federal_dollars&place=51013)

- Federal contract dollars FY2024: **$13.54B** (Pentagon-adjacent).
- Percentile readout will say ~99th.

### Harris County, TX (48201) — Houston-metro everything-bagel
[click](http://localhost:3001/atlas.html?place=48201)

- 8 layer values; the most data-rich county we cover.
- Federal dollars: $5.66B (DoD + NASA Johnson).
- Median income: $73,104.
- Broadband: 91%.
- FDIC: $1.47M avg deposits/bank.
- 27 FEMA declarations since 1999 (hurricane belt).

### New York County, NY (36061) — Manhattan, migration origin
[click](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi&place=36061)

- The thickest origin arcs in the migration view start here.
- The top single flow: NY → Miami-Dade (~$3B AGI per year).

### Los Angeles County, CA (06037) — biggest county everything
[click](http://localhost:3001/atlas.html?layer=demo_acs_median_income&place=06037)

- Median income, 24,198-return inflow into Orange Co (06059).

---

## Deep-link URL reference

```
/atlas.html?layer={base}&compare={B}&overlay={point}&place={fips}&lat=&lon=&zoom=
```

Everything round-trips. Any URL you copy from the **"Share view"
button** in the header replays the exact state in a fresh browser.

Examples to bookmark:
- [Default disaster map](http://localhost:3001/atlas.html?layer=disaster_nri)
- [Migration arcs zoomed on East Coast](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi&lat=37.5&lon=-78&zoom=5)
- [Power plants on NRI](http://localhost:3001/atlas.html?layer=disaster_nri&overlay=energy_power_plants)
- [Broadband × income bivariate](http://localhost:3001/atlas.html?layer=infra_broadband_subscription&compare=demo_acs_median_income)
- [Federal $ × wealth bivariate, Arlington VA selected](http://localhost:3001/atlas.html?layer=econ_federal_dollars&compare=demo_acs_median_income&place=51013)

---

## What it doesn't say yet

Honest data caveats to be aware of when reading the map:

| Limitation | Why | Tracked in |
|---|---|---|
| No PostGIS on cloud → boundary geometry uses Python-thin simplification (1.2MB) instead of true Douglas-Peucker | `db-f1-micro` doesn't have PostGIS | SPEC_065 boundaries.py |
| FCC broadband is state-only — county supply-side coverage absent | FCC public API returns 405 for county; bulk-CSV is multi-day work | SPEC_072b (deferred) |
| Multi-year CBP not yet ingested — no time scrubber over industry | Only have CBP 2021 / 2022 | PLAN_067 SPEC_073 |
| SEC filings have no `filing_date` column → can't pin into Recent Activity | Existing `sec_company_metadata` table lacks the column | PLAN_067 SPEC_074 |
| USAspending sub-awards not ingested | Volume + linkage non-trivial | SPEC_071 follow-on |
| Migration: 2021 only at the moment | IRS SOI publishes ~2-year lag | data refresh, not new spec |
| Sparklines exist for 3 layers (FEMA, FDIC, IRS migration) | Per-layer series endpoint needs a kind-handler each | SPEC_066b §non-goals |

---

## Verifying API directly (no browser)

```bash
# Registry — should show 20 layers across 10 domains
curl -s http://localhost:8001/api/v1/atlas/layers | jq '[.layers_by_domain | to_entries[] | {(.key): (.value|length)}]'

# One layer's data
curl -s http://localhost:8001/api/v1/atlas/layer/demo_acs_median_income \
  | jq '{grain, value_count: (.values|length), legend}'

# Place aggregate (Harris Co TX)
curl -s http://localhost:8001/api/v1/atlas/place/48201 \
  | jq '.layers | to_entries | map({layer: .key, value: .value.value, unit: .value.unit})'

# Sparkline series (FEMA per year)
curl -s 'http://localhost:8001/api/v1/atlas/place/48201/series?layer=disaster_fema_declarations' \
  | jq '{kind, points}'

# Migration top-5 flows
curl -s 'http://localhost:8001/api/v1/atlas/migration?top_n=5' | jq
```

---

## When new layers / specs land

Re-run the layer registry probe to confirm what's new:
```bash
curl -s http://localhost:8001/api/v1/atlas/layers | python -c "
import json, sys
d = json.load(sys.stdin)
for dom, layers in d['layers_by_domain'].items():
    print(f'=== {dom} ({len(layers)}) ===')
    for l in layers: print(f'  {l[\"id\"]:40s} {l[\"grain\"]:6s} {l[\"label\"]}')
"
```

Add the new layer rows to the **Tour the layer registry** section above.
