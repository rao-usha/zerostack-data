# Atlas — concrete use cases to click through

Paste-able pilot prompts + URLs. Each one shows a different surface working end-to-end. Use this as a 5-minute or 15-minute walkthrough of the product.

---

## 1. Quick fact — "what is X for one place"

**Try in the Ask the Pilot box:**

```
What is the median household income in Travis County, TX?
```

What happens:
- Agent calls `query_place(48453)` → reads all 9 layer values for Travis County
- Narrates: "$97,169 (2023 ACS 5-year)"
- ~15s wall clock, 2 loops

**Variants:**
- `What is the federal contract spending in Arlington County, VA?` → ~$13.5B (Pentagon-adjacent)
- `How many FEMA declarations has Harris County, TX had since 1999?` → 27
- `What is the broadband subscription rate in Cook County, IL?` → 89.2%

---

## 2. Comparison — "compare A and B on Y"

```
Compare Travis County, TX and Cook County, IL on median income and broadband
```

What happens:
- Agent calls `compare_places([48453, 17031])` → side-by-side
- Narrates: "$97,169 vs $81,797; 93.3% broadband vs 89.2%"
- Watch for **amber unlinked-claims warning** — when the model writes numbers without calling `cite()`, the validator flags them

**Variants:**
- `Compare Manhattan (36061) and Miami-Dade (12086) on income and migration`
- `Compare San Francisco County and Los Angeles County on federal spending and CBP establishments`
- `Compare the demographics of Austin (Travis County) and Boston (Suffolk County)`

---

## 3. Place dive — "show me X"

```
Show me Harris County, Texas
```

What happens:
- Agent calls `highlight_place(48201)` → **map fits to county + place panel opens**
- Place panel shows 8 layer values + sparklines + calendar heatmap
- Agent narrates demographic/economic highlights

**Variants:**
- `Show me Arlington County, Virginia`
- `Show me Maricopa County, Arizona`
- `Show me Miami-Dade County, Florida`

---

## 4. Layer activation — "show me where X is concentrated"

```
Show me where federal contract dollars are concentrated
```

What happens:
- Agent calls `toggle_layer(econ_federal_dollars)` → **choropleth recolors the map**
- Narrates which counties top the list (Arlington VA, Fairfax VA, LA, Harris)

**Variants:**
- `Show me the median household income layer` → income gradient renders
- `Where do the most disaster declarations happen?` → FEMA layer + scrubber activates
- `Show me the migration flows` → animated arcs render

---

## 5. Site simulation — "plant a business here"

This is the **headline differentiator** — the agent drops a focal-node glyph at a specific location AND activates relevant context layers.

```
Plant a chair store at lat=30.272, lon=-97.7457 (NAICS 442110) and show me the broadband subscription layer
```

What happens:
- Agent calls `plant_focal_node(442110, 30.272, -97.7457)` → **amber pin appears at Lamar & 6th in Austin**
- Agent calls `toggle_layer(infra_broadband_subscription)` → **layer activates**
- Agent narrates context (broadband coverage, demographics nearby)

**Variants:**
- `Plant a coffee shop (NAICS 722515) at lat=30.272, lon=-97.7457 and show me the median income layer`
- `Plant a pet supplies store (NAICS 453910) at lat=37.7749, lon=-122.4194 (San Francisco) and show me the demographics`
- `Plant a tutoring center at Cook County's Loop area (lat=41.881, lon=-87.629) and show me median age`

---

## 6. Recent news — "what's been happening"

```
What recent FEMA disasters have been declared?
```

What happens:
- Agent calls `get_recent_events(limit=10, sources="fema")`
- Lists 10 most-recent declarations with dates

**Variants:**
- `What SEC filings happened recently?`
- `What's been happening in disasters and corporate filings?` → merged FEMA + SEC

---

## 7. Migration patterns

```
Where are people moving to most? Show me the migration layer.
```

What happens:
- Agent calls `get_migration_flows(top_n=10)` → reads top 10 county-to-county flows
- Calls `toggle_layer(demo_irs_migration_net_agi)` → **animated arcs render across the map**
- Narrates: top flow is Manhattan → Miami-Dade at $3B AGI

**Variants:**
- `Where are people leaving New York County for?`
- `What are the biggest migration corridors in 2021?`

---

## 8. Honest data limits

The agent should refuse to fabricate. Try:

```
What is the median income in Atlantis, the lost city?
```

What happens:
- Agent calls `query_place` with no valid FIPS → errors / no data
- Honestly narrates "I can't find that place in the corpus"

---

## Direct URL deep-links (no Pilot needed)

If you want to skip the agent and just play with the map:

| URL | What it shows |
|---|---|
| [`/atlas.html`](http://localhost:3001/atlas.html) | Landing: NRI disaster risk choropleth + basemap |
| [`/atlas.html?layer=demo_tract_median_income&lat=30.27&lon=-97.74&zoom=11`](http://localhost:3001/atlas.html?layer=demo_tract_median_income&lat=30.27&lon=-97.74&zoom=11) | Austin tract-grain income — **basemap streets fully visible** with colored-outline choropleth |
| [`/atlas.html?layer=disaster_fema_declarations`](http://localhost:3001/atlas.html?layer=disaster_fema_declarations) | FEMA scrubber: drag year slider 1999→2026 |
| [`/atlas.html?layer=econ_cbp_establishments_county`](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_county) | CBP scrubber: 2018→2022 establishment counts |
| [`/atlas.html?layer=demo_irs_migration_net_agi`](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi) | Migration arcs animate |
| [`/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri`](http://localhost:3001/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri) | Bivariate: wealth × disaster risk |
| [`/atlas.html?place=48201`](http://localhost:3001/atlas.html?place=48201) | Harris Co place panel: 8 layers + sparklines + calendar heatmap |
| [`/atlas.html?recent=1`](http://localhost:3001/atlas.html?recent=1) | Recent Activity feed (FEMA + SEC mixed) |

---

## Zoom behavior to test

The choropleth now **adaptively switches between fill and outline** based on zoom:

| Zoom | Style | Why |
|---|---|---|
| 4-7 (national/regional) | Full fill at 55% opacity | Density read across the whole country |
| 8-9 (state) | Half-fill at 25% opacity | Transition zone |
| 10+ (neighborhood/street) | **Outline only** with colored borders | Basemap streets fully visible underneath |

**Try it:** open the [Austin tract URL](http://localhost:3001/atlas.html?layer=demo_tract_median_income&lat=30.27&lon=-97.74&zoom=11) then zoom out — watch the fills appear as you cross zoom 10.

---

## What to watch for

Per-feature observability:

- **Live tool log** (right panel during Pilot): every tool call streams in as it happens (planning → tool_call_started → tool_call_completed → ...)
- **Citation validator**: amber warning badge if the narration contains specific numbers (dollar amounts, percentages) without a `cite()` call
- **UI actions log**: at the end of each Pilot run, you see which UI actions were queued + applied (zoom_to, plant_focal_node, toggle_layer, highlight_place)
- **Place panel state**: deep-link URL captures every state — share-via-URL works for all of the above
