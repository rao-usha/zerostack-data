# Morning click-through — what shipped

**Two cycles of work:** overnight (Phase A foundation + Phase B Atlas Pilot) and morning (Atlas Pilot ramps from "read-only Q&A" to "actually pilots the UI"). Open `http://localhost:3001/atlas.html` and try the 6 things below in order.

---

## 0. NEW (this morning) — The agent now pilots the map

The biggest change since you slept: **the Atlas Pilot can now drop focal nodes, zoom the map, and toggle layers as part of answering your question.** This is the differentiator from PLAN_073 rev_01 — an LLM that traverses both the data AND the UI.

Try in the **Ask the Pilot** input:

| Question | What the agent does |
|---|---|
| `Plant a chair store at lat=30.272, lon=-97.7457 and show me the broadband subscription layer.` | Drops an amber focal-node pin labeled NAICS 442110 at that exact spot · activates the broadband choropleth · narrates |
| `Show me Travis County, Texas.` | Calls `highlight_place(48453)` — fits map to county, opens place panel · narrates demographics |
| `Compare Travis County and Cook County on income — show me the income layer.` | Calls `compare_places` + `toggle_layer(demo_acs_median_income)` · narrates the comparison |
| `Where are people migrating to most? Show me the layer.` | Calls `get_migration_flows` + `toggle_layer(demo_irs_migration_net_agi)` — arcs auto-render · narrates top corridors |

You'll see a **live tool log** stream in as the agent works (`thinking → tool_call_started → tool_call_completed → ...`). At the end, the **map mutates** with the agent's queued UI actions. Citation-validator warning badge appears if it makes specific numerical claims without calling `cite()`.

Verified live before commit:
- "Plant a chair store + broadband layer" → 4 loops, 4 tools, 2 UI actions (plant_focal_node + toggle_layer), 53.8s

---

## 1. The map looks like a real map now

[Landing](http://localhost:3001/atlas.html)

**What to look for:**
- **Real basemap underneath** (Carto Dark Matter) — ocean, coastlines, country labels visible
- Default choropleth opacity is **55% (was 85%)** so streets show through

## 2. Click a county — it now zooms to neighborhood scale

[Click anywhere in Texas](http://localhost:3001/atlas.html?lat=30.27&lon=-97.74&zoom=8)

**What to look for:**
- Click any county → auto-zooms to ~zoom 14 (1-2 mile radius visible)
- **Real street grid and neighborhood labels appear** — this is the foundation for the focal-node UX in Phase C
- Click "Reset view" to zoom back out

## 3. NEW: 4 tract-grain demographic layers

In the layer panel under **Demographics**, you'll see four new entries marked `tract`:
- Median Household Income (tract)
- Median Age (tract)
- Population (tract)
- Owner-Occupied Housing (tract)

**What to look for:**
- Click one. The frontend triggers a **boundary grain switch** — fetches ~30MB of tract boundaries (first time only; cached after) and replaces the county choropleth with a tract choropleth.
- Toast says "Loading tract boundaries…" then the map recolors at tract grain
- Click a county-grain layer to switch back — instant (county cached client-side)
- The data may be partial if the overnight tract ACS ingest didn't finish all 52 states; in that case the choropleth has gaps. Check `/atlas/layer/demo_tract_median_income` → `.values.length` to see how many tracts have data.

## 4. 🎯 THE BIG ONE — Atlas Pilot

The differentiator. **An LLM that pilots the data + UI in service of your question.**

**Header has a new "Ask the Pilot" input box.** Try these:

| Question | What should happen |
|---|---|
| `What is the median household income in Harris County, TX?` | Agent calls `query_place(48201)` → narrates "$73,104 (2023 ACS)" |
| `Compare Austin and Houston on income and broadband` | Agent calls `query_place(48453)` + `query_place(48201)` → side-by-side narration |
| `What's been happening in the news recently?` | Agent calls `get_recent_events` → narrates recent FEMA + SEC items |
| `Where are people moving to and from the most?` | Agent calls `get_migration_flows` → narrates top NYC→Miami corridor etc |
| `Show me the demographics of Travis County, TX vs Cook County, IL` | Multi-place comparison + analyst summary |

**What to expect:**
- Spinner: "Thinking… (5-30s)" — actual time is 8-25s for most questions
- Response panel slides in from the right with:
  - The question echoed
  - Final narration in plain prose
  - **Tool-call log** showing every step the agent took
  - Citations (when the model decides to call `cite()` — not strict in v0)
  - Meta footer: "X loops · Ys · gpt-4o-mini"

**Verified working live before commit** (commits `534c972` + `acd92ac`):
- "Median income in Harris County" → "$73,104 (2023 ACS)" · 21.8s · 2 tools
- "Compare Austin & Houston on income + broadband" → "$97,169 vs $73,104; 93.3% vs 91.0%" · 12.0s · 2 tools

**Honest limits of v0:**
- Non-streaming — you wait for the full response
- The agent can READ data but can't yet manipulate the map (Phase C/D add `plant_focal_node`, `compute_competition`, `toggle_layer` as tools)
- Citation discipline is soft — model sometimes inlines sources in narration rather than calling `cite()` (Phase E hardens this)
- Cost: ~$0.01-0.05 per question with gpt-4o-mini

## 5. Click into one of the existing dynamic features (no regression)

Quick spot-check that nothing's broken:

| Feature | Try |
|---|---|
| FEMA scrubber | [`?layer=disaster_fema_declarations`](http://localhost:3001/atlas.html?layer=disaster_fema_declarations) — drag year slider |
| CBP scrubber | [`?layer=econ_cbp_establishments_county`](http://localhost:3001/atlas.html?layer=econ_cbp_establishments_county) — drag year slider |
| Migration arcs | [`?layer=demo_irs_migration_net_agi`](http://localhost:3001/atlas.html?layer=demo_irs_migration_net_agi) — amber arcs render |
| Bivariate | [`?layer=demo_irs_county_agi_per_return&compare=disaster_nri`](http://localhost:3001/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri) — 3×3 grid |
| Recent Activity | [`?recent=1`](http://localhost:3001/atlas.html?recent=1) — panel with FEMA + SEC |
| Place panel | [`?place=48201`](http://localhost:3001/atlas.html?place=48201) — Harris Co + sparklines + calendar |

---

## What's NOT done yet

- **ZCTA boundaries** — deferred to SPEC_077b (Census WAF blocks the large default page; chunked workaround written but not run)
- **Tract bbox filtering on `/atlas/boundaries?geo_level=tract`** — currently returns ~30MB full collection; Phase C adds `?bbox=` for neighborhood-scoped fetches
- **Atlas Pilot map-mutating tools** (`plant_focal_node`, `toggle_layer`, `compute_competition`, `compute_demand_surface`) — Phase C/D
- **Atlas Pilot streaming** — non-streaming v0
- **Citation hardening** — Phase E

## What I'd do next

In priority order:

1. **Set ANTHROPIC_API_KEY in `.env` if you prefer Claude** for the agent (current path uses OpenAI gpt-4o-mini since OPENAI_API_KEY was set). Switch via env var `ATLAS_PILOT_MODEL` once you wire Anthropic.
2. **Phase C — focal-node + competition + customers** (~1 week). Adds the "plant a business" UI primitive + the first map-mutating tools for the agent.
3. **Phase D — supplier + logistics arcs** (~1 week). BEA Input-Output table + the differentiator visual.
4. **Phase E — agent quality** (~1 week). 20-benchmark suite + citation discipline + streaming.

---

## Commits from overnight

```
acd92ac  feat: SPEC_078 Phase B — Atlas Pilot agent (LLM that pilots the map)
534c972  feat: SPEC_077 Phase A (server) — tract boundaries + tract ACS layers
9e16920  feat: SPEC_077 A.1 — Carto Dark Matter basemap (PLAN_073 Phase A starts)
ec6ad21  docs: PLAN_073 + rev_01 — Atlas Pilot commerce-simulator pivot
e15653c  fix: SPEC_076 — boundary 'glass shards'
```
