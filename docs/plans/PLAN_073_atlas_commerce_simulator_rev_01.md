# PLAN 073 rev_01 — Atlas Pilot (LLM-driven commerce simulator)

**Status:** Draft — awaiting approval
**Date:** 2026-05-26
**Revises:** PLAN_073 rev_00 (2026-05-26)
**Phase:** New program — supersedes PLAN_066 v3 as the headline product
**Preserves:** all SPEC_064 / 065 / 066 / 066b / 066c / 066e / 067 / 070-075 infrastructure
**Companion:** `memory/feedback/corrections.md` 2026-05-26 entry

---

## Revisions

### What changed from rev_00

| Before (rev_00) | After (rev_01) |
|---|---|
| Headline entry surface: "Plant focal node" button → user manually picks NAICS + drops a pin | Headline entry surface: **question box** ("Should I open a chair store at Lamar & 6th?") → Atlas Pilot agent does the planting, zooming, layer toggling, macro stress, and narrates |
| Layer panel + focal-node UX are the primary interaction model | Layer panel + focal-node UX are the **canvas the agent paints on**; user can still drive manually, but the default path is agent-piloted |
| LLM was Phase F polish ("per-focal-node insight generation") | LLM is **the core differentiator and Phase B-E backbone**. Every UI primitive is exposed as a tool the agent can call |
| 6 phases (A–F), ~5-6 weeks | 7 phases (A–G), ~6-7 weeks. The extra time is the agent infrastructure: tool surface, narration UI, planning template, cost-controlled inference loop |
| GATE 0 = 10 consultants try static simulator | GATE 0 = 10 consultants try **the piloted flow specifically** — does the agent-narrated exploration feel like a junior analyst, or like a janky chatbot? |

### Why

User feedback (2026-05-26):
> *"I don't want something Perplexity does. We need to integrate an LLM to traverse the data and the UI. It needs to be much better."*

The honest reading: Perplexity-style "ask a question → text answer + citations" is commoditizing — anyone can build that. The differentiated product is an **LLM that pilots the actual interface in service of a question**, with the visual evidence on the map *being the answer* (text narration is the thin wrapper).

This is structurally closer to **Cursor / Replit Agent / ChatGPT Advanced Data Analysis** than to Perplexity — agents that drive a domain-specific tool surface, narrating as they work. Nobody is doing this for geographic + economic data exploration. That's the category slot.

The rev_00 framing ("focal-node UX + static graph") was correct but insufficient — it would have shipped a more useful tool than the existing Atlas, but it wouldn't have been the differentiated category-defining product the audience needs. The agent layer turns the simulator from "a better Census Reporter" into "a junior site-selection analyst you point at questions."

### Out-of-scope changes from rev_00 (preserved)

- Audience: still solo / boutique site-selection consultants for retail.
- Free tier: still free; monetization gates remain on Phase-2 loop proof.
- Data sources: still OSM + USAspending + BEA + ACS + ZBP hybrid.
- NRI critique + operational-disruption-risk replacement: unchanged.
- Honest-data principle: strengthened — citations are now first-class via the `cite()` tool.
- Existing PLAN_064-072 infrastructure: still reused, just with each primitive exposed as a callable tool.

---

## 1 · The honest read of what we have (unchanged from rev_00)

The Atlas as built (22 layers, 14 endpoints, 40/40 smoke, FEMA scrubber,
Recent Activity, etc.) is impressive infrastructure but a **weak
product for any audience we can articulate**. It works at county
grain, has no basemap, no competition data, no demand surface at sub-
county detail, and offers no workflow beyond "browse layers."

PLAN_066 v3's bet was *"let telemetry name the wedge"* — but a tool
that produces no return-visit behavior produces no telemetry to learn
from. This plan replaces that strategy with a specific audience,
a specific job-to-be-done, **and a category-defining differentiator
(LLM-piloted interactive exploration)**.

---

## 2 · The audience and the job (lightly updated)

**Audience: solo / boutique site-selection consultants for retail.**

Their daily job:
> "Evaluate whether a specific street address is a good location for
> a specific retail concept (e.g., a chair store, coffee shop,
> tutoring center, pet supplies store)."

Their unmet need:
> "Get a comprehensive, defensible analyst-grade evaluation of a
> specific site in minutes, not days, on free public data, with
> evidence I can paste into a client deck."

The competitors (Placer.ai $50-150k/yr, Esri Business Analyst,
Buxton enterprise) sell **data depth**. We don't compete on data depth.
We sell **a piloted analyst** that drives a unified cross-domain
visualization and gives you the deck-ready output for free.

---

## 3 · The product (rewritten)

### The headline interaction

The Atlas home page is now a **question box**, not a layer panel.

```
┌────────────────────────────────────────────────────────────────────┐
│  Nexdata Atlas — ask the map                                       │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │ Should I open a chair store at Lamar & 6th in Austin?        │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                          [ Ask the Pilot → ]                       │
│                                                                    │
│  Or browse layers manually  ·  See example explorations            │
└────────────────────────────────────────────────────────────────────┘
```

Below the fold: the existing map + layer panel UX, for power users who
want to drive manually. Most users won't scroll there.

### The agent-piloted flow

User submits a question. The interface splits into three regions:

```
┌─────────────────┬────────────────────────────────┬─────────────────┐
│  ATLAS PILOT    │                                │   EVIDENCE      │
│  side panel     │           MAP CANVAS           │   side panel    │
│                 │                                │                 │
│  [Plan]         │  (agent drives focal node,    │  (every claim   │
│  [Current step] │   layers, zoom, arcs, macro    │   the agent     │
│  [Narration]    │   sliders in real time)        │   makes lives   │
│  [Citations]    │                                │   here, with    │
│                 │                                │   tool-call    │
│                 │                                │   receipts)     │
└─────────────────┴────────────────────────────────┴─────────────────┘
```

The agent does the following, narrating as it goes:

> **Plan:** I'll evaluate this site by: (1) planting the focal node,
> (2) measuring nearby competition, (3) profiling the demand surface,
> (4) mapping the supply chain, (5) stress-testing macro variables.
>
> **Step 1 / 5:** Planting focal node at 1234 Lamar Ave (NAICS 442110,
> furniture retailers). `[map: pin appears, zoom to 14]`
>
> **Step 2 / 5:** Competition within 5 miles. `[map: 12 pins appear,
> 1/3/5mi rings drawn]`
> → 12 furniture-related businesses. Closest: West Elm (0.4mi), Article
> (1.2mi), IKEA (2.1mi). [cite: tool=compute_competition]
>
> **Step 3 / 5:** Demand surface. `[map: tract choropleth renders]`
> → Median income within 3 miles: $94k (vs $73k national).
> Age 30-55 share: 41%. [cite: tool=compute_demand_surface]
>
> **Step 4 / 5:** Supply chain. `[map: amber arcs flow from upstream
> sector clusters into focal]`
> → Top upstream sector (upholstery wholesale) clusters 230 miles east
> in Houston metro. Secondary: lumber yards within Texas.
> [cite: tool=compute_supplier_arcs]
>
> **Step 5 / 5:** Stress test at +200bp interest rate.
> `[macro slider auto-moves, demand surface re-renders]`
> → Demand drops ~14% in trade area. Big-ticket retail is rate-sensitive.
> [cite: tool=stress_macro]
>
> **Final analyst summary:**
> Strong demographics for furniture retail — high income, age-aligned
> trade area. Heavy existing competition (12 stores within 5 miles,
> two within walking distance). Supply chain depends on long-haul truck
> routes from Houston-area wholesalers, fuel-cost sensitive. The big
> risk is interest-rate sensitivity: at current 5.5% rates this looks
> viable; at 7.5% (200bp scenario) margin gets thin.

**Every numerical claim in that narration links to a `cite()` call**
whose payload is the tool name + arguments + result — clicking a
citation highlights the corresponding map evidence.

### Why this is the differentiator

This is not chat-over-data. It's **a piloted exploration that produces
visual evidence as the deliverable, narrated**. The consultant doesn't
copy text into a deck — they screenshot the map and link the URL. The
map state IS the deliverable; the narration is the explanation of how
it was produced.

Competing tools:
- **Perplexity** — text + citations, no domain-specific data,
  no UI to pilot. Different product.
- **ChatGPT Advanced Data Analysis** — pilots a Python sandbox,
  generic. We pilot a domain-specific visual canvas.
- **Cursor / Replit Agent** — same architectural pattern, different
  domain (code).
- **Placer.ai / Esri** — sell raw data + traditional dashboards,
  no piloted exploration.

Nobody else is at the intersection of (governed public-data corpus) ×
(LLM-piloted UI) × (free) × (consultant-grade output). That's the
slot.

---

## 4 · What we kill, demote, reuse (mostly unchanged from rev_00)

### Reused as-is
- All 14 API endpoints (now exposed as tools for the agent)
- Boundary serving (extends to ZCTA + tract)
- Smoke harness pattern + deep-link URL state + telemetry
- Bivariate compare → competition × demand overlay (now agent-driven)
- Migration arcs → supplier→focal + focal→customer arcs (agent-driven)
- Recent Activity → "openings/closures in your trade area" (agent-driven)
- 22 existing layers → inputs to the simulator + tools for the agent
- The dynamic-UI library (D3, Plot, Leaflet.heat)
- The honest-data principle — **strengthened via citations**

### Demoted / removed from headline UX
- NRI as default layer (replaced by operational-disruption-risk; rationale in §6)
- FEMA scrubber as signature feature (still available as a tool the agent can call when relevant)
- "Browse 22 layers" as entry UX — moved below-the-fold
- County-default landing — replaced by the question box

### Wholly new (rev_01-specific additions in **bold**)
- ZCTA + census-tract boundary ingest + serving
- Real basemap (Carto dark or OSM tiles)
- Sub-county ACS layers (median income, age, household at tract grain)
- Business-location point data (OSM + USAspending hybrid)
- BEA Input-Output Use Table ingest
- Simulator service (focal-node → suppliers + customers + logistics)
- Macro stress sliders + elasticity model
- Operational-disruption-risk layer (replaces NRI as primary)
- **Atlas Pilot agent infrastructure: tool dispatcher, Anthropic tool-use API integration, planning template, cost-controlled inference loop**
- **The 11-tool callable surface (§7 below)**
- **Narration UI: streaming plan + step-by-step updates + final analyst summary**
- **Evidence/citation side panel — every claim resolves to a tool call**

---

## 5 · Phases (restructured for rev_01)

Each phase ships independently and is end-to-end usable. The major
shift from rev_00: phases B-D now interleave **data work + tool
exposure**, so by Phase E the agent has a real tool surface to plan
over.

### Phase A — Hyper-local foundation (3 days)
*Same as rev_00.*
- A.1 — Add Carto/OSM basemap tile layer under choropleth
- A.2 — Ingest ZCTA + tract boundaries from Census TIGER
- A.3 — Tract-grain ACS demand layers (income, age, density, tenure)
- A.4 — Zoom defaults: click → neighborhood (zoom 14)

**Acceptance:** the map shows real streets at neighborhood zoom with
tract-grain layers rendering.

### Phase B — Tool surface + agent shell (5 days) 🆕
**Goal:** every UI primitive is callable; the empty agent shell exists.
- B.1 — Server-side **tool dispatcher**: each existing API endpoint
  becomes a tool with a JSON Schema definition usable by Anthropic's
  tool-use API. ~14 tools to start, all parameterized.
- B.2 — Frontend **agent state machine**: connects to a new
  `POST /atlas/pilot` endpoint that takes a question + session state
  and streams back tool calls + narration tokens.
- B.3 — **Narration UI shell**: the left side panel renders the agent's
  plan (upfront), each step's pre-action narration, post-action result,
  and final summary. All streamed.
- B.4 — **Evidence panel**: right side panel collects every `cite()`
  call as a clickable entry; clicking jumps the map / highlights the
  evidence layer.
- B.5 — **Cost controls**: cap loop depth at 12 tool calls per
  question; per-user / per-session token budget; cheap model for
  routine tool selection, premium (Claude Sonnet 4.6 or Opus 4.7) for
  the final analyst narration.
- B.6 — **Telemetry**: `pilot_question_submitted`, `pilot_tool_called`,
  `pilot_cite_clicked`, `pilot_completed`, `pilot_aborted`.

**Acceptance:** an end-to-end "what is the median income in Austin TX?"
question routes through the agent, fires `query_place(48453)`, narrates
the answer, and cites the tool call. No real product value yet — just
the agent harness working end-to-end.

### Phase C — Focal-node + competition + customers (8 days)
*Combines rev_00 phases B + C.* Each capability ships as both UI and tool.
- C.1 — Focal-node UX (manual-drive path): "Plant focal node" button
  + NAICS picker + click-to-place. Deep-link state.
- C.2 — Business-location ingest hybrid (OSM Geofabrik + USAspending
  POP + ZBP density)
- C.3 — Competition section: count + locate within 1/3/5mi radii
- C.4 — Customer surface: demand-relevant ACS choropleth at tract grain
- C.5 — Honest data badges (points vs density)
- C.6 — **Expose as tools:** `plant_focal_node`, `compute_competition`,
  `compute_demand_surface`, `search_businesses`.
- C.7 — **First agent script:** the agent can now answer "evaluate this
  site for a chair store" with the first three sections.

**Acceptance:** ask the Pilot "evaluate a chair store at Lamar & 6th";
the agent plants the node, computes competition + demand, narrates,
cites.

### Phase D — Supplier + logistics + arcs (7 days)
*Same as rev_00 phase D, with tool exposure.*
- D.1 — BEA Input-Output Use Table ingest
- D.2 — Upstream-sector lookup per NAICS
- D.3 — Supplier-node geocoding from C.2's point data
- D.4 — Arc renderer (reuse SPEC_066b migration-arc engine)
- D.5 — Logistics nodes: existing transport_airports + new freight
  centers ingest
- D.6 — **Expose as tools:** `compute_supplier_arcs`,
  `compute_logistics_routes`.
- D.7 — Agent script extended: full 4-section evaluation works end-to-end.

**Acceptance:** the chair-store question fires through Steps 1-4 of
the example narration in §3 with arcs rendering correctly.

### Phase E — Atlas Pilot agent (6 days) 🆕
**Goal:** the agent itself becomes good, not just functional.
- E.1 — **Planning template**: a system prompt that constrains the
  agent to a fixed evaluation pattern for site-selection questions
  (5-step plan from §3, hardcoded skeleton, tool order suggested but
  not forced).
- E.2 — **Citation discipline**: hard rule in the system prompt —
  every numerical claim in the final narration must be backed by a
  prior `cite()` call. Unlinked claims get flagged in red as
  "model-derived assumption."
- E.3 — **Conversational follow-up**: user can ask "what about Round
  Rock instead?" → agent re-plans, re-uses prior context (don't
  re-ingest competition for a totally new city if not needed),
  produces a comparative narration.
- E.4 — **Interrupt + control**: user can pause the agent mid-stream,
  manually adjust the map, then resume — the agent picks up with the
  new state.
- E.5 — **Quality benchmarking**: 20 hand-graded reference questions
  ("evaluate a chair store at X," "compare these 3 coffee shop
  sites," "what supply chain risk do my Houston suppliers carry?")
  with expected tool sequences. Regression-test against these on
  every agent prompt change.

**Acceptance:** Atlas Pilot reliably handles the 20 reference
questions; output reads like a junior analyst, not a chatbot;
citation discipline is enforced.

### Phase F — Macro stress overlays (4 days)
*Same as rev_00 phase E, now agent-driven.*
- F.1 — FRED + EIA ingest (FRED is partly already there)
- F.2 — Elasticity model per retail NAICS family
- F.3 — Macro slider UI (manual-drive path)
- F.4 — **Expose as tools:** `stress_macro(var, delta)`,
  `list_macro_baselines()`.
- F.5 — Agent script extended: stress-tests are part of the standard
  5-step plan.

**Acceptance:** agent can answer "what happens at +200bp" by firing
the macro tool and re-narrating; manual sliders still work.

### Phase G — Sharing + storytelling (ongoing)
*Mostly same as rev_00 phase F.*
- G.1 — **Question-as-URL**: every piloted exploration produces a
  sharable URL that replays the agent's plan + tool calls + narration
  + final map state. This is *the share unit*, not just a map view.
- G.2 — Server-side preview snapshot for social cards.
- G.3 — Pre-baked **case-study explorations** as the marketing
  surface ("a chair store in Austin," "a coffee shop in Brooklyn,"
  "a pet supplies store in Phoenix").
- G.4 — Optional: per-site LLM insight panel still useful for the
  "what's unusual about this place" prompt that's distinct from
  evaluating a specific business.

---

## 6 · Operational disruption risk (unchanged from rev_00 §6)

NRI replacement layer is unchanged. The Atlas Pilot can now *cite*
this layer explicitly when narrating supply-chain or site risk —
"your supplier in Ocean County NJ has an operational disruption
score of 28 (expected 3-4 business-closure days per year), which
is moderate."

Existing NRI layer stays as an optional tool the agent can choose
to call when an actuarial framing is genuinely useful (insurance,
real-estate underwriting questions).

---

## 7 · The 11-tool callable surface

The agent operates over a **constrained tool set** for v0 (per user
guidance recommendation). Open-ended tool composition is a v2 upgrade.

```
1.  plant_focal_node(naics: str, lat: float, lon: float) → focal_id
    Sets the focal node on the map; auto-zooms to neighborhood.

2.  zoom_to(lat: float, lon: float, level: int) → bbox
    Repositions the map.

3.  toggle_layer(layer_id: str, mode: 'choropleth' | 'overlay' | 'off')
    Activates a registered layer.

4.  query_place(geo_id: str, layers: list[str] = []) → dict
    Reads multi-layer aggregate values for a county/tract/zcta.

5.  compute_competition(focal_id, radius_mi: int) → {count, top_n: list}
    Counts + locates same-NAICS-family businesses within radius.

6.  compute_demand_surface(focal_id) → tract_choropleth
    Renders demand-relevant ACS at tract grain around focal.

7.  compute_supplier_arcs(focal_id, top_n: int = 5) → [arcs]
    Uses BEA IO table to find upstream sectors + their nearest
    geographic representatives; renders arcs.

8.  compute_logistics_routes(focal_id) → [nodes]
    Nearest freight nodes (rail, port, airport, distribution).

9.  stress_macro(var: enum, delta: float) → simulator_state
    Drives a macro slider; re-runs the simulator.

10. compare_sites(sites: list[(naics, lat, lon)]) → comparison
    Runs the focal-node loop on multiple sites in parallel.

11. cite(claim: str, evidence: dict) → citation_id
    Pure logging tool — every numerical claim in the final narration
    MUST be backed by a cite() call. Renders into the evidence panel.
```

Each tool has a JSON Schema definition the Anthropic API consumes.
Each invocation streams to the frontend so narration + UI update
together.

**Constrained design rationale:** 90% of the consultant workflow falls
in these 11 tools. The agent can compose plans across them but can't
invent new ones. This is the v0 boundary; expanding to "can call any
layer endpoint" or "can compose new tools" is v2.

---

## 8 · The data hybrid approach (unchanged from rev_00 §7)

Business-location data: OSM + USAspending POP + ZBP density.
Honest UI badges per section. See rev_00 §7 for the source table —
unchanged in rev_01.

---

## 9 · What this plan refuses to do (mostly unchanged)

- No paid tier in v1. Free, period.
- No real-time data.
- No mobile-native experience in v1.
- No competitor-tier feature parity with Placer.ai on data depth.
- No premature integrations (CRM hooks, Slack, PDF export).
- **No AI chat over data without piloting.** If the agent can't drive
  the UI, we're just Perplexity. The pilot loop is non-negotiable.
- **No free-form code execution in v0.** Constrained 11-tool surface.
- **No unlinked numerical claims in agent narration.** Citation
  discipline is enforced.

---

## 10 · Sequencing relative to existing work

**Stop:**
- The launch sequencing in `LAUNCH_READINESS.md` §10 (soft-launch of
  general-explorer Atlas). Hold until Phase E ships — the public-
  facing Atlas should debut as the piloted product, not as the
  general explorer with a chatbot bolted on.
- The PLAN_068 "growth" thinking as scoped.

**Keep running:**
- PLAN_067 data backfills are done; layers remain useful as agent tools.

**New gate (revised from rev_00):**
- **GATE 0 — Phase E ships + 10 real consultants try the Atlas Pilot
  on real work.** The agent-piloted flow is what's tested, not the
  static simulator. Critical questions:
  - Does the narration read like a junior analyst or like a chatbot?
  - Do consultants screenshot/share the map states?
  - Does citation discipline hold up — every claim has a receipt?
  - Where do they hit "I wish the agent could do X" walls?

---

## 11 · Specs to draft under this plan (revised list)

| Phase | Spec | Title |
|---|---|---|
| A | SPEC_077 | Basemap + sub-county boundary ingest + tract ACS layers |
| B | SPEC_078 | **Tool dispatcher + agent shell + narration UI** 🆕 |
| C | SPEC_079 | Business-location point ingest (OSM + USAspending) |
| C | SPEC_080 | Focal-node UX + competition + demand surface (with tool exposure) |
| D | SPEC_081 | BEA IO table ingest + supplier-sector graph |
| D | SPEC_082 | Supplier + logistics arc renderer (with tool exposure) |
| E | SPEC_083 | **Atlas Pilot agent: planning template + citations + 20 benchmarks** 🆕 |
| F | SPEC_084 | Macro stress overlays + elasticity model (with tool exposure) |
| F | SPEC_085 | Operational disruption risk layer (NRI replacement) |
| G | SPEC_086 | Question-as-URL + server-side preview + case-study library |

**Total: 10 specs, ~32 working days, ~6-7 calendar weeks at a
reasonable pace.** End-to-end usable after Phase E.

---

## 12 · Definition of done

This plan closes when:
- All seven phases A-G ship
- The Atlas Pilot reliably handles 20 benchmark questions with proper
  citation discipline
- GATE 0 passes: 10 site-selection consultants try the agent on real
  work; output reads like a junior analyst they'd actually use
- The retired "general explorer" framing is fully removed from
  product surfaces
- Honest-data principles survive (now reinforced by citation tooling)

---

## 13 · The honest unknowns (updated)

| Unknown | Mitigation |
|---|---|
| Will site-selection consultants use a free agent-piloted tool? | GATE 0 |
| Is OSM coverage good enough? | Spike Phase C.2 across 5 metros × 10 NAICS first |
| Is the BEA IO table specific enough for retail-grain consulting? | Spike Phase D.1 with chair retail (442110) before committing to the full ingest |
| Will the agent feel like a junior analyst, or like a janky chatbot? | Phase E benchmark suite + GATE 0 |
| Latency budget — can we keep a piloted exploration under ~15s end-to-end? | Tool result caching, parallel tool dispatch where possible, plan up front so user knows what's coming |
| Cost per question | Token budget cap + cheap model for routine tool selection + premium model only for final narration |
| Citation discipline — does the agent reliably link every claim? | Hard system-prompt rule + automated post-hoc check that flags unlinked numerical claims in red |
| LLM hallucinations (wrong NAICS code, made-up coordinates) | Constrained tool inputs validate against enums + geocoder; agent can't "invent" a coordinate, only call a real tool |

**The biggest risk is still the audience risk + the agent-quality
risk.** Build Phase A-C with manual UX first; ship the agent shell in
Phase B with a *minimal* tool set; test the planning template in Phase
E against 20 benchmarks; talk to consultants at GATE 0. If GATE 0 says
"the agent is the differentiator but the data isn't deep enough,"
v1.5 adds data depth. If it says "the agent narration is great but
my clients want PDF exports," v1.5 adds PDF export. If it says "the
agent is too slow," v1.5 hardens the latency budget.

---

## 14 · What stays from the old Atlas (unchanged)

None of the engineering from PLAN_064-072 is wasted:
- Layer API, place API, series API, events API, recent API, migration
  API, cascade APIs — all reused, now as agent tools
- Boundary serving — extends to ZCTA + tract
- Dynamic UI library — all reused
- Telemetry table + Phase-2 measurement framework — reused; what
  changes is what the metrics measure (pilot_question_submitted,
  pilot_tool_called, pilot_cite_clicked, pilot_completed)
- 22 layers — most become inputs to the simulator + tools for the agent
- Smoke harness, tests, deep-link URL state — all reused
- Honest-data principle — strengthened via the citation tool

What changes is the **product framing** on top of all that
engineering. The Atlas was a city. The commerce simulator turned the
city into a specific neighborhood you can live in. The Atlas Pilot
puts a junior analyst on call who lives in that neighborhood with you.

---

## 15 · The Atlas Pilot architecture (new section in rev_01)

### High-level shape

```
USER  ─── question ──►  /atlas/pilot  ─── plan ─── tool calls ──► tool dispatcher
                              │                                      │
                              │                                      ├─► /atlas/layer/{id}
                              │                                      ├─► /atlas/place/{geo_id}
                              │                                      ├─► /atlas/recent
                              │                                      ├─► simulator service
                              │                                      └─► [11 tools total]
                              │                                      
                              ◄── streaming SSE: plan + tool_call + tool_result + narration_tokens
                              
FRONTEND  ─── routes SSE events to:
                              │
                              ├─► Atlas Pilot panel (plan + step + narration)
                              ├─► Map canvas (tool side effects: focal node, layer toggle, arcs, macro)
                              └─► Evidence panel (cite() entries → clickable receipts)
```

### Cost model

- **Tier 1 (cheap model — Haiku 4.5):** Routine tool selection +
  intermediate planning. ~10-20¢ per question.
- **Tier 2 (premium model — Sonnet 4.6 or Opus 4.7):** Final analyst
  narration synthesis. ~25-50¢ per question.
- **Total target:** under $1/question on average; $5/question worst
  case.
- **Cap:** 12 tool calls per question (hard limit); 60s wall-clock
  agent timeout; user can interrupt anytime.
- **Free-tier budget:** N questions/user/day (number TBD by Phase E
  testing).

### Latency budget

- **Target:** initial plan visible within 2s; first tool result
  within 5s; final narration within 15s end-to-end.
- **Mechanisms:**
  - Stream the plan upfront so user has something to read while tools fire
  - Parallel tool dispatch where the plan permits (competition + demand can run concurrently)
  - In-memory tool result caching within a session (re-ask "what about Round Rock instead?" reuses many prior tool results)
  - Premium model is only called once at the end, after all tool results are in

### Hard safety rules

1. **Citation discipline:** every numerical claim in the final
   narration must trace to a prior `cite()` call. Post-hoc validator
   flags unlinked claims; user sees a red warning badge.
2. **No coordinates from the LLM:** the agent calls geocoder tools;
   it never types lat/lon from memory.
3. **NAICS validation:** all NAICS codes route through a known-enum
   check.
4. **Interrupt:** user can pause the agent mid-stream; manual map
   actions take precedence and the agent re-plans on resume.
5. **Cost caps enforced server-side**, not by the LLM agreeing to
   stop.

### Open architectural questions (resolve during Phase B)

- Tool dispatcher language: Python (FastAPI, same as existing API) ✓
- Streaming protocol: SSE ✓
- Anthropic API mode: Tool Use (native) ✓
- Conversation state storage: in-memory per session (v0) → Postgres-
  backed (v1 if needed)
- Multi-turn conversation tracking: session_id-keyed; questions in a
  thread share prior tool results

---

## 16 · Refuses to do (revised)

- **Pre-build all 10 specs before talking to consultants.** GATE 0 exists.
- **Re-debate the audience choice.** Locked: solo / boutique site-selection consultants for retail.
- **Add features that compete with Placer.ai head-on on data depth.**
- **Skip the Phase-2 measurement loop.** Discipline survives the pivot.
- **Build the agent before the static data tools work.** Phase B-D
  must ship a working static commerce simulator that the agent then
  drives. Agent on top of broken tools = bad demos.
- **Free-form agent code execution in v0.** Constrained 11-tool surface.
- **Unlinked numerical claims in narration.** Citation discipline non-negotiable.
- **Premium-model tool selection.** Cheap model for routine; premium only for final synthesis.
