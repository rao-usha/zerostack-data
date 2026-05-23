# PLAN 066 — Atlas: The General Public-Data Explorer

**Status:** Draft (v3 — general-explorer reframe) — awaiting approval
**Date:** 2026-05-22
**Builds on:** SPEC_064 (Atlas explorer engine — resolver / cards / graph / telemetry, shipped `be1bb97`)
**Supersedes:** PLAN_066 v1 (generic 8-layer map) and v2 (persona-anchored on "Dana")
**Deprioritizes:** PLAN_065 remaining specs; PLAN_064 lead-ops stays paused.

---

## 0 · What changed across versions

- **v1** — "a map with 8 layers." No user, no data reality.
- **v2** — anchored hard on one persona (Dana, the regional economic-
  development officer). That narrowed Atlas to economic-development layers
  and discarded most of the platform.
- **v3 (this)** — **general explorer.** "Pick one wedge persona" is advice
  for sales-led vertical tools. Atlas is a curiosity-driven *exploration*
  product — breadth is the asset. We design for exploration itself,
  instrument it, and **let telemetry reveal the wedge** rather than guessing
  it. This restores SPEC_064's original general framing.

---

## 1 · Strategic frame

SPEC_064 shipped a working Atlas engine — query → entities → cross-dataset
insight cards → connections → provenance → telemetry. The engine is sound;
the *interaction* is a search box (blank-page problem, no ambient pull,
geographic data shown as text).

Benchmarked against OSIRIS (osirisai.live) — a general live map you land on
and can't stop toggling — the lesson is **map-first, zero-query, many
layers, deep-linkable.** This plan inverts Atlas: the **map becomes the
product**, the SPEC_064 cards become the **click-a-place drill-down**, search
becomes a corner shortcut.

Nexdata holds **~400 governed public-data tables across a dozen domains.**
That breadth *is* the product. Atlas makes all of it explorable, spatially,
connected, and source-cited. We do not cosplay OSIRIS's spy aesthetic — no
flight-tracking / CCTV / SIGINT (not our data). Our identity: **"explore
American public data — every dataset, on one map, all sourced."** The moat
is governed cross-dataset joins + provenance + the usage telemetry that
compounds.

**Kept, zero rework:** the SPEC_064 engine. `POST /atlas/explore` becomes the
click-a-place call.

---

## 2 · The user — exploration, not a persona

v2's mistake was naming one user. v3 is explicit: **for a general explorer
you design for the *behavior* (curious exploration) and let the audience
self-select.** Atlas's plausible users are broad and we do not pre-rank them:

- investors / analysts, operators, lenders, economic-development teams,
  journalists, researchers, students, policy people, and the simply curious.

**We do not pick one. We instrument and discover.** Telemetry
(`atlas_events`, `atlas_queries`) records which domains get explored, which
layers get toggled, which places get clicked, what gets shared. After 30 days
of real traffic, the data names the wedge — *then* PLAN_068/069 can target
it. This is the honest version of "find your wedge," and it's exactly
SPEC_064's stated thesis: the product learns which answers matter.

**Design consequences of going general:**
- Landing opens on a **compelling featured view** (a striking default layer,
  optionally rotating), not a fixed vertical home.
- The layer catalog is **organized by domain**, broad, browsable.
- Drill-down cards span **all domains**, not just economic ones.
- `follow` is general — follow a place, a layer, or a domain.
- Stories range across every domain — the breadth is itself the hook
  ("there's a map of *that*?").

---

## 3 · Product principles
1. **Map-first, zero-query.** Land on a live featured layer; value before typing.
2. **Breadth is the asset.** Every governed domain we hold becomes explorable.
3. **Honest coverage.** A layer states its grain + vintage; thin layers say so;
   nothing is faked (skip-on-empty, coverage notes — already in SPEC_064).
4. **Provenance is the moat.** Every layer cell + card metric traces to source rows.
5. **Every view is a URL.** `?lat=&lon=&zoom=&layers=&place=` — shareable, forkable.
6. **No-build frontend.** Single self-contained HTML, vanilla JS, Leaflet via CDN.
7. **Instrument everything.** The product can't learn what it doesn't measure.

---

## 4 · Data foundation — the domain catalog

Nexdata's cloud DB holds ~400 tables. Atlas groups them into **domain layer
families.** Coverage was spot-audited (the strong economic layers verified in
detail 2026-05-22); a **full catalog audit is the SPEC_065 pre-flight.**

| Domain family | Representative sources | Map readiness |
|---|---|---|
| **Disaster & risk** | FEMA declarations (1999–2026, full county), National Risk Index (full county), flood zones, FEMA PA/HMA projects | ✅ strong — county grain |
| **Environment** | EPA ECHO (1.07M facilities, 100% geocoded), EPA GHG, brownfields, wetlands, water systems/monitoring | ✅ strong — point layers |
| **Energy & infrastructure** | power plants, transmission lines, substations, EIA | ✅ point/line layers |
| **Transport & logistics** | rail lines, airports, BTS border crossings, ports, container freight, air cargo | ✅/🟡 |
| **Demographics & income** | IRS SOI county income/migration (full county), Census ACS (ZIP grain) | ✅ IRS county / ⏸ ACS needs crosswalk |
| **Economy & business** | Census CBP, BLS, BEA | 🟡 state grain (county partial) |
| **Finance & banking** | FDIC (county-mappable via institutions), Treasury, FRED, SEC | ✅/national |
| **Health** | CMS hospitals, drug pricing, Medicare utilization, NPPES providers | ✅/🟡 — audit in pre-flight |
| **Real estate** | FHFA HPI, building permits, land use, zoning | 🟡 — audit in pre-flight |
| **Trade** | US trade exports/imports by state + HS code | 🟡 state grain |
| **Federal activity** | USAspending awards | 🔴 thin/dateless — PLAN_067 backfills |
| **Agriculture** | USDA crop production | 🟡 — audit in pre-flight |
| **Markets / intl** | CFTC COT, World Bank WDI, OECD | national/intl — context panels |

**Honest rules carried from the audit:**
- A layer ships at the grain the data actually supports; it never promises a
  county map it can only draw at state grain.
- National / time-series data (FRED, Treasury, markets) → context panels, not
  choropleths.
- Weak layers (USAspending) are flagged in-product; PLAN_067 backfills them.
- Default featured layer = **Disaster Exposure** — full county, current,
  visually compelling. (A rotating featured layer is a fast-follow.)

---

## 5 · The product — Atlas v3 surfaces

1. **Featured landing** — map opens on a compelling default layer; command-
   center counters populate ("~400 datasets · 1.07M EPA facilities · 3,143
   counties · 12 domains"). No query required.
2. **Domain layer catalog** — a browsable panel grouping every layer by
   domain; toggle any layer, instant re-render.
3. **Click-a-place drill-down** — click any county/place → SPEC_064 cross-
   dataset insight cards for that place (expanded across all domains), with
   provenance.
4. **Recent Activity** — event-dated data (FEMA, EPA, more as PLAN_067 lands)
   as a timestamped pin layer + side feed; the map changes over time.
5. **Search** — corner fast-travel (place / domain / dataset), not the gate.
6. **Follow** — watch a place, layer, or domain.
7. **Stories** — curated narrated tours across all domains.
8. **Share / fork** — every view is a deep-link; "fork this view," "compare."

---

## 6 · Implementation — 5 specs

### SPEC_065 — Atlas layer-data + boundaries API
**Pre-flight (important):** audit the full ~400-table catalog — classify each
candidate layer by domain, geo grain, vintage, coverage; produce the layer
registry. This is the data-honest foundation of the whole explorer.
**Files:** `app/services/atlas/layers.py` (multi-domain layer registry +
query builders), `app/services/atlas/boundaries.py` (serve `geojson_boundaries`,
simplified geometry), extend `app/api/v1/atlas.py`.
**Endpoints:** `GET /atlas/layers` (registry, grouped by domain),
`GET /atlas/layer/{id}` (choropleth `{geo_id:value}` or point GeoJSON),
`GET /atlas/boundaries`, `GET /atlas/place/{geo_id}` (everything-known-here).
**Correctness:** FDIC via `cert`→`fdic_institutions.stcnty`; IRS keyed on
`county_code` (= 5-digit FIPS); honest grain per layer; weak layers flagged.
**Tests:** registry shape + domain grouping; each layer returns declared
grain; unknown layer → 404; boundaries ≥3,000 county features.

### SPEC_066 — Atlas Map frontend v3 (general explorer)
**Files:** `frontend/atlas.html` rewritten — Leaflet canvas; featured-layer
landing; domain-grouped layer catalog panel; click→drill-down cards;
command-center counters; deep-linkable `?lat=&lon=&zoom=&layers=&place=`;
corner search; share/fork.
**Tests:** static page, headless-Chrome e2e. Acceptance: map renders on a
featured layer; catalog lists layers by domain; ≥2 layers toggle; click →
cards; URL round-trips.

### SPEC_067 — Recent Activity layer
Event-dated sources — FEMA declarations (the backbone: dated, county,
1999–2026), EPA enforcement; more as PLAN_067 backfills land. `activity.py`
+ `GET /atlas/activity`. Pin layer + feed. Skip-on-empty, no fabrication.

### SPEC_068 — Follow + weekly digest
General `follow` — a place, layer, or domain. `atlas_follows` table,
follow/unfollow endpoints, APScheduler weekly digest via `EmailService`
(SPEC_052), lightweight account via passwordless auth (SPEC_053).

### SPEC_069 — Guided stories / tours
`data/reference/atlas_stories.json` + story endpoints + a frontend Stories
rail. Launch stories span domains — disaster, environment, migration,
infrastructure — breadth as the hook. Screenshot-clean for journalist embeds.

---

## 7 · Tech decisions
- **Leaflet** via CDN (~42KB, no build), **canvas renderer** for the 3,221
  county polygons, muted dark raster basemap. MapLibre GL is the escape hatch.
- Layer API serves `{fips:value}`; frontend joins to cached boundary GeoJSON.
- Boundary geometry simplified server-side (Douglas-Peucker); measure payload
  in SPEC_065 pre-flight; state-level first, county on zoom.
- No new infra — runs on the cloud-connected API.

---

## 8 · Non-goals / honest deferrals
- No pricing / payments / order forms (that's PLAN_069, gated).
- No real-time streaming — Recent Activity is daily-refresh.
- No flight-tracking / CCTV / SIGINT — not our data, not our identity.
- USAspending / ACS-county / FCC-county layers ship thin or deferred until
  PLAN_067 backfills them — the explorer is honest about it.
- No time-scrubber / compare-mode polish in v1 (v2.1).

## 9 · Success metrics — discover the wedge
Telemetry (`atlas_events`) — 30-day read:
- **Activation** — sessions that toggle ≥1 layer + click ≥1 place.
- **Breadth** — distinct domains explored per session (is breadth used?).
- **Return** — 7-day repeat-visit rate.
- **Follow / share** — conversion to follow; `share_created` rate.
- **Wedge discovery** — *which domains/layers/places dominate exploration* —
  this names the audience PLAN_068/069 should target.

## 10 · Risks
| Risk | Mitigation |
|---|---|
| General explorer is hard to make sticky / monetize | Instrument heavily; discover the wedge from telemetry (§9); PLAN_069 is gated on proof |
| Breadth feels shallow — many thin layers | Honest grading (§4); lead with the strong domains; PLAN_067 deepens the rest |
| 3,221 county polygons slow in Leaflet | Canvas renderer + simplified geometry; SPEC_065 pre-flight measures it |
| ~400-table catalog audit is large | Time-boxed in SPEC_065 pre-flight; ship the verified strong layers first, expand the catalog iteratively |

## 11 · Sequencing
```
SPEC_065 (layer API + catalog audit) → SPEC_066 (map frontend)
                                          ↓ [usable explorer shipped]
            SPEC_067 (activity) · SPEC_068 (follow) · SPEC_069 (stories)
```
SPEC_065+066 are the critical path. One commit per spec.
