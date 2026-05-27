# SPEC 077 — Phase A foundation: basemap + sub-county boundaries + tract ACS

**Status:** Draft
**Task type:** collector + frontend integration
**Date:** 2026-05-26
**Plan:** PLAN_073 rev_01 Phase A
**Test file:** `tests/test_spec_077_phase_a.py`
**Builds on:** SPEC_065 boundary serving; SPEC_070 county_acs.py; SPEC_076 boundary fix.

## Goal

Phase A of the Atlas Pilot pivot. Three deliverables that **make the
map look like a real map at neighborhood scale**, which is the
foundation everything else (focal node, competition radii, demand
surface, supplier arcs) builds on.

Specifically:
- **A.1** — add a real basemap tile layer (Carto-dark, ~3 lines of Leaflet)
- **A.2** — ingest ZCTA + census-tract boundaries from Census TIGER
- **A.3** — tract-grain ACS layers for the demand surface (median
  income, age, density, owner-occupied housing)
- **A.4** — zoom defaults: click-to-place auto-zooms to neighborhood
  scale (~zoom 14)

These four ship as one spec because they're all "the new foundation"
and each one is too small to be its own spec. They sub-ship in order
of cost: A.1 (1 hour) → A.2 (1 day) → A.3 (1 day) → A.4 (1 hour).

## Why Phase A first

PLAN_073 §3's headline UX needs:
- a focal node *on a real street* (needs basemap A.1)
- competitor radii *visible at neighborhood scale* (needs zoom A.4)
- demand surface *at trade-area grain* (needs A.3 at tract grain)
- supplier arcs *over actual geography*, not abstract county centroids (needs A.1 basemap)

Phase A is the substrate Phases B-G depend on. None of the agent
infrastructure or simulator logic adds value until the map can
zoom-in and show real streets.

## Acceptance Criteria

### A.1 — Basemap
- [ ] `frontend/atlas.html` loads a Carto Dark Matter basemap tile
      layer under the existing canvas choropleth.
- [ ] At all zoom levels 4-18, the basemap renders street/place context.
- [ ] Basemap attribution string visible.
- [ ] Existing 40/40 smoke scenarios stay green (basemap is purely additive).

### A.2 — Sub-county boundary ingest
- [x] Tract rows in `geojson_boundaries` (geo_level='tract'),
      78,383 rows from TIGERweb 2020.
- [ ] **ZCTA deferred** — TIGERweb's WAF blocks the standard 1000-
      feature page size (returns HTML 200 reject). The 200-feature
      smaller-page workaround works but adds ~15min ingest time.
      Tracts are sufficient for Phase C demand surfaces; ZCTAs
      arrive as SPEC_077b once the focal-node UX needs ZIP-based
      input (which it doesn't strictly need — tract is finer-grain).
- [x] `GET /atlas/boundaries?geo_level=tract` returns 78,383 features.
- [ ] `GET /atlas/boundaries?geo_level=zcta` will work once SPEC_077b
      ships ZCTAs (endpoint already accepts the geo_level).
- Performance note: tract payload is ~30-50 MB unsimplified. Phase A
  doesn't bbox-filter; Phase C will add `?bbox=` when the focal-node
  UX needs neighborhood-scoped tracts. Until then, the endpoint is
  available but slow to fully load — fine for dev testing, not
  production use.

### A.3 — Tract-grain ACS demand layers
- [ ] New table `acs5_tract_2023_demand` with one row per tract,
      columns: median income (B19013), median age (B01002), total
      population (B01003), owner-occupied housing share derived from
      B25003.
- [ ] Reuses SPEC_070's `county_acs.py` multi-variable pattern with
      `for=tract:*&in=state:*`.
- [ ] ≥70,000 tract rows landed.
- [ ] Four new Atlas layers registered with `grain="tract"`:
  - `demo_tract_median_income`
  - `demo_tract_median_age`
  - `demo_tract_population_density` (computed pop / tract area)
  - `demo_tract_owner_occupied`

### A.4 — Zoom defaults
- [ ] Clicking a place via the frontend (place panel or recent-feed
      item) auto-zooms the map to level 14 (~neighborhood scale).
- [ ] At zoom ≥10, the boundary layer switches from county → tract
      (denser detail). At zoom <8, stays at county.
- [ ] Hover tooltip respects the current grain (tract names at zoom
      10+, county names at zoom <10).

## Test Cases

| ID | What | Where |
|---|---|---|
| T1 | `geojson_boundaries_tract` has ≥70k rows post-ingest | pytest |
| T2 | every tract geo_id is 11-digit (state+county+tract) | pytest |
| T3 | `geojson_boundaries_zcta` has ≥30k rows | pytest |
| T4 | every ZCTA geo_id is 5-digit | pytest |
| T5 | `/atlas/boundaries?geo_level=tract` returns FeatureCollection with ≥70k features | pytest |
| T6 | `/atlas/boundaries?geo_level=zcta` returns FeatureCollection with ≥30k features | pytest |
| T7 | `acs5_tract_2023_demand` has ≥70k rows | pytest |
| T8 | each tract row has non-null median_income for ≥80% of tracts | pytest |
| T9 | `/atlas/layer/demo_tract_median_income` returns ≥70k values | pytest |
| T10 | smoke harness: basemap renders + tract layers render | smoke_atlas.py |

## Design Notes

### A.1 Basemap (frontend only)
```js
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  attribution: '© OpenStreetMap contributors, © CARTO',
  subdomains: 'abcd',
  maxZoom: 19,
}).addTo(MAP);
```
Added before `renderBoundaries()` so it sits under the choropleth.

### A.2 Sub-county ingest
- Source: Census TIGER bulk shapefiles (free, public)
- Local conversion: `ogr2ogr` shapefile → GeoJSON, then INSERT into
  `geojson_boundaries_{zcta|tract}` with same schema as existing
  `geojson_boundaries` (geo_id, geo_name, geojson, geo_level).
- One-shot ingest script: `scripts/ingest_tiger_boundaries.py`
- Likely runs locally on a dev machine (not in the api container) due
  to shapefile dependencies; ingests to cloud DB.

### A.3 Tract ACS
- Reuses `app/sources/census/county_acs.py::ingest_county_acs_multi`
  pattern. Will need a small generalization to accept `geo_level`
  parameter ("county" vs "tract").
- One API call per state (Census API requires `in=state:XX` for tract
  pulls) — ~50 calls × ~1,500 tracts each = ~70k rows total.
- Variables: B19013_001E, B01002_001E, B01003_001E, B25003_002E, B25003_001E.

### A.4 Zoom defaults
- Existing `openPlace()` in `frontend/atlas.html` sets `MAP.fitBounds`
  with `maxZoom: 8`. Bump to `maxZoom: 14` for county clicks, and add
  a separate handler for tract clicks (`maxZoom: 16`).
- Boundary-grain switching: in the boundaries fetch, the frontend
  watches `MAP.getZoom()`; on zoom-end ≥10 it requests
  `?geo_level=tract` and replaces the boundary layer; on zoom-end <8
  it requests `?geo_level=county`.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_077_phase_a_foundation.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_077_phase_a_foundation` |
| `tests/test_spec_077_phase_a.py` | Create | T1-T10 |
| `frontend/atlas.html` | Modify | A.1 basemap, A.4 zoom defaults |
| `scripts/ingest_tiger_boundaries.py` | Create | A.2 ZCTA + tract ingest |
| `scripts/ingest_acs_tract_demand.py` | Create | A.3 tract ACS ingest |
| `app/sources/census/county_acs.py` | Modify | Accept `geo_level` param ("county" vs "tract") |
| `app/services/atlas/boundaries.py` | Modify | Support `geo_level="zcta"` and `="tract"` |
| `app/services/atlas/layers.py` | Modify | Register 4 new tract-grain layers |
| `scripts/smoke_atlas.py` | Modify | Add scenarios for tract-grain + zoom-14 |

## Verification (manual)

1. Frontend: open `/atlas.html` — see street-context basemap rendered.
2. `curl /atlas/boundaries?geo_level=tract | jq '.features | length'` ≥ 70,000
3. `curl /atlas/layer/demo_tract_median_income | jq '.values | length'` ≥ 70,000
4. Click a county → map auto-zooms to 14, boundaries switch to tract grain.
5. `python scripts/smoke_atlas.py` — all scenarios pass.

## Feedback History

_No corrections yet._
