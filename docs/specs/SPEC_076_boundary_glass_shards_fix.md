# SPEC 076 — Fix boundary "glass shards" (disable lossy Python simplifier)

**Status:** Done (shipped same-session as the user report)
**Task type:** bug_fix
**Date:** 2026-05-25
**Plan:** none — bug surfaced by user during click-through
**Test file:** _none_ — verified by smoke harness + screenshot regression

## Goal

Eliminate inter-county "glass shards" (thin gaps along shared
boundaries) the user reported when zooming into the map.

## Root cause

`app/services/atlas/boundaries.py::_simplify_python` did naive
per-ring vertex skipping (`ring[::step]`) when PostGIS wasn't
available. The Atlas cloud DB doesn't have PostGIS, so this path was
always active. The bug: adjacent counties' rings start at different
vertices and traverse their shared edge in different orientations.
`[::step]` picks DIFFERENT vertices on each side of the shared
boundary → the simplified borders no longer align → background shows
through as a thin sliver ("glass shards").

The same source coordinates produced consistent simplification with
PostGIS's `ST_Simplify` (topology-aware on shared edges) but cloud
SQL Standard doesn't expose PostGIS.

## Fix

Renamed `_simplify_python` → `_unwrap_geometry` and stripped the
vertex-thinning logic. The function now only unwraps the
`geojson_boundaries.geojson` Feature wrapper into a bare geometry —
no vertex dropping. Source data is already cartographically
simplified to ~50-100 vertices per county at ingest time, so payload
is acceptable raw.

Trade-off:
- Payload: 1.2 MB → 2.7 MB (one-time fetch + in-memory cache)
- Visual: shards gone

Meta label changed from `"python_thin"` → `"raw_unsimplified"` so
clients can tell which simplifier is active.

## Acceptance Criteria

- [x] `/atlas/boundaries` returns geometry with no inter-county gaps
- [x] Headless screenshot at zoom 7 over Texas shows clean borders
- [x] Smoke harness stays green (40/40)
- [x] SPEC_065 T9 (boundaries shape) still passes — no change to schema

## Proper fix (deferred to SPEC_076b)

The right answer is **TopoJSON-based simplification**: convert the
source GeoJSON into TopoJSON's shared-arc representation, simplify
each arc once with Visvalingam-Whyatt or Douglas-Peucker, then
serve. This preserves topology across shared edges by construction.

Approach when we get to it:
1. Add the `topojson` Python package as a dependency
2. Offline pre-processing job: read `geojson_boundaries`, convert to
   TopoJSON, simplify at multiple zoom-tolerance levels, store back
   into a new `geojson_boundaries_topojson` table keyed by
   `(geo_level, tolerance)`
3. `fetch_boundaries` queries the pre-simplified table by tolerance
4. Payload shrinks to <500 KB at zoom-out tolerance, full detail at
   zoom-in tolerance

Out of scope here — fix the visual regression first, optimize later.

## Files Modified

| File | Change |
|---|---|
| `app/services/atlas/boundaries.py` | `_simplify_python` → `_unwrap_geometry`; removed vertex thinning; meta label updated |
| `docs/specs/.active_spec` | → `SPEC_076_boundary_glass_shards_fix` |

## Verification

- `GET /atlas/boundaries?geo_level=county` returns 2.7 MB,
  meta.simplifier = `raw_unsimplified`
- Texas zoom screenshot shows clean adjacent-county borders
- Smoke harness 40/40 pass (no regression)

## Feedback History

User report (2026-05-25): "The county borders are bizarre.. They are
like glass shards" — fixed within same session.
