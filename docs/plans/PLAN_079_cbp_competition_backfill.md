# PLAN 079 — CBP Competition Backfill + Lookup (replace Yelp)

**Spec:** docs/specs/SPEC_100_cbp_competition_lookup.md
**Date:** 2026-06-03
**Driver:** Yelp Fusion trial expired. User picked "backfill county_yearly with NAICS detail (cleanest)" over the live-API and state-grain fallbacks. Ratings are explicitly out of scope for this plan (handled by a follow-up spec).

## Phase split — what ships when

The work splits into four phases that can be reviewed/landed independently:

### Phase 1 — Industry → NAICS resolver (pure function, no DB)
- Create `app/services/atlas/industry_naics.py` with `industry_to_naics(label, naics_hint)`.
- Hand-curated keyword table for ~30 common decision-map industries.
- Falls back to `None` (caller uses NAICS='00').
- Tests T1–T3.
- **Ship gate:** unit tests pass. Cheapest phase; lands first.

### Phase 2 — CBP backfill ingest (DB writes)
- Modify `app/sources/census/county_cbp.py` to loop the 20 NAICS-2 sectors
  per year and ingest detail rows via `NAICS2017={sector}*` (which Census
  returns as a tree — sector + children).
- Add a CLI entry: `python -m app.sources.census.county_cbp --year 2022 --naics-all`
  that runs the full backfill.
- Tests: T11 (mock Census API → verify all NAICS levels persisted).
- **Ship gate:** dry-run smoke against one county on the live Census API
  succeeds. Full backfill scheduled as a one-off `ingestion_jobs` task.
- **Estimate:** ~3M rows for 2022, ~15-30 min wall clock for the API
  calls (rate-limited 5 req/s default).

### Phase 3 — Competition service + Pilot tool refactor
- `app/services/atlas/competition.py`: new `find_competition_cbp` query;
  legacy `find_competition` becomes a thin adaptor that resolves lat/lon
  → focal_geo_id via `county_centroids` reverse and term → NAICS via the
  new resolver.
- `app/services/atlas/pilot_tools.py`: `_tool_find_competition` returns
  the new shape (`per_county` instead of `businesses`).
- `app/api/v1/atlas.py`: `/competition` body model — add optional
  `focal_geo_id` + `naics`; keep lat/lon + term for backwards compat.
- Tests: T4–T9.
- **Ship gate:** all SPEC_100 backend tests pass; SPEC_091 rewritten
  test variant also passes.

### Phase 4 — Frontend Trade Area card rework
- `frontend/atlas.html`:
  - `updateCompetitionCardRow(r)`: read `r.per_county` instead of
    `r.businesses`. Render "**N** *NAICS-label* establishments in this
    county; **M** across N neighbouring counties." Optionally render a
    mini per-county breakdown table.
  - `renderCompetitionPins`: no-op stub (CBP gives no pin lat/lons).
  - `COMPETITION_LAYER`: remove all addLayer / clearLayer calls; the
    orange pin layer is gone.
- Tests: T10 (structural HTML / JS assertion).
- **Ship gate:** Trade Area card renders without errors; no `cursor: grab`
  or click-passthrough regressions.

## Acceptance Criteria (whole-PR)

- Yelp dependency removed from the Decision Map's primary path. The
  `yelp` source still exists in the codebase for other features.
- A Trade Area card opened on Loudoun with thesis "Furniture stores"
  shows e.g. "183 furniture & home furnishings establishments in
  Loudoun County; 1,247 across 12 neighbouring counties (NAICS 442,
  2022)."
- Backend round-trip < 100ms (SQL only; no external API).
- 199 + 11 = 210 tests green; SPEC_091 (the original Yelp suite)
  rewritten in-place; no XFAIL leftovers.
- DB rowcount in `census_cbp_county_yearly` jumps from 16,222 → ~3M
  (2022 only) or ~15M (2017–2022).

## Risks + Mitigations

| Risk | Mitigation |
|---|---|
| Census API rate-limit during backfill | Loop sectors at 1 req/s; checkpoint per (year, sector) so resume is cheap |
| NAICS prefix match misses (e.g. user says "café", we stored "cafe") | Normalize input (lowercase, strip accents) before lookup; tests on a few common typos |
| Frontend rendering breaks because shape changed | Phase 4 lands AFTER phase 3 so the backend serves the new shape first; SPEC_091 rewrite catches the shape drift |
| Backfill takes too long for a single job runner cycle | Split into per-year jobs; APScheduler can chain them |

## Out of Scope (next specs)

- Ratings / review counts (SPEC_101: Foursquare Places integration)
- Live Census API call as a fallback when DB row is missing (SPEC_102)
- Time-series of competition density (CBP is already multi-year — easy follow-up)

## Phase order rationale

1. NAICS resolver first — pure function, no DB, no external API; gives
   the rest of the work a clean dependency.
2. Backfill second — has the longest wall clock and is risk-isolated.
   Can run overnight.
3. Service refactor third — gates the frontend.
4. Frontend last — visible to the user, but only meaningful once 1-3
   are landed.
