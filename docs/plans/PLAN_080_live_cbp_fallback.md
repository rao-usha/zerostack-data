# PLAN 080 — Live Census CBP fallback for missing cells

**Spec:** docs/specs/SPEC_102_live_census_cbp_fallback.md
**Date:** 2026-06-03
**Driver:** SPEC_100's bulk backfill loaded 1.1M rows but Census disclosure suppression + the wildcard prefix call sometimes skip individual (county, NAICS) tuples that ARE retrievable via a direct lookup. SPEC_102 plugs that gap with a per-request live API fallback that also persists the result.

## Phase 1 — cbp_live.py + cache + persistence (T1-T4)
- `fetch_cbp_cell(db, geo_id, naics, year)` — single Census call, parses one row, returns int or None.
- In-process LRU keyed on `(geo_id, naics, year)` with 60s TTL.
- On success: UPSERT into `census_cbp_county_yearly` so the next request hits the DB.
- Verified live during SPEC_100: `for=county:{c}&in=state:{s}&NAICS2017={n}` works for any NAICS.

## Phase 2 — find_competition_cbp wiring (T5-T7, T9)
- New `live_fallback=True` arg.
- After the SQL aggregate, scan `per_county` for rows where `establishments` is `None` (no DB row).
- Issue up to 30 `fetch_cbp_cell` calls in parallel via `asyncio.gather` wrapped in `asyncio.run`.
- Augment the response with `live_fetched: int`.
- Refresh per_county from the now-fuller DB and re-sort.

## Phase 3 — endpoint plumbing + soft-fail tests (T8)
- `/atlas/competition` body accepts `live_fallback: Optional[bool] = True`.
- Per-request cap is enforced inside `find_competition_cbp`, not the endpoint.
- Soft-fail on Census timeout / 5xx → log WARN, return what we have.

## Acceptance (whole-PR)
- A trade-area request for a focal county at a NAICS the backfill skipped now returns the right count instead of 0.
- Second request for the same tuple is served from the table.
- 9/9 SPEC_102 tests green; 210/210 SPEC_082-100 regression unchanged.

## Risks + Mitigations
| Risk | Mitigation |
|---|---|
| 30 live calls per request blows the rate limit | LRU + per-request cap + the Census key is rate-limited generously (500 req/day for unkeyed, unlimited for keyed) |
| Race condition on concurrent UPSERT for the same tuple | `ON CONFLICT DO UPDATE` — Postgres serializes, no corruption |
| Census API outage drives every request into the slow path | LRU absorbs the spike; soft-fail keeps the UI green |

## Out of Scope
- Multi-year fallback (2022 only)
- Bulk fetch in one HTTP call (single tuples only)
