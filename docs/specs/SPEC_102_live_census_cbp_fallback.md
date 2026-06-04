# SPEC 102 — Live Census CBP fallback for missing (county, NAICS) rows

**Status:** Draft
**Task type:** service
**Date:** 2026-06-03
**Test file:** tests/test_spec_102_live_cbp_fallback.py
**Plan:** docs/plans/PLAN_080_live_cbp_fallback.md

## Goal

When `find_competition_cbp` queries a (county, NAICS) tuple that isn't
in `census_cbp_county_yearly`, fall back to a live Census CBP API call
for just that tuple, **persist the result** back into the table, and
serve it. Subsequent requests for the same tuple hit the DB.

This closes the gap left by the SPEC_100 backfill — Census disclosure
suppression and the wildcard-prefix loop sometimes skip individual
(county, NAICS) cells that ARE retrievable via a direct query.

## Acceptance Criteria

- [ ] New `app/services/atlas/cbp_live.py` exposes
      `fetch_cbp_cell(db, geo_id, naics, year) -> Optional[int]`.
      Returns the `establishments` count, or None when Census also has
      nothing.
- [ ] Single Census call per (geo_id, naics) — no wildcard, no batching.
      Verified live: `?for=county:{cty}&in=state:{st}&NAICS2017={code}`
      returns one row when data exists, 204 when truly missing.
- [ ] On success, UPSERTs into `census_cbp_county_yearly`. Subsequent
      reads via the existing SQL path return the new row.
- [ ] `find_competition_cbp` gains an optional `live_fallback=True`
      parameter (default True for the Pilot tool + endpoint, False
      for tests). When True, any geo_ids in the request that returned
      0 rows OR `establishments IS NULL` get a single live API call
      per missing cell.
- [ ] In-process LRU cache (60s TTL) on the `fetch_cbp_cell` function
      so a flurry of trade-area requests in the same minute don't
      hammer Census for the same tuple.
- [ ] Per-request cap: at most 30 live calls per
      `find_competition_cbp` invocation. Above that, fall back
      silently to whatever the DB has.
- [ ] Soft-fail: any Census error (timeout, 5xx, malformed JSON)
      returns None and logs WARN. The trade-area card never breaks
      because of a missing cell.
- [ ] Response gets a new `live_fetched: int` field counting how many
      cells were filled live. Frontend can show "(N cells from live
      Census)" as a debug breadcrumb.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_fetch_cbp_cell_happy_path | Mock Census 200 → single row → returns int |
| T2 | test_fetch_cbp_cell_returns_none_on_204 | Census 204 (truly missing) → None |
| T3 | test_fetch_cbp_cell_caches | Same (geo_id, naics) within 60s = 1 API call |
| T4 | test_fetch_cbp_cell_upserts_to_table | After fetch, the row is in the table |
| T5 | test_find_competition_cbp_falls_back | Missing rows trigger live calls; aggregate now correct |
| T6 | test_find_competition_cbp_respects_per_request_cap | >30 misses → first 30 fetched, rest skipped |
| T7 | test_find_competition_cbp_live_fallback_false | When disabled, behaves like SPEC_100 |
| T8 | test_fetch_cbp_cell_soft_fails_on_timeout | Timeout returns None, no exception |
| T9 | test_live_fetched_counter_in_response | Response includes `live_fetched: int` |

## Design Notes

```python
# app/services/atlas/cbp_live.py
from functools import lru_cache
from datetime import datetime, timedelta

_CACHE: dict[tuple[str, str, int], tuple[Optional[int], datetime]] = {}
_TTL = timedelta(seconds=60)

def fetch_cbp_cell(db, geo_id, naics, year=2022) -> Optional[int]:
    key = (geo_id, naics, year)
    now = datetime.utcnow()
    hit = _CACHE.get(key)
    if hit and (now - hit[1]) < _TTL:
        return hit[0]
    # Census call (~50-200ms)
    n = _call_census(geo_id, naics, year)
    _CACHE[key] = (n, now)
    if n is not None:
        _upsert(db, geo_id, naics, year, n)
    return n
```

The Census call uses the same `_create_table_sql` + `httpx` pattern as
`county_cbp.py`, but with a single county + single NAICS — no wildcards.
We learned during SPEC_100 that `for=county:{cty}&in=state:{st}` works
for any NAICS code (including `'00'`, `'442'`, `'442110'`, etc) without
needing the `state:*` workaround.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/services/atlas/cbp_live.py` | Create | `fetch_cbp_cell` + LRU cache + upsert |
| `app/services/atlas/competition.py` | Modify | `find_competition_cbp(live_fallback=True)` |
| `app/api/v1/atlas.py` | Modify | Threading the new param through `/competition` |
| `tests/test_spec_102_live_cbp_fallback.py` | Create | T1-T9 |

## Out of Scope

- Backfilling other years on demand (still 2022 only)
- Bulk fetch optimisation (we do one call per missing cell)
- Refresh policy for stale cells (table is read-mostly; cells age out
  naturally on the next backfill)

## Feedback History

_No corrections yet._
