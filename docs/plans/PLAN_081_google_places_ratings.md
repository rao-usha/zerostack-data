# PLAN 081 — Google Places ratings for competition

**Spec:** docs/specs/SPEC_101_google_places_ratings.md
**Date:** 2026-06-03
**Driver:** User said ratings matter for the "is this a competitive trade area?" question. CBP gives counts; only paid sources give ratings. User picked Google over Foursquare. Free tier ($200/mo credit) is enough for demo workloads.

## ToS-driven design constraints
1. 30-day cache TTL (max permitted).
2. "Powered by Google" attribution in the Trade Area card footer when the ratings strip is visible.
3. Never persist place_id outside the cache table; never expose place_id in the API response.
4. No review text — rating + count only.

## Phase 1 — Google client + NAICS category map (T1-T3)
- `app/sources/google_places/client.py` — BaseAPIClient subclass. `searchNearby` endpoint only.
- `app/services/atlas/google_place_categories.py` — NAICS → Google Place Type. ~30 entries.
- Unit tests with mocked HTTP.

## Phase 2 — ratings service + cache table (T4-T7, T11)
- `app/services/atlas/ratings.py` — `fetch_ratings(db, geo_id, naics)`.
- `GooglePlacesCache` table: `(geo_id, naics, asked_at, summary_json)`. Index on (geo_id, naics).
- 30-day TTL check on read.
- Aggregation: mean, median, distribution buckets {1..5}.
- Soft-fail on missing key + 429 + 5xx.

## Phase 3 — endpoint wiring + ToS checks (T8-T9, T12)
- `/atlas/competition` response gains `ratings` block (or None).
- Test that no place_id strings leak into the response (regex assert).

## Phase 4 — frontend strip + attribution (T10)
- Trade Area card gets a ratings strip just below the CBP row.
- "★ 4.3 average · 47 reviewed locations" + "Powered by Google" footer.
- Strip is hidden when `r.ratings` is null.
- New CSS class `.ta-comp-ratings` + `.ta-comp-attribution`.

## Acceptance (whole-PR)
- User adds GOOGLE_PLACES_API_KEY to .env, restarts the api; Trade Area card for Loudoun + "Furniture stores" shows a Google-rated strip.
- Without the key, the strip is hidden; the CBP-only experience is unchanged.
- 12/12 SPEC_101 tests green; 210+ regression unchanged.

## Risks + Mitigations
| Risk | Mitigation |
|---|---|
| Quota burn from a viral demo | 30-day cache + 1 call per request + monitor via `llm_cost_tracker`-style log |
| ToS violation (caching, attribution, place_id leak) | Encoded in T12 regression test + frontend attribution strip + Postgres cache TTL gate |
| NAICS category map drift | Hand-curated; tests assert known mappings; unknown NAICS → null block (no API call) |

## Out of Scope
- Per-neighbour ratings
- Place Details / photos / reviews text
- Yelp / Foursquare (already decided)
- Live multi-source rating consensus (just Google for now)
