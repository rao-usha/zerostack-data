# SPEC 101 — Google Places ratings for competition

**Status:** Draft
**Task type:** service
**Date:** 2026-06-03
**Test file:** tests/test_spec_101_google_places_ratings.py
**Plan:** docs/plans/PLAN_081_google_places_ratings.md

## Goal

Layer Google Places ratings onto SPEC_100's CBP-derived competition
count. For a focal county + thesis NAICS, return the median rating and
review-count distribution of POIs in the focal county matching the
NAICS-mapped category, so the Trade Area card answers both "how many?"
(CBP) and "how good?" (Google).

## Background

User confirmed ratings matter for the "is this a competitive trade
area?" question. No free public source provides ratings; we picked
Google Places over Foursquare. Google Places (New) has $200/mo free
credit (≈4k Text Search calls or ≈11k Place Details calls). For a
Decision Map demoing a handful of theses, that comfortably covers
expected usage.

## Google Places API caveats (read before implementing)

- **ToS — cache TTL**: Place data may be cached for up to **30 days**
  for performance/caching purposes. We cache rating + total ratings
  per (place_id) for 30 days, hard expiry.
- **ToS — display**: When displaying Place data in our UI, we must
  show **"Powered by Google"** attribution. Spec includes a CSS class
  + text label in the Trade Area card footer.
- **ToS — no bulk redistribution**: We aggregate ratings on the fly
  per request and never expose raw place_id lists in our API. The
  /atlas/competition response gets a derived `ratings` block — median
  + count buckets only.
- **PII**: place owners' content is not PII; user-generated reviews
  ARE — we never fetch review text, only rating + total_ratings.

## Acceptance Criteria

- [ ] New `app/sources/google_places/client.py` with `BaseAPIClient`
      subclass. Auth via `GOOGLE_PLACES_API_KEY`. Free-tier safe rate
      limiting (default 5 req/s).
- [ ] New `app/services/atlas/ratings.py` exposes
      `fetch_ratings(db, geo_id, naics) -> RatingSummary` where
      RatingSummary has `{count, mean_rating, median_rating,
      rating_distribution: {1: n, 2: n, ..., 5: n}}`.
- [ ] NAICS → Google Place category mapping in
      `app/services/atlas/google_place_categories.py`. ~30 entries
      covering the common Decision Map industries.
- [ ] Two-stage Google fetch:
      1. Nearby Search (`searchNearby`) within the focal county
         centroid + 5-mi radius, filtered by category.
      2. (Optional) Place Details per top-N for aggregate.
      Free-tier-friendly: searchNearby is the cheaper endpoint.
- [ ] 30-day Postgres cache table `google_places_cache` keyed on
      `(geo_id, naics, asked_at)`. Reads cache when fresh; refreshes
      otherwise. Cache stores aggregates only — no place_id leak.
- [ ] `/atlas/competition` response adds an optional `ratings` block
      when GOOGLE_PLACES_API_KEY is set. Block is `None` when the key
      is unset or Google soft-fails.
- [ ] Trade Area card adds a thin ratings strip below the CBP row:
      "★ 4.3 average across 47 reviewed locations · Powered by
      Google". Strip is hidden when ratings is null.
- [ ] Soft-fail: no key → null block, log INFO. Quota exceeded →
      null block, log WARN. Any 5xx → null block, log WARN.
- [ ] Per-request cap: at most 1 searchNearby call per
      find_competition. The aggregate uses ONLY the searchNearby
      result; no Place Details N-times-roundtripping.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_google_client_uses_env_key | GoogleClient reads GOOGLE_PLACES_API_KEY |
| T2 | test_naics_to_category_known_industries | 442 → "furniture_store", 722515 → "coffee_shop", etc. |
| T3 | test_naics_to_category_unknown_falls_back | Unknown NAICS → returns None (caller skips fetch) |
| T4 | test_fetch_ratings_happy_path | Mock Google response → RatingSummary populated |
| T5 | test_fetch_ratings_caches_30d | Same (geo_id, naics) within 30 days → no API call |
| T6 | test_fetch_ratings_soft_fails_missing_key | No GOOGLE_PLACES_API_KEY → returns None, no exception |
| T7 | test_fetch_ratings_soft_fails_on_quota | 429 → None + WARN log |
| T8 | test_competition_endpoint_includes_ratings_when_key_set | /competition response.ratings populated |
| T9 | test_competition_endpoint_omits_ratings_without_key | Block is None when key unset |
| T10 | test_trade_area_card_renders_ratings_strip | Frontend reads r.ratings; shows "Powered by Google" |
| T11 | test_cache_table_schema | google_places_cache columns present + indexed |
| T12 | test_no_place_id_leakage | /competition response has no place_id strings (ToS) |

## Design Notes

```python
# app/services/atlas/ratings.py
@dataclass
class RatingSummary:
    count: int
    mean_rating: Optional[float]
    median_rating: Optional[float]
    rating_distribution: dict[int, int]  # {1: n, 2: n, ..., 5: n}
    source: str = "google_places"

def fetch_ratings(db, geo_id, naics) -> Optional[RatingSummary]:
    if not os.getenv("GOOGLE_PLACES_API_KEY"):
        return None
    cached = _read_cache(db, geo_id, naics, max_age_days=30)
    if cached:
        return cached
    category = naics_to_google_category(naics)
    if not category:
        return None
    centroid = county_centroids(db).get(geo_id)
    if not centroid:
        return None
    lat, lon, _ = centroid
    try:
        places = google_nearby_search(lat, lon, radius_m=8000,
                                       included_types=[category],
                                       max_count=20)
    except Exception:
        return None
    summary = _aggregate(places)
    _write_cache(db, geo_id, naics, summary)
    return summary
```

Sample NAICS → category map (Google's "Place Type" v3):
```python
NAICS_TO_GOOGLE_CATEGORY = {
    "442": "furniture_store",
    "445110": "supermarket",
    "722511": "restaurant",
    "722513": "fast_food_restaurant",
    "722515": "coffee_shop",
    "447110": "gas_station",
    "5311": "real_estate_agency",
    "5413": "engineering_company",
    # ... ~25 more
}
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/sources/google_places/client.py` | Create | BaseAPIClient subclass + searchNearby |
| `app/sources/google_places/__init__.py` | Create | |
| `app/services/atlas/google_place_categories.py` | Create | NAICS → category map |
| `app/services/atlas/ratings.py` | Create | `fetch_ratings` + cache I/O |
| `app/services/atlas/competition.py` | Modify | `find_competition_cbp` optionally fills `ratings` |
| `app/api/v1/atlas.py` | Modify | `/competition` response shape |
| `app/core/models.py` | Modify | `GooglePlacesCache` table |
| `frontend/atlas.html` | Modify | Ratings strip rendering + "Powered by Google" attribution |
| `tests/test_spec_101_google_places_ratings.py` | Create | T1-T12 |

## Out of Scope

- Yelp Fusion (paid)
- Foursquare (skipped per user choice)
- Place Details / photos / reviews text (cost + PII risk)
- Per-neighbour ratings (focal-only for now; neighbour ratings come in
  a follow-up if needed)
- Real-time refresh — 30-day TTL is the contract

## Feedback History

_No corrections yet._
