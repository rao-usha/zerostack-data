# SPEC 100 — CBP-backed competition lookup (replace Yelp)

**Status:** Draft
**Task type:** service
**Date:** 2026-06-03
**Test file:** tests/test_spec_100_cbp_competition.py
**Plan:** docs/plans/PLAN_079_cbp_competition_backfill.md

## Goal

Replace Yelp Fusion as the source of "competition near this place" with
a Census CBP (County Business Patterns) lookup at the thesis NAICS. The
Yelp free trial just expired, and CBP is the federal establishment
census — more authoritative for *count* than Yelp's opt-in directory,
and free + public. Ratings/reviews are explicitly out of scope (a later
spec can wire Foursquare or paid Yelp for that signal).

## Background

DB state today (verified 2026-06-03):
- `census_cbp_county_yearly`: 16,222 rows, NAICS=`'00'` only (totals).
  PK already `(year, geo_id, naics_code)`.
- `census_business_patterns`: 63,369 rows, NAICS 2–6 digit but STATE
  grain only.
- No NAICS-by-county breakdown anywhere.

The competition lookup needs *NAICS × county* grain. The simplest path
that respects "public-data, in-DB, fast" is to backfill
`census_cbp_county_yearly` with NAICS detail, then have
`find_competition` do a SQL aggregate at the thesis NAICS.

## Acceptance Criteria

- [ ] CBP backfill ingests all NAICS-2 / NAICS-4 / NAICS-6 rows for the
      latest CBP vintage (2022) into `census_cbp_county_yearly`.
      Rowcount jumps from ~16k to several million.
- [ ] No schema change required — PK already includes `naics_code`.
- [ ] `app/services/atlas/competition.py` exposes a new
      `find_competition_cbp(db, focal_geo_id, neighbor_geo_ids, naics, year)`
      that returns the *same response shape* as the old Yelp wrapper,
      minus pin lat/lons and ratings.
- [ ] The old `find_competition(lat, lon, ..., term)` becomes a thin
      adaptor: looks up the focal county FIPS from lat/lon (via
      `county_centroids` reverse), looks up the NAICS via the new
      `industry_to_naics(label)` helper, and calls the CBP query.
- [ ] A new helper `app/services/atlas/industry_naics.py` maps a thesis
      `industry_label` → NAICS prefix (2/4/6-digit). Uses
      `app/services/diligence/taxonomies.py` and a hand-curated
      keyword table for the 30 most common decision-map industries
      (furniture, coffee, restaurants, manufacturing, warehousing,
      etc.).
- [ ] Pilot `find_competition` tool (`pilot_tools._tool_find_competition`)
      returns the new shape; `top` becomes a per-county breakdown
      `[{geo_id, name, establishments, distance_mi}, ...]` instead of
      Yelp businesses.
- [ ] Trade Area frontend card (`#trade-area-card .ta-comp`) shows
      "**N** retail establishments in this county; **M** across N
      neighbouring counties." No more orange Yelp pins on the map —
      `renderCompetitionPins` becomes a no-op or is removed.
- [ ] Soft-fail: if no row for the focal county × NAICS, return
      `count=0` + a clean message ("No CBP rows for this NAICS at this
      county"). Never raise into the trade-area flow.
- [ ] SPEC_091 tests (the original Yelp suite) are kept but XFAIL or
      rewritten to the new shape — the count assertion stays; the
      Yelp-specific fields (rating, review_count, url) get dropped.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_industry_to_naics_known_keywords | "Furniture stores" → "442"; "Coffee shop" → "722515"; "warehouse" → "493" |
| T2 | test_industry_to_naics_falls_back_to_total | Unknown label → returns `None` (caller falls back to NAICS='00' total) |
| T3 | test_industry_to_naics_uses_explicit_naics_field | Thesis with `industry_naics="722"` short-circuits the label lookup |
| T4 | test_find_competition_cbp_focal_only | Mock CBP rows for one county; helper returns `{count, focal_count, per_county: []}` |
| T5 | test_find_competition_cbp_focal_plus_neighbours | 1 focal + 3 neighbour counties; returns aggregate + per-county breakdown sorted by establishments desc |
| T6 | test_find_competition_cbp_naics_prefix_match | Querying NAICS="44" should aggregate all 442xxx, 443xxx, 444xxx rows for the county |
| T7 | test_find_competition_cbp_no_rows_soft_fails | Empty DB → returns `count=0`, no exception |
| T8 | test_find_competition_legacy_adaptor | Old `find_competition(lat,lon,term)` signature still works; routes lat/lon → focal_geo_id → new path |
| T9 | test_pilot_tool_find_competition_new_shape | `_tool_find_competition` returns `per_county` instead of Yelp `businesses` |
| T10 | test_trade_area_card_shows_cbp_count | Frontend `updateCompetitionCardRow` reads `r.per_county` and renders the new message |
| T11 | test_ingest_county_cbp_year_all_naics | Mock Census API response with NAICS-2/4/6 mix; verify all get inserted with correct PK |

## Rubric Checklist

Generic service-spec checklist (no rubric for `service` task type at memory dir time of writing):
- [ ] Public-data only — no scraping, no PII
- [ ] Soft-fails return a structured response, not exceptions
- [ ] `BaseAPIClient` semantics not violated (CBP ingest already uses raw httpx)
- [ ] SQL parameterised
- [ ] New table columns documented
- [ ] Tests cover happy path + soft-fail + edge cases

## Design Notes

### Industry → NAICS mapping (`app/services/atlas/industry_naics.py`)
```python
INDUSTRY_NAICS_MAP = {
    # Retail
    "furniture": "442",       # Furniture & home furnishings stores
    "home furnishings": "442",
    "coffee": "722515",        # Snack & nonalcoholic beverage bars
    "coffee shop": "722515",
    "restaurant": "722511",    # Full-service restaurants
    "cafe": "722513",          # Limited-service eating places
    "grocery": "445110",
    "convenience": "445120",
    "clothing": "448",
    "electronics": "443",
    # Industrial
    "warehouse": "493",        # Warehousing & storage
    "manufacturing": "31-33",  # Manufacturing
    "logistics": "493",
    "distribution": "493",
    "food processing": "311",
    # Tech / services
    "data center": "518210",
    "office": "5311",          # Lessors of buildings — proxy
    # Healthcare
    "hospital": "622",
    "clinic": "6211",
    # Hospitality
    "hotel": "721",
    "lodging": "721",
}

def industry_to_naics(label: str, naics_hint: Optional[str] = None) -> Optional[str]:
    """Resolve a thesis industry label to a NAICS prefix.
    - If naics_hint is a valid NAICS code, use it.
    - Else match keywords in the label against INDUSTRY_NAICS_MAP.
    - Return None for unknown labels (caller falls back to NAICS='00')."""
```

### CBP query (`competition.find_competition_cbp`)
```python
SELECT geo_id, SUM(establishments) AS n
FROM census_cbp_county_yearly
WHERE geo_id = ANY(:geo_ids)
  AND year = :year
  AND naics_code LIKE :naics_prefix
GROUP BY geo_id
```

The `LIKE` lets a 2-digit NAICS prefix aggregate the underlying 4/6-digit
detail rows. We exclude NAICS=`'00'` (which is the total — would
double-count). If `naics_prefix` is None (unknown industry), fall back
to NAICS=`'00'` (gives the focal+neighbours total — same number we
already display in `Establishments`, but useful as a sanity row).

### Response shape (replaces Yelp shape)
```python
{
  "count": int,                # total across focal + neighbours
  "focal_count": int,
  "neighbours_count": int,
  "per_county": [               # sorted by establishments desc
    {"geo_id": "51107", "name": "Loudoun",
     "establishments": 183, "distance_mi": 0.0,  # focal = 0
     "is_focal": True},
    {"geo_id": "51059", "name": "Fairfax",
     "establishments": 142, "distance_mi": 22.3,
     "is_focal": False},
    ...
  ],
  "naics_used": "442",
  "naics_label": "Furniture & home furnishings stores",
  "year": 2022,
  "radius_mi": float,
  "error": None | str,
}
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/sources/census/county_cbp.py` | Modify | Loop over NAICS prefixes (2-digit sectors) to backfill detail rows |
| `app/services/atlas/industry_naics.py` | Create | `industry_to_naics(label, naics_hint)` helper + keyword table |
| `app/services/atlas/competition.py` | Modify | New `find_competition_cbp`; legacy `find_competition` becomes adaptor |
| `app/services/atlas/pilot_tools.py` | Modify | `_tool_find_competition` returns new shape (`per_county`) |
| `app/api/v1/atlas.py` | Modify | `/competition` endpoint body validates new shape |
| `frontend/atlas.html` | Modify | `fetchCompetition` body, `updateCompetitionCardRow`, drop `renderCompetitionPins` |
| `tests/test_spec_100_cbp_competition.py` | Create | All 11 tests |
| `tests/test_spec_091_decision_map_competition.py` | Modify | Re-shape the 12 existing tests to the new CBP shape (drop Yelp-specifics) |

## Feedback History

_No corrections yet._
