# SPEC 091 — Decision Map: real competition data (Yelp Fusion)

**Status:** Draft
**Task type:** service (backend + frontend)
**Date:** 2026-05-31
**Plan:** PLAN_075 extension (Phase D enhancement)
**Test file:** tests/test_spec_091_decision_map_competition.py
**Builds on:** SPEC_089 (trade-area) + SPEC_090 (Pilot tools)

## Goal

When the user enters trade-area mode, also surface **real competing
businesses** within the radius — pulled live from Yelp Fusion using the
user's thesis (industry_label → Yelp `term`). Map: a separate point
layer on top of the radius circle. Trade Area card: a "Competition" line
with the count + a top-list. Pilot can call it too.

## Acceptance Criteria

- [ ] New backend service `app/services/atlas/competition.py` with
      `find_competition(lat, lon, radius_mi, term=None, categories=None,
      limit=20)`. Caps `radius_mi` at 25 (Yelp's hard limit) and
      converts to meters internally.
- [ ] New endpoint `POST /atlas/competition` body
      `{lat, lon, radius_mi, term?, categories?, limit?}`. Returns
      `{count, total, businesses: [{name, lat, lon, rating, review_count,
      url, address, distance_mi}], term_used, radius_mi}`. When Yelp is
      unconfigured / errors, returns `{count: 0, businesses: [],
      error: "..."}` (HTTP 200 — soft fail so the trade-area UI still
      shows demographics).
- [ ] Frontend: on `enterTradeArea`, if the saved thesis has
      `industry_label`, ALSO `POST /atlas/competition` with the focal
      lat/lon + min(25, radius_mi) and render the results as **distinct
      orange pins** inside the radius circle. Tooltip shows
      `<name> · ★ rating · <reviews>`.
- [ ] Trade Area card gains a Competition row:
      `Competition: <count> "<term>" businesses within <r> mi`.
- [ ] `exitTradeArea` removes the competition pins too.
- [ ] Pilot tool `find_competition(geo_id, radius_mi, term?)` (read kind)
      that resolves the geo_id to centroid via existing
      `county_centroids`, calls `find_competition`, returns count + top 5.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_radius_capped_at_25mi | radius_mi > 25 caps to 25 |
| T2 | test_meters_conversion | request to client uses round(radius_mi*1609.34) meters |
| T3 | test_response_shape | response has count, businesses[], term_used, radius_mi |
| T4 | test_returns_empty_when_yelp_key_missing | YELP_API_KEY absent → soft fail, no exception |
| T5 | test_business_distance_field_computed | each business gets distance_mi (haversine from focal) |
| T6 | test_competition_body_schema | CompetitionBody validates {lat, lon, radius_mi} required-ish |
| T7 | test_frontend_competition_pin_class | atlas.html: CSS for `.competition-pin`; renderCompetitionPins fn present |
| T8 | test_frontend_enter_trade_area_calls_competition | enterTradeArea invokes fetchCompetition or similar when industry_label set |
| T9 | test_pilot_tool_find_competition_registered | pilot_tools.TOOL_CALLABLES contains find_competition (read) |
| T10 | test_pilot_tool_find_competition_returns_count | dispatch with mocked find_competition returns count + top |

## Design

### Backend service

```python
# app/services/atlas/competition.py
import asyncio, math, os
from typing import Any, Dict, List, Optional
from app.sources.yelp.client import YelpClient
from app.services.atlas.trade_area import haversine_miles

_YELP_MAX_RADIUS_MI = 25.0
_METERS_PER_MILE = 1609.34

def _miles_to_meters(mi: float) -> int:
    return int(round(mi * _METERS_PER_MILE))

async def _do_search(lat, lon, radius_mi, term, categories, limit):
    if not os.getenv("YELP_API_KEY"):
        return {"count": 0, "total": 0, "businesses": [],
                "error": "YELP_API_KEY not set", "term_used": term,
                "radius_mi": radius_mi}
    radius_mi = max(0.5, min(_YELP_MAX_RADIUS_MI, float(radius_mi or 5)))
    async with YelpClient() as yc:
        res = await yc.search_businesses(
            latitude=lat, longitude=lon,
            term=term, categories=categories,
            radius=_miles_to_meters(radius_mi),
            limit=min(50, int(limit or 20)),
        )
    raw = res.get("businesses", []) or []
    bs = []
    for b in raw:
        coord = (b.get("coordinates") or {})
        bla, blo = coord.get("latitude"), coord.get("longitude")
        if bla is None or blo is None: continue
        loc = b.get("location") or {}
        bs.append({
            "id":   b.get("id"),
            "name": b.get("name"),
            "lat":  bla, "lon": blo,
            "rating": b.get("rating"),
            "review_count": b.get("review_count"),
            "url": b.get("url"),
            "address": ", ".join(loc.get("display_address") or []),
            "distance_mi": round(haversine_miles(lat, lon, bla, blo), 1),
        })
    bs.sort(key=lambda x: x["distance_mi"])
    return {
        "count": len(bs),
        "total": res.get("total", len(bs)),
        "businesses": bs,
        "term_used": term, "radius_mi": radius_mi,
    }

def find_competition(lat, lon, radius_mi=5.0, term=None,
                      categories=None, limit=20):
    try:
        return asyncio.run(_do_search(lat, lon, radius_mi, term,
                                        categories, limit))
    except Exception as exc:
        return {"count": 0, "businesses": [], "error": str(exc),
                "term_used": term, "radius_mi": radius_mi}
```

### Endpoint

```python
class CompetitionBody(BaseModel):
    lat: float; lon: float
    radius_mi: Optional[float] = 5.0
    term: Optional[str] = None
    categories: Optional[str] = None
    limit: Optional[int] = 20

@router.post("/competition")
def atlas_competition(body, db=Depends(get_db)):
    from app.services.atlas.competition import find_competition
    return find_competition(body.lat, body.lon, body.radius_mi,
                             body.term, body.categories, body.limit)
```

### Frontend

- After `enterTradeArea` paints the card, kick off `fetchCompetition`
  if `loadThesis().industry_label` exists:
  ```js
  async function fetchCompetition(lat, lon, radiusMi, term) {
    const r = await fetch(API + '/competition', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ lat, lon,
        radius_mi: Math.min(25, radiusMi || 25), term, limit: 30 }),
    }).then(x => x.json());
    renderCompetitionPins(r);
    updateCardCompetition(r);
  }
  ```
- `renderCompetitionPins(r)`: build a `L.layerGroup` of orange
  divIcon markers; bind tooltip `name · ★rating · n reviews`.
- `clearCompetitionPins()` called from `exitTradeArea`.

### Pilot tool

```python
def _tool_find_competition(db, geo_id, radius_mi=5.0, term=None):
    from app.services.atlas.trade_area import county_centroids
    from app.services.atlas.competition import find_competition
    c = county_centroids(db).get(geo_id)
    if not c: return {"error": f"unknown geo_id {geo_id!r}"}
    lat, lon, _ = c
    r = find_competition(lat, lon, radius_mi, term=term, limit=20)
    return {"count": r.get("count"), "term_used": r.get("term_used"),
            "top": (r.get("businesses") or [])[:5]}
```

Registered as `"read"` in TOOLS.

## Files

| File | Action |
|------|--------|
| `app/services/atlas/competition.py` | Create |
| `app/api/v1/atlas.py` | CompetitionBody + POST /competition |
| `app/services/atlas/pilot_tools.py` | `find_competition` tool + registration |
| `frontend/atlas.html` | competition CSS + fetchCompetition / renderCompetitionPins / clearCompetitionPins, hook into enterTradeArea/exitTradeArea + Trade Area card row |
| `tests/test_spec_091_decision_map_competition.py` | T1-T10 |

## Verification

1. Demo thesis (Furniture stores) → click top pin → Trade Area card opens,
   orange competition pins render within radius, card shows `Competition:
   N "Furniture stores" within 25 mi`.
2. POST `/atlas/competition {lat, lon, radius_mi: 5, term: "coffee"}`
   returns a non-empty `businesses` list (assuming network + key OK).
3. Without `YELP_API_KEY`: response is `{count:0, businesses:[], error:…}`
   and the Trade Area card just doesn't show the competition row.
4. Pilot: "How many competing furniture stores are in Loudoun's trade
   area?" → agent calls `find_competition(geo_id="51107", radius_mi=10,
   term="furniture")`, narrates the count + top 5.
