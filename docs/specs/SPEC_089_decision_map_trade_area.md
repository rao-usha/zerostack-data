# SPEC 089 — Decision Map Phase D: trade-area mode

**Status:** Draft
**Task type:** service (backend + frontend)
**Date:** 2026-05-31
**Plan:** PLAN_075 (Phase D of E)
**Test file:** tests/test_spec_089_decision_map_trade_area.py
**Builds on:** SPEC_087 (fit-score) + SPEC_088 (chips)

## Goal

Clicking a top-N pin (or programmatic call) **enters trade-area mode**:
- Zoom to the candidate's centroid.
- Draw a configurable radius circle.
- Show a **Trade Area card** summarising the candidate + neighbours
  within the radius (avg income, population, broadband, NRI; count of
  neighbour counties).
- Backend `/atlas/trade-area?geo_id=…&radius_mi=…` returns the focal
  geo's stats + the set of neighbour counties within radius (haversine
  on county centroids) + an aggregate summary.

## Acceptance Criteria

- [ ] `POST /atlas/trade-area` with `{geo_id, radius_mi}` returns
      `{focal, neighbors: [...], summary: {...}}`.
- [ ] `radius_mi` clamped to 1..250 server-side; defaults 50.
- [ ] Neighbors are counties whose centroids fall within `radius_mi`
      of the focal centroid (haversine), excluding the focal itself.
- [ ] Summary aggregates: avg_income, total_population (or a proxy),
      avg_broadband, max_nri, n_neighbors.
- [ ] Frontend: clicking a top-N pin enters trade-area mode — zooms
      to centroid, draws an L.circle radius, opens a Trade Area card.
- [ ] The Trade Area card shows focal name + score + the summary + a
      neighbours list (sortable by distance).
- [ ] An "✕ Exit trade area" control returns to the fit-score view.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_haversine_basic | great-circle distance correct for known pairs |
| T2 | test_neighbors_within_radius_excludes_focal | focal geo not in neighbours list |
| T3 | test_radius_clamps | radius_mi clamped to [1, 250] |
| T4 | test_trade_area_returns_focal_and_summary | focal contains name + score-ready stats; summary has the 5 aggregate keys |
| T5 | test_trade_area_unknown_geo | unknown geo_id → 404-like error response, not crash |
| T6 | test_body_schema_trade_area | TradeAreaBody validates fields, defaults |
| T7 | test_frontend_pin_click_enters_trade_area | atlas.html: pin click handler routes through enterTradeArea(geo_id) |
| T8 | test_frontend_trade_area_card_markup | #trade-area-card + exit button exist |
| T9 | test_frontend_trade_area_radius_default | default radius is 50 miles in the request |

## Design

### Backend

```python
# app/services/atlas/trade_area.py
import math
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from app.services.atlas.layers import build_layer
from app.services.atlas.boundaries import county_centroids_cache

def haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R = 3958.7613
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlam/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def compute_trade_area(db, geo_id, radius_mi=50.0):
    radius_mi = max(1.0, min(250.0, float(radius_mi or 50.0)))
    centroids = county_centroids_cache(db)  # {gid: (lat, lon, name)}
    if geo_id not in centroids:
        return {"error": f"unknown geo_id {geo_id}"}
    lat, lon, name = centroids[geo_id]
    # Pull the layers we want to summarize once
    inc = (build_layer(db, "demo_acs_median_income").values or {})
    est = (build_layer(db, "econ_cbp_establishments_county").values or {})
    bb  = (build_layer(db, "infra_broadband_subscription").values or {})
    nri = (build_layer(db, "disaster_nri").values or {})
    neighbors = []
    for gid, (la, lo, nm) in centroids.items():
        if gid == geo_id: continue
        d = haversine_miles(lat, lon, la, lo)
        if d > radius_mi: continue
        neighbors.append({
          "geo_id": gid, "name": nm, "distance_mi": round(d, 1),
          "income": inc.get(gid), "establishments": est.get(gid),
          "broadband": bb.get(gid), "nri": nri.get(gid),
        })
    neighbors.sort(key=lambda n: n["distance_mi"])
    def _avg(key):
        vs = [n[key] for n in neighbors if isinstance(n[key], (int, float))]
        return round(sum(vs) / len(vs), 1) if vs else None
    return {
      "focal": {
        "geo_id": geo_id, "name": name, "lat": lat, "lon": lon,
        "income": inc.get(geo_id), "establishments": est.get(geo_id),
        "broadband": bb.get(geo_id), "nri": nri.get(geo_id),
      },
      "radius_mi": radius_mi,
      "neighbors": neighbors,
      "summary": {
        "n_neighbors": len(neighbors),
        "avg_income": _avg("income"),
        "avg_establishments": _avg("establishments"),
        "avg_broadband": _avg("broadband"),
        "max_nri": max([n["nri"] for n in neighbors
                         if isinstance(n["nri"], (int, float))], default=None),
      },
    }
```

`county_centroids_cache(db)` — simple computation over the county
GeoJSON (cached in-process). I'll plumb it through `boundaries.py`.

### Endpoint

```python
class TradeAreaBody(BaseModel):
    geo_id: str
    radius_mi: Optional[float] = 50.0

@router.post("/trade-area")
def atlas_trade_area(body, db=Depends(get_db)):
    from app.services.atlas.trade_area import compute_trade_area
    return compute_trade_area(db, body.geo_id, body.radius_mi)
```

### Frontend

- Pin click no longer just calls `selectPlace`; it also calls
  `enterTradeArea(geo_id)` which:
  - POSTs `/trade-area` with `geo_id` + the active `radius_mi`.
  - Zooms to `focal` and draws an `L.circle([lat,lon], radius_mi*1609.34)`.
  - Renders a `#trade-area-card` overlay with focal stats + summary +
    sortable neighbours list.
- An `#trade-area-exit` (×) clears the circle, hides the card,
  re-paints the fit-score view (no zoom-out automatically — leave the
  user where they were).
- Persist `state.tradeArea = {geo_id, radius_mi}` for back-compat with
  URL share later (deferred).

## Files

| File | Action |
|------|--------|
| `app/services/atlas/trade_area.py` | Create |
| `app/services/atlas/boundaries.py` | Add `county_centroids_cache` helper |
| `app/api/v1/atlas.py` | TradeAreaBody + POST /trade-area |
| `frontend/atlas.html` | enterTradeArea + Trade Area card + pin handler update |
| `tests/test_spec_089_decision_map_trade_area.py` | T1-T9 |

## Verification

1. Load demo → fit-score paints with 10 pins.
2. Click pin #1 (Loudoun VA) → map zooms, draws 50-mi circle, card
   pops with focal stats + neighbour aggregate.
3. Click × on card → circle + card disappear, fit-score still painted.
4. Try `POST /atlas/trade-area {geo_id: '06037', radius_mi: 30}` →
   200 with focal + neighbours.
