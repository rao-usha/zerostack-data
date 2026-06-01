"""SPEC_089 — Atlas Decision Map: trade-area mode.

Given a focal `geo_id`, return the focal county's summary stats plus
the counties whose centroids fall within `radius_mi` miles (haversine).
Used by the frontend when a top-N fit-score pin is clicked, and by the
Pilot's `enter_trade_area` tool (SPEC_090).
"""
from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.services.atlas.boundaries import fetch_boundaries
from app.services.atlas.layers import build_layer

logger = logging.getLogger(__name__)


# Earth radius (statute miles) for haversine
_EARTH_R_MI = 3958.7613

# In-process cache for {geo_id: (lat, lon, name)}. Built once from
# fetch_boundaries('county') then re-used across requests.
_COUNTY_CENTROIDS: Optional[Dict[str, Tuple[float, float, str]]] = None


def haversine_miles(lat1: float, lon1: float,
                     lat2: float, lon2: float) -> float:
    """Great-circle distance in statute miles."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2)
    return 2 * _EARTH_R_MI * math.asin(math.sqrt(a))


def _polygon_centroid(geom: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """Naive vertex-mean centroid for Polygon or MultiPolygon. Adequate
    for 'is X within radius mi of Y' at county scale."""
    coords: List[List[float]] = []
    if not geom:
        return None
    t = geom.get("type")
    cs = geom.get("coordinates")
    if t == "Polygon" and cs:
        for ring in cs:
            coords.extend(ring)
    elif t == "MultiPolygon" and cs:
        for poly in cs:
            for ring in poly:
                coords.extend(ring)
    else:
        return None
    if not coords:
        return None
    lon = sum(c[0] for c in coords) / len(coords)
    lat = sum(c[1] for c in coords) / len(coords)
    return (lat, lon)


def county_centroids(db: Session, force_refresh: bool = False
                      ) -> Dict[str, Tuple[float, float, str]]:
    """Return `{geo_id: (lat, lon, name)}` for every county boundary.
    Cached in-process; first call computes from the boundaries table."""
    global _COUNTY_CENTROIDS
    if _COUNTY_CENTROIDS is not None and not force_refresh:
        return _COUNTY_CENTROIDS
    fc = fetch_boundaries(db, geo_level="county", tolerance=0.01)
    out: Dict[str, Tuple[float, float, str]] = {}
    for feat in (fc.get("features") or []):
        props = feat.get("properties") or {}
        gid = props.get("geo_id")
        if not gid:
            continue
        name = props.get("geo_name") or gid
        c = _polygon_centroid(feat.get("geometry") or {})
        if c is None:
            continue
        out[gid] = (c[0], c[1], name)
    _COUNTY_CENTROIDS = out
    logger.info("trade_area: built centroid cache for %d counties", len(out))
    return out


def _avg(items: List[Optional[float]]) -> Optional[float]:
    nums = [v for v in items if isinstance(v, (int, float))]
    if not nums:
        return None
    return round(sum(nums) / len(nums), 1)


def compute_trade_area(
    db: Session,
    geo_id: str,
    radius_mi: Optional[float] = 50.0,
) -> Dict[str, Any]:
    """Return focal + neighbours within radius + an aggregate summary."""
    radius_mi = max(1.0, min(250.0, float(radius_mi or 50.0)))
    centroids = county_centroids(db)
    if geo_id not in centroids:
        return {"error": f"unknown geo_id {geo_id!r}",
                "radius_mi": radius_mi,
                "focal": None, "neighbors": [], "summary": {}}

    focal_lat, focal_lon, focal_name = centroids[geo_id]

    # Pull the layers we want to summarise once
    def _vals(layer_id: str) -> Dict[str, Any]:
        try:
            return build_layer(db, layer_id).values or {}
        except Exception as exc:  # noqa: BLE001
            logger.warning("trade_area: layer %s failed: %s", layer_id, exc)
            return {}

    inc = _vals("demo_acs_median_income")
    est = _vals("econ_cbp_establishments_county")
    bb  = _vals("infra_broadband_subscription")
    nri = _vals("disaster_nri")

    neighbors: List[Dict[str, Any]] = []
    for gid, (la, lo, nm) in centroids.items():
        if gid == geo_id:
            continue
        d = haversine_miles(focal_lat, focal_lon, la, lo)
        if d > radius_mi:
            continue
        neighbors.append({
            "geo_id": gid, "name": nm,
            "distance_mi": round(d, 1),
            "income": inc.get(gid),
            "establishments": est.get(gid),
            "broadband": bb.get(gid),
            "nri": nri.get(gid),
        })
    neighbors.sort(key=lambda n: n["distance_mi"])

    nri_vals = [n["nri"] for n in neighbors
                 if isinstance(n["nri"], (int, float))]
    summary = {
        "n_neighbors": len(neighbors),
        "avg_income": _avg([n["income"] for n in neighbors]),
        "avg_establishments": _avg([n["establishments"] for n in neighbors]),
        "avg_broadband": _avg([n["broadband"] for n in neighbors]),
        "max_nri": (max(nri_vals) if nri_vals else None),
    }

    return {
        "focal": {
            "geo_id": geo_id, "name": focal_name,
            "lat": focal_lat, "lon": focal_lon,
            "income": inc.get(geo_id),
            "establishments": est.get(geo_id),
            "broadband": bb.get(geo_id),
            "nri": nri.get(geo_id),
        },
        "radius_mi": radius_mi,
        "neighbors": neighbors,
        "summary": summary,
    }
