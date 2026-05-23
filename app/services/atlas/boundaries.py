"""
Atlas boundaries service — SPEC_065 / PLAN_066 v3.

Serves the `geojson_boundaries` table as a GeoJSON FeatureCollection for the
map's choropleth join. 3,279 county + 52 state polygons live on cloud.

Two concerns:
  1. Payload size — the raw Census geometries are heavy. We simplify
     server-side (Douglas-Peucker via PostGIS when available, Python
     fallback otherwise) and cache the result in-process since the geometry
     doesn't change.
  2. Performance — the geometry table is queried once per `geo_level`; we
     hold the simplified FeatureCollection in memory keyed by
     (geo_level, tolerance).

The frontend joins layer values to this geometry client-side.
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ── Cache key — geometry is immutable per deploy ────────────────────────────
# We can't lru_cache on `db` (unhashable), so cache the GeoJSON payload itself
# at module level keyed by geo_level.
_GEOMETRY_CACHE: Dict[str, Dict[str, Any]] = {}


def _has_postgis(db: Session) -> bool:
    """Probe whether PostGIS is installed on the connected DB."""
    try:
        row = db.execute(text("SELECT extname FROM pg_extension WHERE extname='postgis'"))
        return row.first() is not None
    except Exception:  # noqa: BLE001
        try:
            db.rollback()
        except Exception:  # noqa: BLE001
            pass
        return False


def _simplify_python(geojson: Dict[str, Any], tolerance: float) -> Dict[str, Any]:
    """Fallback: very light vertex-skipping when PostGIS isn't available.

    Not a true Douglas-Peucker — drops every Nth point per ring. Good enough
    for v1 county-scale display; PostGIS is the real path when present.
    Tolerance is interpreted as the keep-1-in-N factor (tolerance=0.005 →
    keep 1 in ~5, drop 4 of 5).
    """
    if not geojson or geojson.get("type") not in ("Polygon", "MultiPolygon"):
        return geojson
    # Keep every Nth vertex; minimum 4 vertices per ring.
    step = max(1, int(1.0 / max(tolerance, 0.001) / 50))   # 0.005 → ~step 4
    if step <= 1:
        return geojson

    def thin(ring: List[List[float]]) -> List[List[float]]:
        if len(ring) <= 8:
            return ring
        kept = ring[::step]
        if kept[-1] != ring[-1]:
            kept.append(ring[-1])  # keep closing point
        return kept if len(kept) >= 4 else ring

    g = dict(geojson)
    if g["type"] == "Polygon":
        g["coordinates"] = [thin(r) for r in g["coordinates"]]
    else:  # MultiPolygon
        g["coordinates"] = [[thin(r) for r in poly] for poly in g["coordinates"]]
    return g


def fetch_boundaries(
    db: Session,
    geo_level: str = "county",
    tolerance: float = 0.005,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """Return a GeoJSON FeatureCollection of boundaries at `geo_level`."""
    if geo_level not in ("county", "state"):
        raise ValueError(f"geo_level must be 'county' or 'state'; got {geo_level!r}")

    cache_key = f"{geo_level}:{tolerance}"
    if not force_refresh and cache_key in _GEOMETRY_CACHE:
        return _GEOMETRY_CACHE[cache_key]

    postgis = _has_postgis(db)
    logger.info("Atlas boundaries: geo_level=%s postgis=%s tolerance=%s",
                geo_level, postgis, tolerance)

    if postgis:
        rows = db.execute(text("""
            SELECT geo_id, geo_name,
                   ST_AsGeoJSON(ST_Simplify(
                       ST_GeomFromGeoJSON(geojson::text), :tol
                   ))::json AS geom
            FROM geojson_boundaries
            WHERE geo_level = :lvl
        """), {"lvl": geo_level, "tol": tolerance}).mappings().all()
        features = [
            {
                "type": "Feature",
                "properties": {"geo_id": r["geo_id"], "geo_name": r["geo_name"]},
                "geometry": r["geom"],
            }
            for r in rows if r["geom"]
        ]
    else:
        rows = db.execute(text("""
            SELECT geo_id, geo_name, geojson
            FROM geojson_boundaries
            WHERE geo_level = :lvl
        """), {"lvl": geo_level}).mappings().all()
        features = []
        for r in rows:
            geom = r["geojson"]
            if isinstance(geom, str):
                try:
                    geom = json.loads(geom)
                except Exception:  # noqa: BLE001
                    continue
            if not geom:
                continue
            features.append({
                "type": "Feature",
                "properties": {"geo_id": r["geo_id"], "geo_name": r["geo_name"]},
                "geometry": _simplify_python(geom, tolerance),
            })

    fc = {
        "type": "FeatureCollection",
        "_meta": {
            "geo_level": geo_level,
            "tolerance": tolerance,
            "simplifier": "postgis" if postgis else "python_thin",
            "feature_count": len(features),
        },
        "features": features,
    }
    _GEOMETRY_CACHE[cache_key] = fc
    return fc


def clear_cache() -> None:
    """Test helper — drop the in-process cache."""
    _GEOMETRY_CACHE.clear()
