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
from typing import Any, Dict, List, Optional

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


def _unwrap_geometry(geojson: Dict[str, Any], tolerance: float) -> Dict[str, Any]:
    """Pure Feature-unwrapper — `geojson_boundaries.geojson` stores wrapped
    Features, we need bare geometries for the frontend.

    Previously this function ALSO did naive per-ring vertex skipping
    (`ring[::step]`) when PostGIS was unavailable. That approach created
    inter-county gaps ("glass shards"): adjacent counties' rings start
    at different vertices and traverse the shared edge in opposite
    orientations, so `[::step]` picks DIFFERENT vertices on each side
    and the shared boundary no longer aligns.

    The proper fix is TopoJSON / shared-arc simplification (a future
    SPEC), which simplifies the shared edge once instead of per-county.
    Until then, ship raw geometry (~3.3MB unsimplified vs the broken
    1.2MB simplified). Browser handles 3MB GeoJSON fine; in-memory
    cache means the payload only ships once per session.

    The `tolerance` parameter is preserved on the API surface and the
    cache key so the future TopoJSON path can use it without breaking
    callers.
    """
    if not geojson:
        return geojson
    # Unwrap a Feature → its geometry
    if geojson.get("type") == "Feature":
        geojson = geojson.get("geometry") or {}
    if not geojson or geojson.get("type") not in ("Polygon", "MultiPolygon"):
        return geojson
    # Raw geometry — no thinning. See docstring above for the why.
    _ = tolerance  # intentionally ignored on this path
    return geojson


def fetch_boundaries(
    db: Session,
    geo_level: str = "county",
    tolerance: float = 0.005,
    force_refresh: bool = False,
    bbox: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a GeoJSON FeatureCollection of boundaries at `geo_level`.

    `bbox` is "minx,miny,maxx,maxy" — when set, restricts to polygons
    whose stored bbox intersects the query bbox. Critical for tract
    grain (78k+ features nationally would otherwise OOM the frontend
    or timeout the response)."""
    # SPEC_077 A.2 — added tract + zcta for the commerce simulator
    if geo_level not in ("county", "state", "tract", "zcta"):
        raise ValueError(
            f"geo_level must be one of 'county', 'state', 'tract', 'zcta'; "
            f"got {geo_level!r}"
        )

    # Parse bbox if provided: "minx,miny,maxx,maxy"
    bbox_clause = ""
    bbox_params: Dict[str, Any] = {}
    if bbox:
        try:
            mn_x, mn_y, mx_x, mx_y = [float(x) for x in bbox.split(",")]
            # Stored bbox columns are TEXT (per existing schema); cast to numeric
            bbox_clause = (
                " AND CAST(bbox_minx AS NUMERIC) <= :mx_x"
                " AND CAST(bbox_maxx AS NUMERIC) >= :mn_x"
                " AND CAST(bbox_miny AS NUMERIC) <= :mx_y"
                " AND CAST(bbox_maxy AS NUMERIC) >= :mn_y"
            )
            bbox_params = {"mn_x": mn_x, "mn_y": mn_y, "mx_x": mx_x, "mx_y": mx_y}
        except (ValueError, TypeError) as exc:
            raise ValueError(f"invalid bbox {bbox!r}: {exc}")

    # Cache key includes bbox so different viewports cache independently.
    cache_key = f"{geo_level}:{tolerance}:{bbox or 'none'}"
    if not force_refresh and cache_key in _GEOMETRY_CACHE:
        return _GEOMETRY_CACHE[cache_key]

    postgis = _has_postgis(db)
    logger.info("Atlas boundaries: geo_level=%s postgis=%s tolerance=%s",
                geo_level, postgis, tolerance)

    if postgis:
        rows = db.execute(text(f"""
            SELECT geo_id, geo_name,
                   ST_AsGeoJSON(ST_Simplify(
                       ST_GeomFromGeoJSON(geojson::text), :tol
                   ))::json AS geom
            FROM geojson_boundaries
            WHERE geo_level = :lvl{bbox_clause}
        """), {"lvl": geo_level, "tol": tolerance, **bbox_params}).mappings().all()
        features = [
            {
                "type": "Feature",
                "properties": {"geo_id": r["geo_id"], "geo_name": r["geo_name"]},
                "geometry": r["geom"],
            }
            for r in rows if r["geom"]
        ]
    else:
        rows = db.execute(text(f"""
            SELECT geo_id, geo_name, geojson
            FROM geojson_boundaries
            WHERE geo_level = :lvl{bbox_clause}
        """), {"lvl": geo_level, **bbox_params}).mappings().all()
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
                "geometry": _unwrap_geometry(geom, tolerance),
            })

    fc = {
        "type": "FeatureCollection",
        "_meta": {
            "geo_level": geo_level,
            "tolerance": tolerance,
            "simplifier": "postgis" if postgis else "raw_unsimplified",
            "feature_count": len(features),
        },
        "features": features,
    }
    _GEOMETRY_CACHE[cache_key] = fc
    return fc


def clear_cache() -> None:
    """Test helper — drop the in-process cache."""
    _GEOMETRY_CACHE.clear()
